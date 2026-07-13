"""
Resolve parent links in one batched, parallel pass over netCDF headers

`cmip_data_manager.search.aggregate` matches model variants using an injected
`ParentResolver` (`Cell -> Cell | None`) and performs no I/O itself.  This module
computes those links up front: `resolve_parent_links` finds the near-miss cells,
reads their parents from netCDF headers, and returns a plain `dict[Cell, Cell]`
whose `.get` is the resolver the matcher uses.

Resolving up front (rather than lazily, one cell at a time) is what makes this
scale: every near-miss cell's per-variable header reads across the whole run — and
across each hop of a parent chain — are flattened into a single `read_map` batch,
so the cost is roughly *(chain hops x one parallel read batch)* regardless of how
many cells there are.  The search client is not picklable, so file lookups happen
here (thread-friendly httpx) while only the netCDF reads are handed to `read_map`
(a process pool, since netCDF is not thread-safe).

One header is read per `variable_id` (parent attributes are per-simulation
globals); a cell whose variables disagree is recorded as a `ParentConflict` and
skipped rather than aborting the run.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from functools import partial

from cmip_data_manager.esgf.client import ESGFSearchClient
from cmip_data_manager.esgf.concurrency import MapFn, serial_map
from cmip_data_manager.esgf.models import DatasetRecord
from cmip_data_manager.esgf.parents import (
    HeaderReader,
    ParentInfo,
    _read_first_readable,
    read_parent_info,
    variable_url_groups,
)
from cmip_data_manager.esgf.query import FacetQuery
from cmip_data_manager.search.aggregate import Cell, Cells


@dataclass(frozen=True)
class ParentConflict:
    """A cell whose variables disagreed on their parent metadata (and was skipped)."""

    cell: Cell
    infos: frozenset[ParentInfo]


def parent_cell_of(info: ParentInfo, source_id: str) -> Cell | None:
    """
    Turn a `ParentInfo` into the parent's cell key, if it identifies one

    Parameters
    ----------
    info
        Parent metadata read from a file header.

    source_id
        The child's `source_id`, used when the header omits `parent_source_id`
        (the parent of a CMIP6 run is the same model).

    Returns
    -------
    :
        The parent `(source_id, variant_label, experiment_id)`, or `None` when the
        header declares no usable parent (e.g. `piControl`, which has none).
    """
    if info.variant_label is None or info.experiment_id is None:
        return None
    return (info.source_id or source_id, info.variant_label, info.experiment_id)


def _ids_by_cell(records: list[DatasetRecord]) -> dict[Cell, list[str]]:
    """Index dataset ids by their `(source_id, variant_label, experiment_id)` cell."""
    ids: dict[Cell, list[str]] = defaultdict(list)
    for record in records:
        if record.source_id and record.variant_label and record.experiment_id:
            ids[(record.source_id, record.variant_label, record.experiment_id)].append(
                record.id
            )
    return ids


def _near_miss_bases(
    cells: Cells, via_parent: Mapping[str, str], needed: set[str]
) -> set[Cell]:
    """Return base cells to walk from (base covered, target not covered directly)."""
    frontier: set[Cell] = set()
    for source, variant in {(s, v) for (s, v, _e) in cells}:
        for target, base in via_parent.items():
            covers_base = needed <= cells.get((source, variant, base), set())
            covers_target = needed <= cells.get((source, variant, target), set())
            if covers_base and not covers_target:
                frontier.add((source, variant, base))
    return frontier


def resolve_parent_links(  # noqa: PLR0913 - deliberately configurable DI seam
    cells: Cells,
    *,
    via_parent: Mapping[str, str],
    required_vars: Sequence[str],
    records: list[DatasetRecord],
    client: ESGFSearchClient,
    reader: HeaderReader = read_parent_info,
    read_map: MapFn = serial_map,
    ignore_hosts: frozenset[str] = frozenset(),
    conflicts: list[ParentConflict] | None = None,
    max_hops: int = 5,
) -> dict[Cell, Cell]:
    """
    Resolve every near-miss cell's parent chain in batched, parallel passes

    Parameters
    ----------
    cells
        Aggregated cells for the run (see `build_cells`).

    via_parent
        Maps a required experiment to the base experiment to walk its parent chain
        from (e.g. `{"piControl": "abrupt-4xCO2"}`).

    required_vars
        Variables a base experiment must contain for its cell to be worth walking.

    records
        The run's datasets, used to find each cell's files (by dataset id).  Walking
        a chain requires the intermediate experiments' datasets to be present here.

    client
        Search client used to look up a cell's files.

    reader
        Reads one file's `ParentInfo`.  Defaults to `read_parent_info` (netCDF4).

    read_map
        Strategy for the header reads.  Defaults to serial; pass
        `process_pool_map(...)` for parallelism (netCDF is not thread-safe).

    ignore_hosts
        Hostnames to never read headers from (e.g. unresponsive data nodes).

    conflicts
        If given, cells whose variables disagree on their parent are appended here
        and skipped, rather than aborting.

    max_hops
        Maximum parent-chain depth to walk (guards against cycles/runaways).

    Returns
    -------
    :
        A `dict` mapping each resolved cell to its parent cell; its `.get` is a
        `ParentResolver` for `pairs_all_experiments`.
    """
    needed = set(required_vars)
    ids_by_cell = _ids_by_cell(records)
    reader_worker = partial(_read_first_readable, reader=reader)

    links: dict[Cell, Cell] = {}
    attempted: set[Cell] = set()
    frontier = _near_miss_bases(cells, via_parent, needed)

    for _hop in range(max_hops):
        todo = sorted(c for c in frontier if c not in attempted and c in ids_by_cell)
        attempted |= frontier
        if not todo:
            break

        owners: list[Cell] = []
        groups: list[list[str]] = []
        for cell in todo:
            files = client.search_files(
                FacetQuery(type="File", dataset_id=tuple(ids_by_cell[cell]))
            )
            for group in variable_url_groups(files, ignore_hosts):
                owners.append(cell)
                groups.append(group)

        results = read_map(reader_worker, groups) if groups else []
        infos_by_cell: dict[Cell, set[ParentInfo]] = defaultdict(set)
        for owner, info in zip(owners, results, strict=True):
            if info is not None:
                infos_by_cell[owner].add(info)

        frontier = set()
        for cell in todo:
            infos = infos_by_cell.get(cell)
            if not infos:
                continue
            if len(infos) > 1:
                if conflicts is not None:
                    conflicts.append(ParentConflict(cell, frozenset(infos)))
                continue
            parent = parent_cell_of(next(iter(infos)), cell[0])
            if parent is None:
                continue
            links[cell] = parent
            # Keep walking up the chain — even through a target experiment — so a
            # required experiment that is itself an intermediate link (e.g. the
            # `historical` between `ssp119` and `piControl`) does not cut the walk
            # short.  The walk still terminates naturally: a cell whose header
            # declares no parent (e.g. `piControl`) yields `None` above, and
            # `attempted`/`max_hops` guard against cycles and runaways.
            if parent not in attempted:
                frontier.add(parent)

    return links
