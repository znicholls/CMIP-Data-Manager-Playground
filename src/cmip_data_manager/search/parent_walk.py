"""
Step 4 — walk the parent chain on the node-independent model, one search per parent

Starting from a set of child datasets, this reads each child's header-only metadata
(via Step 3), projects the declared parent `(source_id, experiment_id, variant_label)`,
and issues **one index search per distinct parent simulation** (never institution — a
parent's institution is unknown from the child, so it is discovered from the search;
`source_id` must match, and a missing `parent_source_id` defaults to the child's).  Each
found parent is stored, and the child *version* is linked to the parent *version* of the
same variable (`parent_version_key`); the walk repeats from the newly-found parents.

Stopping is **error-based, not warnings** (design decisions D5/D7/D8):

- **existence gate** — if a `stopping_experiment` is given, one search confirms it
  exists on the index node; if not, `ParentExperimentMissingError` is raised;
- reaching the `stopping_experiment` is success (that parent is a terminal; its header
  is not read and the walk does not go higher);
- `stopping_experiment=None` walks to the CMIP6 `"no parent"` sentinel (a natural top);
- a declared parent with **zero index results** is `ParentNotFound`; a chain that
  reaches the top **without** passing through a given `stopping_experiment` is
  `ParentNotAncestor`.  All chains are walked, then a single `ParentResolutionError`
  aggregates every failure.

A `parent_overrides` mapping (child simulation -> corrected parent simulation) takes
precedence over the header — the seam for user corrections / a future fixes library.
"""
# QUESTION: is it here that the parent search for (source_id, experimetn_id,
# variant_id) is defined? want to be more specific in search -> for variable/s
# and table_id (ned multiple variables eg. for Gregory use case)

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

from cmip_data_manager.db.repository import Repository
from cmip_data_manager.esgf.client import DeepPaginationError, ESGFSearchClient
from cmip_data_manager.esgf.concurrency import MapFn, serial_map
from cmip_data_manager.esgf.dispatch import (
    DEFAULT_MAX_WORKERS,
    DEFAULT_NODE_CONCURRENCY,
)
from cmip_data_manager.esgf.headers import (
    DEFAULT_READ_TIMEOUT,
    HeaderMetadata,
    SimulationKey,
    read_header,
    simulation_key,
)
from cmip_data_manager.esgf.health import NodeHealth
from cmip_data_manager.esgf.models import DatasetRecord
from cmip_data_manager.esgf.query import FacetQuery
from cmip_data_manager.search.files import add_files
from cmip_data_manager.search.version_headers import (
    HeaderReader,
    enrich_version_headers,
)

NO_PARENT_SENTINELS: frozenset[str] = frozenset({"no parent"})
"""
CMIP6 marker(s) for "this run branches from nothing"

A run at the top of the tree (typically a `piControl-spinup`) sets its `parent_*`
attributes to the controlled-vocabulary string `"no parent"` rather than leaving
them blank.  Treating that as a real parent named `"no parent"` would send a walk
chasing a simulation that cannot exist; recognising the sentinel lets the walk stop
where the metadata says the tree ends.
"""


def _is_no_parent(value: str | None) -> bool:
    """Whether a `parent_*` attribute means "no parent" (blank or a CMIP6 sentinel)."""
    if value is None or not value.strip():
        return True
    return value.strip().casefold() in NO_PARENT_SENTINELS


@dataclass(frozen=True)
class _Declared:
    """A child's projected parent declaration (before it is located/verified)."""

    parent: SimulationKey | None
    branch_time_in_parent: str | None


def declared_parent(
    header: HeaderMetadata | None,
    child: SimulationKey,
    parent_experiment: str | None = None,
) -> _Declared:
    """
    Project a child's declared parent from its header's `parent_*` attributes

    The parent's `source_id` defaults to the child's when the header omits
    `parent_source_id` (a CMIP6 run's parent is the same model).  A parent is only
    returned when the header names a `parent_experiment_id` *and* a
    `parent_variant_label` — the variant is never assumed to match the child's.

    Parameters
    ----------
    header
        The child's stored header, or `None` if it could not be read.

    child
        The child simulation key `(source_id, experiment_id, variant_label)`.

    parent_experiment
        The experiment the parent must be (e.g. `"piControl"`).  Pass `None` (the
        default) to accept **whatever** experiment the header declares — the mode a
        multi-hop walk uses, where the chain of experiments is not assumed and each
        parent's experiment is discovered from the header itself.

    Returns
    -------
    :
        The declared parent simulation (or `None`) and the child's
        `branch_time_in_parent`.

    Examples
    --------
    >>> header = HeaderMetadata(
    ...     attrs={
    ...         "parent_experiment_id": "piControl",
    ...         "parent_variant_label": "r1i1p1f1",
    ...         "branch_time_in_parent": "0.0",
    ...     }
    ... )
    >>> declared_parent(header, ("M", "abrupt-4xCO2", "r1i1p1f2"), "piControl")
    _Declared(parent=('M', 'piControl', 'r1i1p1f1'), branch_time_in_parent='0.0')
    """
    if header is None:
        return _Declared(parent=None, branch_time_in_parent=None)
    branch = header.get("branch_time_in_parent")
    experiment = header.get("parent_experiment_id")
    variant = header.get("parent_variant_label")
    if experiment is None or variant is None:
        return _Declared(parent=None, branch_time_in_parent=branch)
    # A blank or the CMIP6 `"no parent"` sentinel also means "no parent".
    if _is_no_parent(experiment) or _is_no_parent(variant):
        return _Declared(parent=None, branch_time_in_parent=branch)
    if parent_experiment is not None and experiment != parent_experiment:
        return _Declared(parent=None, branch_time_in_parent=branch)
    source = header.get("parent_source_id") or child[0]
    parent = (source, experiment, variant)
    return _Declared(parent=parent, branch_time_in_parent=branch)


class ParentExperimentMissingError(RuntimeError):
    """A user-declared stopping experiment does not exist on the index node."""

    def __init__(self, experiment: str) -> None:
        self.experiment = experiment
        super().__init__(
            f"The declared parent experiment {experiment!r} does not exist on the "
            f"index node."
        )


@dataclass(frozen=True)
class ParentNotFound:
    """A child declared a parent that has no datasets on the index node."""

    child: SimulationKey
    parent: SimulationKey

    def __str__(self) -> str:
        """Render the failure as the diagnostic message shown to the user."""
        c = " ".join(self.child)
        p = ", ".join(self.parent)
        return (
            f"{c} declares parent ({p}) but no such dataset is published on the "
            f"index node — the chain cannot be completed."
        )


@dataclass(frozen=True)
class ParentNotAncestor:
    """A chain reached the top without passing through the stopping experiment."""

    child: SimulationKey
    stopping_experiment: str

    def __str__(self) -> str:
        """Render the failure as the diagnostic message shown to the user."""
        c = " ".join(self.child)
        return (
            f"{c} reached the top of the parent tree ('no parent') without passing "
            f"through the declared stopping experiment "
            f"{self.stopping_experiment!r}."
        )


class ParentResolutionError(RuntimeError):
    """One or more chains could not be resolved (aggregates every failure)."""

    def __init__(self, failures: Sequence[ParentNotFound | ParentNotAncestor]) -> None:
        self.failures = list(failures)
        body = "\n".join(f"  - {failure}" for failure in failures)
        super().__init__(
            f"{len(failures)} parent chain(s) could not be resolved:\n{body}"
        )


@dataclass(frozen=True)
class ParentWalkResult:
    """The outcome of a successful parent walk."""

    links: list[tuple[str, str]] = field(default_factory=list)
    """Resolved `(child_version_id, parent_version_id)` edges."""

    terminals: list[SimulationKey] = field(default_factory=list)
    """Simulations where a chain stopped (the stopping experiment or the true top)."""

    hops: int = 0
    """How many frontier hops the walk performed."""


# QUESTION: Removing default of CMIP6?
def parent_experiment_exists(
    client: ESGFSearchClient, experiment: str, *, project: str = "CMIP6"
) -> bool:
    """Whether any dataset for an experiment exists on the index node."""
    return client.count(FacetQuery(project=project, experiment_id=(experiment,))) > 0


def find_parent_datasets(
    client: ESGFSearchClient, parent: SimulationKey, *, project: str = "CMIP6"
) -> list[DatasetRecord]:
    """
    Search the index for one parent simulation's datasets (broad, one query)

    Matches `(source_id, experiment_id, variant_label)` across *any* variable; a
    single simulation with too many replicas to page is retried for primary
    (non-replica) datasets only.  Institution is never constrained.
    """
    source_id, experiment_id, variant_label = parent
    query = FacetQuery(
        project=project,
        source_id=(source_id,),
        experiment_id=(experiment_id,),
        variant_label=(variant_label,),
    )
    try:
        return client.search(query)
    except DeepPaginationError:
        return client.search(
            FacetQuery(
                project=project,
                source_id=(source_id,),
                experiment_id=(experiment_id,),
                variant_label=(variant_label,),
                replica=False,
            )
        )


def resolve_parent_chains(  # noqa: PLR0913 - a DI seam; every parameter has a default
    root_records: Sequence[DatasetRecord],
    *,
    client: ESGFSearchClient,
    repository: Repository,
    stopping_experiment: str | None = None,
    parent_overrides: Mapping[SimulationKey, SimulationKey] | None = None,
    project: str = "CMIP6",
    max_hops: int = 8,
    map_fn: MapFn = serial_map,
    reader: HeaderReader = read_header,
    use_timeout: bool = True,
    health: NodeHealth | None = None,
    preferred_hosts: tuple[str, ...] = (),
    ignore_hosts: frozenset[str] = frozenset(),
    max_workers: int = DEFAULT_MAX_WORKERS,
    node_concurrency: int = DEFAULT_NODE_CONCURRENCY,
    timeout: float = DEFAULT_READ_TIMEOUT,
    skip_cached: bool = True,
) -> ParentWalkResult:
    """
    Walk each child's parent chain, linking versions and raising on any failure

    Parameters
    ----------
    root_records
        The starting child datasets (node-specific records).

    client, repository
        Search client and cache.

    stopping_experiment
        The experiment the chain should stop at; `None` walks to the `"no parent"`
        sentinel.  When given, it is confirmed to exist before walking.

    parent_overrides
        Child simulation -> corrected parent simulation; overrides the header.

    project, max_hops, map_fn, reader, use_timeout
        Search/read controls.

    health
        Node-health registry shared across every hop; defaults to the one persisted
        in `repository` (so health accumulates across runs).

    preferred_hosts, ignore_hosts, max_workers, node_concurrency, timeout
        Routing/read controls forwarded to each hop's `enrich_version_headers` (e.g.
        prefer NCI, avoid known-dead nodes); see that function.

    skip_cached
        Skip simulations whose versions already carry header metadata on each hop.

    Returns
    -------
    :
        The resolved edges, terminals and hop count.

    Raises
    ------
    ParentExperimentMissingError
        If `stopping_experiment` does not exist on the index node.

    ParentResolutionError
        If any chain declared a parent absent from the index, or reached the top
        without passing through `stopping_experiment`.
    """
    if stopping_experiment is not None and not parent_experiment_exists(
        client, stopping_experiment, project=project
    ):
        raise ParentExperimentMissingError(stopping_experiment)

    overrides = dict(parent_overrides or {})
    health = repository.load_node_health() if health is None else health
    state = _WalkState(
        client=client,
        repository=repository,
        stopping_experiment=stopping_experiment,
        overrides=overrides,
        project=project,
        map_fn=map_fn,
        reader=reader,
        use_timeout=use_timeout,
        health=health,
        preferred_hosts=preferred_hosts,
        ignore_hosts=ignore_hosts,
        max_workers=max_workers,
        node_concurrency=node_concurrency,
        timeout=timeout,
        skip_cached=skip_cached,
    )

    frontier = list(root_records)
    hops = 0
    while frontier and hops < max_hops:
        hops += 1
        frontier = state.hop(frontier)

    repository.save_node_health(health)
    if state.failures:
        raise ParentResolutionError(state.failures)
    return ParentWalkResult(links=state.links, terminals=state.terminals, hops=hops)


@dataclass
class _WalkState:
    """Mutable state threaded through the walk's hops."""

    client: ESGFSearchClient
    repository: Repository
    stopping_experiment: str | None
    overrides: dict[SimulationKey, SimulationKey]
    project: str
    map_fn: MapFn
    reader: HeaderReader
    use_timeout: bool
    health: NodeHealth
    preferred_hosts: tuple[str, ...] = ()
    ignore_hosts: frozenset[str] = frozenset()
    max_workers: int = DEFAULT_MAX_WORKERS
    node_concurrency: int = DEFAULT_NODE_CONCURRENCY
    timeout: float = DEFAULT_READ_TIMEOUT
    skip_cached: bool = True
    visited: set[SimulationKey] = field(default_factory=set)
    links: list[tuple[str, str]] = field(default_factory=list)
    terminals: list[SimulationKey] = field(default_factory=list)
    failures: list[ParentNotFound | ParentNotAncestor] = field(default_factory=list)

    def hop(self, frontier: list[DatasetRecord]) -> list[DatasetRecord]:
        """Read a frontier's headers, resolve each declared parent, return the next."""
        add_files(
            frontier, client=self.client, repository=self.repository, map_fn=self.map_fn
        )
        enrich_version_headers(
            list(frontier),
            repository=self.repository,
            health=self.health,
            reader=self.reader,
            preferred_hosts=self.preferred_hosts,
            ignore_hosts=self.ignore_hosts,
            max_workers=self.max_workers,
            node_concurrency=self.node_concurrency,
            timeout=self.timeout,
            use_timeout=self.use_timeout,
            skip_cached=self.skip_cached,
            persist_health=False,
        )

        by_sim: dict[SimulationKey, list[DatasetRecord]] = defaultdict(list)
        for record in frontier:
            sim = simulation_key(record)
            if sim is not None:
                by_sim[sim].append(record)

        declared = {
            sim: self._declared_parent(sim, recs) for sim, recs in by_sim.items()
        }
        for sim in by_sim:
            self.visited.add(sim)

        wanted = sorted({p for p in declared.values() if p is not None})
        found = dict(self.map_fn(self._search, wanted))
        for parent, records in found.items():
            if records:
                self.repository.record_run(
                    records,
                    endpoint_url=self.client.base_url,
                    spec={"parent": list(parent)},
                )

        next_frontier: list[DatasetRecord] = []
        for sim, recs in by_sim.items():
            self._resolve(sim, recs, declared[sim], found, next_frontier)
        return [r for r in next_frontier if simulation_key(r) not in self.visited]

    def _search(
        self, parent: SimulationKey
    ) -> tuple[SimulationKey, list[DatasetRecord]]:
        """Search one parent simulation (the per-parent, parallelisable unit)."""
        return parent, find_parent_datasets(self.client, parent, project=self.project)

    def _declared_parent(
        self, sim: SimulationKey, records: list[DatasetRecord]
    ) -> SimulationKey | None:
        """Project a simulation's declared parent (override first, else its header)."""
        if sim in self.overrides:
            return self.overrides[sim]
        header = self.repository.version_header(records[0].instance_key)
        return declared_parent(header, sim, None).parent

    def _resolve(
        self,
        sim: SimulationKey,
        records: list[DatasetRecord],
        parent: SimulationKey | None,
        found: dict[SimulationKey, list[DatasetRecord]],
        next_frontier: list[DatasetRecord],
    ) -> None:
        """Link, terminate or record a failure for one simulation's declared parent."""
        if parent is None:  # reached the top of the tree
            if self.stopping_experiment is not None:
                self.failures.append(ParentNotAncestor(sim, self.stopping_experiment))
            else:
                self.terminals.append(sim)
            return
        if not found.get(parent):
            self.failures.append(ParentNotFound(sim, parent))
            return
        self._link(records, parent)
        if parent[1] == self.stopping_experiment:
            self.terminals.append(parent)  # reached target: stop, do not read higher
        elif parent not in self.visited:
            next_frontier.extend(found[parent])

    def _link(self, records: list[DatasetRecord], parent: SimulationKey) -> None:
        """Link each child version to the parent version of the same variable."""
        source_id, experiment_id, variant_label = parent
        for record in records:
            parent_version = self.repository.latest_version_for(
                source_id,
                experiment_id,
                variant_label,
                record.variable_id,
                record.table_id,
                record.grid_label,
            )
            if parent_version is not None:
                self.repository.set_parent_version(record.instance_key, parent_version)
                self.links.append((record.instance_key, parent_version))
