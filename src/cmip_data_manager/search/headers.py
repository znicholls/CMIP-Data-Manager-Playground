"""
End-to-end header enrichment for a set of datasets

`enrich_headers` is the use-case-agnostic pipeline that turns search results into
cached header metadata.  For a batch of datasets it:

1. loads persisted node health so ordering and timeouts benefit from earlier runs;
2. groups datasets into simulations `(source_id, experiment_id, variant_label)` —
   the header describes the run, so one read serves every variable of a simulation;
3. skips datasets whose header is already cached, and *reuses* a sibling variable's
   cached header for a simulation rather than re-reading (the payoff of storing at
   the dataset grain: a user who read `tas` gets `rsut`'s header for free);
4. reads one header per remaining simulation, from the best mirror — candidates are
   ordered by preferred node then learned health, each read is bounded by
   `with_timeout`, retried on transient failure by `with_retry`, recorded into
   `NodeHealth`, and a dead mirror falls through to the next;
5. stores the headers (one row per dataset) and saves node health back.

Because `with_timeout` isolates each read in a spawned child process, the reads
are fanned out with a **thread** (or serial) `read_map`, never a process pool, and
a caller using the default (spawning) reader must run from a `__main__`-guarded
script.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

from cmip_data_manager.db.repository import Repository
from cmip_data_manager.esgf.client import ESGFSearchClient
from cmip_data_manager.esgf.concurrency import MapFn, serial_map
from cmip_data_manager.esgf.headers import (
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_READ_TIMEOUT,
    HeaderKey,
    HeaderMetadata,
    SimulationKey,
    candidate_urls_for_files,
    header_key,
    read_first_readable,
    read_header,
    simulation_key,
    with_retry,
    with_timeout,
)
from cmip_data_manager.esgf.health import NodeHealth, recording
from cmip_data_manager.esgf.models import DatasetRecord
from cmip_data_manager.esgf.query import FacetQuery

HeaderReader = Callable[[str], HeaderMetadata]
"""Reads one file's header from a URL (e.g. `read_header`)."""


@dataclass(frozen=True)
class EnrichResult:
    """Summary of an `enrich_headers` pass."""

    read: int = 0
    """Simulations whose header was read from a data node."""

    reused: int = 0
    """Datasets filled from a sibling variable's cached header (no read)."""

    stored: int = 0
    """Header rows written or updated."""

    skipped_cached: int = 0
    """Datasets skipped because their header was already cached."""

    failed: list[SimulationKey] = field(default_factory=list)
    """Simulations whose every candidate mirror failed."""


def _group_by_simulation(
    records: Sequence[DatasetRecord],
) -> dict[SimulationKey, list[DatasetRecord]]:
    """Group keyable datasets by their simulation `(source, experiment, variant)`."""
    groups: dict[SimulationKey, list[DatasetRecord]] = defaultdict(list)
    for record in records:
        sim = simulation_key(record)
        if sim is not None and header_key(record) is not None:
            groups[sim].append(record)
    return groups


def enrich_headers(  # noqa: PLR0913 - a DI seam; every parameter has a default
    records: Sequence[DatasetRecord],
    *,
    client: ESGFSearchClient,
    repository: Repository,
    health: NodeHealth | None = None,
    reader: HeaderReader = read_header,
    preferred_hosts: Sequence[str] = (),
    read_map: MapFn = serial_map,
    timeout: float = DEFAULT_READ_TIMEOUT,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    use_timeout: bool = True,
    skip_cached: bool = True,
    persist_health: bool = True,
) -> EnrichResult:
    """
    Read and cache header metadata for a batch of datasets

    Parameters
    ----------
    records
        Datasets to enrich (e.g. a use case's cached datasets).

    client
        Search client, used to look up each simulation's files.

    repository
        Cache to read existing headers/health from and write results back to.

    health
        Node-health registry to record into; defaults to the one persisted in
        `repository` (so health accumulates across runs).

    reader
        Per-URL header read; defaults to `read_header` (netCDF over byte-range).

    preferred_hosts
        Data nodes to try first (e.g. `("esgf.nci.org.au",)`); optional.

    read_map
        Strategy for the per-simulation reads — `serial_map` (default) or
        `thread_pool_map(...)`.  Not a process pool: `with_timeout` already
        isolates each read in a child process.

    timeout
        Per-read deadline in seconds (see `with_timeout`).

    max_attempts
        Attempts per mirror before falling through (see `with_retry`).

    use_timeout
        Wrap reads in `with_timeout` (spawns a child per read).  Disable only when
        the reader is already bounded or in tests that avoid subprocesses.

    skip_cached
        Skip datasets whose header is already cached, and reuse a sibling
        variable's cached header for a simulation instead of re-reading.

    persist_health
        Save the (updated) node health back to `repository` at the end.

    Returns
    -------
    :
        A summary of what was read, reused, stored, skipped and failed.
    """
    health = repository.load_node_health() if health is None else health
    ignore = health.unreliable_hosts()

    per_read = with_timeout(reader, seconds=timeout) if use_timeout else reader
    per_read = recording(per_read, health)
    per_read = with_retry(per_read, max_attempts=max_attempts)

    to_read: list[SimulationKey] = []
    dataset_ids: dict[SimulationKey, list[str]] = {}
    missing_keys: dict[SimulationKey, list[HeaderKey]] = {}
    reuse: dict[HeaderKey, HeaderMetadata] = {}
    skipped = 0

    for sim, sim_records in _group_by_simulation(records).items():
        keys = [k for r in sim_records if (k := header_key(r)) is not None]
        if skip_cached:
            missing = [k for k in keys if repository.get_header(k) is None]
            skipped += len(keys) - len(missing)
        else:
            missing = keys
        if not missing:
            continue
        siblings = repository.get_simulation_headers(*sim) if skip_cached else []
        if siblings:
            for key in missing:
                reuse[key] = siblings[0]
            continue
        missing_keys[sim] = missing
        dataset_ids[sim] = [r.id for r in sim_records]
        to_read.append(sim)

    candidates: dict[SimulationKey, list[str]] = {}
    for sim in to_read:
        files = client.search_files(
            FacetQuery(type="File", dataset_id=tuple(dataset_ids[sim]))
        )
        candidates[sim] = candidate_urls_for_files(
            files,
            preferred_hosts=preferred_hosts,
            ignore_hosts=ignore,
            host_rank=health.host_rank,
        )

    def read_one(sim: SimulationKey) -> HeaderMetadata | None:
        return read_first_readable(candidates[sim], per_read)

    results = list(read_map(read_one, to_read)) if to_read else []

    headers: dict[HeaderKey, HeaderMetadata] = dict(reuse)
    read_count = 0
    failed: list[SimulationKey] = []
    for sim, metadata in zip(to_read, results, strict=True):
        if metadata is None:
            failed.append(sim)
            continue
        read_count += 1
        for key in missing_keys[sim]:
            headers[key] = metadata

    stored = repository.store_headers(headers) if headers else 0
    if persist_health:
        repository.save_node_health(health)

    return EnrichResult(
        read=read_count,
        reused=len(reuse),
        stored=stored,
        skipped_cached=skipped,
        failed=failed,
    )
