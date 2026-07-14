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
from collections.abc import Callable, Iterator, Sequence
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
from cmip_data_manager.esgf.models import DatasetRecord, FileRecord
from cmip_data_manager.esgf.query import FacetQuery

DEFAULT_FILE_LOOKUP_BATCH = 50
"""Upper bound on datasets OR'd into one file-search request (a count backstop)."""

DEFAULT_FILE_LOOKUP_MAX_CHARS = 3000
"""
Character budget for a batched `dataset_id` query, the real batching limit.

Dataset ids are long (~90 chars) and every replica adds another, so a fixed count
can still build a query the endpoint rejects (one ESGF node 400s past ~4000 raw id
chars).  Batches are packed up to this budget instead, well under that ceiling.
"""


def _chunk_ids(
    ids: Sequence[str], *, max_count: int, max_chars: int
) -> Iterator[list[str]]:
    """Split ids into chunks bounded by both a count and a character budget."""
    chunk: list[str] = []
    chars = 0
    for identifier in ids:
        if chunk and (len(chunk) >= max_count or chars + len(identifier) > max_chars):
            yield chunk
            chunk, chars = [], 0
        chunk.append(identifier)
        chars += len(identifier)
    if chunk:
        yield chunk


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


@dataclass
class _Plan:
    """What each simulation needs: read fresh, reuse a sibling, or nothing."""

    to_read: list[SimulationKey] = field(default_factory=list)
    dataset_ids: dict[SimulationKey, list[str]] = field(default_factory=dict)
    missing_keys: dict[SimulationKey, list[HeaderKey]] = field(default_factory=dict)
    reuse: dict[HeaderKey, HeaderMetadata] = field(default_factory=dict)
    skipped: int = 0


def _plan_reads(
    records: Sequence[DatasetRecord], repository: Repository, *, skip_cached: bool
) -> _Plan:
    """Decide, per simulation, whether to read, reuse a cached sibling, or skip."""
    plan = _Plan()
    for sim, sim_records in _group_by_simulation(records).items():
        keys = [k for r in sim_records if (k := header_key(r)) is not None]
        if skip_cached:
            missing = [k for k in keys if repository.get_header(k) is None]
            plan.skipped += len(keys) - len(missing)
        else:
            missing = keys
        if not missing:
            continue
        siblings = repository.get_simulation_headers(*sim) if skip_cached else []
        if siblings:
            for key in missing:
                plan.reuse[key] = siblings[0]
            continue
        plan.missing_keys[sim] = missing
        plan.dataset_ids[sim] = [r.id for r in sim_records]
        plan.to_read.append(sim)
    return plan


def _lookup_candidates(  # noqa: PLR0913 - internal helper; all callers keyword-pass
    plan: _Plan,
    *,
    client: ESGFSearchClient,
    preferred_hosts: Sequence[str],
    ignore_hosts: frozenset[str],
    host_rank: Callable[[str], tuple[float, float]],
    batch: int,
    max_chars: int,
) -> dict[SimulationKey, list[str]]:
    """
    Find and rank candidate mirror URLs for every simulation to be read

    Files are looked up in a few batched queries rather than one request per
    simulation: ESGF ORs a comma-separated `dataset_id` list, so a chunk of ids
    returns all their files in a single round trip, which are then bucketed back to
    their simulation by `dataset_id`.
    """
    sim_by_dataset_id: dict[str, SimulationKey] = {
        dataset_id: sim for sim in plan.to_read for dataset_id in plan.dataset_ids[sim]
    }
    files_by_sim: dict[SimulationKey, list[FileRecord]] = defaultdict(list)
    all_ids = list(sim_by_dataset_id)
    for chunk in _chunk_ids(all_ids, max_count=batch, max_chars=max_chars):
        for file in client.search_files(
            FacetQuery(type="File", dataset_id=tuple(chunk))
        ):
            owner = sim_by_dataset_id.get(file.dataset_id)
            if owner is not None:
                files_by_sim[owner].append(file)

    return {
        sim: candidate_urls_for_files(
            files_by_sim[sim],
            preferred_hosts=preferred_hosts,
            ignore_hosts=ignore_hosts,
            host_rank=host_rank,
        )
        for sim in plan.to_read
    }


def enrich_headers(  # noqa: PLR0913 - a DI seam; every parameter has a default
    records: Sequence[DatasetRecord],
    *,
    client: ESGFSearchClient,
    repository: Repository,
    health: NodeHealth | None = None,
    reader: HeaderReader = read_header,
    preferred_hosts: Sequence[str] = (),
    ignore_hosts: frozenset[str] = frozenset(),
    read_map: MapFn = serial_map,
    timeout: float = DEFAULT_READ_TIMEOUT,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    use_timeout: bool = True,
    skip_cached: bool = True,
    persist_health: bool = True,
    file_lookup_batch: int = DEFAULT_FILE_LOOKUP_BATCH,
    file_lookup_max_chars: int = DEFAULT_FILE_LOOKUP_MAX_CHARS,
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

    ignore_hosts
        Data nodes to never read from, unioned with the ones `NodeHealth` has
        learned are unreliable.  Use this to skip known-dead nodes on a cold run,
        before any health has been recorded.

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

    file_lookup_batch
        Upper bound on how many datasets to OR into a single file-search request.
        The per-simulation file lookups are batched into a few requests rather than
        one each (see the module note), cutting round trips.

    file_lookup_max_chars
        Character budget for a batched query's `dataset_id` list — the real limit,
        since ids are long and vary; a batch is capped by whichever of this or
        `file_lookup_batch` it hits first.

    Returns
    -------
    :
        A summary of what was read, reused, stored, skipped and failed.
    """
    health = repository.load_node_health() if health is None else health
    ignore = health.unreliable_hosts() | ignore_hosts

    per_read = with_timeout(reader, seconds=timeout) if use_timeout else reader
    per_read = recording(per_read, health)
    per_read = with_retry(per_read, max_attempts=max_attempts)

    plan = _plan_reads(records, repository, skip_cached=skip_cached)

    candidates = _lookup_candidates(
        plan,
        client=client,
        preferred_hosts=preferred_hosts,
        ignore_hosts=ignore,
        host_rank=health.host_rank,
        batch=file_lookup_batch,
        max_chars=file_lookup_max_chars,
    )

    def read_one(sim: SimulationKey) -> HeaderMetadata | None:
        return read_first_readable(candidates[sim], per_read)

    results = list(read_map(read_one, plan.to_read)) if plan.to_read else []

    headers: dict[HeaderKey, HeaderMetadata] = dict(plan.reuse)
    read_count = 0
    failed: list[SimulationKey] = []
    for sim, metadata in zip(plan.to_read, results, strict=True):
        if metadata is None:
            failed.append(sim)
            continue
        read_count += 1
        for key in plan.missing_keys[sim]:
            headers[key] = metadata

    stored = repository.store_headers(headers) if headers else 0
    if persist_health:
        repository.save_node_health(health)

    return EnrichResult(
        read=read_count,
        reused=len(plan.reuse),
        stored=stored,
        skipped_cached=plan.skipped,
        failed=failed,
    )
