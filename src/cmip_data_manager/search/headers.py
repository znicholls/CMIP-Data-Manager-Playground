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
4. reads one header per remaining simulation, routed across the data nodes by
   `dispatch_reads` (`esgf.dispatch`): each simulation goes to its best candidate
   node (preferred → learned health → HTTPS) that is under a per-node connection
   cap, spilling to an alternative rather than idling a capable node and requeuing
   past any node that fails it.  Each read is bounded by `with_timeout`, classified
   by `promote_blocks`, retried on transient failure by `with_retry`, and recorded
   into `NodeHealth`;
5. stores the headers (one row per dataset) and saves node health back.

Because `with_timeout` isolates each read in a spawned child process, the reads are
fanned out on a **thread** pool, never a process pool, and a caller using the
default (spawning) reader must run from a `__main__`-guarded script.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from urllib.parse import urlparse

from cmip_data_manager.db.repository import HeaderAttempt, Repository
from cmip_data_manager.esgf.client import DeepPaginationError, ESGFSearchClient
from cmip_data_manager.esgf.dispatch import (
    DEFAULT_CONCURRENCY_CEILING,
    DEFAULT_EVICT_AFTER_ATTEMPTS,
    DEFAULT_EVICT_MAX_SUCCESS_RATE,
    DEFAULT_MAX_WORKERS,
    DEFAULT_NODE_CONCURRENCY,
    dispatch_reads,
)
from cmip_data_manager.esgf.headers import (
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_READ_TIMEOUT,
    HeaderKey,
    HeaderMetadata,
    SimulationKey,
    header_key,
    http_download_urls,
    https_twin,
    promote_blocks,
    read_header,
    simulation_key,
    with_retry,
    with_timeout,
)
from cmip_data_manager.esgf.health import AttemptLog, NodeHealth, recording
from cmip_data_manager.esgf.models import DatasetRecord, FileRecord
from cmip_data_manager.esgf.query import FacetQuery
from cmip_data_manager.esgf.routing import (
    AffinityKey,
    SimulationCandidates,
    build_candidates,
    source_id_affinity,
)

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
    records: dict[SimulationKey, list[DatasetRecord]] = field(default_factory=dict)
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
        plan.records[sim] = sim_records
        plan.to_read.append(sim)
    return plan


def _search_files_for_ids(
    client: ESGFSearchClient, ids: Sequence[str]
) -> list[FileRecord]:
    """
    Search the files of a chunk of dataset ids, bisecting a too-deep result set

    ESGF ORs a comma-separated `dataset_id` list, but a chunk whose files exceed the
    index's single-query retrieval cap (10000) raises `DeepPaginationError` — file-
    heavy simulations such as long, replicated `piControl` runs hit this.  The chunk
    is then split in half and each half retried, down to a single dataset; a lone
    dataset that still overflows is retried for its *primary* (non-replica) files
    only, and skipped if even that is too deep.
    """
    query = FacetQuery(type="File", dataset_id=tuple(ids))
    try:
        return client.search_files(query)
    except DeepPaginationError:
        if len(ids) > 1:
            mid = len(ids) // 2
            return _search_files_for_ids(client, ids[:mid]) + _search_files_for_ids(
                client, ids[mid:]
            )
        try:  # a single dataset with too many replicas: primary files are enough
            return client.search_files(
                FacetQuery(type="File", dataset_id=tuple(ids), replica=False)
            )
        except DeepPaginationError:
            return []


def _lookup_files(
    plan: _Plan,
    *,
    client: ESGFSearchClient,
    batch: int,
    max_chars: int,
) -> dict[SimulationKey, list[FileRecord]]:
    """
    Look up every to-read simulation's files in a few batched queries

    Files are looked up in a few batched queries rather than one request per
    simulation: ESGF ORs a comma-separated `dataset_id` list, so a chunk of ids
    returns all their files in a single round trip, which are then bucketed back to
    their simulation by `dataset_id`.  A chunk whose files exceed the retrieval cap
    is bisected by `_search_files_for_ids`.  Ranking the resulting mirrors is left to
    `build_candidates`.
    """
    sim_by_dataset_id: dict[str, SimulationKey] = {
        dataset_id: sim for sim in plan.to_read for dataset_id in plan.dataset_ids[sim]
    }
    files_by_sim: dict[SimulationKey, list[FileRecord]] = defaultdict(list)
    all_ids = list(sim_by_dataset_id)
    for chunk in _chunk_ids(all_ids, max_count=batch, max_chars=max_chars):
        for file in _search_files_for_ids(client, chunk):
            owner = sim_by_dataset_id.get(file.dataset_id)
            if owner is not None:
                files_by_sim[owner].append(file)
    return files_by_sim


def _seed_concurrency(
    health: NodeHealth,
    *,
    default: int,
    overrides: Mapping[str, int],
    ceiling: int,
) -> Callable[[str], int]:
    """
    Build a host's *starting* per-node cap for this run

    Precedence: an explicit user override wins (and pins the host); otherwise the
    cap this host converged on in a previous run (`last_concurrency`, capped by the
    ceiling); otherwise the conservative default.
    """

    def initial(host: str) -> int:
        if host in overrides:
            return overrides[host]
        stat = health.stat(host)
        if stat is not None and stat.last_concurrency > 0:
            return min(ceiling, stat.last_concurrency)
        return default

    return initial


def _attempt_rows(
    attempt_log: AttemptLog,
    *,
    candidates: Mapping[SimulationKey, SimulationCandidates],
    files_by_sim: Mapping[SimulationKey, Sequence[FileRecord]],
    records_by_sim: Mapping[SimulationKey, Sequence[DatasetRecord]],
    failed: Sequence[SimulationKey],
) -> list[HeaderAttempt]:
    """
    Turn recorded reads into persistable per-attempt rows

    Joins each logged `(url, outcome, seconds)` back to the simulation, host and
    variable it belonged to (via `candidates` and `files_by_sim`), numbers repeated
    reads of the same URL as retries, and adds a `no_candidate` marker row for every
    fully-failed simulation that had no mirror to try — so no failure is silent.
    """
    url_to_sim: dict[str, SimulationKey] = {}
    url_meta: dict[str, tuple[str | None, str | None]] = {}
    for sim, candidate in candidates.items():
        for urls in candidate.urls_by_host.values():
            for url in urls:
                url_to_sim[url] = sim
        table_by_dataset = {r.id: r.table_id for r in records_by_sim.get(sim, ())}
        for file in files_by_sim.get(sim, ()):
            for url in http_download_urls(file):
                meta = (file.variable_id, table_by_dataset.get(file.dataset_id))
                url_meta[url] = meta
                twin = https_twin(url)
                if twin is not None:  # the synthesised https mirror shares the file
                    url_meta[twin] = meta

    rows: list[HeaderAttempt] = []
    seen: dict[str, int] = {}
    for record in attempt_log.records():
        owner = url_to_sim.get(record.url)
        if owner is None:  # a URL not from these candidates — should not happen
            continue
        seen[record.url] = seen.get(record.url, 0) + 1
        variable_id, table_id = url_meta.get(record.url, (None, None))
        source_id, experiment_id, variant_label = owner
        rows.append(
            HeaderAttempt(
                source_id=source_id,
                experiment_id=experiment_id,
                variant_label=variant_label,
                outcome=record.outcome.value,
                host=urlparse(record.url).hostname,
                url=record.url,
                variable_id=variable_id,
                table_id=table_id,
                seconds=record.seconds,
                attempt_no=seen[record.url],
                detail=record.message,
            )
        )

    # Make sure no failure is silent.  A failed simulation that produced no attempt
    # row is either `no_candidate` (the index listed no usable mirror) or `stranded`
    # (it had mirrors, but every one was evicted mid-run before it was dispatched —
    # e.g. its only node got circuit-broken by other simulations' failures); the
    # latter records the mirrors it *would* have tried, so the failure is traceable.
    logged = {(r.source_id, r.experiment_id, r.variant_label) for r in rows}
    for failed_sim in failed:
        if failed_sim in logged:
            continue
        source_id, experiment_id, variant_label = failed_sim
        cand = candidates.get(failed_sim)
        if cand is None or not cand.hosts:
            rows.append(
                HeaderAttempt(
                    source_id=source_id,
                    experiment_id=experiment_id,
                    variant_label=variant_label,
                    outcome="no_candidate",
                    detail="no HTTPServer mirror was indexed for this simulation",
                )
            )
            continue
        for host in cand.hosts:
            host_urls = cand.urls_by_host.get(host, ())
            best_url = host_urls[0] if host_urls else None
            variable_id, table_id = (
                url_meta.get(best_url, (None, None))
                if best_url is not None
                else (None, None)
            )
            rows.append(
                HeaderAttempt(
                    source_id=source_id,
                    experiment_id=experiment_id,
                    variant_label=variant_label,
                    outcome="stranded",
                    host=host,
                    url=best_url,
                    variable_id=variable_id,
                    table_id=table_id,
                    detail="all candidate mirrors were evicted before it was tried",
                )
            )
    return rows


def enrich_headers(  # noqa: PLR0913 - a DI seam; every parameter has a default
    records: Sequence[DatasetRecord],
    *,
    client: ESGFSearchClient,
    repository: Repository,
    health: NodeHealth | None = None,
    reader: HeaderReader = read_header,
    preferred_hosts: Sequence[str] = (),
    ignore_hosts: frozenset[str] = frozenset(),
    affinity_key: AffinityKey = source_id_affinity,
    max_workers: int = DEFAULT_MAX_WORKERS,
    node_concurrency: int = DEFAULT_NODE_CONCURRENCY,
    node_concurrency_overrides: Mapping[str, int] | None = None,
    concurrency_ceiling: int = DEFAULT_CONCURRENCY_CEILING,
    evict_after_attempts: int = DEFAULT_EVICT_AFTER_ATTEMPTS,
    evict_max_success_rate: float = DEFAULT_EVICT_MAX_SUCCESS_RATE,
    timeout: float = DEFAULT_READ_TIMEOUT,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    use_timeout: bool = True,
    skip_cached: bool = True,
    avoid_unreliable_hosts: bool = True,
    record_attempts: bool = True,
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

    affinity_key
        Tags each simulation so a data node's queue clusters shared-tag
        simulations (default `source_id`); see `esgf.routing`.

    max_workers
        Cap on header reads in flight anywhere — the shared local worker budget
        (see `esgf.dispatch`).

    node_concurrency
        Default cap on simultaneous reads to a *single* data node (conservative
        by default; these are shared servers).

    node_concurrency_overrides
        Per-host caps overriding `node_concurrency` (e.g.
        `{"esgf.nci.org.au": 4}` for a node known to tolerate more).  An overridden
        host is *pinned*: the adaptive controller will not grow or shrink its cap.

    concurrency_ceiling
        Hard upper bound the adaptive per-node cap will never grow past.

    evict_after_attempts
        Minimum reads on a node before it can be judged for eviction (a fair
        sample, so a busy node's short failure run does not evict it).

    evict_max_success_rate
        A judged node whose overall success rate is at or below this is evicted for
        the rest of the run (its queue draining to other nodes).

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

    avoid_unreliable_hosts
        When `True` (default), also avoid the hosts `NodeHealth` has learned are
        unreliable (unioned with `ignore_hosts`).  Set `False` to **re-probe** those
        condemned nodes — needed to retry simulations whose only mirrors a past run
        marked dead, and so to notice a node that has since recovered.  The static
        `ignore_hosts` are always honoured either way.

    record_attempts
        Persist every read attempt (host, URL, outcome, duration — including retries
        and fully-failed simulations) to the `HeaderReadAttempt` log for later
        diagnosis and per-node/per-model reporting.

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
    ignore = ignore_hosts
    if avoid_unreliable_hosts:
        ignore = health.unreliable_hosts() | ignore_hosts

    attempt_log = AttemptLog() if record_attempts else None
    per_read = with_timeout(reader, seconds=timeout) if use_timeout else reader
    per_read = promote_blocks(per_read)
    per_read = recording(per_read, health, attempts=attempt_log)
    per_read = with_retry(per_read, max_attempts=max_attempts)

    plan = _plan_reads(records, repository, skip_cached=skip_cached)

    files_by_sim = _lookup_files(
        plan,
        client=client,
        batch=file_lookup_batch,
        max_chars=file_lookup_max_chars,
    )
    candidates = build_candidates(
        plan.records,
        files_by_sim,
        preferred_hosts=preferred_hosts,
        ignore_hosts=ignore,
        host_rank=health.host_rank,
        affinity_key=affinity_key,
    )

    overrides = dict(node_concurrency_overrides or {})
    dispatched = dispatch_reads(
        candidates,
        per_read,
        initial_concurrency=_seed_concurrency(
            health,
            default=node_concurrency,
            overrides=overrides,
            ceiling=concurrency_ceiling,
        ),
        pinned_hosts=frozenset(overrides),
        max_workers=max_workers,
        ceiling=concurrency_ceiling,
        evict_after_attempts=evict_after_attempts,
        evict_max_success_rate=evict_max_success_rate,
    )

    headers: dict[HeaderKey, HeaderMetadata] = dict(plan.reuse)
    for sim, metadata in dispatched.headers.items():
        for key in plan.missing_keys[sim]:
            headers[key] = metadata

    for host, (max_safe, last) in dispatched.learned.items():
        health.record_concurrency(host, max_safe=max_safe, last=last)

    stored = repository.store_headers(headers) if headers else 0
    if attempt_log is not None:
        repository.record_header_attempts(
            _attempt_rows(
                attempt_log,
                candidates=candidates,
                files_by_sim=files_by_sim,
                records_by_sim=plan.records,
                failed=dispatched.failed,
            )
        )
    if persist_health:
        repository.save_node_health(health)

    return EnrichResult(
        read=len(dispatched.headers),
        reused=len(plan.reuse),
        stored=stored,
        skipped_cached=plan.skipped,
        failed=dispatched.failed,
    )
