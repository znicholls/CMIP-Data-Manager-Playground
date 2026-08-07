"""
Step 3 — read one header per simulation, store it on the file, promote to the version

This is the header step in the node-independent model.  A netCDF header is read once
per *simulation* `(source_id, experiment_id, variant_label)` — the header describes the
run, so one read serves every variable — and then:

- the full header is stored on the **`File`** it was read from
  (`File.header_attrs_json`);
- the dataset-applicable `parent_*` subset is **promoted onto every `DatasetVersion`**
  of the simulation (with `header_from_file_key` recording which file it came from,
  possibly a sibling variable's file — the reuse case).

Candidate URLs come from the **persisted `FileAccess`** rows written in Step 2 (no
re-search), and the read itself reuses the existing health-aware, timeout/retry-bounded
dispatcher (`esgf.dispatch`), so per-node concurrency, ranking, node-health learning and
the append-only attempt log all still apply and **persist**.

Header-only metadata is **independent of variable**: one read per simulation serves
every variable, so the header is filed at the simulation grain and copied rather than
re-read wherever it is already known.  A simulation whose versions already carry the
metadata is skipped; when only some versions of *this run* have it, it is copied onto
the rest; and when a **different variable/version of the same simulation** already has
it — including one read in an **earlier run** — that stored header is copied onto the
newly-searched versions, again without a read (see `_reuse_simulation`).

Persistence is **save-as-you-go** by default (`persist_as_you_go`): each header is
written the instant it is read, with the attempt log and node health flushed
periodically, so interrupting a long run (e.g. one stuck on dead nodes) keeps every
header already fetched rather than discarding the whole batch.
"""

from __future__ import annotations

import time
from collections import defaultdict
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from urllib.parse import urlparse

from cmip_data_manager.db.repository import HeaderAttempt, Repository
from cmip_data_manager.db.schema import File
from cmip_data_manager.esgf.concurrency import thread_pool_map
from cmip_data_manager.esgf.dispatch import (
    DEFAULT_MAX_WORKERS,
    DEFAULT_NODE_CONCURRENCY,
    dispatch_reads,
)
from cmip_data_manager.esgf.headers import (
    DEFAULT_CONNECT_TIMEOUT,
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_READ_TIMEOUT,
    HeaderMetadata,
    SimulationKey,
    https_twin,
    promote_blocks,
    promote_host_faults,
    read_header,
    simulation_key,
    with_connect_probe,
    with_retry,
    with_timeout,
)
from cmip_data_manager.esgf.health import (
    AttemptLog,
    AttemptRecord,
    NodeHealth,
    recording,
)
from cmip_data_manager.esgf.models import DatasetRecord, FileRecord
from cmip_data_manager.esgf.preflight import (
    DEFAULT_PROBE_READ_TIMEOUT,
    ProbeCache,
    probe_nodes,
    sample_urls_by_host,
)
from cmip_data_manager.esgf.routing import (
    AffinityKey,
    SimulationCandidates,
    build_candidates,
    source_id_affinity,
)

HeaderReader = Callable[[str], HeaderMetadata]
"""Reads one file's header from a URL (e.g. `read_header`)."""

_HEALTH_SAVE_INTERVAL = 2.0
"""Minimum seconds between save-as-you-go node-health flushes (headers persist every
read; the full-snapshot health upsert is throttled so it is not rewritten per read)."""


@dataclass(frozen=True)
class VersionEnrichResult:
    """Summary of a Step-3 header pass."""

    read: int = 0
    """Simulations whose header was read from a data node."""

    reused: int = 0
    """Simulations filled from a sibling version's already-stored header (no read)."""

    promoted: int = 0
    """Dataset versions that received promoted header-only metadata."""

    skipped_cached: int = 0
    """Simulations skipped because every version already had header metadata."""

    failed: list[SimulationKey] = field(default_factory=list)
    """Simulations whose every candidate mirror failed."""

    no_files: list[SimulationKey] = field(default_factory=list)
    """Simulations to read that had no stored file mirror (run Step 2 first)."""


@dataclass
class _HeaderWriter:
    """
    Save-as-you-go persistence for the header step

    Holds the small amount of state needed to persist each header **the instant it is
    read** — the file/version lookups, the shared node-health and attempt-log objects,
    and a cursor/counter so incremental attempt flushes neither duplicate nor
    re-number rows.  Its `promote`/`flush` methods are the callbacks handed to
    `dispatch_reads`; both run in that dispatcher's single controller thread, so the
    DB writes are serialised without any locking of our own.
    """

    repository: Repository
    url_to_file: dict[str, int]
    versions_by_sim: dict[SimulationKey, list[str]]
    health: NodeHealth
    attempt_log: AttemptLog | None
    url_to_sim: dict[str, SimulationKey]
    read: int = 0
    promoted: int = 0
    _attempt_cursor: int = 0
    _seen_attempts: dict[str, int] = field(default_factory=dict)
    _last_health_save: float = 0.0

    def promote(self, sim: SimulationKey, metadata: HeaderMetadata) -> None:
        """Store one just-read header on its file and promote it onto its versions."""
        file_id = self.url_to_file.get(metadata.source_url or "")
        if file_id is None:  # a URL not from the stored files — should not happen
            return
        self.promoted += self.repository.promote_header(
            file_id=file_id,
            source_url=metadata.source_url,
            attrs=metadata.attrs,
            version_keys=self.versions_by_sim[sim],
        )
        self.read += 1

    def _flush_attempts(self) -> None:
        """Append attempt rows logged since the last flush (append-only, no dupes)."""
        if self.attempt_log is None:
            return
        records = self.attempt_log.records()
        if len(records) <= self._attempt_cursor:
            return
        rows = _attempt_rows_from(
            records[self._attempt_cursor :], self.url_to_sim, self._seen_attempts
        )
        self._attempt_cursor = len(records)
        if rows:
            self.repository.record_header_attempts(rows)

    def flush(self) -> None:
        """Per-iteration callback: new attempts, then a throttled node-health save."""
        self._flush_attempts()
        now = time.monotonic()
        if now - self._last_health_save >= _HEALTH_SAVE_INTERVAL:
            self.repository.save_node_health(self.health)
            self._last_health_save = now

    def finalise(
        self,
        *,
        persist_attempts: bool,
        candidates: dict[SimulationKey, SimulationCandidates],
    ) -> None:
        """Write the attempt log's tail: the whole log (batch path) or the remainder."""
        if self.attempt_log is None:
            return
        if persist_attempts:  # legacy batch path: nothing flushed yet, write it all
            self.repository.record_header_attempts(
                _attempt_rows(self.attempt_log, candidates)
            )
        else:  # save-as-you-go: flush whatever arrived since the last on-the-go flush
            self._flush_attempts()


def enrich_version_headers(  # noqa: PLR0913 - a DI seam; every parameter has a default
    records: list[DatasetRecord],
    *,
    repository: Repository,
    health: NodeHealth | None = None,
    reader: HeaderReader = read_header,
    preferred_hosts: tuple[str, ...] = (),
    ignore_hosts: frozenset[str] = frozenset(),
    affinity_key: AffinityKey = source_id_affinity,
    max_workers: int = DEFAULT_MAX_WORKERS,
    node_concurrency: int = DEFAULT_NODE_CONCURRENCY,
    timeout: float = DEFAULT_READ_TIMEOUT,
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    use_timeout: bool = True,
    skip_cached: bool = True,
    avoid_unreliable_hosts: bool = True,
    record_attempts: bool = True,
    persist_health: bool = True,
    persist_as_you_go: bool = True,
    preflight_probe: bool = False,
    probe_cache: ProbeCache | None = None,
    force_alive_hosts: frozenset[str] = frozenset(),
    probe_read_timeout: float = DEFAULT_PROBE_READ_TIMEOUT,
) -> VersionEnrichResult:
    """
    Read one header per simulation from stored files and promote it onto its versions

    Parameters
    ----------
    records
        The dataset versions to enrich (grouped internally by simulation).

    repository
        Cache to read stored files/headers from and write promoted metadata into.

    health
        Node-health registry; defaults to the one persisted in `repository`.

    reader
        Per-URL header read; defaults to `read_header` (netCDF over byte-range).

    preferred_hosts, ignore_hosts, affinity_key
        Routing controls forwarded to `build_candidates`.

    max_workers, node_concurrency, timeout, max_attempts, use_timeout
        Read-dispatch controls (see `esgf.dispatch`).  `timeout` bounds the read once
        connected; a node connecting more slowly than this is not cut off.

    connect_timeout
        Deadline for the connect probe to *reach* a node (only when `use_timeout`);
        far longer than `timeout`, since a distant-but-healthy node can be slow to
        connect but should read its header quickly once it does.

    skip_cached
        Skip simulations whose versions already carry header-only metadata, and reuse
        an already-stored header for the rest instead of re-reading — whether from a
        sibling version in this run or from a different variable/version of the same
        simulation stored by an earlier run (header-only metadata is variable-
        independent, so any one read of the simulation serves them all).

    avoid_unreliable_hosts
        Also avoid the hosts `NodeHealth` has learned are unreliable.

    record_attempts, persist_health
        Persist the per-attempt log and the (updated) node health.

    persist_as_you_go
        Persist each header **the moment it is read** (and periodically flush the
        attempt log and node health) rather than only after the whole batch.  This is
        the crash/kill-safe default: if the run is interrupted partway (e.g. a long
        run stuck on dead nodes), every header already read is on disk.  Pass `False`
        for the legacy behaviour that writes everything once, after the batch returns.

    preflight_probe
        Before reading, sweep the candidate data nodes with a cheap chunk probe
        (`esgf.preflight`) and drop the ones that fail it from `ignore_hosts` for this
        run.  This front-loads dead-node discovery — a cold run gets its `ignore_hosts`
        populated *before* the first real read instead of learning dead nodes the slow,
        costly way — and because the probe tests **today's** reality it **overrides**
        the stale persisted `unreliable_hosts()` exclusion: when it runs, that
        history-based exclusion is skipped (persisted health still *orders* the
        survivors via `host_rank`), so a node dead in history but alive now is reused.

    probe_cache
        Session verdict store for the probe (see `preflight.ProbeCache`).  Pass a shared
        one across calls (e.g. every hop of a parent walk) so each node is probed at
        most once per session; a fresh one is used when omitted.

    force_alive_hosts
        Hosts the probe must keep in play regardless of its verdict — the user override
        for a node wrongly judged dead.

    probe_read_timeout
        The probe's post-connect deadline (a single chunk is one round-trip, so far
        tighter than the header-read `timeout`); only used when `preflight_probe`.

    Returns
    -------
    :
        A summary of what was read, reused, promoted, skipped and failed.
    """
    health = repository.load_node_health() if health is None else health
    ignore = ignore_hosts
    # The pre-flight probe decides alive/dead from a live chunk read, so when it runs
    # the stale persisted "unreliable" exclusion is dropped (health still *orders*
    # survivors via host_rank below) — a node dead in history but alive today is reused.
    if avoid_unreliable_hosts and not preflight_probe:
        ignore = health.unreliable_hosts() | ignore_hosts

    by_sim: dict[SimulationKey, list[DatasetRecord]] = defaultdict(list)
    for record in records:
        sim = simulation_key(record)
        if sim is not None:
            by_sim[sim].append(record)
    versions_by_sim = {
        sim: sorted({r.instance_key for r in recs}) for sim, recs in by_sim.items()
    }

    to_read, reused, promoted, skipped = _plan_headers(
        repository, versions_by_sim, skip_cached=skip_cached
    )

    files_by_sim, url_to_file = _stored_candidates(repository, to_read, versions_by_sim)

    def _candidates(
        ignore_set: frozenset[str],
    ) -> dict[SimulationKey, SimulationCandidates]:
        return build_candidates(
            {sim: by_sim[sim] for sim in to_read},
            files_by_sim,
            preferred_hosts=preferred_hosts,
            ignore_hosts=ignore_set,
            host_rank=health.host_rank,
            affinity_key=affinity_key,
        )

    candidates = _candidates(ignore)
    if preflight_probe:
        ignore, candidates = _apply_preflight(
            candidates,
            ignore,
            build=_candidates,
            cache=probe_cache if probe_cache is not None else ProbeCache(),
            force_alive_hosts=force_alive_hosts,
            max_workers=max_workers,
            connect_timeout=connect_timeout,
            read_timeout=probe_read_timeout,
        )
    no_files = [sim for sim in to_read if not candidates[sim].hosts]

    attempt_log = AttemptLog() if record_attempts else None
    per_read = with_timeout(reader, seconds=timeout) if use_timeout else reader
    if use_timeout:
        # Gate the read behind a connect probe: a long window to *reach* the node
        # (a slow-but-alive replica is not dead) but only `timeout` seconds once
        # connected, so a node that connects then stalls is cut off as a host fault.
        per_read = with_connect_probe(
            per_read, connect_timeout=connect_timeout, read_timeout=timeout
        )
    per_read = promote_blocks(per_read)
    per_read = promote_host_faults(per_read)
    per_read = recording(per_read, health, attempts=attempt_log)
    per_read = with_retry(per_read, max_attempts=max_attempts)

    writer = _HeaderWriter(
        repository=repository,
        url_to_file=url_to_file,
        versions_by_sim=versions_by_sim,
        health=health,
        attempt_log=attempt_log,
        url_to_sim=_url_to_sim(candidates),
        promoted=promoted,  # carry the reuse-path promotions forward
    )

    dispatched = dispatch_reads(
        candidates,
        per_read,
        initial_concurrency=lambda _host: node_concurrency,
        max_workers=max_workers,
        on_success=writer.promote if persist_as_you_go else None,
        on_flush=writer.flush if persist_as_you_go else None,
    )

    if not persist_as_you_go:
        # Legacy batch path: nothing was written during the run, so persist it all now.
        for sim, metadata in dispatched.results.items():
            writer.promote(sim, metadata)

    for host, (max_safe, last) in dispatched.learned.items():
        health.record_concurrency(host, max_safe=max_safe, last=last)
    writer.finalise(persist_attempts=not persist_as_you_go, candidates=candidates)
    if persist_health:
        repository.save_node_health(health)

    return VersionEnrichResult(
        read=writer.read,
        reused=reused,
        promoted=writer.promoted,
        skipped_cached=skipped,
        failed=dispatched.failed,
        no_files=no_files,
    )


def _apply_preflight(  # noqa: PLR0913 - probe knobs, all keyword-only from one caller
    candidates: dict[SimulationKey, SimulationCandidates],
    ignore: frozenset[str],
    *,
    build: Callable[[frozenset[str]], dict[SimulationKey, SimulationCandidates]],
    cache: ProbeCache,
    force_alive_hosts: frozenset[str],
    max_workers: int,
    connect_timeout: float,
    read_timeout: float,
) -> tuple[frozenset[str], dict[SimulationKey, SimulationCandidates]]:
    """
    Probe the candidate data nodes, add the dead to `ignore`, and rebuild candidates

    Runs the pre-flight sweep over one representative URL per candidate host (its
    `https` twin and `http` original), unions the hosts that fail into `ignore`, and
    rebuilds the routing candidates with them excluded — so no read is ever dispatched
    to a node the probe just found dead.  A shared `cache` means each node is probed at
    most once per session, so this is cheap to call again on a later hop with new hosts.

    Parameters
    ----------
    candidates
        The candidates built with the current `ignore` (probed for their hosts).

    ignore
        The hosts already excluded (user `ignore_hosts`); dead nodes are unioned in.

    build
        Rebuilds candidates for a given `ignore` set (the caller's `_candidates`).

    cache, force_alive_hosts, max_workers, connect_timeout, read_timeout
        Forwarded to `preflight.probe_nodes` (the sweep runs parallel across
        `max_workers`).

    Returns
    -------
    :
        The (possibly enlarged) `ignore` set and the candidates rebuilt against it —
        unchanged when every probed node was alive.
    """
    outcomes = probe_nodes(
        sample_urls_by_host(candidates),
        force_alive_hosts=force_alive_hosts,
        cache=cache,
        map_fn=thread_pool_map(max_workers=max_workers),
        connect_timeout=connect_timeout,
        read_timeout=read_timeout,
    )
    dead = frozenset(host for host, outcome in outcomes.items() if not outcome.alive)
    if not dead:
        return ignore, candidates
    enlarged = ignore | dead
    return enlarged, build(enlarged)


def _plan_headers(
    repository: Repository,
    versions_by_sim: dict[SimulationKey, list[str]],
    *,
    skip_cached: bool,
) -> tuple[list[SimulationKey], int, int, int]:
    """Decide, per sim, whether to read, reuse a sibling, or skip (all cached)."""
    to_read: list[SimulationKey] = []
    reused = 0
    promoted = 0
    skipped = 0
    for sim, version_keys in versions_by_sim.items():
        if not skip_cached:
            to_read.append(sim)
            continue
        done = [vk for vk in version_keys if repository.version_has_header(vk)]
        if len(done) == len(version_keys):
            skipped += 1
            continue
        missing = [vk for vk in version_keys if vk not in done]
        if done:
            # Some of this run's versions already carry it — copy onto the rest.
            promoted += _reuse_sibling(repository, done, missing)
            reused += 1
        elif (copied := _reuse_simulation(repository, sim, missing)) is not None:
            # A different variable/version of this simulation (possibly read in an
            # earlier run) already has the header — copy it, no read needed.
            promoted += copied
            reused += 1
        else:
            to_read.append(sim)
    return to_read, reused, promoted, skipped


def _reuse_sibling(repository: Repository, done: list[str], missing: list[str]) -> int:
    """Copy an already-read sibling version's header onto the missing versions."""
    header = repository.version_header(done[0])
    file_id = repository.version_header_file_id(done[0])
    if header is None or file_id is None:
        return 0
    return repository.promote_header(
        file_id=file_id,
        source_url=header.source_url,
        attrs=header.attrs,
        version_keys=missing,
    )


def _reuse_simulation(
    repository: Repository, sim: SimulationKey, missing: list[str]
) -> int | None:
    """Copy a header already stored for this simulation (any variable) onto `missing`.

    Header-only metadata is **independent of variable**, so a header read for *any*
    variable/version of the simulation `(source_id, experiment_id, variant_label)` —
    including one read in an **earlier run**, before these versions were even searched —
    serves the versions searched now.  This is the cross-run reuse that lets a later
    search for a new variable (e.g. `rsut` after `tas`) copy the parent metadata instead
    of re-reading a data node.  Returns the number of versions promoted, or `None` if no
    version of the simulation has a stored header yet.
    """
    found = repository.simulation_header(*sim)
    if found is None:
        return None
    header, file_id = found
    return repository.promote_header(
        file_id=file_id,
        source_url=header.source_url,
        attrs=header.attrs,
        version_keys=missing,
    )


def _stored_candidates(
    repository: Repository,
    to_read: list[SimulationKey],
    versions_by_sim: dict[SimulationKey, list[str]],
) -> tuple[dict[SimulationKey, list[FileRecord]], dict[str, int]]:
    """Rebuild each sim's files from stored `File`/`FileAccess`, mapping URL -> file."""
    files_by_sim: dict[SimulationKey, list[FileRecord]] = {}
    url_to_file: dict[str, int] = {}
    for sim in to_read:
        records: list[FileRecord] = []
        for version_key in versions_by_sim[sim]:
            for file in repository.get_version_files(version_key):
                if file.id is None:
                    continue
                records.append(_reconstruct_file(file))
                for access in file.accesses:
                    if access.url:
                        url_to_file[access.url] = file.id
                        twin = https_twin(access.url)
                        if twin is not None:
                            url_to_file[twin] = file.id
        files_by_sim[sim] = records
    return files_by_sim, url_to_file


def _reconstruct_file(file: File) -> FileRecord:
    """Rebuild a `FileRecord` from a stored `File` and its accesses (for ranking)."""
    urls = tuple(
        f"{access.url}|application/netcdf|{access.service}"
        for access in file.accesses
        if access.url and access.service
    )
    return FileRecord(
        id=str(file.id),
        dataset_id="",
        title=file.filename,
        size=file.size,
        checksum=file.checksum,
        checksum_type=file.checksum_type,
        tracking_id=file.tracking_id,
        urls=urls,
        raw={},
    )


def _url_to_sim(
    candidates: dict[SimulationKey, SimulationCandidates],
) -> dict[str, SimulationKey]:
    """Map every candidate URL back to the simulation it belongs to."""
    url_to_sim: dict[str, SimulationKey] = {}
    for sim, candidate in candidates.items():
        for urls in candidate.urls_by_host.values():
            for url in urls:
                url_to_sim[url] = sim
    return url_to_sim


def _attempt_rows_from(
    records: Sequence[AttemptRecord],
    url_to_sim: dict[str, SimulationKey],
    seen: dict[str, int],
) -> list[HeaderAttempt]:
    """
    Turn logged reads into per-attempt rows, joining each URL back to its sim

    `seen` carries the per-URL attempt counter **across calls**, so incremental
    save-as-you-go flushes number a URL's retries continuously (1, 2, 3, …) instead
    of restarting the count each flush.
    """
    rows: list[HeaderAttempt] = []
    for record in records:
        owner = url_to_sim.get(record.url)
        if owner is None:
            continue
        seen[record.url] = seen.get(record.url, 0) + 1
        source_id, experiment_id, variant_label = owner
        rows.append(
            HeaderAttempt(
                source_id=source_id,
                experiment_id=experiment_id,
                variant_label=variant_label,
                outcome=record.outcome.value,
                host=urlparse(record.url).hostname,
                url=record.url,
                seconds=record.seconds,
                attempt_no=seen[record.url],
                detail=record.message,
            )
        )
    return rows


def _attempt_rows(
    attempt_log: AttemptLog,
    candidates: dict[SimulationKey, SimulationCandidates],
) -> list[HeaderAttempt]:
    """Turn a whole attempt log into per-attempt rows (the batch-path convenience)."""
    return _attempt_rows_from(attempt_log.records(), _url_to_sim(candidates), {})
