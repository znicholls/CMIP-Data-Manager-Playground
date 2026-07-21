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

A simulation whose versions already carry header-only metadata is skipped; when only
some versions have it, the existing header is copied onto the rest without a read.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from urllib.parse import urlparse

from cmip_data_manager.db.repository import HeaderAttempt, Repository
from cmip_data_manager.db.schema import File
from cmip_data_manager.esgf.dispatch import (
    DEFAULT_MAX_WORKERS,
    DEFAULT_NODE_CONCURRENCY,
    dispatch_reads,
)
from cmip_data_manager.esgf.headers import (
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_READ_TIMEOUT,
    HeaderMetadata,
    SimulationKey,
    https_twin,
    promote_blocks,
    read_header,
    simulation_key,
    with_retry,
    with_timeout,
)
from cmip_data_manager.esgf.health import AttemptLog, NodeHealth, recording
from cmip_data_manager.esgf.models import DatasetRecord, FileRecord
from cmip_data_manager.esgf.routing import (
    AffinityKey,
    SimulationCandidates,
    build_candidates,
    source_id_affinity,
)

HeaderReader = Callable[[str], HeaderMetadata]
"""Reads one file's header from a URL (e.g. `read_header`)."""


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
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    use_timeout: bool = True,
    skip_cached: bool = True,
    avoid_unreliable_hosts: bool = True,
    record_attempts: bool = True,
    persist_health: bool = True,
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
        Read-dispatch controls (see `esgf.dispatch`).

    skip_cached
        Skip simulations whose versions already carry header-only metadata, and
        reuse a sibling version's header for the rest instead of re-reading.

    avoid_unreliable_hosts
        Also avoid the hosts `NodeHealth` has learned are unreliable.

    record_attempts, persist_health
        Persist the per-attempt log and the (updated) node health.

    Returns
    -------
    :
        A summary of what was read, reused, promoted, skipped and failed.
    """
    health = repository.load_node_health() if health is None else health
    ignore = ignore_hosts
    if avoid_unreliable_hosts:
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
    candidates = build_candidates(
        {sim: by_sim[sim] for sim in to_read},
        files_by_sim,
        preferred_hosts=preferred_hosts,
        ignore_hosts=ignore,
        host_rank=health.host_rank,
        affinity_key=affinity_key,
    )
    no_files = [sim for sim in to_read if not candidates[sim].hosts]

    attempt_log = AttemptLog() if record_attempts else None
    per_read = with_timeout(reader, seconds=timeout) if use_timeout else reader
    per_read = promote_blocks(per_read)
    per_read = recording(per_read, health, attempts=attempt_log)
    per_read = with_retry(per_read, max_attempts=max_attempts)

    dispatched = dispatch_reads(
        candidates,
        per_read,
        initial_concurrency=lambda _host: node_concurrency,
        max_workers=max_workers,
    )

    read = 0
    for sim, metadata in dispatched.headers.items():
        file_id = url_to_file.get(metadata.source_url or "")
        if file_id is None:  # a URL not from the stored files — should not happen
            continue
        promoted += repository.promote_header(
            file_id=file_id,
            source_url=metadata.source_url,
            attrs=metadata.attrs,
            version_keys=versions_by_sim[sim],
        )
        read += 1

    for host, (max_safe, last) in dispatched.learned.items():
        health.record_concurrency(host, max_safe=max_safe, last=last)
    if attempt_log is not None:
        repository.record_header_attempts(_attempt_rows(attempt_log, candidates))
    if persist_health:
        repository.save_node_health(health)

    return VersionEnrichResult(
        read=read,
        reused=reused,
        promoted=promoted,
        skipped_cached=skipped,
        failed=dispatched.failed,
        no_files=no_files,
    )


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
        elif done:
            missing = [vk for vk in version_keys if vk not in done]
            promoted += _reuse_sibling(repository, done, missing)
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


def _attempt_rows(
    attempt_log: AttemptLog,
    candidates: dict[SimulationKey, SimulationCandidates],
) -> list[HeaderAttempt]:
    """Turn logged reads into per-attempt rows, joining each URL back to its sim."""
    url_to_sim: dict[str, SimulationKey] = {}
    for sim, candidate in candidates.items():
        for urls in candidate.urls_by_host.values():
            for url in urls:
                url_to_sim[url] = sim
    rows: list[HeaderAttempt] = []
    seen: dict[str, int] = {}
    for record in attempt_log.records():
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
