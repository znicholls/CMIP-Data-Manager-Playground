"""
Step 5 — download the files a search found, across data nodes, to a local DRS tree

The download step in the node-independent model.  Where Step 3 read one *header* per
simulation, this fetches every *file* of every version a search stored, reusing the
same health-aware, adaptive dispatcher (`esgf.dispatch`) — now driven at file grain —
so per-node concurrency, ranking, node-health learning and the append-only attempt log
all still apply and **persist**.

Each file is streamed by the httpx leaf (`esgf.download.download_to_path`): to a
sibling `.part`, resumed via `Range`, checksum-verified where a checksum is available,
then atomically renamed onto its **DRS path** under `download_root` (the `path_for`
seam, era-aware: CMIP6 from the stored facets, CMIP5 from the native id).

Key differences from the header step:

- **File grain.** The work-item key is the `File.id`; routing is
  `build_file_candidates` (no one-file-per-host collapse — every file is fetched).
- **Throughput health, not latency.** Outcomes record MB/s into a *separate*
  `DownloadNodeHealth`; header dead-verdicts are never inherited, and dead nodes are
  found per-run by the pre-flight probe (never a persisted cross-restart ignore list).
- **Skip-if-present.** A file already on disk and checksum-verified (or size-matched
  when it has no checksum) is recorded `skipped`, not re-fetched.
- **Save-as-you-go.** Each file's terminal state (`FileDownload`) is written the
  instant it completes, with the attempt log and node health flushed periodically, so
  an interrupted run keeps every file already fetched.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Hashable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlparse

from cmip_data_manager.db.repository import DownloadAttempt, Repository
from cmip_data_manager.db.schema import File
from cmip_data_manager.esgf.concurrency import thread_pool_map
from cmip_data_manager.esgf.dispatch import dispatch_reads
from cmip_data_manager.esgf.download import (
    DEFAULT_DOWNLOAD_CHUNK_BYTES,
    DEFAULT_DOWNLOAD_READ_TIMEOUT,
    ChecksumMismatch,
    DownloadBlocked,
    DownloadError,
    DownloadHostFault,
    DownloadInfo,
    DownloadTimeout,
    download_to_path,
    resolve_checksum,
    verify_file,
)
from cmip_data_manager.esgf.download_health import DownloadNodeHealth, DownloadOutcome
from cmip_data_manager.esgf.headers import (
    DEFAULT_CONNECT_TIMEOUT,
    DEFAULT_MAX_ATTEMPTS,
    https_twin,
    with_retry,
)
from cmip_data_manager.esgf.models import DatasetRecord, FileRecord
from cmip_data_manager.esgf.preflight import (
    DEFAULT_PROBE_READ_TIMEOUT,
    ProbeCache,
    probe_nodes,
)
from cmip_data_manager.esgf.routing import (
    AffinityKey,
    FileCandidates,
    build_file_candidates,
    source_id_affinity,
)

_MB = 1_000_000.0
"""Bytes per SI megabyte, for the MB/s throughput figure."""

_HEALTH_SAVE_INTERVAL = 2.0
"""Minimum seconds between save-as-you-go download-health flushes."""

# Conservative download-tuning defaults (see design/download-step-plan.md, D9): on a
# bandwidth-bound local machine the *global* worker budget is the real throttle, so
# these start far lower than the header defaults and are meant to be raised on a
# fat-pipe host.
DEFAULT_DOWNLOAD_MAX_WORKERS = 4
"""Default cap on file downloads in flight anywhere (the shared local budget)."""

DEFAULT_DOWNLOAD_NODE_CONCURRENCY = 2
"""Default starting cap on simultaneous downloads to a single host."""

DEFAULT_DOWNLOAD_CEILING = 3
"""Hard upper bound the adaptive per-host cap will not grow a download node past."""

FileKey = int
"""The download work-item key: a `File.id`."""


class FileDownloader(Protocol):
    """The leaf download seam (default `esgf.download.download_to_path`)."""

    def __call__(  # noqa: PLR0913 - matches `download_to_path`; all knobs keyword-only
        self,
        url: str,
        dest: Path,
        *,
        expected_checksum: str | None,
        checksum_type: str | None,
        chunk_bytes: int,
        connect_timeout: float,
        read_timeout: float,
    ) -> DownloadInfo:
        """Download `url` to `dest`, verifying against the checksum; see the default."""
        ...


PathBuilder = Callable[[Path, DatasetRecord, str], Path]
"""Build a file's on-disk path: `(download_root, version_record, filename) -> Path`."""


@dataclass(frozen=True)
class DownloadResult:
    """Summary of a Step-5 download pass."""

    downloaded: int = 0
    """Files fetched from a data node this run (`verified` + `unverified`)."""

    verified: int = 0
    """Of the downloaded files, those whose bytes were checksum-verified."""

    unverified: int = 0
    """Of the downloaded files, those with no usable checksum to verify against."""

    skipped: int = 0
    """Files already present on disk (verified, or size-matched) and not re-fetched."""

    failed: list[FileKey] = field(default_factory=list)
    """Files whose every candidate mirror failed."""

    no_http_access: list[FileKey] = field(default_factory=list)
    """Files with no `HTTPServer` mirror to download from at all."""

    no_files: list[str] = field(default_factory=list)
    """Selected version keys that had **no stored files** to download — Step 2 (the
    file search) was never run for them, so there is nothing to fetch yet."""

    bytes_downloaded: int = 0
    """Total bytes transferred this run (a resume counts only its new bytes)."""

    mean_mbps: float | None = None
    """Mean throughput of the run's successful downloads in MB/s (`None` if none)."""


def _scalar(value: Any) -> str | None:
    """Return a raw facet value as a scalar string (ESGF facets arrive as lists)."""
    if isinstance(value, list):
        value = value[0] if value else None
    return str(value) if value is not None else None


def drs_path(root: Path, record: DatasetRecord, filename: str) -> Path:
    """
    Build a file's ESGF-DRS path under `root` (the default `path_for`)

    CMIP6 is laid out from the stored facets
    (`mip_era/activity/institution/source/experiment/variant/table/variable/grid/
    vVERSION/filename`); CMIP5 is rebuilt from its native DRS id
    (`raw["instance_id"]`), whose dotted segments are the directory tree.  A missing
    CMIP6 facet becomes `"unknown"` rather than breaking the path (override the seam
    for a bespoke layout).

    Parameters
    ----------
    root
        The download root directory.

    record
        A version's dataset record (its facets and version drive the path).

    filename
        The file's name (the leaf of the path).

    Returns
    -------
    :
        The absolute path the file should be written to.
    """
    era = record.mip_era or record.project or "unknown"
    if era.upper() == "CMIP5":
        native = _scalar(record.raw.get("instance_id")) or record.instance_key
        parts = [segment for segment in native.split(".") if segment]
        return root.joinpath(*parts, filename)
    activity = (
        _scalar(record.raw.get("activity_drs"))
        or _scalar(record.raw.get("activity_id"))
        or "unknown"
    )
    version = record.version or "unknown"
    version = version if version.lower().startswith("v") else f"v{version}"
    parts = [
        era,
        activity,
        record.institution_id or "unknown",
        record.source_id or "unknown",
        record.experiment_id or "unknown",
        record.variant_label or "unknown",
        record.table_id or "unknown",
        record.variable_id or "unknown",
        record.grid_label or "unknown",
        version,
    ]
    return root.joinpath(*parts, filename)


@dataclass(frozen=True)
class _FileContext:
    """What the download reader needs for one file, keyed by each of its URLs."""

    file_id: FileKey
    dest: Path
    expected_checksum: str | None
    checksum_type: str | None


@dataclass(frozen=True)
class _DownloadAttemptRecord:
    """One download attempt: URL, outcome, duration, bytes, and whether it resumed."""

    url: str
    outcome: DownloadOutcome
    seconds: float
    num_bytes: int = 0
    resumed: bool = False
    message: str | None = None


class DownloadAttemptLog:
    """Thread-safe collector of download attempts (the download `AttemptLog`)."""

    def __init__(self) -> None:
        self._records: list[_DownloadAttemptRecord] = []
        self._lock = threading.Lock()

    def add(  # noqa: PLR0913 - one field per column; all but the first three default
        self,
        url: str,
        outcome: DownloadOutcome,
        seconds: float,
        *,
        num_bytes: int = 0,
        resumed: bool = False,
        message: str | None = None,
    ) -> None:
        """Append one attempt record (thread-safe)."""
        with self._lock:
            self._records.append(
                _DownloadAttemptRecord(
                    url, outcome, seconds, num_bytes, resumed, message
                )
            )

    def records(self) -> list[_DownloadAttemptRecord]:
        """Return the attempts recorded so far, in append order (a copy)."""
        with self._lock:
            return list(self._records)


def record_download(
    reader: Callable[[str], DownloadInfo],
    health: DownloadNodeHealth,
    *,
    attempts: DownloadAttemptLog | None = None,
) -> Callable[[str], DownloadInfo]:
    """
    Wrap a download so every attempt's outcome, duration and bytes are recorded

    The download counterpart of `health.recording`: composed *outside* `with_retry` so
    each retry is recorded as its own attempt.  It maps the download exception taxonomy
    to `DownloadOutcome` and, on success, records the bytes moved so throughput (MB/s)
    accumulates in `health`.

    Parameters
    ----------
    reader
        The `url -> DownloadInfo` download to observe (the context-bound leaf).

    health
        Registry to record aggregate per-host throughput/outcomes into.

    attempts
        Optional per-attempt log; when given, each download appends its raw record.

    Returns
    -------
    :
        A reader with the same contract that records before returning/raising.
    """

    def emit(  # noqa: PLR0913 - one field per column; all but the first three default
        url: str,
        outcome: DownloadOutcome,
        seconds: float,
        *,
        num_bytes: int = 0,
        resumed: bool = False,
        message: str | None = None,
    ) -> None:
        health.record(url, outcome, seconds, num_bytes=num_bytes)
        if attempts is not None:
            attempts.add(
                url,
                outcome,
                seconds,
                num_bytes=num_bytes,
                resumed=resumed,
                message=message,
            )

    def read(url: str) -> DownloadInfo:
        started = time.monotonic()
        try:
            info = reader(url)
        except OSError as exc:
            # Every download failure is an OSError; the typed download exceptions carry
            # the node-level outcome, anything else is a transient error.
            emit(url, _outcome_of(exc), time.monotonic() - started, message=str(exc))
            raise
        emit(
            url,
            DownloadOutcome.SUCCESS,
            time.monotonic() - started,
            num_bytes=info.bytes_downloaded,
            resumed=info.resumed,
        )
        return info

    return read


_OUTCOME_BY_ERROR: tuple[tuple[type[OSError], DownloadOutcome], ...] = (
    (DownloadBlocked, DownloadOutcome.BLOCKED),
    (DownloadTimeout, DownloadOutcome.TIMEOUT),
    (DownloadHostFault, DownloadOutcome.HOST_FAULT),
    (ChecksumMismatch, DownloadOutcome.CHECKSUM_FAILED),
)
"""Maps each typed download exception to its recorded outcome (mutually exclusive)."""


def _outcome_of(exc: OSError) -> DownloadOutcome:
    """Classify a download failure into a `DownloadOutcome` (default `ERROR`)."""
    for error_type, outcome in _OUTCOME_BY_ERROR:
        if isinstance(exc, error_type):
            return outcome
    return DownloadOutcome.ERROR


@dataclass
class _DownloadWriter:
    """Save-as-you-go persistence for the download step (the `_HeaderWriter` twin)."""

    repository: Repository
    url_to_file: dict[str, FileKey]
    url_to_access: dict[str, int]
    health: DownloadNodeHealth
    attempt_log: DownloadAttemptLog | None
    downloaded: int = 0
    verified: int = 0
    unverified: int = 0
    bytes_downloaded: int = 0
    _attempt_cursor: int = 0
    _seen: dict[str, int] = field(default_factory=dict)
    _attempts_by_file: dict[FileKey, int] = field(default_factory=dict)
    _last_health_save: float = 0.0

    def record(self, key: FileKey, info: DownloadInfo) -> None:
        """Write one just-completed file's terminal `FileDownload` state."""
        self._flush_attempts()  # persist + count this file's attempts so far
        status = "complete" if info.verified else "unverified"
        self.repository.mark_download(
            file_id=key,
            status=status,
            local_path=str(info.path),
            size_bytes=info.total_bytes,
            verified=info.verified,
            verified_algo=info.verified_algo,
            download_from_access_key=self.url_to_access.get(info.url),
            attempts=self._attempts_by_file.get(key, 1),
        )
        self.downloaded += 1
        if info.verified:
            self.verified += 1
        else:
            self.unverified += 1
        self.bytes_downloaded += info.bytes_downloaded

    def _flush_attempts(self) -> None:
        """Append attempt rows logged since the last flush (append-only, no dupes)."""
        if self.attempt_log is None:
            return
        records = self.attempt_log.records()
        if len(records) <= self._attempt_cursor:
            return
        rows = _attempt_rows_from(
            records[self._attempt_cursor :],
            self.url_to_file,
            self._seen,
            self._attempts_by_file,
        )
        self._attempt_cursor = len(records)
        if rows:
            self.repository.record_download_attempts(rows)

    def flush(self) -> None:
        """Per-iteration callback: new attempts, then a throttled health save."""
        self._flush_attempts()
        now = time.monotonic()
        if now - self._last_health_save >= _HEALTH_SAVE_INTERVAL:
            self.repository.save_download_health(self.health)
            self._last_health_save = now

    def finalise(self) -> None:
        """Flush the attempt log's tail after the run."""
        self._flush_attempts()

    def mark_failed(self, key: FileKey) -> None:
        """Record a file that failed on every mirror (or had none)."""
        self.repository.mark_download(
            file_id=key,
            status="failed",
            local_path=None,
            attempts=self._attempts_by_file.get(key, 0),
        )


def download_files(  # noqa: PLR0913, PLR0912, PLR0915 - DI seam; a linear plan loop
    records: list[DatasetRecord],
    *,
    repository: Repository,
    download_root: Path | str,
    health: DownloadNodeHealth | None = None,
    downloader: FileDownloader = download_to_path,
    path_for: PathBuilder = drs_path,
    preferred_hosts: tuple[str, ...] = (),
    ignore_hosts: frozenset[str] = frozenset(),
    affinity_key: AffinityKey = source_id_affinity,
    max_workers: int = DEFAULT_DOWNLOAD_MAX_WORKERS,
    node_concurrency: int = DEFAULT_DOWNLOAD_NODE_CONCURRENCY,
    ceiling: int = DEFAULT_DOWNLOAD_CEILING,
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
    read_timeout: float = DEFAULT_DOWNLOAD_READ_TIMEOUT,
    chunk_bytes: int = DEFAULT_DOWNLOAD_CHUNK_BYTES,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    skip_existing: bool = True,
    record_attempts: bool = True,
    persist_health: bool = True,
    persist_as_you_go: bool = True,
    preflight_probe: bool = False,
    probe_cache: ProbeCache | None = None,
    force_alive_hosts: frozenset[str] = frozenset(),
    probe_read_timeout: float = DEFAULT_PROBE_READ_TIMEOUT,
) -> DownloadResult:
    """
    Download every stored file of the given versions across data nodes to a DRS tree

    Parameters
    ----------
    records
        The dataset versions whose files to download (the searched set, already
        filtered by the caller — e.g. to a few models).

    repository
        Cache to read stored files from and write download state/attempts/health into.

    download_root
        Root directory the DRS tree is written under.

    health
        Download-health registry; defaults to the one persisted in `repository`.
        Header health is never inherited — download health is throughput-based and
        learned from downloads alone.

    downloader
        The leaf `url, dest -> DownloadInfo`; defaults to `download_to_path` (httpx).

    path_for
        `(root, record, filename) -> Path` DRS builder; defaults to `drs_path`.

    preferred_hosts, ignore_hosts, affinity_key
        Routing controls forwarded to `build_file_candidates`.

    max_workers, node_concurrency, ceiling
        Concurrency controls (conservative by default: the global `max_workers` is the
        real throttle on a bandwidth-bound local machine).

    connect_timeout, read_timeout, chunk_bytes, max_attempts
        Per-download transport controls.

    skip_existing
        Skip a file already on disk and checksum-verified (or size-matched when it has
        no checksum), recording it `skipped` rather than re-downloading.

    record_attempts, persist_health, persist_as_you_go
        Persist the per-attempt log, the download health, and each file's state the
        moment it completes (crash/kill-safe default).

    preflight_probe, probe_cache, force_alive_hosts, probe_read_timeout
        Front-load dead-node discovery with a live chunk probe whose verdicts are
        **session-scoped** (never a persisted cross-restart ignore list); see
        `esgf.preflight`.

    Returns
    -------
    :
        A summary of what was downloaded, skipped, failed and how fast.
    """
    root = Path(download_root)
    health = repository.load_download_health() if health is None else health
    # NB: unlike the header step there is no persisted-unreliable-hosts exclusion for
    # downloads — dead verdicts are decided per-run by the pre-flight probe only.
    ignore = ignore_hosts

    records_by_version: dict[str, DatasetRecord] = {}
    for record in records:
        records_by_version.setdefault(record.instance_key, record)

    files_by_key: dict[FileKey, FileRecord] = {}
    group_by_key: dict[FileKey, Hashable] = {}
    context_by_url: dict[str, _FileContext] = {}
    url_to_file: dict[str, FileKey] = {}
    url_to_access: dict[str, int] = {}
    skipped = 0
    no_files: list[str] = []
    for version_key, record in records_by_version.items():
        version_files = repository.get_version_files(version_key)
        if not version_files:
            # Selected but never file-searched (no File rows) — nothing to download.
            no_files.append(version_key)
            continue
        group = affinity_key(record)
        for file in version_files:
            if file.id is None:
                continue
            dest = path_for(root, record, file.filename)
            if skip_existing and _record_if_present(repository, file, dest):
                skipped += 1
                continue
            files_by_key[file.id] = _file_record(file)
            group_by_key[file.id] = group
            context = _FileContext(
                file_id=file.id,
                dest=dest,
                expected_checksum=file.checksum,
                checksum_type=file.checksum_type,
            )
            for access in file.accesses:
                if not access.url:
                    continue
                for url in (access.url, https_twin(access.url)):
                    if url is None:
                        continue
                    context_by_url[url] = context
                    url_to_file[url] = file.id
                    if access.id is not None:
                        url_to_access[url] = access.id

    def _rebuild(ignore_set: frozenset[str]) -> dict[FileKey, FileCandidates]:
        return build_file_candidates(
            files_by_key,
            group_by_key=group_by_key,
            preferred_hosts=preferred_hosts,
            ignore_hosts=ignore_set,
            host_rank=health.host_rank,
        )

    candidates = _rebuild(ignore)
    if preflight_probe:
        ignore, candidates = _apply_preflight(
            candidates,
            ignore,
            rebuild=_rebuild,
            cache=probe_cache if probe_cache is not None else ProbeCache(),
            force_alive_hosts=force_alive_hosts,
            max_workers=max_workers,
            connect_timeout=connect_timeout,
            read_timeout=probe_read_timeout,
        )
    no_http = [key for key, candidate in candidates.items() if not candidate.hosts]

    attempt_log = DownloadAttemptLog() if record_attempts else None
    leaf = _make_reader(
        context_by_url,
        downloader=downloader,
        chunk_bytes=chunk_bytes,
        connect_timeout=connect_timeout,
        read_timeout=read_timeout,
    )
    per_read = record_download(leaf, health, attempts=attempt_log)
    per_read = with_retry(
        per_read, max_attempts=max_attempts, give_up_on=(DownloadError,)
    )

    writer = _DownloadWriter(
        repository=repository,
        url_to_file=url_to_file,
        url_to_access=url_to_access,
        health=health,
        attempt_log=attempt_log,
    )

    dispatched = dispatch_reads(
        candidates,
        per_read,
        initial_concurrency=lambda _host: node_concurrency,
        max_workers=max_workers,
        ceiling=ceiling,
        block_errors=(DownloadBlocked,),
        fault_errors=(DownloadTimeout, DownloadHostFault, ChecksumMismatch),
        on_success=writer.record if persist_as_you_go else None,
        on_flush=writer.flush if persist_as_you_go else None,
    )

    if not persist_as_you_go:
        for key, info in dispatched.results.items():
            writer.record(key, info)

    for host, (max_safe, last) in dispatched.learned.items():
        health.record_concurrency(host, max_safe=max_safe, last=last)
    writer.finalise()

    no_http_set = set(no_http)
    for key in dispatched.failed:
        writer.mark_failed(key)
    if persist_health:
        repository.save_download_health(health)

    return DownloadResult(
        downloaded=writer.downloaded,
        verified=writer.verified,
        unverified=writer.unverified,
        skipped=skipped,
        failed=[key for key in dispatched.failed if key not in no_http_set],
        no_http_access=no_http,
        no_files=no_files,
        bytes_downloaded=writer.bytes_downloaded,
        mean_mbps=_mean_mbps(attempt_log),
    )


def _apply_preflight(  # noqa: PLR0913 - probe knobs, all keyword-only from one caller
    candidates: dict[FileKey, FileCandidates],
    ignore: frozenset[str],
    *,
    rebuild: Callable[[frozenset[str]], dict[FileKey, FileCandidates]],
    cache: ProbeCache,
    force_alive_hosts: frozenset[str],
    max_workers: int,
    connect_timeout: float,
    read_timeout: float,
) -> tuple[frozenset[str], dict[FileKey, FileCandidates]]:
    """Probe the candidate data nodes, add the dead to `ignore`, and rebuild."""
    outcomes = probe_nodes(
        _sample_urls_by_host(candidates),
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
    return enlarged, rebuild(enlarged)


def _sample_urls_by_host(
    candidates: dict[FileKey, FileCandidates],
) -> dict[str, tuple[str, ...]]:
    """Pick one representative file's URLs per host, to probe each node once."""
    urls: dict[str, tuple[str, ...]] = {}
    for candidate in candidates.values():
        for host, host_urls in candidate.urls_by_host.items():
            if host not in urls and host_urls:
                urls[host] = host_urls
    return urls


def _make_reader(
    context_by_url: dict[str, _FileContext],
    *,
    downloader: FileDownloader,
    chunk_bytes: int,
    connect_timeout: float,
    read_timeout: float,
) -> Callable[[str], DownloadInfo]:
    """Bind each candidate URL to its file's destination and checksum for the leaf."""

    def reader(url: str) -> DownloadInfo:
        context = context_by_url[url]
        return downloader(
            url,
            context.dest,
            expected_checksum=context.expected_checksum,
            checksum_type=context.checksum_type,
            chunk_bytes=chunk_bytes,
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
        )

    return reader


def _record_if_present(repository: Repository, file: File, dest: Path) -> bool:
    """
    Record a file already on disk as `skipped` and return whether it was skipped

    Skips when the file exists and either verifies against its checksum or (with no
    usable checksum) matches its recorded size.  A present-but-corrupt or wrong-size
    file is not skipped, so it is re-downloaded.
    """
    if file.id is None or not dest.exists():
        return False
    verified, algo = False, None
    if resolve_checksum(file.checksum, file.checksum_type) is not None:
        verified, algo = verify_file(dest, file.checksum, file.checksum_type)
        if not verified:
            return False  # present but corrupt -> re-download
    elif file.size is not None and file.size != dest.stat().st_size:
        return False  # size mismatch on an unverifiable file -> re-download
    repository.mark_download(
        file_id=file.id,
        status="skipped",
        local_path=str(dest),
        size_bytes=dest.stat().st_size,
        verified=verified,
        verified_algo=algo,
    )
    return True


def _file_record(file: File) -> FileRecord:
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


def _attempt_rows_from(
    records: Sequence[_DownloadAttemptRecord],
    url_to_file: dict[str, FileKey],
    seen: dict[str, int],
    attempts_by_file: dict[FileKey, int],
) -> list[DownloadAttempt]:
    """Turn logged downloads into per-attempt rows, joining each URL to its file."""
    rows: list[DownloadAttempt] = []
    for record in records:
        file_id = url_to_file.get(record.url)
        if file_id is None:
            continue
        seen[record.url] = seen.get(record.url, 0) + 1
        attempts_by_file[file_id] = attempts_by_file.get(file_id, 0) + 1
        mbps = (
            record.num_bytes / _MB / record.seconds
            if record.outcome is DownloadOutcome.SUCCESS and record.seconds > 0
            else 0.0
        )
        rows.append(
            DownloadAttempt(
                outcome=record.outcome.value,
                file_id=file_id,
                host=urlparse(record.url).hostname,
                url=record.url,
                bytes_downloaded=record.num_bytes,
                seconds=record.seconds,
                throughput_mbps=mbps,
                attempt_no=seen[record.url],
                resumed=record.resumed,
                detail=record.message,
            )
        )
    return rows


def _mean_mbps(attempt_log: DownloadAttemptLog | None) -> float | None:
    """Mean MB/s over the run's successful downloads, or `None` if there were none."""
    if attempt_log is None:
        return None
    total_bytes = 0
    total_seconds = 0.0
    for record in attempt_log.records():
        if record.outcome is DownloadOutcome.SUCCESS and record.seconds > 0:
            total_bytes += record.num_bytes
            total_seconds += record.seconds
    if total_seconds <= 0.0:
        return None
    return total_bytes / _MB / total_seconds
