"""
Download the files a search already found, to a local DRS tree (Step 5).

This is the **download** step: it reads an already-searched database (Steps 1-2 must
have run first, so `File`/`FileAccess` rows exist), picks a few models, and streams
their files from the ESGF data nodes to `DOWNLOAD_ROOT`, laid out as an ESGF-DRS tree.
It reuses the same health-aware, adaptive dispatcher the header step uses — now at
file grain — with a **separate, throughput-based** download-node health.

Configured tiny by default (`ONLY_SOURCE_IDS` + `VERSION_SELECTION = "latest"`) so a
live run pulls only a handful of files to your machine.  It is idempotent: a file
already on disk and checksum-verified is **skipped**, so re-running resumes/repairs a
partial run rather than re-fetching everything.

What it writes (all in `DB_PATH`, alongside the search tables):

- **`FileDownload`** — one row per file: its `local_path`, `status`
  (`complete`/`unverified`/`failed`/`skipped`), size and whether it verified.
- **`FileDownloadAttempt`** — the append-only per-attempt log (host, URL, outcome,
  bytes, seconds, MB/s), including retries, resumes and mirror fallbacks.
- **`DownloadNodeHealthStat`** — per-host throughput/reliability, learned across runs
  (ranking information only — never a cross-restart ignore list).

Inspect what a run wrote:

```sh
DB=uc1_full_ranked.sqlite
sqlite3 $DB 'SELECT status, count(*) FROM filedownload GROUP BY status;'
sqlite3 $DB 'SELECT local_path, verified FROM filedownload WHERE status="complete";'
sqlite3 $DB 'SELECT host, outcome, seconds, throughput_mbps FROM filedownloadattempt;'
sqlite3 $DB 'SELECT host, successes, attempts, total_bytes FROM downloadnodehealthstat;'
```

Run with: `uv run python scripts/download_uc1.py`
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

from cmip_data_manager import open_repository
from cmip_data_manager.search import download_files, select_target_versions

# --- Configuration (edit me) -------------------------------------------------
DB_PATH = "uc1_full_ranked.sqlite"
"""The already-searched database to download from (must hold File/FileAccess rows)."""

USE_CASE = "uc1_tas_ssp245"
"""The cached use-case tag whose stored files to download."""

DOWNLOAD_ROOT = "downloads"
"""Root directory the ESGF-DRS tree is written under (created if needed)."""

ONLY_SOURCE_IDS: tuple[str, ...] = ("ACCESS-CM2", "MIROC6", "CESM2")
"""Restrict the run to these models (empty tuple = every searched model).  Keep this
small for a live run — you are downloading real files to this machine."""

ONLY_VARIANT: str | None = "r1i1p1f1"
"""Restrict to a single ensemble member (variant_label); `None` = every variant.
`"r1i1p1f1"` is the canonical first member most models publish."""

VERSION_SELECTION = "latest"
"""Which version(s) to download per dataset: `"latest"` (true latest by version
date), `"all"`, or a `{master_id: version}` mapping to pin specific versions."""

PREFERRED_HOSTS: tuple[str, ...] = ()
"""Data nodes to try first (empty = no preference; ranking is by learned download
throughput, then HTTPS-first, with dead nodes dropped by the pre-flight probe)."""

IGNORE_HOSTS: frozenset[str] = frozenset()
"""Hosts to exclude outright (empty; the pre-flight probe finds dead nodes per-run —
download health is never turned into a persisted ignore list)."""

MAX_WORKERS = 4
"""Global cap on downloads in flight anywhere — the real throttle on a bandwidth-bound
local machine (raise it on a fat-pipe server)."""

NODE_CONCURRENCY = 2
"""Starting cap on simultaneous downloads to a single host."""

PREFLIGHT_PROBE = True
"""Front-load dead-node discovery with a live chunk probe before downloading."""

FORCE_REDOWNLOAD = False
"""If True, re-download even files already present and verified (ignore the skip)."""

RECORD_ATTEMPTS = True
"""Persist the per-attempt `FileDownloadAttempt` log (needed for the failure trail)."""


def _narrow_records(records):
    """Apply the `ONLY_SOURCE_IDS` / `ONLY_VARIANT` narrowings for a small live run."""
    if ONLY_SOURCE_IDS:
        allow = set(ONLY_SOURCE_IDS)
        records = [r for r in records if r.source_id in allow]
        print(f"  (only {sorted(allow)} -> {len(records)} datasets)")
    if ONLY_VARIANT is not None:
        records = [r for r in records if r.variant_label == ONLY_VARIANT]
        print(f"  (only variant {ONLY_VARIANT} -> {len(records)} datasets)")
    return records


_UNIT_STEP = 1024.0
"""Bytes per step between successive size units (B -> KB -> MB -> ...)."""


def _format_bytes(num: int) -> str:
    """Render a byte count in human-readable units."""
    size = float(num)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < _UNIT_STEP:
            return f"{size:.1f} {unit}"
        size /= _UNIT_STEP
    return f"{size:.1f} PB"


def _print_download_health(repository) -> None:
    """Print the download-node reliability and throughput rankings."""
    reliability = repository.rank_download_nodes_by_reliability()
    if not reliability:
        print("  (no download-node health recorded)")
        return

    print("  by reliability (failed % of downloads, best first):")
    for stat in reliability:
        failed = stat.attempts - stat.successes
        pct = 100.0 * failed / stat.attempts if stat.attempts else 0.0
        print(
            f"    {stat.host:<40} {pct:5.1f}% failed "
            f"({stat.successes}/{stat.attempts} ok, "
            f"{stat.timeouts} timeout, {stat.host_faults} fault, "
            f"{stat.blocks} blocked, {stat.checksum_failures} badsum, "
            f"{stat.errors} error)"
        )

    print("  by throughput (mean MB/s of successful downloads, fastest first):")
    for stat in repository.rank_download_nodes_by_throughput():
        mbps = (stat.total_bytes / 1_000_000.0) / stat.total_success_seconds
        print(f"    {stat.host:<40} {mbps:7.2f} MB/s mean")

    learned = [s for s in reliability if s.last_concurrency or s.max_safe_concurrency]
    if learned:
        print("  learned concurrency (cap converged / max concurrent seen clean):")
        for stat in learned:
            evicted = " EVICTED" if stat.last_concurrency == 0 else ""
            print(
                f"    {stat.host:<40} cap={stat.last_concurrency} "
                f"max_safe={stat.max_safe_concurrency}{evicted}"
            )


def _print_failed_trail(repository, file_id: int, since: datetime) -> None:
    """Print every node/URL attempted for a failed file this run, and how it ended."""
    attempts = repository.get_download_attempts(file_id=file_id, since=since)
    if not attempts:
        print("      (no attempts recorded — no candidate mirror to try)")
        return
    for attempt in reversed(attempts):  # get_download_attempts returns newest-first
        host = attempt.host or "(no host)"
        print(
            f"      [{attempt.outcome:<13}] {host:<32} "
            f"{attempt.seconds:6.1f}s  {attempt.url or ''}"
        )


def main() -> None:
    """Download the use case's stored files to a local DRS tree and report results."""
    repository = open_repository(DB_PATH)
    print(f"=== download {USE_CASE} -> {DOWNLOAD_ROOT} ===")

    started = time.perf_counter()
    records = repository.get_dataset_records(USE_CASE)
    elapsed = time.perf_counter() - started
    print(f"cache load: {len(records)} datasets in {elapsed:.2f}s")
    if not records:
        print("No datasets. Run a search into this DB first.")
        return

    records = _narrow_records(records)
    before = len({r.instance_key for r in records})
    records = select_target_versions(records, selection=VERSION_SELECTION)
    after = len({r.instance_key for r in records})
    print(f"version selection ({VERSION_SELECTION}): {before} -> {after} versions")

    run_started = datetime.now(timezone.utc)
    started = time.perf_counter()
    result = download_files(
        records,
        repository=repository,
        download_root=DOWNLOAD_ROOT,
        preferred_hosts=PREFERRED_HOSTS,
        ignore_hosts=IGNORE_HOSTS,
        max_workers=MAX_WORKERS,
        node_concurrency=NODE_CONCURRENCY,
        skip_existing=not FORCE_REDOWNLOAD,
        record_attempts=RECORD_ATTEMPTS,
        preflight_probe=PREFLIGHT_PROBE,
    )
    seconds = time.perf_counter() - started

    print(
        f"download step: downloaded={result.downloaded} "
        f"(verified={result.verified} unverified={result.unverified}) "
        f"skipped={result.skipped} failed={len(result.failed)} "
        f"no_http={len(result.no_http_access)} no_files={len(result.no_files)} "
        f"in {seconds:.2f}s"
    )
    throughput = (
        f" at {result.mean_mbps:.2f} MB/s mean" if result.mean_mbps is not None else ""
    )
    print(f"  transferred {_format_bytes(result.bytes_downloaded)}{throughput}")

    if result.failed:
        print(f"fully-failed files ({len(result.failed)}):")
        for file_id in result.failed:
            print(f"  file id {file_id}")
            if RECORD_ATTEMPTS:
                _print_failed_trail(repository, file_id, run_started)
    else:
        print("fully-failed files: none")

    if result.no_http_access:
        print(
            f"files with no HTTPServer mirror ({len(result.no_http_access)}): "
            f"{result.no_http_access}"
        )

    if result.no_files:
        print(
            f"versions with no stored files ({len(result.no_files)}) — "
            f"run the Step-2 file search for these first:"
        )
        for version_key in result.no_files:
            print(f"  {version_key}")

    print("download node health:")
    _print_download_health(repository)

    if RECORD_ATTEMPTS:
        print("this run's download attempts by node:")
        for summary in repository.download_attempt_summary(since=run_started):
            print(
                f"    {summary.key:<40} {summary.attempts:4d} attempts "
                f"({summary.successes} ok, {summary.failures} failed)"
            )


if __name__ == "__main__":
    main()
