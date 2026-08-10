"""
Download the CMIP7 files a search already found, to a local DRS tree (Step 5).

The CMIP7 counterpart of `scripts/download_uc1.py`: it reads a database that
`scripts/cmip7_search.py` populated (Steps 1-2 must have run, so `File`/`FileAccess`
rows exist from the STAC assets) and streams the files from the ESGF-NG data nodes to
`DOWNLOAD_ROOT`, laid out as a CMIP7-DRS tree (rebuilt from the STAC feature id, so the
branding suffix and region appear in the path).  It reuses the same health-aware,
adaptive download dispatcher UC1 uses and is idempotent (verified files are skipped).

Run with: ``uv run python scripts/download_cmip7.py``
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

from cmip_data_manager import open_repository
from cmip_data_manager.search import download_files, select_target_versions

# --- Configuration (edit me) -------------------------------------------------
DB_PATH = "cmip7_cache.sqlite"
"""The already-searched CMIP7 database (must hold File/FileAccess rows)."""

USE_CASE = "cmip7_picontrol_tas"
"""The cached use-case tag whose stored files to download (matches cmip7_search.py)."""

DOWNLOAD_ROOT = "downloads_cmip7"
"""Root directory the CMIP7-DRS tree is written under (created if needed)."""

VERSION_SELECTION = "latest"
"""Which version(s) to download per dataset (`"latest"`, `"all"`, or a pin mapping)."""

MAX_WORKERS = 4
"""Global cap on concurrent downloads (the real throttle on a local machine)."""

NODE_CONCURRENCY = 2
"""Starting cap on simultaneous downloads to a single host."""

PREFLIGHT_PROBE = True
"""Front-load dead-node discovery with a live chunk probe before downloading."""

FORCE_REDOWNLOAD = False
"""If True, re-download even files already present and verified."""
# -----------------------------------------------------------------------------


def main() -> None:
    """Download the CMIP7 use case's stored files to a local DRS tree and report."""
    repository = open_repository(DB_PATH)
    print(f"=== download {USE_CASE} -> {DOWNLOAD_ROOT} ===")

    records = repository.get_dataset_records(USE_CASE)
    print(f"cache load: {len(records)} datasets")
    if not records:
        print("No datasets. Run scripts/cmip7_search.py into this DB first.")
        return

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
        max_workers=MAX_WORKERS,
        node_concurrency=NODE_CONCURRENCY,
        skip_existing=not FORCE_REDOWNLOAD,
        record_attempts=True,
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
    if result.mean_mbps is not None:
        print(f"  mean throughput {result.mean_mbps:.2f} MB/s")
    if result.failed:
        print(f"fully-failed files: {result.failed}")
        for summary in repository.download_attempt_summary(since=run_started):
            print(
                f"    {summary.key:<40} {summary.attempts:4d} attempts "
                f"({summary.successes} ok, {summary.failures} failed)"
            )


if __name__ == "__main__":
    main()
