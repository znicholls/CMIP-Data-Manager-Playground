# Download step — implementation plan

## Context

The repository already **searches** CMIP5/CMIP6 data across ESGF1 and ESGF-NG and
persists, for every file, its access URLs (`FileAccess.url` / `fsspec_url`) plus
node-independent identity/checksum/size on `File`. Complex use cases have already
connected to data nodes (header reads) and left behind reusable node-health,
routing, dispatch and preflight machinery.

The next phase is **downloading**: pull the actual file bytes from ESGF data nodes
to local disk, reusing the search step's data-node connection parallelism, and — as
with search — persist detailed records of every data-node attempt (successes,
failure reasons, timings, throughput).

This document is the agreed design. **It is a plan only — do not implement yet.**
The first live run will download only a few hand-picked models to a local machine.

## Guiding decisions (resolved with the user)

1. **Input = the searched set.** The engine loads `DatasetRecords` for a use-case
   `tag` via `Repository.get_dataset_records(TAG)` — whatever was searched is the
   download candidate set. Narrowing happens primarily *at search time* (the live
   run searches ~2–3 models, one variant/experiment/variable, latest version), but
   the download engine also accepts an **optional selection filter** (source_id
   allowlist, latest-only) so search and download stay decoupled: a user can search
   broadly, inspect availability, then download a subset without re-searching.

2. **fsspec is the stored handle; httpx does the local fetch.** The exposed URL
   stays **fsspec-format** (`FileAccess.fsspec_url`) — the single handle that keeps
   *both* doors open: pull-to-local **and** future remote/Pangeo-style lazy access.
   This local-download step **self-streams with httpx** underneath (Range-based
   `.part` resume, on-the-fly checksum, direct MB/s measurement). Remote-interaction
   is a future consumer of the same handle and must not be precluded.

3. **File-grain work unit; reuse-and-generalize the dispatcher.** The atomic unit
   becomes a **file** (not a `SimulationKey`). Generalize `esgf/dispatch.py`
   `dispatch_reads`/`_Scheduler` over a generic hashable work-item key (backward
   compatible — `SimulationKey` is one instantiation) and add a **non-collapsing,
   file-grain candidate builder** to `esgf/routing.py` (do **not** remove the
   header `_one_file` collapse). Reuse the AIMD per-host caps, spill, eviction and
   `source_id` affinity clustering unchanged.

4. **httpx-native in-thread timeouts, with the subprocess kill kept as a future
   option.** The header path's subprocess `with_timeout`/`_OrphanReaper` exists only
   because libnetcdf byte-range reads block in uninterruptible C. httpx streaming
   honours timeouts natively, so downloads use an in-thread chunk read-timeout + a
   stall/wall-clock guard, no subprocess — enabling live progress + trivial resume.
   **OPEN RISK (do not write off):** if in-thread timeouts leave wedged OS-level
   sockets in practice, we must be able to swap the subprocess isolation back in.
   Therefore the leaf downloader's timeout/isolation is a **pluggable seam** so the
   existing `with_timeout`/`_OrphanReaper` can be dropped in later *without touching
   the orchestrator*. The header path itself is left bit-for-bit unchanged.

5. **Separate, throughput-based download health; session-scoped dead-verdicts.**
   Downloads get their own health store (`DownloadNodeHealthStat`) ranked by
   **measured MB/s** and download-success rate — header read-latency health does
   **not** transfer (NOTES.md:493, 875–884: header-"dead" nodes often download
   fine). We run our own liveness/throughput test in download space (reuse the
   preflight chunk probe for a real dead-verdict + initial MB/s seed). **Dead
   verdicts are session-scoped only** — held in an in-memory `ProbeCache` for one
   run, **never persisted as a cross-restart ignore list**. Every restart begins
   with `ignore_hosts` empty and re-probes every node (data nodes are a moving
   target). Persisted `DownloadNodeHealthStat` is informational ranking only and
   must never become a hard exclusion.

6. **DRS directory tree under a configurable root, via a pluggable path-builder.**
   Files land at
   `{DOWNLOAD_ROOT}/{mip_era}/{activity}/{institution}/{source_id}/{experiment}/{variant}/{table}/{variable}/{grid}/v{version}/{filename}`.
   Self-describing, standard (synda/esgpull/replica-compatible, crawlable by
   `intake-esm`/`xarray`), collision-free, version-isolated. `path_for(file,
   version, dataset) -> Path` is a **DI seam** dispatching on `mip_era`.

7. **Resume / idempotency / verification.**
   - Stream to a `.part` file; `fsync` + **atomic rename** to the final DRS path
     only on success (a killed run never leaves a truncated file looking complete).
   - **Resume**: existing `.part` ⇒ `Range: bytes={size}-` append (fall back to full
     re-fetch if the server ignores Range).
   - **Skip-if-done**: final path exists *and* passes checksum ⇒ skip (record
     `skipped`). Exists but no stored checksum ⇒ skip on size match (configurable to
     force re-verify).
   - **Verify** against `File.checksum`/`checksum_type` after download; mismatch ⇒
     discard `.part`, record `checksum_failed`, retry on the **next mirror** (corrupt
     file = node-quality signal). Missing/undeterminable checksum ⇒ accept on
     completion + size match, flag `unverified`.

8. **Persistence — three new tables** (each mirrors an existing pattern):
   - `FileDownloadAttempt` — append-only fact table (twin of `HeaderReadAttempt` /
     `FileAccessAttempt`); its write-side DTO is `DownloadAttempt` (mirroring
     `HeaderReadAttempt`↔`HeaderAttempt`).
   - `DownloadNodeHealthStat` — per-host aggregate (twin of `DataNodeHealthStat`),
     throughput-oriented; its in-memory registry is `DownloadNodeHealth` in
     `esgf/download_health.py` (twin of `esgf/health.py`'s `NodeHealth`).
   - `FileDownload` — 1:1 side table for terminal state (side-table pattern like
     `Cmip5VersionExtra`), **not** columns on `File`.

   **Build status: Steps 1–6 DONE (engine complete; live run pending).**
   - Steps 1–2: schema + `esgf/download_health.py` + `Repository` methods + tests.
   - Step 3: `esgf/download.py` leaf httpx downloader (`.part`/Range resume, atomic
     rename, multihash-aware verify, inline error classification) + tests.
   - Step 4: `esgf/dispatch.py` generalized over a `WorkKey`/`Result` TypeVar via a
     structural `NodeCandidates` protocol + `block_errors`/`fault_errors` params
     (`DispatchResult.headers`→`results`); `esgf/routing.py` gained `FileCandidates`
     + `build_file_candidates` (no `_one_file` collapse). Header path behaviour
     preserved.
   - Step 5: `search/download.py` orchestrator — `download_files` + `drs_path`
     (era-aware DRS seam) + `record_download`/`DownloadAttemptLog` (throughput) +
     `_DownloadWriter` (save-as-you-go) + url→context reader binding +
     `DownloadResult` + tests (fake downloader, no network).
   - Step 6: `scripts/download_uc1.py` (UPPERCASE config, loads a searched DB, narrows
     via `ONLY_SOURCE_IDS` + `select_target_versions`, calls `download_files`, prints a
     `DownloadResult` summary + throughput node-health + per-file failure trail).
     `download_files`/`DownloadResult`/`drs_path` exported from `search`.
   - Full suite green (507 tests, 95.81% coverage), `mypy --strict` + `ruff` clean.
     Script wiring + reporting smoke-tested against a real DB (failover, throughput,
     failure trail all render); the **actual network download is the user's live run**.
   - **Remaining: the live run** — the user downloads a few models to their machine
     via `scripts/download_uc1.py` and confirms real DRS paths, checksum verification,
     resume (kill + rerun), and skip-on-rerun.

9. **Conservative, user-overridable concurrency.** Reuse AIMD/eviction/spill but
   ship low defaults (`max_workers ≈ 4`, per-host start `≈ 2`, ceiling `≈ 3–4`)
   because on a bandwidth-bound local machine the **global `max_workers` is the real
   throttle**, not server fan-out. All exposed as script config. Likely to evolve
   once live downloads reveal real bandwidth behaviour (e.g. fat-pipe server runs).

10. **Layering = Option A (spread across existing layers).** Leaf I/O in
    `esgf/download.py`, dispatcher generalization in `esgf/dispatch.py`, file-grain
    routing in `esgf/routing.py`, orchestrator in `search/download.py`, tables in
    `db/schema.py`, methods in `db/repository.py`, config/wiring in
    `scripts/download_<usecase>.py`. Download is "Step 5" in the same orchestration
    layer as search.

## Cross-cutting correctness: CMIP5/6 × ESGF1/NG

The byte-moving machinery is **era- and backend-agnostic** — it operates only on
`File`/`FileAccess` rows, which are populated identically across all four
combinations. Two accommodations are required:

- **CMIP5 DRS path** differs from CMIP6 (`esgf/cmip5.py:4–16`): CMIP6-shaped columns
  are deliberately overloaded (`grid_label` holds the composite
  `"{source_id}_{experiment_id}_{realm}"`; `nominal_resolution` dropped;
  `product`/`realm` not in standard columns). The CMIP5 `path_for` branch
  reconstructs the true CMIP5 tree from `Cmip5VersionExtra.native_dataset_id` /
  `native_master_id` / `realm` (`schema.py:327–367`, `cmip5.py:240–243`) — the native
  id is already the dotted CMIP5-DRS string.
- **ESGF-NG checksum metadata** differs (`search/files_ng.py:86–87`): NG sets
  `checksum = asset["file:checksum"]` but `checksum_type = None` (STAC `file:checksum`
  is multihash-encoded). Verification must be **checksum-type-aware**: use
  `checksum_type` when present (ESGF1); when `None` with a checksum, **decode the
  multihash prefix** to recover the algorithm; otherwise fall into `unverified`.
  Everything else on the NG path is identical: `files_ng._asset_urls` emits the same
  `url|mime|HTTPServer` `FileRecord`s, and per-host routing keys off the URL hostname
  even though NG datasets flatten to `data_node is None`.

## New / changed files

| File | Change |
|---|---|
| `src/cmip_data_manager/esgf/download.py` | **New.** Leaf httpx streaming downloader (module-level fn), `.part`/Range resume, chunked write, on-the-fly checksum, MB/s measurement; **multihash-aware** verification; pluggable timeout/isolation seam (in-thread default, subprocess future option). Parallels `esgf/headers.py`. |
| `src/cmip_data_manager/esgf/dispatch.py` | **Generalize in place.** Widen `_Scheduler`/`dispatch_reads` work-item key from `SimulationKey` to a `Hashable` TypeVar. Behaviour-preserving; header path unaffected. |
| `src/cmip_data_manager/esgf/routing.py` | **Add** a file-grain, non-collapsing candidate builder (one entry per file with its per-host ranked mirror URLs). Existing `build_candidates`/`_one_file` untouched. |
| `src/cmip_data_manager/search/download.py` | **New.** Orchestrator `download_files(...)` (DI-seam signature) + `_DownloadWriter` (save-as-you-go `on_success`/`on_flush`), throughput health load/save, preflight wiring, `path_for` DRS seam, `DownloadResult` dataclass. Parallels `search/version_headers.py`. |
| `src/cmip_data_manager/db/schema.py` | **New tables** `DownloadAttempt`, `DownloadNodeHealthStat`, `FileDownload`. |
| `src/cmip_data_manager/db/repository.py` | **New methods** `record_download_attempts` / `get_download_attempts` / `download_attempt_summary`; `save_download_health` / `load_download_health` / `rank_download_nodes_by_throughput`; `mark_download` / `get_download_state` / `version_downloads`. Extend `_access_columns` producers as needed. |
| `src/cmip_data_manager/db/__init__.py` | Export new tables as appropriate. |
| `scripts/download_<usecase>.py` | **New.** UPPERCASE config + `main()`: `open_repository`, engine call, `print()` timings/summary/failure-trail/health, exactly like `enrich_uc*_headers.py`. |
| `tests/unit/…`, `tests/integration/…` | New tests to hold coverage ≥ 90% and `mypy --strict`. |

## Schema detail

**`DownloadAttempt`** (append-only): `id` PK · `created_at` idx · `file_id` idx →
`File.id` · `host` idx · `url` · `outcome` idx
(`success`/`timeout`/`blocked`/`host_fault`/`checksum_failed`/`error`/`no_candidate`/`stranded`)
· `bytes_downloaded` · `seconds` · `throughput_mbps` · `attempt_no` · `resumed` bool ·
`detail`.

**`DownloadNodeHealthStat`** (per-host aggregate): `host` PK · `attempts` ·
`successes` · `failures` · `timeouts` · `blocks` · `host_faults` ·
`checksum_failures` · `total_bytes` · `total_success_seconds` · `max_success_seconds`
· `max_safe_concurrency` · `last_concurrency` · `updated_at`. Mean MB/s derived.

**`FileDownload`** (1:1 terminal state): `file_id` PK → `File.id` · `local_path` ·
`status` (`complete`/`failed`/`unverified`/`skipped`) · `size_bytes` · `verified`
bool · `verified_algo` · `download_from_access_key` (soft ptr → `FileAccess.id`) ·
`attempts` · `completed_at`.

Migration: additive-only, handled by existing `init_db` / `_add_missing_columns`
(`db/engine.py:85,103`). SQLite WAL already on.

## Engine flow (`download_files`)

Mirrors `enrich_version_headers` (`search/version_headers.py:192`):
1. Load `DatasetRecords` for `tag`; apply optional selection filter (source_id
   allowlist, latest-only). Load `DownloadNodeHealthStat`.
2. Expand to **file-grain** work items via `Repository.get_version_files(...)`
   (`File` + eager `FileAccess`); compute each file's DRS target via `path_for`.
3. **Skip planning**: final path present + checksum-verified ⇒ `skipped`; else queue.
4. Build **file-grain candidates** (per-host ranked mirror URLs, no collapse).
5. Preflight chunk-probe (session `ProbeCache`, empty `ignore_hosts` at cold start)
   ⇒ union dead hosts into the session ignore set + seed initial MB/s ranking.
6. Compose the per-download pipeline: `leaf_download` → in-thread timeout/stall guard
   → `promote_blocks` → `promote_host_faults` → throughput-`recording` → `with_retry`.
   (Reuse `promote_*`/`with_retry` from `esgf/headers.py`; swap the leaf + timeout.)
7. Dispatch via the generalized `dispatch_reads` (file-grain key, conservative
   `max_workers`/`node_concurrency`, AIMD/spill/eviction/affinity).
8. **Save-as-you-go** (`_DownloadWriter`): `on_success` writes `FileDownload` the
   instant a file verifies+renames; `on_flush` appends `DownloadAttempt` rows +
   throttled `save_download_health`. Single controller thread ⇒ no locking.
9. Finalise: record learned per-host caps, write attempt-log tail, save health,
   return `DownloadResult(downloaded, skipped_cached, verified, unverified,
   failed, no_http_access, bytes_total, mean_mbps)`.

Version-level completeness is **derived** for reporting (all a version's files
`complete`); a failed file is just a failed file — never blocks.

## Reporting (script-side, mirrors `enrich_uc*_headers.py`)

`print()` only, engine stays silent: banner · per-phase `perf_counter` timings ·
`DownloadResult` summary · per-file failure trail from `get_download_attempts`
(`_print_failed_trail` analog) · download node-health summary — throughput ranking +
learned caps (`_print_node_health` analog) · preflight verdicts (`_print_probe`
analog).

## Open risks / expected-to-evolve

- **In-thread vs subprocess timeout (D4).** If wedged OS sockets appear live, swap
  the subprocess isolation in via the leaf's timeout seam. Do **not** design it out.
- **AIMD vs client-bandwidth ceiling (D9).** AIMD grows to a *server* ceiling;
  local downloads are *client-bandwidth* bound. Conservative caps + global
  `max_workers` handle it now; revisit for fat-pipe/server runs
  (`dispatch.py:56` already anticipates this).
- **Concurrency defaults (D9)** are a "logical first step," likely retuned on live data.

## Verification (how we'll test the change end-to-end)

- **Unit**: multihash decode + checksum verify (md5/sha256/NG-multihash/none-⇒-unverified);
  DRS `path_for` for CMIP6 (column-driven) and CMIP5 (`Cmip5VersionExtra`-driven);
  `.part` resume Range logic; skip-if-done; file-grain candidate builder (no collapse);
  generalized dispatcher still passes existing header tests unchanged.
- **Repository**: round-trip the three new tables (append-only attempts, health
  upsert/rank, `FileDownload` state), save-as-you-go crash-safety.
- **Integration / live smoke**: a `scripts/download_<usecase>.py` run against an
  already-searched DB restricted via `ONLY_SOURCE_IDS` to 2–3 models, one
  variant/experiment/variable, latest version, into a scratch `DOWNLOAD_ROOT`;
  confirm DRS paths, checksum verification, resume (kill + rerun), skip-on-rerun,
  and populated `DownloadAttempt`/`DownloadNodeHealthStat`/`FileDownload` + printed
  health/throughput summary. Keep coverage ≥ 90%, `mypy --strict`, `ruff` clean
  (`make checks`).
