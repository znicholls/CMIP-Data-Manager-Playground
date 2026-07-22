"""
Single-model walk-through of use case 1 (monthly `tas`, experiment `ssp245`).

Configured tiny by default (`SINGLE_MODEL` + `ONE_VARIANT_PER_MODEL`) so you can
watch **one** simulation flow through the three node-independent steps and inspect
exactly what each writes.  It runs the live index search, stores the version's
files, then reads and promotes one header — NCI preferred — reporting timing,
failures and node health along the way.

The three steps, the function each calls, and the tables each writes:

- **Step 1 — Search.**  `client.search(...)` hits the index node, then
  `Repository.record_run` writes `SearchRun`, `Dataset`, `DatasetVersion`,
  `DatasetNodeSpecificInfo`, `RunMembership` and `DatasetChange`.
- **Step 2 — Files.**  `search.add_files` searches each version's files and
  `Repository.store_files` writes `File` + `FileAccess` (one access per node URL).
- **Step 3 — Header.**  `search.enrich_version_headers` reads one header from the
  stored `FileAccess` URLs and `Repository.promote_header` stores it on
  `File.header_attrs_json`, promotes the `parent_*` subset onto `DatasetVersion`
  (with `header_from_file_key`), and logs `HeaderReadAttempt` + `NodeHealthStat`.

Everything lands in `DB_PATH` (a fresh `uc1_walkthrough.sqlite` by default).  To see
what each step wrote, open it between runs — the tables above are all queryable:

```sh
DB=uc1_walkthrough.sqlite
sqlite3 $DB '.tables'
# Step 1: the simulation and where it is published
sqlite3 $DB 'SELECT master_id FROM dataset;'
sqlite3 $DB 'SELECT instance_id, is_latest FROM datasetversion;'
sqlite3 $DB 'SELECT version_key, data_node FROM datasetnodespecificinfo;'
# Step 2: its files and their per-node access URLs
sqlite3 $DB 'SELECT filename, size FROM file;'
sqlite3 $DB 'SELECT data_node, service, url FROM fileaccess;'
# Step 3: the header on the file + the parent_* promoted onto the version
sqlite3 $DB 'SELECT filename, substr(header_attrs_json,1,80) FROM file;'
sqlite3 $DB 'SELECT parent_experiment_id, header_from_file_key FROM datasetversion;'
sqlite3 $DB 'SELECT host, outcome, seconds FROM headerreadattempt;'
```

Run with: `uv run python scripts/enrich_uc1_headers.py`
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

from cmip_data_manager import (
    Settings,
    build_client,
    build_file_search_clients,
    open_repository,
)
from cmip_data_manager.config import DEFAULT_INDEX_ENDPOINTS
from cmip_data_manager.esgf.concurrency import exponential_backoff, thread_pool_map
from cmip_data_manager.esgf.query import FacetQuery
from cmip_data_manager.search import (
    add_files,
    enrich_version_headers,
    select_target_versions,
)

# --- Configuration (edit me) -------------------------------------------------
DB_PATH = "uc1_full_ranked.sqlite"
"""Fresh database for the full ranked cold run (NCI->ORNL preference).  A distinct
file from the one-model walk-through so this run starts with no learned node health
(genuinely cold) and leaves the earlier walk-through DB untouched.  Delete to redo."""

USE_CASE = "uc1_tas_ssp245"
"""The cached use case whose datasets to enrich."""

SEARCH_SOURCE = "api"
"""`"api"` to run the use case 1 index search live (from scratch), `"db"` to load
the datasets from the cache.  For the walk-through use `"api"` so Step 1 actually
runs (`record_run` populates Dataset / DatasetVersion / DatasetNodeSpecificInfo)."""

UC1_QUERY = FacetQuery(
    project="CMIP6",
    variable_id=("tas",),
    frequency=("mon",),
    experiment_id=("ssp245",),
)
"""Use case 1: all monthly `tas` datasets for `ssp245`."""

VERSION_SELECTION = "latest"
"""Which dataset version(s) to fetch files/headers for.  Step 1 now stores **all**
published versions; this narrows Steps 2-3.  `"latest"` (true latest by version
date), `"all"` (every version), or a `{master_id: version}` mapping to pin specific
datasets to a chosen (e.g. older) version."""

SINGLE_MODEL: str | None = None
"""If set to a `source_id`, restrict the ENTIRE run to that one model — the tiniest
possible live walk-through of steps 1→2→3.  With `ONE_VARIANT_PER_MODEL = True` this
traces exactly one simulation (one `DatasetVersion`, its `File`/`FileAccess` rows,
one header read).  Set to `None` to run the full use case (~553 simulations)."""

ONE_VARIANT_PER_MODEL = False
"""If True, enrich only one variant (ensemble member) per model — a ~49-simulation
smoke test rather than the full ~553.  With `SINGLE_MODEL` set, this narrows to a
single simulation.  Set to False for the complete run."""

PREFERRED_HOSTS: tuple[str, ...] = ("esgf.nci.org.au", "esgf-node.ornl.gov")
"""Data nodes to try first when reading headers, **most-preferred first** (ranked):
NCI, then ORNL.  `order_candidates` ranks by tuple position, above the health tier,
so on this cold run the NCI->ORNL preference dominates and health only breaks ties
among the remaining mirrors.  Empty tuple = no preference."""

IGNORE_HOSTS: frozenset[str] = frozenset(
    {
        # http-only nodes observed to stall/refuse byte-range reads (~75s each to
        # fail to connect); the same files are served over https elsewhere.
        "esgf-data02.diasjp.net",
        "esgf-data03.diasjp.net",
        "esgf-data04.diasjp.net",
        "esg.iap.ac.cn",
    }
)
"""Data nodes to never read headers from (unioned with what NodeHealth has learned)."""

FORCE_REREAD = True
"""If True, re-read every simulation even if its header is already cached — useful
to re-time a run now that node health is known.  Leave False for normal operation."""

AVOID_UNRELIABLE_HOSTS = False
"""If True, skip nodes NodeHealth has learned are unreliable (plus `IGNORE_HOSTS`).
Set False for a *warm re-probe*: retry simulations whose only mirrors an earlier run
condemned (e.g. the 31 that failed on ucar/crd-esgf-drc, which still serve the files)
so a recovered node is noticed.  The static `IGNORE_HOSTS` are always honoured."""

RECORD_ATTEMPTS = True
"""If True, log every read attempt (host, URL, outcome, duration — retries and
failures included) to the `HeaderReadAttempt` table for per-node/per-model
diagnosis and day-to-day availability tracking."""

MAX_WORKERS = 12
"""Cap on header reads in flight across *all* nodes (the shared local budget)."""

NODE_CONCURRENCY = 2
"""Default cap on simultaneous reads to a single data node (conservative)."""

FILE_SEARCH_WORKERS = 8
"""Parallel per-version file searches in the Step-2 `add_files` pass (HTTP I/O)."""

FILE_SEARCH_ENDPOINTS: tuple[str, ...] = DEFAULT_INDEX_ENDPOINTS
"""Preference-ordered index endpoints for the Step-2 file search (metagrid-west, then
CEDA).  add_files tries each in turn, falling back only after an endpoint's retries and
requeues are exhausted.  Edit to search different mirrors or change the order."""

FILE_SEARCH_RETRIES = 5
"""In-request exponential-backoff retries per version, per endpoint, before requeue."""

READ_TIMEOUT_FALLBACK = 90.0
"""Stall timeout used on a cold DB with no learned health yet.  Once health exists,
the run sizes the timeout from the slowest healthy read observed (see `main`)."""

SETTINGS = Settings()
"""Endpoint/paging settings for the live file lookups."""
# -----------------------------------------------------------------------------


def _search_uc1(client, repository):
    """Run the use case 1 index search live and cache the datasets."""
    records = client.search(_effective_query())
    repository.record_run(
        records,
        endpoint_url=client.base_url,
        spec={"note": "from-scratch uc1 index search"},
        tag=USE_CASE,
    )
    return records


def _effective_query() -> FacetQuery:
    """Return the UC1 query, narrowed to `SINGLE_MODEL` when one is configured."""
    if SINGLE_MODEL is None:
        return UC1_QUERY
    return FacetQuery(
        project="CMIP6",
        variable_id=("tas",),
        frequency=("mon",),
        experiment_id=("ssp245",),
        source_id=(SINGLE_MODEL,),
    )


def _one_variant_per_model(records):
    """Keep only the datasets of one (the lowest-labelled) variant per model.

    Turns the full set of simulations into roughly one per model, for a cheaper
    first pass.  All of the chosen variant's datasets are kept (any table/replica).
    """
    chosen: dict[str, str] = {}
    for record in records:
        if record.source_id and record.variant_label:
            current = chosen.get(record.source_id)
            if current is None or record.variant_label < current:
                chosen[record.source_id] = record.variant_label
    return [
        record
        for record in records
        if chosen.get(record.source_id) == record.variant_label
    ]


def _narrow_records(records):
    """Apply the `SINGLE_MODEL` and `ONE_VARIANT_PER_MODEL` smoke-test narrowings."""
    if SINGLE_MODEL is not None:
        records = [r for r in records if r.source_id == SINGLE_MODEL]
        print(f"  (single model: {SINGLE_MODEL} -> {len(records)} datasets)")
    if ONE_VARIANT_PER_MODEL:
        records = _one_variant_per_model(records)
        models = len({r.source_id for r in records})
        sims = len({(r.source_id, r.experiment_id, r.variant_label) for r in records})
        print(f"  (one variant per model: {sims} simulations across {models} models)")
    else:
        sims = len({(r.source_id, r.experiment_id, r.variant_label) for r in records})
        print(f"  ({sims} simulations to enrich)")
    return records


def _print_node_health(repository) -> None:
    """Print the reliability and speed rankings from the persisted node health."""
    reliability = repository.rank_nodes_by_reliability()
    if not reliability:
        print("  (no node health recorded)")
        return

    print("  by reliability (failed % of header requests, best first):")
    for stat in reliability:
        failed = stat.attempts - stat.successes
        pct = 100.0 * failed / stat.attempts if stat.attempts else 0.0
        print(
            f"    {stat.host:<40} {pct:5.1f}% failed "
            f"({stat.successes}/{stat.attempts} ok, "
            f"{stat.timeouts} timeout, {stat.crashes} crash, "
            f"{stat.errors} error, {stat.blocks} blocked)"
        )

    print("  by speed (mean successful read, fastest first):")
    for stat in repository.rank_nodes_by_speed():
        mean = stat.total_success_seconds / stat.successes
        print(
            f"    {stat.host:<40} {mean:6.2f}s mean, "
            f"{stat.max_success_seconds:6.2f}s max"
        )

    # Learned per-node concurrency (what the adaptive controller converged to).
    # A learned cap of 0 means the node was circuit-broken (evicted) this run.
    learned = [s for s in reliability if s.last_concurrency or s.max_safe_concurrency]
    if learned:
        print("  learned concurrency (cap converged / max concurrent seen clean):")
        for stat in learned:
            evicted = " EVICTED" if stat.last_concurrency == 0 else ""
            print(
                f"    {stat.host:<40} cap={stat.last_concurrency} "
                f"max_safe={stat.max_safe_concurrency}{evicted}"
            )

    suggested = repository.load_node_health().suggested_timeout()
    if suggested is not None:
        print(f"  suggested read timeout (from observed reads): {suggested:.1f}s")


def _print_index_health(repository) -> None:
    """Print the persisted per-endpoint (index-node) file-search health."""
    ranked = repository.rank_index_nodes_by_reliability()
    if not ranked:
        print("  (no index-node health recorded)")
        return
    print("  index-node health (file searches, best first):")
    for stat in ranked:
        mean = stat.total_success_seconds / stat.successes if stat.successes else 0.0
        pct = 100.0 * (stat.attempts - stat.successes) / stat.attempts
        print(
            f"    {stat.endpoint:<52} {pct:5.1f}% failed "
            f"({stat.successes}/{stat.attempts} ok, {stat.retries} retries, "
            f"{stat.server_errors} 5xx, {stat.timeouts} timeout) "
            f"{mean:5.2f}s mean"
        )


def _print_failed_trail(
    repository,
    source_id: str,
    experiment_id: str,
    variant_label: str,
    since: datetime,
) -> None:
    """Print every node/URL attempted for a failed simulation this run, and how."""
    attempts = repository.get_header_attempts(
        source_id=source_id,
        experiment_id=experiment_id,
        variant_label=variant_label,
        since=since,
    )
    if not attempts:
        print("      (no attempts recorded — no candidate mirror to try)")
        return
    for attempt in reversed(attempts):  # get_header_attempts returns newest-first
        host = attempt.host or "(no host)"
        detail = attempt.url or attempt.outcome
        print(
            f"      [{attempt.outcome:<8}] {host:<32} {attempt.seconds:6.1f}s  {detail}"
        )


def main() -> None:
    """Enrich use case 1's headers from the live nodes and report the results."""
    repository = open_repository(DB_PATH)
    client = build_client(
        SETTINGS,
        retry=exponential_backoff(retries=4),
        map_fn=thread_pool_map(max_workers=8),
    )
    print(f"=== {USE_CASE} (search source: {SEARCH_SOURCE}) ===")

    # 1. Search step: either query the index node live, or load from the cache.
    started = time.perf_counter()
    if SEARCH_SOURCE == "api":
        records = _search_uc1(client, repository)
        step = "index search (live)"
    else:
        records = repository.get_dataset_records(USE_CASE)
        step = "cache load"
    search_seconds = time.perf_counter() - started
    print(f"search step ({step}): {len(records)} datasets in {search_seconds:.2f}s")
    if not records:
        print(
            "No datasets. Set SEARCH_SOURCE='api' or run scripts/esgf_search.py first."
        )
        return

    records = _narrow_records(records)

    # Version selection: Step 1 stored ALL versions; narrow Steps 2-3 to the target
    # version per dataset (default: true latest by version date, not ESGF's flag).
    before_versions = len({r.instance_key for r in records})
    records = select_target_versions(records, selection=VERSION_SELECTION)
    after_versions = len({r.instance_key for r in records})
    print(
        f"version selection ({VERSION_SELECTION}): "
        f"{before_versions} -> {after_versions} dataset versions"
    )

    # 2. File step: search each version's files once and store them as File /
    #    FileAccess rows (the header read in step 3 reads from these, not the index).
    #    One search per version, parallelised across workers, with per-endpoint
    #    backoff -> requeue -> fallback over FILE_SEARCH_ENDPOINTS, saving as it goes.
    file_clients = build_file_search_clients(FILE_SEARCH_ENDPOINTS, settings=SETTINGS)
    print(f"  file-search endpoints (in order): {list(FILE_SEARCH_ENDPOINTS)}")
    started = time.perf_counter()
    files = add_files(
        records,
        clients=file_clients,
        repository=repository,
        map_fn=thread_pool_map(max_workers=FILE_SEARCH_WORKERS),
        retries=FILE_SEARCH_RETRIES,
        skip_cached=not FORCE_REREAD,
    )
    files_seconds = time.perf_counter() - started
    print(
        f"file step: searched={files.searched} "
        f"skipped_cached={files.skipped_cached} files_stored={files.files_stored} "
        f"overflowed={len(files.overflowed)} failed={len(files.failed)} "
        f"in {files_seconds:.2f}s"
    )
    _print_index_health(repository)

    # 3. Header step: read + promote one header per simulation, from the live nodes.
    #    Size the stall timeout from what healthy nodes actually took last run
    #    (falls back to READ_TIMEOUT_FALLBACK on a cold DB with no health yet).
    health = repository.load_node_health()
    read_timeout = health.suggested_timeout(default=READ_TIMEOUT_FALLBACK)
    source = "learned from node health" if health.snapshot() else "default (no health)"
    print(f"  read timeout for this run: {read_timeout:.1f}s ({source})")
    if not AVOID_UNRELIABLE_HOSTS:
        print("  re-probing health-condemned nodes this run (avoid_unreliable=False)")
    run_started = datetime.now(timezone.utc)
    started = time.perf_counter()
    outcome = enrich_version_headers(
        records,
        repository=repository,
        health=health,
        timeout=read_timeout,
        preferred_hosts=PREFERRED_HOSTS,
        ignore_hosts=IGNORE_HOSTS,
        max_workers=MAX_WORKERS,
        node_concurrency=NODE_CONCURRENCY,
        skip_cached=not FORCE_REREAD,
        avoid_unreliable_hosts=AVOID_UNRELIABLE_HOSTS,
        record_attempts=RECORD_ATTEMPTS,
    )
    header_seconds = time.perf_counter() - started

    print(
        f"header step: read={outcome.read} reused={outcome.reused} "
        f"promoted={outcome.promoted} skipped_cached={outcome.skipped_cached} "
        f"failed={len(outcome.failed)} no_files={len(outcome.no_files)} "
        f"in {header_seconds:.2f}s"
    )

    # 3. Which simulations fully failed (every candidate mirror unreadable)?
    #    With RECORD_ATTEMPTS on, print each one's attempt trail from this run so a
    #    failure shows exactly which nodes/URLs were tried and how each ended.
    if outcome.failed:
        print(f"fully-failed simulations ({len(outcome.failed)}):")
        for source_id, experiment_id, variant_label in sorted(outcome.failed):
            print(f"  {source_id} / {experiment_id} / {variant_label}")
            if RECORD_ATTEMPTS:
                _print_failed_trail(
                    repository, source_id, experiment_id, variant_label, run_started
                )
    else:
        print("fully-failed simulations: none")

    # 4. Node-health summary (explore the full tables in the sqlite file).
    print("node health:")
    _print_node_health(repository)

    # 5. This run's attempts rolled up by node (the raw log holds far more).
    if RECORD_ATTEMPTS:
        print("this run's attempts by node:")
        for summary in repository.header_attempt_summary(since=run_started):
            print(
                f"    {summary.key:<40} {summary.attempts:4d} attempts "
                f"({summary.successes} ok, {summary.failures} failed) "
                f"{summary.outcomes}"
            )


if __name__ == "__main__":
    main()
