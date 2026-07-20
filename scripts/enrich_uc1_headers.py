"""
Live header enrichment for use case 1 (monthly `tas`, experiment `ssp245`).

This reads use case 1's datasets straight from the local cache
(`esgf_cache.sqlite`, populated by `scripts/esgf_search.py`), then reads and stores
one header (global attributes) per *simulation* `(source_id, experiment_id,
variant_label)` from the live ESGF data nodes — NCI preferred — and reports:

- **timing**: the search step (loading the cached datasets) and the header-read
  step, separately;
- **failures**: which simulations fully failed (every candidate mirror unreadable);
- **node health**: a reliability and speed ranking of the data nodes that served
  the reads, the per-node concurrency the adaptive controller learned (and any node
  it evicted), plus a data-driven timeout suggestion.

The header rows land in the `DatasetHeader` table and the node statistics in
`NodeHealthStat`, both in `esgf_cache.sqlite`, so you can explore them afterwards
(e.g. `sqlite3 esgf_cache.sqlite "SELECT * FROM datasetheader"`).

Run with: `uv run python scripts/enrich_uc1_headers.py`
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

from cmip_data_manager import Settings, build_client, open_repository
from cmip_data_manager.esgf.concurrency import exponential_backoff, thread_pool_map
from cmip_data_manager.esgf.query import FacetQuery
from cmip_data_manager.search import enrich_headers

# --- Configuration (edit me) -------------------------------------------------
DB_PATH = "esgf_cache.sqlite"
"""The cache written by `scripts/esgf_search.py` (must already hold use case 1)."""

USE_CASE = "uc1_tas_ssp245"
"""The cached use case whose datasets to enrich."""

SEARCH_SOURCE = "db"
"""`"api"` to run the use case 1 index search live (from scratch), `"db"` to load
the datasets from the cache.  `"db"` here reuses run 1's cached search results so
the header step is a *cold* re-run (see the header/health table wipe below)."""

UC1_QUERY = FacetQuery(
    project="CMIP6",
    variable_id=("tas",),
    frequency=("mon",),
    experiment_id=("ssp245",),
)
"""Use case 1: all monthly `tas` datasets for `ssp245`."""

ONE_VARIANT_PER_MODEL = False
"""If True, enrich only one variant (ensemble member) per model — a ~49-simulation
smoke test rather than the full ~553.  Set to False for the complete run."""

PREFERRED_HOSTS: tuple[str, ...] = ("esgf.nci.org.au",)
"""Data nodes to try first when reading headers (empty tuple = no preference)."""

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

NODE_CONCURRENCY_OVERRIDES: dict[str, int] = {"esgf.nci.org.au": 4}
"""Per-node caps overriding `NODE_CONCURRENCY` (NCI tolerates more, and is fast).
An overridden node is *pinned*: the adaptive controller won't grow or shrink it."""

CONCURRENCY_CEILING = 8
"""Hard upper bound the adaptive per-node cap will never grow past (stay polite)."""

EVICT_AFTER_ATTEMPTS = 6
"""Minimum reads on a node before it can be judged for eviction (a fair sample)."""

EVICT_MAX_SUCCESS_RATE = 0.2
"""Evict a judged node whose overall success rate is at or below this (near-dead)."""

READ_TIMEOUT_FALLBACK = 90.0
"""Stall timeout used on a cold DB with no learned health yet.  Once health exists,
the run sizes the timeout from the slowest healthy read observed (see `main`)."""

SETTINGS = Settings()
"""Endpoint/paging settings for the live file lookups."""
# -----------------------------------------------------------------------------


def _search_uc1(client, repository):
    """Run the use case 1 index search live and cache the datasets."""
    records = client.search(UC1_QUERY)
    repository.record_run(
        records,
        endpoint_url=client.base_url,
        spec={"note": "from-scratch uc1 index search"},
        tag=USE_CASE,
    )
    return records


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

    if ONE_VARIANT_PER_MODEL:
        records = _one_variant_per_model(records)
        models = len({r.source_id for r in records})
        sims = len({(r.source_id, r.experiment_id, r.variant_label) for r in records})
        print(f"  (one variant per model: {sims} simulations across {models} models)")
    else:
        sims = len({(r.source_id, r.experiment_id, r.variant_label) for r in records})
        print(f"  ({sims} simulations to enrich)")

    # 2. Header step: read + store one header per simulation, from the live nodes.
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
    outcome = enrich_headers(
        records,
        client=client,
        repository=repository,
        health=health,
        timeout=read_timeout,
        preferred_hosts=PREFERRED_HOSTS,
        ignore_hosts=IGNORE_HOSTS,
        max_workers=MAX_WORKERS,
        node_concurrency=NODE_CONCURRENCY,
        node_concurrency_overrides=NODE_CONCURRENCY_OVERRIDES,
        concurrency_ceiling=CONCURRENCY_CEILING,
        evict_after_attempts=EVICT_AFTER_ATTEMPTS,
        evict_max_success_rate=EVICT_MAX_SUCCESS_RATE,
        skip_cached=not FORCE_REREAD,
        avoid_unreliable_hosts=AVOID_UNRELIABLE_HOSTS,
        record_attempts=RECORD_ATTEMPTS,
    )
    header_seconds = time.perf_counter() - started

    print(
        f"header step: read={outcome.read} reused={outcome.reused} "
        f"stored={outcome.stored} skipped_cached={outcome.skipped_cached} "
        f"failed={len(outcome.failed)} in {header_seconds:.2f}s"
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
