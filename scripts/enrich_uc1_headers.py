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
  the reads, plus a data-driven timeout suggestion.

The header rows land in the `DatasetHeader` table and the node statistics in
`NodeHealthStat`, both in `esgf_cache.sqlite`, so you can explore them afterwards
(e.g. `sqlite3 esgf_cache.sqlite "SELECT * FROM datasetheader"`).

Run with: `uv run python scripts/enrich_uc1_headers.py`
"""

from __future__ import annotations

import time

from cmip_data_manager import Settings, build_client, open_repository
from cmip_data_manager.esgf.concurrency import exponential_backoff, thread_pool_map
from cmip_data_manager.esgf.query import FacetQuery
from cmip_data_manager.search import enrich_headers

# --- Configuration (edit me) -------------------------------------------------
DB_PATH = "esgf_cache.sqlite"
"""The cache written by `scripts/esgf_search.py` (must already hold use case 1)."""

USE_CASE = "uc1_tas_ssp245"
"""The cached use case whose datasets to enrich."""

SEARCH_SOURCE = "api"
"""`"api"` to run the use case 1 index search live (from scratch), `"db"` to load
the datasets from the cache."""

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

FORCE_REREAD = False
"""If True, re-read every simulation even if its header is already cached — useful
to re-time a run now that node health is known.  Leave False for normal operation."""

HEADER_READS = thread_pool_map(max_workers=8)
"""Fan header reads out on a thread pool — `with_timeout` isolates each in a child."""

SETTINGS = Settings()
"""Endpoint/paging settings for the live file lookups."""
# -----------------------------------------------------------------------------


def _search_uc1(client, repository):
    """Run the use case 1 index search live and cache the datasets."""
    records = client.search(UC1_QUERY)
    repository.record_run(
        USE_CASE,
        records,
        endpoint_url=client.base_url,
        spec={"note": "from-scratch uc1 index search"},
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
            f"{stat.timeouts} timeout, {stat.crashes} crash, {stat.errors} error)"
        )

    print("  by speed (mean successful read, fastest first):")
    for stat in repository.rank_nodes_by_speed():
        mean = stat.total_success_seconds / stat.successes
        print(
            f"    {stat.host:<40} {mean:6.2f}s mean, "
            f"{stat.max_success_seconds:6.2f}s max"
        )

    suggested = repository.load_node_health().suggested_timeout()
    if suggested is not None:
        print(f"  suggested read timeout (from observed reads): {suggested:.1f}s")


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
    started = time.perf_counter()
    outcome = enrich_headers(
        records,
        client=client,
        repository=repository,
        preferred_hosts=PREFERRED_HOSTS,
        ignore_hosts=IGNORE_HOSTS,
        read_map=HEADER_READS,
        skip_cached=not FORCE_REREAD,
    )
    header_seconds = time.perf_counter() - started

    print(
        f"header step: read={outcome.read} reused={outcome.reused} "
        f"stored={outcome.stored} skipped_cached={outcome.skipped_cached} "
        f"failed={len(outcome.failed)} in {header_seconds:.2f}s"
    )

    # 3. Which simulations fully failed (every candidate mirror unreadable)?
    if outcome.failed:
        print(f"fully-failed simulations ({len(outcome.failed)}):")
        for source_id, experiment_id, variant_label in sorted(outcome.failed):
            print(f"  {source_id} / {experiment_id} / {variant_label}")
    else:
        print("fully-failed simulations: none")

    # 4. Node-health summary (explore the full tables in the sqlite file).
    print("node health:")
    _print_node_health(repository)


if __name__ == "__main__":
    main()
