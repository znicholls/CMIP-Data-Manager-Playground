"""
Live, multi-hop header enrichment for G6solar (the walk-the-parent-tree case).

This takes use case 2's one-hop idea and follows the parent chain *all the way up*,
making no assumptions about which experiments it passes through.  Starting from
`G6solar` / `tas` / `mon` alone, it:

1. reads one header per `G6solar` simulation (the same health-aware, timeout/retry
   pipeline use cases 1 and 2 use), stored on the `File` and promoted onto the
   `DatasetVersion`;
2. follows whatever parent each header declares (`parent_*` metadata) — typically
   `G6solar -> ssp585 -> historical -> piControl`, but the experiments and variants
   are discovered, never assumed — via one index search per distinct parent;
3. links each child *version* to its parent *version* (`parent_version_key`), a
   shared ancestor searched and read once (a global visited set collapses the tree);
4. with `stopping_experiment=None`, walks all the way to the CMIP6 `"no parent"`
   sentinel — the true top of the tree — and records those as terminals;
5. reports the resolved version links, the terminals, and node health.

By default this runs **cold**: a fresh database (`g6solar_cache.sqlite`) with no
learned node health, so timeouts fall back to `READ_TIMEOUT_FALLBACK`.  Headers are
stored on `File.header_attrs_json`, promoted onto `DatasetVersion`, and node
statistics on `NodeHealthStat`.

Run with: `uv run python scripts/enrich_g6solar_headers.py`
"""

from __future__ import annotations

import os
import time

from cmip_data_manager import Settings, build_client, open_repository
from cmip_data_manager.esgf.concurrency import exponential_backoff, thread_pool_map
from cmip_data_manager.search import (
    ParentResolutionError,
    per_variable_experiment,
    resolve_parent_chains,
)

# --- Configuration (edit me) -------------------------------------------------
DB_PATH = os.environ.get("G6_DB_PATH", "g6solar_cache.sqlite")
"""Cache to use.  Defaults to a **fresh** `g6solar_cache.sqlite` for a cold run
(no learned health); override with `G6_DB_PATH`."""

USE_CASE = "g6solar"
"""The cached use case to record the from-scratch index search under."""

SEARCH_SOURCE = "api"
"""`"api"` to run the G6solar index search live from scratch (and cache it), or
`"db"` to reuse the datasets from the latest cached run."""

PROJECT = "CMIP6"
"""Project to search when `SEARCH_SOURCE == "api"`."""

CHILD_EXPERIMENT = "G6solar"
"""The experiment whose parent chain we walk up."""

REQUIRED_VARS: tuple[str, ...] = ("tas",)
"""Variable to read a header for (single variable, as requested)."""

FREQUENCY = ("mon",)
"""Frequency the from-scratch index search covers."""

MAX_HOPS = 8
"""Safety cap on the number of hops up the tree (guards a metadata cycle)."""

PREFERRED_HOSTS: tuple[str, ...] = ("esgf.nci.org.au",)
"""Data nodes to try first when reading headers (empty tuple = no preference)."""

IGNORE_HOSTS: frozenset[str] = frozenset(
    {
        # http-only nodes observed to stall byte-range reads (~75s each to fail);
        # the same files are served over https elsewhere.
        "esgf-data02.diasjp.net",
        "esgf-data03.diasjp.net",
        "esgf-data04.diasjp.net",
        "esg.iap.ac.cn",
    }
)
"""Data nodes to never read headers from (unioned with what NodeHealth has learned)."""

SKIP_CACHED = True
"""Skip simulations whose header is already cached (don't re-read a known header)."""

MAX_WORKERS = 12
"""Cap on header reads in flight across all nodes (the shared local budget)."""

NODE_CONCURRENCY = 2
"""Default cap on simultaneous reads to a single data node (conservative)."""

SEARCH_WORKERS = 8
"""Parallel per-parent index searches and per-version file searches in the walk."""

READ_TIMEOUT_FALLBACK = 90.0
"""Stall timeout used on a cold DB with no learned health yet."""

SETTINGS = Settings()
"""Endpoint/paging settings for the live file lookups and ancestor re-fetches."""
# -----------------------------------------------------------------------------


def _search_g6solar(client, repository):
    """Run the G6solar index search live from scratch and cache the datasets."""
    queries = per_variable_experiment(
        PROJECT, REQUIRED_VARS, (CHILD_EXPERIMENT,), frequency=FREQUENCY
    )
    by_id = {r.id: r for result in client.search_many(queries) for r in result}
    records = list(by_id.values())
    run = repository.record_run(
        records,
        endpoint_url=client.base_url,
        spec={"queries": [q.as_spec() for q in queries]},
        tag=USE_CASE,
    )
    print(
        f"index search (live): {len(records)} datasets; "
        f"changes +{len(run.added)} / -{len(run.removed)} / ~{len(run.modified)}"
    )
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
            f"({stat.successes}/{stat.attempts} ok)"
        )
    print("  by speed (mean successful read, fastest first):")
    for stat in repository.rank_nodes_by_speed():
        mean = stat.total_success_seconds / stat.successes
        print(
            f"    {stat.host:<40} {mean:6.2f}s mean, "
            f"{stat.max_success_seconds:6.2f}s max"
        )


def main() -> None:
    """Walk G6solar's parent chains, verify each hop, and report."""
    repository = open_repository(DB_PATH)
    client = build_client(
        SETTINGS,
        retry=exponential_backoff(retries=4),
        map_fn=thread_pool_map(max_workers=8),
    )
    print(f"=== {USE_CASE} (walk the parent tree; no assumed chain) ===")
    print(f"db: {DB_PATH}   search source: {SEARCH_SOURCE}")

    search_started = time.perf_counter()
    if SEARCH_SOURCE == "api":
        records = _search_g6solar(client, repository)
    else:
        records = repository.get_dataset_records(USE_CASE)
    search_seconds = time.perf_counter() - search_started
    print(
        f"search stage ({SEARCH_SOURCE}): {len(records)} datasets "
        f"in {search_seconds:.2f}s"
    )
    if not records:
        print(f"No datasets for {USE_CASE!r}. Set SEARCH_SOURCE='api' to search live.")
        return
    children = sum(1 for r in records if r.experiment_id == CHILD_EXPERIMENT)
    print(f"loaded {len(records)} datasets ({children} in {CHILD_EXPERIMENT})")

    health = repository.load_node_health()
    read_timeout = health.suggested_timeout(default=READ_TIMEOUT_FALLBACK)
    print(f"  read timeout for this run: {read_timeout:.1f}s")

    # Roots are the G6solar children; stopping_experiment=None walks all the way to
    # the CMIP6 "no parent" sentinel (the true top), searching each parent once.
    roots = [r for r in records if r.experiment_id == CHILD_EXPERIMENT]
    started = time.perf_counter()
    try:
        result = resolve_parent_chains(
            roots,
            client=client,
            repository=repository,
            stopping_experiment=None,
            project=PROJECT,
            max_hops=MAX_HOPS,
            health=health,
            timeout=read_timeout,
            preferred_hosts=PREFERRED_HOSTS,
            ignore_hosts=IGNORE_HOSTS,
            max_workers=MAX_WORKERS,
            node_concurrency=NODE_CONCURRENCY,
            skip_cached=SKIP_CACHED,
            map_fn=thread_pool_map(max_workers=SEARCH_WORKERS),
        )
    except ParentResolutionError as exc:
        print(f"parent resolution FAILED:\n{exc}")
        print("node health:")
        _print_node_health(repository)
        return
    elapsed = time.perf_counter() - started

    print(
        f"walked {result.hops} hop(s): {len(result.links)} version links, "
        f"{len(result.terminals)} terminal(s) in {elapsed:.2f}s"
    )
    for child_version, parent_version in sorted(result.links):
        print(f"  {child_version}")
        print(f"    -> {parent_version}")
    if result.terminals:
        print("terminals (tops of the tree reached):")
        for source_id, experiment_id, variant_label in sorted(result.terminals):
            print(f"  {source_id} / {experiment_id} / {variant_label}")

    print("node health:")
    _print_node_health(repository)


if __name__ == "__main__":
    main()
