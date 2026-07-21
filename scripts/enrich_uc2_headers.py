"""
Live header enrichment for use case 2 (the Gregory / forcing case).

This is use case 1's header workflow taken one hop up the parent tree.  Starting
from use case 2's cached datasets (the abrupt-forcing experiments `abrupt-4xCO2`,
`abrupt-2xCO2`, `abrupt-0p5xCO2` and `piControl`, for `tas`, `rsdt`, `rsut`,
`rlut`), it:

1. reads one header per *abrupt* simulation (single variable read; the header is
   stored on the `File` and its `parent_*` subset promoted onto each
   `DatasetVersion`) — the same health-aware, timeout/retry pipeline use case 1 uses;
2. follows each abrupt run's declared `parent_*` metadata up to its `piControl`
   parent via one index search per distinct parent, **without** assuming the parent
   shares the child's `variant_label`;
3. links each child *version* to the matching parent *version*
   (`parent_version_key`), stopping at `piControl` (the `stopping_experiment`);
4. reports the resolved version links, the terminals, and node health.

Any chain that cannot be resolved (a declared parent absent from the index, or a
chain that reaches the top without passing through `piControl`) raises a
`ParentResolutionError`, which this script catches and prints.  Headers are stored
on `File.header_attrs_json`, promoted onto `DatasetVersion`, and node statistics on
`NodeHealthStat`, all in `esgf_cache.sqlite`.

Run with: `uv run python scripts/enrich_uc2_headers.py`
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

DEFAULT_CHILD_EXPERIMENTS: tuple[str, ...] = (
    "abrupt-4xCO2",
    "abrupt-2xCO2",
    "abrupt-0p5xCO2",
)
"""Abrupt-forcing experiments whose piControl parent this use case resolves."""

DEFAULT_PARENT_EXPERIMENT = "piControl"
"""The experiment the children are expected to branch from."""

DEFAULT_REQUIRED_VARS: tuple[str, ...] = ("tas", "rsdt", "rsut", "rlut")
"""Variables the forcing (Gregory) calculation needs; scopes the index search."""

# --- Configuration (edit me) -------------------------------------------------
DB_PATH = os.environ.get("UC2_DB_PATH", "esgf_cache.sqlite")
"""Cache to use.  Defaults to `esgf_cache.sqlite`; override with `UC2_DB_PATH`
(e.g. point it at a fresh file for a cold, from-scratch run)."""

USE_CASE = "uc2_forcing"
"""The cached use case whose datasets to enrich (and record a fresh run under)."""

SEARCH_SOURCE = "api"
"""`"api"` to run the use case 2 index search live from scratch (and cache it), or
`"db"` to reuse the datasets from the latest cached run (`scripts/esgf_search.py`)."""

PROJECT = "CMIP6"
"""Project to search when `SEARCH_SOURCE == "api"`."""

CHILD_EXPERIMENTS = DEFAULT_CHILD_EXPERIMENTS
"""Abrupt-forcing experiments to resolve piControl parents for."""

PARENT_EXPERIMENT = DEFAULT_PARENT_EXPERIMENT
"""The experiment the children branch from."""

REQUIRED_VARS = DEFAULT_REQUIRED_VARS
"""Variables the forcing calculation needs (used to scope a parent re-fetch)."""

SEARCH_EXPERIMENTS: tuple[str, ...] = (*CHILD_EXPERIMENTS, PARENT_EXPERIMENT)
"""Experiments the from-scratch index search covers (children plus piControl)."""

FREQUENCY = ("mon",)
"""Frequency the from-scratch index search covers."""

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
"""Endpoint/paging settings for the live file lookups and parent re-fetches."""
# -----------------------------------------------------------------------------


def _search_uc2(client, repository):
    """Run the use case 2 index search live from scratch and cache the datasets."""
    queries = per_variable_experiment(
        PROJECT, REQUIRED_VARS, SEARCH_EXPERIMENTS, frequency=FREQUENCY
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
    """Enrich use case 2's headers, resolve piControl parents, and report."""
    repository = open_repository(DB_PATH)
    client = build_client(
        SETTINGS,
        retry=exponential_backoff(retries=4),
        map_fn=thread_pool_map(max_workers=8),
    )
    print(f"=== {USE_CASE} (Gregory / forcing; one hop to {PARENT_EXPERIMENT}) ===")
    print(f"search source: {SEARCH_SOURCE}")

    search_started = time.perf_counter()
    if SEARCH_SOURCE == "api":
        records = _search_uc2(client, repository)
    else:
        records = repository.get_dataset_records(USE_CASE)
    search_seconds = time.perf_counter() - search_started
    print(
        f"search stage ({SEARCH_SOURCE}): {len(records)} datasets "
        f"in {search_seconds:.2f}s"
    )
    if not records:
        print(
            f"No datasets for {USE_CASE!r}. "
            "Set SEARCH_SOURCE='api', or run scripts/esgf_search.py first."
        )
        return
    children = sum(1 for r in records if r.experiment_id in set(CHILD_EXPERIMENTS))
    print(f"loaded {len(records)} datasets ({children} in {list(CHILD_EXPERIMENTS)})")

    health = repository.load_node_health()
    read_timeout = health.suggested_timeout(default=READ_TIMEOUT_FALLBACK)
    print(f"  read timeout for this run: {read_timeout:.1f}s")

    # Roots are the abrupt-forcing children; the walk discovers piControl parents
    # itself (one index search per distinct parent) and stops at PARENT_EXPERIMENT.
    roots = [r for r in records if r.experiment_id in set(CHILD_EXPERIMENTS)]
    started = time.perf_counter()
    try:
        result = resolve_parent_chains(
            roots,
            client=client,
            repository=repository,
            stopping_experiment=PARENT_EXPERIMENT,
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
        f"resolved {len(result.links)} version links across "
        f"{result.hops} hop(s), {len(result.terminals)} terminal(s) "
        f"in {elapsed:.2f}s"
    )
    for child_version, parent_version in sorted(result.links):
        print(f"  {child_version}")
        print(f"    -> {parent_version}")
    if result.terminals:
        print("terminals (chain stops):")
        for source_id, experiment_id, variant_label in sorted(result.terminals):
            print(f"  {source_id} / {experiment_id} / {variant_label}")

    print("node health:")
    _print_node_health(repository)


if __name__ == "__main__":
    main()
