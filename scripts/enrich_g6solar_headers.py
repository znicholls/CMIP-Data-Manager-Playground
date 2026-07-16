"""
Live, multi-hop header enrichment for G6solar (the walk-the-parent-tree case).

This takes use case 2's one-hop idea and follows the parent chain *all the way up*,
making no assumptions about which experiments it passes through.  Starting from
`G6solar` / `tas` / `mon` alone, it:

1. reads and stores one header per `G6solar` simulation (the same health-aware,
   timeout/retry pipeline use cases 1 and 2 use);
2. follows whatever parent each header declares (`parent_*` metadata) — typically
   `G6solar -> ssp585 -> historical -> piControl`, but the experiments and variants
   are discovered, never assumed — reading and verifying each parent's header;
3. re-fetches an ancestor that is not already among the datasets (here, essentially
   every one, since the search covers only `G6solar`);
4. de-duplicates ancestors shared across branches, so a scenario/historical/
   piControl run reached by many children is read once;
5. reports each child's resolved chain, where and why any chain stopped short, the
   de-duplication win, and node health.

By default this runs **cold**: a fresh database (`g6solar_cache.sqlite`) with no
learned node health, so timeouts fall back to `READ_TIMEOUT_FALLBACK`.  Header rows
land in `DatasetHeader` (with populated `parent_*` columns) and node statistics in
`NodeHealthStat`.

Run with: `uv run python scripts/enrich_g6solar_headers.py`
"""

from __future__ import annotations

import os
import time
from collections import Counter

from cmip_data_manager import Settings, build_client, open_repository
from cmip_data_manager.esgf.concurrency import exponential_backoff, thread_pool_map
from cmip_data_manager.search import enrich_parent_chains, per_variable_experiment

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
"""Frequency to search for and to scope an ancestor re-fetch to."""

MAX_HOPS = 8
"""Safety cap on the number of hops up the tree (guards a metadata cycle)."""

REFETCH_MISSING = True
"""Re-fetch a declared ancestor that is not among the cached datasets."""

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

NODE_CONCURRENCY_OVERRIDES: dict[str, int] = {"esgf.nci.org.au": 4}
"""Per-node caps overriding `NODE_CONCURRENCY` (NCI tolerates more, and is fast)."""

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
        USE_CASE,
        records,
        endpoint_url=client.base_url,
        spec={"queries": [q.as_spec() for q in queries]},
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


def _chain_line(chain) -> str:
    """Render one resolved chain as `root -> parent -> ... [flags]`."""
    src, exp, var = chain.root
    parts = [f"{src} {exp}/{var}"]
    for edge in chain.edges:
        _p_src, p_exp, p_var = edge.parent
        flags = []
        if not edge.same_variant:
            flags.append("diff-variant")
        if edge.refetched:
            flags.append("re-fetched")
        suffix = f" [{','.join(flags)}]" if flags else ""
        parts.append(f"{p_exp}/{p_var}{suffix}")
    tail = "" if chain.complete else f"  (stopped: {chain.terminal_reason})"
    return " -> ".join(parts) + tail


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

    started = time.perf_counter()
    result = enrich_parent_chains(
        records,
        client=client,
        repository=repository,
        child_experiments=(CHILD_EXPERIMENT,),
        required_vars=REQUIRED_VARS,
        frequency=FREQUENCY,
        project=PROJECT,
        refetch_missing=REFETCH_MISSING,
        max_hops=MAX_HOPS,
        health=health,
        timeout=read_timeout,
        preferred_hosts=PREFERRED_HOSTS,
        ignore_hosts=IGNORE_HOSTS,
        max_workers=MAX_WORKERS,
        node_concurrency=NODE_CONCURRENCY,
        node_concurrency_overrides=NODE_CONCURRENCY_OVERRIDES,
        skip_cached=SKIP_CACHED,
    )
    elapsed = time.perf_counter() - started

    # Per-hop enrichment summary.
    for hop, enrichment in enumerate(result.enrichment, start=1):
        print(
            f"hop {hop}: read={enrichment.read} reused={enrichment.reused} "
            f"stored={enrichment.stored} skipped_cached={enrichment.skipped_cached} "
            f"failed={len(enrichment.failed)}"
        )
    if result.refetched:
        print(f"re-fetched {len(result.refetched)} datasets for absent ancestors")
    print(f"walked {len(result.chains)} chains in {result.hops} hops ({elapsed:.2f}s)")

    # The chains themselves.
    complete = result.complete
    print(f"chains ({len(complete)}/{len(result.chains)} reached the top of the tree):")
    for chain in result.chains:
        print(f"  {_chain_line(chain)}")

    # Summary: distinct simulations read per experiment (the dedup grain).
    per_experiment: Counter[str] = Counter()
    for chain in result.chains:
        per_experiment[chain.root[1]] += 1
    seen: set = set()
    for chain in result.chains:
        for edge in chain.edges:
            if edge.parent not in seen:
                seen.add(edge.parent)
                per_experiment[edge.parent[1]] += 1
    total_edges = sum(len(chain.edges) for chain in result.chains)
    print("summary:")
    print(f"  simulations touched by experiment: {dict(per_experiment)}")
    print(
        f"  headers read: {result.reads}  (vs {total_edges} chain edges = "
        "the shared-ancestor dedup win)"
    )
    stopped = Counter(
        chain.terminal_reason for chain in result.chains if not chain.complete
    )
    if stopped:
        print(f"  chains that stopped short, by reason: {dict(stopped)}")

    print("node health:")
    _print_node_health(repository)


if __name__ == "__main__":
    main()
