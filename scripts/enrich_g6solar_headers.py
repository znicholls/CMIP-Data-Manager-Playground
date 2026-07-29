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
statistics on `DataNodeHealthStat`.

Run with: `uv run python scripts/enrich_g6solar_headers.py`
"""

from __future__ import annotations

import os
import time

from cmip_data_manager import (
    Settings,
    build_client,
    build_file_search_clients,
    open_repository,
)
from cmip_data_manager.config import CEDA_BASE_URL
from cmip_data_manager.esgf.concurrency import exponential_backoff, thread_pool_map
from cmip_data_manager.esgf.preflight import ProbeCache
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

STOPPING_EXPERIMENT: str | None = "piControl"
"""Experiment the walk stops at.  `"piControl"` here: confirm it exists on the index
node, walk each chain up, and stop the moment a parent lands on `piControl` (its header
is not read, the walk does not go higher).  `None` would instead walk to the CMIP6
`"no parent"` sentinel (the true top)."""

REQUIRED_VARS: tuple[str, ...] = ("tas",)
"""Variable to read a header for (single variable, as requested)."""

FREQUENCY = ("mon",)
"""Frequency the from-scratch index search covers."""

MAX_HOPS = 8
"""Safety cap on the number of hops up the tree (guards a metadata cycle)."""

PREFERRED_HOSTS: tuple[str, ...] = ("esgf.nci.org.au",)
"""Data nodes to try first when reading headers (empty tuple = no preference)."""

IGNORE_HOSTS: frozenset[str] = frozenset()
"""Data nodes to never read headers from.

COLD-RUN POLICY: keep this **empty**.  A cold run learns node health from scratch and
must give *every* node a fresh chance — never pre-exclude one.  Only a **warm** run
should populate an ignore list (from learned health), and even then we prefer re-probing
"dead" nodes in case they have recovered (a node that stalled last week may have fixed
its https).  Cost accepted: a cold run may eat a ~75-100s stall on a genuinely-dead node
before falling back to a healthy mirror.

Known http-only stallers observed historically (byte-range reads stall ~75-100s each;
the same files are served over https elsewhere) — candidates for a *warm-run* ignore
list only, deliberately NOT wired in here: `esgf-data02.diasjp.net`,
`esgf-data03.diasjp.net`, `esgf-data04.diasjp.net`, `esg.iap.ac.cn`."""

PREFLIGHT_PROBE = True
"""Sweep every candidate data node with a cheap chunk probe before reading headers,
dropping the ones that fail it into `ignore_hosts` for this session (`esgf.preflight`).

This is the payoff for a **multi-hop** cold run like G6solar: instead of eating a
~75-100s stall on each historically-dead node (the diasjp/iap stallers above) the moment
the walk routes a read there, one cheap parallel sweep condemns them up front — and a
`ProbeCache` shared across every hop means a node the deeper `ssp585 -> historical ->
piControl` parents introduce is probed only when it first appears."""

FORCE_ALIVE_HOSTS: frozenset[str] = frozenset()
"""Data nodes to keep in play even if the probe judges them dead (user override)."""

SKIP_CACHED = True
"""Skip simulations whose header is already cached (don't re-read a known header).

Reuse is at the simulation `(source_id, experiment_id, variant_label)` grain (header
metadata is variable-independent): a header stored by any variable/earlier run is
copied onto new versions instead of re-reading, and its file search is skipped (files
are only substrate for the read)."""

PARENT_OVERRIDES: dict[tuple[str, str, str], tuple[str, str, str]] = {
    # Correct a wrong/missing declared parent in the ESGF metadata.  Keys/values are
    # (source_id, experiment_id, variant_label) tuples; an entry overrides the header.
    # On a `ParentResolutionError`, add the correct parent here and re-run.
}
"""User overrides for wrong/missing child->parent links (empty = trust the headers)."""

MAX_WORKERS = 12
"""Cap on header reads in flight across all nodes (the shared local budget)."""

NODE_CONCURRENCY = 2
"""Default cap on simultaneous reads to a single data node (conservative)."""

SEARCH_WORKERS = 8
"""Parallel per-parent index searches and per-version file searches in the walk."""

READ_TIMEOUT_FALLBACK = 90.0
"""Stall timeout used on a cold DB with no learned health yet."""

SETTINGS = Settings(base_url=CEDA_BASE_URL)
"""Endpoint/paging settings.  `base_url` drives the Step-1 G6solar index search and the
`piControl` existence gate; pointed at CEDA today because metagrid-west is in
maintenance.  The parent walk's per-hop file and per-parent dataset searches use the
preference-ordered `SEARCH_CLIENTS` (CEDA -> ORNL -> metagrid-west) instead."""
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


def _print_probe(probe_cache: ProbeCache) -> None:
    """Print the pre-flight probe's verdict for every data node it touched."""
    outcomes = probe_cache.outcomes()
    if not outcomes:
        print("  (no nodes probed)")
        return
    alive = sorted(probe_cache.alive_hosts())
    dead = sorted(probe_cache.dead_hosts())
    print(f"  {len(alive)} alive, {len(dead)} dead (session verdict):")
    for host in dead:
        outcome = outcomes[host]
        print(f"    DEAD  {host:<40} {outcome.reason}")
    for host in alive:
        outcome = outcomes[host]
        forced = " (forced)" if outcome.attempts == 0 else ""
        print(f"    alive {host:<40} {outcome.seconds:5.1f}s chunk{forced}")


def main() -> None:
    """Walk G6solar's parent chains, verify each hop, and report."""
    repository = open_repository(DB_PATH)
    client = build_client(
        SETTINGS,
        retry=exponential_backoff(retries=4),
        map_fn=thread_pool_map(max_workers=8),
    )
    # Preference-ordered endpoints (CEDA -> ORNL -> metagrid-west) that drive both the
    # per-hop Step-2 file search and each per-parent dataset search in the walk, with
    # backoff -> requeue -> next-endpoint fallback.
    search_clients = build_file_search_clients(settings=SETTINGS)
    endpoints = ", ".join(c.base_url for c in search_clients)
    print(f"=== {USE_CASE} (walk the parent tree; stop at {STOPPING_EXPERIMENT}) ===")
    print(f"db: {DB_PATH}   search source: {SEARCH_SOURCE}")
    print(f"parent search endpoints (in order): {endpoints}")

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

    # Roots are the G6solar children; the walk climbs each chain and stops the moment a
    # parent lands on STOPPING_EXPERIMENT ("piControl"), searching each parent once.
    roots = [r for r in records if r.experiment_id == CHILD_EXPERIMENT]
    print(f"  pre-flight node probe: {'on' if PREFLIGHT_PROBE else 'off'}")
    # One probe cache shared across every hop, so each data node is probed at most once;
    # kept here so its verdicts can be reported after the walk (or on a failure).
    probe_cache = ProbeCache()
    started = time.perf_counter()
    try:
        result = resolve_parent_chains(
            roots,
            client=client,
            search_clients=search_clients,
            repository=repository,
            stopping_experiment=STOPPING_EXPERIMENT,
            project=PROJECT,
            variables=REQUIRED_VARS,
            parent_overrides=PARENT_OVERRIDES,
            max_hops=MAX_HOPS,
            health=health,
            timeout=read_timeout,
            preferred_hosts=PREFERRED_HOSTS,
            ignore_hosts=IGNORE_HOSTS,
            max_workers=MAX_WORKERS,
            node_concurrency=NODE_CONCURRENCY,
            skip_cached=SKIP_CACHED,
            map_fn=thread_pool_map(max_workers=SEARCH_WORKERS),
            preflight_probe=PREFLIGHT_PROBE,
            force_alive_hosts=FORCE_ALIVE_HOSTS,
            probe_cache=probe_cache,
        )
    except ParentResolutionError as exc:
        print(f"parent resolution FAILED:\n{exc}")
        print("pre-flight probe:")
        _print_probe(probe_cache)
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
    if result.variable_gaps:
        # The parent simulation exists (found for some variable), it just does not
        # publish the requested variable here — a lineage hole, not a broken chain.
        print("variable gaps (parent exists but lacks the requested variable):")
        for gap in sorted(result.variable_gaps, key=str):
            print(f"  {gap}")

    print("pre-flight probe:")
    _print_probe(probe_cache)
    print("node health:")
    _print_node_health(repository)


if __name__ == "__main__":
    main()
