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
`DataNodeHealthStat`, all in `esgf_cache.sqlite`.

Run with: `uv run python scripts/enrich_uc2_headers.py`
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
from cmip_data_manager.config import CEDA_BASE_URL, DEFAULT_INDEX_ENDPOINTS
from cmip_data_manager.esgf.concurrency import exponential_backoff, thread_pool_map
from cmip_data_manager.esgf.preflight import ProbeCache
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
DB_PATH = os.environ.get("UC2_DB_PATH", "uc2_gregory.sqlite")
"""Cache to use.  Defaults to a fresh `uc2_gregory.sqlite` so the run is genuinely
**cold** — no node/index health learned from earlier live tests, nothing cached.
Delete the file (or override `UC2_DB_PATH`) to redo a cold run from scratch."""

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

SEARCH_EXPERIMENTS: tuple[str, ...] = CHILD_EXPERIMENTS
"""Experiments the from-scratch index search covers: just the abrupt-forcing children
(the roots).  The `piControl` parents are **not** searched up front — the walk
discovers each child's actual parent simulation itself, one index search per distinct
parent, so a broad piControl search here would be wasted work."""

FREQUENCY = ("mon",)
"""Frequency the from-scratch index search covers."""

PREFERRED_HOSTS: tuple[str, ...] = ("esgf.nci.org.au",)
"""Data nodes to try first when reading headers (empty tuple = no preference)."""

IGNORE_HOSTS: frozenset[str] = frozenset()
"""Data nodes to never read headers from (unioned with what NodeHealth has learned).

COLD-RUN POLICY: kept **empty** for this cold run — every node gets a fresh chance and
none is pre-excluded (a node that stalled before may have fixed its https).  Cost
accepted: a genuinely-dead node may eat a stall before the connect probe falls back."""

PREFLIGHT_PROBE = True
"""Sweep every candidate data node with a cheap chunk probe before reading headers,
dropping the ones that fail it into `ignore_hosts` for this session (`esgf.preflight`).

On this **cold** run it front-loads dead-node discovery, so the walk never wastes a full
read (or a stall) on a black hole; on a **warm** run it re-confirms *today's* reality
over stale learned health (a node dead last week but alive now is re-probed and used).
One `ProbeCache` is shared across every hop, so each node is probed at most once —
parents on data nodes the roots never touched are probed only when they appear."""

FORCE_ALIVE_HOSTS: frozenset[str] = frozenset()
"""Data nodes to keep in play even if the probe judges them dead (user override)."""

SKIP_CACHED = True
"""Skip simulations whose header is already cached (don't re-read a known header).

Header-only metadata is **independent of variable**, so this reuse is at the
simulation `(source_id, experiment_id, variant_label)` grain: once *any* variable of a
simulation has had its header read (even in an earlier run — e.g. a `tas` run before
this `rsut` one), the stored header is copied onto the new versions instead of
re-reading a data node, and its file search is skipped too (files are only substrate
for the read)."""

PARENT_OVERRIDES: dict[tuple[str, str, str], tuple[str, str, str]] = {
    # Correct a wrong/missing declared parent that the ESGF metadata gets wrong.  Keys
    # and values are simulation tuples (source_id, experiment_id, variant_label); an
    # entry takes precedence over the child's header.  Example (delete or edit):
    #   ("CanESM5", "abrupt-4xCO2", "r1i1p1f2"): ("CanESM5", "piControl", "r1i1p1f1"),
    # Workflow: on a `ParentResolutionError` (a declared parent absent from the index),
    # add the correct parent here and re-run — the walk resumes with the fix applied.
}
"""User overrides for wrong/missing child->parent links (empty = trust the headers)."""

MAX_WORKERS = 12
"""Cap on header reads in flight across all nodes (the shared local budget)."""

NODE_CONCURRENCY = 2
"""Default cap on simultaneous reads to a single data node (conservative)."""

SEARCH_WORKERS = 8
"""Parallel per-parent index searches and per-version file searches in the walk."""

READ_TIMEOUT_FALLBACK = 75.0
"""Read (post-connect) deadline used on a cold DB with no learned health yet.

This bounds the **read once connected**, not reaching the node: a black-holed
connect is already short-circuited by the connect probe's separate 90s
`connect_timeout`, so this no longer has to double as a dead-node detector.  That
frees it to fit a **slow-but-alive** mirror.  netCDF/HDF5 reads a header in ~30 small
byte-range requests, and on a high-latency node each pays ~1.5s of server round-trip
time, so a *genuine* header read can legitimately take 40-60s.  Measured (2026-07-28):
`vesg.ipsl.upmc.fr` read a real header in 39.8s and `esg-dn1.nsc.liu.se` in 58.8s —
both falsely condemned as `read-stall` by the old 25s cap even though the files read
(and downloaded) fine.  75s clears those while still cutting off a true post-connect
stall, and because *our* kill raises `HeaderReadTimeout` (on the give-up list) the read
is **not** retried 3x."""

FILE_SEARCH_ENDPOINTS: tuple[str, ...] = DEFAULT_INDEX_ENDPOINTS
"""Preference-ordered index endpoints for each hop's Step-2 file search (CEDA, then
ORNL, then metagrid-west).  `resolve_parent_chains` hands these to `add_files`, which
tries each in turn (backoff -> requeue -> next endpoint) — the same fallback uc1 uses.
Without this the walk would search files on a single endpoint with no fallback."""

SETTINGS = Settings(base_url=CEDA_BASE_URL)
"""Endpoint/paging settings.  `base_url` drives the Step-1 index search and the walk's
per-parent index searches; pointed at CEDA today because metagrid-west (the default) is
under maintenance.  The Step-2 file search uses `FILE_SEARCH_ENDPOINTS` instead."""
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
    # The endpoint-fallback list drives BOTH each hop's Step-2 file search AND each
    # hop's per-parent dataset search, so a piControl variant published on ORNL but
    # not CEDA is still found (uc1 parity for files; new for parent search).
    search_clients = build_file_search_clients(FILE_SEARCH_ENDPOINTS, settings=SETTINGS)
    print(f"  search endpoints (in order): {list(FILE_SEARCH_ENDPOINTS)}")
    print(f"  pre-flight node probe: {'on' if PREFLIGHT_PROBE else 'off'}")
    roots = [r for r in records if r.experiment_id in set(CHILD_EXPERIMENTS)]
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
            stopping_experiment=PARENT_EXPERIMENT,
            # Variable-scoped parent search: the per-parent index query carries the
            # required variables (an OR), so the walk only follows those lineages and
            # the file search never fans across every variable.
            variables=REQUIRED_VARS,
            parent_overrides=PARENT_OVERRIDES,
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
