"""
Time one use case's phases against the live ESGF API, to find the bottleneck.

This runs the same steps `run_use_case` does, but calls the exported building
blocks directly so each phase can be timed separately:

1. **build_queries** — assemble the queries (may discover experiments live),
2. **search** — query ESGF for the datasets (parallel, retry/backoff),
3. **record_run** — write the run to the local SQLite cache,
4. **resolve parents** — read netCDF parent headers for near-miss cells,
5. **aggregate** — intersect the cells into model-variant matches (pure CPU).

Phase 4 (the netCDF byte-range reads) is the usual hold-up; the breakdown shows
by how much.  Configuration is reused from `esgf_search.py`.

Run with: ``uv run python scripts/time_use_case.py``
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager

from esgf_search import (
    DB_PATH,
    IGNORE_HOSTS,
    PARENT_READS,
    SETTINGS,
    uc5_tas_ssp119_chain,
)

from cmip_data_manager import build_client, open_repository
from cmip_data_manager.esgf.concurrency import exponential_backoff, thread_pool_map
from cmip_data_manager.esgf.models import DatasetRecord
from cmip_data_manager.search import (
    ParentConflict,
    build_cells,
    resolve_parent_links,
)

# --- Configuration (edit me) -------------------------------------------------
USE_CASE = uc5_tas_ssp119_chain()
"""The use case to time.  Swap in any factory from `esgf_search.py`."""


@contextmanager
def timed(label: str, timings: dict[str, float]) -> Iterator[None]:
    """Time the wrapped block, recording and printing its wall-clock seconds."""
    start = time.perf_counter()
    yield
    elapsed = time.perf_counter() - start
    timings[label] = elapsed
    print(f"  {label:<16} {elapsed:8.2f}s")


def _dedupe(results: list[list[DatasetRecord]]) -> list[DatasetRecord]:
    """Flatten per-query results, de-duplicating by dataset id."""
    by_id: dict[str, DatasetRecord] = {}
    for result in results:
        for record in result:
            by_id[record.id] = record
    return list(by_id.values())


def main() -> None:
    """Run the configured use case phase by phase and print a timing breakdown."""
    repository = open_repository(DB_PATH)
    client = build_client(
        SETTINGS,
        retry=exponential_backoff(retries=4),
        map_fn=thread_pool_map(max_workers=8),
    )

    timings: dict[str, float] = {}
    print(f"=== timing {USE_CASE.name} ===")

    with timed("build_queries", timings):
        queries = USE_CASE.build_queries(client)

    with timed("search", timings):
        records = _dedupe(client.search_many(queries))

    with timed("record_run", timings):
        repository.record_run(
            records,
            endpoint_url=client.base_url,
            spec={"queries": [q.as_spec() for q in queries]},
            tag=USE_CASE.name,
        )

    conflicts: list[ParentConflict] = []
    links: dict[tuple[str, str, str], tuple[str, str, str]] = {}
    if USE_CASE.parent_spec is not None:
        with timed("resolve_parents", timings):
            links = resolve_parent_links(
                build_cells(records),
                via_parent=USE_CASE.parent_spec.via_parent,
                required_vars=USE_CASE.parent_spec.required_vars,
                records=records,
                client=client,
                read_map=PARENT_READS,
                ignore_hosts=IGNORE_HOSTS,
                conflicts=conflicts,
            )

    matches = None
    if USE_CASE.aggregate is not None:
        with timed("aggregate", timings):
            matches = USE_CASE.aggregate(records, links.get)

    print("\n--- summary ---")
    print(f"queries:            {len(queries)}")
    print(f"datasets:           {len(records)}")
    print(f"parent links:       {len(links)}")
    print(f"parent conflicts:   {len(conflicts)}")
    if matches is not None:
        print(f"matching pairs:     {len(matches)}")
        via_parent = sum(1 for m in matches if m.parent_experiments)
        print(f"  (satisfied via a parent link: {via_parent})")
    print(f"total:              {sum(timings.values()):.2f}s")


if __name__ == "__main__":
    main()
