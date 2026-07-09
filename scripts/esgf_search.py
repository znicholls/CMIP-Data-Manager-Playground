"""
Run the ESGF search use cases and cache the results.

Configuration is hard-coded below on purpose: this script is an example of using
the `cmip_data_manager` Python API, not a general-purpose CLI.  Copy and tweak it.

What it does, for every use case:

1. queries the live ESGF API (in parallel, with retry/backoff),
2. stores the results in a local SQLite database,
3. records the diff against the previous run of that use case,
4. prints the matching model-variant pairs (or dataset count for use case 1).

Re-run it to update the cache: the diff shows what changed.  Set ``SOURCE = "db"``
to re-run the aggregation purely from the cache, with no network access.

Run with: ``uv run python scripts/esgf_search.py``
"""

from __future__ import annotations

from cmip_data_manager import Settings, build_client, open_repository
from cmip_data_manager.esgf.concurrency import exponential_backoff, thread_pool_map
from cmip_data_manager.search import (
    UseCase,
    run_use_case,
    uc1_tas_ssp245,
    uc2_forcing,
    uc3_carbon,
    uc4_esm,
)

# --- Configuration (edit me) -------------------------------------------------
DB_PATH = "esgf_cache.sqlite"
"""Where to store the SQLite database."""

SOURCE = "api"
"""``"api"`` to query ESGF (and update the cache), ``"db"`` for offline."""

SETTINGS = Settings()
"""Endpoint/paging settings; swap ``base_url`` here to use a different mirror."""

USE_CASES: list[UseCase] = [
    uc1_tas_ssp245(),
    uc2_forcing(),
    uc3_carbon(),
    uc4_esm(),
]
# -----------------------------------------------------------------------------


def main() -> None:
    """Run every configured use case and report the results."""
    repository = open_repository(DB_PATH)
    client = build_client(
        SETTINGS,
        retry=exponential_backoff(retries=4),
        map_fn=thread_pool_map(max_workers=8),
    )

    for use_case in USE_CASES:
        result = run_use_case(
            use_case,
            repository,
            client=client if SOURCE == "api" else None,
            source=SOURCE,
        )
        print(f"\n=== {result.name} ===")
        print(f"datasets: {len(result.records)}")
        if result.run is not None:
            run = result.run
            print(
                f"changes vs previous run: "
                f"+{len(run.added)} / -{len(run.removed)} / ~{len(run.modified)}"
            )
        if result.matches is None:
            continue
        print(f"matching model-variant pairs: {len(result.matches)}")
        for match in result.matches:
            pair = match.model_variant
            extra = []
            if match.optional_experiments:
                extra.append(f"opt-expts={','.join(match.optional_experiments)}")
            if match.optional_variables:
                extra.append(f"opt-vars={','.join(match.optional_variables)}")
            suffix = f" ({'; '.join(extra)})" if extra else ""
            print(f"  {pair.source_id} / {pair.variant_label}{suffix}")


if __name__ == "__main__":
    main()
