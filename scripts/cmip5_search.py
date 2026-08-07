"""
Search CMIP5 for monthly `ta` (air temperature) under `rcp45`, and cache it.

This is the CMIP5 counterpart of `scripts/esgf_search.py`: same workflow, but the
`mip_era="CMIP5"` client translates the canonical (CMIP6) vocabulary out to CMIP5's
native facet names (`source_id`->`model`, `variable_id`->`variable`, ...), expands each
multi-variable CMIP5 *table* dataset into one record per requested variable, and
rebuilds a CMIP6-shaped `master_id` from the columns.

It is deliberately pointed at a variable (`ta`) whose `rcp45` monthly data includes a
**product collision**: `CanCM4` publishes it under both `output` and `output1`.  Two
datasets that are otherwise identical therefore share one reconstructed base id; the
repository disambiguates the second with a `.N` suffix and records what differs, and
this script prints that choice at the end so you can see exactly how it is handled.

Run with: ``uv run python scripts/cmip5_search.py``
"""

from __future__ import annotations

from cmip_data_manager import (
    Settings,
    build_client,
    build_file_search_clients,
    open_repository,
)
from cmip_data_manager.config import ORNL_BASE_URL
from cmip_data_manager.db.repository import Repository
from cmip_data_manager.esgf.concurrency import exponential_backoff, thread_pool_map
from cmip_data_manager.esgf.query import FacetQuery
from cmip_data_manager.search import UseCase, add_files, run_use_case

# --- Configuration (edit me) -------------------------------------------------
DB_PATH = "cmip5_cache.sqlite"
"""Where to store the SQLite database."""

SOURCE = "api"
"""``"api"`` to query ESGF (and update the cache), ``"db"`` for offline."""

MIP_ERA = "CMIP5"
"""The MIP era this script searches; selects the CMIP5 facet-name vocabulary."""

# CEDA's esg-search currently 501s and metagrid-west is in maintenance, so Step 1 (index
# search) and Step 2 (file search) both use ORNL's MetaGrid proxy, which serves CMIP5.
ENDPOINT = ORNL_BASE_URL
SETTINGS = Settings(base_url=ENDPOINT)

VARIABLE = "tas"
"""Near-surface air temperature (a real CMIP5 monthly variable)."""

EXPERIMENT = "rcp45"
FREQUENCY = ("mon",)

ENABLE_FILE_SEARCH = True
"""Whether to run Step 2 (variable-scoped file search) after the index search.

Enabled here so the download step has `File`/`FileAccess` rows to fetch — for CMIP5
this searches by the native table `dataset_id` **plus** the variable, so only `tas`'s
files come back (not the whole table), and `number_of_files` becomes per-variable."""

FILE_SEARCH_WORKERS = 8
"""Parallel per-version file searches in the Step-2 `add_files` pass (HTTP I/O)."""
# -----------------------------------------------------------------------------


def rcp45_ta() -> UseCase:
    """
    All monthly `ta` datasets for `rcp45` (CMIP5 vocabulary, translated by the backend)

    Returns
    -------
    :
        The configured use case (a single query, no aggregation).
    """
    query = FacetQuery(
        mip_era=MIP_ERA,
        variable_id=(VARIABLE,),
        frequency=FREQUENCY,
        experiment_id=(EXPERIMENT,),
    )
    return UseCase(
        name="cmip5_rcp45_tas",
        build_queries=lambda _client: [query],
        aggregate=None,
        description="All monthly ta datasets for CMIP5 experiment rcp45.",
    )


def print_product_choices(repository: Repository) -> None:
    """Print any CMIP5 datasets that collided and were disambiguated by a facet."""
    choices = repository.cmip5_distinguishing_conflicts()
    print(f"\nproduct/collision choices: {len(choices)}")
    if not choices:
        print("  (no datasets needed disambiguation)")
        return
    print(
        "  each block is one simulation published under >1 product; pick the\n"
        "  master_id you want — same data under a different ESGF `product`:"
    )
    for choice in choices:
        print(f"\n  base: {choice.base_master_id}")
        for distinguishing, master_id in choice.options:
            print(f"    {distinguishing:<28} -> {master_id}")


def main() -> None:
    """Run the CMIP5 rcp45 `ta` search and report the result and product choices."""
    repository = open_repository(DB_PATH)
    client = build_client(
        SETTINGS,
        retry=exponential_backoff(retries=4),
        map_fn=thread_pool_map(max_workers=8),
        mip_era=MIP_ERA,
    )

    result = run_use_case(
        rcp45_ta(),
        repository,
        client=client if SOURCE == "api" else None,
        source=SOURCE,
    )

    print(f"\n=== {result.name} ===")
    print(f"datasets (per-variable): {len(result.records)}")
    if result.run is not None:
        run = result.run
        print(
            f"changes vs previous run: "
            f"+{len(run.added)} / -{len(run.removed)} / ~{len(run.modified)}"
        )

    print_product_choices(repository)

    if ENABLE_FILE_SEARCH and SOURCE == "api":
        files = add_files(
            result.records,
            clients=build_file_search_clients(
                (ENDPOINT,), settings=SETTINGS, mip_era=MIP_ERA
            ),
            repository=repository,
            map_fn=thread_pool_map(max_workers=FILE_SEARCH_WORKERS),
        )
        print(
            "\nfile search (variable-scoped): "
            f"searched={files.searched} skipped_cached={files.skipped_cached} "
            f"files_stored={files.files_stored} overflowed={len(files.overflowed)} "
            f"failed={len(files.failed)}"
        )


if __name__ == "__main__":
    main()
