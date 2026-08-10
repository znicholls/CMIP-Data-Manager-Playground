"""
Search CMIP7 (ESGF-NG / STAC) for a variable and cache it — the CMIP7 counterpart.

This mirrors `scripts/cmip5_search.py`, but CMIP7 is served **only** over ESGF-NG
(STAC/CQL2), so the `mip_era="CMIP7"` client resolves a STAC backend from the endpoint
and:

- speaks the **CMIP6-identical** facet vocabulary (CMIP7's STAC properties use the same
  names under a `cmip7:` collection prefix, which the backend strips), so no field-name
  translation is needed;
- folds CMIP7's **branding suffix** (`tavg-h2m-hxy-u`, which replaces the CMOR
  `table_id`) into the `table_id` column and promotes the rest of the branded-variable
  facets (region, sampling labels, licence, …) to a `Cmip7VersionExtra` side row;
- reads each dataset's files from the STAC item's **assets** in-process — there is no
  network file search on ESGF-NG — via `add_files_auto` (Step 2 collapses to a
  transform).

It is deliberately pointed at **both** NG deployments in preference order (east, then
west): it `count`s each so you can see what each holds (today only east has CMIP7 data;
west returns `0` under its lower-cased `cmip7` collection), then runs the full search
against the first endpoint that has data.  Nothing is hard-coded per collection — the
`project="CMIP7"` on the query drives the collection selector, and the west backend
lower-cases it automatically.

Run with: ``uv run python scripts/cmip7_search.py``
"""

from __future__ import annotations

from cmip_data_manager import (
    Settings,
    build_client,
    open_repository,
)
from cmip_data_manager.config import EAST_BASE_URL, WEST_BASE_URL
from cmip_data_manager.esgf.concurrency import exponential_backoff, thread_pool_map
from cmip_data_manager.esgf.query import FacetQuery
from cmip_data_manager.search import UseCase, add_files_auto, run_use_case

# --- Configuration (edit me) -------------------------------------------------
DB_PATH = "cmip7_cache.sqlite"
"""Where to store the SQLite database."""

SOURCE = "api"
"""``"api"`` to query ESGF (and update the cache), ``"db"`` for offline."""

MIP_ERA = "CMIP7"
"""The MIP era this script searches; selects the CMIP7 (ESGF-NG) profile."""

# CMIP7 is ESGF-NG-first; try east (has data today) then west (currently empty).  Both
# are STAC endpoints — the flavour is auto-detected from the URL.
NG_ENDPOINTS = (EAST_BASE_URL, WEST_BASE_URL)
"""ESGF-NG search endpoints in preference order for the CMIP7 Step-1 search."""

VARIABLE = "tas"
"""Near-surface air temperature (the first published CMIP7 variable)."""

EXPERIMENT = "piControl"
"""The experiment to search (the first CMIP7 data published is CanESM5-1 piControl)."""

FREQUENCY: tuple[str, ...] = ("mon",)

ENABLE_FILE_SEARCH = True
"""Whether to run Step 2 (assets transform) so the download step has File rows."""
# -----------------------------------------------------------------------------


def cmip7_use_case() -> UseCase:
    """
    All monthly `tas` CMIP7 datasets for the configured experiment

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
        name="cmip7_picontrol_tas",
        build_queries=lambda _client: [query],
        aggregate=None,
        description=f"All monthly {VARIABLE} CMIP7 datasets for {EXPERIMENT}.",
    )


def pick_endpoint() -> str | None:
    """
    Count the use-case query on each NG endpoint and return the first with data

    Prints what each endpoint reports so you can see the east/west split (and the
    east↔west result-envelope-key difference the backend already handles) live.
    """
    query = FacetQuery(
        mip_era=MIP_ERA,
        variable_id=(VARIABLE,),
        frequency=FREQUENCY,
        experiment_id=(EXPERIMENT,),
    )
    chosen: str | None = None
    for endpoint in NG_ENDPOINTS:
        client = build_client(Settings(base_url=endpoint), mip_era=MIP_ERA)
        try:
            n = client.count(query)
        except Exception as exc:  # a probe: report the failure and keep going
            print(f"  {endpoint:<40} error: {type(exc).__name__}: {exc}")
            continue
        print(f"  {endpoint:<40} count={n}")
        if n > 0 and chosen is None:
            chosen = endpoint
    return chosen


def main() -> None:
    """Run the CMIP7 `tas` search against whichever NG endpoint has data; cache it."""
    repository = open_repository(DB_PATH)
    print(f"=== CMIP7 search: {VARIABLE} / {EXPERIMENT} ===")

    if SOURCE == "api":
        print("probing NG endpoints:")
        endpoint = pick_endpoint()
        if endpoint is None:
            print("no NG endpoint reported CMIP7 data for this query; nothing to do.")
            return
        print(f"using endpoint: {endpoint}")
        client = build_client(
            Settings(base_url=endpoint),
            retry=exponential_backoff(retries=4),
            map_fn=thread_pool_map(max_workers=8),
            mip_era=MIP_ERA,
        )
    else:
        client = None

    result = run_use_case(
        cmip7_use_case(),
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
    for record in result.records:
        print(
            f"  {record.source_id} / {record.experiment_id} / {record.variant_label} "
            f"/ {record.variable_id} [{record.table_id}] v{record.version}"
        )

    if ENABLE_FILE_SEARCH and SOURCE == "api":
        files = add_files_auto(result.records, repository=repository)
        print(
            "\nfiles (from STAC assets): "
            f"versions={files.searched} skipped_cached={files.skipped_cached} "
            f"files_stored={files.files_stored}"
        )


if __name__ == "__main__":
    main()
