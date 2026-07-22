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

from cmip_data_manager import (
    Settings,
    build_client,
    build_file_search_clients,
    open_repository,
)
from cmip_data_manager.esgf.client import ESGFSearchClient
from cmip_data_manager.esgf.concurrency import (
    exponential_backoff,
    process_pool_map,
    thread_pool_map,
)
from cmip_data_manager.esgf.models import DatasetRecord
from cmip_data_manager.esgf.query import FacetQuery
from cmip_data_manager.search import (
    PairMatch,
    ParentResolver,
    ParentSpec,
    UseCase,
    add_files,
    build_cells,
    discover_experiments,
    enrich_version_headers,
    pairs_all_experiments,
    pairs_any_experiment,
    per_variable_experiment,
    run_use_case,
)

# --- Configuration (edit me) -------------------------------------------------
DB_PATH = "esgf_cache.sqlite"
"""Where to store the SQLite database."""

SOURCE = "api"
"""``"api"`` to query ESGF (and update the cache), ``"db"`` for offline."""

SETTINGS = Settings()
"""Endpoint/paging settings; swap ``base_url`` here to use a different mirror."""

FREQUENCY = ("mon",)
"""The ``frequency`` these use cases search for (ESGF uses ``"mon"`` for monthly)."""

PARENT_READS = process_pool_map(max_workers=8)
"""How to read netCDF parent headers online: process pool (netCDF isn't thread-safe)."""

ENRICH_HEADERS = True
"""Whether to read + cache header metadata (global attrs) for use case 1's datasets."""

PREFERRED_HOSTS: tuple[str, ...] = ("esgf.nci.org.au",)
"""Data nodes to try first when reading headers (empty tuple = no preference)."""

HEADER_MAX_WORKERS = 12
"""Cap on header reads in flight across all data nodes (the shared local budget)."""

HEADER_NODE_CONCURRENCY = 2
"""Default cap on simultaneous reads to a single data node (conservative)."""

FILE_SEARCH_WORKERS = 8
"""Parallel per-version file searches in the Step-2 `add_files` pass (HTTP I/O)."""

IGNORE_HOSTS: frozenset[str] = frozenset(
    {
        # http-only nodes observed to stall byte-range reads (~75-100s each);
        # the same files are served over https by ornl/globus mirrors.
        "esgf-data02.diasjp.net",
        "esgf-data03.diasjp.net",
        "esgf-data04.diasjp.net",
        "esg.iap.ac.cn",
    }
)
"""Data-node hostnames to never read parent headers from (add slow/dead nodes here)."""


# --- Use cases (edit me: add, remove or tweak these) -------------------------
def uc1_tas_ssp245(project: str = "CMIP6") -> UseCase:
    """
    All monthly `tas` datasets for `ssp245`

    Parameters
    ----------
    project
        Project to search.

    Returns
    -------
    :
        The configured use case (a single query, no aggregation).
    """
    query = FacetQuery(
        project=project,
        variable_id=("tas",),
        frequency=FREQUENCY,
        experiment_id=("ssp245",),
    )
    return UseCase(
        name="uc1_tas_ssp245",
        build_queries=lambda _client: [query],
        aggregate=None,
        description="All monthly tas datasets for experiment ssp245.",
    )


def uc2_forcing(project: str = "CMIP6") -> UseCase:
    """
    Model variants with tas, rsdt, rlut and rsut in abrupt-4xCO2 and piControl

    Required variables must be present in *both* required experiments (strict
    cross-product).  `abrupt-2xCO2` and `abrupt-0p5xCO2` are recorded when present
    but are not required.

    Parameters
    ----------
    project
        Project to search.

    Returns
    -------
    :
        The configured use case.
    """
    variables = ("tas", "rsdt", "rlut", "rsut")
    experiments = ("abrupt-4xCO2", "piControl", "abrupt-2xCO2", "abrupt-0p5xCO2")
    queries = per_variable_experiment(
        project, variables, experiments, frequency=FREQUENCY
    )
    # piControl is legitimately a different variant (the parent), so fall back to the
    # parent chain from abrupt-4xCO2 when it isn't present under the same variant.
    via_parent = {"piControl": "abrupt-4xCO2"}

    def aggregate(
        records: list[DatasetRecord], resolver: ParentResolver | None
    ) -> list[PairMatch]:
        return pairs_all_experiments(
            build_cells(records),
            required_vars=variables,
            required_experiments=("abrupt-4xCO2", "piControl"),
            optional_experiments=("abrupt-2xCO2", "abrupt-0p5xCO2"),
            resolver=resolver,
            via_parent=via_parent,
        )

    return UseCase(
        name="uc2_forcing",
        build_queries=lambda _client: queries,
        aggregate=aggregate,
        parent_spec=ParentSpec(via_parent=via_parent, required_vars=variables),
        description=(
            "Model-variant pairs with tas, rsdt, rlut, rsut in both "
            "abrupt-4xCO2 and piControl (optionally abrupt-2xCO2/-0p5xCO2)."
        ),
    )


def _is_historical_or_ssp(experiment: str) -> bool:
    """Match `historical` or any experiment whose name starts with `ssp`."""
    return experiment == "historical" or experiment.startswith("ssp")


def _is_esm_hist_or_esm_ssp(experiment: str) -> bool:
    """Match `esm-hist` or any experiment whose name starts with `esm-ssp`."""
    return experiment == "esm-hist" or experiment.startswith("esm-ssp")


def uc3_carbon(project: str = "CMIP6") -> UseCase:
    """
    Model variants with tas, fgco2 and nbp (and optionally co2s/co2)

    The qualifying experiments are `historical` or any experiment starting with
    `ssp` (discovered exactly at run time); required variables must co-occur
    within a single such experiment.  The optional variable is `co2s`, falling
    back to `co2`.

    Parameters
    ----------
    project
        Project to search.

    Returns
    -------
    :
        The configured use case.
    """
    variables = ("tas", "fgco2", "nbp", "co2s", "co2")

    def build_queries(client: ESGFSearchClient) -> list[FacetQuery]:
        experiments = discover_experiments(
            client, _is_historical_or_ssp, project=project, frequency=FREQUENCY
        )
        return per_variable_experiment(
            project, variables, experiments, frequency=FREQUENCY
        )

    def aggregate(
        records: list[DatasetRecord], _resolver: ParentResolver | None
    ) -> list[PairMatch]:
        return pairs_any_experiment(
            build_cells(records),
            required_vars=("tas", "fgco2", "nbp"),
            optional_variable_preferences=("co2s", "co2"),
        )

    return UseCase(
        name="uc3_carbon",
        build_queries=build_queries,
        aggregate=aggregate,
        description=(
            "Model-variant pairs with tas, fgco2, nbp (optionally co2s/co2) in a "
            "single historical or ssp* experiment."
        ),
    )


def uc4_esm(project: str = "CMIP6") -> UseCase:
    """
    Model variants with tas in esm-hist or an esm-ssp* experiment

    The qualifying experiments (`esm-hist` or any starting with `esm-ssp`) are
    discovered exactly at run time.

    Parameters
    ----------
    project
        Project to search.

    Returns
    -------
    :
        The configured use case.
    """

    def build_queries(client: ESGFSearchClient) -> list[FacetQuery]:
        experiments = discover_experiments(
            client, _is_esm_hist_or_esm_ssp, project=project, frequency=FREQUENCY
        )
        return per_variable_experiment(
            project, ("tas",), experiments, frequency=FREQUENCY
        )

    def aggregate(
        records: list[DatasetRecord], _resolver: ParentResolver | None
    ) -> list[PairMatch]:
        return pairs_any_experiment(build_cells(records), required_vars=("tas",))

    return UseCase(
        name="uc4_esm",
        build_queries=build_queries,
        aggregate=aggregate,
        description="Model-variant pairs with tas in esm-hist or esm-ssp*.",
    )


def uc5_tas_ssp119_chain(project: str = "CMIP6") -> UseCase:
    """
    Model variants with monthly `tas` in `ssp119` and its historical/piControl parents

    Requires the strict scenario chain `ssp119 -> historical -> piControl`: every
    variant must have monthly `tas` in `ssp119`, in its parent `historical` run, and
    in that run's parent `piControl`.  `historical` and `piControl` are frequently
    the *same* variant as the child (matched directly, no header reads), but when a
    parent is a different variant it is reached by walking the netCDF parent chain
    upward from the `ssp119` cell — so the work stays bounded by the ssp119 variants,
    not the whole `historical` experiment.

    All three experiments are queried so that (a) each ancestor's coverage can be
    checked and (b) an ancestor's own header is available to read the next hop.

    Parameters
    ----------
    project
        Project to search.

    Returns
    -------
    :
        The configured use case.
    """
    variables = ("tas",)
    experiments = ("ssp119", "historical", "piControl")
    queries = per_variable_experiment(
        project, variables, experiments, frequency=FREQUENCY
    )
    # Anchor both parents on the ssp119 cell: walk its netCDF parent chain
    # (ssp119 -> historical -> piControl) so resolution stays bounded by the ssp119
    # variants we actually care about, rather than fanning out over every historical
    # run.  A parent under a different variant is still linked along that chain.
    via_parent = {"historical": "ssp119", "piControl": "ssp119"}

    def aggregate(
        records: list[DatasetRecord], resolver: ParentResolver | None
    ) -> list[PairMatch]:
        return pairs_all_experiments(
            build_cells(records),
            required_vars=variables,
            # Cheapest/base experiment first so an early miss short-circuits before
            # any parent (network) read.
            required_experiments=("ssp119", "historical", "piControl"),
            resolver=resolver,
            via_parent=via_parent,
        )

    return UseCase(
        name="uc5_tas_ssp119_chain",
        build_queries=lambda _client: queries,
        aggregate=aggregate,
        parent_spec=ParentSpec(via_parent=via_parent, required_vars=variables),
        description=(
            "Model-variant pairs with monthly tas in ssp119 and its historical and "
            "piControl parents (parents reached via the netCDF parent chain)."
        ),
    )


USE_CASES: list[UseCase] = [
    uc1_tas_ssp245(),
    uc2_forcing(),
    uc3_carbon(),
    uc4_esm(),
    uc5_tas_ssp119_chain(),
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
            parent_map_fn=PARENT_READS,
            ignore_hosts=IGNORE_HOSTS,
        )
        print(f"\n=== {result.name} ===")
        print(f"datasets: {len(result.records)}")
        if result.run is not None:
            run = result.run
            print(
                f"changes vs previous run: "
                f"+{len(run.added)} / -{len(run.removed)} / ~{len(run.modified)}"
            )
        if result.parent_conflicts:
            print(
                f"parent-metadata conflicts (skipped): {len(result.parent_conflicts)}"
            )
        if ENRICH_HEADERS and SOURCE == "api" and use_case.name == "uc1_tas_ssp245":
            # Step 2: store each version's files, then Step 3: read + promote headers.
            files = add_files(
                result.records,
                clients=build_file_search_clients(settings=SETTINGS),
                repository=repository,
                map_fn=thread_pool_map(max_workers=FILE_SEARCH_WORKERS),
            )
            print(
                "file search: "
                f"searched={files.searched} skipped_cached={files.skipped_cached} "
                f"files_stored={files.files_stored} overflowed={len(files.overflowed)} "
                f"failed={len(files.failed)}"
            )
            outcome = enrich_version_headers(
                result.records,
                repository=repository,
                preferred_hosts=PREFERRED_HOSTS,
                ignore_hosts=IGNORE_HOSTS,
                max_workers=HEADER_MAX_WORKERS,
                node_concurrency=HEADER_NODE_CONCURRENCY,
            )
            print(
                "header enrichment: "
                f"read={outcome.read} reused={outcome.reused} "
                f"promoted={outcome.promoted} skipped_cached={outcome.skipped_cached} "
                f"failed={len(outcome.failed)} no_files={len(outcome.no_files)}"
            )

        if result.matches is None:
            continue
        print(f"matching model-variant pairs: {len(result.matches)}")
        for match in result.matches:
            pair = match.model_variant
            extra = []
            if match.parent_experiments:
                extra.append(f"via-parent={','.join(match.parent_experiments)}")
            if match.optional_experiments:
                extra.append(f"opt-expts={','.join(match.optional_experiments)}")
            if match.optional_variables:
                extra.append(f"opt-vars={','.join(match.optional_variables)}")
            suffix = f" ({'; '.join(extra)})" if extra else ""
            print(f"  {pair.source_id} / {pair.variant_label}{suffix}")


if __name__ == "__main__":
    main()
