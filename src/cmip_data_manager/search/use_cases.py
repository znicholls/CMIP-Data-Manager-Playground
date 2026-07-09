"""
The four concrete search use cases, plus a runner that ties everything together

Each use case bundles a *query factory* (which the client can call to build the
queries, discovering experiments on the fly where a prefix is required) with the
aggregation to apply to their combined results.  `run_use_case` executes a use
case either against the live API (recording the run and its diff) or against the
local cache (offline, in which case the query factory is never called).

Prefix experiment requirements (`ssp*`, `esm-ssp*`) are expanded to an exact list
via `facet_values` rather than a free-text wildcard: the API tokenises free-text
`experiment_id` queries on hyphens, so `esm-hist` would wrongly match `hist-GHG`
and `ssp*` would wrongly match the `ssp585` inside `esm-ssp585`.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

from cmip_data_manager.db.repository import Repository, RunResult
from cmip_data_manager.esgf.client import ESGFSearchClient
from cmip_data_manager.esgf.models import DatasetRecord
from cmip_data_manager.esgf.query import FacetQuery
from cmip_data_manager.search.aggregate import (
    PairMatch,
    build_cells,
    pairs_all_experiments,
    pairs_any_experiment,
)

MONTHLY = ("mon",)
"""The `frequency` value ESGF uses for monthly data."""

Aggregator = Callable[[list[DatasetRecord]], list[PairMatch]]
QueryFactory = Callable[[ESGFSearchClient], list[FacetQuery]]
ExperimentPredicate = Callable[[str], bool]


@dataclass(frozen=True)
class UseCase:
    """A named use case: how to build its queries and how to aggregate them."""

    name: str
    build_queries: QueryFactory
    """Given a client, return the queries to run (may discover experiments)."""

    aggregate: Aggregator | None = None
    """Aggregation to apply, or `None` when the raw datasets are the answer."""

    description: str = ""


@dataclass(frozen=True)
class UseCaseResult:
    """The outcome of running a use case."""

    name: str
    records: list[DatasetRecord]
    run: RunResult | None
    """The recorded run (online) or `None` (offline)."""

    matches: list[PairMatch] | None = field(default=None)
    """Aggregated model-variant matches, or `None` if the use case has none."""


def _per_variable_experiment(
    project: str,
    variables: Sequence[str],
    experiments: Sequence[str],
) -> list[FacetQuery]:
    """
    Build one query per (variable, experiment) pair

    Splitting this finely keeps every query comfortably below the API's
    10000-result retrieval cap (a single variable across many experiments can
    exceed it), and lets the queries fan out in parallel.
    """
    return [
        FacetQuery(
            project=project,
            variable_id=(variable,),
            frequency=MONTHLY,
            experiment_id=(experiment,),
        )
        for variable in variables
        for experiment in experiments
    ]


def discover_experiments(
    client: ESGFSearchClient,
    predicate: ExperimentPredicate,
    *,
    project: str = "CMIP6",
    frequency: Sequence[str] = MONTHLY,
) -> tuple[str, ...]:
    """
    Return the exact `experiment_id`s matching a predicate

    Parameters
    ----------
    client
        Client to query facets with.

    predicate
        Returns `True` for experiment ids that should be included.

    project
        Project to search.

    frequency
        Frequencies to restrict the facet population to.

    Returns
    -------
    :
        The matching experiment ids, sorted.
    """
    query = FacetQuery(project=project, frequency=tuple(frequency))
    values = client.facet_values(query, "experiment_id")
    return tuple(sorted(name for name in values if predicate(name)))


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
        frequency=MONTHLY,
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
    queries = _per_variable_experiment(project, variables, experiments)

    def aggregate(records: list[DatasetRecord]) -> list[PairMatch]:
        return pairs_all_experiments(
            build_cells(records),
            required_vars=variables,
            required_experiments=("abrupt-4xCO2", "piControl"),
            optional_experiments=("abrupt-2xCO2", "abrupt-0p5xCO2"),
        )

    return UseCase(
        name="uc2_forcing",
        build_queries=lambda _client: queries,
        aggregate=aggregate,
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
            client, _is_historical_or_ssp, project=project
        )
        return _per_variable_experiment(project, variables, experiments)

    def aggregate(records: list[DatasetRecord]) -> list[PairMatch]:
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
            client, _is_esm_hist_or_esm_ssp, project=project
        )
        return _per_variable_experiment(project, ("tas",), experiments)

    def aggregate(records: list[DatasetRecord]) -> list[PairMatch]:
        return pairs_any_experiment(build_cells(records), required_vars=("tas",))

    return UseCase(
        name="uc4_esm",
        build_queries=build_queries,
        aggregate=aggregate,
        description="Model-variant pairs with tas in esm-hist or esm-ssp*.",
    )


def _dedupe(results: list[list[DatasetRecord]]) -> list[DatasetRecord]:
    """Flatten per-query results, de-duplicating by dataset id."""
    by_id: dict[str, DatasetRecord] = {}
    for result in results:
        for record in result:
            by_id[record.id] = record
    return list(by_id.values())


def fetch_records(use_case: UseCase, client: ESGFSearchClient) -> list[DatasetRecord]:
    """
    Run a use case's queries and return the de-duplicated datasets

    Parameters
    ----------
    use_case
        Use case whose queries to run.

    client
        Client to run them with (its injected `MapFn` controls parallelism).

    Returns
    -------
    :
        The combined datasets, de-duplicated by dataset id.
    """
    queries = use_case.build_queries(client)
    return _dedupe(client.search_many(queries))


def run_use_case(
    use_case: UseCase,
    repository: Repository,
    *,
    client: ESGFSearchClient | None = None,
    source: str = "api",
) -> UseCaseResult:
    """
    Execute a use case online (querying the API) or offline (from the cache)

    Parameters
    ----------
    use_case
        Use case to execute.

    repository
        Cache to read from and/or record into.

    client
        Search client, required when `source == "api"`.

    source
        `"api"` to query the live endpoint (recording the run and its diff) or
        `"db"` to reuse the datasets from the latest cached run.

    Returns
    -------
    :
        The datasets, the recorded run (online only) and the aggregated matches.

    Raises
    ------
    ValueError
        If `source` is not recognised, or `source == "api"` without a `client`.
    """
    if source == "api":
        if client is None:
            msg = "A client is required when source='api'."
            raise ValueError(msg)
        queries = use_case.build_queries(client)
        records = _dedupe(client.search_many(queries))
        run: RunResult | None = repository.record_run(
            use_case.name,
            records,
            endpoint_url=client.base_url,
            spec={"queries": [q.as_spec() for q in queries]},
        )
    elif source == "db":
        records = repository.get_dataset_records(use_case.name)
        run = None
    else:
        msg = f"Unknown source {source!r}; expected 'api' or 'db'."
        raise ValueError(msg)

    matches = use_case.aggregate(records) if use_case.aggregate is not None else None
    return UseCaseResult(name=use_case.name, records=records, run=run, matches=matches)
