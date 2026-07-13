"""
The engine for defining and running search use cases

A `UseCase` bundles a *query factory* (which the client can call to build the
queries, discovering experiments on the fly where a prefix is required) with the
aggregation to apply to their combined results.  `run_use_case` executes a use
case either against the live API (recording the run and its diff) or against the
local cache (offline, in which case the query factory is never called).

This module deliberately defines no concrete use cases: the searches a user cares
about are user-specific starting points, so they are assembled from these building
blocks in the caller (see `scripts/esgf_search.py`), not baked into the package.

The building blocks provided here are `per_variable_experiment` (fan a
(variable, experiment) requirement out into one query each) and
`discover_experiments` (expand a prefix requirement such as `ssp*` to an exact
list via `facet_values` rather than a free-text wildcard: the API tokenises
free-text `experiment_id` queries on hyphens, so `esm-hist` would wrongly match
`hist-GHG` and `ssp*` would wrongly match the `ssp585` inside `esm-ssp585`).
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field

from cmip_data_manager.db.repository import Repository, RunResult
from cmip_data_manager.esgf.client import ESGFSearchClient
from cmip_data_manager.esgf.concurrency import MapFn, serial_map
from cmip_data_manager.esgf.models import DatasetRecord
from cmip_data_manager.esgf.query import FacetQuery
from cmip_data_manager.search.aggregate import PairMatch, ParentResolver, build_cells
from cmip_data_manager.search.parentage import ParentConflict, resolve_parent_links

Aggregator = Callable[[list[DatasetRecord], "ParentResolver | None"], list[PairMatch]]
"""
Aggregate a run's datasets into model-variant matches

The second argument is an optional parent resolver (present online, `None`
offline); aggregators that do not need parent links simply ignore it.
"""

QueryFactory = Callable[[ESGFSearchClient], list[FacetQuery]]
ExperimentPredicate = Callable[[str], bool]


@dataclass(frozen=True)
class ParentSpec:
    """
    Declares that a use case satisfies some experiments through a parent chain

    When set on a `UseCase`, `run_use_case` resolves the parent links up front (in a
    batched, parallel pass) so the aggregator's `resolver` is a ready dict lookup.
    """

    via_parent: Mapping[str, str]
    """Maps a required experiment to the base experiment to walk its parent from."""

    required_vars: tuple[str, ...]
    """Variables a base experiment must contain for its cell to be worth walking."""


@dataclass(frozen=True)
class UseCase:
    """A named use case: how to build its queries and how to aggregate them."""

    name: str
    build_queries: QueryFactory
    """Given a client, return the queries to run (may discover experiments)."""

    aggregate: Aggregator | None = None
    """Aggregation to apply, or `None` when the raw datasets are the answer."""

    description: str = ""

    parent_spec: ParentSpec | None = None
    """If set, resolve parent links online and pass them to `aggregate`."""


@dataclass(frozen=True)
class UseCaseResult:
    """The outcome of running a use case."""

    name: str
    records: list[DatasetRecord]
    run: RunResult | None
    """The recorded run (online) or `None` (offline)."""

    matches: list[PairMatch] | None = field(default=None)
    """Aggregated model-variant matches, or `None` if the use case has none."""

    parent_conflicts: list[ParentConflict] = field(default_factory=list)
    """Cells whose files disagreed on their parent and were skipped (online only)."""


def per_variable_experiment(
    project: str,
    variables: Sequence[str],
    experiments: Sequence[str],
    frequency: Sequence[str] = (),
) -> list[FacetQuery]:
    """
    Build one query per (variable, experiment) pair

    Splitting this finely keeps every query comfortably below the API's
    10000-result retrieval cap (a single variable across many experiments can
    exceed it), and lets the queries fan out in parallel.

    Parameters
    ----------
    project
        Project to search.

    variables
        `variable_id` values; one query is emitted per variable.

    experiments
        `experiment_id` values; one query is emitted per experiment.

    frequency
        `frequency` values to restrict every query to (e.g. `("mon",)` for
        monthly).  Empty (the default) applies no frequency restriction.

    Returns
    -------
    :
        One `FacetQuery` per (variable, experiment) pair.
    """
    return [
        FacetQuery(
            project=project,
            variable_id=(variable,),
            frequency=tuple(frequency),
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
    frequency: Sequence[str] = (),
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
        `frequency` values to restrict the facet population to (e.g. `("mon",)`
        for monthly).  Empty (the default) applies no frequency restriction.

    Returns
    -------
    :
        The matching experiment ids, sorted.
    """
    query = FacetQuery(project=project, frequency=tuple(frequency))
    values = client.facet_values(query, "experiment_id")
    return tuple(sorted(name for name in values if predicate(name)))


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


def run_use_case(  # noqa: PLR0913 - deliberately configurable DI seam
    use_case: UseCase,
    repository: Repository,
    *,
    client: ESGFSearchClient | None = None,
    source: str = "api",
    parent_map_fn: MapFn = serial_map,
    ignore_hosts: frozenset[str] = frozenset(),
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

    parent_map_fn
        Strategy for reading netCDF headers during online parent resolution.
        Defaults to serial; pass `process_pool_map(...)` for parallel reads (netCDF
        is not thread-safe, so a process pool is required, not `thread_pool_map`).

    ignore_hosts
        Hostnames to never read parent headers from (e.g. dead data nodes).

    Returns
    -------
    :
        The datasets, the recorded run (online only) and the aggregated matches.
        Parent-aware matching only runs online (it reads netCDF headers via the
        `client`); offline aggregation falls back to direct coverage.

    Raises
    ------
    ValueError
        If `source` is not recognised, or `source == "api"` without a `client`.
    """
    conflicts: list[ParentConflict] = []
    resolver: ParentResolver | None = None
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
        if use_case.parent_spec is not None:
            links = resolve_parent_links(
                build_cells(records),
                via_parent=use_case.parent_spec.via_parent,
                required_vars=use_case.parent_spec.required_vars,
                records=records,
                client=client,
                read_map=parent_map_fn,
                ignore_hosts=ignore_hosts,
                conflicts=conflicts,
            )
            resolver = links.get
    elif source == "db":
        records = repository.get_dataset_records(use_case.name)
        run = None
    else:
        msg = f"Unknown source {source!r}; expected 'api' or 'db'."
        raise ValueError(msg)

    matches = (
        use_case.aggregate(records, resolver)
        if use_case.aggregate is not None
        else None
    )
    return UseCaseResult(
        name=use_case.name,
        records=records,
        run=run,
        matches=matches,
        parent_conflicts=conflicts,
    )
