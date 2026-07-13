"""Tests for the use-case engine: query building, discovery and the runner."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest

from cmip_data_manager.esgf.client import ESGFSearchClient
from cmip_data_manager.esgf.models import DatasetRecord
from cmip_data_manager.esgf.query import FacetQuery
from cmip_data_manager.search import (
    PairMatch,
    UseCase,
    build_cells,
    discover_experiments,
    fetch_records,
    pairs_all_experiments,
    per_variable_experiment,
    run_use_case,
)


def _facet_fetch(experiments: list[str], docs: list[dict[str, Any]] | None = None):
    """A fetch returning facet counts for facet requests, else paged docs."""
    docs = docs or []

    def fetch(_url: str, params: Mapping[str, str]) -> dict[str, Any]:
        if "facets" in params:
            flat: list[Any] = []
            for name in experiments:
                flat.extend([name, 1])
            return {"facet_counts": {"facet_fields": {"experiment_id": flat}}}
        offset = int(params["offset"])
        limit = int(params["limit"])
        return {
            "response": {"numFound": len(docs), "docs": docs[offset : offset + limit]}
        }

    return fetch


def _client(experiments=None, docs=None):
    return ESGFSearchClient(
        "https://example/search", fetch=_facet_fetch(experiments or [], docs)
    )


def _single_query_use_case() -> UseCase:
    """A dummy use case: one query, no aggregation."""
    query = FacetQuery(
        variable_id=("tas",), experiment_id=("ssp245",), frequency=("mon",)
    )
    return UseCase(name="dummy_single", build_queries=lambda _client: [query])


def _aggregating_use_case() -> UseCase:
    """A dummy use case whose datasets are aggregated into model-variant pairs."""
    variables = ("tas", "rsdt", "rlut", "rsut")
    experiments = ("abrupt-4xCO2", "piControl")
    queries = per_variable_experiment(
        "CMIP6", variables, experiments, frequency=("mon",)
    )

    def aggregate(records: list[DatasetRecord], _resolver: object) -> list[PairMatch]:
        return pairs_all_experiments(
            build_cells(records),
            required_vars=variables,
            required_experiments=experiments,
        )

    return UseCase(
        name="dummy_agg", build_queries=lambda _client: queries, aggregate=aggregate
    )


def test_per_variable_experiment_builds_one_query_per_pair():
    queries = per_variable_experiment(
        "CMIP6", ("tas", "fgco2"), ("historical", "ssp245"), frequency=("mon",)
    )
    # One query per (variable, experiment): 2 x 2.
    assert len(queries) == 4
    # Each query targets a single facet value (keeps under the 10000 cap).
    assert all(len(q.variable_id) == 1 for q in queries)
    assert all(len(q.experiment_id) == 1 for q in queries)
    assert all(q.frequency == ("mon",) for q in queries)
    # Exact facets, never a tokenisable free-text wildcard.
    assert all(q.query is None for q in queries)


def test_per_variable_experiment_frequency_is_optional():
    queries = per_variable_experiment("CMIP6", ("tas",), ("ssp245",))
    assert queries[0].frequency == ()


def test_discover_experiments_filters_by_predicate():
    client = _client(["historical", "ssp126", "esm-ssp585", "esm-hist", "amip"])
    got = discover_experiments(
        client, lambda e: e == "historical" or e.startswith("ssp")
    )
    assert got == ("historical", "ssp126")  # esm-* and amip excluded, sorted


def test_fetch_records_dedupes_across_queries(solr_dataset):
    docs = [solr_dataset("d0", variable_id="tas", frequency="mon")]
    client = _client(docs=docs)
    two_queries = UseCase(
        name="dup",
        build_queries=lambda _client: [
            FacetQuery(variable_id=("tas",)),
            FacetQuery(variable_id=("tas",)),
        ],
    )
    records = fetch_records(two_queries, client)
    assert [r.id for r in records] == ["d0"]  # same id from both queries, deduped


def test_run_use_case_api_then_offline(repository, solr_dataset):
    docs = [
        solr_dataset(
            "d0",
            source_id="M",
            variant_label="r1",
            experiment_id="ssp245",
            variable_id="tas",
            frequency="mon",
        )
    ]
    client = _client(docs=docs)

    online = run_use_case(_single_query_use_case(), repository, client=client)
    assert online.run is not None
    assert len(online.records) == 1
    assert online.matches is None  # no aggregation

    offline = run_use_case(_single_query_use_case(), repository, source="db")
    assert offline.run is None
    assert len(offline.records) == 1


def test_run_use_case_with_aggregation(repository, solr_dataset):
    needed = ["tas", "rsdt", "rlut", "rsut"]
    docs = [
        solr_dataset(
            f"{var}.{exp}",
            source_id="M",
            variant_label="r1",
            experiment_id=exp,
            variable_id=var,
            frequency="mon",
        )
        for exp in ("abrupt-4xCO2", "piControl")
        for var in needed
    ]
    client = _client(docs=docs)
    result = run_use_case(_aggregating_use_case(), repository, client=client)
    assert result.matches is not None
    assert len(result.matches) == 1
    assert result.matches[0].model_variant.source_id == "M"


def test_run_use_case_api_requires_client(repository):
    with pytest.raises(ValueError, match="client is required"):
        run_use_case(_single_query_use_case(), repository, source="api")


def test_run_use_case_unknown_source(repository):
    with pytest.raises(ValueError, match="Unknown source"):
        run_use_case(_single_query_use_case(), repository, source="nonsense")
