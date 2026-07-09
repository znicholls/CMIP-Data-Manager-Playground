"""Tests for the use-case definitions and runner."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest

from cmip_data_manager.esgf.client import ESGFSearchClient
from cmip_data_manager.search import (
    discover_experiments,
    run_use_case,
    uc1_tas_ssp245,
    uc2_forcing,
    uc3_carbon,
    uc4_esm,
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


def test_uc1_uc2_query_shapes():
    client = _client()
    q1 = uc1_tas_ssp245().build_queries(client)
    assert len(q1) == 1
    assert uc1_tas_ssp245().aggregate is None

    uc2 = uc2_forcing()
    q2 = uc2.build_queries(client)
    # One query per (variable, experiment): 4 variables x 4 experiments.
    assert len(q2) == 16
    # Every query targets a single experiment (keeps under the 10000 cap).
    assert all(len(q.experiment_id) == 1 for q in q2)


def _experiments_used(queries):
    return {q.experiment_id[0] for q in queries}


def test_discover_experiments_filters_by_predicate():
    client = _client(["historical", "ssp126", "esm-ssp585", "esm-hist", "amip"])
    got = discover_experiments(
        client, lambda e: e == "historical" or e.startswith("ssp")
    )
    assert got == ("historical", "ssp126")  # esm-* and amip excluded, sorted


def test_uc3_discovers_ssp_but_not_esm():
    # esm-ssp585 must NOT leak in via a tokenised wildcard: exact enumeration.
    client = _client(["historical", "ssp245", "ssp126", "esm-ssp585", "esm-hist"])
    queries = uc3_carbon().build_queries(client)
    assert len(queries) == 15  # 5 variables x 3 experiments
    assert _experiments_used(queries) == {"historical", "ssp126", "ssp245"}
    assert all(q.query is None for q in queries)  # no free-text wildcard


def test_uc4_discovers_esm_only():
    client = _client(
        ["esm-hist", "esm-ssp585", "esm-ssp534-over", "ssp245", "hist-GHG"]
    )
    queries = uc4_esm().build_queries(client)
    assert len(queries) == 3  # tas x 3 esm experiments
    assert _experiments_used(queries) == {"esm-hist", "esm-ssp534-over", "esm-ssp585"}


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

    online = run_use_case(uc1_tas_ssp245(), repository, client=client, source="api")
    assert online.run is not None
    assert len(online.records) == 1
    assert online.matches is None  # uc1 has no aggregation

    offline = run_use_case(uc1_tas_ssp245(), repository, source="db")
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
    result = run_use_case(uc2_forcing(), repository, client=client, source="api")
    assert result.matches is not None
    assert len(result.matches) == 1
    assert result.matches[0].model_variant.source_id == "M"


def test_run_use_case_api_requires_client(repository):
    with pytest.raises(ValueError, match="client is required"):
        run_use_case(uc1_tas_ssp245(), repository, source="api")


def test_run_use_case_unknown_source(repository):
    with pytest.raises(ValueError, match="Unknown source"):
        run_use_case(uc1_tas_ssp245(), repository, source="nonsense")
