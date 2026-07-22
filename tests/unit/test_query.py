"""Tests for building ESGF query parameters."""

from __future__ import annotations

from cmip_data_manager.esgf.query import FacetQuery


def test_and_across_facets_or_within():
    query = FacetQuery(
        variable_id=("tas",),
        experiment_id=("ssp245", "historical"),
        frequency=("mon",),
    )
    params = query.to_params(offset=0, limit=10)
    # OR within a facet is comma-joined.
    assert params["experiment_id"] == "ssp245,historical"
    # Different facets appear as separate (AND-ed) parameters.
    assert params["variable_id"] == "tas"
    assert params["frequency"] == "mon"
    assert params["type"] == "Dataset"
    # `latest` is omitted by default, so the search returns all versions.
    assert "latest" not in params
    assert params["format"] == "application/solr+json"
    assert params["offset"] == "0"
    assert params["limit"] == "10"


def test_latest_default_omitted_and_explicit():
    # Default: no `latest` param -> all published versions returned.
    assert "latest" not in FacetQuery().to_params(0, 10)
    # Explicit True/False are still emitted.
    assert FacetQuery(latest=True).to_params(0, 10)["latest"] == "true"
    assert FacetQuery(latest=False).to_params(0, 10)["latest"] == "false"


def test_optional_flags_and_free_text():
    query = FacetQuery(
        variable_id=("tas",),
        query="experiment_id:ssp*",
        replica=False,
        distrib=True,
        latest=False,
        fields=("id", "source_id"),
    )
    params = query.to_params(offset=5, limit=100)
    assert params["query"] == "experiment_id:ssp*"
    assert params["replica"] == "false"
    assert params["distrib"] == "true"
    assert params["latest"] == "false"
    assert params["fields"] == "id,source_id"


def test_extra_facets_and_empty_facets_omitted():
    query = FacetQuery(
        source_id=("MODEL-A",),
        extra_facets={"grid_label": ("gn", "gr"), "realm": ()},
    )
    params = query.to_params(offset=0, limit=1)
    assert params["source_id"] == "MODEL-A"
    assert params["grid_label"] == "gn,gr"
    # Empty facets are not emitted at all.
    assert "realm" not in params
    assert "experiment_id" not in params


def test_as_spec_round_trip():
    query = FacetQuery(variable_id=("tas",), experiment_id=("ssp245",))
    spec = query.as_spec()
    assert spec["variable_id"] == ("tas",)
    assert spec["experiment_id"] == ("ssp245",)
