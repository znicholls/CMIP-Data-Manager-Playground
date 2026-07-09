"""Tests for the synchronous, paginating search client."""

from __future__ import annotations

import pytest

from cmip_data_manager.esgf.client import (
    DeepPaginationError,
    ESGFResponseError,
    ESGFSearchClient,
)
from cmip_data_manager.esgf.query import FacetQuery


def _query():
    return FacetQuery(variable_id=("tas",))


def test_pagination_stitches_pages(solr_dataset, paged_fetch):
    docs = [solr_dataset(f"d{i}", variable_id="tas") for i in range(25)]
    client = ESGFSearchClient(
        "https://example/search", fetch=paged_fetch(docs), page_size=10
    )
    records = client.search(_query())
    assert [r.id for r in records] == [f"d{i}" for i in range(25)]


def test_count_uses_num_found(solr_dataset, paged_fetch):
    docs = [solr_dataset(f"d{i}") for i in range(3)]
    client = ESGFSearchClient(
        "https://example/search", fetch=paged_fetch(docs, num_found=999)
    )
    assert client.count(_query()) == 999


def test_deep_pagination_guard(solr_dataset, paged_fetch):
    docs = [solr_dataset("d0")]
    client = ESGFSearchClient(
        "https://example/search",
        fetch=paged_fetch(docs, num_found=1_000_000),
        max_results=100,
    )
    with pytest.raises(DeepPaginationError) as excinfo:
        client.search(_query())
    assert excinfo.value.num_found == 1_000_000


def test_stall_is_detected(solr_dataset):
    # numFound claims 10 but the backend returns an empty page after the first.
    calls = {"n": 0}

    def fetch(url, params):
        calls["n"] += 1
        docs = [solr_dataset("d0")] if calls["n"] == 1 else []
        return {"response": {"numFound": 10, "docs": docs}}

    client = ESGFSearchClient("https://example/search", fetch=fetch, page_size=1)
    with pytest.raises(ESGFResponseError, match="stalled"):
        client.search(_query())


def test_malformed_response_raises():
    client = ESGFSearchClient(
        "https://example/search", fetch=lambda url, params: {"unexpected": True}
    )
    with pytest.raises(ESGFResponseError, match="response"):
        client.count(_query())


def test_search_files(solr_dataset, paged_fetch):
    docs = [
        {"id": "f0", "dataset_id": ["ds0"], "title": ["a.nc"]},
        {"id": "f1", "dataset_id": ["ds0"], "title": ["b.nc"]},
    ]
    client = ESGFSearchClient("https://example/search", fetch=paged_fetch(docs))
    files = client.search_files(FacetQuery(type="File", dataset_id=("ds0",)))
    assert [f.id for f in files] == ["f0", "f1"]


def test_search_many_runs_each_query(solr_dataset, paged_fetch):
    docs = [solr_dataset("d0", variable_id="tas")]
    client = ESGFSearchClient("https://example/search", fetch=paged_fetch(docs))
    results = client.search_many([_query(), _query()])
    assert len(results) == 2
    assert all(len(r) == 1 for r in results)


def test_base_url_property(paged_fetch):
    client = ESGFSearchClient("https://example/search", fetch=paged_fetch([]))
    assert client.base_url == "https://example/search"


def test_facet_values_parses_flat_list():
    def fetch(url, params):
        assert params["facets"] == "experiment_id"
        return {
            "facet_counts": {
                "facet_fields": {"experiment_id": ["ssp245", 1296, "historical", 2404]}
            }
        }

    client = ESGFSearchClient("https://example/search", fetch=fetch)
    values = client.facet_values(_query(), "experiment_id")
    assert values == {"ssp245": 1296, "historical": 2404}


def test_facet_values_missing_block_is_empty():
    client = ESGFSearchClient(
        "https://example/search", fetch=lambda url, params: {"response": {}}
    )
    assert client.facet_values(_query(), "experiment_id") == {}
