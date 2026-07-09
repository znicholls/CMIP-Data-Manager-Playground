"""Tests for the asynchronous search client (driven synchronously via asyncio)."""

from __future__ import annotations

import asyncio

import pytest

from cmip_data_manager.esgf.async_client import AsyncESGFSearchClient
from cmip_data_manager.esgf.client import DeepPaginationError
from cmip_data_manager.esgf.query import FacetQuery


def _query():
    return FacetQuery(variable_id=("tas",))


def test_async_pagination(solr_dataset, async_paged_fetch):
    docs = [solr_dataset(f"d{i}", variable_id="tas") for i in range(25)]
    client = AsyncESGFSearchClient(
        "https://example/search", fetch=async_paged_fetch(docs), page_size=10
    )
    records = asyncio.run(client.search(_query()))
    assert [r.id for r in records] == [f"d{i}" for i in range(25)]
    assert client.base_url == "https://example/search"


def test_async_search_many(solr_dataset, async_paged_fetch):
    docs = [solr_dataset("d0", variable_id="tas")]
    client = AsyncESGFSearchClient(
        "https://example/search", fetch=async_paged_fetch(docs)
    )
    results = asyncio.run(client.search_many([_query(), _query()]))
    assert len(results) == 2


def test_async_deep_pagination_guard(solr_dataset, async_paged_fetch):
    docs = [solr_dataset("d0")]
    client = AsyncESGFSearchClient(
        "https://example/search",
        fetch=async_paged_fetch(docs, num_found=1_000_000),
        max_results=100,
    )
    with pytest.raises(DeepPaginationError):
        asyncio.run(client.search(_query()))
