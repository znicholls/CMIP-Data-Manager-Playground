"""Fixtures for the unit tests: in-memory fake transports and builders."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

import pytest

from cmip_data_manager.db.repository import Repository
from cmip_data_manager.factory import open_repository

SolrBuilder = Callable[..., dict[str, Any]]
Fetch = Callable[[str, Mapping[str, str]], dict[str, Any]]


@pytest.fixture
def repository(tmp_path) -> Repository:
    """Return a repository backed by a fresh temporary SQLite database."""
    return open_repository(tmp_path / "cache.sqlite")


@pytest.fixture
def solr_dataset() -> SolrBuilder:
    """Return a builder for raw Solr-style dataset documents."""

    def build(dataset_id: str, **fields: Any) -> dict[str, Any]:
        doc: dict[str, Any] = {"id": dataset_id}
        for key, value in fields.items():
            doc[key] = value if isinstance(value, list) else [value]
        return doc

    return build


@pytest.fixture
def paged_fetch() -> Callable[..., Fetch]:
    """
    Return a factory building a `Fetch` that pages through documents

    The factory takes `docs` and an optional `num_found`; setting `num_found`
    above `len(docs)` simulates a truncating or deep result set.
    """

    def factory(docs: list[dict[str, Any]], num_found: int | None = None) -> Fetch:
        total = len(docs) if num_found is None else num_found

        def fetch(_url: str, params: Mapping[str, str]) -> dict[str, Any]:
            offset = int(params["offset"])
            limit = int(params["limit"])
            return {
                "response": {"numFound": total, "docs": docs[offset : offset + limit]}
            }

        return fetch

    return factory


@pytest.fixture
def async_paged_fetch(
    paged_fetch: Callable[..., Fetch],
) -> Callable[..., Callable[..., Any]]:
    """Async counterpart of `paged_fetch`."""

    def factory(docs: list[dict[str, Any]], num_found: int | None = None):
        sync = paged_fetch(docs, num_found)

        async def fetch(url: str, params: Mapping[str, str]) -> dict[str, Any]:
            return sync(url, params)

        return fetch

    return factory
