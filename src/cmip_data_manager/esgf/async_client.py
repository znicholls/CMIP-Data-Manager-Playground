"""
Asynchronous client for the ESGF search API

This mirrors `ESGFSearchClient` but uses `asyncio`, which is the natural fit when
many queries should be in flight at once.  Concurrency is bounded by a semaphore,
and the transport is injected as an `AsyncFetch` so tests (and alternative HTTP
stacks) need not touch the network.

**ESGF1 / CMIP6 only — not MIP-era aware.**  This client predates the
`SearchBackend` seam and bypasses it: it builds requests with `FacetQuery.to_params`
directly (so it emits **canonical CMIP6 facet names**, never renaming `source_id` →
`model` etc.) and parses with `DatasetRecord.from_solr` directly (so it does **no**
CMIP5 doc-expansion, name-canonicalisation or id reconstruction).  Pointed at CMIP5 it
would send wrong facet names and raise `AmbiguousFieldError` on a multi-variable table
doc.  Use the synchronous `ESGFSearchClient` (which routes everything through an
era-aware backend) for anything other than CMIP6; migrating this client onto the
backend seam is a separate, deferred task.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

import httpx

from cmip_data_manager.config import MAX_PAGE_SIZE, MAX_RETRIEVABLE
from cmip_data_manager.esgf.client import (
    DeepPaginationError,
    ESGFResponseError,
    _docs,
    _num_found,
)
from cmip_data_manager.esgf.models import DatasetRecord
from cmip_data_manager.esgf.query import FacetQuery

AsyncFetch = Callable[[str, Mapping[str, str]], Awaitable[dict[str, Any]]]
"""Async transport: `(url, params) -> awaitable parsed JSON`."""


def httpx_async_fetch(
    timeout: float = 60.0,
    client: httpx.AsyncClient | None = None,
) -> AsyncFetch:
    """
    Build an `AsyncFetch` backed by httpx

    Parameters
    ----------
    timeout
        Per-request timeout in seconds (ignored if `client` is given).

    client
        Optional pre-configured async client to reuse.  If provided, the caller
        owns its lifecycle.

    Returns
    -------
    :
        An async callable that GETs `url` with `params` and returns parsed JSON.
    """

    async def fetch(url: str, params: Mapping[str, str]) -> dict[str, Any]:
        if client is not None:
            response = await client.get(url, params=params)
            response.raise_for_status()
            data: dict[str, Any] = response.json()
            return data
        async with httpx.AsyncClient(timeout=timeout) as owned:
            response = await owned.get(url, params=params)
            response.raise_for_status()
            owned_data: dict[str, Any] = response.json()
            return owned_data

    return fetch


class AsyncESGFSearchClient:
    """
    Asynchronous, paginating ESGF search client

    Examples
    --------
    >>> import asyncio
    >>> async def fake_fetch(url, params):
    ...     return {"response": {"numFound": 1, "docs": [{"id": "abc"}]}}
    >>> async def main():
    ...     client = AsyncESGFSearchClient("https://example/search", fetch=fake_fetch)
    ...     records = await client.search(FacetQuery(variable_id=("tas",)))
    ...     return [r.id for r in records]
    >>> asyncio.run(main())
    ['abc']
    """

    def __init__(  # noqa: PLR0913 - deliberately configurable DI seam
        self,
        base_url: str,
        *,
        fetch: AsyncFetch | None = None,
        page_size: int = 2_000,
        max_results: int = MAX_RETRIEVABLE,
        max_concurrency: int = 8,
        timeout: float = 60.0,
    ) -> None:
        """
        Initialise the client

        Parameters
        ----------
        base_url
            Search endpoint.

        fetch
            Async transport for a single request.  Defaults to an httpx-backed
            fetch built with `timeout`.

        page_size
            Results requested per page, clamped to the API maximum.

        max_results
            Largest result set to page through before raising
            `DeepPaginationError` (the API cannot page beyond 10000).

        max_concurrency
            Maximum number of concurrent queries in `search_many`.

        timeout
            Timeout used only when `fetch` is not supplied.

        Notes
        -----
        Retry/backoff for the async client is expected to be layered onto the
        injected `fetch` (for example with a decorator), keeping this class free
        of a hard-coded policy.
        """
        self._base_url = base_url
        self._fetch = fetch if fetch is not None else httpx_async_fetch(timeout=timeout)
        self._page_size = min(page_size, MAX_PAGE_SIZE)
        self._max_results = max_results
        self._semaphore = asyncio.Semaphore(max_concurrency)

    @property
    def base_url(self) -> str:
        """Search endpoint this client talks to."""
        return self._base_url

    async def _iter_docs(self, query: FacetQuery) -> list[dict[str, Any]]:
        """Page through `query` and return every raw document."""
        docs: list[dict[str, Any]] = []
        offset = 0
        num_found: int | None = None
        while True:
            payload = await self._fetch(
                self._base_url, query.to_params(offset=offset, limit=self._page_size)
            )
            page = _docs(payload)
            if num_found is None:
                num_found = _num_found(payload)
                if num_found > self._max_results:
                    raise DeepPaginationError(num_found, self._max_results)
            docs.extend(page)
            offset += len(page)
            if offset >= num_found:
                break
            if not page:
                msg = (
                    f"Pagination stalled at offset {offset} with "
                    f"{num_found} results expected; the backend appears to have "
                    f"truncated the result set. Narrow the query."
                )
                raise ESGFResponseError(msg)
        return docs

    async def search(self, query: FacetQuery) -> list[DatasetRecord]:
        """
        Run a dataset query and return every matching dataset

        Parameters
        ----------
        query
            Query with `type="Dataset"`.

        Returns
        -------
        :
            All matching datasets, across every page.
        """
        async with self._semaphore:
            docs = await self._iter_docs(query)
        return [DatasetRecord.from_solr(doc) for doc in docs]

    async def search_many(self, queries: list[FacetQuery]) -> list[list[DatasetRecord]]:
        """
        Run several dataset queries concurrently

        Parameters
        ----------
        queries
            Queries to run.

        Returns
        -------
        :
            One result list per query, in the same order as `queries`.
        """
        return list(await asyncio.gather(*(self.search(q) for q in queries)))
