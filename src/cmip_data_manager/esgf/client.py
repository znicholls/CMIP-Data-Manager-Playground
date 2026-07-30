"""
Synchronous client for the ESGF search API

The client is transport-agnostic: it is handed a `Fetch`, a `RetryPolicy` and a
`MapFn` (see `cmip_data_manager.esgf.concurrency`) so that retry/backoff and
parallelism are entirely the caller's choice.  It is also **dialect-agnostic**: a
`SearchBackend` (see `cmip_data_manager.esgf.backends`) does the dialect-specific
work of building requests, walking pages and parsing responses, so the same client
serves ESGF1 (esg-search / Solr) and, in future, ESGF-NG (STAC / CQL2).  Its jobs
are:

- drive the injected backend to turn a `FacetQuery` into paged requests and stitch
  the pages back together;
- guard (via the backend) against the API silently truncating deep result sets;
- fan several queries out through the injected `MapFn`.

`ESGFResponseError`, `DeepPaginationError` and the Solr JSON helpers
(`_response`, `_num_found`, `_docs`, `_facet_values`) are re-exported here for the
callers (and the async client) that import them from this module.
"""

from __future__ import annotations

from typing import Any

from cmip_data_manager.config import MAX_PAGE_SIZE, MAX_RETRIEVABLE
from cmip_data_manager.esgf.backends.base import (
    Cursor,
    DeepPaginationError,
    ESGFResponseError,
    Flavour,
    SearchBackend,
)
from cmip_data_manager.esgf.backends.esgf1 import (
    Esgf1Backend,
    _docs,
    _facet_values,
    _num_found,
    _response,
)
from cmip_data_manager.esgf.concurrency import (
    Fetch,
    MapFn,
    RetryPolicy,
    httpx_fetch,
    no_retry,
    serial_map,
)
from cmip_data_manager.esgf.models import DatasetRecord, FileRecord
from cmip_data_manager.esgf.query import FacetQuery

__all__ = [
    "DeepPaginationError",
    "ESGFResponseError",
    "ESGFSearchClient",
    "_docs",
    "_facet_values",
    "_num_found",
    "_response",
]


class ESGFSearchClient:
    """
    Paginating, transport-agnostic ESGF search client

    Examples
    --------
    Inject a fake transport so no network is needed:

    >>> def fake_fetch(url, params):
    ...     return {"response": {"numFound": 1, "docs": [{"id": "abc"}]}}
    >>> client = ESGFSearchClient("https://example/search", fetch=fake_fetch)
    >>> records = client.search(FacetQuery(variable_id=("tas",)))
    >>> [r.id for r in records]
    ['abc']
    """

    def __init__(  # noqa: PLR0913 - deliberately configurable DI seam
        self,
        base_url: str,
        *,
        backend: SearchBackend | None = None,
        fetch: Fetch | None = None,
        retry: RetryPolicy = no_retry,
        map_fn: MapFn = serial_map,
        page_size: int = 2_000,
        max_results: int = MAX_RETRIEVABLE,
        timeout: float = 60.0,
    ) -> None:
        """
        Initialise the client

        Parameters
        ----------
        base_url
            Search endpoint (e.g. the metagrid proxy or another mirror).

        backend
            The search dialect to speak (request building, paging, parsing).
            Defaults to `Esgf1Backend` (esg-search / Solr), preserving the historical
            behaviour; pass an ESGF-NG backend to talk to a STAC/CQL2 endpoint.

        fetch
            Transport used for a single request.  Defaults to an httpx-backed
            fetch built with `timeout`.

        retry
            Policy wrapping `fetch` with retry/backoff behaviour.

        map_fn
            Strategy for running several queries; defaults to serial.  Pass
            `thread_pool_map(...)` for parallelism.

        page_size
            Results requested per page, clamped to the API maximum.

        max_results
            Largest result set to page through before raising
            `DeepPaginationError` (the API cannot page beyond 10000).

        timeout
            Timeout used only when `fetch` is not supplied.
        """
        self._base_url = base_url
        self._backend: SearchBackend = (
            backend if backend is not None else Esgf1Backend()
        )
        base_fetch = fetch if fetch is not None else httpx_fetch(timeout=timeout)
        self._fetch = retry(base_fetch)
        self._map = map_fn
        self._page_size = min(page_size, MAX_PAGE_SIZE)
        self._max_results = max_results

    @property
    def base_url(self) -> str:
        """Search endpoint this client talks to."""
        return self._base_url

    @property
    def flavour(self) -> Flavour:
        """The search dialect (`Flavour`) this client's backend speaks."""
        return self._backend.flavour

    def count(self, query: FacetQuery) -> int:
        """
        Return the number of results a query matches without fetching them

        Parameters
        ----------
        query
            Query to count.

        Returns
        -------
        :
            The `numFound` reported by the API.
        """
        url, params = self._backend.count_request(self._base_url, query)
        payload = self._fetch(url, params)
        return self._backend.parse_count(payload)

    def facet_values(self, query: FacetQuery, facet_field: str) -> dict[str, int]:
        """
        Return the distinct values (and counts) of a facet for a query

        This is used to discover, for example, every `experiment_id` that exists,
        so a prefix requirement can be expanded into an exact list of experiments.
        Exact facet enumeration is used deliberately in preference to free-text
        wildcards, which tokenise on hyphens and mis-match names like `esm-hist`.

        Parameters
        ----------
        query
            Query describing the population to facet over.

        facet_field
            Field to facet on (e.g. `"experiment_id"`).

        Returns
        -------
        :
            Mapping of facet value to its count.
        """
        url, params = self._backend.facets_request(self._base_url, query, facet_field)
        payload = self._fetch(url, params)
        return self._backend.parse_facets(payload, facet_field)

    def _iter_docs(self, query: FacetQuery) -> list[dict[str, Any]]:
        """Page through `query` (via the backend's cursor) and return every raw doc."""
        docs: list[dict[str, Any]] = []
        cursor: Cursor | None = self._backend.start_cursor()
        while cursor is not None:
            url, params = self._backend.page_request(
                self._base_url, query, cursor=cursor, page_size=self._page_size
            )
            payload = self._fetch(url, params)
            page = self._backend.parse_page(
                payload, cursor=cursor, max_results=self._max_results
            )
            docs.extend(page.docs)
            cursor = page.next_cursor
        return docs

    def search(self, query: FacetQuery) -> list[DatasetRecord]:
        """
        Run a dataset query and return every matching dataset

        Parameters
        ----------
        query
            Query with `type="Dataset"` (the default).

        Returns
        -------
        :
            All matching datasets, across every page.
        """
        return [self._backend.parse_dataset(doc) for doc in self._iter_docs(query)]

    def search_files(self, query: FacetQuery) -> list[FileRecord]:
        """
        Run a file query and return every matching file

        Parameters
        ----------
        query
            Query with `type="File"`.

        Returns
        -------
        :
            All matching files, across every page.
        """
        return [self._backend.parse_file(doc) for doc in self._iter_docs(query)]

    def search_many(self, queries: list[FacetQuery]) -> list[list[DatasetRecord]]:
        """
        Run several dataset queries, using the injected `MapFn`

        Parameters
        ----------
        queries
            Queries to run.

        Returns
        -------
        :
            One result list per query, in the same order as `queries`.
        """
        return list(self._map(self.search, queries))
