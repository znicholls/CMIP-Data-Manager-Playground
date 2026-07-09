"""
Synchronous client for the ESGF search API

The client is transport-agnostic: it is handed a `Fetch`, a `RetryPolicy` and a
`MapFn` (see `cmip_data_manager.esgf.concurrency`) so that retry/backoff and
parallelism are entirely the caller's choice.  Its jobs are:

- turn a `FacetQuery` into paged requests and stitch the pages back together;
- guard against the API silently truncating deep result sets;
- fan several queries out through the injected `MapFn`.
"""

from __future__ import annotations

from typing import Any

from cmip_data_manager.config import MAX_PAGE_SIZE, MAX_RETRIEVABLE
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


class ESGFResponseError(RuntimeError):
    """Raised when a search response is not shaped like a Solr result."""


class DeepPaginationError(RuntimeError):
    """
    Raised when a result set is too large to page through safely

    ESGF/Globus Search reject or silently truncate very deep pagination, so
    rather than return a partial answer we stop and ask the caller to narrow the
    query.
    """

    def __init__(self, num_found: int, max_results: int) -> None:
        self.num_found = num_found
        self.max_results = max_results
        super().__init__(
            f"Search matched {num_found} results, which exceeds the maximum "
            f"retrievable for a single query ({max_results}); the API cannot page "
            f"beyond this. Narrow the query, e.g. split it by experiment_id or "
            f"source_id."
        )


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
        base_fetch = fetch if fetch is not None else httpx_fetch(timeout=timeout)
        self._fetch = retry(base_fetch)
        self._map = map_fn
        self._page_size = min(page_size, MAX_PAGE_SIZE)
        self._max_results = max_results

    @property
    def base_url(self) -> str:
        """Search endpoint this client talks to."""
        return self._base_url

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
        payload = self._fetch(self._base_url, query.to_params(offset=0, limit=0))
        return _num_found(payload)

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
        params = dict(query.to_params(offset=0, limit=0))
        params["facets"] = facet_field
        payload = self._fetch(self._base_url, params)
        return _facet_values(payload, facet_field)

    def _iter_docs(self, query: FacetQuery) -> list[dict[str, Any]]:
        """Page through `query` and return every raw document."""
        docs: list[dict[str, Any]] = []
        offset = 0
        num_found: int | None = None
        while True:
            payload = self._fetch(
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
                # The backend stopped returning results before we reached
                # num_found: this is the silent-truncation case we must surface.
                msg = (
                    f"Pagination stalled at offset {offset} with "
                    f"{num_found} results expected; the backend appears to have "
                    f"truncated the result set. Narrow the query."
                )
                raise ESGFResponseError(msg)
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
        return [DatasetRecord.from_solr(doc) for doc in self._iter_docs(query)]

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
        return [FileRecord.from_solr(doc) for doc in self._iter_docs(query)]

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


def _response(payload: dict[str, Any]) -> dict[str, Any]:
    """Return the `response` block, raising if the payload is not a result."""
    response = payload.get("response")
    if not isinstance(response, dict):
        msg = (
            "Unexpected search response: missing 'response' block. "
            f"Got keys: {sorted(payload)}"
        )
        raise ESGFResponseError(msg)
    return response


def _num_found(payload: dict[str, Any]) -> int:
    """Extract `numFound` from a search payload."""
    return int(_response(payload)["numFound"])


def _docs(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract the `docs` list from a search payload."""
    docs = _response(payload).get("docs", [])
    if not isinstance(docs, list):
        msg = f"Unexpected 'docs' type in response: {type(docs)!r}"
        raise ESGFResponseError(msg)
    return docs


def _facet_values(payload: dict[str, Any], facet_field: str) -> dict[str, int]:
    """Parse a Solr `facet_counts` block (`[value, count, value, count, ...]`)."""
    facet_fields = payload.get("facet_counts", {}).get("facet_fields", {})
    flat = facet_fields.get(facet_field, [])
    return {str(value): int(count) for value, count in zip(flat[0::2], flat[1::2])}
