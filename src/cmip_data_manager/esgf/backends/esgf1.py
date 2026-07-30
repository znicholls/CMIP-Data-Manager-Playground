"""
The ESGF1 backend: esg-search / Apache Solr

This is the current, unchanged behaviour, lifted out of `ESGFSearchClient` behind
the `SearchBackend` seam: flat facet params in, Solr JSON out.  A query pages with
an integer `offset` cursor, the total comes from `numFound`, and results beyond
`offset >= max_results` cannot be reached (the esg-search / Globus Search deep-page
wall), which is why `parse_page` raises `DeepPaginationError` instead of returning a
truncated set.  A page that comes back empty *before* `numFound` is reached is the
silent-truncation case, surfaced as an `ESGFResponseError`.

The `_response` / `_num_found` / `_docs` / `_facet_values` helpers stay module-level
(and are re-exported from `cmip_data_manager.esgf.client` for the async client and
existing callers) — they are the Solr JSON parsers, useful anywhere a raw esg-search
payload is handled.
"""

from __future__ import annotations

from typing import Any

from cmip_data_manager.esgf.backends.base import (
    Cursor,
    DeepPaginationError,
    ESGFResponseError,
    Flavour,
    Page,
)
from cmip_data_manager.esgf.models import DatasetRecord, FileRecord
from cmip_data_manager.esgf.query import FacetQuery


class Esgf1Backend:
    """
    esg-search / Solr dialect (the default backend)

    Examples
    --------
    >>> backend = Esgf1Backend()
    >>> url, params = backend.page_request(
    ...     "https://example/search",
    ...     FacetQuery(variable_id=("tas",)),
    ...     cursor=0,
    ...     page_size=10,
    ... )
    >>> url, params["variable_id"], params["offset"], params["limit"]
    ('https://example/search', 'tas', '0', '10')
    >>> page = backend.parse_page(
    ...     {"response": {"numFound": 1, "docs": [{"id": "abc"}]}},
    ...     cursor=0,
    ...     max_results=10,
    ... )
    >>> page.num_found, page.next_cursor, backend.parse_dataset(page.docs[0]).id
    (1, None, 'abc')
    """

    flavour: Flavour = Flavour.ESGF1

    def start_cursor(self) -> Cursor:
        """Return the first-page offset (`0`)."""
        return 0

    def page_request(
        self, base_url: str, query: FacetQuery, *, cursor: Cursor, page_size: int
    ) -> tuple[str, dict[str, str]]:
        """Render the page at `offset=cursor` as `(base_url, esg-search params)`."""
        offset = _as_offset(cursor)
        return base_url, query.to_params(offset=offset, limit=page_size)

    def parse_page(
        self, payload: dict[str, Any], *, cursor: Cursor, max_results: int
    ) -> Page:
        """
        Parse a Solr page, advancing the offset cursor and guarding pagination

        `DeepPaginationError` is raised when the total exceeds `max_results` (the API
        cannot page that far); an empty page before `numFound` is reached is the
        silent-truncation case and raises `ESGFResponseError`.
        """
        offset = _as_offset(cursor)
        num_found = _num_found(payload)
        if num_found > max_results:
            raise DeepPaginationError(num_found, max_results)
        docs = _docs(payload)
        next_offset = offset + len(docs)
        if next_offset >= num_found:
            return Page(docs=docs, num_found=num_found, next_cursor=None)
        if not docs:
            msg = (
                f"Pagination stalled at offset {next_offset} with "
                f"{num_found} results expected; the backend appears to have "
                f"truncated the result set. Narrow the query."
            )
            raise ESGFResponseError(msg)
        return Page(docs=docs, num_found=num_found, next_cursor=next_offset)

    def parse_dataset(self, doc: dict[str, Any]) -> DatasetRecord:
        """Build a `DatasetRecord` from a raw Solr dataset document."""
        return DatasetRecord.from_solr(doc)

    def parse_file(self, doc: dict[str, Any]) -> FileRecord:
        """Build a `FileRecord` from a raw Solr file document."""
        return FileRecord.from_solr(doc)

    def count_request(
        self, base_url: str, query: FacetQuery
    ) -> tuple[str, dict[str, str]]:
        """Render a count-only request (`limit=0`, read `numFound`)."""
        return base_url, query.to_params(offset=0, limit=0)

    def parse_count(self, payload: dict[str, Any]) -> int:
        """Read `numFound` from a count-request payload."""
        return _num_found(payload)

    def facets_request(
        self, base_url: str, query: FacetQuery, facet_field: str
    ) -> tuple[str, dict[str, str]]:
        """Render a facet-enumeration request for `facet_field`."""
        params = dict(query.to_params(offset=0, limit=0))
        params["facets"] = facet_field
        return base_url, params

    def parse_facets(self, payload: dict[str, Any], facet_field: str) -> dict[str, int]:
        """Parse the Solr `facet_counts` block for `facet_field`."""
        return _facet_values(payload, facet_field)


def _as_offset(cursor: Cursor) -> int:
    """Narrow an opaque cursor to the integer offset ESGF1 pages with."""
    if not isinstance(cursor, int):
        msg = f"ESGF1 backend expects an integer offset cursor, got {cursor!r}"
        raise TypeError(msg)
    return cursor


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
