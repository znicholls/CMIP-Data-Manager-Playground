"""
The search-backend seam: one protocol, many ESGF dialects behind one client

`ESGFSearchClient` owns transport, retry and paging *orchestration*; a
`SearchBackend` owns everything that is **dialect-specific**: how a `FacetQuery`
becomes a request, how a page of results is parsed, how the next page is reached,
and how a raw document becomes a `DatasetRecord`/`FileRecord`.  Today's ESGF1
(esg-search / Solr) behaviour lives in `Esgf1Backend`; the ESGF-NG (STAC / CQL2)
backends slot in beside it without the client changing.

The client walks pages with an **opaque cursor** so the two very different paging
contracts share one loop: ESGF1's cursor is the next integer `offset`; ESGF-NG's is
the token lifted from the `rel="next"` link.  A backend returns `next_cursor=None`
to signal "no more pages".
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from typing import Any, Protocol

from cmip_data_manager.esgf.models import DatasetRecord, FileRecord
from cmip_data_manager.esgf.query import FacetQuery

Cursor = int | str
"""An opaque page cursor: an integer `offset` (ESGF1) or a `token` string (ESGF-NG)."""


class Flavour(enum.Enum):
    """
    Which search dialect an endpoint speaks

    East and west are kept **separate** deliberately (they already differ — different
    collection-id casing, different result-envelope keys, different capability
    profiles), so the workflow can diverge their handling later without a refactor;
    for now they may share an implementation.
    """

    ESGF1 = "esgf1"
    """esg-search / Apache Solr — flat facet params, Solr JSON (the current backend)."""

    ESGF_NG_EAST = "esgf-ng-east"
    """ESGF-NG STAC/CQL2, east deployment (`api.stac.esgf.ceda.ac.uk`)."""

    ESGF_NG_WEST = "esgf-ng-west"
    """ESGF-NG STAC/CQL2, west deployment (`discovery.production.esgf-west.org`)."""


class ESGFResponseError(RuntimeError):
    """Raised when a search response is not shaped the way its backend expects."""


class DeepPaginationError(RuntimeError):
    """
    Raised when a result set is too large to page through safely

    ESGF/Globus Search reject or silently truncate very deep pagination, so rather
    than return a partial answer we stop and ask the caller to narrow the query.
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


class UnsupportedOnBackend(RuntimeError):
    """
    Raised when a query uses a capability the target backend does not have

    Some `FacetQuery` features are ESGF1-only (e.g. free-text Lucene `query`, the
    `replica`/`distrib` flags).  Rather than silently drop them — which would return
    subtly wrong results — a backend that cannot honour them raises this loudly.
    """

    def __init__(self, feature: str, flavour: Flavour) -> None:
        self.feature = feature
        self.flavour = flavour
        super().__init__(
            f"{feature!r} is not supported by the {flavour.value} search backend."
        )


@dataclass(frozen=True)
class Page:
    """
    One parsed page of results plus how to reach the next

    `next_cursor` is `None` on the final page.  `num_found` is the total the query
    matched (used for the deep-pagination guard and for `count`-style summaries); a
    backend that cannot report a total leaves it `None`.
    """

    docs: list[dict[str, Any]]
    """The raw result documents on this page (backend-native shape)."""

    num_found: int | None
    """Total results the query matched, if the backend reports it."""

    next_cursor: Cursor | None
    """Cursor for the next page, or `None` when this is the last page."""


class SearchBackend(Protocol):
    """
    The dialect-specific half of a search: request building, paging and parsing

    An implementation translates the neutral `FacetQuery` vocabulary into its own
    request shape and parses its own response shape back into the shared
    `DatasetRecord`/`FileRecord` models, so `ESGFSearchClient` stays dialect-agnostic.
    """

    flavour: Flavour
    """The dialect this backend speaks."""

    supports_file_search: bool
    """Whether this backend can run a Step-2 file search (`search_files`).

    ESGF1 (Solr) searches files by a `type=File` query; ESGF-NG carries a dataset's
    files as STAC `assets`, so it has **no** file search (`search_files` raises
    `UnsupportedOnBackend`).  Callers that hand a *ranked, mixed-dialect* client list to
    `search.files.add_files` use this to **skip** the clients that cannot file-search,
    rather than crash on them — the same way the parent search tolerates them."""

    def start_cursor(self) -> Cursor:
        """Return the cursor for the first page (e.g. offset `0`)."""
        ...

    def page_request(
        self, base_url: str, query: FacetQuery, *, cursor: Cursor, page_size: int
    ) -> tuple[str, dict[str, str]]:
        """Build the `(url, params)` for the page at `cursor`."""
        ...

    def parse_page(
        self, payload: dict[str, Any], *, cursor: Cursor, max_results: int
    ) -> Page:
        """Parse a page payload into a `Page`, guarding against unsafe pagination."""
        ...

    def parse_dataset(self, doc: dict[str, Any]) -> DatasetRecord:
        """Turn one raw dataset document into a `DatasetRecord`."""
        ...

    def parse_file(self, doc: dict[str, Any]) -> FileRecord:
        """Turn one raw file document into a `FileRecord`."""
        ...

    def count_request(
        self, base_url: str, query: FacetQuery
    ) -> tuple[str, dict[str, str]]:
        """Build the `(url, params)` for a count-only request."""
        ...

    def parse_count(self, payload: dict[str, Any]) -> int:
        """Extract the total match count from a count-request payload."""
        ...

    def facets_request(
        self, base_url: str, query: FacetQuery, facet_field: str
    ) -> tuple[str, dict[str, str]]:
        """Build the `(url, params)` for enumerating one facet's values."""
        ...

    def parse_facets(self, payload: dict[str, Any], facet_field: str) -> dict[str, int]:
        """Parse a facet-enumeration payload into `{value: count}`."""
        ...
