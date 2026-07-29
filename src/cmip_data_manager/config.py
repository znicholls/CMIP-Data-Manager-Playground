"""
Configuration for searching and caching ESGF data

The search endpoint deliberately lives here (and is injected everywhere else)
rather than being hard-coded, so a different mirror can be swapped in when, for
example, `metagrid.esgf-west.org` is down for maintenance.  Any URL that speaks
the ESGF esg-search RESTful API will work.
"""

from __future__ import annotations

from dataclasses import dataclass

DEFAULT_BASE_URL = "https://metagrid.esgf-west.org/proxy/search"
"""Default ESGF search endpoint (a thin proxy in front of the esg-search API)."""

CEDA_BASE_URL = "https://esgf.ceda.ac.uk/esg-search/search"
"""CEDA's esg-search endpoint — a fast, independent index.  Returns the same Solr JSON
shape as the metagrid proxy, so no separate parsing is needed."""

ORNL_BASE_URL = "https://esgf-node.ornl.gov/proxy/search"
"""ORNL's MetaGrid search proxy — another fast, independent index (the `/esg-search/`
path on that host is only the web UI; the API lives at `/proxy/search`)."""

DEFAULT_INDEX_ENDPOINTS = (CEDA_BASE_URL, ORNL_BASE_URL, DEFAULT_BASE_URL)
"""Preference-ordered search-index endpoints for the Step-2 file search.

`search.files.add_files` tries each in turn, falling back to the next only after an
endpoint's retries and requeues are exhausted.  Override the order or set for different
mirrors.

NOTE (2026-07): `metagrid.esgf-west.org` is under maintenance, so it is currently placed
**last**; normally it would lead.  Order today is CEDA -> ORNL -> metagrid-west.  Move
metagrid-west back to the front once the proxy is healthy again."""

# QUESTION: do we wan to get rid of this? Should this be something we specify?
# Likely this changes as we do project/esgf integration (honestly same with above)
DEFAULT_PROJECT = "CMIP6"
"""Project searched by default."""

# QUESTION: Are these still used? With current workflow should
# never hit limit? Or is it possible to hit limit on index node search?
# Or is index node search not where the 10000 limit applies?
MAX_PAGE_SIZE = 10_000
"""Hard limit the ESGF API places on the `limit` parameter of a single query."""

MAX_RETRIEVABLE = 10_000
"""
Largest number of results retrievable for a single query.

The API rejects any request with `offset >= 10000` (HTTP 422), so results beyond
the first 10000 of a query cannot be paged to.  A query matching more than this
must be split into narrower queries.
"""


@dataclass(frozen=True)
class Settings:
    """
    Settings that control how we talk to the ESGF search API

    Examples
    --------
    >>> Settings().base_url
    'https://metagrid.esgf-west.org/proxy/search'
    >>> Settings(page_size=500).page_size
    500
    """

    base_url: str = DEFAULT_BASE_URL
    """Search endpoint to query."""

    project: str = DEFAULT_PROJECT
    """Project to restrict searches to (e.g. `"CMIP6"`)."""

    page_size: int = 2_000
    """
    Number of results to request per page.

    Clamped to `MAX_PAGE_SIZE` by the client.
    """

    timeout: float = 60.0
    """Per-request timeout in seconds."""

    max_results: int = MAX_RETRIEVABLE
    """
    Largest result set we are willing to page through for a single query.

    ESGF (and the Globus Search backend behind this proxy) reject pagination past
    the first 10000 results, so we raise instead of silently returning a partial
    result set.  Narrow the query (e.g. add facets) if you hit it.
    """

    def __post_init__(self) -> None:
        """Validate the settings."""
        if self.page_size < 1:
            msg = f"page_size must be >= 1, got {self.page_size}"
            raise ValueError(msg)
        if self.max_results < 1:
            msg = f"max_results must be >= 1, got {self.max_results}"
            raise ValueError(msg)
