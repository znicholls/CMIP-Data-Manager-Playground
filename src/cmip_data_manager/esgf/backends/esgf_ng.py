"""
The ESGF-NG backend: STAC / CQL2 (east and west)

ESGF-NG is a STAC API (stac-fastapi; OGC API - Features + CQL2 filtering), a wholly
different dialect from ESGF1's esg-search/Solr.  This backend translates the neutral
`FacetQuery` vocabulary into a **CQL2-text filter** and parses the GeoJSON
`FeatureCollection` back into the shared `DatasetRecord` model — populating the same
ESGF1-vocabulary columns for the database while keeping the **STAC feature verbatim**
in `DatasetRecord.raw` (nothing the NG API returned is lost).

Key differences from ESGF1, all handled here:

- **Query** is a CQL2 filter (`filter=cmip6:variable_id='tas' AND ...`), not flat
  facet params; multiple values for one facet use `IN (...)`.  The collection-facet
  prefix (`cmip6:`) is **derived** from the query's `project`, never hard-coded.
- **Project** selects a STAC `collections=` (east uses `CMIP6`, west lower-cases to
  `cmip6` — see `lowercase_collection`).
- **Paging** follows the opaque `token` on the `rel="next"` link, not an `offset`.
- **Count** uses `limit=1` + `numberMatched` (NG rejects `limit=0`); the result
  envelope key differs east↔west (`numberMatched` vs `numMatched`), so both are read.
- **Files, replicas, free-text and facet enumeration** have no Step-1 equivalent and
  raise `UnsupportedOnBackend` (files come from item `assets` in a later step).

East and west are separate `Flavour`s that currently share this one implementation;
`lowercase_collection` is the only behavioural knob so far (west's item shape is still
unconfirmed while it has no data).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qs, urlparse

from cmip_data_manager.esgf.backends.base import (
    Cursor,
    DeepPaginationError,
    ESGFResponseError,
    Flavour,
    Page,
    UnsupportedOnBackend,
)
from cmip_data_manager.esgf.models import DatasetRecord, FileRecord
from cmip_data_manager.esgf.query import FacetQuery

_START_CURSOR = ""
"""First-page cursor: an empty token (the first request carries no `token`)."""

# Labels for `UnsupportedOnBackend`, named so the raise sites pass a value (not an
# inline literal) — matching the repo's "compute the arg" exception convention.
_F_FILE_SEARCH = "file search"
_F_FACETS = "facet enumeration"
_F_NON_DATASET = "non-Dataset (e.g. File) search"
_F_FREE_TEXT = "free-text 'query'"
_F_REPLICA = "replica flag"
_F_DISTRIB = "distrib flag"
_F_DATASET_ID = "dataset_id facet"

_FACET_FIELDS = (
    "variable_id",
    "experiment_id",
    "frequency",
    "source_id",
    "variant_label",
)
"""`FacetQuery` facet fields rendered into the CQL2 filter, in a stable order.

`project` becomes the `collections=` selector (not a filter conjunct) and
`dataset_id` is an ESGF1 file-search construct with no NG counterpart, so both are
excluded here."""


@dataclass
class EsgfNgBackend:
    """
    ESGF-NG STAC/CQL2 dialect

    Examples
    --------
    >>> backend = EsgfNgBackend()
    >>> url, params = backend.page_request(
    ...     "https://search.east.esgf.io/search",
    ...     FacetQuery(variable_id=("tas",), experiment_id=("historical", "ssp245")),
    ...     cursor="",
    ...     page_size=100,
    ... )
    >>> params["collections"], params["limit"]
    ('CMIP6', '100')
    >>> params["filter"]
    "cmip6:variable_id='tas' AND cmip6:experiment_id IN ('historical','ssp245')"
    >>> feature = {
    ...     "id": "CMIP6.CMIP.X.M.historical.r1i1p1f1.Amon.tas.gn.v20200101",
    ...     "collection": "CMIP6",
    ...     "properties": {"cmip6:variable_id": "tas", "version": "20200101"},
    ... }
    >>> rec = backend.parse_dataset(feature)
    >>> rec.variable_id, rec.version, rec.data_node is None
    ('tas', '20200101', True)
    """

    flavour: Flavour = Flavour.ESGF_NG_EAST
    """Which NG deployment this backend talks to (`ESGF_NG_EAST`/`ESGF_NG_WEST`)."""

    lowercase_collection: bool = False
    """Lower-case the `collections=` value (west's collection ids are lower-case)."""

    supports_file_search: bool = False
    """No file search: a STAC item carries its files as `assets` (see `search_files`,
    which raises `UnsupportedOnBackend`).  Left `False`; not meant to be overridden."""

    def start_cursor(self) -> Cursor:
        """Return the first-page cursor (an empty token)."""
        return _START_CURSOR

    def page_request(
        self, base_url: str, query: FacetQuery, *, cursor: Cursor, page_size: int
    ) -> tuple[str, dict[str, str]]:
        """Render the page at `cursor` as `(url, STAC item-search params)`."""
        params = self._base_params(query, limit=page_size)
        token = _as_token(cursor)
        if token:
            params["token"] = token
        return base_url, params

    def parse_page(
        self, payload: dict[str, Any], *, cursor: Cursor, max_results: int
    ) -> Page:
        """
        Parse a STAC `FeatureCollection` page and find the next-page token

        `num_found` comes from `numberMatched`/`numMatched`.  NG has no `offset` wall,
        but a soft `max_results` ceiling still raises `DeepPaginationError` so an
        unbounded query is narrowed rather than paged forever.  Paging stops at the
        `rel="next"` link's absence (or an empty page).
        """
        features = _features(payload)
        num_found = _num_matched(payload)
        if num_found is not None and num_found > max_results:
            raise DeepPaginationError(num_found, max_results)
        next_cursor = _next_token(payload) if features else None
        return Page(docs=features, num_found=num_found, next_cursor=next_cursor)

    def expand_dataset_doc(
        self, doc: dict[str, Any], requested_variables: tuple[str, ...]
    ) -> list[dict[str, Any]]:
        """Identity: an ESGF-NG (STAC) item is already a single-variable dataset."""
        return [doc]

    def parse_dataset(self, doc: dict[str, Any]) -> DatasetRecord:
        """Turn one STAC feature into a `DatasetRecord` (raw kept verbatim)."""
        return DatasetRecord.from_stac(doc)

    def parse_file(self, doc: dict[str, Any]) -> FileRecord:
        """File records come from item `assets`, not a file search (a later step)."""
        raise UnsupportedOnBackend(_F_FILE_SEARCH, self.flavour)

    def count_request(
        self, base_url: str, query: FacetQuery
    ) -> tuple[str, dict[str, str]]:
        """Render a count request (`limit=1`; NG rejects `limit=0`)."""
        return base_url, self._base_params(query, limit=1)

    def parse_count(self, payload: dict[str, Any]) -> int:
        """Read `numberMatched`/`numMatched` from a count-request payload."""
        num_found = _num_matched(payload)
        if num_found is None:
            msg = (
                "Unexpected STAC response: no 'numberMatched'/'numMatched'. "
                f"Got keys: {sorted(payload)}"
            )
            raise ESGFResponseError(msg)
        return num_found

    def facets_request(
        self, base_url: str, query: FacetQuery, facet_field: str
    ) -> tuple[str, dict[str, str]]:
        """NG facet enumeration (STAC `/aggregate`) is not supported yet."""
        raise UnsupportedOnBackend(_F_FACETS, self.flavour)

    def parse_facets(self, payload: dict[str, Any], facet_field: str) -> dict[str, int]:
        """NG facet enumeration is not supported yet (see `facets_request`)."""
        raise UnsupportedOnBackend(_F_FACETS, self.flavour)

    def _base_params(self, query: FacetQuery, *, limit: int) -> dict[str, str]:
        """Build the shared `collections`/`filter`/`limit` params (NG-limit guarded)."""
        self._reject_unsupported(query)
        collection = (
            query.project.lower() if self.lowercase_collection else query.project
        )
        params: dict[str, str] = {"collections": collection, "limit": str(limit)}
        cql2 = _build_filter(query)
        if cql2:
            params["filter"] = cql2
        return params

    def _reject_unsupported(self, query: FacetQuery) -> None:
        """Raise `UnsupportedOnBackend` for any ESGF1-only feature in `query`."""
        if query.type != "Dataset":
            raise UnsupportedOnBackend(_F_NON_DATASET, self.flavour)
        if query.query is not None:
            raise UnsupportedOnBackend(_F_FREE_TEXT, self.flavour)
        if query.replica is not None:
            raise UnsupportedOnBackend(_F_REPLICA, self.flavour)
        if query.distrib is not None:
            raise UnsupportedOnBackend(_F_DISTRIB, self.flavour)
        if query.dataset_id:
            raise UnsupportedOnBackend(_F_DATASET_ID, self.flavour)


def _build_filter(query: FacetQuery) -> str:
    """
    Render a `FacetQuery` to a CQL2-text filter

    Facets become collection-prefixed conjuncts (`cmip6:variable_id='tas'`), multiple
    values use `IN (...)`, and `latest`/`retracted` are **bare** properties (confirmed
    against east).  Prefix is derived from `project`, never hard-coded.
    """
    prefix = f"{query.project.lower()}:"
    conjuncts: list[str] = []
    for field in _FACET_FIELDS:
        values: tuple[str, ...] = getattr(query, field)
        if values:
            conjuncts.append(_facet_clause(f"{prefix}{field}", values))
    for name, values in query.extra_facets.items():
        if values:
            conjuncts.append(_facet_clause(f"{prefix}{name}", values))
    if query.latest is not None:
        conjuncts.append(f"latest={_cql_bool(query.latest)}")
    return " AND ".join(conjuncts)


def _facet_clause(field: str, values: tuple[str, ...]) -> str:
    """Render one facet as `field=value` (single) or `field IN (...)` (multiple)."""
    if len(values) == 1:
        return f"{field}={_cql_str(values[0])}"
    joined = ",".join(_cql_str(v) for v in values)
    return f"{field} IN ({joined})"


def _cql_str(value: str) -> str:
    """Quote a string as a CQL2-text literal, escaping single quotes (`'` -> `''`)."""
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def _cql_bool(value: bool) -> str:
    """Render a boolean as the CQL2-text literal `true`/`false`."""
    return "true" if value else "false"


def _features(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Return the `features` list, raising if the payload is not a FeatureCollection."""
    features = payload.get("features")
    if not isinstance(features, list):
        msg = (
            "Unexpected STAC response: missing 'features' list. "
            f"Got keys: {sorted(payload)}"
        )
        raise ESGFResponseError(msg)
    return features


def _num_matched(payload: dict[str, Any]) -> int | None:
    """Read the total match count (`numberMatched` east / `numMatched` west)."""
    for key in ("numberMatched", "numMatched"):
        value = payload.get(key)
        if value is not None:
            return int(value)
    return None


def _next_token(payload: dict[str, Any]) -> str | None:
    """Extract the `token` from the `rel="next"` link, or `None` if there is none."""
    for link in payload.get("links", []) or []:
        if link.get("rel") == "next":
            tokens = parse_qs(urlparse(str(link.get("href", ""))).query).get("token")
            if tokens:
                return tokens[0]
    return None


def _as_token(cursor: Cursor) -> str:
    """Narrow an opaque cursor to the token string NG pages with."""
    if not isinstance(cursor, str):
        msg = f"ESGF-NG backend expects a string token cursor, got {cursor!r}"
        raise TypeError(msg)
    return cursor
