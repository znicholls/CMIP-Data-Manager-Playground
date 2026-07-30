"""Tests for the ESGF-NG (STAC / CQL2) search backend, offline with fake transports."""

from __future__ import annotations

from typing import Any
from urllib.parse import parse_qs, urlparse

import pytest

from cmip_data_manager.esgf.backends import (
    DeepPaginationError,
    EsgfNgBackend,
    ESGFResponseError,
    Flavour,
    UnsupportedOnBackend,
)
from cmip_data_manager.esgf.client import ESGFSearchClient
from cmip_data_manager.esgf.query import FacetQuery

EAST = "https://search.east.esgf.io/search"


def stac_feature(instance_id: str, **props: Any) -> dict[str, Any]:
    """Build a minimal CMIP6 STAC feature with `cmip6:`-prefixed properties."""
    collection = "CMIP6"
    properties: dict[str, Any] = {"version": "20200101", "latest": True}
    for key, value in props.items():
        properties[f"cmip6:{key}"] = value
    return {
        "type": "Feature",
        "id": instance_id,
        "collection": collection,
        "properties": properties,
        "assets": {
            "a.nc": {"href": "https://h/a.nc", "roles": ["data"]},
            "b.nc": {"href": "https://h/b.nc", "roles": ["data"]},
        },
    }


def stac_fetch(features: list[dict[str, Any]], *, count_key: str = "numberMatched"):
    """Fake transport paging `features` by a numeric `token` (envelope key given)."""

    def fetch(url: str, params: dict[str, str]) -> dict[str, Any]:
        limit = int(params["limit"])
        token = params.get("token")
        start = int(token) if token else 0
        page = features[start : start + limit]
        next_start = start + len(page)
        links = [{"rel": "root", "href": url}]
        if page and next_start < len(features):
            links.append({"rel": "next", "href": f"{url}?token={next_start}"})
        return {
            "type": "FeatureCollection",
            count_key: len(features),
            "features": page,
            "links": links,
        }

    return fetch


# --- renderer -------------------------------------------------------------------


def test_single_value_facet_renders_equality():
    backend = EsgfNgBackend()
    _url, params = backend.page_request(
        EAST, FacetQuery(variable_id=("tas",)), cursor="", page_size=50
    )
    assert params["collections"] == "CMIP6"
    assert params["limit"] == "50"
    assert params["filter"] == "cmip6:variable_id='tas'"
    assert "token" not in params  # first page carries no token


def test_multi_value_facet_renders_in_clause_and_latest_is_bare():
    backend = EsgfNgBackend()
    _url, params = backend.page_request(
        EAST,
        FacetQuery(
            variable_id=("tas", "pr"),
            experiment_id=("historical",),
            extra_facets={"table_id": ("Amon",)},
            latest=True,
        ),
        cursor="",
        page_size=10,
    )
    assert params["filter"] == (
        "cmip6:variable_id IN ('tas','pr') AND cmip6:experiment_id='historical' "
        "AND cmip6:table_id='Amon' AND latest=true"
    )


def test_prefix_is_derived_from_project_not_hardcoded():
    backend = EsgfNgBackend()
    _url, params = backend.page_request(
        EAST,
        FacetQuery(project="CORDEX-CMIP6", variable_id=("tas",)),
        cursor="",
        page_size=10,
    )
    assert params["collections"] == "CORDEX-CMIP6"
    assert params["filter"] == "cordex-cmip6:variable_id='tas'"


def test_west_lowercases_the_collection():
    backend = EsgfNgBackend(flavour=Flavour.ESGF_NG_WEST, lowercase_collection=True)
    _url, params = backend.page_request(
        EAST, FacetQuery(variable_id=("tas",)), cursor="", page_size=10
    )
    assert params["collections"] == "cmip6"
    assert params["filter"] == "cmip6:variable_id='tas'"


def test_single_quote_in_value_is_escaped():
    backend = EsgfNgBackend()
    _url, params = backend.page_request(
        EAST, FacetQuery(source_id=("a'b",)), cursor="", page_size=10
    )
    assert params["filter"] == "cmip6:source_id='a''b'"


def test_token_cursor_is_passed_through():
    backend = EsgfNgBackend()
    _url, params = backend.page_request(
        EAST, FacetQuery(variable_id=("tas",)), cursor="abc123", page_size=10
    )
    assert params["token"] == "abc123"  # noqa: S105 - a paging token, not a secret


# --- unsupported features -------------------------------------------------------


@pytest.mark.parametrize(
    "query",
    [
        FacetQuery(query="experiment_id:ssp*"),
        FacetQuery(replica=True),
        FacetQuery(distrib=False),
        FacetQuery(type="File", dataset_id=("d0",)),
        FacetQuery(dataset_id=("d0",)),
    ],
)
def test_unsupported_features_raise(query):
    backend = EsgfNgBackend()
    with pytest.raises(UnsupportedOnBackend):
        backend.page_request(EAST, query, cursor="", page_size=10)


def test_search_files_raises_unsupported():
    # A File-type query is rejected up-front when building the request...
    client = ESGFSearchClient(EAST, backend=EsgfNgBackend(), fetch=stac_fetch([]))
    with pytest.raises(UnsupportedOnBackend, match="non-Dataset"):
        client.search_files(FacetQuery(type="File", dataset_id=("d0",)))
    # ...and even a Dataset query routed to search_files refuses to parse a file record.
    one = ESGFSearchClient(
        EAST,
        backend=EsgfNgBackend(),
        fetch=stac_fetch([stac_feature("CMIP6.x.d0")]),
    )
    with pytest.raises(UnsupportedOnBackend, match="file search"):
        one.search_files(FacetQuery(type="Dataset"))


def test_facet_values_raises_unsupported():
    client = ESGFSearchClient(EAST, backend=EsgfNgBackend(), fetch=stac_fetch([]))
    with pytest.raises(UnsupportedOnBackend, match="facet enumeration"):
        client.facet_values(FacetQuery(variable_id=("tas",)), "experiment_id")


# --- parsing --------------------------------------------------------------------


def test_parse_dataset_maps_stac_to_esgf1_columns_and_keeps_raw_verbatim():
    backend = EsgfNgBackend()
    feature = stac_feature(
        "CMIP6.CMIP.X.MODEL.historical.r1i1p1f1.Amon.tas.gn.v20200101",
        source_id="MODEL",
        experiment_id="historical",
        variant_label="r1i1p1f1",
        variable_id="tas",
        table_id="Amon",
        grid_label="gn",
        institution_id="X",
    )
    rec = backend.parse_dataset(feature)
    assert rec.source_id == "MODEL"
    assert rec.experiment_id == "historical"
    assert rec.variable_id == "tas"
    assert rec.table_id == "Amon"
    assert rec.version == "20200101"
    assert rec.latest is True
    assert rec.number_of_files == 2  # from the two data assets
    assert rec.data_node is None and rec.replica is None  # node-independent
    assert rec.instance_key == feature["id"]  # id is the instance id (no |data_node)
    assert rec.master_key.endswith(".Amon.tas.gn")  # version stripped
    assert rec.raw == feature  # STAC feature retained verbatim (not Solr-translated)
    assert "cmip6:source_id" in rec.raw["properties"]  # native STAC keys kept


# --- paging + count -------------------------------------------------------------


def test_token_pagination_stitches_pages_without_duplicates():
    features = [stac_feature(f"CMIP6.x.d{i}") for i in range(25)]
    client = ESGFSearchClient(
        EAST, backend=EsgfNgBackend(), fetch=stac_fetch(features), page_size=10
    )
    recs = client.search(FacetQuery(variable_id=("tas",)))
    assert [r.id for r in recs] == [f"CMIP6.x.d{i}" for i in range(25)]


@pytest.mark.parametrize("count_key", ["numberMatched", "numMatched"])
def test_count_reads_both_east_and_west_envelope_keys(count_key):
    features = [stac_feature(f"CMIP6.x.d{i}") for i in range(7)]
    client = ESGFSearchClient(
        EAST, backend=EsgfNgBackend(), fetch=stac_fetch(features, count_key=count_key)
    )
    assert client.count(FacetQuery(variable_id=("tas",))) == 7


def test_count_uses_limit_one_not_zero():
    seen: dict[str, str] = {}

    def fetch(url: str, params: dict[str, str]) -> dict[str, Any]:
        seen.update(params)
        return {"type": "FeatureCollection", "numberMatched": 3, "features": []}

    client = ESGFSearchClient(EAST, backend=EsgfNgBackend(), fetch=fetch)
    client.count(FacetQuery(variable_id=("tas",)))
    assert seen["limit"] == "1"  # NG rejects limit=0


def test_deep_pagination_guard_uses_num_matched():
    features = [stac_feature("CMIP6.x.d0")]
    client = ESGFSearchClient(
        EAST,
        backend=EsgfNgBackend(),
        fetch=lambda url, params: {
            "type": "FeatureCollection",
            "numberMatched": 50_000,
            "features": features,
            "links": [],
        },
        max_results=100,
    )
    with pytest.raises(DeepPaginationError):
        client.search(FacetQuery(variable_id=("tas",)))


def test_malformed_response_missing_features_raises():
    client = ESGFSearchClient(
        EAST,
        backend=EsgfNgBackend(),
        fetch=lambda url, params: {"type": "FeatureCollection", "numberMatched": 1},
    )
    with pytest.raises(ESGFResponseError, match="features"):
        client.search(FacetQuery(variable_id=("tas",)))


def test_count_missing_both_keys_raises():
    client = ESGFSearchClient(
        EAST,
        backend=EsgfNgBackend(),
        fetch=lambda url, params: {"type": "FeatureCollection", "features": []},
    )
    with pytest.raises(ESGFResponseError, match="numberMatched"):
        client.count(FacetQuery(variable_id=("tas",)))


def test_empty_result_returns_no_records():
    client = ESGFSearchClient(EAST, backend=EsgfNgBackend(), fetch=stac_fetch([]))
    assert client.search(FacetQuery(variable_id=("tas",))) == []


def test_next_link_token_is_extracted():
    features = [stac_feature(f"CMIP6.x.d{i}") for i in range(3)]
    fetch = stac_fetch(features)
    # first page of 2 yields a next link whose token is the offset 2
    payload = fetch(EAST, {"collections": "CMIP6", "limit": "2"})
    nexts = [link for link in payload["links"] if link["rel"] == "next"]
    assert parse_qs(urlparse(nexts[0]["href"]).query)["token"] == ["2"]
