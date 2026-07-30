"""Tests for endpoint dialect detection (registry, override, probe, cache)."""

from __future__ import annotations

from typing import Any

import pytest

from cmip_data_manager.esgf.backends import (
    DetectionCache,
    Esgf1Backend,
    EsgfNgBackend,
    Flavour,
    backend_for,
    detect_flavour,
    resolve_backend,
)

CEDA = "https://esgf.ceda.ac.uk/esg-search/search"
EAST = "https://search.east.esgf.io/search"
WEST = "https://search.west.esgf.io/search"
UNKNOWN = "https://brand-new-index.example.org/search"

STAC_ROOT = {
    "type": "Catalog",
    "id": "stac-fastapi",
    "conformsTo": ["http://www.opengis.net/spec/cql2/1.0/conf/basic-cql2"],
}
SOLR_ROOT = {"responseHeader": {"status": 0}, "response": {"numFound": 0}}


def _boom_fetch(url: str, params: Any) -> dict[str, Any]:
    raise AssertionError(f"registry/override should not have probed: {url}")


# --- registry (no probe) --------------------------------------------------------


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        (CEDA, Flavour.ESGF1),
        ("https://esgf-node.ornl.gov/proxy/search", Flavour.ESGF1),
        (EAST, Flavour.ESGF_NG_EAST),
        ("https://api.stac.esgf.ceda.ac.uk/search", Flavour.ESGF_NG_EAST),
        (WEST, Flavour.ESGF_NG_WEST),
        ("https://discovery.production.esgf-west.org/search", Flavour.ESGF_NG_WEST),
    ],
)
def test_known_hosts_resolve_without_probing(url, expected):
    assert detect_flavour(url, fetch=_boom_fetch) == expected


# --- override wins --------------------------------------------------------------


def test_override_by_url_wins_over_registry():
    got = detect_flavour(
        CEDA, overrides={CEDA: Flavour.ESGF_NG_EAST}, fetch=_boom_fetch
    )
    assert got == Flavour.ESGF_NG_EAST


def test_override_by_host_wins():
    got = detect_flavour(
        UNKNOWN,
        overrides={"brand-new-index.example.org": Flavour.ESGF_NG_WEST},
        fetch=_boom_fetch,
    )
    assert got == Flavour.ESGF_NG_WEST


# --- probe for unknown hosts ----------------------------------------------------


def test_probe_detects_stac_as_esgf_ng():
    calls: list[str] = []

    def fetch(url: str, params: Any) -> dict[str, Any]:
        calls.append(url)
        return STAC_ROOT

    got = detect_flavour(UNKNOWN, fetch=fetch)
    assert got == Flavour.ESGF_NG_EAST  # host has no "west" -> east
    assert calls == ["https://brand-new-index.example.org/"]  # probed the root only


def test_probe_stac_host_with_west_is_west():
    got = detect_flavour(
        "https://something-west.example.org/search",
        fetch=lambda url, params: STAC_ROOT,
    )
    assert got == Flavour.ESGF_NG_WEST


def test_probe_non_stac_defaults_to_esgf1():
    got = detect_flavour(UNKNOWN, fetch=lambda url, params: SOLR_ROOT)
    assert got == Flavour.ESGF1


def test_probe_failure_defaults_to_esgf1():
    def fetch(url: str, params: Any) -> dict[str, Any]:
        raise RuntimeError("connection refused")

    assert detect_flavour(UNKNOWN, fetch=fetch) == Flavour.ESGF1


# --- caching --------------------------------------------------------------------


def test_cache_short_circuits_second_probe():
    calls: list[str] = []

    def fetch(url: str, params: Any) -> dict[str, Any]:
        calls.append(url)
        return STAC_ROOT

    cache = DetectionCache()
    first = detect_flavour(UNKNOWN, fetch=fetch, cache=cache)
    second = detect_flavour(UNKNOWN, fetch=_boom_fetch, cache=cache)  # must not probe
    assert first == second == Flavour.ESGF_NG_EAST
    assert calls == ["https://brand-new-index.example.org/"]  # probed exactly once


# --- backend construction -------------------------------------------------------


def test_backend_for_maps_each_flavour():
    assert isinstance(backend_for(Flavour.ESGF1), Esgf1Backend)
    east = backend_for(Flavour.ESGF_NG_EAST)
    west = backend_for(Flavour.ESGF_NG_WEST)
    assert isinstance(east, EsgfNgBackend) and east.lowercase_collection is False
    assert isinstance(west, EsgfNgBackend) and west.lowercase_collection is True


def test_resolve_backend_returns_ready_backend():
    assert isinstance(resolve_backend(CEDA, fetch=_boom_fetch), Esgf1Backend)
    ng = resolve_backend(EAST, fetch=_boom_fetch)
    assert isinstance(ng, EsgfNgBackend)
    assert ng.flavour == Flavour.ESGF_NG_EAST
