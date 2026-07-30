"""The factory resolves each endpoint's dialect from its URL (Phase 3 wiring)."""

from __future__ import annotations

from cmip_data_manager.config import CEDA_BASE_URL, EAST_BASE_URL, Settings
from cmip_data_manager.esgf.backends import (
    DetectionCache,
    Esgf1Backend,
    EsgfNgBackend,
    Flavour,
)
from cmip_data_manager.factory import build_client, build_file_search_clients


def test_build_client_defaults_to_esgf1_backend():
    # Default settings point at a known ESGF1 endpoint -> Solr backend, no probe.
    client = build_client(Settings())
    assert client.flavour == Flavour.ESGF1
    assert isinstance(client._backend, Esgf1Backend)


def test_build_client_resolves_ng_from_east_url():
    client = build_client(Settings(base_url=EAST_BASE_URL))
    assert client.flavour == Flavour.ESGF_NG_EAST
    assert isinstance(client._backend, EsgfNgBackend)


def test_build_client_override_pins_flavour():
    client = build_client(
        Settings(base_url=CEDA_BASE_URL),
        flavour_overrides={CEDA_BASE_URL: Flavour.ESGF_NG_EAST},
    )
    assert client.flavour == Flavour.ESGF_NG_EAST


def test_build_client_explicit_backend_bypasses_detection():
    client = build_client(Settings(base_url=EAST_BASE_URL), backend=Esgf1Backend())
    assert client.flavour == Flavour.ESGF1


def test_build_file_search_clients_mix_esgf1_and_ng():
    clients = build_file_search_clients(
        (CEDA_BASE_URL, EAST_BASE_URL),
        detection_cache=DetectionCache(),
    )
    assert [c.flavour for c in clients] == [
        Flavour.ESGF1,
        Flavour.ESGF_NG_EAST,
    ]
