"""Tests for the convenience constructors."""

from __future__ import annotations

from cmip_data_manager import (
    Settings,
    build_client,
    build_file_search_clients,
    open_repository,
)
from cmip_data_manager.config import DEFAULT_INDEX_ENDPOINTS
from cmip_data_manager.db.repository import Repository


def test_build_client_uses_settings():
    client = build_client(Settings(base_url="https://mirror.example/search"))
    assert client.base_url == "https://mirror.example/search"


def test_build_file_search_clients_defaults_to_preference_order():
    clients = build_file_search_clients()
    assert [c.base_url for c in clients] == list(DEFAULT_INDEX_ENDPOINTS)


def test_build_file_search_clients_honours_a_custom_endpoint_list():
    endpoints = ("https://a/search", "https://b/search", "https://c/search")
    clients = build_file_search_clients(endpoints)
    assert tuple(c.base_url for c in clients) == endpoints


def test_build_client_default_settings():
    client = build_client()
    assert client.base_url.startswith("https://")


def test_open_repository_is_usable(tmp_path):
    repo = open_repository(tmp_path / "db.sqlite")
    assert isinstance(repo, Repository)
    # Tables exist: recording a run does not raise.
    result = repo.record_run([], endpoint_url="u", spec={}, tag="uc")
    assert result.num_found == 0
