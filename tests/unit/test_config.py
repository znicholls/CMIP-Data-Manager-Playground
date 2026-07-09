"""Tests for the settings object."""

from __future__ import annotations

import pytest

from cmip_data_manager.config import DEFAULT_BASE_URL, Settings


def test_defaults():
    settings = Settings()
    assert settings.base_url == DEFAULT_BASE_URL
    assert settings.project == "CMIP6"
    assert settings.page_size > 0


def test_base_url_is_swappable():
    settings = Settings(base_url="https://mirror.example/esg-search")
    assert settings.base_url == "https://mirror.example/esg-search"


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"page_size": 0}, "page_size"),
        ({"max_results": 0}, "max_results"),
    ],
)
def test_validation(kwargs, match):
    with pytest.raises(ValueError, match=match):
        Settings(**kwargs)
