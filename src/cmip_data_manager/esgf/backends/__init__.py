"""
Search backends: the dialect-specific half of `ESGFSearchClient`

`SearchBackend` (see `base`) is the seam; `Esgf1Backend` is the current
esg-search / Solr implementation.  ESGF-NG (STAC / CQL2) backends will be added
here beside it.
"""

from __future__ import annotations

from cmip_data_manager.esgf.backends.base import (
    Cursor,
    DeepPaginationError,
    ESGFResponseError,
    Flavour,
    Page,
    SearchBackend,
    UnsupportedOnBackend,
)
from cmip_data_manager.esgf.backends.detect import (
    DetectionCache,
    backend_for,
    detect_flavour,
    resolve_backend,
)
from cmip_data_manager.esgf.backends.esgf1 import Esgf1Backend
from cmip_data_manager.esgf.backends.esgf_ng import EsgfNgBackend

__all__ = [
    "Cursor",
    "DeepPaginationError",
    "DetectionCache",
    "ESGFResponseError",
    "Esgf1Backend",
    "EsgfNgBackend",
    "Flavour",
    "Page",
    "SearchBackend",
    "UnsupportedOnBackend",
    "backend_for",
    "detect_flavour",
    "resolve_backend",
]
