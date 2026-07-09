"""
Client and models for the ESGF esg-search RESTful API

This subpackage is deliberately independent of the local database: it only knows
how to build queries, talk to the search endpoint, and normalise the raw
responses into plain records.
"""

from __future__ import annotations

from cmip_data_manager.esgf.client import ESGFSearchClient
from cmip_data_manager.esgf.concurrency import (
    exponential_backoff,
    httpx_fetch,
    no_retry,
    serial_map,
    thread_pool_map,
)
from cmip_data_manager.esgf.models import (
    AmbiguousFieldError,
    DatasetRecord,
    FileRecord,
)
from cmip_data_manager.esgf.query import FacetQuery

__all__ = [
    "AmbiguousFieldError",
    "DatasetRecord",
    "ESGFSearchClient",
    "FacetQuery",
    "FileRecord",
    "exponential_backoff",
    "httpx_fetch",
    "no_retry",
    "serial_map",
    "thread_pool_map",
]
