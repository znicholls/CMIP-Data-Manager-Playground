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
    process_pool_map,
    serial_map,
    thread_pool_map,
)
from cmip_data_manager.esgf.headers import (
    HeaderMetadata,
    HeaderReadCrashed,
    HeaderReadTimeout,
    candidate_urls_for_files,
    order_candidates,
    read_first_readable,
    read_header,
    simulation_key,
    with_timeout,
)
from cmip_data_manager.esgf.health import (
    NodeHealth,
    NodeStat,
    ReadOutcome,
    recording,
)
from cmip_data_manager.esgf.models import (
    AmbiguousFieldError,
    DatasetRecord,
    FileRecord,
)
from cmip_data_manager.esgf.parents import (
    ParentInfo,
    ParentMetadataConflictError,
    http_download_url,
    http_download_urls,
    read_parent_info,
    resolve_dataset_parent,
)
from cmip_data_manager.esgf.query import FacetQuery

__all__ = [
    "AmbiguousFieldError",
    "DatasetRecord",
    "ESGFSearchClient",
    "FacetQuery",
    "FileRecord",
    "HeaderMetadata",
    "HeaderReadCrashed",
    "HeaderReadTimeout",
    "NodeHealth",
    "NodeStat",
    "ParentInfo",
    "ParentMetadataConflictError",
    "ReadOutcome",
    "candidate_urls_for_files",
    "exponential_backoff",
    "http_download_url",
    "http_download_urls",
    "httpx_fetch",
    "no_retry",
    "order_candidates",
    "process_pool_map",
    "read_first_readable",
    "read_header",
    "read_parent_info",
    "recording",
    "resolve_dataset_parent",
    "serial_map",
    "simulation_key",
    "thread_pool_map",
    "with_timeout",
]
