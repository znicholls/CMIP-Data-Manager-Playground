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
    PROMOTED_ATTRS,
    HeaderKey,
    HeaderMetadata,
    HeaderReadBlocked,
    HeaderReadCrashed,
    HeaderReadTimeout,
    candidate_urls_for_files,
    header_key,
    is_block_signal,
    order_candidates,
    promote_blocks,
    read_first_readable,
    read_header,
    simulation_key,
    with_retry,
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
from cmip_data_manager.esgf.routing import (
    AffinityKey,
    SimulationCandidates,
    build_candidates,
    hosts_to_simulations,
    simulation_candidates,
    source_id_affinity,
)

__all__ = [
    "PROMOTED_ATTRS",
    "AffinityKey",
    "AmbiguousFieldError",
    "DatasetRecord",
    "ESGFSearchClient",
    "FacetQuery",
    "FileRecord",
    "HeaderKey",
    "HeaderMetadata",
    "HeaderReadBlocked",
    "HeaderReadCrashed",
    "HeaderReadTimeout",
    "NodeHealth",
    "NodeStat",
    "ParentInfo",
    "ParentMetadataConflictError",
    "ReadOutcome",
    "SimulationCandidates",
    "build_candidates",
    "candidate_urls_for_files",
    "exponential_backoff",
    "header_key",
    "hosts_to_simulations",
    "http_download_url",
    "http_download_urls",
    "httpx_fetch",
    "is_block_signal",
    "no_retry",
    "order_candidates",
    "process_pool_map",
    "promote_blocks",
    "read_first_readable",
    "read_header",
    "read_parent_info",
    "recording",
    "resolve_dataset_parent",
    "serial_map",
    "simulation_candidates",
    "simulation_key",
    "source_id_affinity",
    "thread_pool_map",
    "with_retry",
    "with_timeout",
]
