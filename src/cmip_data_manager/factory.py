"""
Convenience constructors that wire the pieces together

These keep the top-level scripts small: a script hard-codes its configuration and
calls these to obtain a ready-to-use client and repository.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from cmip_data_manager.config import DEFAULT_INDEX_ENDPOINTS, Settings
from cmip_data_manager.db.engine import create_db_engine, init_db
from cmip_data_manager.db.repository import Repository
from cmip_data_manager.esgf.client import ESGFSearchClient
from cmip_data_manager.esgf.concurrency import MapFn, RetryPolicy, no_retry, serial_map


def build_client(
    settings: Settings | None = None,
    *,
    retry: RetryPolicy = no_retry,
    map_fn: MapFn = serial_map,
) -> ESGFSearchClient:
    """
    Build a search client from settings

    Parameters
    ----------
    settings
        Settings to use; defaults to `Settings()`.

    retry
        Retry policy to wrap the transport with (e.g. `exponential_backoff(...)`).

    map_fn
        Concurrency strategy for multi-query searches (e.g. `thread_pool_map(...)`).

    Returns
    -------
    :
        A configured client.
    """
    settings = settings or Settings()
    return ESGFSearchClient(
        settings.base_url,
        retry=retry,
        map_fn=map_fn,
        page_size=settings.page_size,
        max_results=settings.max_results,
        timeout=settings.timeout,
    )


def build_file_search_clients(
    endpoints: Sequence[str] = DEFAULT_INDEX_ENDPOINTS,
    *,
    settings: Settings | None = None,
    map_fn: MapFn = serial_map,
) -> list[ESGFSearchClient]:
    """
    Build one search client per endpoint, in preference order, for Step 2

    The clients are deliberately built with **`no_retry`**: retry/backoff for the file
    search is owned by `search.files.add_files`, so it can count retries in index-node
    health and drive the endpoint fallback.  A version is tried on the next endpoint
    only after the current one's retries and requeues are exhausted.

    Parameters
    ----------
    endpoints
        Search endpoints in preference order (defaults to `DEFAULT_INDEX_ENDPOINTS`:
        CEDA, then ORNL, then metagrid-west — the latter is currently last while it is
        in maintenance).

    settings
        Paging/timeout settings shared by every client; defaults to `Settings()`.
        Its `base_url` is ignored — the endpoints come from `endpoints`.

    map_fn
        Concurrency strategy handed to each client (only used for multi-query
        searches, not the single-query file lookups Step 2 makes).

    Returns
    -------
    :
        One configured, non-retrying client per endpoint, in the given order.
    """
    settings = settings or Settings()
    return [
        ESGFSearchClient(
            endpoint,
            retry=no_retry,
            map_fn=map_fn,
            page_size=settings.page_size,
            max_results=settings.max_results,
            timeout=settings.timeout,
        )
        for endpoint in endpoints
    ]


def open_repository(db: str | Path, *, echo: bool = False) -> Repository:
    """
    Open (creating if needed) a repository backed by the given database

    Parameters
    ----------
    db
        SQLite path/filename or a full SQLAlchemy URL.

    echo
        If `True`, log all SQL.

    Returns
    -------
    :
        A repository whose tables have been created.
    """
    engine = create_db_engine(db, echo=echo)
    init_db(engine)
    return Repository(engine)
