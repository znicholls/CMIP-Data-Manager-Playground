"""
Convenience constructors that wire the pieces together

These keep the top-level scripts small: a script hard-codes its configuration and
calls these to obtain a ready-to-use client and repository.
"""

from __future__ import annotations

from pathlib import Path

from cmip_data_manager.config import Settings
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
