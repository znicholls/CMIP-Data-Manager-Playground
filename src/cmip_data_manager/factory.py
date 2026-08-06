"""
Convenience constructors that wire the pieces together

These keep the top-level scripts small: a script hard-codes its configuration and
calls these to obtain a ready-to-use client and repository.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path

from cmip_data_manager.config import DEFAULT_INDEX_ENDPOINTS, Settings
from cmip_data_manager.db.engine import create_db_engine, init_db
from cmip_data_manager.db.repository import Repository
from cmip_data_manager.esgf.backends import (
    DetectionCache,
    Flavour,
    SearchBackend,
    resolve_backend,
)
from cmip_data_manager.esgf.client import ESGFSearchClient
from cmip_data_manager.esgf.concurrency import MapFn, RetryPolicy, no_retry, serial_map
from cmip_data_manager.esgf.eras import get_profile


def build_client(  # noqa: PLR0913 - a DI seam; every parameter has a default
    settings: Settings | None = None,
    *,
    retry: RetryPolicy = no_retry,
    map_fn: MapFn = serial_map,
    backend: SearchBackend | None = None,
    flavour_overrides: Mapping[str, Flavour] | None = None,
    detection_cache: DetectionCache | None = None,
    mip_era: str = "CMIP6",
) -> ESGFSearchClient:
    """
    Build a search client from settings, resolving the dialect from the endpoint

    The endpoint URL decides whether the client speaks ESGF1 (esg-search / Solr) or
    ESGF-NG (STAC / CQL2) — the caller never states it.  Every endpoint we ship is in
    the known-host registry, so this stays network-free for them; only an unknown host
    triggers a one-off probe (see `cmip_data_manager.esgf.backends.detect`).

    Parameters
    ----------
    settings
        Settings to use; defaults to `Settings()`.

    retry
        Retry policy to wrap the transport with (e.g. `exponential_backoff(...)`).

    map_fn
        Concurrency strategy for multi-query searches (e.g. `thread_pool_map(...)`).

    backend
        Force a specific backend, bypassing detection (e.g. a pre-tuned NG backend).
        When given, `mip_era` is ignored (the caller's backend carries its own era).

    flavour_overrides
        Optional `{url_or_host: Flavour}` map that pins an endpoint's dialect, winning
        over the registry/probe (handy for a new endpoint or offline tests).

    detection_cache
        Shared detection memo, so an endpoint is probed at most once across calls.

    mip_era
        The MIP era to bind to the backend (`"CMIP6"` default, `"CMIP5"`, …), selecting
        the facet-name vocabulary.  A transport-and-era mismatch (e.g. CMIP5 on an
        ESGF-NG endpoint) raises `UnsupportedOnBackend`.

    Returns
    -------
    :
        A configured client whose backend matches its endpoint's dialect and era.
    """
    settings = settings or Settings()
    resolved = (
        backend
        if backend is not None
        else resolve_backend(
            settings.base_url,
            overrides=flavour_overrides,
            cache=detection_cache,
            probe_timeout=settings.timeout,
            era=get_profile(mip_era),
        )
    )
    return ESGFSearchClient(
        settings.base_url,
        backend=resolved,
        retry=retry,
        map_fn=map_fn,
        page_size=settings.page_size,
        max_results=settings.max_results,
        timeout=settings.timeout,
    )


def build_file_search_clients(  # noqa: PLR0913 - a DI seam; every parameter has a default
    endpoints: Sequence[str] = DEFAULT_INDEX_ENDPOINTS,
    *,
    settings: Settings | None = None,
    map_fn: MapFn = serial_map,
    flavour_overrides: Mapping[str, Flavour] | None = None,
    detection_cache: DetectionCache | None = None,
    mip_era: str = "CMIP6",
) -> list[ESGFSearchClient]:
    """
    Build one search client per endpoint, in preference order, for Step 2

    The clients are deliberately built with **`no_retry`**: retry/backoff for the file
    search is owned by `search.files.add_files`, so it can count retries in index-node
    health and drive the endpoint fallback.  A version is tried on the next endpoint
    only after the current one's retries and requeues are exhausted.

    Each endpoint's dialect is resolved from its URL (ESGF1 vs ESGF-NG), so a ranked
    list may mix both; a shared detection cache means each endpoint is probed at most
    once (and every endpoint we ship is in the registry, so no probe is needed).

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

    flavour_overrides
        Optional `{url_or_host: Flavour}` map pinning an endpoint's dialect.

    detection_cache
        Shared detection memo; a fresh one (shared across these endpoints) is used when
        omitted.

    mip_era
        The MIP era to bind to every endpoint's backend (`"CMIP6"` default), selecting
        the facet-name vocabulary for the file search.

    Returns
    -------
    :
        One configured, non-retrying client per endpoint, in the given order.
    """
    settings = settings or Settings()
    cache = detection_cache if detection_cache is not None else DetectionCache()
    era = get_profile(mip_era)
    return [
        ESGFSearchClient(
            endpoint,
            backend=resolve_backend(
                endpoint,
                overrides=flavour_overrides,
                cache=cache,
                probe_timeout=settings.timeout,
                era=era,
            ),
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
