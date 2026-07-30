"""
Resolve which search dialect an endpoint speaks — so callers pass only URLs

The user never states "ESGF1" or "ESGF-NG": they supply a **ranked list of endpoint
links** (e.g. CEDA, ORNL, metagrid-west, east) and the endpoint *identity* decides
the dialect.  This module turns a base URL into a `Flavour` (and a ready
`SearchBackend`) three ways, in priority order:

1. **explicit override** — a caller-supplied `{url_or_host: Flavour}` map wins
   outright (used for tests, and to pin a new/odd endpoint before it is well known);
2. **known-endpoint registry** — the hosts we already ship are mapped directly, so no
   network probe is needed for any of the ranked links;
3. **probe** — for an unknown host, GET the endpoint root once: a STAC `Catalog`
   (advertising CQL2/OGC-Features) is ESGF-NG, anything else defaults to ESGF1.

Every verdict is memoised per endpoint in a `DetectionCache`, so a probe costs at most
one request per endpoint per session (and a shared cache spans a whole ranked list).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from urllib.parse import urlparse

from cmip_data_manager.esgf.backends.base import Flavour, SearchBackend
from cmip_data_manager.esgf.backends.esgf1 import Esgf1Backend
from cmip_data_manager.esgf.backends.esgf_ng import EsgfNgBackend
from cmip_data_manager.esgf.concurrency import Fetch, httpx_fetch

_KNOWN_HOSTS: dict[str, Flavour] = {
    # ESGF1 (esg-search / Solr) — the current ranked file-search endpoints.
    "esgf.ceda.ac.uk": Flavour.ESGF1,
    "esgf-node.ornl.gov": Flavour.ESGF1,
    "metagrid.esgf-west.org": Flavour.ESGF1,
    # ESGF-NG (STAC / CQL2) — east.
    "search.east.esgf.io": Flavour.ESGF_NG_EAST,
    "discovery.east.esgf.io": Flavour.ESGF_NG_EAST,
    "api.stac.esgf.ceda.ac.uk": Flavour.ESGF_NG_EAST,
    # ESGF-NG (STAC / CQL2) — west.
    "search.west.esgf.io": Flavour.ESGF_NG_WEST,
    "discovery.west.esgf.io": Flavour.ESGF_NG_WEST,
    "discovery.production.esgf-west.org": Flavour.ESGF_NG_WEST,
}
"""Hosts whose dialect we already know, matched so the `/search` path is irrelevant."""

DEFAULT_PROBE_TIMEOUT = 20.0
"""Timeout (seconds) for the endpoint-root probe when a host is unknown."""


@dataclass
class DetectionCache:
    """Per-session memo of endpoint → `Flavour` (so each endpoint is probed once)."""

    verdicts: dict[str, Flavour] = field(default_factory=dict)

    def get(self, base_url: str) -> Flavour | None:
        """Return the cached flavour for `base_url`, or `None` if not yet resolved."""
        return self.verdicts.get(base_url)

    def set(self, base_url: str, flavour: Flavour) -> None:
        """Record the resolved flavour for `base_url`."""
        self.verdicts[base_url] = flavour


def detect_flavour(
    base_url: str,
    *,
    overrides: Mapping[str, Flavour] | None = None,
    fetch: Fetch | None = None,
    cache: DetectionCache | None = None,
    probe_timeout: float = DEFAULT_PROBE_TIMEOUT,
) -> Flavour:
    """
    Resolve the search dialect of `base_url`

    Parameters
    ----------
    base_url
        The endpoint's search URL (e.g. `.../search` or a metagrid proxy).

    overrides
        Optional `{url_or_host: Flavour}` map that wins over registry and probe.  Keys
        may be the full base URL or just the host.

    fetch
        Transport for the probe (only used for an unknown host).  Defaults to an
        httpx-backed fetch built with `probe_timeout`.

    cache
        Memo of previous verdicts; a shared cache avoids re-probing a ranked list.

    probe_timeout
        Timeout for the probe fetch when one is built here.

    Returns
    -------
    :
        The resolved `Flavour` (defaulting to `ESGF1` if a probe is inconclusive).
    """
    if cache is not None and (cached := cache.get(base_url)) is not None:
        return cached

    flavour = _resolve(base_url, overrides, fetch, probe_timeout)
    if cache is not None:
        cache.set(base_url, flavour)
    return flavour


def _resolve(
    base_url: str,
    overrides: Mapping[str, Flavour] | None,
    fetch: Fetch | None,
    probe_timeout: float,
) -> Flavour:
    """Apply the override → registry → probe precedence for one endpoint."""
    host = urlparse(base_url).hostname or ""
    if overrides:
        if base_url in overrides:
            return overrides[base_url]
        if host in overrides:
            return overrides[host]
    if host in _KNOWN_HOSTS:
        return _KNOWN_HOSTS[host]
    return _probe(base_url, host, fetch, probe_timeout)


def _probe(
    base_url: str, host: str, fetch: Fetch | None, probe_timeout: float
) -> Flavour:
    """GET the endpoint root once; a STAC `Catalog` is ESGF-NG, else default ESGF1."""
    do_fetch = fetch if fetch is not None else httpx_fetch(timeout=probe_timeout)
    root = _root_url(base_url)
    try:
        payload = do_fetch(root, {})
    except Exception:  # any probe failure (non-JSON, 404, transport) => assume ESGF1
        return Flavour.ESGF1
    if _is_stac_root(payload):
        return Flavour.ESGF_NG_WEST if "west" in host else Flavour.ESGF_NG_EAST
    return Flavour.ESGF1


def _root_url(base_url: str) -> str:
    """Return the API root (`scheme://host/`) to probe for a STAC catalog document."""
    parsed = urlparse(base_url)
    return f"{parsed.scheme}://{parsed.netloc}/"


def _is_stac_root(payload: object) -> bool:
    """Whether a root payload is a STAC `Catalog` advertising CQL2/OGC-Features."""
    if not isinstance(payload, dict):
        return False
    if payload.get("type") != "Catalog":
        return False
    conforms = payload.get("conformsTo", [])
    if not isinstance(conforms, list):
        return False
    return any(
        isinstance(c, str) and ("cql2" in c or "ogcapi-features" in c or "stac" in c)
        for c in conforms
    )


def backend_for(flavour: Flavour) -> SearchBackend:
    """
    Build the `SearchBackend` for a resolved `Flavour`

    Parameters
    ----------
    flavour
        The dialect an endpoint speaks.

    Returns
    -------
    :
        A backend instance: `Esgf1Backend` for ESGF1, an `EsgfNgBackend` tuned for the
        east/west deployment otherwise (west lower-cases collection ids).
    """
    if flavour is Flavour.ESGF1:
        return Esgf1Backend()
    if flavour is Flavour.ESGF_NG_WEST:
        return EsgfNgBackend(flavour=flavour, lowercase_collection=True)
    return EsgfNgBackend(flavour=Flavour.ESGF_NG_EAST)


def resolve_backend(
    base_url: str,
    *,
    overrides: Mapping[str, Flavour] | None = None,
    fetch: Fetch | None = None,
    cache: DetectionCache | None = None,
    probe_timeout: float = DEFAULT_PROBE_TIMEOUT,
) -> SearchBackend:
    """Resolve an endpoint's dialect and return a ready `SearchBackend` for it."""
    flavour = detect_flavour(
        base_url,
        overrides=overrides,
        fetch=fetch,
        cache=cache,
        probe_timeout=probe_timeout,
    )
    return backend_for(flavour)
