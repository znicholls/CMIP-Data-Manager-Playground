"""
Resolve a dataset's CMIP6 parent from its files' netCDF headers

The ESGF *search* index does not expose the CMIP6 parent facets
(`parent_source_id`, `parent_variant_label`, `parent_experiment_id`,
`parent_activity_id`): they live in each netCDF file's **global attributes**.
This matters for use cases such as `uc2_forcing`, where an `abrupt-4xCO2` run and
its `piControl` parent are legitimately different variants, so matching on
`variant_label` alone misses valid pairs — the link is the parent metadata.

We read those attributes straight from the header over HTTP.  netCDF4/HDF5
supports byte-range reads via the `#bytes` URL suffix, so only the header is
fetched, never the whole (possibly multi-gigabyte) file.

A dataset is made up of several files, all of which should agree on the parent.
`resolve_dataset_parent` reads every file's header: if they agree it returns the
shared `ParentInfo`, and if they disagree it raises `ParentMetadataConflictError`
rather than guessing.  Helpers to reconcile such conflicts may come later.

The actual header read is a dependency-injection seam (`reader`): the default
`read_parent_info` uses `netCDF4` (the optional `netcdf` extra), while tests and
callers with their own transport can supply any `str -> ParentInfo` callable.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from functools import partial
from typing import Any
from urllib.parse import urlparse

from cmip_data_manager.esgf.concurrency import MapFn, serial_map
from cmip_data_manager.esgf.models import FileRecord

HTTP_SERVICE = "HTTPServer"
"""The `service` token marking a directly downloadable HTTP URL in `FileRecord.urls`."""

_URL_PARTS = 3
"""Number of `|`-separated fields in a `FileRecord.urls` entry (`url|mime|service`)."""


@dataclass(frozen=True, order=True)
class ParentInfo:
    """
    The identity of a dataset's parent, read from netCDF global attributes

    Every field mirrors a `parent_*` global attribute and may be `None` when the
    file does not declare it (e.g. `piControl`, which has no parent).
    """

    source_id: str | None = None
    variant_label: str | None = None
    experiment_id: str | None = None
    activity_id: str | None = None


class ParentMetadataConflictError(ValueError):
    """
    Raised when a dataset's files disagree on their parent metadata

    Carries the distinct `ParentInfo` values found so a caller (or a future
    reconciliation helper) can inspect the conflict.
    """

    def __init__(self, infos: set[ParentInfo]) -> None:
        self.infos = infos
        listed = ", ".join(str(info) for info in sorted(infos))
        super().__init__(
            f"Dataset files disagree on parent metadata: {len(infos)} distinct "
            f"values found: {listed}"
        )


HeaderReader = Callable[[str], ParentInfo]
"""Reads a single file's `ParentInfo` from its URL (the DI seam)."""


def http_download_urls(file: FileRecord) -> list[str]:
    """
    Return all of a file's directly downloadable HTTP URLs (its mirrors)

    `FileRecord.urls` entries are formatted `url|mime-type|service`; the
    downloadable ones are the `HTTPServer` service.  A single file record usually
    lists one, but pooling these across a dataset's replicas yields every host a
    given file is available from.

    Parameters
    ----------
    file
        File whose URLs to search.

    Returns
    -------
    :
        The `HTTPServer` URLs, in the order listed (possibly empty).
    """
    urls = []
    for entry in file.urls:
        parts = entry.split("|")
        if len(parts) == _URL_PARTS and parts[2] == HTTP_SERVICE:
            urls.append(parts[0])
    return urls


def http_download_url(file: FileRecord) -> str | None:
    """
    Return a file's first directly downloadable HTTP URL, if it has one

    Parameters
    ----------
    file
        File whose URLs to search.

    Returns
    -------
    :
        The first `HTTPServer` URL, or `None` if the file exposes none.
    """
    urls = http_download_urls(file)
    return urls[0] if urls else None


def _ordered_candidates(urls: Iterable[str], ignore_hosts: frozenset[str]) -> list[str]:
    """De-duplicate URLs, drop ignore-listed hosts, and order HTTPS first."""
    kept: dict[str, None] = {}
    for url in urls:
        if urlparse(url).hostname not in ignore_hosts:
            kept.setdefault(url, None)
    return sorted(kept, key=lambda url: urlparse(url).scheme != "https")


def _read_first_readable(
    candidates: list[str], reader: HeaderReader
) -> ParentInfo | None:
    """
    Read the first candidate mirror whose header loads

    Mirrors that raise `OSError` (dead node, no byte-range support) are skipped so a
    single bad host does not lose an otherwise-available file.
    """
    for url in candidates:
        try:
            return reader(url)
        except OSError:
            continue
    return None


def variable_url_groups(
    files: Sequence[FileRecord], ignore_hosts: frozenset[str] = frozenset()
) -> list[list[str]]:
    """
    Group a dataset's files into one ordered mirror list per `variable_id`

    The parent (`parent_*`) attributes are per-simulation globals, identical across a
    variable's time-chunks, so one read per variable suffices.  Each group pools that
    variable's HTTPServer URLs (across time-chunks and replicas), dropped of
    `ignore_hosts` and ordered HTTPS-first; empty groups are omitted.

    Parameters
    ----------
    files
        The files making up a dataset (may span variables, time-chunks, replicas).

    ignore_hosts
        Hostnames to exclude.

    Returns
    -------
    :
        One ordered candidate-URL list per variable that has a usable mirror.
    """
    mirrors_by_variable: dict[str | None, list[str]] = defaultdict(list)
    for file in files:
        mirrors_by_variable[file.variable_id].extend(http_download_urls(file))
    return [
        candidates
        for urls in mirrors_by_variable.values()
        if (candidates := _ordered_candidates(urls, ignore_hosts))
    ]


def read_parent_info(url: str) -> ParentInfo:  # pragma: no cover - needs netCDF4
    """
    Read one netCDF file's parent metadata over HTTP, header only

    Opens the file in `netCDF4`'s byte-range mode (the `#bytes` URL suffix) so
    only the header is transferred, and reads the `parent_*` global attributes.
    Requires the optional `netcdf` extra (`pip install cmip-data-manager[netcdf]`).

    Parameters
    ----------
    url
        Directly downloadable `HTTPServer` URL of the netCDF file.

    Returns
    -------
    :
        The parent identity declared in the file's global attributes.
    """
    import netCDF4  # noqa: PLC0415 - optional `netcdf` extra, imported lazily

    with netCDF4.Dataset(f"{url}#bytes") as dataset:
        return ParentInfo(
            source_id=_attr(dataset, "parent_source_id"),
            variant_label=_attr(dataset, "parent_variant_label"),
            experiment_id=_attr(dataset, "parent_experiment_id"),
            activity_id=_attr(dataset, "parent_activity_id"),
        )


def _attr(dataset: Any, name: str) -> str | None:  # pragma: no cover - needs netCDF4
    """Return a global attribute as a string, or `None` when absent."""
    value = getattr(dataset, name, None)
    return None if value is None else str(value)


def resolve_dataset_parent(
    files: Sequence[FileRecord],
    *,
    reader: HeaderReader = read_parent_info,
    map_fn: MapFn = serial_map,
    ignore_hosts: frozenset[str] = frozenset(),
) -> ParentInfo:
    """
    Read one header per variable and return the parent they agree on

    The parent (`parent_*`) attributes are per-simulation globals, identical across
    a variable's time-chunks, so we read only **one header per `variable_id`** — its
    files' mirror URLs are pooled, ordered HTTPS-first (minus `ignore_hosts`) and
    tried in turn until one reads, so a dead data node does not lose that variable.
    Requiring the variables to agree still catches the realistic inconsistency (a
    mislabelled variable); if they disagree the disagreement is raised.

    Reading one header per variable (rather than every file) keeps this tractable:
    a single simulation can have well over a thousand files.

    Parameters
    ----------
    files
        The files making up a single dataset (may span variables, time-chunks and
        replica data nodes).

    reader
        Reads one file's `ParentInfo` from a URL.  Defaults to `read_parent_info`
        (netCDF4 over HTTP); inject a fake for testing or an alternative transport.

    map_fn
        Strategy for reading the per-variable headers.  Defaults to serial; pass
        `process_pool_map(...)` to read them in parallel (netCDF is not
        thread-safe, so this must be process- rather than thread-based).

    ignore_hosts
        Hostnames to never read from (e.g. known-unresponsive data nodes).

    Returns
    -------
    :
        The `ParentInfo` shared by every variable that could be read.

    Raises
    ------
    ValueError
        If `files` is empty, or no header could be read.

    ParentMetadataConflictError
        If the variables disagree on their parent metadata.

    Examples
    --------
    A fake reader keeps the example offline; real use relies on `read_parent_info`:

    >>> from cmip_data_manager.esgf.models import FileRecord
    >>> urls = ("http://host|x|HTTPServer",)
    >>> files = [
    ...     FileRecord(id="f1", dataset_id="d", variable_id="tas", urls=urls, raw={}),
    ...     FileRecord(id="f2", dataset_id="d", variable_id="rsdt", urls=urls, raw={}),
    ... ]
    >>> parent = ParentInfo(source_id="M", variant_label="r1i1p1f1")
    >>> resolve_dataset_parent(files, reader=lambda _url: parent).variant_label
    'r1i1p1f1'
    """
    if not files:
        msg = "Cannot resolve a parent from zero files."
        raise ValueError(msg)

    groups = variable_url_groups(files, ignore_hosts)
    if not groups:
        msg = "No file exposed an HTTP URL to read parent metadata from."
        raise ValueError(msg)

    results = map_fn(partial(_read_first_readable, reader=reader), groups)
    infos: set[ParentInfo] = {info for info in results if info is not None}
    if not infos:
        msg = "No file header could be read for parent metadata."
        raise ValueError(msg)
    if len(infos) > 1:
        raise ParentMetadataConflictError(infos)
    return next(iter(infos))
