"""
Robust, general access to a netCDF file's header (global attributes)

The ESGF *search* index omits most of a file's global attributes — including the
CMIP6 `parent_*` facets — so anything beyond the search facets has to be read from
the file itself.  Fortunately netCDF4/HDF5 can read **just the header** over HTTP
byte-range (the `#bytes` URL suffix): opening a file and reading its global
attributes transfers only a few KB, never the (multi-hundred-MB) data arrays.
Measured against a real node, a header-only open took ~1.2s versus ~45s to pull
the full variable, confirming the data is never touched.

This module is deliberately **independent of any particular use case**.  It reads
*all* global attributes into a plain `HeaderMetadata` record; parent resolution
(`parents.py`) is just one consumer that projects `parent_*` out of it.

Two facts shape the design:

- **A header describes a *simulation*, not a variable.**  The global attributes
  (`parent_*`, `branch_time_*`, `variant_label`, `source_id`, ...) are identical
  across every variable of the same `(source_id, experiment_id, variant_label)`,
  so we read **one file for the whole simulation** — a user who has `tas` and
  later wants `rsut` for the same simulation already has its header.
  `SimulationKey` / `simulation_key` capture that identity.

- **Reads are slow and nodes are flaky.**  `order_candidates` pools a simulation's
  mirror URLs and orders them (a caller-chosen `preferred_hosts` first, then HTTPS,
  minus `ignore_hosts`); `read_first_readable` tries them in turn so a dead host is
  skipped.  `with_timeout` bounds the pathological case — a node that connects then
  *stalls* for up to ~25 minutes — by running the read in a child process and
  killing it if it overruns (a stall is not an exception, so nothing else catches
  it).  Because isolation comes from that child process, the reads should be fanned
  out with a **thread** pool, not a process pool.
"""

from __future__ import annotations

import multiprocessing as mp
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from multiprocessing.connection import Connection, wait
from multiprocessing.process import BaseProcess
from typing import TypeVar
from urllib.parse import urlparse

from cmip_data_manager.esgf.models import DatasetRecord, FileRecord

T = TypeVar("T")

HTTP_SERVICE = "HTTPServer"
"""The `service` token marking a directly downloadable HTTP URL in `FileRecord.urls`."""

_URL_PARTS = 3
"""Number of `|`-separated fields in a `FileRecord.urls` entry (`url|mime|service`)."""

DEFAULT_READ_TIMEOUT = 90.0
"""
Default per-read wall-clock deadline in seconds.

A healthy header read was measured at ~1s from a nearby node and ~20s from a
distant one, so this is generous enough that a slow-but-alive node completes while
sitting far below the ~25-minute stall it exists to cut off.  Tune it towards a
high percentile of the *observed* healthy-read time once `NodeHealth` has data.
"""

DEFAULT_KILL_GRACE = 2.0
"""Seconds to wait for a terminated child to exit before escalating to `kill`."""


SimulationKey = tuple[str, str, str]
"""A simulation's identity: `(source_id, experiment_id, variant_label)`."""


def simulation_key(record: DatasetRecord) -> SimulationKey | None:
    """
    Return the `(source_id, experiment_id, variant_label)` a dataset belongs to

    This is the grain at which headers are read and stored: every variable of the
    same simulation shares one header, so this key (not the `variable_id`) is what
    a cached header is filed under.

    Parameters
    ----------
    record
        The dataset to key.

    Returns
    -------
    :
        The simulation key, or `None` if the dataset is missing any of the three
        identifying facets.

    Examples
    --------
    >>> from cmip_data_manager.esgf.models import DatasetRecord
    >>> record = DatasetRecord(
    ...     id="d",
    ...     source_id="ACCESS-ESM1-5",
    ...     experiment_id="ssp245",
    ...     variant_label="r1i1p1f1",
    ...     variable_id="tas",
    ...     raw={},
    ... )
    >>> simulation_key(record)
    ('ACCESS-ESM1-5', 'ssp245', 'r1i1p1f1')
    """
    if record.source_id and record.experiment_id and record.variant_label:
        return (record.source_id, record.experiment_id, record.variant_label)
    return None


@dataclass(frozen=True)
class HeaderMetadata:
    """
    A netCDF file's global attributes, read from its header

    `attrs` holds every global attribute read (values coerced to `str`), so it
    carries whatever a use case might want — `parent_*`, `branch_time_in_parent`,
    `tracking_id`, grid and forcing metadata, and so on — without re-reading.
    """

    attrs: dict[str, str]
    """The global attributes read, keyed by name."""

    source_url: str | None = None
    """The mirror the header was read from (provenance)."""

    def get(self, name: str) -> str | None:
        """
        Return one global attribute, or `None` if the file did not declare it

        Parameters
        ----------
        name
            Attribute name (e.g. `"parent_experiment_id"`).

        Returns
        -------
        :
            The attribute value, or `None`.

        Examples
        --------
        >>> HeaderMetadata(attrs={"parent_experiment_id": "historical"}).get(
        ...     "parent_experiment_id"
        ... )
        'historical'
        >>> HeaderMetadata(attrs={}).get("missing") is None
        True
        """
        return self.attrs.get(name)


def http_download_urls(file: FileRecord) -> list[str]:
    """
    Return all of a file's directly downloadable HTTP URLs (its mirrors)

    `FileRecord.urls` entries are formatted `url|mime-type|service`; the
    downloadable ones use the `HTTPServer` service.

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


def order_candidates(
    urls: Iterable[str],
    *,
    preferred_hosts: Sequence[str] = (),
    ignore_hosts: frozenset[str] = frozenset(),
) -> list[str]:
    """
    De-duplicate and rank mirror URLs, best first

    Ordering is: any `preferred_hosts` first (in the order given — e.g. a nearby
    node such as `esgf.nci.org.au`), then HTTPS before plain HTTP (the byte-range
    driver only accepts HTTPS), preserving input order within a tier.  Hosts in
    `ignore_hosts` are dropped entirely.

    Parameters
    ----------
    urls
        Candidate mirror URLs (typically pooled across a simulation's files).

    preferred_hosts
        Hostnames to try first, most-preferred first.

    ignore_hosts
        Hostnames to exclude (e.g. known-dead nodes from `NodeHealth`).

    Returns
    -------
    :
        The kept URLs, ordered best-first.

    Examples
    --------
    >>> order_candidates(
    ...     [
    ...         "http://far/f.nc",
    ...         "https://far/f.nc",
    ...         "https://nci.org.au/f.nc",
    ...     ],
    ...     preferred_hosts=("nci.org.au",),
    ... )
    ['https://nci.org.au/f.nc', 'https://far/f.nc', 'http://far/f.nc']
    """
    kept: dict[str, None] = {}
    for url in urls:
        if urlparse(url).hostname not in ignore_hosts:
            kept.setdefault(url, None)

    def rank(url: str) -> tuple[int, int]:
        host = urlparse(url).hostname or ""
        try:
            preference = list(preferred_hosts).index(host)
        except ValueError:
            preference = len(preferred_hosts)
        return (preference, 0 if urlparse(url).scheme == "https" else 1)

    return sorted(kept, key=rank)


def candidate_urls_for_files(
    files: Iterable[FileRecord],
    *,
    preferred_hosts: Sequence[str] = (),
    ignore_hosts: frozenset[str] = frozenset(),
) -> list[str]:
    """
    Pool and rank every mirror URL across a simulation's files

    Because one header serves the whole simulation, we only need *one* readable
    file: this pools the `HTTPServer` mirrors of all the given files (any variable,
    any time-chunk, any replica) and ranks them with `order_candidates`.

    Parameters
    ----------
    files
        Files belonging to a single simulation.

    preferred_hosts
        Hostnames to try first (see `order_candidates`).

    ignore_hosts
        Hostnames to exclude.

    Returns
    -------
    :
        Ranked candidate URLs, best-first (possibly empty).
    """
    urls: list[str] = []
    for file in files:
        urls.extend(http_download_urls(file))
    return order_candidates(
        urls, preferred_hosts=preferred_hosts, ignore_hosts=ignore_hosts
    )


def read_first_readable(
    candidates: Iterable[str], reader: Callable[[str], T]
) -> T | None:
    """
    Return the result of the first candidate mirror that reads

    Candidates that raise `OSError` — a dead node, a stall surfaced as
    `HeaderReadTimeout`, a missing byte-range endpoint — are skipped so one bad
    host does not lose an otherwise-available file.

    Parameters
    ----------
    candidates
        Mirror URLs to try, best-first.

    reader
        Reads one URL (e.g. `read_header`, optionally wrapped in `with_timeout`).

    Returns
    -------
    :
        The first successful read, or `None` if every candidate failed.
    """
    for url in candidates:
        try:
            return reader(url)
        except OSError:
            continue
    return None


def read_header(url: str, attrs: Iterable[str] | None = None) -> HeaderMetadata:
    # pragma: no cover - needs netCDF4 and the network
    """
    Read a netCDF file's global attributes over HTTP, header only

    Opens the file in `netCDF4`'s byte-range mode (the `#bytes` URL suffix) so only
    the header is transferred, and reads its global attributes.  Requires the
    optional `netcdf` extra (`pip install cmip-data-manager[netcdf]`).

    Parameters
    ----------
    url
        Directly downloadable `HTTPServer` URL of the netCDF file.

    attrs
        If given, read only these global attributes (those actually present);
        otherwise read every global attribute.

    Returns
    -------
    :
        The global attributes read, with the source URL recorded.
    """
    import netCDF4  # noqa: PLC0415 - optional `netcdf` extra, imported lazily

    with netCDF4.Dataset(f"{url}#bytes") as dataset:
        available = dataset.ncattrs()
        names = available if attrs is None else [n for n in attrs if n in available]
        values = {name: str(dataset.getncattr(name)) for name in names}
    return HeaderMetadata(attrs=values, source_url=url)


class HeaderReadTimeout(OSError):
    """
    Raised when a header read exceeds its deadline (a stalled data node)

    Subclasses `OSError` so `read_first_readable` skips a stalled mirror like any
    other dead host.
    """

    def __init__(self, url: str, seconds: float) -> None:
        self.url = url
        self.seconds = seconds
        super().__init__(
            f"Reading {url!r} exceeded its {seconds:g}s deadline; treating the "
            f"data node as stalled."
        )


class HeaderReadCrashed(OSError):
    """
    Raised when the child process died without returning a result

    Distinct from `HeaderReadTimeout`: the read did not stall, the worker crashed
    (e.g. a libnetcdf/HDF5 failure on a corrupt header).  Also an `OSError`, so it
    too falls through to the next mirror.
    """

    def __init__(self, url: str, exitcode: int | None) -> None:
        self.url = url
        self.exitcode = exitcode
        super().__init__(
            f"Reading {url!r} crashed the worker process (exit code {exitcode})."
        )


def _run_reader(reader: Callable[[str], T], url: str, conn: Connection) -> None:
    """Child entry point: send `("ok", result)` or `("err", exception)` back."""
    try:
        # Relay *any* failure (including the reader's OSErrors) to the parent.
        conn.send(("ok", reader(url)))
    except BaseException as exc:
        try:
            conn.send(("err", exc))
        except Exception:  # noqa: S110 - unpicklable error; parent sees EOF instead
            pass
    finally:
        conn.close()


def _terminate(proc: BaseProcess, grace: float) -> None:
    """Stop a process, escalating from `terminate` to `kill` if it clings on."""
    proc.terminate()
    proc.join(grace)
    if proc.is_alive():
        proc.kill()
        proc.join()


def with_timeout(
    reader: Callable[[str], T],
    seconds: float = DEFAULT_READ_TIMEOUT,
    *,
    grace: float = DEFAULT_KILL_GRACE,
) -> Callable[[str], T]:
    """
    Wrap a reader so any single read is killed after `seconds`

    The wrapped reader runs in a spawned child process; if it has not returned
    within `seconds` the child is terminated (then killed) and `HeaderReadTimeout`
    is raised.  A child that dies without a result raises `HeaderReadCrashed`.
    Both are `OSError`s, so `read_first_readable` moves on to the next mirror.

    `reader`, its argument and its return value must be picklable, since they cross
    a process boundary under the `spawn` start method — a module-level function
    such as `read_header` qualifies; a lambda or closure does not.  Because the
    child provides the isolation, fan these reads out with a **thread** pool.

    Parameters
    ----------
    reader
        The underlying `url -> value` read to bound (e.g. `read_header`).

    seconds
        Wall-clock deadline for a single read.

    grace
        Seconds to wait after `terminate` before escalating to `kill`.

    Returns
    -------
    :
        A reader with the same `url -> value` contract that never blocks for
        materially longer than `seconds`.
    """
    ctx = mp.get_context("spawn")

    def read(url: str) -> T:
        receiver, sender = ctx.Pipe(duplex=False)
        proc = ctx.Process(target=_run_reader, args=(reader, url, sender), daemon=True)
        proc.start()
        # The parent holds no write end, so it sees EOF the instant the child exits.
        sender.close()

        if not wait([receiver], timeout=seconds):
            _terminate(proc, grace)
            receiver.close()
            raise HeaderReadTimeout(url, seconds)

        try:
            status, payload = receiver.recv()
        except EOFError:
            _terminate(proc, grace)
            raise HeaderReadCrashed(url, proc.exitcode) from None
        finally:
            receiver.close()

        proc.join(grace)
        if proc.is_alive():
            _terminate(proc, grace)
        if status == "err":
            raise payload
        result: T = payload
        return result

    return read
