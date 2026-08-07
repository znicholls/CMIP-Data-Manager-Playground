"""
Download a single ESGF file from a data node to local disk

The download counterpart of `headers.read_header`: where a header read pulls a few
bytes over a `#bytes` byte-range, this streams the **whole** file from an
`HTTPServer` URL to disk.  It is the leaf that the download dispatcher fans across
mirrors, and it is deliberately self-contained so it can be composed with the same
`with_retry` and node-health machinery the header path uses.

Design choices (see `design/download-step-plan.md`):

- **httpx streaming, in-thread.**  Unlike the netCDF byte-range reader — whose reads
  block in uninterruptible C and so need a subprocess kill (`headers.with_timeout`) —
  httpx honours timeouts natively, so a stalled transfer is cut off in-thread by a
  per-chunk read timeout.  This keeps `.part` resume and byte-accounting simple.  The
  isolation is a **pluggable seam**: this function is a module-level, picklable
  callable, so if in-thread timeouts ever prove to leave wedged sockets it can be
  wrapped in the existing subprocess isolation *without* changing the orchestrator.
- **`.part` + atomic rename.**  Bytes stream to a sibling `*.part` file, which is
  `fsync`ed and atomically `os.replace`d onto the final path only once complete and
  (where a checksum is available) verified — a killed run never leaves a truncated
  file masquerading as done.
- **Range resume.**  A pre-existing `*.part` is continued with an HTTP `Range`
  request; a server that ignores it (answering `200` instead of `206`) transparently
  restarts from scratch.
- **Checksum-type-aware verification.**  ESGF1 supplies an explicit
  `checksum`/`checksum_type`; ESGF-NG supplies a STAC `file:checksum` **multihash**
  with no separate type — the algorithm is decoded from the multihash prefix.  A file
  with no usable checksum is downloaded and returned `verified=False` (the caller
  records it `unverified`), never failed.

Because the transfer runs in-thread, this leaf classifies httpx failures directly
into `OSError` subclasses (a *block*, a *host fault*, a *timeout* or a *checksum
mismatch* fall through to the next mirror; a plain refusal/transport blip stays a
retryable `OSError`), so `with_retry` and the dispatcher's next-mirror fallthrough
work unchanged.
"""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from pathlib import Path

import httpx

from cmip_data_manager.esgf.headers import DEFAULT_CONNECT_TIMEOUT

DEFAULT_DOWNLOAD_CHUNK_BYTES = 1 << 20
"""Streaming/hashing chunk size in bytes (1 MiB) — the unit written per iteration."""

DEFAULT_DOWNLOAD_READ_TIMEOUT = 60.0
"""
Per-chunk read deadline in seconds.

httpx applies the read timeout to each socket read, so a slow-but-steady transfer
that keeps delivering bytes never trips it; only a genuinely stalled socket (no bytes
for this long) is cut off.  Distinct from the header read timeout: a full-file
download legitimately takes far longer overall, but each *chunk* should still arrive
promptly on a live node.
"""

_OK_STATUS = 200
_PARTIAL_STATUS = 206
_BAD_STATUS = 400
_BLOCK_STATUSES = frozenset({403, 429, 503})
"""HTTP statuses that mark a node-level block/rate-limit (never retried on the host)."""

_MISSING_STATUSES = frozenset({404, 410})
"""HTTP statuses that mark this file absent *at this mirror* (try the next mirror)."""

_MULTIHASH_ALGOS = {0x11: "sha1", 0x12: "sha256", 0x13: "sha512", 0xD5: "md5"}
"""Multihash function codes (STAC `file:checksum`) mapped to `hashlib` names."""

_KNOWN_ALGOS = frozenset({"md5", "sha1", "sha256", "sha512"})
"""`hashlib` algorithm names we will verify against."""

__all__ = [
    "DEFAULT_DOWNLOAD_CHUNK_BYTES",
    "DEFAULT_DOWNLOAD_READ_TIMEOUT",
    "ChecksumMismatch",
    "DownloadBlocked",
    "DownloadError",
    "DownloadHostFault",
    "DownloadInfo",
    "DownloadTimeout",
    "download_to_path",
    "resolve_checksum",
    "verify_file",
]


class DownloadError(OSError):
    """
    Base for a download failure that should fall through to the next mirror

    Subclasses `OSError` so the dispatcher (and `read_first_readable`) skip a failed
    mirror like any dead host, and is listed in `with_retry`'s `give_up_on` so it is
    **not** retried on the same URL — a fresh mirror is tried instead.  A *retryable*
    failure (a plain refusal or a transient transport blip) is raised as a plain
    `OSError`, not a `DownloadError`, so `with_retry` does retry it on the same host.
    """


class DownloadTimeout(DownloadError):
    """Raised when a transfer stalls past its per-chunk read deadline."""

    def __init__(self, url: str, seconds: float) -> None:
        self.url = url
        self.seconds = seconds
        super().__init__(
            f"Downloading {url!r} stalled past its {seconds:g}s read deadline; "
            f"treating the data node as stalled."
        )


class DownloadBlocked(DownloadError):
    """Raised when a data node rate-limits or refuses us (HTTP 429/403/503)."""

    def __init__(self, url: str, reason: str) -> None:
        self.url = url
        self.reason = reason
        super().__init__(
            f"Data node for {url!r} signalled a block/rate-limit: {reason}"
        )


class DownloadHostFault(DownloadError):
    """Raised on a node-level connection fault (SSL/DNS/connect-timeout)."""

    def __init__(self, url: str, reason: str) -> None:
        self.url = url
        self.reason = reason
        super().__init__(
            f"Data node for {url!r} has a connection-level fault: {reason}"
        )


class ChecksumMismatch(DownloadError):
    """Raised when downloaded bytes fail checksum verification."""

    def __init__(self, url: str, *, expected: str, actual: str, algo: str) -> None:
        self.url = url
        self.expected = expected
        self.actual = actual
        self.algo = algo
        super().__init__(
            f"{algo} checksum mismatch for {url!r}: expected {expected}, got {actual}"
        )


@dataclass(frozen=True)
class DownloadInfo:
    """The outcome of one successful `download_to_path` call."""

    url: str
    """The mirror URL the bytes were pulled from."""

    path: Path
    """The final on-disk path the file was atomically moved to."""

    bytes_downloaded: int
    """Bytes transferred **this call** (excludes bytes a resume reused from `.part`);
    this is what per-attempt throughput (`bytes/seconds`) is measured from."""

    total_bytes: int
    """Final size of the file on disk in bytes."""

    resumed: bool
    """Whether this call continued a partial `.part` via an HTTP `Range` request."""

    verified: bool
    """Whether the bytes were checked against a published checksum."""

    verified_algo: str | None
    """The digest algorithm used to verify (`md5`/`sha256`/…), or `None` if the file
    could not be verified (no usable checksum)."""


def _normalise_algo(name: str) -> str | None:
    """Return the `hashlib` name for a checksum-type string, or `None` if unknown."""
    key = name.strip().lower().replace("-", "")
    return key if key in _KNOWN_ALGOS else None


def _decode_multihash(value: str) -> tuple[str, str] | None:
    """
    Decode a STAC `file:checksum` multihash into `(algorithm, hex_digest)`

    A multihash is `<function-code><digest-length><digest>` as hex; the function code
    identifies the algorithm.  Returns `None` if `value` is not a well-formed multihash
    over a recognised algorithm (so the caller falls back to *unverified*).
    """
    try:
        raw = bytes.fromhex(value.strip())
    except ValueError:
        return None
    if len(raw) < 2:  # noqa: PLR2004 - need at least a code byte and a length byte
        return None
    algo = _MULTIHASH_ALGOS.get(raw[0])
    length = raw[1]
    digest = raw[2:]
    if algo is None or len(digest) != length:
        return None
    return algo, digest.hex()


def resolve_checksum(
    expected_checksum: str | None, checksum_type: str | None
) -> tuple[str, str] | None:
    """
    Resolve a published checksum into `(hashlib_algorithm, expected_hex)`

    Handles both backends' shapes: ESGF1 supplies an explicit `checksum_type`
    (`"md5"`/`"SHA256"`/…) alongside a plain-hex `checksum`; ESGF-NG supplies a STAC
    `file:checksum` **multihash** with `checksum_type` `None`, from which the algorithm
    is decoded.

    Parameters
    ----------
    expected_checksum
        The published digest (plain hex for ESGF1, multihash hex for ESGF-NG).

    checksum_type
        The digest algorithm if the backend gave one, else `None`.

    Returns
    -------
    :
        `(algorithm, expected_hex)` if the file can be verified, else `None`.

    Examples
    --------
    >>> resolve_checksum("ABCD", "SHA256")
    ('sha256', 'abcd')
    >>> resolve_checksum("1220" + "aa" * 32, None)[0]  # a sha2-256 multihash
    'sha256'
    >>> resolve_checksum(None, None) is None
    True
    """
    if not expected_checksum:
        return None
    if checksum_type:
        algo = _normalise_algo(checksum_type)
        if algo is not None:
            return algo, expected_checksum.strip().lower()
        return None
    return _decode_multihash(expected_checksum)


def verify_file(
    path: str | os.PathLike[str],
    expected_checksum: str | None,
    checksum_type: str | None,
    *,
    chunk_bytes: int = DEFAULT_DOWNLOAD_CHUNK_BYTES,
) -> tuple[bool, str | None]:
    """
    Verify a file on disk against a published checksum

    Used both by the download leaf (via the incremental hash it keeps while streaming)
    and by the orchestrator's skip-if-already-present check.

    Parameters
    ----------
    path
        The file to hash.

    expected_checksum, checksum_type
        The published digest and its type; see `resolve_checksum`.

    chunk_bytes
        Read/hash block size.

    Returns
    -------
    :
        `(verified, algorithm)`.  `verified` is `False` with `algorithm` `None` when
        the file has no usable checksum to check against.
    """
    resolved = resolve_checksum(expected_checksum, checksum_type)
    if resolved is None:
        return (False, None)
    algo, expected_hex = resolved
    hasher = hashlib.new(algo)
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(chunk_bytes), b""):
            hasher.update(block)
    return (hasher.hexdigest().lower() == expected_hex, algo)


def _part_path(dest: Path) -> Path:
    """Return the sibling `*.part` staging path for a destination file."""
    return dest.parent / (dest.name + ".part")


def download_to_path(  # noqa: PLR0913 - a leaf I/O op; every knob keyword-defaulted
    url: str,
    dest: str | os.PathLike[str],
    *,
    expected_checksum: str | None = None,
    checksum_type: str | None = None,
    resume: bool = True,
    chunk_bytes: int = DEFAULT_DOWNLOAD_CHUNK_BYTES,
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
    read_timeout: float = DEFAULT_DOWNLOAD_READ_TIMEOUT,
    client: httpx.Client | None = None,
) -> DownloadInfo:
    """
    Stream `url` to `dest`, resuming, verifying and atomically renaming

    Bytes go to `dest`'s sibling `*.part`, then — once complete and (where possible)
    checksum-verified — the `*.part` is `fsync`ed and atomically moved onto `dest`.

    Parameters
    ----------
    url
        The `HTTPServer` mirror URL to download.

    dest
        Final path to write (its parent directories are created if needed).

    expected_checksum, checksum_type
        Published digest to verify against; see `resolve_checksum`.  When no usable
        checksum is available the file is still downloaded and returned
        `verified=False`.

    resume
        When true, continue a pre-existing `*.part` via an HTTP `Range` request.

    chunk_bytes
        Streaming/hashing chunk size.

    connect_timeout, read_timeout
        Connect deadline and per-chunk read-stall deadline.

    client
        An optional httpx client to stream through (injectable for tests); a private
        client is created and closed per call when omitted.

    Returns
    -------
    :
        A `DownloadInfo` describing the completed download.

    Raises
    ------
    DownloadBlocked, DownloadHostFault, DownloadTimeout, ChecksumMismatch
        Node-level failures that should fall through to the next mirror.

    OSError
        A transient/retryable failure (a refusal or transport blip).
    """
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    part = _part_path(dest)

    existing = part.stat().st_size if (resume and part.exists()) else 0
    headers = {"Range": f"bytes={existing}-"} if existing else {}
    timeout = httpx.Timeout(
        connect=connect_timeout,
        read=read_timeout,
        write=read_timeout,
        pool=connect_timeout,
    )

    owns_client = client is None
    active = client if client is not None else httpx.Client(follow_redirects=True)
    try:
        with active.stream("GET", url, headers=headers, timeout=timeout) as response:
            _raise_for_status(url, response.status_code)
            resumed = existing > 0 and response.status_code == _PARTIAL_STATUS
            info = _stream_to_part(
                response,
                url=url,
                dest=dest,
                part=part,
                resumed=resumed,
                prior_bytes=existing if resumed else 0,
                expected_checksum=expected_checksum,
                checksum_type=checksum_type,
                chunk_bytes=chunk_bytes,
            )
    except DownloadError:
        raise  # already the right OSError subclass — do not re-wrap
    except httpx.ConnectTimeout as exc:
        raise DownloadHostFault(
            url, f"connect timed out after {connect_timeout:g}s"
        ) from exc
    except httpx.ReadTimeout as exc:
        raise DownloadTimeout(url, read_timeout) from exc
    except httpx.ConnectError as exc:
        # A plain refusal can be a momentary overload (retryable); DNS/TLS failures
        # surface here too and are a genuine node-level fault (not retryable).
        if "refused" in str(exc).lower():
            msg = f"connection refused for {url!r}: {exc}"
            raise OSError(msg) from exc
        raise DownloadHostFault(url, f"connect error: {exc}") from exc
    except httpx.HTTPError as exc:
        msg = f"transport error downloading {url!r}: {exc}"
        raise OSError(msg) from exc
    finally:
        if owns_client:
            active.close()
    return info


def _raise_for_status(url: str, status: int) -> None:
    """Turn a non-2xx status into the right download exception (or a retryable one)."""
    if status in _BLOCK_STATUSES:
        raise DownloadBlocked(url, f"HTTP {status}")
    if status in _MISSING_STATUSES:
        raise DownloadError(url, f"file not found at this mirror (HTTP {status})")
    if status >= _BAD_STATUS:
        msg = f"HTTP {status} downloading {url!r}"
        raise OSError(msg)


def _stream_to_part(  # noqa: PLR0913 - internal helper; threads the streaming context
    response: httpx.Response,
    *,
    url: str,
    dest: Path,
    part: Path,
    resumed: bool,
    prior_bytes: int,
    expected_checksum: str | None,
    checksum_type: str | None,
    chunk_bytes: int,
) -> DownloadInfo:
    """Write the streaming body to `part`, verify, then rename onto `dest`."""
    resolved = resolve_checksum(expected_checksum, checksum_type)
    hasher = hashlib.new(resolved[0]) if resolved is not None else None
    if resumed and hasher is not None:  # seed the hash with the bytes already on disk
        with open(part, "rb") as prior:
            for block in iter(lambda: prior.read(chunk_bytes), b""):
                hasher.update(block)

    bytes_downloaded = 0
    with open(part, "ab" if resumed else "wb") as handle:
        for chunk in response.iter_bytes(chunk_bytes):
            if not chunk:
                continue
            handle.write(chunk)
            if hasher is not None:
                hasher.update(chunk)
            bytes_downloaded += len(chunk)
        handle.flush()
        os.fsync(handle.fileno())

    verified, algo = False, None
    if resolved is not None and hasher is not None:
        algo, expected_hex = resolved[0], resolved[1]
        actual = hasher.hexdigest().lower()
        if actual != expected_hex:
            part.unlink(missing_ok=True)  # corrupt: discard so the next mirror is clean
            raise ChecksumMismatch(url, expected=expected_hex, actual=actual, algo=algo)
        verified = True

    os.replace(part, dest)  # atomic within a filesystem
    return DownloadInfo(
        url=url,
        path=dest,
        bytes_downloaded=bytes_downloaded,
        total_bytes=prior_bytes + bytes_downloaded,
        resumed=resumed,
        verified=verified,
        verified_algo=algo,
    )
