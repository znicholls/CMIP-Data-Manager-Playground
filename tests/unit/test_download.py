"""Tests for the leaf httpx file downloader (`esgf.download`)."""

from __future__ import annotations

import hashlib
from collections.abc import Callable

import httpx
import pytest

from cmip_data_manager.esgf.download import (
    ChecksumMismatch,
    DownloadBlocked,
    DownloadError,
    DownloadHostFault,
    DownloadTimeout,
    download_to_path,
    resolve_checksum,
    verify_file,
)

_URL = "https://node.example/thredds/fileServer/tas.nc"

Handler = Callable[[httpx.Request], httpx.Response]


def _md5(data: bytes) -> str:
    """Hex md5 digest (ESGF publishes md5 checksums; this is test-only)."""
    return hashlib.new("md5", data).hexdigest()  # noqa: S324 - checksum, not security


def _client(handler: Handler) -> httpx.Client:
    """A client whose every request is served by `handler` (no network)."""
    return httpx.Client(transport=httpx.MockTransport(handler))


def _serve(content: bytes, *, status: int = 200, honor_range: bool = True) -> Handler:
    """Serve `content`, honouring an HTTP `Range` with a 206 unless told otherwise."""

    def handler(request: httpx.Request) -> httpx.Response:
        if status != 200:  # a non-200 error server; the range is irrelevant
            return httpx.Response(status, content=content)
        rng = request.headers.get("range")
        if honor_range and rng:
            start = int(rng.removeprefix("bytes=").split("-")[0])
            return httpx.Response(206, content=content[start:])
        return httpx.Response(200, content=content)

    return handler


def _raiser(exc: Exception) -> Handler:
    """A handler that raises `exc` when the request is made."""

    def handler(_request: httpx.Request) -> httpx.Response:
        raise exc

    return handler


# --- checksum resolution / verification --------------------------------------


def test_resolve_checksum_explicit_type_multihash_and_none():
    assert resolve_checksum("ABC", "MD5") == ("md5", "abc")  # normalised, lowercased
    assert resolve_checksum("abc", "sha-3") is None  # unknown algorithm
    assert resolve_checksum("", "md5") is None  # no digest
    assert resolve_checksum("nothex", None) is None  # malformed multihash
    assert resolve_checksum("12", None) is None  # too short for a multihash
    assert resolve_checksum("1220aa", None) is None  # length byte != digest length


def test_verify_file_match_mismatch_and_unverifiable(tmp_path):
    path = tmp_path / "a.nc"
    path.write_bytes(b"hello")
    assert verify_file(path, _md5(b"hello"), "md5") == (True, "md5")
    assert verify_file(path, "0" * 32, "md5") == (False, "md5")
    assert verify_file(path, None, None) == (False, None)  # nothing to check against


# --- happy paths -------------------------------------------------------------


def test_download_verifies_and_atomically_renames(tmp_path):
    content = b"netcdf-bytes" * 1000
    dest = tmp_path / "sub" / "dir" / "tas.nc"  # parents created on the fly
    with _client(_serve(content)) as client:
        info = download_to_path(
            _URL,
            dest,
            expected_checksum=_md5(content),
            checksum_type="md5",
            client=client,
        )
    assert dest.read_bytes() == content
    assert info.verified is True
    assert info.verified_algo == "md5"
    assert info.bytes_downloaded == len(content)
    assert info.total_bytes == len(content)
    assert info.resumed is False
    assert not (dest.parent / "tas.nc.part").exists()  # staging file cleaned up


def test_download_creates_and_closes_its_own_client(tmp_path, monkeypatch):
    content = b"data" * 10
    dest = tmp_path / "tas.nc"
    backing = httpx.Client(transport=httpx.MockTransport(_serve(content)))
    closed = {"value": False}
    original_close = backing.close

    def tracked_close() -> None:
        closed["value"] = True
        original_close()

    monkeypatch.setattr(backing, "close", tracked_close)
    monkeypatch.setattr(
        "cmip_data_manager.esgf.download.httpx.Client", lambda **_kw: backing
    )
    info = download_to_path(_URL, dest)  # client=None -> owns/creates its own client
    assert dest.read_bytes() == content
    assert info.total_bytes == len(content)
    assert closed["value"] is True  # the private client was closed in `finally`


def test_download_without_checksum_is_unverified(tmp_path):
    content = b"abc" * 10
    dest = tmp_path / "tas.nc"
    with _client(_serve(content)) as client:
        info = download_to_path(_URL, dest, client=client)
    assert dest.read_bytes() == content
    assert info.verified is False
    assert info.verified_algo is None


def test_download_verifies_ng_multihash(tmp_path):
    content = b"cmip-ng" * 20
    dest = tmp_path / "tas.nc"
    digest = hashlib.sha256(content).digest()
    multihash = bytes([0x12, len(digest)]).hex() + digest.hex()  # sha2-256 multihash
    with _client(_serve(content)) as client:
        info = download_to_path(
            _URL, dest, expected_checksum=multihash, checksum_type=None, client=client
        )
    assert info.verified is True
    assert info.verified_algo == "sha256"
    assert dest.read_bytes() == content


# --- resume ------------------------------------------------------------------


def test_download_resumes_a_partial_part(tmp_path):
    content = b"0123456789" * 100  # 1000 bytes
    dest = tmp_path / "tas.nc"
    (tmp_path / "tas.nc.part").write_bytes(content[:400])  # 400 bytes already fetched
    with _client(_serve(content)) as client:
        info = download_to_path(
            _URL,
            dest,
            expected_checksum=_md5(content),
            checksum_type="md5",
            client=client,
        )
    assert dest.read_bytes() == content
    assert info.resumed is True
    assert info.bytes_downloaded == 600  # only the *new* bytes this call
    assert info.total_bytes == 1000
    assert info.verified is True  # hash seeded with the pre-existing bytes


def test_download_restarts_when_server_ignores_range(tmp_path):
    content = b"z" * 500
    dest = tmp_path / "tas.nc"
    (tmp_path / "tas.nc.part").write_bytes(b"stale-wrong-partial")  # must be discarded
    with _client(_serve(content, honor_range=False)) as client:
        info = download_to_path(_URL, dest, client=client)
    assert dest.read_bytes() == content  # full fresh content, stale bytes gone
    assert info.resumed is False
    assert info.bytes_downloaded == 500


# --- failure classification --------------------------------------------------


def test_checksum_mismatch_raises_and_discards_part(tmp_path):
    content = b"x" * 100
    dest = tmp_path / "tas.nc"
    with _client(_serve(content)) as client, pytest.raises(ChecksumMismatch):
        download_to_path(
            _URL,
            dest,
            expected_checksum="de" * 32,  # 64 hex chars, wrong sha256
            checksum_type="sha256",
            client=client,
        )
    assert not dest.exists()
    assert not (tmp_path / "tas.nc.part").exists()  # corrupt staging file discarded


def test_block_status_raises_downloadblocked_and_keeps_part(tmp_path):
    dest = tmp_path / "tas.nc"
    part = tmp_path / "tas.nc.part"
    part.write_bytes(b"partial")
    with _client(_serve(b"", status=503)) as client, pytest.raises(DownloadBlocked):
        download_to_path(_URL, dest, client=client)
    assert part.exists()  # kept so a later run can resume
    assert not dest.exists()


def test_missing_status_raises_downloaderror_but_not_block(tmp_path):
    dest = tmp_path / "tas.nc"
    with _client(_serve(b"", status=404)) as client:
        with pytest.raises(DownloadError) as exc:
            download_to_path(_URL, dest, client=client)
    assert not isinstance(exc.value, DownloadBlocked)  # try next mirror, not a block


def test_bad_status_is_retryable_plain_oserror(tmp_path):
    dest = tmp_path / "tas.nc"
    with _client(_serve(b"", status=500)) as client, pytest.raises(OSError) as exc:
        download_to_path(_URL, dest, client=client)
    assert not isinstance(exc.value, DownloadError)  # retried on the same mirror


def test_connect_error_dns_or_ssl_is_host_fault(tmp_path):
    dest = tmp_path / "tas.nc"
    handler = _raiser(httpx.ConnectError("SSL: CERTIFICATE_VERIFY_FAILED"))
    with _client(handler) as client, pytest.raises(DownloadHostFault):
        download_to_path(_URL, dest, client=client)


def test_connect_timeout_is_host_fault(tmp_path):
    dest = tmp_path / "tas.nc"
    with _client(_raiser(httpx.ConnectTimeout("connect timed out"))) as client:
        with pytest.raises(DownloadHostFault):
            download_to_path(_URL, dest, client=client)


def test_transport_error_is_retryable_oserror(tmp_path):
    dest = tmp_path / "tas.nc"
    with _client(_raiser(httpx.RemoteProtocolError("server disconnected"))) as client:
        with pytest.raises(OSError) as exc:
            download_to_path(_URL, dest, client=client)
    assert not isinstance(exc.value, DownloadError)  # transport blip: retry same host


def test_connection_refused_is_retryable_oserror(tmp_path):
    dest = tmp_path / "tas.nc"
    with _client(_raiser(httpx.ConnectError("Connection refused"))) as client:
        with pytest.raises(OSError) as exc:
            download_to_path(_URL, dest, client=client)
    assert not isinstance(exc.value, DownloadError)  # a blip: retry the same host


def test_read_timeout_is_downloadtimeout(tmp_path):
    dest = tmp_path / "tas.nc"
    with _client(_raiser(httpx.ReadTimeout("read stalled"))) as client:
        with pytest.raises(DownloadTimeout):
            download_to_path(_URL, dest, client=client)
