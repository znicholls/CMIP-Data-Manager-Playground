"""Tests for general netCDF header access: URL ranking and the stall timeout."""

from __future__ import annotations

import os
import time

import pytest

from cmip_data_manager.esgf.headers import (
    HeaderReadCrashed,
    HeaderReadTimeout,
    candidate_urls_for_files,
    http_download_url,
    http_download_urls,
    order_candidates,
    read_first_readable,
    simulation_key,
    with_timeout,
)
from cmip_data_manager.esgf.models import DatasetRecord, FileRecord

# ---- module-level readers: with_timeout uses `spawn`, so these must be picklable ----


def _echo_reader(url: str) -> str:
    """Return the url immediately (a fast, successful read)."""
    return url


def _slow_reader(url: str) -> str:
    """Sleep well past any test deadline to force a timeout."""
    time.sleep(30)
    return url


def _oserror_reader(url: str) -> str:
    """Raise an OSError, as a dead node would."""
    raise OSError("connection refused")


def _crash_reader(url: str) -> str:
    """Die without returning, as a segfaulting HDF5 read would."""
    os._exit(1)


def _file(file_id: str, *urls: str, variable_id: str | None = None) -> FileRecord:
    return FileRecord(
        id=file_id, dataset_id="d", variable_id=variable_id, urls=urls, raw={}
    )


def test_http_download_urls_returns_all_httpserver_mirrors():
    file = _file(
        "f",
        "http://a/f.nc|x|HTTPServer",
        "gsiftp://a/f.nc|x|GridFTP",
        "https://b/f.nc|x|HTTPServer",
    )
    assert http_download_urls(file) == ["http://a/f.nc", "https://b/f.nc"]


def test_http_download_url_none_when_no_httpserver():
    assert http_download_url(_file("f", "gsiftp://a/f.nc|x|GridFTP")) is None


def test_order_candidates_prefers_preferred_then_https():
    urls = ["http://far/f.nc", "https://far/f.nc", "https://nci/f.nc"]
    assert order_candidates(urls, preferred_hosts=("nci",)) == [
        "https://nci/f.nc",
        "https://far/f.nc",
        "http://far/f.nc",
    ]


def test_order_candidates_honours_preferred_order():
    urls = ["https://b/f.nc", "https://a/f.nc", "https://c/f.nc"]
    assert order_candidates(urls, preferred_hosts=("a", "b")) == [
        "https://a/f.nc",
        "https://b/f.nc",
        "https://c/f.nc",
    ]


def test_order_candidates_drops_ignored_hosts_and_dedupes():
    urls = ["https://a/f.nc", "https://a/f.nc", "https://bad/f.nc"]
    assert order_candidates(urls, ignore_hosts=frozenset({"bad"})) == ["https://a/f.nc"]


def test_candidate_urls_pool_across_a_simulations_files():
    files = [
        _file("f1", "https://nci/tas.nc|x|HTTPServer", variable_id="tas"),
        _file("f2", "http://far/rsut.nc|x|HTTPServer", variable_id="rsut"),
    ]
    assert candidate_urls_for_files(files, preferred_hosts=("nci",)) == [
        "https://nci/tas.nc",
        "http://far/rsut.nc",
    ]


def test_read_first_readable_skips_dead_mirrors():
    calls: list[str] = []

    def reader(url: str) -> str:
        calls.append(url)
        if "dead" in url:
            raise OSError("boom")
        return url

    result = read_first_readable(["https://dead/f.nc", "https://ok/f.nc"], reader)
    assert result == "https://ok/f.nc"
    assert calls == ["https://dead/f.nc", "https://ok/f.nc"]


def test_read_first_readable_none_when_all_fail():
    def reader(_url: str) -> str:
        raise OSError("dead")

    assert read_first_readable(["https://a/f.nc"], reader) is None


def test_simulation_key_from_record():
    record = DatasetRecord(
        id="d",
        source_id="ACCESS-ESM1-5",
        experiment_id="ssp245",
        variant_label="r1i1p1f1",
        variable_id="tas",
        raw={},
    )
    assert simulation_key(record) == ("ACCESS-ESM1-5", "ssp245", "r1i1p1f1")


def test_simulation_key_none_when_incomplete():
    assert simulation_key(DatasetRecord(id="d", source_id="M", raw={})) is None


# ---- with_timeout: real subprocesses, kept fast with small deadlines ----


def test_with_timeout_returns_result_under_deadline():
    read = with_timeout(_echo_reader, seconds=10)
    assert read("https://host/f.nc") == "https://host/f.nc"


def test_with_timeout_raises_on_stall_and_returns_promptly():
    read = with_timeout(_slow_reader, seconds=0.3, grace=0.5)
    started = time.monotonic()
    with pytest.raises(HeaderReadTimeout):
        read("https://stalled/f.nc")
    # The stalling reader sleeps 30s; we must not have waited anywhere near that.
    assert time.monotonic() - started < 10


def test_with_timeout_propagates_reader_oserror():
    read = with_timeout(_oserror_reader, seconds=10)
    with pytest.raises(OSError, match="connection refused"):
        read("https://dead/f.nc")


def test_with_timeout_raises_crashed_when_worker_dies():
    read = with_timeout(_crash_reader, seconds=10)
    with pytest.raises(HeaderReadCrashed):
        read("https://corrupt/f.nc")


def test_with_timeout_timeout_is_an_oserror_so_fallback_skips_it():
    # read_first_readable catches OSError; a stall must be caught the same way.
    slow = with_timeout(_slow_reader, seconds=0.3, grace=0.5)

    def reader(url: str) -> str:
        if "stall" in url:
            return slow(url)
        return url

    assert read_first_readable(["https://stall/f.nc", "https://ok/f.nc"], reader) == (
        "https://ok/f.nc"
    )
