"""Tests for general netCDF header access: URL ranking and the stall timeout."""

from __future__ import annotations

import os
import time

import numpy as np
import pytest

from cmip_data_manager.esgf.headers import (
    HeaderReadBlocked,
    HeaderReadCrashed,
    HeaderReadTimeout,
    _coerce_attr,
    candidate_urls_for_files,
    header_key,
    http_download_url,
    http_download_urls,
    https_twin,
    is_block_signal,
    order_candidates,
    promote_blocks,
    read_first_readable,
    simulation_key,
    with_retry,
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


def test_order_candidates_uses_host_rank_health():
    urls = ["https://slow/f.nc", "https://fast/f.nc", "https://unseen/f.nc"]
    rank = {"fast": (0.0, 1.0), "slow": (0.0, 9.0)}  # unseen absent -> neutral 0.5

    def host_rank(host: str) -> tuple[float, float]:
        return rank.get(host, (0.5, 0.0))

    # fast (reliable+quick) first, slow (reliable but slow) next, unseen last.
    assert order_candidates(urls, host_rank=host_rank) == [
        "https://fast/f.nc",
        "https://slow/f.nc",
        "https://unseen/f.nc",
    ]


def test_order_candidates_preferred_beats_health():
    urls = ["https://healthy/f.nc", "https://nci/f.nc"]

    def host_rank(host: str) -> tuple[float, float]:
        return {"healthy": (0.0, 0.1)}.get(host, (0.9, 0.0))  # nci looks unreliable

    # An explicit preference still wins over learned health.
    assert order_candidates(urls, preferred_hosts=("nci",), host_rank=host_rank) == [
        "https://nci/f.nc",
        "https://healthy/f.nc",
    ]


def test_candidate_urls_pool_across_a_simulations_files():
    files = [
        _file("f1", "https://nci/tas.nc|x|HTTPServer", variable_id="tas"),
        _file("f2", "http://far/rsut.nc|x|HTTPServer", variable_id="rsut"),
    ]
    # The http-only mirror contributes an https twin (ranked ahead), keeping the
    # original http URL as a fallback.
    assert candidate_urls_for_files(files, preferred_hosts=("nci",)) == [
        "https://nci/tas.nc",
        "https://far/rsut.nc",
        "http://far/rsut.nc",
    ]


def test_https_twin_upgrades_only_http_urls():
    assert https_twin("http://a/f.nc") == "https://a/f.nc"
    assert https_twin("https://a/f.nc") is None
    assert https_twin("gsiftp://a/f.nc") is None


def test_candidate_urls_add_https_twin_for_http_only_mirror():
    # A replica indexed only as http still yields an https attempt first.
    files = [_file("f", "http://ucar/tas.nc|x|HTTPServer", variable_id="tas")]
    assert candidate_urls_for_files(files) == [
        "https://ucar/tas.nc",
        "http://ucar/tas.nc",
    ]


def test_candidate_urls_no_duplicate_when_both_schemes_indexed():
    # When the index already lists both schemes, the twin does not double them up.
    files = [
        _file(
            "f",
            "http://ornl/tas.nc|x|HTTPServer",
            "https://ornl/tas.nc|x|HTTPServer",
            variable_id="tas",
        )
    ]
    assert candidate_urls_for_files(files) == [
        "https://ornl/tas.nc",
        "http://ornl/tas.nc",
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


def _rec(**fields):
    base = {
        "id": "d",
        "source_id": "ACCESS-ESM1-5",
        "experiment_id": "ssp245",
        "variant_label": "r1i1p1f1",
        "variable_id": "tas",
        "table_id": "Amon",
        "raw": {},
    }
    base.update(fields)
    return DatasetRecord(**base)


def test_header_key_includes_variable_and_table():
    assert header_key(_rec()) == (
        "ACCESS-ESM1-5",
        "ssp245",
        "r1i1p1f1",
        "tas",
        "Amon",
    )


def test_header_key_none_when_table_missing():
    assert header_key(_rec(table_id=None)) is None


def test_coerce_attr_plain_string_and_number():
    assert _coerce_attr("historical") == "historical"
    assert _coerce_attr("CNRM-CM6-1-HR") == "CNRM-CM6-1-HR"  # hyphens, not spaced
    assert _coerce_attr(60225.0) == "60225.0"
    assert _coerce_attr(np.float64(60225.0)) == "60225.0"


def test_coerce_attr_collapses_spaced_string():
    # netCDF4 can return a char-array attribute already spaced out as a str.
    assert _coerce_attr("h i s t o r i c a l") == "historical"
    # A normal multi-word/hyphenated value is left untouched.
    assert _coerce_attr("piControl") == "piControl"


def test_coerce_attr_joins_char_arrays():
    # The bug: a netCDF char array (one element per letter) must not be spaced out.
    assert _coerce_attr(np.array(list("historical"), dtype="S1")) == "historical"
    assert _coerce_attr(np.array(list("historical"), dtype="U1")) == "historical"
    assert _coerce_attr(np.array("historical")) == "historical"  # 0-d string array


def test_coerce_attr_bytes():
    assert _coerce_attr(b"hdl:21.14100/abc") == "hdl:21.14100/abc"


def test_with_retry_succeeds_after_transient_failures():
    calls = {"n": 0}

    def flaky(url: str) -> str:
        calls["n"] += 1
        if calls["n"] < 3:
            raise OSError("connection reset")
        return url

    read = with_retry(flaky, max_attempts=3, base=0.0, jitter=0.0)
    assert read("https://node/f.nc") == "https://node/f.nc"
    assert calls["n"] == 3


def test_with_retry_gives_up_after_max_attempts():
    calls = {"n": 0}

    def always_fails(url: str) -> str:
        calls["n"] += 1
        raise OSError("refused")

    read = with_retry(always_fails, max_attempts=3, base=0.0, jitter=0.0)
    with pytest.raises(OSError, match="refused"):
        read("https://dead/f.nc")
    assert calls["n"] == 3  # one try plus two retries, then propagates


def test_with_retry_does_not_retry_a_stall():
    calls = {"n": 0}

    def staller(url: str) -> str:
        calls["n"] += 1
        raise HeaderReadTimeout(url, 90.0)

    read = with_retry(staller, max_attempts=3, base=0.0, jitter=0.0)
    with pytest.raises(HeaderReadTimeout):
        read("https://stalled/f.nc")
    assert calls["n"] == 1  # a stall is given up on immediately


def test_with_retry_does_not_retry_a_crash():
    calls = {"n": 0}

    def crasher(url: str) -> str:
        calls["n"] += 1
        raise HeaderReadCrashed(url, 1)

    read = with_retry(crasher, max_attempts=3, base=0.0, jitter=0.0)
    with pytest.raises(HeaderReadCrashed):
        read("https://corrupt/f.nc")
    assert calls["n"] == 1


def test_with_retry_backoff_sleeps_between_attempts():
    slept: list[float] = []

    def flaky(url: str) -> str:
        if len(slept) < 2:
            raise OSError("reset")
        return url

    read = with_retry(flaky, max_attempts=5, base=1.0, jitter=0.0, sleep=slept.append)
    assert read("https://node/f.nc") == "https://node/f.nc"
    assert slept == [1.0, 2.0]  # exponential: base*2**0, base*2**1


@pytest.mark.parametrize(
    "message",
    [
        "HTTP error 429: Too Many Requests",
        "server said 403 Forbidden",
        "503 Service Unavailable",
        "you have hit the rate limit",
    ],
)
def test_is_block_signal_matches_rate_limit_and_refusal(message):
    assert is_block_signal(OSError(message))


@pytest.mark.parametrize(
    "message",
    ["Connection refused", "Connection reset by peer", "NetCDF: HDF error"],
)
def test_is_block_signal_ignores_ordinary_errors(message):
    assert not is_block_signal(OSError(message))


def test_promote_blocks_reraises_block_signal_as_blocked():
    def reader(url: str) -> str:
        raise OSError("HTTP 429 Too Many Requests")

    read = promote_blocks(reader)
    with pytest.raises(HeaderReadBlocked) as excinfo:
        read("https://busy/f.nc")
    assert excinfo.value.url == "https://busy/f.nc"
    assert "429" in excinfo.value.reason


def test_promote_blocks_passes_through_ordinary_and_typed_errors():
    def erroring(url: str) -> str:
        raise OSError("Connection refused")

    def stalling(url: str) -> str:
        raise HeaderReadTimeout(url, 90.0)

    with pytest.raises(OSError, match="refused"):
        promote_blocks(erroring)("https://dead/f.nc")
    # An already-typed failure is not re-wrapped.
    with pytest.raises(HeaderReadTimeout):
        promote_blocks(stalling)("https://stalled/f.nc")


def test_promote_blocks_returns_value_on_success():
    assert promote_blocks(lambda url: url.upper())("https://a/f.nc") == "HTTPS://A/F.NC"


def test_with_retry_does_not_retry_a_block():
    calls = {"n": 0}

    def blocked(url: str) -> str:
        calls["n"] += 1
        raise HeaderReadBlocked(url, "429 Too Many Requests")

    read = with_retry(blocked, max_attempts=3, base=0.0, jitter=0.0)
    with pytest.raises(HeaderReadBlocked):
        read("https://busy/f.nc")
    assert calls["n"] == 1  # not retried — the controller backs the node off instead
