"""Tests for the pre-flight data-node liveness probe."""

from __future__ import annotations

from collections.abc import Iterator, Sequence

import httpx
import pytest

from cmip_data_manager.esgf import preflight
from cmip_data_manager.esgf.preflight import (
    ProbeCache,
    ProbeOutcome,
    _Attempt,
    probe_host,
    probe_node,
    probe_nodes,
    sample_urls_by_host,
)
from cmip_data_manager.esgf.routing import SimulationCandidates


# --- fakes for the httpx byte-range probe ------------------------------------
class _FakeResponse:
    def __init__(self, status_code: int, chunks: Sequence[bytes]) -> None:
        self.status_code = status_code
        self._chunks = chunks

    def iter_bytes(self) -> Iterator[bytes]:
        yield from self._chunks


class _FakeStream:
    def __init__(self, response: _FakeResponse) -> None:
        self._response = response

    def __enter__(self) -> _FakeResponse:
        return self._response

    def __exit__(self, *args: object) -> bool:
        return False


def _fake_stream(
    *,
    status_code: int = 206,
    chunks: Sequence[bytes] = (b"data",),
    exc: Exception | None = None,
):
    def stream(method: str, url: str, **kwargs: object) -> _FakeStream:
        if exc is not None:
            raise exc
        return _FakeStream(_FakeResponse(status_code, chunks))

    return stream


def _alive(
    host: str, url: str, seconds: float = 0.1, attempts: int = 1
) -> ProbeOutcome:
    return ProbeOutcome(
        host=host, alive=True, reason=None, seconds=seconds, url=url, attempts=attempts
    )


def _dead(host: str, url: str, reason: str = "dead", attempts: int = 1) -> ProbeOutcome:
    return ProbeOutcome(
        host=host, alive=False, reason=reason, seconds=1.0, url=url, attempts=attempts
    )


# --- ProbeCache --------------------------------------------------------------
def test_probe_cache_records_and_partitions_hosts():
    cache = ProbeCache()
    cache.record(_alive("good", "https://good/f.nc"))
    cache.record(_dead("dead", "https://dead/f.nc"))
    assert cache.known("good") and not cache.known("unseen")
    assert cache.get("dead").reason == "dead"
    assert cache.dead_hosts() == frozenset({"dead"})
    assert cache.alive_hosts() == frozenset({"good"})
    assert set(cache.outcomes()) == {"good", "dead"}


def test_probe_cache_record_overwrites():
    cache = ProbeCache()
    cache.record(_dead("h", "https://h/f.nc"))
    cache.record(_alive("h", "https://h/f.nc"))
    assert cache.alive_hosts() == frozenset({"h"})


# --- sample_urls_by_host -----------------------------------------------------
def test_sample_urls_by_host_keeps_both_schemes_first_host_seen():
    a = SimulationCandidates(
        ("A", "ssp245", "r1"),
        "A",
        ("nci", "ornl"),
        {
            "nci": ("https://nci/f.nc", "http://nci/f.nc"),
            "ornl": ("https://ornl/g.nc",),
        },
    )
    # A second simulation re-uses nci with a *different* file; the first wins.
    b = SimulationCandidates(
        ("B", "ssp245", "r1"),
        "B",
        ("nci",),
        {"nci": ("https://nci/other.nc",)},
    )
    got = sample_urls_by_host({a.simulation: a, b.simulation: b})
    assert got == {
        "nci": ("https://nci/f.nc", "http://nci/f.nc"),
        "ornl": ("https://ornl/g.nc",),
    }


# --- _probe_chunk classification ---------------------------------------------
def _chunk(monkeypatch, **stream_kwargs) -> _Attempt:
    monkeypatch.setattr(httpx, "stream", _fake_stream(**stream_kwargs))
    return preflight._probe_chunk(
        "https://h/f.nc", connect_timeout=90.0, read_timeout=30.0, chunk_bytes=64
    )


def test_probe_chunk_alive_when_bytes_flow(monkeypatch):
    result = _chunk(monkeypatch, status_code=206, chunks=(b"x",))
    assert result.alive and result.reason is None


def test_probe_chunk_skips_empty_chunks_until_data(monkeypatch):
    # An empty keep-alive chunk before real data must not be mistaken for "no data".
    result = _chunk(monkeypatch, status_code=200, chunks=(b"", b"payload"))
    assert result.alive and result.reason is None


def test_probe_chunk_per_file_status_is_reachable(monkeypatch):
    # A 404 means the node answered about a specific file — it is reachable, not dead.
    result = _chunk(monkeypatch, status_code=404, chunks=())
    assert result.alive and not result.retryable


def test_probe_chunk_error_status_is_transient(monkeypatch):
    result = _chunk(monkeypatch, status_code=403, chunks=())
    assert not result.alive and result.retryable and "403" in result.reason


def test_probe_chunk_no_data_is_transient(monkeypatch):
    result = _chunk(monkeypatch, status_code=200, chunks=())
    assert not result.alive and result.retryable
    assert "no data" in result.reason


def test_probe_chunk_connect_timeout_is_fatal(monkeypatch):
    result = _chunk(monkeypatch, exc=httpx.ConnectTimeout("slow"))
    assert not result.alive and not result.retryable
    assert "connect timed out" in result.reason


def test_probe_chunk_read_timeout_is_fatal(monkeypatch):
    result = _chunk(monkeypatch, exc=httpx.ReadTimeout("stalled"))
    assert not result.alive and not result.retryable
    assert "read stalled" in result.reason


def test_probe_chunk_refused_is_transient(monkeypatch):
    result = _chunk(monkeypatch, exc=httpx.ConnectError("Connection refused"))
    assert not result.alive and result.retryable
    assert "refused" in result.reason


def test_probe_chunk_dns_failure_is_fatal(monkeypatch):
    result = _chunk(monkeypatch, exc=httpx.ConnectError("could not resolve host"))
    assert not result.alive and not result.retryable
    assert "connect error" in result.reason


def test_probe_chunk_other_transport_error_is_transient(monkeypatch):
    result = _chunk(monkeypatch, exc=httpx.HTTPError("boom"))
    assert not result.alive and result.retryable
    assert "transport error" in result.reason


# --- probe_node retry --------------------------------------------------------
def test_probe_node_retries_transient_then_succeeds(monkeypatch):
    slept: list[float] = []
    it = iter(
        [
            _Attempt(alive=False, retryable=True, reason="refused", seconds=0.2),
            _Attempt(alive=True, retryable=False, reason=None, seconds=0.3),
        ]
    )
    monkeypatch.setattr(preflight, "_probe_chunk", lambda *a, **k: next(it))
    outcome = probe_node(
        "https://h/f.nc", max_attempts=3, base=0.0, jitter=0.0, sleep=slept.append
    )
    assert outcome.alive and outcome.attempts == 2
    assert slept == [0.0]


def test_probe_node_transient_exhausted_is_dead(monkeypatch):
    it = iter(
        [
            _Attempt(alive=False, retryable=True, reason="refused", seconds=0.2),
            _Attempt(alive=False, retryable=True, reason="refused", seconds=0.2),
        ]
    )
    monkeypatch.setattr(preflight, "_probe_chunk", lambda *a, **k: next(it))
    outcome = probe_node(
        "https://h/f.nc", max_attempts=2, base=0.0, jitter=0.0, sleep=lambda _s: None
    )
    assert not outcome.alive and outcome.attempts == 2
    assert outcome.reason == "refused"


def test_probe_node_fatal_fault_is_not_retried(monkeypatch):
    calls = {"n": 0}

    def one(*args: object, **kwargs: object) -> _Attempt:
        calls["n"] += 1
        return _Attempt(alive=False, retryable=False, reason="ssl", seconds=0.1)

    monkeypatch.setattr(preflight, "_probe_chunk", one)
    outcome = probe_node("https://h/f.nc", max_attempts=5, sleep=lambda _s: None)
    assert not outcome.alive and outcome.attempts == 1 and calls["n"] == 1
    assert outcome.host == "h"


# --- probe_host across schemes -----------------------------------------------
def test_probe_host_alive_on_second_scheme(monkeypatch):
    def fake(url: str, **kwargs: object) -> ProbeOutcome:
        if url.startswith("https"):
            return _dead("h", url, reason="ssl")
        return _alive("h", url)

    monkeypatch.setattr(preflight, "probe_node", fake)
    outcome = probe_host(("https://h/f.nc", "http://h/f.nc"))
    assert outcome.alive and outcome.url == "http://h/f.nc"


def test_probe_host_dead_when_all_schemes_fail(monkeypatch):
    def fake(url: str, **kwargs: object) -> ProbeOutcome:
        reason = "ssl" if url.startswith("https") else "refused"
        return _dead("h", url, reason=reason, attempts=2)

    monkeypatch.setattr(preflight, "probe_node", fake)
    outcome = probe_host(("https://h/f.nc", "http://h/f.nc"))
    assert not outcome.alive
    assert "https: ssl" in outcome.reason and "http: refused" in outcome.reason
    assert outcome.attempts == 4  # summed across the two schemes


def test_probe_host_empty_urls_is_dead():
    outcome = probe_host(())
    assert not outcome.alive and outcome.attempts == 0


# --- probe_nodes sweep -------------------------------------------------------
def test_probe_nodes_probes_each_host_and_keys_by_host():
    def fake(urls: Sequence[str]) -> ProbeOutcome:
        alive = "good" in urls[0]
        # Return a deliberately wrong host to prove the sweep re-keys by the asked host.
        return ProbeOutcome(
            host="WRONG",
            alive=alive,
            reason=None if alive else "dead",
            seconds=0.1,
            url=urls[0],
            attempts=1,
        )

    urls_by_host = {
        "good": ("https://good/f.nc",),
        "dead": ("https://dead/f.nc",),
    }
    result = probe_nodes(urls_by_host, probe=fake)
    assert set(result) == {"good", "dead"}
    assert result["good"].alive and result["good"].host == "good"
    assert not result["dead"].alive and result["dead"].host == "dead"


def test_probe_nodes_forces_alive_without_probing():
    def fake(urls: Sequence[str]) -> ProbeOutcome:
        raise AssertionError("forced-alive host must not be probed")

    result = probe_nodes(
        {"keep": ("https://keep/f.nc", "http://keep/f.nc")},
        probe=fake,
        force_alive_hosts=frozenset({"keep"}),
    )
    assert result["keep"].alive and result["keep"].attempts == 0
    assert result["keep"].url == "https://keep/f.nc"


def test_probe_nodes_skips_hosts_already_in_cache():
    cache = ProbeCache()
    cache.record(_dead("known", "https://known/f.nc"))
    calls: list[str] = []

    def fake(urls: Sequence[str]) -> ProbeOutcome:
        calls.append(urls[0])
        return _alive("new", urls[0])

    result = probe_nodes(
        {"known": ("https://known/f.nc",), "new": ("https://new/f.nc",)},
        probe=fake,
        cache=cache,
    )
    assert calls == ["https://new/f.nc"]  # the cached host was not re-probed
    assert not result["known"].alive and result["new"].alive


def test_probe_nodes_default_probe_calls_probe_host(monkeypatch):
    # With no injected `probe`, the sweep must fall back to `probe_host` per node.
    seen: list[Sequence[str]] = []

    def fake_probe_host(urls, **kwargs):
        seen.append(tuple(urls))
        return _alive("h", urls[0])

    monkeypatch.setattr(preflight, "probe_host", fake_probe_host)
    result = probe_nodes({"h": ("https://h/f.nc", "http://h/f.nc")})
    assert seen == [("https://h/f.nc", "http://h/f.nc")]
    assert result["h"].alive


def test_probe_nodes_uses_map_fn():
    seen: list[int] = []

    def map_fn(func, items):
        materialised = list(items)
        seen.append(len(materialised))
        return [func(item) for item in materialised]

    result = probe_nodes(
        {"a": ("https://a/f.nc",), "b": ("https://b/f.nc",)},
        probe=lambda urls: _alive("x", urls[0]),
        map_fn=map_fn,
    )
    assert seen == [2] and len(result) == 2


@pytest.mark.parametrize("scheme", ["https", "http"])
def test_probe_node_derives_host_from_url(monkeypatch, scheme):
    monkeypatch.setattr(
        preflight,
        "_probe_chunk",
        lambda *a, **k: _Attempt(alive=True, retryable=False, reason=None, seconds=0.1),
    )
    outcome = probe_node(f"{scheme}://esgf.nci.org.au/f.nc")
    assert outcome.host == "esgf.nci.org.au"
