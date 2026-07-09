"""Tests for the injectable transport/retry/concurrency helpers."""

from __future__ import annotations

import httpx
import pytest

from cmip_data_manager.esgf.concurrency import (
    exponential_backoff,
    httpx_fetch,
    no_retry,
    serial_map,
    thread_pool_map,
)


def test_serial_map_preserves_order():
    assert serial_map(lambda x: x * 2, [1, 2, 3]) == [2, 4, 6]


def test_thread_pool_map_preserves_order():
    mapper = thread_pool_map(max_workers=4)
    assert mapper(lambda x: x * 2, [1, 2, 3, 4]) == [2, 4, 6, 8]


def test_no_retry_is_identity():
    def fetch(url, params):
        return {"ok": True}

    assert no_retry(fetch) is fetch


def test_exponential_backoff_retries_then_succeeds():
    calls = {"n": 0}
    sleeps: list[float] = []

    def flaky(url, params):
        calls["n"] += 1
        if calls["n"] < 3:
            raise httpx.HTTPError("boom")
        return {"ok": True}

    policy = exponential_backoff(retries=5, sleep=sleeps.append)
    wrapped = policy(flaky)
    assert wrapped("u", {}) == {"ok": True}
    assert calls["n"] == 3
    assert len(sleeps) == 2  # slept before the two retries


def test_exponential_backoff_gives_up():
    def always_fails(url, params):
        raise httpx.HTTPError("boom")

    policy = exponential_backoff(retries=2, sleep=lambda _d: None)
    with pytest.raises(httpx.HTTPError):
        policy(always_fails)("u", {})


def test_exponential_backoff_does_not_retry_other_errors():
    def bad(url, params):
        raise ValueError("not retryable")

    policy = exponential_backoff(retries=3, sleep=lambda _d: None)
    with pytest.raises(ValueError, match="not retryable"):
        policy(bad)("u", {})


def test_httpx_fetch_with_injected_client():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["variable_id"] == "tas"
        return httpx.Response(200, json={"response": {"numFound": 0, "docs": []}})

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport) as client:
        fetch = httpx_fetch(client=client)
        payload = fetch("https://example/search", {"variable_id": "tas"})
    assert payload["response"]["numFound"] == 0
