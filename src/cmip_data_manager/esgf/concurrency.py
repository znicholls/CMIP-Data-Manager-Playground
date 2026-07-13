"""
Dependency-injection seams for transport, retry and concurrency

Fetching from ESGF is expressed through three small, composable callables so the
strategy can be swapped without touching the client:

- `Fetch` performs a single HTTP request and returns parsed JSON;
- `RetryPolicy` wraps a `Fetch` to add retry/backoff behaviour;
- `MapFn` runs a function over many inputs, optionally in parallel.

Sensible defaults are provided (`httpx_fetch`, `no_retry`, `serial_map`), and
richer strategies are available as factories (`exponential_backoff`,
`thread_pool_map`).  Users can inject their own — for example, a `tenacity`
decorator satisfies `RetryPolicy`.
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable, Iterable, Mapping
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import Any, TypeVar

import httpx

T = TypeVar("T")
R = TypeVar("R")

Fetch = Callable[[str, Mapping[str, str]], dict[str, Any]]
"""Perform one request: `(url, params) -> parsed JSON`."""

RetryPolicy = Callable[[Fetch], Fetch]
"""Wrap a `Fetch` to add retry behaviour."""

MapFn = Callable[[Callable[[Any], Any], Iterable[Any]], list[Any]]
"""Apply a function across many inputs, preserving input order."""


def httpx_fetch(
    timeout: float = 60.0,
    client: httpx.Client | None = None,
) -> Fetch:
    """
    Build a `Fetch` backed by httpx

    Parameters
    ----------
    timeout
        Per-request timeout in seconds (ignored if `client` is given).

    client
        Optional pre-configured client to reuse (e.g. for connection pooling).
        If provided, the caller owns its lifecycle.

    Returns
    -------
    :
        A callable that GETs `url` with `params` and returns the parsed JSON.
    """

    def fetch(url: str, params: Mapping[str, str]) -> dict[str, Any]:
        if client is not None:
            response = client.get(url, params=params)
            response.raise_for_status()
            data: dict[str, Any] = response.json()
            return data
        with httpx.Client(timeout=timeout) as owned:
            response = owned.get(url, params=params)
            response.raise_for_status()
            owned_data: dict[str, Any] = response.json()
            return owned_data

    return fetch


def no_retry(fetch: Fetch) -> Fetch:
    """
    Identity `RetryPolicy` that performs no retries

    Parameters
    ----------
    fetch
        The fetch to wrap.

    Returns
    -------
    :
        The same fetch, unchanged.
    """
    return fetch


def exponential_backoff(  # noqa: PLR0913 - deliberately configurable DI seam
    retries: int = 4,
    base: float = 0.5,
    cap: float = 30.0,
    jitter: float = 0.1,
    retry_on: tuple[type[Exception], ...] = (httpx.HTTPError,),
    sleep: Callable[[float], None] = time.sleep,
) -> RetryPolicy:
    """
    Build a `RetryPolicy` with capped exponential backoff and jitter

    Parameters
    ----------
    retries
        Number of retries after the first attempt (so `retries + 1` attempts).

    base
        Base delay in seconds; attempt `n` waits `base * 2**n`.

    cap
        Maximum delay in seconds between attempts.

    jitter
        Fractional random jitter added to each delay, in `[0, jitter]`.

    retry_on
        Exception types that trigger a retry.  Anything else propagates.

    sleep
        Sleep function, injectable so tests need not actually wait.

    Returns
    -------
    :
        A policy that wraps a `Fetch` with the described backoff behaviour.
    """

    def policy(fetch: Fetch) -> Fetch:
        def retrying(url: str, params: Mapping[str, str]) -> dict[str, Any]:
            attempt = 0
            while True:
                try:
                    return fetch(url, params)
                except retry_on:
                    if attempt >= retries:
                        raise
                    delay = min(cap, base * (2**attempt))
                    delay += random.uniform(0, jitter) * delay  # noqa: S311
                    sleep(delay)
                    attempt += 1

        return retrying

    return policy


def serial_map(func: Callable[[T], R], items: Iterable[T]) -> list[R]:
    """
    Apply `func` to each item in order, on a single thread

    Parameters
    ----------
    func
        Function to apply.

    items
        Items to apply it to.

    Returns
    -------
    :
        Results in input order.
    """
    return [func(item) for item in items]


def thread_pool_map(max_workers: int = 8) -> MapFn:
    """
    Build a `MapFn` that runs calls concurrently on a thread pool

    Threads suit this workload because the calls are I/O bound (waiting on HTTP).

    Parameters
    ----------
    max_workers
        Maximum number of worker threads.

    Returns
    -------
    :
        A callable with the same contract as `serial_map` but concurrent.  Order
        is preserved and the first exception raised by any call propagates.
    """

    def mapper(func: Callable[[T], R], items: Iterable[T]) -> list[R]:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            return list(executor.map(func, items))

    return mapper


def process_pool_map(max_workers: int = 8) -> MapFn:
    """
    Build a `MapFn` that runs calls concurrently across separate processes

    Unlike `thread_pool_map`, this uses processes, which is required for work that
    is not thread-safe — notably reading netCDF headers, since `netCDF4`/HDF5 can
    crash under concurrent use in a single process.  `func`, its arguments and its
    results must all be picklable (so `func` must be importable, not a lambda or
    closure).

    Parameters
    ----------
    max_workers
        Maximum number of worker processes.

    Returns
    -------
    :
        A callable with the same contract as `serial_map` but process-parallel.
        Order is preserved and the first exception raised by any call propagates.
    """

    def mapper(func: Callable[[T], R], items: Iterable[T]) -> list[R]:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            return list(executor.map(func, items))

    return mapper
