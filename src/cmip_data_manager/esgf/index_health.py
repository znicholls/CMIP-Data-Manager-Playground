"""
Track per-index-node health from Step-2 file-search outcomes

Where `NodeHealth` (in `health.py`) watches *data nodes* during header reads, this
watches the **search index** endpoints hit in Step 2 (the file searches).  The
bottleneck there is a *good* index mirror: one endpoint can be healthy while
another (e.g. `metagrid.esgf-west.org`) is returning HTTP 500s or answering a
trivial count in ~77s, which is exactly what drives the endpoint fallback.

`IndexNodeHealth` accumulates what actually happened per endpoint — attempts,
successes, failures, how many of those calls were **retries**, and the subset of
failures that were **server errors** (5xx) or **timeouts** — so a run learns which
mirror is failing and that knowledge can be persisted (`IndexNodeHealthStat`) and
ranked.  It is a passive observer: the Step-2 orchestrator calls `record(...)` once
per search call (including every backoff retry), and what to *do* with the numbers
(here: keep the user's explicit endpoint preference, record health for observability)
is the caller's choice.

The registry is thread-safe because the per-dataset searches fan out across a thread
pool.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from enum import Enum

import httpx


class SearchOutcome(Enum):
    """How a single index-node search call ended."""

    SUCCESS = "success"

    SERVER_ERROR = "server_error"
    """The endpoint returned HTTP 5xx (e.g. the `metagrid-west` 500s) — index-node
    distress that should drive the fallback, kept distinct from a client-side 4xx."""

    TIMEOUT = "timeout"
    """The call timed out connecting to or reading from the endpoint."""

    ERROR = "error"
    """Any other transport/HTTP failure (connection refused, 4xx, malformed JSON)."""


def classify_search_error(exc: Exception) -> SearchOutcome:
    """
    Map a transport/HTTP exception to a coarse search outcome for health

    Parameters
    ----------
    exc
        The exception raised by a failed search call.

    Returns
    -------
    :
        `TIMEOUT` for an httpx timeout, `SERVER_ERROR` for an HTTP 5xx, otherwise
        `ERROR`.

    Examples
    --------
    >>> classify_search_error(httpx.ConnectTimeout("slow")).value
    'timeout'
    >>> classify_search_error(ValueError("bad json")).value
    'error'
    """
    if isinstance(exc, httpx.TimeoutException):
        return SearchOutcome.TIMEOUT
    if isinstance(exc, httpx.HTTPStatusError):
        if exc.response.status_code >= 500:  # noqa: PLR2004 - the HTTP 5xx band
            return SearchOutcome.SERVER_ERROR
        return SearchOutcome.ERROR
    return SearchOutcome.ERROR


@dataclass
class IndexNodeStat:
    """Accumulated file-search outcomes for one search-index endpoint."""

    endpoint: str
    attempts: int = 0
    """Total search calls made to this endpoint, **including retries**."""

    successes: int = 0
    failures: int = 0
    """Calls that errored (`attempts == successes + failures`)."""

    retries: int = 0
    """Calls that were a retry of an earlier attempt (attempt number > 1)."""

    server_errors: int = 0
    """Failures that were an HTTP 5xx (a subset of `failures`)."""

    timeouts: int = 0
    """Failures that were a connect/read timeout (a subset of `failures`)."""

    total_success_seconds: float = 0.0
    """Summed duration of successful calls (for the mean)."""

    max_success_seconds: float = 0.0
    """Slowest successful call seen (surfaces a pathologically slow-but-alive node)."""

    @property
    def success_rate(self) -> float:
        """Fraction of attempts that succeeded (`0.0` if never attempted)."""
        return self.successes / self.attempts if self.attempts else 0.0

    @property
    def failure_rate(self) -> float:
        """Fraction of attempts that did *not* succeed (`0.0` if never tried)."""
        return 1.0 - self.success_rate if self.attempts else 0.0

    @property
    def mean_success_seconds(self) -> float | None:
        """Mean duration of a successful call, or `None` if none succeeded."""
        if not self.successes:
            return None
        return self.total_success_seconds / self.successes


class IndexNodeHealth:
    """
    A thread-safe registry of per-endpoint search outcomes

    Examples
    --------
    >>> health = IndexNodeHealth()
    >>> url = "https://west/search"
    >>> health.record(url, SearchOutcome.SUCCESS, 0.9)
    >>> health.record(url, SearchOutcome.SERVER_ERROR, 30.0, retry=True)
    >>> stat = health.stat(url)
    >>> stat.attempts, stat.successes, stat.failures, stat.retries, stat.server_errors
    (2, 1, 1, 1, 1)
    >>> round(stat.success_rate, 2)
    0.5
    """

    def __init__(self) -> None:
        self._stats: dict[str, IndexNodeStat] = {}
        self._lock = threading.Lock()

    def record(
        self,
        endpoint: str,
        outcome: SearchOutcome,
        seconds: float,
        *,
        retry: bool = False,
    ) -> None:
        """
        Record one search call's outcome and duration against its endpoint

        Parameters
        ----------
        endpoint
            The search endpoint URL that was called (the key).

        outcome
            How the call ended.

        seconds
            Wall-clock duration of the call.

        retry
            Whether this call was a retry of an earlier attempt for the same
            dataset (counted so a run can see how much retrying an endpoint cost).
        """
        with self._lock:
            stat = self._stats.setdefault(endpoint, IndexNodeStat(endpoint))
            stat.attempts += 1
            if retry:
                stat.retries += 1
            if outcome is SearchOutcome.SUCCESS:
                stat.successes += 1
                stat.total_success_seconds += seconds
                stat.max_success_seconds = max(stat.max_success_seconds, seconds)
            else:
                stat.failures += 1
                if outcome is SearchOutcome.SERVER_ERROR:
                    stat.server_errors += 1
                elif outcome is SearchOutcome.TIMEOUT:
                    stat.timeouts += 1

    def restore(self, stat: IndexNodeStat) -> None:
        """
        Seed an endpoint's stats wholesale (e.g. from persisted rows)

        Replaces any existing counters for `stat.endpoint`, so a run can pick up
        where earlier runs left off before recording fresh outcomes on top.

        Parameters
        ----------
        stat
            The accumulated stats to install for `stat.endpoint`.
        """
        with self._lock:
            self._stats[stat.endpoint] = stat

    def stat(self, endpoint: str) -> IndexNodeStat | None:
        """Return the accumulated stats for `endpoint`, or `None` if unseen."""
        with self._lock:
            return self._stats.get(endpoint)

    def snapshot(self) -> dict[str, IndexNodeStat]:
        """Return a shallow copy of every endpoint's stats, keyed by endpoint."""
        with self._lock:
            return dict(self._stats)

    def rank_by_reliability(self, *, min_attempts: int = 1) -> list[IndexNodeStat]:
        """
        Rank endpoints best-to-worst by the share of calls that succeeded

        Ordered by ascending `failure_rate`, breaking ties in favour of the
        more-tried endpoint (more evidence) then alphabetically for determinism.

        Parameters
        ----------
        min_attempts
            Ignore endpoints tried fewer than this many times (too little evidence).

        Returns
        -------
        :
            The qualifying endpoints' stats, most reliable first.
        """
        with self._lock:
            candidates = [
                stat for stat in self._stats.values() if stat.attempts >= min_attempts
            ]
        return sorted(
            candidates, key=lambda s: (s.failure_rate, -s.attempts, s.endpoint)
        )
