"""
Track per-data-node health from file-download outcomes

The download counterpart of `health.NodeHealth`, kept separate on purpose: a header
read measures *latency* over a few KB, whereas a download measures *throughput* over
the whole (often multi-gigabyte) file — and a node that is unreachable for header
reads can still download perfectly well.  So download health is learned from
downloads alone, ranks nodes by **measured MB/s**, and never inherits header verdicts.

Like `NodeHealth` it is a passive, thread-safe observer: a download recorder wraps a
downloader and every transfer records its host, outcome, duration and bytes moved.
What to *do* with the numbers is the caller's choice — `rank_by_throughput` and
`host_rank` steer mirror selection, and the raw `snapshot` is persisted.

Deliberately, this registry has **no** `unreliable_hosts` method.  Download
dead-verdicts are decided per-run by the preflight probe (a live chunk fetch) and
held in a session-scoped cache; they are never derived from persisted health and
never carried across restarts (data nodes are a moving target).  Persisted download
health is ranking *information* only, never a cross-restart exclusion list.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from enum import Enum
from urllib.parse import urlparse

_BYTES_PER_MB = 1_000_000.0
"""Bytes in a megabyte (SI MB = 1e6 bytes) for the MB/s throughput figure."""


class DownloadOutcome(Enum):
    """How a single file download ended."""

    SUCCESS = "success"

    TIMEOUT = "timeout"
    """The transfer stalled past its deadline."""

    BLOCKED = "blocked"
    """The node rate-limited or refused us (HTTP 429/403/503) — host-level distress
    that should drive the node's concurrency down, kept distinct from a per-file
    `ERROR`."""

    HOST_FAULT = "host_fault"
    """The node was unreachable (SSL/DNS/connect failure) — skip it this run."""

    CHECKSUM_FAILED = "checksum_failed"
    """The bytes arrived but the digest did not match — a node-quality signal, kept
    distinct from a transient transfer `ERROR`."""

    ERROR = "error"
    """Any other transient transfer error (dropped connection, short read, ...)."""


@dataclass
class DownloadStat:
    """Accumulated download outcomes for one data node (host)."""

    host: str
    attempts: int = 0
    successes: int = 0
    timeouts: int = 0
    blocks: int = 0
    host_faults: int = 0
    checksum_failures: int = 0
    errors: int = 0

    total_bytes: int = 0
    """Summed bytes of successful downloads (numerator of the mean throughput)."""

    total_success_seconds: float = 0.0
    """Summed duration of successful downloads (denominator of the mean throughput)."""

    max_success_seconds: float = 0.0
    """Slowest successful download seen."""

    max_safe_concurrency: int = 0
    """Highest per-node connection count seen downloading cleanly (0 = unlearned)."""

    last_concurrency: int = 0
    """Per-node connection count this host converged on last run (0 = unlearned)."""

    @property
    def success_rate(self) -> float:
        """Fraction of attempts that succeeded (`0.0` if never attempted)."""
        return self.successes / self.attempts if self.attempts else 0.0

    @property
    def failure_rate(self) -> float:
        """Fraction of attempts that did *not* succeed (`0.0` if never tried)."""
        return 1.0 - self.success_rate if self.attempts else 0.0

    @property
    def mean_mbps(self) -> float | None:
        """
        Mean throughput of successful downloads in MB/s, or `None` if none succeeded

        This is the download-speed signal a header read cannot provide: total
        successful bytes over total successful seconds (MB = 1e6 bytes).
        """
        if not self.successes or self.total_success_seconds <= 0.0:
            return None
        return (self.total_bytes / _BYTES_PER_MB) / self.total_success_seconds


class DownloadNodeHealth:
    """
    A thread-safe registry of per-host download outcomes

    Examples
    --------
    >>> health = DownloadNodeHealth()
    >>> ok = DownloadOutcome.SUCCESS
    >>> health.record("https://fast/f", ok, 2.0, num_bytes=20_000_000)
    >>> health.record("https://slow/f", ok, 20.0, num_bytes=20_000_000)
    >>> round(health.stat("fast").mean_mbps)
    10
    >>> [s.host for s in health.rank_by_throughput()]
    ['fast', 'slow']
    """

    def __init__(self) -> None:
        self._stats: dict[str, DownloadStat] = {}
        self._lock = threading.Lock()

    def record(
        self, url: str, outcome: DownloadOutcome, seconds: float, *, num_bytes: int = 0
    ) -> None:
        """
        Record one download's outcome, duration and bytes against its host

        Parameters
        ----------
        url
            The URL that was downloaded (its hostname is the key).

        outcome
            How the download ended.

        seconds
            Wall-clock duration of the transfer.

        num_bytes
            Bytes transferred (only counted towards throughput on success).
        """
        host = urlparse(url).hostname or url
        with self._lock:
            stat = self._stats.setdefault(host, DownloadStat(host))
            stat.attempts += 1
            if outcome is DownloadOutcome.SUCCESS:
                stat.successes += 1
                stat.total_bytes += num_bytes
                stat.total_success_seconds += seconds
                stat.max_success_seconds = max(stat.max_success_seconds, seconds)
            elif outcome is DownloadOutcome.TIMEOUT:
                stat.timeouts += 1
            elif outcome is DownloadOutcome.BLOCKED:
                stat.blocks += 1
            elif outcome is DownloadOutcome.HOST_FAULT:
                stat.host_faults += 1
            elif outcome is DownloadOutcome.CHECKSUM_FAILED:
                stat.checksum_failures += 1
            else:
                stat.errors += 1

    def record_concurrency(self, host: str, *, max_safe: int, last: int) -> None:
        """
        Record the per-node concurrency the adaptive controller learned for a host

        `max_safe` accumulates as the largest safe level ever seen (across runs);
        `last` is overwritten with where the cap converged this run, to seed the next
        one.  Creates the host's stats if this is the first thing recorded.

        Parameters
        ----------
        host
            The data node (hostname).

        max_safe
            Highest simultaneous download count that ran cleanly on this host this run.

        last
            The per-node cap this host converged on at the end of the run.
        """
        with self._lock:
            stat = self._stats.setdefault(host, DownloadStat(host))
            stat.max_safe_concurrency = max(stat.max_safe_concurrency, max_safe)
            stat.last_concurrency = last

    def restore(self, stat: DownloadStat) -> None:
        """
        Seed a host's stats wholesale (e.g. from persisted `DownloadNodeHealthStat`)

        Replaces any existing counters for `stat.host`, so a run can pick up where
        earlier runs left off before recording fresh outcomes on top.

        Parameters
        ----------
        stat
            The accumulated stats to install for `stat.host`.
        """
        with self._lock:
            self._stats[stat.host] = stat

    def stat(self, host: str) -> DownloadStat | None:
        """Return the accumulated stats for `host`, or `None` if unseen."""
        with self._lock:
            return self._stats.get(host)

    def snapshot(self) -> dict[str, DownloadStat]:
        """Return a shallow copy of every host's stats, keyed by hostname."""
        with self._lock:
            return dict(self._stats)

    def rank_by_throughput(self, *, min_successes: int = 1) -> list[DownloadStat]:
        """
        Rank hosts fastest-to-slowest by mean download throughput (MB/s)

        Answers "which nodes download quickest?" — the signal that actually matters
        for downloads.  Only hosts with at least `min_successes` successful downloads
        are ranked (a node with no success has no meaningful throughput), ordered by
        descending `mean_mbps`.

        Parameters
        ----------
        min_successes
            Ignore hosts with fewer successful downloads than this.

        Returns
        -------
        :
            The qualifying hosts' stats, fastest first.
        """
        with self._lock:
            candidates = [
                stat for stat in self._stats.values() if stat.successes >= min_successes
            ]
        return sorted(candidates, key=lambda s: (-(s.mean_mbps or 0.0), s.host))

    def rank_by_reliability(self, *, min_attempts: int = 1) -> list[DownloadStat]:
        """
        Rank hosts best-to-worst by the share of downloads that succeeded

        Ordered by ascending `failure_rate`, breaking ties in favour of the
        more-tried host (more evidence) then alphabetically for determinism.

        Parameters
        ----------
        min_attempts
            Ignore hosts tried fewer than this many times (too little evidence).

        Returns
        -------
        :
            The qualifying hosts' stats, most reliable first.
        """
        with self._lock:
            candidates = [
                stat for stat in self._stats.values() if stat.attempts >= min_attempts
            ]
        return sorted(candidates, key=lambda s: (s.failure_rate, -s.attempts, s.host))

    def host_rank(
        self, host: str, *, min_attempts: int = 1, unseen_score: float = 0.5
    ) -> tuple[float, float]:
        """
        Return a best-first sort key for a host, from its observed download health

        Designed to be passed to `headers.order_candidates(..., host_rank=...)` so
        mirror ordering reflects download behaviour.  The key is
        `(failure_rate, -mean_mbps)`: proven-reliable nodes sort ahead of flaky ones,
        and among equally reliable nodes the **faster** (higher MB/s) one sorts first
        (throughput is negated so lower-is-better ordering prefers it).

        A host with fewer than `min_attempts` attempts is *unseen* and gets a neutral
        `failure_rate` of `unseen_score` (default `0.5`): it ranks behind nodes that
        have proved themselves but ahead of ones that have proved unreliable, so
        untried nodes are still explored rather than trusted or condemned on no
        evidence.

        Parameters
        ----------
        host
            Hostname to score.

        min_attempts
            Attempts below which a host is treated as unseen.

        unseen_score
            The neutral failure-rate score given to unseen hosts.

        Returns
        -------
        :
            The `(failure_rate, -mbps)` sort key (lower is better).
        """
        with self._lock:
            stat = self._stats.get(host)
        if stat is None or stat.attempts < min_attempts:
            return (unseen_score, 0.0)
        return (stat.failure_rate, -(stat.mean_mbps or 0.0))
