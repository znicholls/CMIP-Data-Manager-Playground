"""
Track per-data-node health from header-read outcomes

Reaching a *good* mirror is the real bottleneck in reading headers: nodes vary
enormously in speed (a nearby node read a header in ~1s where a distant one took
~20s) and reliability (some connect then stall for ~25 minutes, some are simply
dead).  `NodeHealth` accumulates what actually happened per host so that knowledge
can steer later reads — demoting or ignoring bad nodes and informing the read
timeout.

It is a passive observer: wrap a reader with `recording(reader, health)` and every
read records its host, outcome and duration.  The registry is thread-safe because
reads are fanned out across threads (see `headers.with_timeout`).  What to *do*
with the numbers is the caller's choice — `unreliable_hosts` yields an
`ignore_hosts` set, `mean_read_seconds` informs a sensible timeout, and the raw
`snapshot` can be persisted or ordered on.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import TypeVar
from urllib.parse import urlparse

from cmip_data_manager.esgf.headers import (
    HeaderReadBlocked,
    HeaderReadCrashed,
    HeaderReadTimeout,
)

T = TypeVar("T")


class ReadOutcome(Enum):
    """How a single header read ended."""

    SUCCESS = "success"
    TIMEOUT = "timeout"
    """The node stalled past the deadline (`HeaderReadTimeout`)."""

    CRASH = "crash"
    """The worker died mid-read (`HeaderReadCrashed`)."""

    BLOCKED = "blocked"
    """The node rate-limited or refused us (`HeaderReadBlocked`) — host-level
    distress that should drive the node's concurrency down, kept distinct from a
    per-file `ERROR`."""

    ERROR = "error"
    """Any other `OSError` (connection refused, missing byte-range, ...)."""


@dataclass
class NodeStat:
    """Accumulated read outcomes for one data node (host)."""

    host: str
    attempts: int = 0
    successes: int = 0
    timeouts: int = 0
    crashes: int = 0
    errors: int = 0
    blocks: int = 0
    """Reads that ended in a node-level block/rate-limit (`ReadOutcome.BLOCKED`)."""

    total_success_seconds: float = 0.0
    """Summed duration of successful reads (for the mean)."""

    max_success_seconds: float = 0.0
    """Slowest successful read seen (for sizing a data-driven timeout)."""

    max_safe_concurrency: int = 0
    """
    Highest per-node connection count (`L_n`) observed running cleanly on this host.

    Learned by the adaptive concurrency controller (AIMD): the ceiling this node
    tolerated without a block signal.  `0` means "not yet learned".
    """

    last_concurrency: int = 0
    """
    The per-node connection count (`L_n`) this host converged on at the end of a run.

    Persisted so a later run can seed the node's starting concurrency from where it
    settled rather than re-probing from the conservative default.  `0` means "not
    yet learned".
    """

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
        """Mean duration of a successful read, or `None` if none succeeded."""
        if not self.successes:
            return None
        return self.total_success_seconds / self.successes


@dataclass(frozen=True)
class AttemptRecord:
    """One header-read attempt: the URL tried, how it ended, and how long it took."""

    url: str
    outcome: ReadOutcome
    seconds: float


class AttemptLog:
    """
    A thread-safe collector of individual read attempts

    Where `NodeHealth` keeps per-host *aggregates*, this keeps the raw per-attempt
    records (including `with_retry` sub-attempts) so a caller can persist the full
    trail.  Pass one to `recording(reader, health, attempts=log)` and every read
    appends its `(url, outcome, seconds)`; reads run across threads, so appends are
    lock-guarded.

    Examples
    --------
    >>> log = AttemptLog()
    >>> log.add("https://good/f.nc", ReadOutcome.SUCCESS, 1.2)
    >>> [(r.url, r.outcome.value) for r in log.records()]
    [('https://good/f.nc', 'success')]
    """

    def __init__(self) -> None:
        self._records: list[AttemptRecord] = []
        self._lock = threading.Lock()

    def add(self, url: str, outcome: ReadOutcome, seconds: float) -> None:
        """Append one attempt record (thread-safe)."""
        with self._lock:
            self._records.append(AttemptRecord(url, outcome, seconds))

    def records(self) -> list[AttemptRecord]:
        """Return the attempts recorded so far, in append order (a copy)."""
        with self._lock:
            return list(self._records)


class NodeHealth:
    """
    A thread-safe registry of per-host read outcomes

    Examples
    --------
    >>> health = NodeHealth()
    >>> health.record("https://good/f.nc", ReadOutcome.SUCCESS, 1.2)
    >>> health.record("https://dead/f.nc", ReadOutcome.TIMEOUT, 90.0)
    >>> health.stat("good").success_rate
    1.0
    >>> sorted(health.unreliable_hosts(min_attempts=1))
    ['dead']
    """

    def __init__(self) -> None:
        self._stats: dict[str, NodeStat] = {}
        self._lock = threading.Lock()

    def record(self, url: str, outcome: ReadOutcome, seconds: float) -> None:
        """
        Record one read's outcome and duration against its host

        Parameters
        ----------
        url
            The URL that was read (its hostname is the key).

        outcome
            How the read ended.

        seconds
            Wall-clock duration of the read.
        """
        host = urlparse(url).hostname or url
        with self._lock:
            stat = self._stats.setdefault(host, NodeStat(host))
            stat.attempts += 1
            if outcome is ReadOutcome.SUCCESS:
                stat.successes += 1
                stat.total_success_seconds += seconds
                stat.max_success_seconds = max(stat.max_success_seconds, seconds)
            elif outcome is ReadOutcome.TIMEOUT:
                stat.timeouts += 1
            elif outcome is ReadOutcome.CRASH:
                stat.crashes += 1
            elif outcome is ReadOutcome.BLOCKED:
                stat.blocks += 1
            else:
                stat.errors += 1

    def record_concurrency(self, host: str, *, max_safe: int, last: int) -> None:
        """
        Record the per-node concurrency the adaptive controller learned for a host

        `max_safe` accumulates as the largest safe level ever seen (across runs);
        `last` is overwritten with where the cap converged this run, to seed the
        next one.  Creates the host's stats if this is the first thing recorded.

        Parameters
        ----------
        host
            The data node (hostname).

        max_safe
            Highest simultaneous read count that ran cleanly on this host this run.

        last
            The per-node cap this host converged on at the end of the run.
        """
        with self._lock:
            stat = self._stats.setdefault(host, NodeStat(host))
            stat.max_safe_concurrency = max(stat.max_safe_concurrency, max_safe)
            stat.last_concurrency = last

    def restore(self, stat: NodeStat) -> None:
        """
        Seed a host's stats wholesale (e.g. from persisted `NodeHealthStat` rows)

        Replaces any existing counters for `stat.host`, so a run can pick up where
        earlier runs left off before recording fresh outcomes on top.

        Parameters
        ----------
        stat
            The accumulated stats to install for `stat.host`.
        """
        with self._lock:
            self._stats[stat.host] = stat

    def stat(self, host: str) -> NodeStat | None:
        """Return the accumulated stats for `host`, or `None` if unseen."""
        with self._lock:
            return self._stats.get(host)

    def snapshot(self) -> dict[str, NodeStat]:
        """Return a shallow copy of every host's stats, keyed by hostname."""
        with self._lock:
            return dict(self._stats)

    def mean_read_seconds(self) -> float | None:
        """
        Return the mean successful-read time across all hosts

        Useful for choosing a read timeout from observed behaviour rather than a
        guess.  `None` until at least one read has succeeded.
        """
        with self._lock:
            successes = sum(s.successes for s in self._stats.values())
            total = sum(s.total_success_seconds for s in self._stats.values())
        return total / successes if successes else None

    def unreliable_hosts(
        self, *, min_attempts: int = 3, max_success_rate: float = 0.0
    ) -> frozenset[str]:
        """
        Return hosts that have proved unreliable, for use as `ignore_hosts`

        A host qualifies once it has been tried at least `min_attempts` times and
        its success rate is at or below `max_success_rate` (the default `0.0`
        means "has never once succeeded").

        Parameters
        ----------
        min_attempts
            Minimum attempts before a host can be judged (avoids condemning a host
            on a single blip).

        max_success_rate
            Success-rate threshold at or below which a host is deemed unreliable.

        Returns
        -------
        :
            The hostnames to avoid.
        """
        with self._lock:
            return frozenset(
                host
                for host, stat in self._stats.items()
                if stat.attempts >= min_attempts
                and stat.success_rate <= max_success_rate
            )

    def rank_by_reliability(self, *, min_attempts: int = 1) -> list[NodeStat]:
        """
        Rank hosts best-to-worst by the share of reads that succeeded

        Answers "which nodes fail the fewest header requests?".  Hosts are ordered
        by ascending `failure_rate`, breaking ties in favour of the more-tried host
        (more evidence) then alphabetically for determinism.

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

    def rank_by_speed(self, *, min_successes: int = 1) -> list[NodeStat]:
        """
        Rank hosts fastest-to-slowest by mean successful-read time

        Answers "which nodes respond quickest?".  Only hosts with at least
        `min_successes` successful reads are ranked (a node with no success has no
        meaningful speed), ordered by ascending `mean_success_seconds`.

        Parameters
        ----------
        min_successes
            Ignore hosts with fewer successful reads than this.

        Returns
        -------
        :
            The qualifying hosts' stats, fastest first.
        """
        with self._lock:
            candidates = [
                stat for stat in self._stats.values() if stat.successes >= min_successes
            ]
        return sorted(candidates, key=lambda s: (s.mean_success_seconds or 0.0, s.host))

    def host_rank(
        self, host: str, *, min_attempts: int = 1, unseen_score: float = 0.5
    ) -> tuple[float, float]:
        """
        Return a best-first sort key for a host, from its observed health

        Designed to be passed to `headers.order_candidates(..., host_rank=...)` so
        mirror ordering reflects what a node has actually done.  The key is
        `(failure_rate, mean_success_seconds)`: proven-reliable nodes sort ahead of
        flaky ones, and faster nodes break ties among equally reliable ones.

        A host with fewer than `min_attempts` attempts is *unseen* and gets a
        neutral `failure_rate` of `unseen_score` (default `0.5`): it ranks behind
        nodes that have proved themselves (failure rate below `0.5`) but ahead of
        nodes that have proved unreliable (above `0.5`), so untried nodes still get
        explored rather than being trusted or condemned on no evidence.

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
            The `(failure_rate, seconds)` sort key (lower is better).
        """
        with self._lock:
            stat = self._stats.get(host)
        if stat is None or stat.attempts < min_attempts:
            return (unseen_score, 0.0)
        return (stat.failure_rate, stat.mean_success_seconds or float("inf"))

    def suggested_timeout(
        self, *, safety: float = 1.5, floor: float = 10.0, default: float | None = None
    ) -> float | None:
        """
        Suggest a read timeout from the slowest *healthy* read observed

        The timeout only needs to outlast a genuinely slow-but-alive node; anything
        beyond that is a stall to be cut off.  This takes the slowest successful
        read seen on any host, pads it by `safety`, and floors it so a couple of
        fast early reads cannot set an absurdly tight deadline.  For example, a
        worst healthy read of 45s with `safety=1.5` suggests ~68s — meaningfully
        tighter than a blanket 90s while still clearing a real 45s read.

        Parameters
        ----------
        safety
            Multiplier applied to the slowest healthy read (headroom).

        floor
            Lower bound on the suggested timeout.

        default
            Returned when no read has yet succeeded (no evidence to size from).

        Returns
        -------
        :
            The suggested timeout in seconds, or `default` if nothing has
            succeeded.
        """
        with self._lock:
            worst = max(
                (s.max_success_seconds for s in self._stats.values() if s.successes),
                default=0.0,
            )
        if worst <= 0.0:
            return default
        return max(floor, worst * safety)


def recording(
    reader: Callable[[str], T],
    health: NodeHealth,
    *,
    attempts: AttemptLog | None = None,
) -> Callable[[str], T]:
    """
    Wrap a reader so every read's outcome and duration are recorded

    Compose this *outside* `headers.with_timeout` (this timing/recording stays in
    the parent process; only the wrapped read crosses into the child):

    ```python
    reader = recording(with_timeout(read_header, seconds=90), health)
    ```

    Parameters
    ----------
    reader
        The read to observe (typically a timeout-wrapped `read_header`).

    health
        Registry to record aggregate per-host outcomes into.

    attempts
        Optional per-attempt log; when given, each read also appends its raw
        `(url, outcome, seconds)` record (including `with_retry` sub-attempts), for
        callers that persist the full trail.

    Returns
    -------
    :
        A reader with the same contract that records before returning/raising.
    """

    def emit(url: str, outcome: ReadOutcome, seconds: float) -> None:
        health.record(url, outcome, seconds)
        if attempts is not None:
            attempts.add(url, outcome, seconds)

    def read(url: str) -> T:
        started = time.monotonic()
        try:
            result = reader(url)
        except HeaderReadTimeout:
            emit(url, ReadOutcome.TIMEOUT, time.monotonic() - started)
            raise
        except HeaderReadCrashed:
            emit(url, ReadOutcome.CRASH, time.monotonic() - started)
            raise
        except HeaderReadBlocked:
            emit(url, ReadOutcome.BLOCKED, time.monotonic() - started)
            raise
        except OSError:
            emit(url, ReadOutcome.ERROR, time.monotonic() - started)
            raise
        emit(url, ReadOutcome.SUCCESS, time.monotonic() - started)
        return result

    return read
