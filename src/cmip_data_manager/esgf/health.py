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

from cmip_data_manager.esgf.headers import HeaderReadCrashed, HeaderReadTimeout

T = TypeVar("T")


class ReadOutcome(Enum):
    """How a single header read ended."""

    SUCCESS = "success"
    TIMEOUT = "timeout"
    """The node stalled past the deadline (`HeaderReadTimeout`)."""

    CRASH = "crash"
    """The worker died mid-read (`HeaderReadCrashed`)."""

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
    total_success_seconds: float = 0.0
    """Summed duration of successful reads (for the mean)."""

    @property
    def success_rate(self) -> float:
        """Fraction of attempts that succeeded (`0.0` if never attempted)."""
        return self.successes / self.attempts if self.attempts else 0.0

    @property
    def mean_success_seconds(self) -> float | None:
        """Mean duration of a successful read, or `None` if none succeeded."""
        if not self.successes:
            return None
        return self.total_success_seconds / self.successes


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
            elif outcome is ReadOutcome.TIMEOUT:
                stat.timeouts += 1
            elif outcome is ReadOutcome.CRASH:
                stat.crashes += 1
            else:
                stat.errors += 1

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


def recording(reader: Callable[[str], T], health: NodeHealth) -> Callable[[str], T]:
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
        Registry to record into.

    Returns
    -------
    :
        A reader with the same contract that records before returning/raising.
    """

    def read(url: str) -> T:
        started = time.monotonic()
        try:
            result = reader(url)
        except HeaderReadTimeout:
            health.record(url, ReadOutcome.TIMEOUT, time.monotonic() - started)
            raise
        except HeaderReadCrashed:
            health.record(url, ReadOutcome.CRASH, time.monotonic() - started)
            raise
        except OSError:
            health.record(url, ReadOutcome.ERROR, time.monotonic() - started)
            raise
        health.record(url, ReadOutcome.SUCCESS, time.monotonic() - started)
        return result

    return read
