"""
Node-centric dispatch of header reads across data nodes

Where `read_first_readable` reads *one* simulation independently, this schedules
*many* simulations across the data nodes that can serve them, honouring an
**adaptive** per-node connection cap and a shared local worker budget.  The policy
is **best-node assignment with spill**:

- each simulation is assigned to its best candidate host (preferred → healthy →
  HTTPS, as ranked in `SimulationCandidates.hosts`) that is currently **under its
  cap** — so a preferred/fast node (e.g. NCI) gets first refusal up to its cap;
- if that host is saturated, the simulation **spills** to its next-best host that
  has a free slot rather than idling a capable node — but waits (does not spill)
  when no alternative is free;
- a host that fails a simulation is dropped from that simulation's candidates and
  the simulation is **requeued** to its next-best host; only when every candidate
  is exhausted is it recorded as failed.

The per-node cap adapts as the run learns each node's tolerance (AIMD):

- a streak of clean successes **grows** the cap by one (up to a hard `ceiling`);
- a **block** signal (`HeaderReadBlocked` — an HTTP 429/403/503) **halves** it, so
  we back off a node that is rate-limiting us rather than hammering it;
- a node whose **success rate** stays at/below `evict_max_success_rate` once it has
  `evict_after_attempts` reads is **evicted** for the rest of the run (its queue
  drains to other nodes) — rate-based, so a busy healthy node surviving a short
  failure run is not wrongly dropped.

The learned caps are returned (`DispatchResult.learned`) so a caller can persist
them and seed the next run.  Scheduling state lives entirely in the single
controller thread (`_Scheduler`); only the bounded reads run on the pool, so no
locks are needed beyond the ones the reader already holds.  The reads are I/O-bound
(and the default reader isolates each in a child process), so they are fanned out
on a **thread** pool, never a process pool.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from enum import Enum, auto

from cmip_data_manager.esgf.headers import (
    HeaderMetadata,
    HeaderReadBlocked,
    SimulationKey,
)
from cmip_data_manager.esgf.routing import SimulationCandidates

HeaderReader = Callable[[str], HeaderMetadata]
"""Reads one file's header from a URL (typically wrapped with timeout/retry)."""

# QUESTION: would this be user specific? e.g. if running on cmip-cruncher server
# with 64 CPUs could have more workers?
DEFAULT_MAX_WORKERS = 12
"""Default cap on header reads in flight *anywhere* (the shared local budget)."""

DEFAULT_NODE_CONCURRENCY = 2
"""
Default cap on simultaneous reads to a *single* host.

Deliberately conservative: these are shared scientific servers, and this is the
cold-start value before the controller has learned a node's tolerance.  Raise it
per-host (e.g. NCI) via `concurrency_limit(overrides=...)`.
"""

DEFAULT_CONCURRENCY_CEILING = 8
"""Hard upper bound the adaptive cap will never grow a node past (stay polite)."""

DEFAULT_INCREASE_AFTER = 4
"""Consecutive clean successes on a host before its cap grows by one."""

DEFAULT_EVICT_AFTER_ATTEMPTS = 6
"""
Minimum reads on a host before it can be judged for eviction.

A *rate*, not a streak: a healthy but busy node routinely hits a short run of
failures (e.g. one model with missing files on that mirror, made consecutive by
affinity clustering), so evicting on a raw streak wrongly kills workhorse nodes.
This waits for a fair sample of attempts before judging a node by its success rate.
"""

DEFAULT_EVICT_MAX_SUCCESS_RATE = 0.2
"""
Evict a judged host whose overall success rate is at or below this.

Kept low so only near-dead nodes are dropped; a node succeeding even a fifth of the
time is kept (its failures just requeue their simulations elsewhere).
"""


def concurrency_limit(
    default: int = DEFAULT_NODE_CONCURRENCY,
    overrides: Mapping[str, int] | None = None,
) -> Callable[[str], int]:
    """
    Build a per-host *starting* connection cap from a default and overrides

    Parameters
    ----------
    default
        Cap applied to any host without an override.

    overrides
        Per-host caps (e.g. `{"esgf.nci.org.au": 4}`), for nodes known to tolerate
        more (or less) than the default.

    Returns
    -------
    :
        A `host -> cap` callable; every cap is at least `1` so no host is starved.

    Examples
    --------
    >>> limit = concurrency_limit(2, {"nci": 4})
    >>> (limit("nci"), limit("other"))
    (4, 2)
    """
    table = dict(overrides or {})

    def limit(host: str) -> int:
        return max(1, table.get(host, default))

    return limit


@dataclass(frozen=True)
class DispatchResult:
    """Outcome of a `dispatch_reads` pass."""

    headers: dict[SimulationKey, HeaderMetadata] = field(default_factory=dict)
    """The header read for each simulation that succeeded."""

    failed: list[SimulationKey] = field(default_factory=list)
    """Simulations whose every candidate host failed (or that had no candidate)."""

    learned: dict[str, tuple[int, int]] = field(default_factory=dict)
    """Per touched host, the `(max_safe_concurrency, last_concurrency)` learned."""


class _Status(Enum):
    """Where a simulation is in the schedule."""

    PENDING = auto()
    IN_FLIGHT = auto()
    DONE = auto()
    FAILED = auto()


class _Outcome(Enum):
    """How one dispatched read ended, from the scheduler's point of view."""

    SUCCESS = auto()
    BLOCKED = auto()
    """A node-level rate-limit/refusal (`HeaderReadBlocked`) — backs the node off."""

    FAILURE = auto()
    """Any other failure (stall, crash, refusal) — requeue, no concurrency change."""


class _Scheduler:
    """
    The pure, single-threaded scheduling + concurrency-control core

    Holds the mutable plan — remaining candidate hosts per simulation, per-host
    in-flight counts and *adaptive* caps, statuses — and answers
    `next_assignment()` with the next `(simulation, host)` to read, or `None` when
    nothing can start right now.  The driver feeds completions back via
    `on_success` / `on_block` / `on_failure`, which is where the cap adapts and
    nodes are evicted.  No threads, no I/O: fully deterministic and testable.
    """

    def __init__(  # noqa: PLR0913 - scheduling + AIMD knobs, all keyword from callers
        self,
        candidates: Mapping[SimulationKey, SimulationCandidates],
        *,
        initial_concurrency: Callable[[str], int],
        max_workers: int,
        ceiling: int = DEFAULT_CONCURRENCY_CEILING,
        increase_after: int = DEFAULT_INCREASE_AFTER,
        evict_after_attempts: int = DEFAULT_EVICT_AFTER_ATTEMPTS,
        evict_max_success_rate: float = DEFAULT_EVICT_MAX_SUCCESS_RATE,
        pinned_hosts: frozenset[str] = frozenset(),
    ) -> None:
        self._candidates = candidates
        self._initial = initial_concurrency
        self._max_workers = max(1, max_workers)
        self._ceiling = max(1, ceiling)
        self._increase_after = max(1, increase_after)
        self._evict_after_attempts = max(1, evict_after_attempts)
        self._evict_max_success_rate = evict_max_success_rate
        self._pinned = pinned_hosts

        self._remaining: dict[SimulationKey, list[str]] = {}
        self._status: dict[SimulationKey, _Status] = {}
        self._in_flight: dict[str, int] = {}
        self._global_in_flight = 0

        self._limit: dict[str, int] = {}
        self._successes: dict[str, int] = {}  # consecutive clean successes (for AIMD)
        self._attempts: dict[str, int] = {}  # total completed reads (for evict rate)
        self._success_total: dict[str, int] = {}  # total successes (for evict rate)
        self._max_safe: dict[str, int] = {}
        self._evicted: set[str] = set()
        self.failed: list[SimulationKey] = []

        for simulation in self._priority_order(candidates):
            hosts = list(candidates[simulation].hosts)
            self._remaining[simulation] = hosts
            if hosts:
                self._status[simulation] = _Status.PENDING
            else:  # nothing can serve it — fail it up front
                self._status[simulation] = _Status.FAILED
                self.failed.append(simulation)
        self._order = list(self._status)

    @staticmethod
    def _priority_order(
        candidates: Mapping[SimulationKey, SimulationCandidates],
    ) -> list[SimulationKey]:
        """Order simulations so shared-affinity ones are adjacent (deterministic)."""
        buckets: dict[object, list[SimulationKey]] = {}
        for simulation in sorted(candidates):
            buckets.setdefault(candidates[simulation].group, []).append(simulation)
        return [simulation for group in buckets.values() for simulation in group]

    def current_limit(self, host: str) -> int:
        """Return the host's current adaptive cap (`0` once it has been evicted)."""
        if host in self._evicted:
            return 0
        if host not in self._limit:
            self._limit[host] = max(1, min(self._ceiling, self._initial(host)))
        return self._limit[host]

    def next_assignment(self) -> tuple[SimulationKey, str] | None:
        """
        Return the next `(simulation, host)` to read, or `None` if none can start

        Picks the first pending simulation (in affinity-clustered order) whose
        best remaining host is under its cap, spilling to a next-best host that has
        a free slot.  Returns `None` when the global worker budget is full or every
        pending simulation's hosts are saturated.
        """
        if self._global_in_flight >= self._max_workers:
            return None
        for simulation in self._order:
            if self._status[simulation] is not _Status.PENDING:
                continue
            for host in self._remaining[simulation]:
                if self._in_flight.get(host, 0) < self.current_limit(host):
                    self._status[simulation] = _Status.IN_FLIGHT
                    self._in_flight[host] = self._in_flight.get(host, 0) + 1
                    self._global_in_flight += 1
                    return (simulation, host)
        return None

    def on_success(self, simulation: SimulationKey, host: str) -> None:
        """Mark a simulation done, free the slot, and grow the cap on a streak."""
        # Concurrency actually achieved (this read still counts as in flight).
        self._max_safe[host] = max(self._max_safe.get(host, 0), self._in_flight[host])
        self._release(host)
        self._status[simulation] = _Status.DONE
        self._attempts[host] = self._attempts.get(host, 0) + 1
        self._success_total[host] = self._success_total.get(host, 0) + 1
        if host not in self._pinned and host not in self._evicted:
            self._successes[host] = self._successes.get(host, 0) + 1
            if (
                self._successes[host] >= self._increase_after
                and self.current_limit(host) < self._ceiling
            ):
                self._limit[host] += 1
                self._successes[host] = 0

    def on_block(self, simulation: SimulationKey, host: str) -> None:
        """Back the node off (halve its cap), count the failure, and requeue."""
        self._release(host)
        if host not in self._pinned:
            self._limit[host] = max(1, self.current_limit(host) // 2)
        self._register_failure(host)
        self._requeue(simulation, host)

    def on_failure(self, simulation: SimulationKey, host: str) -> None:
        """Count a (non-block) failure, evict on a streak, and requeue."""
        self._release(host)
        self._register_failure(host)
        self._requeue(simulation, host)

    def _register_failure(self, host: str) -> None:
        self._successes[host] = 0  # a failure breaks the AIMD success streak
        self._attempts[host] = self._attempts.get(host, 0) + 1
        # Evict on a *low success rate* over a fair sample, not on a raw streak: a
        # busy healthy node hits short failure runs (one bad model, made
        # consecutive by affinity) that must not evict it.
        attempts = self._attempts[host]
        if attempts >= self._evict_after_attempts:
            rate = self._success_total.get(host, 0) / attempts
            if rate <= self._evict_max_success_rate:
                self._evict(host)

    def _requeue(self, simulation: SimulationKey, host: str) -> None:
        if host in self._remaining[simulation]:
            self._remaining[simulation].remove(host)
        if self._remaining[simulation]:
            self._status[simulation] = _Status.PENDING
        else:
            self._status[simulation] = _Status.FAILED
            self.failed.append(simulation)

    def _evict(self, host: str) -> None:
        """Drop a node for the rest of the run and drain its pending queue away."""
        self._evicted.add(host)
        self._limit[host] = 0  # a learned cap of 0 marks the node as evicted
        for simulation in self._order:
            if (
                self._status[simulation] is _Status.PENDING
                and host in self._remaining[simulation]
            ):
                self._remaining[simulation].remove(host)
                if not self._remaining[simulation]:
                    self._status[simulation] = _Status.FAILED
                    self.failed.append(simulation)

    def _release(self, host: str) -> None:
        self._in_flight[host] -= 1
        self._global_in_flight -= 1

    def learned(self) -> dict[str, tuple[int, int]]:
        """Return `host -> (max_safe, last)` concurrency for every touched host."""
        return {
            host: (self._max_safe.get(host, 0), limit)
            for host, limit in self._limit.items()
        }


def dispatch_reads(  # noqa: PLR0913 - dispatch + AIMD knobs, all keyword-only
    candidates: Mapping[SimulationKey, SimulationCandidates],
    reader: HeaderReader,
    *,
    initial_concurrency: Callable[[str], int] | None = None,
    pinned_hosts: frozenset[str] = frozenset(),
    max_workers: int = DEFAULT_MAX_WORKERS,
    ceiling: int = DEFAULT_CONCURRENCY_CEILING,
    increase_after: int = DEFAULT_INCREASE_AFTER,
    evict_after_attempts: int = DEFAULT_EVICT_AFTER_ATTEMPTS,
    evict_max_success_rate: float = DEFAULT_EVICT_MAX_SUCCESS_RATE,
) -> DispatchResult:
    """
    Read one header per simulation, routed across data nodes with adaptive load

    Drives a `_Scheduler`: it dispatches every currently-startable `(simulation,
    host)` onto a thread pool, waits for the first read to finish, applies the
    outcome (success, back-off-and-requeue on a block, or requeue on any other
    failure), and repeats until nothing is pending or in flight.

    Each dispatch reads the host's mirror URLs for the simulation in ranked order
    (e.g. an `https` twin before its `http` original), taking the first that reads
    and only then dropping the host.  A `HeaderReadBlocked` on any of them halves
    the node's cap and stops immediately (node-level distress, not a per-URL fault);
    any other `OSError` falls through to the host's next URL, and once all fail the
    simulation requeues to its next host; a node whose success rate stays low over
    enough attempts is evicted.

    Parameters
    ----------
    candidates
        Per-simulation ranked candidates (e.g. from `build_candidates`).

    reader
        Per-URL header read, already wrapped with timeout/recording/retry (and
        `promote_blocks`, so a rate-limit surfaces as `HeaderReadBlocked`).

    initial_concurrency
        Per-host *starting* cap; defaults to `concurrency_limit()` (flat default).

    pinned_hosts
        Hosts whose cap is fixed — the adaptive grow/shrink is skipped (they can
        still be evicted if they fail outright).

    max_workers
        Cap on reads in flight anywhere (the thread-pool size).

    ceiling, increase_after, evict_after_attempts, evict_max_success_rate
        Adaptive-cap and eviction knobs (see the module docstring).

    Returns
    -------
    :
        The headers read, the simulations that failed on every candidate, and the
        per-host caps learned (for persistence/seeding the next run).
    """
    limit = (
        initial_concurrency if initial_concurrency is not None else concurrency_limit()
    )
    scheduler = _Scheduler(
        candidates,
        initial_concurrency=limit,
        max_workers=max_workers,
        ceiling=ceiling,
        increase_after=increase_after,
        evict_after_attempts=evict_after_attempts,
        evict_max_success_rate=evict_max_success_rate,
        pinned_hosts=pinned_hosts,
    )
    headers: dict[SimulationKey, HeaderMetadata] = {}

    def read_on_host(
        simulation: SimulationKey, host: str
    ) -> tuple[_Outcome, HeaderMetadata | None]:
        # Try the host's mirror URLs in ranked order (e.g. an https twin ahead of
        # its http original), falling through only on a plain failure.  A block is
        # node-level distress, so it stops and backs the host off immediately rather
        # than hammering its other URLs.
        outcome: _Outcome = _Outcome.FAILURE
        for url in candidates[simulation].urls_by_host[host]:
            try:
                metadata = reader(url)
            except HeaderReadBlocked:
                return (_Outcome.BLOCKED, None)
            except OSError:
                outcome = _Outcome.FAILURE
                continue
            return (_Outcome.SUCCESS, metadata)
        return (outcome, None)

    Result = tuple[_Outcome, HeaderMetadata | None]
    with ThreadPoolExecutor(max_workers=max(1, max_workers)) as executor:
        in_flight: dict[Future[Result], tuple[SimulationKey, str]] = {}
        while True:
            assignment = scheduler.next_assignment()
            while assignment is not None:
                simulation, host = assignment
                future = executor.submit(read_on_host, simulation, host)
                in_flight[future] = (simulation, host)
                assignment = scheduler.next_assignment()

            if not in_flight:
                break

            done, _ = wait(in_flight, return_when=FIRST_COMPLETED)
            for future in done:
                simulation, host = in_flight.pop(future)
                outcome, metadata = future.result()
                if outcome is _Outcome.SUCCESS and metadata is not None:
                    scheduler.on_success(simulation, host)
                    headers[simulation] = metadata
                elif outcome is _Outcome.BLOCKED:
                    scheduler.on_block(simulation, host)
                else:
                    scheduler.on_failure(simulation, host)

    return DispatchResult(
        headers=headers, failed=scheduler.failed, learned=scheduler.learned()
    )
