"""
Pre-flight liveness probe of the data nodes a run will read from

Reading a header means **connecting** to a data node, and the expensive failure
mode is a node that is *dead* — black-holed, TLS-broken, or DNS-gone — because the
scheduler only discovers that once it routes real work there and burns a full read
(or a stall) finding out.  This module front-loads that discovery: before any header
read, it asks each distinct data node one cheap question — *can you actually hand me a
chunk of one of your files right now?* — and turns the answers into a session verdict
(`alive` / `dead`) the rest of the pipeline can steer on.

The probe is deliberately a **stronger** test than the inline `with_connect_probe`
(`headers.py`): rather than a zero-byte `Range: bytes=0-0` that only proves the TCP+TLS
handshake completes, it pulls a real `chunk_bytes`-sized range and confirms **bytes
actually flow** — a node that accepts the connection then serves nothing is exactly the
black hole we want to catch.  One node is probed once per session (`ProbeCache`), on a
single representative file (`sample_urls_by_host` picks it from the routing candidates)
and — because a node is often indexed on both schemes — over that file's `https` twin
**and** its `http` original, so a node is only called dead when **both** fail.  The
sweep runs in parallel across a `MapFn`, so it costs about one connect-timeout of
wall-clock no matter how many nodes there are.

How it earns its keep on **cold** vs **warm** runs:

- **Cold run** — there is no persisted `NodeHealth`, so `ignore_hosts` starts empty and
  `host_rank` is neutral; the scheduler would otherwise *learn* which nodes are dead the
  slow, costly way (one wasted read/stall at a time, mid-walk).  The probe manufactures
  that knowledge up front in one cheap parallel sweep, so a cold run behaves like a warm
  one: dead nodes are in `ignore_hosts` **before** the first real read.
- **Warm run** — persisted health already suggests `unreliable_hosts()` and ranks nodes,
  but that is *history*.  The probe confirms **today's** reality and takes precedence
  for the session: a node condemned last week but alive now passes and is used; one
  healthy last week but black-holed now is caught before it wastes a read.  Persisted
  health still orders the survivors — the probe decides *alive vs dead*, health decides
  *which alive node first*.

The verdict is **session-scoped**: a node dead now may be fine next week, so a dead
probe is **not** written into persisted `NodeHealth` (matching the cold-run "re-probe
every node" policy).  The user can always override a verdict — `force_alive_hosts` keeps
a node in play regardless, and the printed report shows *why* a node was excluded so it
can be overridden on a re-run.
"""

from __future__ import annotations

import dataclasses
import random
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from urllib.parse import urlparse

import httpx

from cmip_data_manager.esgf.concurrency import MapFn, serial_map
from cmip_data_manager.esgf.headers import SimulationKey
from cmip_data_manager.esgf.routing import SimulationCandidates

DEFAULT_PROBE_CONNECT_TIMEOUT = 90.0
"""
Deadline in seconds for the probe to *reach* a node (TCP + TLS handshake).

Matches `headers.DEFAULT_CONNECT_TIMEOUT`: a distant-but-healthy node can be slow to
connect, so this window is generous — exceeding it is a black-holed connect (dead).
"""

DEFAULT_PROBE_READ_TIMEOUT = 30.0
"""
Deadline in seconds to receive the chunk once connected.

A single byte-range request is one server round-trip of data (not the ~30 round-trips
a full header read makes), so this is far tighter than a header read's timeout while
still clearing a genuinely slow node — a node that connects then stalls here is dead.
"""

DEFAULT_PROBE_CHUNK_BYTES = 65_536
"""Size of the byte-range the probe pulls; enough to prove the node serves file data."""

DEFAULT_PROBE_ATTEMPTS = 2
"""
Total attempts (one try plus one retry) before a *transient* failure is called dead.

A refusal/reset or a momentary `5xx`/`429` can clear on a retry, so a transient failure
gets a second chance; an unmistakable node-level fault (connect-timeout, read stall,
SSL/DNS) is called dead on the first attempt — retrying it just repeats the failure.
"""

DEFAULT_PROBE_RETRY_BASE = 1.0
"""Base backoff delay in seconds between probe retries (`base * 2**(n-1)`)."""

DEFAULT_PROBE_RETRY_CAP = 10.0
"""Maximum backoff delay in seconds between probe retries."""

DEFAULT_PROBE_RETRY_JITTER = 0.5
"""Fractional random jitter (`[0, jitter] * delay`) added to each retry delay."""

_PER_FILE_STATUSES = frozenset({404, 410})
"""HTTP statuses that mean *the node answered about a specific file*, not that it is
dead: the node is reachable (so **alive**), the sample file is just missing/gone."""

_BAD_STATUS = 400
"""At or above this an HTTP response is an error the probe treats as transient."""


@dataclass(frozen=True)
class ProbeOutcome:
    """
    The verdict of probing one data node

    `attempts == 0` marks a verdict that was **not** produced by a live probe — a host
    forced alive by the user (`force_alive_hosts`); a real probe always makes at least
    one attempt.
    """

    host: str
    """The data node (hostname) probed."""

    alive: bool
    """Whether the node served a chunk (or is otherwise reachable/forced-alive)."""

    reason: str | None
    """Why the node was judged dead; `None` when it is alive."""

    seconds: float
    """Wall-clock time of the deciding attempt (the successful chunk pull when alive —
    a signal of the node's latency, usable to size a later read timeout)."""

    url: str
    """The representative file URL the probe was run against."""

    attempts: int
    """How many probe attempts were made (`0` for a user-forced verdict)."""


@dataclass(frozen=True)
class _Attempt:
    """The result of a single probe attempt (before retry logic is applied)."""

    alive: bool
    retryable: bool
    """Whether a *dead* result could clear on a retry (a transient failure)."""
    reason: str | None
    seconds: float


class ProbeCache:
    """
    A thread-safe, session-scoped store of per-host probe verdicts

    Ensures each data node is probed **at most once per session** — the sweep skips any
    host already recorded — which is what makes an incremental, multi-hop walk cheap:
    each hop probes only the nodes it newly discovers.  Thread-safe because a sweep fans
    its probes out across a `MapFn`.

    Examples
    --------
    >>> cache = ProbeCache()
    >>> cache.record(
    ...     ProbeOutcome(
    ...         "good", alive=True, reason=None, seconds=0.3, url="u", attempts=1
    ...     )
    ... )
    >>> cache.record(
    ...     ProbeOutcome(
    ...         "dead", alive=False, reason="x", seconds=90.0, url="u", attempts=2
    ...     )
    ... )
    >>> cache.known("good"), cache.known("unseen")
    (True, False)
    >>> sorted(cache.dead_hosts())
    ['dead']
    >>> sorted(cache.alive_hosts())
    ['good']
    """

    def __init__(self) -> None:
        self._outcomes: dict[str, ProbeOutcome] = {}
        self._lock = threading.Lock()

    def known(self, host: str) -> bool:
        """Return whether `host` already has a recorded verdict this session."""
        with self._lock:
            return host in self._outcomes

    def get(self, host: str) -> ProbeOutcome | None:
        """Return the recorded verdict for `host`, or `None` if unprobed."""
        with self._lock:
            return self._outcomes.get(host)

    def record(self, outcome: ProbeOutcome) -> None:
        """Store a host's verdict (overwriting any earlier one)."""
        with self._lock:
            self._outcomes[outcome.host] = outcome

    def outcomes(self) -> dict[str, ProbeOutcome]:
        """Return a shallow copy of every recorded verdict, keyed by host."""
        with self._lock:
            return dict(self._outcomes)

    def dead_hosts(self) -> frozenset[str]:
        """Return the hosts judged dead this session (for use as `ignore_hosts`)."""
        with self._lock:
            return frozenset(h for h, o in self._outcomes.items() if not o.alive)

    def alive_hosts(self) -> frozenset[str]:
        """Return the hosts judged alive this session."""
        with self._lock:
            return frozenset(h for h, o in self._outcomes.items() if o.alive)


def sample_urls_by_host(
    candidates: Mapping[SimulationKey, SimulationCandidates],
) -> dict[str, tuple[str, ...]]:
    """
    Pick one representative file's URLs per data node from routing candidates

    A node is tested on a single file it actually hosts, so the routing view already
    has the perfect sample: `SimulationCandidates.urls_by_host` holds, per host, that
    host's best-first URLs for **one representative file** — its `https` twin and its
    `http` original.  This keeps both (so the probe can try both schemes before calling
    a node dead) and takes the first host seen across all simulations.

    Parameters
    ----------
    candidates
        Per-simulation ranked candidates (e.g. from `routing.build_candidates`).

    Returns
    -------
    :
        One `host -> (scheme URLs, best-first)` entry per distinct data node.

    Examples
    --------
    >>> from cmip_data_manager.esgf.routing import SimulationCandidates
    >>> a = SimulationCandidates(
    ...     ("A", "ssp245", "r1"),
    ...     "A",
    ...     ("nci",),
    ...     {"nci": ("https://nci/f.nc", "http://nci/f.nc")},
    ... )
    >>> sample_urls_by_host({a.simulation: a})
    {'nci': ('https://nci/f.nc', 'http://nci/f.nc')}
    """
    urls: dict[str, tuple[str, ...]] = {}
    for candidate in candidates.values():
        for host, host_urls in candidate.urls_by_host.items():
            if host not in urls and host_urls:
                urls[host] = tuple(host_urls)
    return urls


def _probe_chunk(  # noqa: PLR0911 - one branch per failure mode, each a distinct verdict
    url: str, *, connect_timeout: float, read_timeout: float, chunk_bytes: int
) -> _Attempt:
    """
    Run one probe attempt: try to pull a chunk of `url` and classify the result

    Splits the two phases with their own deadlines (a slow *connect* is not death; a
    stalled *read* is) and reads a real byte-range so the verdict rests on data actually
    flowing.  A per-file HTTP status (`404`/`410`) still counts the node **reachable**;
    only transport-level failures (or serving no data) make it dead, and the connect
    timeout, read stall, SSL and DNS faults are marked non-retryable (retrying repeats
    them) while a refusal/reset or an error status is retryable.
    """
    timeout = httpx.Timeout(
        connect=connect_timeout,
        read=read_timeout,
        write=read_timeout,
        pool=connect_timeout,
    )
    started = time.monotonic()
    try:
        with httpx.stream(
            "GET",
            url,
            headers={"Range": f"bytes=0-{max(0, chunk_bytes - 1)}"},
            timeout=timeout,
            follow_redirects=True,
        ) as response:
            status = response.status_code
            if status in _PER_FILE_STATUSES:
                return _Attempt(
                    alive=True,
                    retryable=False,
                    reason=None,
                    seconds=time.monotonic() - started,
                )
            if status >= _BAD_STATUS:
                return _Attempt(
                    alive=False,
                    retryable=True,
                    reason=f"HTTP {status}",
                    seconds=time.monotonic() - started,
                )
            for chunk in response.iter_bytes():
                if chunk:  # bytes flowed — the node serves file data
                    return _Attempt(
                        alive=True,
                        retryable=False,
                        reason=None,
                        seconds=time.monotonic() - started,
                    )
            return _Attempt(
                alive=False,
                retryable=True,
                reason="connected but served no data",
                seconds=time.monotonic() - started,
            )
    except httpx.ConnectTimeout:
        return _Attempt(
            alive=False,
            retryable=False,
            reason=f"connect timed out after {connect_timeout:g}s",
            seconds=time.monotonic() - started,
        )
    except httpx.ReadTimeout:
        return _Attempt(
            alive=False,
            retryable=False,
            reason=f"read stalled after {read_timeout:g}s",
            seconds=time.monotonic() - started,
        )
    except httpx.ConnectError as exc:
        # A plain refusal can be a momentary overload (retryable); DNS/TLS failures
        # surface here too and are a genuine node-level fault (not retryable).
        if "refused" in str(exc).lower():
            return _Attempt(
                alive=False,
                retryable=True,
                reason=f"connection refused: {exc}",
                seconds=time.monotonic() - started,
            )
        return _Attempt(
            alive=False,
            retryable=False,
            reason=f"connect error: {exc}",
            seconds=time.monotonic() - started,
        )
    except httpx.HTTPError as exc:
        return _Attempt(
            alive=False,
            retryable=True,
            reason=f"transport error: {exc}",
            seconds=time.monotonic() - started,
        )


def probe_node(  # noqa: PLR0913 - a probe with retry knobs, every one keyword-defaulted
    url: str,
    *,
    connect_timeout: float = DEFAULT_PROBE_CONNECT_TIMEOUT,
    read_timeout: float = DEFAULT_PROBE_READ_TIMEOUT,
    chunk_bytes: int = DEFAULT_PROBE_CHUNK_BYTES,
    max_attempts: int = DEFAULT_PROBE_ATTEMPTS,
    base: float = DEFAULT_PROBE_RETRY_BASE,
    cap: float = DEFAULT_PROBE_RETRY_CAP,
    jitter: float = DEFAULT_PROBE_RETRY_JITTER,
    sleep: Callable[[float], None] = time.sleep,
) -> ProbeOutcome:
    """
    Probe one data node for liveness, retrying only a transient failure

    Pulls a chunk of `url` (a file the node hosts) and returns a `ProbeOutcome`.  A
    node-level fault (connect-timeout, read stall, SSL/DNS) is called dead immediately;
    a transient failure (refusal/reset, a `5xx`/`429`, no data) is retried up to
    `max_attempts` with exponential backoff before being called dead.

    Parameters
    ----------
    url
        A directly-downloadable `HTTPServer` URL of a file the node hosts.

    connect_timeout, read_timeout, chunk_bytes
        Probe deadlines and chunk size (see the module constants).

    max_attempts
        Total attempts before a *transient* failure is called dead (`1` disables retry).

    base, cap, jitter
        Backoff shape: attempt `n` sleeps `min(cap, base * 2**(n-1))` plus up to
        `jitter` of that as random padding.

    sleep
        Sleep function, injectable so tests need not actually wait.

    Returns
    -------
    :
        The node's verdict, with `reason=None` when alive.
    """
    host = urlparse(url).hostname or url
    attempt = 1
    while True:
        result = _probe_chunk(
            url,
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            chunk_bytes=chunk_bytes,
        )
        if result.alive:
            return ProbeOutcome(
                host=host,
                alive=True,
                reason=None,
                seconds=result.seconds,
                url=url,
                attempts=attempt,
            )
        if not result.retryable or attempt >= max_attempts:
            return ProbeOutcome(
                host=host,
                alive=False,
                reason=result.reason,
                seconds=result.seconds,
                url=url,
                attempts=attempt,
            )
        delay = min(cap, base * (2 ** (attempt - 1)))
        delay += random.uniform(0, jitter) * delay  # noqa: S311 - not cryptographic
        sleep(delay)
        attempt += 1


def probe_host(  # noqa: PLR0913 - probe knobs forwarded to probe_node, all defaulted
    urls: Sequence[str],
    *,
    connect_timeout: float = DEFAULT_PROBE_CONNECT_TIMEOUT,
    read_timeout: float = DEFAULT_PROBE_READ_TIMEOUT,
    chunk_bytes: int = DEFAULT_PROBE_CHUNK_BYTES,
    max_attempts: int = DEFAULT_PROBE_ATTEMPTS,
    sleep: Callable[[float], None] = time.sleep,
) -> ProbeOutcome:
    """
    Probe one data node across a representative file's scheme URLs, alive if any works

    A node is usually indexed on both schemes (an `https` twin and its `http` original),
    and one can succeed where the other fails — an `http` mirror the byte-range driver
    would reject may still serve the probe, and an `https` twin can clear a cert issue
    on the `http` original.  So this tries `urls` in order (best-first, e.g. `https`
    then `http`) and returns **alive on the first that serves a chunk**; only when every
    scheme URL fails is the node called dead, with the failures joined into one reason.
    Each URL still gets its own transient retries (see `probe_node`), so a node is only
    condemned after both schemes and their retries are exhausted.

    Parameters
    ----------
    urls
        The representative file's URLs for one host, best-first (its `https` twin and
        `http` original).

    connect_timeout, read_timeout, chunk_bytes, max_attempts, sleep
        Forwarded to `probe_node` for each scheme URL.

    Returns
    -------
    :
        The node's verdict: the first alive URL's outcome, or a dead outcome summing
        the attempts/time across schemes and joining their reasons.
    """
    if not urls:
        return ProbeOutcome(
            host="",
            alive=False,
            reason="no candidate URL",
            seconds=0.0,
            url="",
            attempts=0,
        )
    failures: list[ProbeOutcome] = []
    for url in urls:
        outcome = probe_node(
            url,
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            chunk_bytes=chunk_bytes,
            max_attempts=max_attempts,
            sleep=sleep,
        )
        if outcome.alive:
            return outcome
        failures.append(outcome)
    reason = "; ".join(f"{urlparse(o.url).scheme or '?'}: {o.reason}" for o in failures)
    return ProbeOutcome(
        host=failures[0].host,
        alive=False,
        reason=reason,
        seconds=sum(o.seconds for o in failures),
        url=failures[-1].url,
        attempts=sum(o.attempts for o in failures),
    )


def probe_nodes(  # noqa: PLR0913 - a sweep with probe knobs, every one keyword-defaulted
    urls_by_host: Mapping[str, Sequence[str]],
    *,
    probe: Callable[[Sequence[str]], ProbeOutcome] | None = None,
    force_alive_hosts: frozenset[str] = frozenset(),
    cache: ProbeCache | None = None,
    map_fn: MapFn = serial_map,
    connect_timeout: float = DEFAULT_PROBE_CONNECT_TIMEOUT,
    read_timeout: float = DEFAULT_PROBE_READ_TIMEOUT,
    chunk_bytes: int = DEFAULT_PROBE_CHUNK_BYTES,
    max_attempts: int = DEFAULT_PROBE_ATTEMPTS,
) -> dict[str, ProbeOutcome]:
    """
    Probe every data node in `urls_by_host`, once per session, in parallel

    Skips any host the `cache` already has a verdict for (so a multi-hop walk probes
    only newly-discovered nodes) and any host in `force_alive_hosts` (recorded alive
    without a probe — the user override).  The rest are probed in parallel via `map_fn`.

    Parameters
    ----------
    urls_by_host
        One representative file's `host -> (scheme URLs)` per data node (e.g. from
        `sample_urls_by_host`) — each host's `https` twin and `http` original.

    probe
        Per-host probe over a file's scheme URLs; defaults to `probe_host` with the
        timeouts below.  Injectable so callers (and tests) can supply their own.

    force_alive_hosts
        Hosts to keep in play regardless — recorded alive (`attempts=0`), not probed.

    cache
        Session verdict store; skipped hosts and fresh results are read/written here.
        A fresh `ProbeCache` is used when omitted (verdicts then live only in the
        return value).

    map_fn
        Concurrency strategy for the sweep (e.g. `thread_pool_map(...)`); defaults to
        `serial_map`.

    connect_timeout, read_timeout, chunk_bytes, max_attempts
        Forwarded to the default `probe` (ignored if `probe` is given).

    Returns
    -------
    :
        The verdict for every host in `urls_by_host` (freshly probed, forced, or reused
        from the cache), keyed by host.
    """
    resolved_cache = cache if cache is not None else ProbeCache()

    default_probe = probe
    if default_probe is None:

        def default_probe(urls: Sequence[str]) -> ProbeOutcome:
            return probe_host(
                urls,
                connect_timeout=connect_timeout,
                read_timeout=read_timeout,
                chunk_bytes=chunk_bytes,
                max_attempts=max_attempts,
            )

    to_probe: list[str] = []
    for host, urls in urls_by_host.items():
        if resolved_cache.known(host):
            continue
        if host in force_alive_hosts:
            resolved_cache.record(
                ProbeOutcome(
                    host=host,
                    alive=True,
                    reason=None,
                    seconds=0.0,
                    url=urls[0] if urls else "",
                    attempts=0,
                )
            )
            continue
        to_probe.append(host)

    def run(host: str) -> tuple[str, ProbeOutcome]:
        return host, default_probe(urls_by_host[host])

    for host, outcome in map_fn(run, to_probe):
        # Key the verdict by the host we asked about, not the probe's own parse, so a
        # verdict cannot be filed under the wrong host.
        verdict = (
            outcome if outcome.host == host else dataclasses.replace(outcome, host=host)
        )
        resolved_cache.record(verdict)

    result: dict[str, ProbeOutcome] = {}
    for host in urls_by_host:
        outcome = resolved_cache.get(host)
        if outcome is not None:
            result[host] = outcome
    return result
