"""Tests for node-centric dispatch of header reads."""

from __future__ import annotations

from urllib.parse import urlparse

import pytest

from cmip_data_manager.esgf.dispatch import (
    DispatchResult,
    _Scheduler,
    concurrency_limit,
    dispatch_reads,
)
from cmip_data_manager.esgf.headers import (
    HeaderMetadata,
    HeaderReadBlocked,
    HeaderReadHostFault,
    HeaderReadTimeout,
)
from cmip_data_manager.esgf.routing import SimulationCandidates


def _cand(sim, group, host_urls):
    """Build SimulationCandidates from an ordered {host: [urls]} mapping."""
    return SimulationCandidates(
        simulation=sim,
        group=group,
        hosts=tuple(host_urls),
        urls_by_host={host: tuple(urls) for host, urls in host_urls.items()},
    )


def _hosts(*hosts):
    """One best URL per host, in the given order."""
    return {host: [f"https://{host}/f.nc"] for host in hosts}


def _reader(fail_hosts=frozenset()):
    def reader(url: str) -> HeaderMetadata:
        host = urlparse(url).hostname or ""
        if host in fail_hosts:
            raise OSError(f"{host} down")
        return HeaderMetadata(attrs={"served_by": host}, source_url=url)

    return reader


# --- scheduler (pure, synchronous) ------------------------------------------


def _sched(candidates, initial, workers, **kw):
    """Build a scheduler with a flat/callable initial cap and defaults."""
    if callable(initial):
        seed = initial
    else:

        def seed(host):
            return initial

    return _Scheduler(candidates, initial_concurrency=seed, max_workers=workers, **kw)


def test_scheduler_assigns_best_host_first():
    s1 = ("A", "ssp245", "r1")
    sched = _sched({s1: _cand(s1, "A", _hosts("nci", "ornl"))}, 2, 12)
    assert sched.next_assignment() == (s1, "nci")


def test_scheduler_spills_when_best_host_is_capped():
    s1, s2 = ("A", "ssp245", "r1"), ("B", "ssp245", "r1")
    cands = {
        s1: _cand(s1, "A", _hosts("nci", "ornl")),
        s2: _cand(s2, "B", _hosts("nci", "ornl")),
    }
    # nci tolerates only one connection; the second simulation must spill.
    sched = _sched(cands, lambda h: 1 if h == "nci" else 2, 12)
    assert sched.next_assignment() == (s1, "nci")
    assert sched.next_assignment() == (s2, "ornl")


def test_scheduler_requeues_to_next_host_on_failure():
    s1 = ("A", "ssp245", "r1")
    sched = _sched({s1: _cand(s1, "A", _hosts("nci", "ornl"))}, 2, 12)
    assert sched.next_assignment() == (s1, "nci")
    sched.on_failure(s1, "nci")
    assert sched.next_assignment() == (s1, "ornl")  # requeued to next candidate


def test_scheduler_fails_when_every_host_exhausted():
    s1 = ("A", "ssp245", "r1")
    sched = _sched({s1: _cand(s1, "A", _hosts("nci"))}, 2, 12)
    sched.next_assignment()
    sched.on_failure(s1, "nci")
    assert sched.next_assignment() is None
    assert sched.failed == [s1]


def test_scheduler_marks_unservable_simulation_failed_up_front():
    s1 = ("A", "ssp245", "r1")
    sched = _sched({s1: _cand(s1, "A", {})}, 2, 12)
    assert sched.failed == [s1]
    assert sched.next_assignment() is None


def test_scheduler_respects_the_global_worker_budget():
    s1, s2 = ("A", "ssp245", "r1"), ("B", "ssp245", "r1")
    cands = {s1: _cand(s1, "A", _hosts("nci")), s2: _cand(s2, "B", _hosts("ornl"))}
    sched = _sched(cands, 2, 1)  # only one read allowed anywhere
    assert sched.next_assignment() == (s1, "nci")
    assert sched.next_assignment() is None  # global budget full despite a free host
    sched.on_success(s1, "nci")
    assert sched.next_assignment() == (s2, "ornl")


def test_scheduler_clusters_by_affinity_group():
    a1, a2 = ("A", "ssp245", "r1"), ("A", "hist", "r1")
    b1 = ("B", "ssp245", "r1")
    cands = {
        b1: _cand(b1, "B", _hosts("nci")),
        a1: _cand(a1, "A", _hosts("nci")),
        a2: _cand(a2, "A", _hosts("nci")),
    }
    sched = _sched(cands, 99, 99)
    # nci can take all three; the two "A" simulations are offered before "B".
    order = [sched.next_assignment()[0] for _ in range(3)]
    assert order[:2] == [a2, a1]  # both "A" (sorted within group), then "B"
    assert order[2] == b1


# --- scheduler: adaptive concurrency (AIMD) + circuit breaker ----------------


def test_scheduler_grows_cap_after_a_success_streak():
    sims = [(letter, "ssp245", "r1") for letter in "ABCD"]
    cands = {s: _cand(s, s[0], _hosts("nci")) for s in sims}
    sched = _sched(cands, 2, 12, increase_after=2, ceiling=5)
    assert sched.current_limit("nci") == 2
    # Two clean successes at the cap grow it by one.
    a, b = sched.next_assignment(), sched.next_assignment()
    sched.on_success(*a)
    sched.on_success(*b)
    assert sched.current_limit("nci") == 3


def test_scheduler_does_not_grow_past_the_ceiling():
    sims = [(str(i), "ssp245", "r1") for i in range(10)]
    cands = {s: _cand(s, s[0], _hosts("nci")) for s in sims}
    sched = _sched(cands, 2, 12, increase_after=1, ceiling=3)
    for _ in range(6):  # every success would grow it, but the ceiling caps at 3
        a = sched.next_assignment()
        if a is not None:
            sched.on_success(*a)
    assert sched.current_limit("nci") == 3


def test_scheduler_halves_cap_on_a_block():
    sims = [(letter, "ssp245", "r1") for letter in "AB"]
    cands = {s: _cand(s, s[0], _hosts("nci", "good")) for s in sims}
    sched = _sched(cands, 4, 12)
    a = sched.next_assignment()  # (A, nci)
    sched.on_block(*a)
    assert sched.current_limit("nci") == 2  # 4 -> 2
    # The blocked simulation is requeued to its alternative host.
    assert sched.next_assignment() == (("A", "ssp245", "r1"), "good")


def test_scheduler_pinned_host_does_not_adapt():
    sims = [(letter, "ssp245", "r1") for letter in "AB"]
    cands = {s: _cand(s, s[0], _hosts("nci", "good")) for s in sims}
    sched = _sched(cands, 4, 12, increase_after=1, pinned_hosts=frozenset({"nci"}))
    a = sched.next_assignment()
    sched.on_block(*a)
    assert sched.current_limit("nci") == 4  # pinned: block does not shrink it


def test_scheduler_evicts_a_host_with_a_low_success_rate():
    sims = [(letter, "ssp245", "r1") for letter in "ABC"]
    cands = {s: _cand(s, s[0], _hosts("bad", "good")) for s in sims}
    # Judge after 3 attempts; evict if it has never succeeded.
    sched = _sched(cands, 5, 12, evict_after_attempts=3, evict_max_success_rate=0.0)
    assignments = [sched.next_assignment() for _ in range(3)]
    for sim, host in assignments:
        assert host == "bad"
        sched.on_failure(sim, host)
    assert sched.current_limit("bad") == 0  # evicted (0/3 successes)
    for _ in range(3):
        assert sched.next_assignment()[1] == "good"


def test_scheduler_does_not_evict_a_busy_healthy_node():
    # A workhorse: many successes, then a short run of failures. Must survive.
    sims = [(str(i), "ssp245", "r1") for i in range(12)]
    cands = {s: _cand(s, s[0], _hosts("workhorse", "good")) for s in sims}
    sched = _sched(cands, 8, 12, evict_after_attempts=3, evict_max_success_rate=0.2)
    # 8 clean successes...
    for _ in range(8):
        sched.on_success(*sched.next_assignment())
    # ...then a run of 3 failures on it (e.g. one model's files missing there).
    # Assign all three to the workhorse first, then fail them.
    failing = [sched.next_assignment() for _ in range(3)]
    assert all(host == "workhorse" for _, host in failing)
    for sim, host in failing:
        sched.on_failure(sim, host)
    # 8/11 succeeded -> rate 0.73, well above 0.2: not evicted.
    assert sched.current_limit("workhorse") > 0


def test_scheduler_eviction_fails_simulations_with_no_alternative():
    s1, s2 = ("A", "ssp245", "r1"), ("B", "ssp245", "r1")
    # s1 can fall back to "good"; s2 is stranded on "bad" alone.
    cands = {
        s1: _cand(s1, "A", _hosts("bad", "good")),
        s2: _cand(s2, "B", _hosts("bad")),
    }
    sched = _sched(cands, 5, 12, evict_after_attempts=1, evict_max_success_rate=0.0)
    sim, host = sched.next_assignment()  # (A, bad)
    sched.on_failure(sim, host)  # 0/1 success -> evict "bad"
    assert s2 in sched.failed  # stranded simulation fails with the node
    assert sched.next_assignment() == (s1, "good")  # s1 survives on its alternative


def test_scheduler_learned_reports_max_safe_and_last_cap():
    sims = [(letter, "ssp245", "r1") for letter in "AB"]
    cands = {s: _cand(s, s[0], _hosts("nci")) for s in sims}
    sched = _sched(cands, 2, 12, increase_after=2)
    a, b = sched.next_assignment(), sched.next_assignment()  # both in flight on nci
    sched.on_success(*a)  # in-flight was 2 at this point
    sched.on_success(*b)  # grows cap to 3
    max_safe, last = sched.learned()["nci"]
    assert max_safe == 2  # two concurrent reads ran cleanly
    assert last == 3  # cap converged at 3


# --- dispatch_reads (threaded driver) ---------------------------------------


def test_dispatch_reads_returns_header_from_best_host():
    s1 = ("A", "ssp245", "r1")
    result = dispatch_reads({s1: _cand(s1, "A", _hosts("nci", "ornl"))}, _reader())
    assert isinstance(result, DispatchResult)
    assert result.headers[s1].get("served_by") == "nci"
    assert result.failed == []


def test_dispatch_reads_requeues_past_a_dead_host():
    s1 = ("A", "ssp245", "r1")
    result = dispatch_reads(
        {s1: _cand(s1, "A", _hosts("dead", "good"))},
        _reader(fail_hosts=frozenset({"dead"})),
    )
    assert result.headers[s1].get("served_by") == "good"
    assert result.failed == []


def test_dispatch_reads_records_a_fully_failed_simulation():
    s1 = ("A", "ssp245", "r1")
    result = dispatch_reads(
        {s1: _cand(s1, "A", _hosts("dead"))},
        _reader(fail_hosts=frozenset({"dead"})),
    )
    assert result.headers == {}
    assert result.failed == [s1]


def test_dispatch_reads_spills_second_simulation_to_alternative_node():
    s1, s2 = ("A", "ssp245", "r1"), ("B", "ssp245", "r1")
    cands = {
        s1: _cand(s1, "A", _hosts("nci", "ornl")),
        s2: _cand(s2, "B", _hosts("nci", "ornl")),
    }
    # nci is capped at one connection: s1 gets nci, s2 must spill to ornl.
    result = dispatch_reads(
        cands,
        _reader(),
        initial_concurrency=concurrency_limit(2, {"nci": 1}),
        pinned_hosts=frozenset({"nci"}),  # keep the cap at 1 for a deterministic spill
    )
    assert result.headers[s1].get("served_by") == "nci"
    assert result.headers[s2].get("served_by") == "ornl"


def test_dispatch_reads_fails_an_unservable_simulation():
    s1 = ("A", "ssp245", "r1")
    result = dispatch_reads({s1: _cand(s1, "A", {})}, _reader())
    assert result.headers == {}
    assert result.failed == [s1]


def _kill_reader(kill_host):
    """A reader that raises `KeyboardInterrupt` (a kill) when it hits `kill_host`."""

    def reader(url: str) -> HeaderMetadata:
        host = urlparse(url).hostname or ""
        if host == kill_host:
            raise KeyboardInterrupt("simulated kill")
        return HeaderMetadata(attrs={"served_by": host}, source_url=url)

    return reader


def test_dispatch_reads_invokes_callbacks_per_read():
    s1, s2 = ("A", "ssp245", "r1"), ("B", "ssp245", "r1")
    cands = {s1: _cand(s1, "A", _hosts("nci")), s2: _cand(s2, "B", _hosts("nci"))}
    seen: list[tuple[tuple[str, str, str], str | None]] = []
    flushes: list[int] = []

    dispatch_reads(
        cands,
        _reader(),
        on_success=lambda sim, md: seen.append((sim, md.get("served_by"))),
        on_flush=lambda: flushes.append(1),
    )

    assert sorted(sim for sim, _ in seen) == [s1, s2]  # one callback per success
    assert all(host == "nci" for _, host in seen)
    assert flushes  # the per-iteration flush ran at least once


def test_dispatch_reads_reports_successes_before_a_kill():
    # Three sims, processed in order under a single worker; the third host kills the
    # run. The first two headers must already have been handed to on_success.
    sims = [("A", "ssp245", "r1"), ("B", "ssp245", "r1"), ("C", "ssp245", "r1")]
    cands = {
        sims[0]: _cand(sims[0], "A", _hosts("nci")),
        sims[1]: _cand(sims[1], "B", _hosts("ornl")),
        sims[2]: _cand(sims[2], "C", _hosts("boom")),
    }
    persisted: list[tuple[str, str, str]] = []

    with pytest.raises(KeyboardInterrupt):
        dispatch_reads(
            cands,
            _kill_reader("boom"),
            max_workers=1,  # deterministic order: A, then B, then the killer C
            on_success=lambda sim, _md: persisted.append(sim),
        )

    assert sims[0] in persisted and sims[1] in persisted
    assert sims[2] not in persisted  # the kill fired before its header arrived


def test_dispatch_stops_a_hosts_urls_after_a_stall():
    # A host that STALLS (a timeout) is hanging on every read, so trying its other
    # URLs just burns another full 25s deadline each — the tail that held uc2 open for
    # ~15 min (4 variables x 2 schemes = up to 8 URLs per host, all stalling). One
    # stall must short-circuit the host, not walk all of its URLs.
    s1 = ("A", "ssp245", "r1")
    cand = SimulationCandidates(
        simulation=s1,
        group="A",
        hosts=("nci",),
        urls_by_host={"nci": tuple(f"https://nci/{i}.nc" for i in range(4))},
    )
    calls: list[str] = []

    def reader(url: str) -> HeaderMetadata:
        calls.append(url)
        raise HeaderReadTimeout(url, 25.0)

    result = dispatch_reads({s1: cand}, reader)

    assert s1 in result.failed
    assert len(calls) == 1  # short-circuited on the first stall, not 4 x 25s


def test_dispatch_stops_a_hosts_urls_on_a_host_fault():
    # A host fault (SSL/cert, DNS, connect-timeout) means the node is unreachable, so —
    # like a stall — trying its other URLs is wasted; short-circuit to the next mirror.
    s1 = ("A", "ssp245", "r1")
    cand = SimulationCandidates(
        simulation=s1,
        group="A",
        hosts=("bad-cert",),
        urls_by_host={"bad-cert": tuple(f"https://bad-cert/{i}.nc" for i in range(4))},
    )
    calls: list[str] = []

    def reader(url: str) -> HeaderMetadata:
        calls.append(url)
        raise HeaderReadHostFault(url, "SSL certificate problem")

    result = dispatch_reads({s1: cand}, reader)

    assert s1 in result.failed
    assert len(calls) == 1  # host-fault short-circuits the node, not all 4 URLs


def test_dispatch_still_tries_a_hosts_other_urls_after_a_plain_failure():
    # A plain failure (e.g. connection refused on the http URL) must still fall through
    # to the host's next URL (its https twin) — only a stall short-circuits the host.
    s1 = ("A", "ssp245", "r1")
    cand = SimulationCandidates(
        simulation=s1,
        group="A",
        hosts=("nci",),
        urls_by_host={"nci": ("https://nci/bad.nc", "https://nci/good.nc")},
    )

    def reader(url: str) -> HeaderMetadata:
        if url.endswith("bad.nc"):
            raise OSError("connection refused")
        return HeaderMetadata(attrs={"ok": "1"}, source_url=url)

    result = dispatch_reads({s1: cand}, reader)

    assert result.headers[s1].get("ok") == "1"  # read the 2nd URL after the 1st refused


def test_dispatch_reads_backs_off_and_requeues_on_block():
    s1 = ("A", "ssp245", "r1")

    def reader(url: str) -> HeaderMetadata:
        host = urlparse(url).hostname or ""
        if host == "busy":
            raise HeaderReadBlocked(url, "429 Too Many Requests")
        return HeaderMetadata(attrs={"served_by": host}, source_url=url)

    result = dispatch_reads(
        {s1: _cand(s1, "A", _hosts("busy", "good"))},
        reader,
        initial_concurrency=concurrency_limit(4),
    )
    assert result.headers[s1].get("served_by") == "good"  # requeued past the block
    assert result.learned["busy"][1] == 2  # cap halved from 4 by the block


def test_dispatch_reads_reports_learned_caps_for_touched_hosts():
    s1 = ("A", "ssp245", "r1")
    result = dispatch_reads(
        {s1: _cand(s1, "A", _hosts("nci"))}, _reader(), initial_concurrency=lambda h: 2
    )
    assert result.learned["nci"] == (1, 2)  # one read ran; cap unchanged at 2
