"""Tests for per-data-node health tracking of header reads."""

from __future__ import annotations

import pytest

from cmip_data_manager.esgf.headers import HeaderReadCrashed, HeaderReadTimeout
from cmip_data_manager.esgf.health import (
    NodeHealth,
    NodeStat,
    ReadOutcome,
    recording,
)


def test_record_accumulates_success_rate_and_timing():
    health = NodeHealth()
    health.record("https://a/f.nc", ReadOutcome.SUCCESS, 2.0)
    health.record("https://a/g.nc", ReadOutcome.SUCCESS, 4.0)
    health.record("https://a/h.nc", ReadOutcome.ERROR, 1.0)

    stat = health.stat("a")
    assert stat is not None
    assert stat.attempts == 3
    assert stat.successes == 2
    assert stat.success_rate == pytest.approx(2 / 3)
    assert stat.mean_success_seconds == pytest.approx(3.0)


def test_mean_read_seconds_across_hosts():
    health = NodeHealth()
    health.record("https://a/f.nc", ReadOutcome.SUCCESS, 1.0)
    health.record("https://b/f.nc", ReadOutcome.SUCCESS, 3.0)
    health.record("https://b/g.nc", ReadOutcome.TIMEOUT, 90.0)
    # Timeouts do not enter the mean; only the two successes do.
    assert health.mean_read_seconds() == pytest.approx(2.0)


def test_mean_read_seconds_none_before_any_success():
    health = NodeHealth()
    health.record("https://a/f.nc", ReadOutcome.TIMEOUT, 90.0)
    assert health.mean_read_seconds() is None


def test_unreliable_hosts_needs_enough_attempts():
    health = NodeHealth()
    for _ in range(3):
        health.record("https://dead/f.nc", ReadOutcome.TIMEOUT, 90.0)
    health.record("https://blip/f.nc", ReadOutcome.ERROR, 1.0)  # only one attempt

    unreliable = health.unreliable_hosts(min_attempts=3)
    assert unreliable == frozenset({"dead"})


def test_unreliable_hosts_excludes_a_node_that_sometimes_works():
    health = NodeHealth()
    health.record("https://flaky/f.nc", ReadOutcome.ERROR, 1.0)
    health.record("https://flaky/g.nc", ReadOutcome.ERROR, 1.0)
    health.record("https://flaky/h.nc", ReadOutcome.SUCCESS, 5.0)
    assert health.unreliable_hosts(min_attempts=3) == frozenset()


def test_recording_records_success_and_returns_value():
    health = NodeHealth()
    read = recording(lambda url: url.upper(), health)
    assert read("https://a/f.nc") == "HTTPS://A/F.NC"
    stat = health.stat("a")
    assert stat is not None and stat.successes == 1


def test_recording_classifies_and_reraises_timeout():
    health = NodeHealth()

    def reader(url: str) -> str:
        raise HeaderReadTimeout(url, 90.0)

    read = recording(reader, health)
    with pytest.raises(HeaderReadTimeout):
        read("https://stalled/f.nc")
    stat = health.stat("stalled")
    assert stat is not None and stat.timeouts == 1 and stat.successes == 0


def test_recording_classifies_crash_and_generic_oserror():
    health = NodeHealth()

    def crasher(url: str) -> str:
        raise HeaderReadCrashed(url, 1)

    def erroring(url: str) -> str:
        raise OSError("refused")

    with pytest.raises(HeaderReadCrashed):
        recording(crasher, health)("https://corrupt/f.nc")
    with pytest.raises(OSError, match="refused"):
        recording(erroring, health)("https://dead/f.nc")

    assert health.stat("corrupt").crashes == 1
    assert health.stat("dead").errors == 1


def test_failure_rate_and_max_success_seconds():
    health = NodeHealth()
    health.record("https://a/f.nc", ReadOutcome.SUCCESS, 2.0)
    health.record("https://a/g.nc", ReadOutcome.SUCCESS, 5.0)
    health.record("https://a/h.nc", ReadOutcome.TIMEOUT, 90.0)
    stat = health.stat("a")
    assert stat.failure_rate == pytest.approx(1 / 3)
    assert stat.max_success_seconds == 5.0


def test_failure_rate_zero_when_never_attempted():
    assert NodeStat(host="x").failure_rate == 0.0


def test_rank_by_reliability_orders_best_first():
    health = NodeHealth()
    health.record("https://good/f.nc", ReadOutcome.SUCCESS, 1.0)
    health.record("https://bad/f.nc", ReadOutcome.SUCCESS, 1.0)
    health.record("https://bad/g.nc", ReadOutcome.ERROR, 1.0)
    ranked = [s.host for s in health.rank_by_reliability()]
    assert ranked == ["good", "bad"]


def test_rank_by_reliability_skips_thinly_tried_hosts():
    health = NodeHealth()
    health.record("https://a/f.nc", ReadOutcome.SUCCESS, 1.0)
    health.record("https://a/g.nc", ReadOutcome.SUCCESS, 1.0)
    health.record("https://b/f.nc", ReadOutcome.SUCCESS, 1.0)  # only one attempt
    ranked = [s.host for s in health.rank_by_reliability(min_attempts=2)]
    assert ranked == ["a"]


def test_rank_by_speed_only_hosts_with_success_fastest_first():
    health = NodeHealth()
    health.record("https://slow/f.nc", ReadOutcome.SUCCESS, 9.0)
    health.record("https://fast/f.nc", ReadOutcome.SUCCESS, 1.0)
    health.record("https://never/f.nc", ReadOutcome.TIMEOUT, 90.0)
    ranked = [s.host for s in health.rank_by_speed()]
    assert ranked == ["fast", "slow"]  # "never" excluded — no success


def test_suggested_timeout_from_slowest_healthy_read():
    health = NodeHealth()
    health.record("https://a/f.nc", ReadOutcome.SUCCESS, 45.0)
    health.record("https://b/f.nc", ReadOutcome.SUCCESS, 2.0)
    # slowest healthy read 45s, padded by safety 1.5 -> ~67.5s
    assert health.suggested_timeout(safety=1.5) == pytest.approx(67.5)


def test_suggested_timeout_floor_and_default():
    health = NodeHealth()
    assert health.suggested_timeout(default=90.0) == 90.0  # no evidence yet
    health.record("https://a/f.nc", ReadOutcome.SUCCESS, 1.0)
    # 1s * 1.5 = 1.5s, floored to 10s
    assert health.suggested_timeout(safety=1.5, floor=10.0) == 10.0


def test_restore_replaces_host_stats():
    health = NodeHealth()
    health.restore(NodeStat(host="a", attempts=5, successes=4))
    assert health.stat("a").successes == 4
