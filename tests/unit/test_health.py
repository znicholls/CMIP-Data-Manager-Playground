"""Tests for per-data-node health tracking of header reads."""

from __future__ import annotations

import pytest

from cmip_data_manager.esgf.headers import HeaderReadCrashed, HeaderReadTimeout
from cmip_data_manager.esgf.health import NodeHealth, ReadOutcome, recording


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
