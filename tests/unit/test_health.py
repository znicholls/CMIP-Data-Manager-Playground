"""Tests for per-data-node health tracking of header reads."""

from __future__ import annotations

from urllib.parse import urlparse

import pytest

from cmip_data_manager.esgf.headers import (
    HeaderReadBlocked,
    HeaderReadCrashed,
    HeaderReadTimeout,
)
from cmip_data_manager.esgf.health import (
    AttemptLog,
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


def test_record_counts_blocks_separately_from_errors():
    health = NodeHealth()
    health.record("https://busy/f.nc", ReadOutcome.BLOCKED, 0.5)
    health.record("https://busy/g.nc", ReadOutcome.ERROR, 1.0)
    stat = health.stat("busy")
    assert stat.blocks == 1
    assert stat.errors == 1
    assert stat.attempts == 2
    assert stat.successes == 0  # a block is not a success


def test_recording_classifies_and_reraises_block():
    health = NodeHealth()

    def reader(url: str) -> str:
        raise HeaderReadBlocked(url, "429 Too Many Requests")

    with pytest.raises(HeaderReadBlocked):
        recording(reader, health)("https://busy/f.nc")
    stat = health.stat("busy")
    assert stat is not None and stat.blocks == 1 and stat.errors == 0


def test_new_concurrency_fields_default_to_unlearned():
    stat = NodeStat(host="x")
    assert stat.blocks == 0
    assert stat.max_safe_concurrency == 0
    assert stat.last_concurrency == 0


def test_record_concurrency_accumulates_max_and_overwrites_last():
    health = NodeHealth()
    health.record_concurrency("nci", max_safe=3, last=4)
    health.record_concurrency("nci", max_safe=2, last=2)
    stat = health.stat("nci")
    assert stat.max_safe_concurrency == 3  # keeps the highest safe level seen
    assert stat.last_concurrency == 2  # overwritten with where it converged


def test_attempt_log_captures_url_outcome_and_duration_via_recording():
    health = NodeHealth()
    log = AttemptLog()

    def reader(url: str) -> str:
        if "dead" in url:
            raise OSError("refused")
        return "ok"

    read = recording(reader, health, attempts=log)
    assert read("https://good/f.nc") == "ok"
    with pytest.raises(OSError, match="refused"):
        read("https://dead/f.nc")

    records = log.records()
    assert [r.outcome for r in records] == [ReadOutcome.SUCCESS, ReadOutcome.ERROR]
    assert [urlparse(r.url).hostname for r in records] == ["good", "dead"]
    assert all(r.seconds >= 0.0 for r in records)
    # The aggregate health is still recorded alongside the per-attempt log.
    assert health.stat("good").successes == 1
    assert health.stat("dead").errors == 1


def test_recording_without_attempt_log_leaves_records_empty():
    health = NodeHealth()
    read = recording(lambda url: "ok", health)  # no attempts sink
    read("https://a/f.nc")
    assert health.stat("a").successes == 1  # health still recorded


def test_attempt_log_records_every_retry_of_the_same_url():
    log = AttemptLog()
    log.add("https://a/f.nc", ReadOutcome.TIMEOUT, 90.0)
    log.add("https://a/f.nc", ReadOutcome.SUCCESS, 2.0)
    records = log.records()
    assert len(records) == 2  # both the failed and the retried read are kept
    assert records[1].outcome is ReadOutcome.SUCCESS
