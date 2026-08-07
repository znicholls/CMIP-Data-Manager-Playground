"""Tests for per-data-node download (throughput) health tracking."""

from __future__ import annotations

from cmip_data_manager.esgf.download_health import (
    DownloadNodeHealth,
    DownloadOutcome,
    DownloadStat,
)


def test_record_counts_each_outcome_kind():
    health = DownloadNodeHealth()
    health.record("https://n/a.nc", DownloadOutcome.SUCCESS, 2.0, num_bytes=20_000_000)
    health.record("https://n/b.nc", DownloadOutcome.TIMEOUT, 90.0)
    health.record("https://n/c.nc", DownloadOutcome.BLOCKED, 0.5)
    health.record("https://n/d.nc", DownloadOutcome.HOST_FAULT, 5.0)
    health.record("https://n/e.nc", DownloadOutcome.CHECKSUM_FAILED, 1.0)
    health.record("https://n/f.nc", DownloadOutcome.ERROR, 1.0)
    stat = health.stat("n")
    assert stat.attempts == 6
    assert stat.successes == 1
    assert stat.timeouts == 1
    assert stat.blocks == 1
    assert stat.host_faults == 1
    assert stat.checksum_failures == 1
    assert stat.errors == 1
    # Only successful transfers count toward throughput.
    assert stat.total_bytes == 20_000_000
    assert stat.total_success_seconds == 2.0


def test_stat_rates_and_mean_mbps():
    stat = DownloadStat("n")
    assert stat.success_rate == 0.0
    assert stat.failure_rate == 0.0  # never tried
    assert stat.mean_mbps is None
    stat.attempts = 4
    stat.successes = 3
    stat.total_bytes = 300_000_000
    stat.total_success_seconds = 3.0
    assert stat.success_rate == 0.75
    assert stat.failure_rate == 0.25
    assert stat.mean_mbps == 100.0  # (300e6 / 1e6) / 3s


def test_mean_mbps_none_without_success_or_time():
    assert DownloadStat("n", successes=0, total_success_seconds=0.0).mean_mbps is None


def test_stat_returns_none_for_unseen_host():
    assert DownloadNodeHealth().stat("nope") is None


def test_snapshot_is_a_distinct_dict():
    health = DownloadNodeHealth()
    health.record("https://n/a.nc", DownloadOutcome.SUCCESS, 1.0, num_bytes=1_000_000)
    snap = health.snapshot()
    snap.clear()  # mutating the returned dict...
    assert set(health.snapshot()) == {"n"}  # ...does not affect the registry


def test_rank_by_throughput_orders_fastest_first_and_needs_success():
    health = DownloadNodeHealth()
    ok = DownloadOutcome.SUCCESS
    health.record("https://fast/f", ok, 2.0, num_bytes=200_000_000)
    health.record("https://slow/f", ok, 20.0, num_bytes=200_000_000)
    health.record("https://dead/f", DownloadOutcome.HOST_FAULT, 5.0)
    ranked = [s.host for s in health.rank_by_throughput()]
    assert ranked == ["fast", "slow"]  # dead has no success -> excluded


def test_rank_by_reliability_orders_and_filters_by_min_attempts():
    health = DownloadNodeHealth()
    health.record("https://good/f", DownloadOutcome.SUCCESS, 1.0, num_bytes=1_000_000)
    health.record("https://bad/f", DownloadOutcome.ERROR, 1.0)
    assert [s.host for s in health.rank_by_reliability()] == ["good", "bad"]
    assert health.rank_by_reliability(min_attempts=2) == []  # each tried only once


def test_host_rank_prefers_reliable_then_faster_and_explores_unseen():
    health = DownloadNodeHealth()
    ok = DownloadOutcome.SUCCESS
    # reliable + fast (100 MB/s)
    health.record("https://fast/f", ok, 2.0, num_bytes=200_000_000)
    # reliable but slower (10 MB/s)
    health.record("https://slow/f", ok, 20.0, num_bytes=200_000_000)
    # proven unreliable
    health.record("https://bad/f", DownloadOutcome.ERROR, 1.0)
    fast = health.host_rank("fast")
    slow = health.host_rank("slow")
    bad = health.host_rank("bad")
    unseen = health.host_rank("new")
    assert fast < slow  # same reliability, higher MB/s sorts first
    assert slow < unseen  # proven reliable beats unseen
    assert unseen < bad  # unseen is explored ahead of proven-bad
    assert unseen == (0.5, 0.0)  # neutral score for an untried host


def test_record_concurrency_accumulates_max_and_overwrites_last():
    health = DownloadNodeHealth()
    health.record_concurrency("n", max_safe=3, last=3)
    health.record_concurrency("n", max_safe=2, last=2)  # max stays 3; last -> 2
    stat = health.stat("n")
    assert stat.max_safe_concurrency == 3
    assert stat.last_concurrency == 2
