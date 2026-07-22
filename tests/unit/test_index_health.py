"""Unit tests for the Step-2 search-index health registry and its persistence."""

from __future__ import annotations

import httpx

from cmip_data_manager.esgf.index_health import (
    IndexNodeHealth,
    SearchOutcome,
    classify_search_error,
)

WEST = "https://metagrid.esgf-west.org/proxy/search"
CEDA = "https://esgf.ceda.ac.uk/esg-search/search"


def test_records_success_and_latency():
    health = IndexNodeHealth()
    health.record(CEDA, SearchOutcome.SUCCESS, 0.9)
    health.record(CEDA, SearchOutcome.SUCCESS, 1.5)

    stat = health.stat(CEDA)
    assert stat.attempts == 2
    assert stat.successes == 2
    assert stat.failures == 0
    assert stat.success_rate == 1.0
    assert stat.max_success_seconds == 1.5
    assert stat.mean_success_seconds == 1.2


def test_counts_retries_and_error_kinds():
    health = IndexNodeHealth()
    health.record(WEST, SearchOutcome.SERVER_ERROR, 30.0)
    health.record(WEST, SearchOutcome.SERVER_ERROR, 30.0, retry=True)
    health.record(WEST, SearchOutcome.TIMEOUT, 60.0, retry=True)
    health.record(WEST, SearchOutcome.ERROR, 0.1)

    stat = health.stat(WEST)
    assert stat.attempts == 4
    assert stat.failures == 4
    assert stat.successes == 0
    assert stat.retries == 2
    assert stat.server_errors == 2
    assert stat.timeouts == 1
    assert stat.failure_rate == 1.0
    assert stat.mean_success_seconds is None


def test_unseen_endpoint_and_empty_stat_defaults():
    health = IndexNodeHealth()
    assert health.stat("https://never/search") is None
    assert health.snapshot() == {}


def test_rank_by_reliability_orders_worst_last():
    health = IndexNodeHealth()
    health.record(CEDA, SearchOutcome.SUCCESS, 1.0)
    health.record(WEST, SearchOutcome.SUCCESS, 1.0)
    health.record(WEST, SearchOutcome.SERVER_ERROR, 30.0)

    ranked = [s.endpoint for s in health.rank_by_reliability()]
    assert ranked[0] == CEDA  # flawless first
    assert ranked[-1] == WEST  # has a failure, ranks last


def test_classify_search_error_maps_transport_exceptions():
    request = httpx.Request("GET", WEST)
    server = httpx.HTTPStatusError(
        "boom", request=request, response=httpx.Response(500, request=request)
    )
    client_4xx = httpx.HTTPStatusError(
        "nope", request=request, response=httpx.Response(404, request=request)
    )
    assert classify_search_error(httpx.ConnectTimeout("slow")) is SearchOutcome.TIMEOUT
    assert classify_search_error(server) is SearchOutcome.SERVER_ERROR
    assert classify_search_error(client_4xx) is SearchOutcome.ERROR
    assert classify_search_error(ValueError("bad json")) is SearchOutcome.ERROR


def test_index_health_persists_and_reloads(repository):
    health = IndexNodeHealth()
    health.record(CEDA, SearchOutcome.SUCCESS, 0.9)
    health.record(WEST, SearchOutcome.SERVER_ERROR, 30.0, retry=True)
    assert repository.save_index_health(health) == 2

    reloaded = repository.load_index_health()
    assert reloaded.stat(CEDA).successes == 1
    west = reloaded.stat(WEST)
    assert west.failures == 1
    assert west.server_errors == 1
    assert west.retries == 1


def test_index_health_save_accumulates_across_runs(repository):
    first = IndexNodeHealth()
    first.record(WEST, SearchOutcome.SUCCESS, 1.0)
    repository.save_index_health(first)

    later = repository.load_index_health()
    later.record(WEST, SearchOutcome.SERVER_ERROR, 30.0, retry=True)
    repository.save_index_health(later)

    final = repository.load_index_health().stat(WEST)
    assert final.attempts == 2
    assert final.successes == 1
    assert final.server_errors == 1
    assert final.retries == 1


def test_rank_index_nodes_by_reliability_from_db(repository):
    health = IndexNodeHealth()
    health.record(CEDA, SearchOutcome.SUCCESS, 1.0)
    health.record(WEST, SearchOutcome.SUCCESS, 1.0)
    health.record(WEST, SearchOutcome.SERVER_ERROR, 30.0)
    repository.save_index_health(health)

    ranked = [r.endpoint for r in repository.rank_index_nodes_by_reliability()]
    assert ranked[0] == CEDA
    assert ranked[-1] == WEST
