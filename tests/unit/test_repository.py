"""Tests for persistence, diffing and offline reads."""

from __future__ import annotations

import pytest

from cmip_data_manager.db.repository import HeaderAttempt
from cmip_data_manager.esgf.headers import HeaderMetadata
from cmip_data_manager.esgf.health import NodeHealth, ReadOutcome
from cmip_data_manager.esgf.models import DatasetRecord


def _rec(  # noqa: PLR0913 - a test builder; every field has a default
    instance,
    *,
    node="node1.org",
    timestamp="t0",
    source_id="M",
    variant="r1",
    experiment="ssp245",
    variable="tas",
):
    """Build a node-specific record for dataset `instance` served from `node`."""
    node_id = f"{instance}|{node}"
    return DatasetRecord(
        id=node_id,
        instance_id=instance,
        data_node=node,
        source_id=source_id,
        variant_label=variant,
        experiment_id=experiment,
        variable_id=variable,
        frequency="mon",
        esgf_timestamp=timestamp,
        raw={"id": node_id, "instance_id": instance},
    )


def test_first_run_reports_everything_added(repository):
    result = repository.record_run(
        [_rec("a"), _rec("b")], endpoint_url="u", spec={}, tag="uc"
    )
    assert result.num_found == 2  # two distinct datasets
    assert result.added == ["a", "b"]
    assert result.removed == []
    assert result.modified == []
    assert result.has_changes


def test_second_run_diffs_against_first(repository):
    repository.record_run([_rec("a"), _rec("b")], endpoint_url="u", spec={}, tag="uc")
    result = repository.record_run(
        [_rec("a", timestamp="t1"), _rec("c")],  # a modified, b removed, c added
        endpoint_url="u",
        spec={},
        tag="uc",
    )
    assert result.added == ["c"]
    assert result.removed == ["b"]
    assert result.modified == ["a"]


def test_no_change_run(repository):
    repository.record_run([_rec("a")], endpoint_url="u", spec={}, tag="uc")
    result = repository.record_run([_rec("a")], endpoint_url="u", spec={}, tag="uc")
    assert not result.has_changes


def test_replicas_collapse_to_one_dataset_with_many_locations(repository):
    # The same dataset served from two nodes is one dataset, two locations.
    result = repository.record_run(
        [_rec("a", node="nci"), _rec("a", node="llnl")],
        endpoint_url="u",
        spec={},
        tag="uc",
    )
    assert result.num_found == 1  # one distinct dataset, not two
    assert result.added == ["a"]
    records = repository.get_dataset_records("uc")
    assert {r.instance_key for r in records} == {"a"}
    assert {r.node_key for r in records} == {"nci", "llnl"}  # both nodes reconstructed


def test_diffing_is_keyed_on_spec_not_tag(repository):
    # Different specs are independent series even under the same tag.
    repository.record_run([_rec("a")], endpoint_url="u", spec={"q": 1}, tag="uc")
    result = repository.record_run(
        [_rec("z")], endpoint_url="u", spec={"q": 2}, tag="uc"
    )
    assert result.added == ["z"]
    assert result.removed == []  # not diffed against the spec={"q": 1} run


def test_same_spec_forms_a_diff_series(repository):
    # The same spec forms one series even across different tags.
    repository.record_run([_rec("a")], endpoint_url="u", spec={"q": 1}, tag="uc1")
    result = repository.record_run(
        [_rec("z")], endpoint_url="u", spec={"q": 1}, tag="uc2"
    )
    assert result.added == ["z"]
    assert result.removed == ["a"]


def test_changes_are_logged(repository):
    run = repository.record_run([_rec("a")], endpoint_url="u", spec={}, tag="uc")
    changes = repository.get_changes(run.run_id)
    assert [(c.dataset_key, c.change_type) for c in changes] == [("a", "added")]


def test_offline_read_returns_latest_run_by_tag(repository):
    repository.record_run([_rec("a"), _rec("b")], endpoint_url="u", spec={}, tag="uc")
    repository.record_run([_rec("a")], endpoint_url="u", spec={}, tag="uc")  # latest
    records = repository.get_dataset_records("uc")
    assert {r.instance_key for r in records} == {"a"}


def test_offline_read_unknown_tag_is_empty(repository):
    assert repository.get_dataset_records("never-run") == []


def _header(**attrs):
    return HeaderMetadata(
        attrs={
            "parent_source_id": "ACCESS-ESM1-5",
            "parent_experiment_id": "historical",
            "parent_variant_label": "r1i1p1f1",
            "tracking_id": "hdl:21.14100/abc",
            "branch_time_in_parent": "60225.0",
            "grid": "native atmosphere N96 grid",
            **attrs,
        },
        source_url="https://esgf.nci.org.au/thredds/fileServer/x/tas.nc",
    )


_KEY = ("ACCESS-ESM1-5", "ssp245", "r1i1p1f1", "tas", "Amon")


def test_store_and_get_header_roundtrips_all_attrs(repository):
    stored = repository.store_headers({_KEY: _header()})
    assert stored == 1
    got = repository.get_header(_KEY)
    assert got is not None
    assert got.get("parent_experiment_id") == "historical"
    assert got.get("grid") == "native atmosphere N96 grid"  # from attrs_json
    assert got.source_url.endswith("tas.nc")


def test_get_header_missing_is_none(repository):
    assert repository.get_header(_KEY) is None


def test_store_headers_upserts_on_same_key(repository):
    repository.store_headers({_KEY: _header()})
    repository.store_headers({_KEY: _header(parent_experiment_id="piControl")})
    got = repository.get_header(_KEY)
    assert got.get("parent_experiment_id") == "piControl"


def test_table_id_distinguishes_same_variable(repository):
    day_key = ("ACCESS-ESM1-5", "ssp245", "r1i1p1f1", "tas", "day")
    repository.store_headers(
        {
            _KEY: _header(tracking_id="mon"),
            day_key: _header(tracking_id="day"),
        }
    )
    assert repository.get_header(_KEY).get("tracking_id") == "mon"
    assert repository.get_header(day_key).get("tracking_id") == "day"


def test_get_simulation_headers_spans_variables(repository):
    rsut_key = ("ACCESS-ESM1-5", "ssp245", "r1i1p1f1", "rsut", "Amon")
    other_sim = ("CanESM5", "ssp245", "r1i1p1f1", "tas", "Amon")
    repository.store_headers(
        {_KEY: _header(), rsut_key: _header(), other_sim: _header()}
    )
    headers = repository.get_simulation_headers("ACCESS-ESM1-5", "ssp245", "r1i1p1f1")
    assert len(headers) == 2  # tas + rsut, not the CanESM5 simulation


def test_node_health_persists_and_reloads(repository):
    health = NodeHealth()
    health.record("https://nci/f.nc", ReadOutcome.SUCCESS, 1.5)
    health.record("https://nci/g.nc", ReadOutcome.SUCCESS, 2.5)
    health.record("https://dead/f.nc", ReadOutcome.TIMEOUT, 90.0)
    assert repository.save_node_health(health) == 2

    reloaded = repository.load_node_health()
    nci = reloaded.stat("nci")
    assert nci.successes == 2
    assert nci.max_success_seconds == 2.5
    assert reloaded.stat("dead").timeouts == 1


def test_node_health_save_accumulates_across_runs(repository):
    first = NodeHealth()
    first.record("https://nci/f.nc", ReadOutcome.SUCCESS, 1.0)
    repository.save_node_health(first)

    # A later run loads, records more, saves back.
    later = repository.load_node_health()
    later.record("https://nci/g.nc", ReadOutcome.ERROR, 1.0)
    repository.save_node_health(later)

    final = repository.load_node_health().stat("nci")
    assert final.attempts == 2 and final.successes == 1 and final.errors == 1


def test_node_health_persists_blocks_and_learned_concurrency(repository):
    health = NodeHealth()
    health.record("https://busy/f.nc", ReadOutcome.BLOCKED, 0.5)
    # Learned per-node concurrency, seeded onto the stat the way the AIMD
    # controller will (round-trips through the persisted columns).
    stat = health.stat("busy")
    stat.max_safe_concurrency = 4
    stat.last_concurrency = 3
    health.restore(stat)
    repository.save_node_health(health)

    reloaded = repository.load_node_health().stat("busy")
    assert reloaded.blocks == 1
    assert reloaded.max_safe_concurrency == 4
    assert reloaded.last_concurrency == 3


def test_rank_nodes_by_reliability_and_speed(repository):
    health = NodeHealth()
    # fast + flawless
    health.record("https://nci/f.nc", ReadOutcome.SUCCESS, 1.0)
    # slower but reliable
    health.record("https://ceda/f.nc", ReadOutcome.SUCCESS, 8.0)
    # sometimes fails
    health.record("https://flaky/f.nc", ReadOutcome.SUCCESS, 3.0)
    health.record("https://flaky/g.nc", ReadOutcome.ERROR, 1.0)
    repository.save_node_health(health)

    by_reliability = [r.host for r in repository.rank_nodes_by_reliability()]
    assert by_reliability[-1] == "flaky"  # worst success rate ranks last
    by_speed = [r.host for r in repository.rank_nodes_by_speed()]
    assert by_speed[0] == "nci"  # fastest mean response first


def _attempt(**overrides):
    """Build a HeaderAttempt with sensible defaults for the attempt-log tests."""
    fields = {
        "source_id": "CanESM5",
        "experiment_id": "ssp245",
        "variant_label": "r1i1p1f1",
        "outcome": "success",
        "host": "nci",
        "url": "https://nci/tas.nc",
        "variable_id": "tas",
        "table_id": "Amon",
        "seconds": 1.5,
        "attempt_no": 1,
    }
    fields.update(overrides)
    return HeaderAttempt(**fields)


def test_record_header_attempts_is_append_only(repository):
    assert repository.record_header_attempts([]) == 0  # nothing to write
    n = repository.record_header_attempts(
        [_attempt(url="https://nci/a.nc"), _attempt(url="https://nci/b.nc")]
    )
    assert n == 2
    # Recording the same logical attempt again appends rather than upserting.
    repository.record_header_attempts([_attempt(url="https://nci/a.nc")])
    assert len(repository.get_header_attempts()) == 3


def test_record_header_attempts_persists_the_error_detail(repository):
    repository.record_header_attempts(
        [_attempt(outcome="error", detail="Connection refused by data node")]
    )
    (stored,) = repository.get_header_attempts()
    assert stored.detail == "Connection refused by data node"


def test_get_header_attempts_filters(repository):
    repository.record_header_attempts(
        [
            _attempt(host="ucar", source_id="CESM2-WACCM", outcome="timeout"),
            _attempt(host="nci", source_id="CanESM5", outcome="success"),
            _attempt(host="ucar", source_id="CESM2-WACCM", outcome="error"),
        ]
    )
    by_host = repository.get_header_attempts(host="ucar")
    assert len(by_host) == 2
    assert {a.source_id for a in by_host} == {"CESM2-WACCM"}
    only_timeout = repository.get_header_attempts(host="ucar", outcome="timeout")
    assert [a.outcome for a in only_timeout] == ["timeout"]
    assert repository.get_header_attempts(source_id="CanESM5", limit=1)[0].host == "nci"


def test_get_header_attempts_newest_first(repository):
    repository.record_header_attempts([_attempt(url="https://nci/first.nc")])
    repository.record_header_attempts([_attempt(url="https://nci/second.nc")])
    urls = [a.url for a in repository.get_header_attempts()]
    assert urls == ["https://nci/second.nc", "https://nci/first.nc"]


def test_header_attempt_summary_rolls_up_by_host(repository):
    repository.record_header_attempts(
        [
            _attempt(host="ornl", outcome="success"),
            _attempt(host="ornl", outcome="success"),
            _attempt(host="ornl", outcome="timeout"),
            _attempt(host="ucar", outcome="error"),
        ]
    )
    summary = {s.key: s for s in repository.header_attempt_summary(group_by="host")}
    assert summary["ornl"].attempts == 3
    assert summary["ornl"].successes == 2
    assert summary["ornl"].failures == 1
    assert summary["ornl"].outcomes == {"success": 2, "timeout": 1}
    assert summary["ucar"].failures == 1
    # Most-attempts host ranks first.
    assert repository.header_attempt_summary()[0].key == "ornl"


def test_header_attempt_summary_by_source_id_and_none_host(repository):
    repository.record_header_attempts(
        [
            _attempt(source_id="CanESM5", host="nci"),
            _attempt(source_id="MIROC6", host=None, outcome="no_candidate"),
        ]
    )
    by_source = {
        s.key: s for s in repository.header_attempt_summary(group_by="source_id")
    }
    assert by_source["MIROC6"].failures == 1
    by_host = {s.key: s for s in repository.header_attempt_summary(group_by="host")}
    assert "(none)" in by_host  # a null host groups under "(none)"


def test_header_attempt_summary_rejects_bad_group_by(repository):
    with pytest.raises(ValueError, match="group_by"):
        repository.header_attempt_summary(group_by="variant_label")
