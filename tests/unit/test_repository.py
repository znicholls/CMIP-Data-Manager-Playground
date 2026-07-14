"""Tests for persistence, diffing and offline reads."""

from __future__ import annotations

from cmip_data_manager.esgf.headers import HeaderMetadata
from cmip_data_manager.esgf.health import NodeHealth, ReadOutcome
from cmip_data_manager.esgf.models import DatasetRecord, FileRecord


def _rec(
    dataset_id, *, timestamp="t0", source_id="M", variant="r1", experiment="ssp245"
):
    return DatasetRecord(
        id=dataset_id,
        source_id=source_id,
        variant_label=variant,
        experiment_id=experiment,
        variable_id="tas",
        frequency="mon",
        esgf_timestamp=timestamp,
        raw={"id": dataset_id},
    )


def test_first_run_reports_everything_added(repository):
    result = repository.record_run(
        "uc", [_rec("a"), _rec("b")], endpoint_url="u", spec={}
    )
    assert result.num_found == 2
    assert result.added == ["a", "b"]
    assert result.removed == []
    assert result.modified == []
    assert result.has_changes


def test_second_run_diffs_against_first(repository):
    repository.record_run("uc", [_rec("a"), _rec("b")], endpoint_url="u", spec={})
    result = repository.record_run(
        "uc",
        [_rec("a", timestamp="t1"), _rec("c")],  # a modified, b removed, c added
        endpoint_url="u",
        spec={},
    )
    assert result.added == ["c"]
    assert result.removed == ["b"]
    assert result.modified == ["a"]


def test_no_change_run(repository):
    repository.record_run("uc", [_rec("a")], endpoint_url="u", spec={})
    result = repository.record_run("uc", [_rec("a")], endpoint_url="u", spec={})
    assert not result.has_changes


def test_use_cases_are_isolated(repository):
    repository.record_run("uc1", [_rec("a")], endpoint_url="u", spec={})
    # A different use case should not diff against uc1.
    result = repository.record_run("uc2", [_rec("z")], endpoint_url="u", spec={})
    assert result.added == ["z"]
    assert result.removed == []


def test_changes_are_logged(repository):
    run = repository.record_run("uc", [_rec("a")], endpoint_url="u", spec={})
    changes = repository.get_changes(run.run_id)
    assert [(c.dataset_id, c.change_type) for c in changes] == [("a", "added")]


def test_offline_read_returns_latest_run(repository):
    repository.record_run("uc", [_rec("a"), _rec("b")], endpoint_url="u", spec={})
    repository.record_run("uc", [_rec("a")], endpoint_url="u", spec={})  # latest
    records = repository.get_dataset_records("uc")
    assert {r.id for r in records} == {"a"}


def test_offline_read_unknown_use_case_is_empty(repository):
    assert repository.get_dataset_records("never-run") == []


def test_store_files_links_to_known_datasets(repository):
    repository.record_run("uc", [_rec("ds0")], endpoint_url="u", spec={})
    files = [
        FileRecord(id="f0", dataset_id="ds0", raw={}),
        FileRecord(id="f1", dataset_id="unknown", raw={}),  # parent not cached
    ]
    stored = repository.store_files(files)
    assert stored == 1  # only the file whose parent exists


def test_store_files_upserts(repository):
    repository.record_run("uc", [_rec("ds0")], endpoint_url="u", spec={})
    repository.store_files([FileRecord(id="f0", dataset_id="ds0", size=1, raw={})])
    stored = repository.store_files(
        [FileRecord(id="f0", dataset_id="ds0", size=2, raw={})]
    )
    assert stored == 1


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
