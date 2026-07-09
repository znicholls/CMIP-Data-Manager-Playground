"""Tests for persistence, diffing and offline reads."""

from __future__ import annotations

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
