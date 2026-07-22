"""Tests for selecting a target dataset version to carry into Steps 2-3."""

from __future__ import annotations

from cmip_data_manager.esgf.models import DatasetRecord
from cmip_data_manager.search.versions import latest_version, select_target_versions


def _rec(master, version, *, node="esgf.nci.org.au"):
    """A node-specific dataset record for `master` at `version`."""
    instance = f"{master}.{version}"
    node_id = f"{instance}|{node}"
    return DatasetRecord(
        id=node_id,
        instance_id=instance,
        master_id=master,
        version=version,
        data_node=node,
        raw={"id": node_id},
    )


def test_latest_version_uses_date_not_string_sort():
    # A `v` prefix would sort AFTER bare digits lexically; by date it does not.
    assert latest_version(["v20200225", "20221112", "v20191115"]) == "20221112"
    assert latest_version(["20190101"]) == "20190101"


def test_latest_version_ignores_unparseable_and_empty():
    assert latest_version(["not-a-date", "v20200101"]) == "v20200101"
    assert latest_version(["not-a-date"]) is None
    assert latest_version([]) is None


def test_latest_selection_keeps_newest_version_all_nodes():
    records = [
        _rec("M1", "v20200101", node="a"),
        _rec("M1", "v20200101", node="b"),
        _rec("M1", "v20221231", node="a"),  # newer, one node
    ]
    kept = select_target_versions(records)  # default "latest"
    assert {r.version for r in kept} == {"v20221231"}
    assert len(kept) == 1


def test_all_selection_returns_every_version():
    records = [_rec("M1", "v20200101"), _rec("M1", "v20221231")]
    kept = select_target_versions(records, selection="all")
    assert kept == records


def test_explicit_pin_overrides_and_unpinned_falls_back_to_latest():
    records = [
        _rec("M1", "v20200101"),
        _rec("M1", "v20221231"),  # M1 latest, but we pin the older one
        _rec("M2", "v20190101"),
        _rec("M2", "v20240101"),  # M2 not pinned -> latest
    ]
    kept = select_target_versions(records, selection={"M1": "v20200101"})
    got = {(r.master_key, r.version) for r in kept}
    assert got == {("M1", "v20200101"), ("M2", "v20240101")}


def test_undetermined_version_keeps_whole_dataset():
    records = [_rec("M1", "not-a-date"), _rec("M1", "also-bad")]
    kept = select_target_versions(records)
    assert kept == records  # nothing parseable -> keep all, drop nothing


def test_input_order_preserved():
    records = [
        _rec("M2", "v20240101"),
        _rec("M1", "v20240101"),
        _rec("M2", "v20240101", node="b"),
    ]
    kept = select_target_versions(records)
    assert kept == records
