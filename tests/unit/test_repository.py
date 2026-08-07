"""Tests for persistence, diffing and offline reads."""

from __future__ import annotations

import pytest
from sqlmodel import Session, select

from cmip_data_manager.db.repository import (
    DownloadAttempt,
    FileSearchAttempt,
    HeaderAttempt,
)
from cmip_data_manager.db.schema import Cmip5VersionExtra
from cmip_data_manager.esgf.cmip5 import reconstruct_ids
from cmip_data_manager.esgf.download_health import DownloadNodeHealth, DownloadOutcome
from cmip_data_manager.esgf.health import NodeHealth, ReadOutcome
from cmip_data_manager.esgf.models import DatasetRecord, FileRecord

_V = "v20240101"


_C5_NATIVE = "cmip5.output1.INST.M.rcp45.mon.atmos.Amon.r1i1p1"


def _cmip5_rec(product, *, node="node1.org", version="20120101"):
    """A reconstructed CMIP5 record (as the era-aware backend would hand it in)."""
    raw = {
        "id": f"{_C5_NATIVE}.v{version}|{node}",
        "product": [product],
        "realm": ["atmos"],
        "master_id": [_C5_NATIVE],
        "instance_id": [f"{_C5_NATIVE}.v{version}"],
    }
    rec = DatasetRecord(
        id=raw["id"],
        project="CMIP5",
        mip_era="CMIP5",
        source_id="M",
        institution_id="INST",
        experiment_id="rcp45",
        variant_label="r1i1p1",
        variable_id="tas",
        frequency="mon",
        table_id="Amon",
        version=version,
        data_node=node,
        raw=raw,
    )
    return reconstruct_ids(rec)


def _inst(master, version=_V):
    """The instance id (version key) for a master + version."""
    return f"{master}.{version}"


def _rec(  # noqa: PLR0913 - a test builder; every field has a default
    master,
    *,
    version=_V,
    node="node1.org",
    timestamp="t0",
    source_id="M",
    variant="r1",
    experiment="ssp245",
    variable="tas",
    mip_era=None,
):
    """Build a node-specific record for dataset `master` at `version`, on `node`."""
    instance = f"{master}.{version}"
    node_id = f"{instance}|{node}"
    return DatasetRecord(
        id=node_id,
        instance_id=instance,
        master_id=master,
        version=version,
        mip_era=mip_era,
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
    assert result.num_found == 2  # two distinct datasets (masters)
    assert result.added == [_inst("a"), _inst("b")]  # reported at the version grain
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
    assert result.added == [_inst("c")]
    assert result.removed == [_inst("b")]
    assert result.modified == [_inst("a")]


def test_no_change_run(repository):
    repository.record_run([_rec("a")], endpoint_url="u", spec={}, tag="uc")
    result = repository.record_run([_rec("a")], endpoint_url="u", spec={}, tag="uc")
    assert not result.has_changes


def test_replicas_collapse_to_one_dataset_with_many_locations(repository):
    # The same version served from two nodes is one dataset, one version, two locations.
    result = repository.record_run(
        [_rec("a", node="nci"), _rec("a", node="llnl")],
        endpoint_url="u",
        spec={},
        tag="uc",
    )
    assert result.num_found == 1  # one distinct dataset, not two
    assert result.added == [_inst("a")]
    records = repository.get_dataset_records("uc")
    assert {r.master_key for r in records} == {"a"}
    assert {r.node_key for r in records} == {"nci", "llnl"}  # both nodes reconstructed


def test_mip_era_persists_and_round_trips(repository):
    # The era discriminator survives a store -> reconstruct cycle at the record grain.
    repository.record_run(
        [_rec("a", mip_era="CMIP5"), _rec("b", mip_era="CMIP6")],
        endpoint_url="u",
        spec={},
        tag="uc",
    )
    records = repository.get_dataset_records("uc")
    eras = {r.master_key: r.mip_era for r in records}
    assert eras == {"a": "CMIP5", "b": "CMIP6"}


def test_cmip5_extra_row_is_stored_with_base_realm_and_native_ids(repository):
    rec = _cmip5_rec("output1")
    repository.record_run([rec], endpoint_url="u", spec={}, tag="uc")
    with Session(repository._engine) as session:
        extras = session.exec(select(Cmip5VersionExtra)).all()
    assert len(extras) == 1
    extra = extras[0]
    assert extra.version_key == rec.instance_id  # output1 seen first -> no suffix
    assert extra.base_master_id == rec.master_id
    assert extra.distinguishing_json == '{"product": "output1"}'
    assert extra.realm == "atmos"
    assert extra.native_master_id == _C5_NATIVE  # table-grained, no variable
    assert extra.native_dataset_id == f"{_C5_NATIVE}.v20120101"


def test_product_collision_gets_suffix_and_is_surfaced(repository):
    base_rec = _cmip5_rec("output1")
    repository.record_run(
        [base_rec, _cmip5_rec("output2")],
        endpoint_url="u",
        spec={},
        tag="uc",
    )
    # output1 (first seen) keeps the bare master id; output2 gets .1.
    records = repository.get_dataset_records("uc")
    masters = sorted(r.master_key for r in records)
    assert masters == [base_rec.master_id, base_rec.master_id + ".1"]

    choices = repository.cmip5_distinguishing_conflicts()
    assert len(choices) == 1
    choice = choices[0]
    assert choice.base_master_id == base_rec.master_id
    assert choice.options == [
        ('{"product": "output1"}', base_rec.master_id),
        ('{"product": "output2"}', base_rec.master_id + ".1"),
    ]


def test_product_suffix_is_stable_across_runs(repository):
    repository.record_run(
        [_cmip5_rec("output1"), _cmip5_rec("output2")],
        endpoint_url="u",
        spec={},
        tag="uc",
    )
    # A later run that sees output2 first must NOT rekey it to the bare id.
    repository.record_run(
        [_cmip5_rec("output2"), _cmip5_rec("output1")],
        endpoint_url="u",
        spec={},
        tag="uc",
    )
    with Session(repository._engine) as session:
        rows = session.exec(
            select(Cmip5VersionExtra.distinguishing_json, Cmip5VersionExtra.version_key)
        ).all()
    by_product = {d: v for d, v in rows}
    assert by_product['{"product": "output1"}'].endswith("atmos.v20120101")
    assert by_product['{"product": "output2"}'].endswith("atmos.1.v20120101")


def test_single_product_is_not_a_conflict(repository):
    repository.record_run([_cmip5_rec("output1")], endpoint_url="u", spec={}, tag="uc")
    assert repository.cmip5_distinguishing_conflicts() == []


def test_cmip6_records_create_no_cmip5_extra(repository):
    repository.record_run([_rec("a")], endpoint_url="u", spec={}, tag="uc")
    with Session(repository._engine) as session:
        assert session.exec(select(Cmip5VersionExtra)).all() == []
    assert repository.cmip5_distinguishing_conflicts() == []


def test_versions_of_one_dataset_are_one_master_many_versions(repository):
    # Two versions of the same dataset: one master, two DatasetVersion rows.
    result = repository.record_run(
        [_rec("a", version="v20240101"), _rec("a", version="v20240202")],
        endpoint_url="u",
        spec={},
        tag="uc",
    )
    assert result.num_found == 1  # one logical dataset
    assert result.added == [_inst("a", "v20240101"), _inst("a", "v20240202")]
    records = repository.get_dataset_records("uc")
    assert {r.master_key for r in records} == {"a"}  # one master
    assert {r.version for r in records} == {"v20240101", "v20240202"}  # two versions


def test_non_numeric_version_is_rejected(repository):
    with pytest.raises(ValueError, match="is not numeric"):
        repository.record_run(
            [_rec("a", version="not-a-date")], endpoint_url="u", spec={}, tag="uc"
        )


def test_integer_version_is_accepted(repository):
    # CMIP5 datasets sometimes carry a plain integer version (e.g. "1"), not a date.
    repository.record_run([_rec("a", version="1")], endpoint_url="u", spec={}, tag="uc")
    records = repository.get_dataset_records("uc")
    assert [r.version for r in records] == ["1"]


def test_diffing_is_keyed_on_spec_not_tag(repository):
    # Different specs are independent series even under the same tag.
    repository.record_run([_rec("a")], endpoint_url="u", spec={"q": 1}, tag="uc")
    result = repository.record_run(
        [_rec("z")], endpoint_url="u", spec={"q": 2}, tag="uc"
    )
    assert result.added == [_inst("z")]
    assert result.removed == []  # not diffed against the spec={"q": 1} run


def test_same_spec_forms_a_diff_series(repository):
    # The same spec forms one series even across different tags.
    repository.record_run([_rec("a")], endpoint_url="u", spec={"q": 1}, tag="uc1")
    result = repository.record_run(
        [_rec("z")], endpoint_url="u", spec={"q": 1}, tag="uc2"
    )
    assert result.added == [_inst("z")]
    assert result.removed == [_inst("a")]


def test_changes_are_logged(repository):
    run = repository.record_run([_rec("a")], endpoint_url="u", spec={}, tag="uc")
    changes = repository.get_changes(run.run_id)
    assert [(c.version_key, c.change_type) for c in changes] == [(_inst("a"), "added")]


def test_offline_read_returns_latest_run_by_tag(repository):
    repository.record_run([_rec("a"), _rec("b")], endpoint_url="u", spec={}, tag="uc")
    repository.record_run([_rec("a")], endpoint_url="u", spec={}, tag="uc")  # latest
    records = repository.get_dataset_records("uc")
    assert {r.master_key for r in records} == {"a"}


def test_offline_read_unknown_tag_is_empty(repository):
    assert repository.get_dataset_records("never-run") == []


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


_CEDA = "https://esgf.ceda.ac.uk/esg-search/search"
_ORNL = "https://esgf-node.ornl.gov/proxy/search"


def _file_attempt(**overrides):
    """Build a FileSearchAttempt with sensible defaults for the log tests."""
    fields = {
        "endpoint": _CEDA,
        "version_key": "M.ssp245.r1.tas.v20240101",
        "outcome": "success",
        "files_found": 3,
        "seconds": 1.5,
        "attempt_no": 1,
    }
    fields.update(overrides)
    return FileSearchAttempt(**fields)


def test_record_file_access_attempts_is_append_only(repository):
    assert repository.record_file_access_attempts([]) == 0  # nothing to write
    n = repository.record_file_access_attempts(
        [_file_attempt(version_key="a"), _file_attempt(version_key="b")]
    )
    assert n == 2
    # Recording the same logical attempt again appends rather than upserting.
    repository.record_file_access_attempts([_file_attempt(version_key="a")])
    assert len(repository.get_file_access_attempts()) == 3


def test_record_file_access_attempts_persists_the_error_detail(repository):
    repository.record_file_access_attempts(
        [_file_attempt(outcome="server_error", files_found=0, detail="500 from index")]
    )
    (stored,) = repository.get_file_access_attempts()
    assert stored.outcome == "server_error"
    assert stored.files_found == 0
    assert stored.detail == "500 from index"


def test_get_file_access_attempts_filters(repository):
    repository.record_file_access_attempts(
        [
            _file_attempt(endpoint=_CEDA, version_key="v1", outcome="empty"),
            _file_attempt(endpoint=_ORNL, version_key="v1", outcome="success"),
            _file_attempt(endpoint=_CEDA, version_key="v2", outcome="server_error"),
        ]
    )
    by_endpoint = repository.get_file_access_attempts(endpoint=_CEDA)
    assert len(by_endpoint) == 2
    by_version = repository.get_file_access_attempts(version_key="v1")
    assert {a.endpoint for a in by_version} == {_CEDA, _ORNL}
    only_empty = repository.get_file_access_attempts(outcome="empty")
    assert [a.version_key for a in only_empty] == ["v1"]


def test_get_file_access_attempts_newest_first(repository):
    repository.record_file_access_attempts([_file_attempt(version_key="first")])
    repository.record_file_access_attempts([_file_attempt(version_key="second")])
    keys = [a.version_key for a in repository.get_file_access_attempts()]
    assert keys == ["second", "first"]


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


# --- download attempts, health and state -------------------------------------


def _stored_file(repository, *, checksum="abc123", checksum_type="md5"):
    """Create one `File` via the public API; return its id and a `FileAccess` id."""
    repository.record_run([_rec("m")], endpoint_url="u", spec={}, tag="uc")
    record = FileRecord(
        id="file-tas",
        dataset_id=f"{_inst('m')}|node1.org",
        title="tas.nc",
        size=1000,
        checksum=checksum,
        checksum_type=checksum_type,
        urls=("https://node1.org/tas.nc|application/netcdf|HTTPServer",),
        raw={},
    )
    repository.store_files({_inst("m"): [record]})
    (stored,) = repository.get_version_files(_inst("m"))
    return stored.id, stored.accesses[0].id


def _dl_attempt(**overrides):
    """Build a DownloadAttempt with sensible defaults for the log tests."""
    fields = {
        "outcome": "success",
        "file_id": None,
        "host": "good.node",
        "url": "https://good.node/tas.nc",
        "bytes_downloaded": 200_000_000,
        "seconds": 2.0,
        "throughput_mbps": 100.0,
        "attempt_no": 1,
        "resumed": False,
    }
    fields.update(overrides)
    return DownloadAttempt(**fields)


def test_record_download_attempts_is_append_only(repository):
    assert repository.record_download_attempts([]) == 0  # nothing to write
    n = repository.record_download_attempts(
        [_dl_attempt(url="https://n/a.nc"), _dl_attempt(url="https://n/b.nc")]
    )
    assert n == 2
    # Recording the same logical attempt again appends rather than upserting.
    repository.record_download_attempts([_dl_attempt(url="https://n/a.nc")])
    assert len(repository.get_download_attempts()) == 3


def test_record_download_attempts_persists_bytes_resume_and_detail(repository):
    repository.record_download_attempts(
        [
            _dl_attempt(
                outcome="checksum_failed",
                detail="md5 abc != def",
                throughput_mbps=0.0,
                resumed=True,
            )
        ]
    )
    (stored,) = repository.get_download_attempts()
    assert stored.outcome == "checksum_failed"
    assert stored.detail == "md5 abc != def"
    assert stored.resumed is True
    assert stored.bytes_downloaded == 200_000_000


def test_get_download_attempts_filters(repository):
    repository.record_download_attempts(
        [
            _dl_attempt(host="ornl", outcome="timeout"),
            _dl_attempt(host="nci", outcome="success"),
            _dl_attempt(host="ornl", outcome="host_fault"),
        ]
    )
    by_host = repository.get_download_attempts(host="ornl")
    assert len(by_host) == 2
    only_timeout = repository.get_download_attempts(host="ornl", outcome="timeout")
    assert [a.outcome for a in only_timeout] == ["timeout"]


def test_get_download_attempts_by_file_id_and_newest_first(repository):
    file_id, _ = _stored_file(repository)
    repository.record_download_attempts(
        [_dl_attempt(file_id=file_id, url="https://n/first.nc")]
    )
    repository.record_download_attempts(
        [_dl_attempt(file_id=file_id, url="https://n/second.nc")]
    )
    repository.record_download_attempts(
        [_dl_attempt(file_id=None, url="https://x/o.nc")]
    )
    for_file = repository.get_download_attempts(file_id=file_id)
    assert [a.url for a in for_file] == ["https://n/second.nc", "https://n/first.nc"]


def test_download_attempt_summary_rolls_up_by_host(repository):
    repository.record_download_attempts(
        [
            _dl_attempt(host="ornl", outcome="success"),
            _dl_attempt(host="ornl", outcome="success"),
            _dl_attempt(host="ornl", outcome="checksum_failed"),
            _dl_attempt(host=None, outcome="no_candidate"),
        ]
    )
    summary = {s.key: s for s in repository.download_attempt_summary()}
    assert summary["ornl"].attempts == 3
    assert summary["ornl"].successes == 2
    assert summary["ornl"].failures == 1
    assert summary["ornl"].outcomes == {"success": 2, "checksum_failed": 1}
    assert "(none)" in summary  # a null host groups under "(none)"
    assert repository.download_attempt_summary()[0].key == "ornl"  # most attempts first


def test_download_health_persists_and_reloads(repository):
    health = DownloadNodeHealth()
    ok = DownloadOutcome.SUCCESS
    health.record("https://fast/a", ok, 2.0, num_bytes=200_000_000)
    health.record("https://fast/b", ok, 2.0, num_bytes=200_000_000)
    health.record("https://bad/f", DownloadOutcome.HOST_FAULT, 5.0)
    assert repository.save_download_health(health) == 2

    reloaded = repository.load_download_health()
    fast = reloaded.stat("fast")
    assert fast.successes == 2
    assert fast.total_bytes == 400_000_000
    assert reloaded.stat("bad").host_faults == 1


def test_download_health_accumulates_and_persists_concurrency(repository):
    health = DownloadNodeHealth()
    health.record("https://n/f.nc", DownloadOutcome.CHECKSUM_FAILED, 1.0)
    stat = health.stat("n")
    stat.max_safe_concurrency = 3
    stat.last_concurrency = 2
    health.restore(stat)
    repository.save_download_health(health)

    # A later run loads, records more, saves back.
    later = repository.load_download_health()
    later.record("https://n/g.nc", DownloadOutcome.SUCCESS, 1.0, num_bytes=1_000_000)
    repository.save_download_health(later)

    final = repository.load_download_health().stat("n")
    assert final.attempts == 2
    assert final.checksum_failures == 1
    assert final.successes == 1
    assert final.max_safe_concurrency == 3
    assert final.last_concurrency == 2


def test_rank_download_nodes_by_throughput_and_reliability(repository):
    health = DownloadNodeHealth()
    ok = DownloadOutcome.SUCCESS
    health.record("https://fast/f", ok, 2.0, num_bytes=200_000_000)
    health.record("https://slow/f", ok, 20.0, num_bytes=200_000_000)
    health.record("https://flaky/f", ok, 2.0, num_bytes=100_000_000)
    health.record("https://flaky/g", DownloadOutcome.ERROR, 1.0)
    repository.save_download_health(health)

    by_throughput = [r.host for r in repository.rank_download_nodes_by_throughput()]
    assert by_throughput[0] == "fast"  # highest MB/s first
    assert by_throughput[-1] == "slow"  # lowest MB/s of those with successes
    by_reliability = [r.host for r in repository.rank_download_nodes_by_reliability()]
    assert by_reliability[-1] == "flaky"  # worst success rate ranks last


def test_rank_download_by_throughput_excludes_nodes_without_success(repository):
    health = DownloadNodeHealth()
    health.record("https://dead/f.nc", DownloadOutcome.HOST_FAULT, 5.0)
    repository.save_download_health(health)
    assert repository.rank_download_nodes_by_throughput() == []  # no throughput
    # ...but a never-succeeding node still appears in the reliability ranking.
    assert [r.host for r in repository.rank_download_nodes_by_reliability()] == ["dead"]


def test_mark_download_and_get_state(repository):
    file_id, access_id = _stored_file(repository)
    assert repository.get_download_state(file_id) is None  # nothing yet

    repository.mark_download(
        file_id=file_id,
        status="complete",
        local_path="/data/tas.nc",
        size_bytes=1000,
        verified=True,
        verified_algo="md5",
        download_from_access_key=access_id,
        attempts=2,
    )
    state = repository.get_download_state(file_id)
    assert state.status == "complete"
    assert state.verified is True
    assert state.verified_algo == "md5"
    assert state.local_path == "/data/tas.nc"
    assert state.download_from_access_key == access_id
    assert state.completed_at is not None  # stamped by default


def test_mark_download_upserts_on_reattempt(repository):
    file_id, _ = _stored_file(repository)
    repository.mark_download(file_id=file_id, status="failed", attempts=3)
    repository.mark_download(
        file_id=file_id,
        status="complete",
        local_path="/data/tas.nc",
        size_bytes=1000,
        verified=True,
        verified_algo="md5",
        attempts=4,
    )
    state = repository.get_download_state(file_id)
    assert state.status == "complete"  # overwritten, not duplicated
    assert state.attempts == 4


def test_version_downloads_returns_per_version_state(repository):
    file_id, _ = _stored_file(repository)
    repository.mark_download(
        file_id=file_id, status="complete", local_path="/data/tas.nc"
    )
    downloads = repository.version_downloads(_inst("m"))
    assert [(d.file_id, d.status) for d in downloads] == [(file_id, "complete")]
    assert repository.version_downloads("no-such-version") == []
