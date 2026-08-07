"""Tests for the Step-5 download orchestrator (`search.download`)."""

from __future__ import annotations

import hashlib
from pathlib import Path
from urllib.parse import urlparse

from cmip_data_manager.esgf.download import (
    ChecksumMismatch,
    DownloadHostFault,
    DownloadInfo,
)
from cmip_data_manager.esgf.models import DatasetRecord, FileRecord
from cmip_data_manager.search.download import download_files, drs_path

_INSTANCE = "CMIP6.ScenarioMIP.CSIRO.ACCESS-CM2.ssp245.r1i1p1f1.Amon.tas.gn.v20191108"
_DRS_DIR = Path(
    "CMIP6/ScenarioMIP/CSIRO/ACCESS-CM2/ssp245/r1i1p1f1/Amon/tas/gn/v20191108"
)


def _md5(data: bytes) -> str:
    return hashlib.new("md5", data).hexdigest()  # noqa: S324 - checksum, not security


def _record() -> DatasetRecord:
    """A CMIP6 version record with the facets `drs_path` needs."""
    node_id = f"{_INSTANCE}|node1.org"
    return DatasetRecord(
        id=node_id,
        instance_id=_INSTANCE,
        master_id=_INSTANCE.rsplit(".", 1)[0],
        mip_era="CMIP6",
        project="CMIP6",
        source_id="ACCESS-CM2",
        institution_id="CSIRO",
        experiment_id="ssp245",
        variant_label="r1i1p1f1",
        variable_id="tas",
        table_id="Amon",
        grid_label="gn",
        version="v20191108",
        data_node="node1.org",
        raw={"id": node_id, "activity_drs": ["ScenarioMIP"]},
    )


def _seed(repository, *files):
    """Store a version and its files; return the record and the stored File ids."""
    record = _record()
    repository.record_run([record], endpoint_url="u", spec={}, tag="uc")
    file_records = []
    for filename, checksum, checksum_type, size, hosts in files:
        urls = tuple(
            f"https://{h}/{filename}|application/netcdf|HTTPServer" for h in hosts
        )
        file_records.append(
            FileRecord(
                id=f"file-{filename}",
                dataset_id=record.id,
                title=filename,
                size=size,
                checksum=checksum,
                checksum_type=checksum_type,
                urls=urls,
                raw={},
            )
        )
    repository.store_files({record.instance_key: file_records})
    ids = {f.filename: f.id for f in repository.get_version_files(record.instance_key)}
    return record, ids


def _seed_one(repository, content: bytes, hosts, *, checksum: bool = True):
    """Seed a single `tas.nc` file, with or without a checksum."""
    csum = _md5(content) if checksum else None
    ctype = "md5" if checksum else None
    return _seed(repository, ("tas.nc", csum, ctype, len(content), hosts))


class _FakeDownloader:
    """A downloader that writes bytes to disk (no network), honouring failures."""

    def __init__(
        self, content: bytes = b"netcdf-bytes" * 100, *, fail_hosts=frozenset()
    ):
        self.content = content
        self.fail_hosts = fail_hosts
        self.calls: list[str] = []

    def __call__(  # noqa: PLR0913 - matches the FileDownloader seam
        self,
        url: str,
        dest: Path,
        *,
        expected_checksum: str | None,
        checksum_type: str | None,
        chunk_bytes: int,
        connect_timeout: float,
        read_timeout: float,
    ) -> DownloadInfo:
        self.calls.append(url)
        host = urlparse(url).hostname or ""
        if host in self.fail_hosts:
            raise DownloadHostFault(url, "dead node")
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(self.content)
        verified, algo = False, None
        if expected_checksum and checksum_type:
            algo = checksum_type.lower()
            actual = hashlib.new(algo, self.content).hexdigest()
            if actual != expected_checksum.lower():
                dest.unlink()
                raise ChecksumMismatch(
                    url, expected=expected_checksum, actual=actual, algo=algo
                )
            verified = True
        return DownloadInfo(
            url=url,
            path=dest,
            bytes_downloaded=len(self.content),
            total_bytes=len(self.content),
            resumed=False,
            verified=verified,
            verified_algo=algo,
        )


# --- drs_path ----------------------------------------------------------------


def test_drs_path_cmip6_from_facets():
    root = Path("/data")
    path = drs_path(root, _record(), "tas_Amon_ACCESS-CM2_ssp245.nc")
    assert path == root / _DRS_DIR / "tas_Amon_ACCESS-CM2_ssp245.nc"


def test_drs_path_cmip5_from_native_instance_id():
    native = "cmip5.output1.CSIRO.ACCESS1-0.rcp45.mon.atmos.Amon.r1i1p1.v20120115"
    record = DatasetRecord(
        id=f"{native}|node",
        mip_era="CMIP5",
        project="CMIP5",
        source_id="ACCESS1-0",
        raw={"instance_id": [native]},
    )
    path = drs_path(Path("/data"), record, "tas.nc")
    assert path == Path("/data").joinpath(*native.split("."), "tas.nc")


def test_drs_path_missing_cmip6_facet_becomes_unknown():
    record = DatasetRecord(id="x|n", mip_era="CMIP6", raw={})
    path = drs_path(Path("/data"), record, "f.nc")
    assert "unknown" in path.parts  # a missing facet does not break the path


# --- download_files end-to-end -----------------------------------------------


def test_download_files_writes_files_state_and_attempts(repository, tmp_path):
    content = b"tas-data" * 50
    _, ids = _seed_one(repository, content, ["node1.org"])
    downloader = _FakeDownloader(content)

    result = download_files(
        [_record()],
        repository=repository,
        download_root=tmp_path,
        downloader=downloader,
    )

    dest = tmp_path / _DRS_DIR / "tas.nc"
    assert dest.read_bytes() == content
    assert result.downloaded == 1
    assert result.verified == 1
    assert result.unverified == 0
    assert result.failed == []
    assert result.bytes_downloaded == len(content)
    assert result.mean_mbps is not None

    state = repository.get_download_state(ids["tas.nc"])
    assert state.status == "complete"
    assert state.verified is True
    assert state.verified_algo == "md5"
    assert state.local_path == str(dest)

    attempts = repository.get_download_attempts(file_id=ids["tas.nc"])
    assert [a.outcome for a in attempts] == ["success"]
    assert attempts[0].throughput_mbps >= 0.0


def test_download_files_without_checksum_is_unverified(repository, tmp_path):
    content = b"x" * 200
    _, ids = _seed_one(repository, content, ["node1.org"], checksum=False)

    result = download_files(
        [_record()],
        repository=repository,
        download_root=tmp_path,
        downloader=_FakeDownloader(content),
    )
    assert result.downloaded == 1
    assert result.unverified == 1
    assert repository.get_download_state(ids["tas.nc"]).status == "unverified"


def test_download_files_skips_present_and_verified(repository, tmp_path):
    content = b"already-here" * 20
    _, ids = _seed_one(repository, content, ["node1.org"])
    # Pre-place the file at its DRS path so it should be skipped.
    dest = tmp_path / _DRS_DIR / "tas.nc"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(content)
    downloader = _FakeDownloader(content)

    result = download_files(
        [_record()],
        repository=repository,
        download_root=tmp_path,
        downloader=downloader,
    )
    assert result.skipped == 1
    assert result.downloaded == 0
    assert downloader.calls == []  # nothing was fetched
    assert repository.get_download_state(ids["tas.nc"]).status == "skipped"


def test_download_files_fails_over_to_next_mirror(repository, tmp_path):
    content = b"mirrored" * 30
    _, ids = _seed_one(repository, content, ["bad.node", "good.node"])
    downloader = _FakeDownloader(content, fail_hosts=frozenset({"bad.node"}))

    result = download_files(
        [_record()],
        repository=repository,
        download_root=tmp_path,
        downloader=downloader,
        preferred_hosts=("bad.node",),  # force the dead node to be tried first
    )
    assert result.downloaded == 1
    assert result.failed == []
    attempts = repository.get_download_attempts(file_id=ids["tas.nc"])
    outcomes = {a.outcome for a in attempts}
    assert outcomes == {"host_fault", "success"}  # bad failed, good succeeded


def test_download_files_records_failure_when_all_mirrors_fail(repository, tmp_path):
    content = b"unreachable" * 10
    _, ids = _seed_one(repository, content, ["dead.node"])
    downloader = _FakeDownloader(content, fail_hosts=frozenset({"dead.node"}))

    result = download_files(
        [_record()],
        repository=repository,
        download_root=tmp_path,
        downloader=downloader,
    )
    assert result.downloaded == 0
    assert result.failed == [ids["tas.nc"]]
    assert repository.get_download_state(ids["tas.nc"]).status == "failed"


def test_download_files_reports_no_http_access(repository, tmp_path):
    # A file whose only access is OPeNDAP has no downloadable HTTPServer mirror.
    record = _record()
    repository.record_run([record], endpoint_url="u", spec={}, tag="uc")
    repository.store_files(
        {
            record.instance_key: [
                FileRecord(
                    id="file-tas",
                    dataset_id=record.id,
                    title="tas.nc",
                    urls=("https://node1.org/tas.nc|application/x-netcdf|OPENDAP",),
                    raw={},
                )
            ]
        }
    )
    file_id = repository.get_version_files(record.instance_key)[0].id
    downloader = _FakeDownloader()

    result = download_files(
        [record],
        repository=repository,
        download_root=tmp_path,
        downloader=downloader,
    )
    assert result.no_http_access == [file_id]
    assert result.downloaded == 0
    assert downloader.calls == []
    assert repository.get_download_state(file_id).status == "failed"


def test_download_files_persists_download_health(repository, tmp_path):
    content = b"speed" * 100
    _seed_one(repository, content, ["node1.org"])
    download_files(
        [_record()],
        repository=repository,
        download_root=tmp_path,
        downloader=_FakeDownloader(content),
    )
    health = repository.load_download_health()
    assert health.stat("node1.org").successes == 1
    assert health.stat("node1.org").total_bytes == len(content)


def test_download_files_batch_mode_persists_after_the_run(repository, tmp_path):
    content = b"batch" * 40
    _, ids = _seed_one(repository, content, ["node1.org"])
    result = download_files(
        [_record()],
        repository=repository,
        download_root=tmp_path,
        downloader=_FakeDownloader(content),
        persist_as_you_go=False,  # write everything after the batch
    )
    assert result.downloaded == 1
    assert repository.get_download_state(ids["tas.nc"]).status == "complete"


def test_download_files_without_attempt_log_has_no_mean(repository, tmp_path):
    content = b"quiet" * 40
    _, ids = _seed_one(repository, content, ["node1.org"])
    result = download_files(
        [_record()],
        repository=repository,
        download_root=tmp_path,
        downloader=_FakeDownloader(content),
        record_attempts=False,
    )
    assert result.downloaded == 1
    assert result.mean_mbps is None
    assert repository.get_download_attempts(file_id=ids["tas.nc"]) == []


def test_download_redownloads_size_mismatched_present(repository, tmp_path):
    content = b"correct-content" * 10
    _seed_one(repository, content, ["node1.org"], checksum=False)
    dest = tmp_path / _DRS_DIR / "tas.nc"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(b"short")  # wrong size on an unverifiable file -> re-download
    downloader = _FakeDownloader(content)
    result = download_files(
        [_record()],
        repository=repository,
        download_root=tmp_path,
        downloader=downloader,
    )
    assert result.skipped == 0
    assert result.downloaded == 1
    assert downloader.calls  # it was actually fetched
    assert dest.read_bytes() == content


def test_download_redownloads_checksum_mismatched_present(repository, tmp_path):
    content = b"good-bytes" * 30
    _seed_one(repository, content, ["node1.org"])  # has an md5
    dest = tmp_path / _DRS_DIR / "tas.nc"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(b"corrupt-different")  # fails the checksum -> re-download
    result = download_files(
        [_record()],
        repository=repository,
        download_root=tmp_path,
        downloader=_FakeDownloader(content),
    )
    assert result.skipped == 0
    assert result.downloaded == 1
    assert dest.read_bytes() == content


def test_download_files_reports_versions_with_no_stored_files(repository, tmp_path):
    # A searched version whose files were never stored (Step 2 not run) must be
    # surfaced as `no_files`, not silently ignored.
    record = _record()
    # record_run stores the version; without store_files it has no File rows.
    repository.record_run([record], endpoint_url="u", spec={}, tag="uc")
    downloader = _FakeDownloader()
    result = download_files(
        [record],
        repository=repository,
        download_root=tmp_path,
        downloader=downloader,
    )
    assert result.no_files == [record.instance_key]
    assert result.downloaded == 0
    assert result.failed == []
    assert downloader.calls == []
