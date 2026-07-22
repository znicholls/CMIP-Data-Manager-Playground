"""Tests for Step 3: read one header per simulation, promote onto its versions."""

from __future__ import annotations

from cmip_data_manager.esgf.headers import HeaderMetadata
from cmip_data_manager.esgf.models import DatasetRecord, FileRecord
from cmip_data_manager.esgf.query import FacetQuery
from cmip_data_manager.search.files import add_files
from cmip_data_manager.search.version_headers import enrich_version_headers

_V = "v20240101"
_NODE = "esgf.nci.org.au"
_ATTRS = {
    "parent_source_id": "M",
    "parent_experiment_id": "historical",
    "parent_variant_label": "r1",
    "branch_time_in_parent": "0.0",
}


def _rec(variable):
    """A tas/rsut record for one shared simulation (M, ssp245, r1)."""
    master = f"M.ssp245.r1.{variable}"
    instance = f"{master}.{_V}"
    node_id = f"{instance}|{_NODE}"
    return DatasetRecord(
        id=node_id,
        instance_id=instance,
        master_id=master,
        version=_V,
        data_node=_NODE,
        source_id="M",
        experiment_id="ssp245",
        variant_label="r1",
        variable_id=variable,
        raw={"id": node_id},
    )


def _file(dataset_id, filename):
    url = f"https://{_NODE}/thredds/fileServer/{filename}"
    return FileRecord(
        id=f"file-{filename}",
        dataset_id=dataset_id,
        title=filename,
        urls=(f"{url}|application/netcdf|HTTPServer",),
        raw={},
    )


class _FileClient:
    def __init__(self, files_by_dataset_id):
        self._files = files_by_dataset_id

    @property
    def base_url(self) -> str:
        return "https://index/search"

    def search_files(self, query: FacetQuery) -> list[FileRecord]:
        out: list[FileRecord] = []
        for dataset_id in query.dataset_id:
            out.extend(self._files.get(dataset_id, []))
        return out


class _Reader:
    """A fake header reader that returns canned attrs and counts its calls."""

    def __init__(self, attrs_by_url):
        self._attrs = attrs_by_url
        self.calls: list[str] = []

    def __call__(self, url: str) -> HeaderMetadata:
        self.calls.append(url)
        if url not in self._attrs:
            raise OSError(f"no header at {url}")
        return HeaderMetadata(attrs=self._attrs[url], source_url=url)


def _setup(repository, records):
    """Run Step 1 (versions) and Step 2 (files) so Step 3 has stored candidates."""
    repository.record_run(records, endpoint_url="u", spec={}, tag="uc")
    files = {r.id: [_file(r.id, f"{r.variable_id}.nc")] for r in records}
    add_files(records, clients=(_FileClient(files),), repository=repository)


def _reader_for(records):
    urls = {
        f"https://{_NODE}/thredds/fileServer/{r.variable_id}.nc": dict(_ATTRS)
        for r in records
    }
    return _Reader(urls)


def test_one_read_per_simulation_promotes_to_all_versions(repository):
    tas, rsut = _rec("tas"), _rec("rsut")
    _setup(repository, [tas, rsut])
    reader = _reader_for([tas, rsut])

    result = enrich_version_headers(
        [tas, rsut], repository=repository, reader=reader, use_timeout=False
    )

    assert len(reader.calls) == 1  # one header serves the whole simulation
    assert result.read == 1
    assert result.promoted == 2  # both tas and rsut versions
    # both versions carry the promoted parent metadata
    for rec in (tas, rsut):
        header = repository.version_header(rec.instance_key)
        assert header is not None
        assert header.get("parent_experiment_id") == "historical"


def test_header_is_stored_on_a_file_and_pointed_to(repository):
    tas = _rec("tas")
    _setup(repository, [tas])
    reader = _reader_for([tas])

    enrich_version_headers(
        [tas], repository=repository, reader=reader, use_timeout=False
    )

    (file,) = repository.get_version_files(tas.instance_key)
    assert file.header_attrs_json is not None  # header saved on the file
    assert repository.version_header_file_id(tas.instance_key) == file.id


def test_already_promoted_simulations_are_skipped(repository):
    tas, rsut = _rec("tas"), _rec("rsut")
    _setup(repository, [tas, rsut])
    reader = _reader_for([tas, rsut])

    enrich_version_headers(
        [tas, rsut], repository=repository, reader=reader, use_timeout=False
    )
    again = enrich_version_headers(
        [tas, rsut], repository=repository, reader=reader, use_timeout=False
    )

    assert again.skipped_cached == 1  # the simulation was fully cached
    assert again.read == 0
    assert len(reader.calls) == 1  # no second read


def test_a_new_sibling_version_reuses_an_existing_header(repository):
    tas, rsut = _rec("tas"), _rec("rsut")
    _setup(repository, [tas, rsut])
    reader = _reader_for([tas, rsut])

    # First pass enriches only tas (reads + promotes tas).
    enrich_version_headers(
        [tas], repository=repository, reader=reader, use_timeout=False
    )
    assert len(reader.calls) == 1

    # Second pass sees the whole simulation: tas is done, rsut is copied from it.
    result = enrich_version_headers(
        [tas, rsut], repository=repository, reader=reader, use_timeout=False
    )
    assert result.reused == 1
    assert result.read == 0
    assert len(reader.calls) == 1  # rsut was NOT read; it reused tas's header
    assert repository.version_header(rsut.instance_key) is not None


def test_node_health_persists_after_reads(repository):
    tas = _rec("tas")
    _setup(repository, [tas])
    reader = _reader_for([tas])

    enrich_version_headers(
        [tas], repository=repository, reader=reader, use_timeout=False
    )

    stat = repository.load_node_health().stat(_NODE)
    assert stat is not None
    assert stat.successes >= 1  # the successful read was recorded and saved


def test_simulation_without_stored_files_is_reported(repository):
    tas = _rec("tas")
    # Step 1 only: no add_files, so there are no stored file mirrors.
    repository.record_run([tas], endpoint_url="u", spec={}, tag="uc")
    reader = _reader_for([tas])

    result = enrich_version_headers(
        [tas], repository=repository, reader=reader, use_timeout=False
    )

    assert result.read == 0
    assert ("M", "ssp245", "r1") in result.no_files
    assert reader.calls == []  # nothing to read
