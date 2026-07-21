"""Tests for Step 2: one file search per dataset version, stored as File/FileAccess."""

from __future__ import annotations

from cmip_data_manager.esgf.models import DatasetRecord, FileRecord
from cmip_data_manager.esgf.query import FacetQuery
from cmip_data_manager.search.files import add_files

_V = "v20240101"


def _rec(master, *, node="esgf.nci.org.au"):
    """A node-specific dataset record for `master` at the default version."""
    instance = f"{master}.{_V}"
    node_id = f"{instance}|{node}"
    return DatasetRecord(
        id=node_id,
        instance_id=instance,
        master_id=master,
        version=_V,
        data_node=node,
        source_id="M",
        experiment_id="ssp245",
        variant_label="r1",
        variable_id="tas",
        raw={"id": node_id},
    )


def _file(fid, dataset_id, filename, *, host="esgf.nci.org.au", scheme="https"):
    """A file record with a single HTTPServer URL on `host`."""
    url = f"{scheme}://{host}/thredds/fileServer/{filename}"
    return FileRecord(
        id=fid,
        dataset_id=dataset_id,
        title=filename,
        size=10,
        checksum="abc",
        checksum_type="SHA256",
        tracking_id="hdl:1",
        urls=(f"{url}|application/netcdf|HTTPServer",),
        raw={},
    )


class _FakeClient:
    """A client whose `search_files` returns canned files and counts its calls."""

    def __init__(self, files_by_dataset_id):
        self._files = files_by_dataset_id
        self.queries: list[FacetQuery] = []

    def search_files(self, query: FacetQuery) -> list[FileRecord]:
        self.queries.append(query)
        out: list[FileRecord] = []
        for dataset_id in query.dataset_id:
            out.extend(self._files.get(dataset_id, []))
        return out


def _store_versions(repository, records):
    """Persist the versions (Step 1) so Step 2's foreign keys are satisfied."""
    repository.record_run(records, endpoint_url="u", spec={}, tag="uc")


def test_one_search_per_version_stores_files_and_access(repository):
    a, b = _rec("a"), _rec("b")
    _store_versions(repository, [a, b])
    client = _FakeClient(
        {
            a.id: [_file("fa", a.id, "a.nc")],
            b.id: [_file("fb", b.id, "b.nc")],
        }
    )

    result = add_files([a, b], client=client, repository=repository)

    assert len(client.queries) == 2  # one search per version, not one batched query
    assert all(q.type == "File" for q in client.queries)
    assert result.searched == 2
    assert result.files_stored == 2
    # each query targets exactly one version's dataset ids
    assert {q.dataset_id for q in client.queries} == {(a.id,), (b.id,)}


def test_replicas_of_a_file_collapse_to_one_file_many_accesses(repository):
    nci = _rec("a", node="esgf.nci.org.au")
    llnl = _rec("a", node="aims3.llnl.gov")
    _store_versions(repository, [nci, llnl])
    # same filename served from two nodes -> one File, two FileAccess rows
    client = _FakeClient(
        {
            nci.id: [_file("f1", nci.id, "a.nc", host="esgf.nci.org.au")],
            llnl.id: [_file("f2", llnl.id, "a.nc", host="aims3.llnl.gov")],
        }
    )

    result = add_files([nci, llnl], client=client, repository=repository)

    assert len(client.queries) == 1  # one version (two nodes) -> one search
    assert nci.id in client.queries[0].dataset_id
    assert llnl.id in client.queries[0].dataset_id
    assert result.files_stored == 1  # one logical file
    files = repository.get_version_files(nci.instance_key)
    assert len(files) == 1
    assert {a.data_node for a in files[0].accesses} == {
        "esgf.nci.org.au",
        "aims3.llnl.gov",
    }


def test_http_only_mirror_gets_an_https_fsspec_url(repository):
    a = _rec("a")
    _store_versions(repository, [a])
    client = _FakeClient({a.id: [_file("f", a.id, "a.nc", scheme="http")]})

    add_files([a], client=client, repository=repository)

    (file,) = repository.get_version_files(a.instance_key)
    (access,) = file.accesses
    assert access.url.startswith("http://")
    assert access.fsspec_url == access.url.replace("http://", "https://")


def test_already_cached_versions_are_skipped(repository):
    a = _rec("a")
    _store_versions(repository, [a])
    client = _FakeClient({a.id: [_file("f", a.id, "a.nc")]})

    first = add_files([a], client=client, repository=repository)
    second = add_files([a], client=client, repository=repository)

    assert first.searched == 1 and first.skipped_cached == 0
    assert second.searched == 0 and second.skipped_cached == 1
    assert len(client.queries) == 1  # the cached version was not re-searched


def test_restoring_files_updates_in_place(repository):
    a = _rec("a")
    _store_versions(repository, [a])
    client = _FakeClient({a.id: [_file("f", a.id, "a.nc")]})

    add_files([a], client=client, repository=repository)
    # re-run without the cache skip: the file and its access are updated, not doubled
    add_files([a], client=client, repository=repository, skip_cached=False)

    (file,) = repository.get_version_files(a.instance_key)
    assert len(file.accesses) == 1  # the access URL was upserted, not duplicated
    assert len(client.queries) == 2


def test_files_for_an_unstored_version_are_skipped(repository):
    # No Step 1 for this version, so its files cannot be linked and are dropped.
    a = _rec("a")
    client = _FakeClient({a.id: [_file("f", a.id, "a.nc")]})

    result = add_files([a], client=client, repository=repository)

    assert result.searched == 1
    assert result.files_stored == 0  # foreign key unsatisfiable -> skipped
