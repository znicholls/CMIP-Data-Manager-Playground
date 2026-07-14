"""Tests for the end-to-end header-enrichment pipeline."""

from __future__ import annotations

from urllib.parse import urlparse

from cmip_data_manager.esgf.headers import HeaderMetadata, header_key
from cmip_data_manager.esgf.health import NodeHealth, ReadOutcome
from cmip_data_manager.esgf.models import DatasetRecord, FileRecord
from cmip_data_manager.search.headers import enrich_headers


class FakeClient:
    """A stand-in search client returning canned files per dataset id."""

    def __init__(self, files_by_dataset: dict[str, list[FileRecord]]) -> None:
        self.files_by_dataset = files_by_dataset
        self.calls = 0

    def search_files(self, query) -> list[FileRecord]:
        self.calls += 1
        out: list[FileRecord] = []
        for dsid in query.dataset_id:
            out.extend(self.files_by_dataset.get(dsid, []))
        return out


def _reader(fail_hosts: frozenset[str] = frozenset()):
    def reader(url: str) -> HeaderMetadata:
        host = urlparse(url).hostname or ""
        if host in fail_hosts:
            raise OSError(f"{host} refused")
        return HeaderMetadata(
            attrs={"parent_experiment_id": "historical", "served_by": host},
            source_url=url,
        )

    return reader


def _ds(dsid: str, *, variable="tas", table="Amon", variant="r1i1p1f1"):
    return DatasetRecord(
        id=dsid,
        source_id="ACCESS-ESM1-5",
        experiment_id="ssp245",
        variant_label=variant,
        variable_id=variable,
        table_id=table,
        raw={},
    )


def _file(fid: str, dsid: str, url: str) -> FileRecord:
    return FileRecord(
        id=fid, dataset_id=dsid, urls=(f"{url}|application/netcdf|HTTPServer",), raw={}
    )


def _kw(**overrides):
    """Default enrich kwargs for tests: no subprocess, injected reader."""
    kwargs = {"reader": _reader(), "use_timeout": False}
    kwargs.update(overrides)
    return kwargs


def test_enrich_reads_stores_and_records_health(repository):
    ds = _ds("d0")
    client = FakeClient({"d0": [_file("f0", "d0", "https://nci/tas.nc")]})

    result = enrich_headers([ds], client=client, repository=repository, **_kw())

    assert result.read == 1
    assert result.stored == 1
    assert result.failed == []
    stored = repository.get_header(header_key(ds))
    assert stored is not None
    assert stored.get("parent_experiment_id") == "historical"
    # Health was recorded and persisted for the serving node.
    assert repository.load_node_health().stat("nci").successes == 1


def test_enrich_reads_one_header_per_simulation(repository):
    tas, rsut = _ds("d_tas", variable="tas"), _ds("d_rsut", variable="rsut")
    client = FakeClient(
        {
            "d_tas": [_file("f0", "d_tas", "https://nci/tas.nc")],
            "d_rsut": [_file("f1", "d_rsut", "https://nci/rsut.nc")],
        }
    )

    result = enrich_headers([tas, rsut], client=client, repository=repository, **_kw())

    assert result.read == 1  # one read for the whole simulation
    assert result.stored == 2  # ...stored under both variable keys
    assert repository.get_header(header_key(tas)) is not None
    assert repository.get_header(header_key(rsut)) is not None


def test_enrich_skips_already_cached(repository):
    ds = _ds("d0")
    repository.store_headers({header_key(ds): HeaderMetadata(attrs={"x": "1"})})
    client = FakeClient({"d0": [_file("f0", "d0", "https://nci/tas.nc")]})

    result = enrich_headers([ds], client=client, repository=repository, **_kw())

    assert result.skipped_cached == 1
    assert result.read == 0
    assert client.calls == 0  # nothing to look up


def test_enrich_reuses_sibling_variable_without_reading(repository):
    tas = _ds("d_tas", variable="tas")
    repository.store_headers(
        {header_key(tas): HeaderMetadata(attrs={"parent_experiment_id": "historical"})}
    )
    rsut = _ds("d_rsut", variable="rsut")  # same simulation, new variable
    client = FakeClient({"d_rsut": [_file("f1", "d_rsut", "https://nci/rsut.nc")]})

    result = enrich_headers([rsut], client=client, repository=repository, **_kw())

    assert result.reused == 1
    assert result.read == 0
    assert client.calls == 0  # reused the sibling header; no file lookup or read
    assert repository.get_header(header_key(rsut)).get("parent_experiment_id") == (
        "historical"
    )


def test_enrich_records_failed_simulation(repository):
    ds = _ds("d0")
    client = FakeClient({"d0": [_file("f0", "d0", "https://dead/tas.nc")]})

    result = enrich_headers(
        [ds],
        client=client,
        repository=repository,
        reader=_reader(fail_hosts=frozenset({"dead"})),
        use_timeout=False,
        max_attempts=1,
    )

    assert result.read == 0
    assert result.stored == 0
    assert result.failed == [("ACCESS-ESM1-5", "ssp245", "r1i1p1f1")]
    assert repository.load_node_health().stat("dead").errors == 1


def test_enrich_batches_file_lookups(repository):
    sims = [
        _ds("d1", variant="r1i1p1f1"),
        _ds("d2", variant="r2i1p1f1"),
        _ds("d3", variant="r3i1p1f1"),
    ]
    client = FakeClient(
        {ds.id: [_file(f"f{ds.id}", ds.id, f"https://nci/{ds.id}.nc")] for ds in sims}
    )

    result = enrich_headers(
        sims, client=client, repository=repository, file_lookup_batch=2, **_kw()
    )

    assert result.read == 3  # three distinct simulations, three reads
    assert client.calls == 2  # 3 datasets batched by 2 -> two requests, not three
    for ds in sims:
        assert repository.get_header(header_key(ds)) is not None


def test_enrich_batches_by_char_budget(repository):
    # Long ids force a split by the character budget before the count cap is hit.
    sims = [_ds("X" * 100 + str(i), variant=f"r{i}i1p1f1") for i in range(4)]
    client = FakeClient(
        {
            ds.id: [_file(f"f{i}", ds.id, f"https://nci/{i}.nc")]
            for i, ds in enumerate(sims)
        }
    )

    result = enrich_headers(
        sims,
        client=client,
        repository=repository,
        file_lookup_batch=50,  # count cap won't trigger
        file_lookup_max_chars=250,  # ~2 ids of ~100 chars per request
        **_kw(),
    )

    assert result.read == 4
    assert client.calls == 2  # 4 long ids, ~2 per char-budgeted request


def test_enrich_honours_static_ignore_hosts(repository):
    ds = _ds("d0")
    client = FakeClient(
        {
            "d0": [
                _file("f0", "d0", "https://blocked/tas.nc"),
                _file("f1", "d0", "https://good/tas.nc"),
            ]
        }
    )

    result = enrich_headers(
        [ds],
        client=client,
        repository=repository,
        ignore_hosts=frozenset({"blocked"}),  # skipped on a cold run, no health yet
        **_kw(),
    )

    assert result.read == 1
    assert repository.get_header(header_key(ds)).get("served_by") == "good"


def test_enrich_ignores_unreliable_hosts_from_health(repository):
    health = NodeHealth()
    for _ in range(3):  # condemn "dead" so it is excluded from candidates
        health.record("https://dead/x.nc", ReadOutcome.ERROR, 1.0)
    ds = _ds("d0")
    client = FakeClient(
        {
            "d0": [
                _file("f0", "d0", "https://dead/tas.nc"),
                _file("f1", "d0", "https://good/tas.nc"),
            ]
        }
    )

    result = enrich_headers(
        [ds], client=client, repository=repository, health=health, **_kw()
    )

    assert result.read == 1
    # The read landed on the good node, not the condemned one.
    assert repository.get_header(header_key(ds)).get("served_by") == "good"
