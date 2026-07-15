"""Tests for the end-to-end header-enrichment pipeline."""

from __future__ import annotations

from urllib.parse import urlparse

from cmip_data_manager.esgf.headers import HeaderMetadata, header_key
from cmip_data_manager.esgf.health import AttemptLog, NodeHealth, NodeStat, ReadOutcome
from cmip_data_manager.esgf.models import DatasetRecord, FileRecord
from cmip_data_manager.esgf.routing import SimulationCandidates
from cmip_data_manager.search.headers import (
    _attempt_rows,
    _seed_concurrency,
    enrich_headers,
)


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


def test_enrich_persists_learned_concurrency(repository):
    ds = _ds("d0")
    client = FakeClient({"d0": [_file("f0", "d0", "https://nci/tas.nc")]})

    enrich_headers([ds], client=client, repository=repository, **_kw())

    stat = repository.load_node_health().stat("nci")
    assert stat.last_concurrency == 2  # the default cap; one read, no growth
    assert stat.max_safe_concurrency == 1  # one concurrent read ran cleanly


def test_seed_concurrency_precedence():
    health = NodeHealth()
    health.restore(NodeStat(host="learned", last_concurrency=5))
    health.restore(NodeStat(host="huge", last_concurrency=20))
    seed = _seed_concurrency(health, default=2, overrides={"pinned": 7}, ceiling=8)
    assert seed("pinned") == 7  # explicit override wins
    assert seed("learned") == 5  # else the learned cap
    assert seed("huge") == 8  # ...capped by the ceiling
    assert seed("fresh") == 2  # else the conservative default


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


def test_enrich_logs_successful_attempt_with_full_provenance(repository):
    ds = _ds("d0")
    file = FileRecord(
        id="f0",
        dataset_id="d0",
        variable_id="tas",
        urls=("https://nci/tas.nc|application/netcdf|HTTPServer",),
        raw={},
    )
    client = FakeClient({"d0": [file]})

    enrich_headers([ds], client=client, repository=repository, **_kw())

    attempts = repository.get_header_attempts()
    assert len(attempts) == 1
    a = attempts[0]
    assert (a.host, a.outcome, a.url) == ("nci", "success", "https://nci/tas.nc")
    assert (a.source_id, a.experiment_id, a.variant_label) == (
        "ACCESS-ESM1-5",
        "ssp245",
        "r1i1p1f1",
    )
    assert a.variable_id == "tas" and a.table_id == "Amon"


def test_enrich_logs_every_host_tried_when_spilling(repository):
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
        [ds],
        client=client,
        repository=repository,
        reader=_reader(fail_hosts=frozenset({"dead"})),
        use_timeout=False,
        max_attempts=1,
        preferred_hosts=("dead",),  # force the dead host to be tried first
    )

    assert result.read == 1  # spilled to "good"
    tried = {(a.host, a.outcome) for a in repository.get_header_attempts()}
    assert ("dead", "error") in tried  # the failed node is logged...
    assert ("good", "success") in tried  # ...as well as the one that served it


def test_enrich_logs_no_candidate_marker_for_unservable_sim(repository):
    ds = _ds("d0")
    client = FakeClient({"d0": []})  # index lists no mirror

    result = enrich_headers([ds], client=client, repository=repository, **_kw())

    assert result.failed == [("ACCESS-ESM1-5", "ssp245", "r1i1p1f1")]
    attempts = repository.get_header_attempts()
    assert len(attempts) == 1
    assert attempts[0].outcome == "no_candidate"
    assert attempts[0].host is None and attempts[0].url is None


def test_enrich_can_disable_attempt_logging(repository):
    ds = _ds("d0")
    client = FakeClient({"d0": [_file("f0", "d0", "https://nci/tas.nc")]})

    enrich_headers(
        [ds], client=client, repository=repository, record_attempts=False, **_kw()
    )

    assert repository.get_header_attempts() == []


def test_reprobe_toggle_controls_retry_of_condemned_hosts(repository):
    # Seed health so "revived" looks dead (3 attempts, no success).
    health = repository.load_node_health()
    for _ in range(3):
        health.record("https://revived/x.nc", ReadOutcome.TIMEOUT, 90.0)
    repository.save_node_health(health)

    ds = _ds("d0")
    client = FakeClient({"d0": [_file("f0", "d0", "https://revived/tas.nc")]})

    # Default (avoid): the condemned host is skipped -> the simulation is unservable.
    avoided = enrich_headers([ds], client=client, repository=repository, **_kw())
    assert avoided.read == 0
    assert avoided.failed == [("ACCESS-ESM1-5", "ssp245", "r1i1p1f1")]

    # Re-probe: try the condemned host anyway -> it now serves the header.
    reprobed = enrich_headers(
        [ds],
        client=client,
        repository=repository,
        avoid_unreliable_hosts=False,
        skip_cached=False,
        **_kw(),
    )
    assert reprobed.read == 1
    assert repository.get_header(header_key(ds)) is not None


def test_enrich_reads_over_https_twin_when_index_lists_http_only(repository):
    # A replica indexed only as http: the synthesised https twin is tried first and
    # serves the header, so no http attempt is even made.
    file = FileRecord(
        id="f0",
        dataset_id="d0",
        variable_id="tas",
        urls=("http://ucar/tas.nc|application/netcdf|HTTPServer",),
        raw={},
    )
    client = FakeClient({"d0": [file]})

    def reader(url: str) -> HeaderMetadata:
        if url.startswith("http://"):
            raise OSError("http range refused")
        return HeaderMetadata(attrs={"served_by": "ucar"}, source_url=url)

    result = enrich_headers(
        [_ds("d0")],
        client=client,
        repository=repository,
        reader=reader,
        use_timeout=False,
        max_attempts=1,
    )

    assert result.read == 1
    attempts = repository.get_header_attempts()
    assert len(attempts) == 1  # https succeeded first; http never tried
    a = attempts[0]
    assert a.host == "ucar" and a.url.startswith("https://") and a.outcome == "success"
    assert a.variable_id == "tas" and a.table_id == "Amon"  # twin joins to the file


def test_enrich_falls_back_to_http_when_host_serves_only_http(repository):
    # A node that genuinely serves only http (its https twin fails): the twin is
    # tried first, then the original http URL on the same host serves the header.
    file = FileRecord(
        id="f0",
        dataset_id="d0",
        variable_id="tas",
        urls=("http://bcc/tas.nc|application/netcdf|HTTPServer",),
        raw={},
    )
    client = FakeClient({"d0": [file]})

    def reader(url: str) -> HeaderMetadata:
        if url.startswith("https://"):
            raise OSError("no https listener")
        return HeaderMetadata(attrs={"served_by": "bcc"}, source_url=url)

    result = enrich_headers(
        [_ds("d0")],
        client=client,
        repository=repository,
        reader=reader,
        use_timeout=False,
        max_attempts=1,
    )

    assert result.read == 1  # recovered via the http fallback on the same host
    tried = {
        (urlparse(a.url).scheme, a.outcome) for a in repository.get_header_attempts()
    }
    assert ("https", "error") in tried  # the twin was tried first...
    assert ("http", "success") in tried  # ...then the http original served it


def test_attempt_rows_marks_stranded_failure_that_had_a_mirror():
    # A simulation that had a candidate host but produced no attempt (its mirror was
    # evicted before dispatch) must still be logged — as "stranded", not silently.
    sim = ("CanESM5-1", "ssp245", "r1i1p1f1")
    cand = SimulationCandidates(
        simulation=sim,
        group="CanESM5-1",
        hosts=("crd-esgf-drc.ec.gc.ca",),
        urls_by_host={"crd-esgf-drc.ec.gc.ca": ("https://crd-esgf-drc.ec.gc.ca/x.nc",)},
    )
    rows = _attempt_rows(
        AttemptLog(),  # no reads happened
        candidates={sim: cand},
        files_by_sim={},
        records_by_sim={},
        failed=[sim],
    )
    assert len(rows) == 1
    assert rows[0].outcome == "stranded"
    assert rows[0].host == "crd-esgf-drc.ec.gc.ca"
    assert rows[0].url == "https://crd-esgf-drc.ec.gc.ca/x.nc"


def test_attempt_rows_marks_no_candidate_when_no_mirror_at_all():
    sim = ("MIROC-ES2H", "ssp245", "r1i1p4f2")
    cand = SimulationCandidates(
        simulation=sim, group="MIROC-ES2H", hosts=(), urls_by_host={}
    )
    rows = _attempt_rows(
        AttemptLog(),
        candidates={sim: cand},
        files_by_sim={},
        records_by_sim={},
        failed=[sim],
    )
    assert [r.outcome for r in rows] == ["no_candidate"]
    assert rows[0].host is None


def test_attempt_rows_does_not_double_mark_a_failure_with_a_trail():
    # A failure that already has attempt rows (it was tried and errored) gets no
    # extra stranded/no_candidate marker.
    sim = ("X", "ssp245", "r1i1p1f1")
    cand = SimulationCandidates(
        simulation=sim,
        group="X",
        hosts=("dead",),
        urls_by_host={"dead": ("https://dead/x.nc",)},
    )
    log = AttemptLog()
    log.add("https://dead/x.nc", ReadOutcome.ERROR, 2.0)
    rows = _attempt_rows(
        log,
        candidates={sim: cand},
        files_by_sim={},
        records_by_sim={},
        failed=[sim],
    )
    assert [r.outcome for r in rows] == ["error"]  # no duplicate marker
