"""Tests for Step 3: read one header per simulation, promote onto its versions."""

from __future__ import annotations

import pytest

from cmip_data_manager.esgf.headers import HeaderMetadata
from cmip_data_manager.esgf.health import NodeHealth, ReadOutcome
from cmip_data_manager.esgf.models import DatasetRecord, FileRecord
from cmip_data_manager.esgf.preflight import ProbeCache, ProbeOutcome
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
    supports_file_search = True

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


def test_a_new_variable_reuses_a_prior_runs_header(repository):
    # Cross-run reuse (the key gap): an earlier run read + promoted the header for
    # `tas`.  A LATER run that searches only `rsut` — a different variable of the SAME
    # simulation, whose version did not even exist during the first run — must copy the
    # stored header (header-only metadata is variable-independent) rather than re-read.
    tas = _rec("tas")
    _setup(repository, [tas])
    reader = _reader_for([tas])
    enrich_version_headers(
        [tas], repository=repository, reader=reader, use_timeout=False
    )
    assert len(reader.calls) == 1

    # A separate, later run brings in only rsut (its version/files did not exist yet).
    rsut = _rec("rsut")
    _setup(repository, [rsut])
    result = enrich_version_headers(
        [rsut], repository=repository, reader=reader, use_timeout=False
    )

    assert result.reused == 1
    assert result.read == 0
    assert len(reader.calls) == 1  # no data-node read: copied from the earlier run
    header = repository.version_header(rsut.instance_key)
    assert header is not None
    assert header.get("parent_experiment_id") == "historical"


def test_simulation_header_looks_up_a_stored_header_across_variables(repository):
    # The simulation-grain lookup underpinning cross-run reuse: a header stored for one
    # variable is found by (source_id, experiment_id, variant_label), for any variable.
    tas, rsut = _rec("tas"), _rec("rsut")
    _setup(repository, [tas, rsut])
    reader = _reader_for([tas])
    enrich_version_headers(
        [tas], repository=repository, reader=reader, use_timeout=False
    )

    found = repository.simulation_header("M", "ssp245", "r1")
    assert found is not None
    header, file_id = found
    assert header.get("parent_experiment_id") == "historical"
    assert file_id == repository.version_header_file_id(tas.instance_key)
    # Absent simulation -> None.
    assert repository.simulation_header("M", "ssp245", "rZZ") is None


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


# --- save-as-you-go persistence ---------------------------------------------

_KILL = "https://esgf.nci.org.au/thredds/fileServer/M5_tas.nc"


def _rec_for(source_id):
    """A single-variable (tas) record for a distinct model `source_id`."""
    master = f"{source_id}.ssp245.r1.tas"
    instance = f"{master}.{_V}"
    node_id = f"{instance}|{_NODE}"
    return DatasetRecord(
        id=node_id,
        instance_id=instance,
        master_id=master,
        version=_V,
        data_node=_NODE,
        source_id=source_id,
        experiment_id="ssp245",
        variant_label="r1",
        variable_id="tas",
        raw={"id": node_id},
    )


class _KillReader:
    """Reads canned headers, but raises `KeyboardInterrupt` (a kill) at `kill_url`."""

    def __init__(self, kill_url):
        self._kill = kill_url
        self.calls: list[str] = []

    def __call__(self, url: str) -> HeaderMetadata:
        self.calls.append(url)
        if url == self._kill:
            raise KeyboardInterrupt("simulated kill")
        return HeaderMetadata(attrs=dict(_ATTRS), source_url=url)


def _multi_setup(repository, records):
    """Step 1 + Step 2 for several distinct-model simulations (one file each)."""
    repository.record_run(records, endpoint_url="u", spec={}, tag="uc")
    files = {r.id: [_file(r.id, f"{r.source_id}_{r.variable_id}.nc")] for r in records}
    add_files(records, clients=(_FileClient(files),), repository=repository)


def test_headers_persist_as_each_read_completes(repository, monkeypatch):
    # Five models processed in sorted order under one worker; the 5th read is killed.
    # Drop the health-save throttle so every completed read flushes health too.
    monkeypatch.setattr(
        "cmip_data_manager.search.version_headers._HEALTH_SAVE_INTERVAL", 0.0
    )
    recs = [_rec_for(f"M{i}") for i in range(1, 6)]
    _multi_setup(repository, recs)
    reader = _KillReader(_KILL)

    with pytest.raises(KeyboardInterrupt):
        enrich_version_headers(
            recs,
            repository=repository,
            reader=reader,
            use_timeout=False,
            max_workers=1,
            node_concurrency=1,
        )

    # Every header read before the kill is already committed; the kill sim is not.
    for rec in recs[:4]:
        assert repository.version_header(rec.instance_key) is not None
    assert repository.version_header(recs[4].instance_key) is None
    # Node health was flushed incrementally too, so a kill does not lose it.
    assert repository.load_node_health().stat(_NODE).successes >= 4


def test_legacy_batch_path_persists_nothing_on_a_mid_run_kill(repository):
    recs = [_rec_for(f"M{i}") for i in range(1, 6)]
    _multi_setup(repository, recs)
    reader = _KillReader(_KILL)

    with pytest.raises(KeyboardInterrupt):
        enrich_version_headers(
            recs,
            repository=repository,
            reader=reader,
            use_timeout=False,
            max_workers=1,
            node_concurrency=1,
            persist_as_you_go=False,
        )

    # Legacy path writes only after the batch returns, so the kill loses everything.
    for rec in recs[:4]:
        assert repository.version_header(rec.instance_key) is None


def test_legacy_batch_path_persists_after_a_clean_run(repository):
    tas, rsut = _rec("tas"), _rec("rsut")
    _setup(repository, [tas, rsut])
    reader = _reader_for([tas, rsut])

    result = enrich_version_headers(
        [tas, rsut],
        repository=repository,
        reader=reader,
        use_timeout=False,
        persist_as_you_go=False,
    )

    # A clean legacy run persists the header and the whole attempt log at the end.
    assert result.read == 1
    assert result.promoted == 2
    assert repository.version_header(tas.instance_key) is not None
    assert repository.get_header_attempts(source_id="M")  # attempt log written


# --- pre-flight node probe integration --------------------------------------
def _seeded_cache(alive: bool):
    """A ProbeCache with a verdict already recorded for `_NODE` (no network probe)."""
    cache = ProbeCache()
    cache.record(
        ProbeOutcome(
            host=_NODE,
            alive=alive,
            reason=None if alive else "black hole",
            seconds=0.5 if alive else 90.0,
            url=f"https://{_NODE}/f.nc",
            attempts=1,
        )
    )
    return cache


def test_preflight_probe_excludes_a_dead_node(repository):
    # The only mirror sits on a node the probe found dead: the sim is dropped to
    # no_files and the header read is never attempted.
    tas = _rec("tas")
    _setup(repository, [tas])
    reader = _reader_for([tas])

    result = enrich_version_headers(
        [tas],
        repository=repository,
        reader=reader,
        use_timeout=False,
        preflight_probe=True,
        probe_cache=_seeded_cache(alive=False),
    )

    assert result.read == 0
    assert ("M", "ssp245", "r1") in result.no_files
    assert reader.calls == []  # dead node was excluded before any read


def test_preflight_probe_keeps_an_alive_node(repository):
    tas = _rec("tas")
    _setup(repository, [tas])
    reader = _reader_for([tas])

    result = enrich_version_headers(
        [tas],
        repository=repository,
        reader=reader,
        use_timeout=False,
        preflight_probe=True,
        probe_cache=_seeded_cache(alive=True),
    )

    assert result.read == 1
    assert len(reader.calls) == 1  # probe-alive node was read as normal


def test_force_alive_hosts_overrides_the_probe(repository):
    # No cache seed, so `_NODE` is unprobed; `force_alive_hosts` records it alive
    # WITHOUT any network probe, so the read proceeds.
    tas = _rec("tas")
    _setup(repository, [tas])
    reader = _reader_for([tas])

    result = enrich_version_headers(
        [tas],
        repository=repository,
        reader=reader,
        use_timeout=False,
        preflight_probe=True,
        force_alive_hosts=frozenset({_NODE}),
    )

    assert result.read == 1
    assert len(reader.calls) == 1


def test_preflight_probe_overrides_stale_unreliable_health(repository):
    # Persisted health condemns `_NODE` (3 failures, 0 successes), but a live probe
    # finds it alive today: with the probe on, the stale exclusion is dropped and the
    # node is read.
    tas = _rec("tas")
    _setup(repository, [tas])
    reader = _reader_for([tas])

    health = NodeHealth()
    for _ in range(3):
        health.record(f"https://{_NODE}/f.nc", ReadOutcome.TIMEOUT, 90.0)
    assert _NODE in health.unreliable_hosts()

    result = enrich_version_headers(
        [tas],
        repository=repository,
        reader=reader,
        health=health,
        use_timeout=False,
        avoid_unreliable_hosts=True,
        preflight_probe=True,
        probe_cache=_seeded_cache(alive=True),
    )

    assert result.read == 1  # probe (today) beat stale history
    assert len(reader.calls) == 1


def test_stale_unreliable_health_still_excludes_without_the_probe(repository):
    # The contrast: with the probe OFF, persisted "unreliable" health still excludes
    # the node (existing behaviour preserved).
    tas = _rec("tas")
    _setup(repository, [tas])
    reader = _reader_for([tas])

    health = NodeHealth()
    for _ in range(3):
        health.record(f"https://{_NODE}/f.nc", ReadOutcome.TIMEOUT, 90.0)

    result = enrich_version_headers(
        [tas],
        repository=repository,
        reader=reader,
        health=health,
        use_timeout=False,
        avoid_unreliable_hosts=True,
        preflight_probe=False,
    )

    assert result.read == 0
    assert ("M", "ssp245", "r1") in result.no_files
    assert reader.calls == []
