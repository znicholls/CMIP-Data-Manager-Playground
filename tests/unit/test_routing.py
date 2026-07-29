"""Tests for routing simulations to the data nodes that can serve them."""

from __future__ import annotations

from cmip_data_manager.esgf.models import DatasetRecord, FileRecord
from cmip_data_manager.esgf.routing import (
    SimulationCandidates,
    build_candidates,
    hosts_to_simulations,
    simulation_candidates,
    source_id_affinity,
)


def _dataset(source_id: str, experiment_id: str, variant: str) -> DatasetRecord:
    return DatasetRecord(
        id=f"{source_id}.{experiment_id}.{variant}",
        source_id=source_id,
        experiment_id=experiment_id,
        variant_label=variant,
        variable_id="tas",
        table_id="Amon",
        raw={},
    )


def _file(file_id: str, dataset_id: str, *hosts: str) -> FileRecord:
    urls = tuple(
        f"https://{host}/{file_id}.nc|application/netcdf|HTTPServer" for host in hosts
    )
    return FileRecord(id=file_id, dataset_id=dataset_id, urls=urls, raw={})


def test_source_id_affinity_returns_source_id():
    assert source_id_affinity(_dataset("ACCESS", "ssp245", "r1")) == "ACCESS"


def test_simulation_candidates_ranks_hosts_preferred_first():
    files = [_file("f1", "d1", "ornl"), _file("f2", "d1", "nci")]
    cand = simulation_candidates(
        ("ACCESS", "ssp245", "r1"),
        files,
        group="ACCESS",
        preferred_hosts=("nci",),
    )
    assert cand.hosts == ("nci", "ornl")
    assert cand.urls_by_host["nci"] == ("https://nci/f2.nc",)
    assert cand.urls_by_host["ornl"] == ("https://ornl/f1.nc",)


def test_simulation_candidates_collapses_a_host_to_one_representative_file():
    # Many files (variables/time-chunks) served by the same host collapse to just
    # the first one: one readable file answers the whole simulation, so probing a
    # dead node once per file would only multiply wasted attempts.
    files = [
        _file("tas_2000", "d1", "nci"),
        _file("tas_2001", "d1", "nci"),
        _file("pr_2000", "d1", "nci"),
    ]
    cand = simulation_candidates(("A", "ssp245", "r1"), files, group="A")
    assert cand.hosts == ("nci",)
    assert cand.urls_by_host["nci"] == ("https://nci/tas_2000.nc",)


def test_simulation_candidates_keeps_both_schemes_of_the_representative_file():
    # The https twin and http original of the *same* file are both kept, so the
    # collapse still lets a scheme fall through on one node.
    http_only = FileRecord(
        id="tas_2000",
        dataset_id="d1",
        urls=(
            "http://nci/tas_2000.nc|application/netcdf|HTTPServer",
            "http://nci/tas_2001.nc|application/netcdf|HTTPServer",
        ),
        raw={},
    )
    cand = simulation_candidates(("A", "ssp245", "r1"), [http_only], group="A")
    assert cand.hosts == ("nci",)
    # https twin ranked ahead of its http original; the second file is dropped.
    assert cand.urls_by_host["nci"] == (
        "https://nci/tas_2000.nc",
        "http://nci/tas_2000.nc",
    )


def test_simulation_candidates_empty_when_no_httpserver_mirror():
    only_opendap = FileRecord(
        id="f1",
        dataset_id="d1",
        urls=("https://nci/f1.nc|application/x-netcdf|OPENDAP",),
        raw={},
    )
    cand = simulation_candidates(("A", "ssp245", "r1"), [only_opendap], group="A")
    assert cand.hosts == ()
    assert dict(cand.urls_by_host) == {}


def test_build_candidates_derives_group_and_keeps_unservable_simulation():
    sim_a = ("ACCESS", "ssp245", "r1")
    sim_b = ("CanESM5", "ssp245", "r1")  # no files -> unservable
    records = {
        sim_a: [_dataset("ACCESS", "ssp245", "r1")],
        sim_b: [_dataset("CanESM5", "ssp245", "r1")],
    }
    files = {sim_a: [_file("f1", "dA", "nci")]}

    candidates = build_candidates(records, files)

    assert candidates[sim_a].group == "ACCESS"
    assert candidates[sim_a].hosts == ("nci",)
    # Still present, but with nowhere to read it from.
    assert candidates[sim_b].hosts == ()


def test_build_candidates_honours_a_custom_affinity_key():
    sim = ("ACCESS", "ssp245", "r1")
    records = {sim: [_dataset("ACCESS", "ssp245", "r1")]}
    files = {sim: [_file("f1", "dA", "nci")]}

    candidates = build_candidates(
        records, files, affinity_key=lambda record: record.experiment_id
    )
    assert candidates[sim].group == "ssp245"


def test_hosts_to_simulations_inverts_and_clusters_by_affinity():
    # Two ACCESS simulations and one CanESM5, all on nci; ACCESS-only on ornl.
    a1 = SimulationCandidates(
        ("ACCESS", "ssp245", "r1"), "ACCESS", ("nci", "ornl"), {"nci": (), "ornl": ()}
    )
    a2 = SimulationCandidates(
        ("ACCESS", "historical", "r1"), "ACCESS", ("nci",), {"nci": ()}
    )
    c1 = SimulationCandidates(
        ("CanESM5", "ssp245", "r1"), "CanESM5", ("nci",), {"nci": ()}
    )
    queues = hosts_to_simulations(
        {a1.simulation: a1, a2.simulation: a2, c1.simulation: c1}
    )

    # The two ACCESS simulations are adjacent (clustered), then CanESM5.
    assert queues["nci"] == [
        ("ACCESS", "historical", "r1"),
        ("ACCESS", "ssp245", "r1"),
        ("CanESM5", "ssp245", "r1"),
    ]
    # ornl only serves the ACCESS simulation that listed it.
    assert queues["ornl"] == [("ACCESS", "ssp245", "r1")]


def test_hosts_to_simulations_omits_hosts_with_no_work():
    unservable = SimulationCandidates(("A", "ssp245", "r1"), "A", (), {})
    assert hosts_to_simulations({unservable.simulation: unservable}) == {}
