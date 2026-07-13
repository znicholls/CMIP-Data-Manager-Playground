"""Tests for the batched, cross-cell parent-link resolver."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from cmip_data_manager.esgf.client import ESGFSearchClient
from cmip_data_manager.esgf.models import DatasetRecord
from cmip_data_manager.esgf.parents import ParentInfo
from cmip_data_manager.search import build_cells, parent_cell_of, resolve_parent_links

NEEDED = ("tas", "rsdt")


def _rec(
    dataset_id: str, variant: str, experiment: str, variable: str
) -> DatasetRecord:
    return DatasetRecord(
        id=dataset_id,
        source_id="M",
        variant_label=variant,
        experiment_id=experiment,
        variable_id=variable,
        raw={},
    )


def _file_doc(dataset_id: str, url: str, variable: str) -> dict[str, Any]:
    return {
        "id": f"file.{dataset_id}",
        "dataset_id": dataset_id,
        "variable_id": [variable],
        "url": [f"{url}|application/netcdf|HTTPServer"],
    }


def _client_with_files(files_by_dataset: dict[str, list[dict[str, Any]]]):
    def fetch(_url: str, params: Mapping[str, str]) -> dict[str, Any]:
        if params.get("type") != "File":
            return {"response": {"numFound": 0, "docs": []}}
        wanted = set(params["dataset_id"].split(","))
        docs = [
            doc
            for ds_id, ds_docs in files_by_dataset.items()
            if ds_id in wanted
            for doc in ds_docs
        ]
        offset, limit = int(params["offset"]), int(params["limit"])
        return {
            "response": {"numFound": len(docs), "docs": docs[offset : offset + limit]}
        }

    return ESGFSearchClient("https://example/search", fetch=fetch)


def test_parent_cell_of_uses_fallback_source_id():
    info = ParentInfo(variant_label="r0", experiment_id="piControl")
    assert parent_cell_of(info, "M") == ("M", "r0", "piControl")


def test_parent_cell_of_none_when_no_parent():
    assert parent_cell_of(ParentInfo(), "M") is None


def _single_hop_records() -> list[DatasetRecord]:
    # abrupt-4xCO2 under r3 (near-miss); piControl under a different variant r1.
    records = []
    for var in NEEDED:
        records.append(_rec(f"ab.{var}", "r3", "abrupt-4xCO2", var))
        records.append(_rec(f"pi.{var}", "r1", "piControl", var))
    return records


def _single_hop_files() -> dict[str, list[dict[str, Any]]]:
    return {
        f"ab.{var}": [_file_doc(f"ab.{var}", f"http://h/{var}", var)] for var in NEEDED
    }


def test_resolves_near_miss_to_parent():
    records = _single_hop_records()
    client = _client_with_files(_single_hop_files())
    parent = ParentInfo(source_id="M", variant_label="r1", experiment_id="piControl")

    links = resolve_parent_links(
        build_cells(records),
        via_parent={"piControl": "abrupt-4xCO2"},
        required_vars=NEEDED,
        records=records,
        client=client,
        reader=lambda _url: parent,
    )

    assert links == {("M", "r3", "abrupt-4xCO2"): ("M", "r1", "piControl")}


def test_resolves_multiple_cells_in_one_pass():
    records = _single_hop_records()
    files = _single_hop_files()
    for var in NEEDED:  # a second near-miss variant r8
        records.append(_rec(f"ab8.{var}", "r8", "abrupt-4xCO2", var))
        files[f"ab8.{var}"] = [_file_doc(f"ab8.{var}", f"http://h8/{var}", var)]
    client = _client_with_files(files)
    parent = ParentInfo(source_id="M", variant_label="r1", experiment_id="piControl")

    links = resolve_parent_links(
        build_cells(records),
        via_parent={"piControl": "abrupt-4xCO2"},
        required_vars=NEEDED,
        records=records,
        client=client,
        reader=lambda _url: parent,
    )

    assert links == {
        ("M", "r3", "abrupt-4xCO2"): ("M", "r1", "piControl"),
        ("M", "r8", "abrupt-4xCO2"): ("M", "r1", "piControl"),
    }


def test_follows_a_multi_hop_chain():
    # ssp245 -> historical -> piControl, each hop a different variant.
    records = [
        _rec("ds_ssp", "v", "ssp245", "tas"),
        _rec("ds_hist", "vh", "historical", "tas"),
        _rec("ds_pi", "vp", "piControl", "tas"),
    ]
    files = {
        "ds_ssp": [_file_doc("ds_ssp", "http://ssp", "tas")],
        "ds_hist": [_file_doc("ds_hist", "http://hist", "tas")],
    }
    parents = {
        "http://ssp": ParentInfo(
            source_id="M", variant_label="vh", experiment_id="historical"
        ),
        "http://hist": ParentInfo(
            source_id="M", variant_label="vp", experiment_id="piControl"
        ),
    }

    links = resolve_parent_links(
        build_cells(records),
        via_parent={"piControl": "ssp245"},
        required_vars=("tas",),
        records=records,
        client=_client_with_files(files),
        reader=lambda url: parents[url],
    )

    assert links == {
        ("M", "v", "ssp245"): ("M", "vh", "historical"),
        ("M", "vh", "historical"): ("M", "vp", "piControl"),
    }


def test_walks_through_an_intermediate_target_experiment():
    # uc5 shape: both parents anchored on ssp119 (via_parent maps them both to it),
    # so `historical` is itself a target.  The walk must still continue *through*
    # historical to reach piControl, building both links.
    records = [
        _rec("ds_ssp", "v", "ssp119", "tas"),
        _rec("ds_hist", "vh", "historical", "tas"),
        _rec("ds_pi", "vp", "piControl", "tas"),
    ]
    files = {
        "ds_ssp": [_file_doc("ds_ssp", "http://ssp", "tas")],
        "ds_hist": [_file_doc("ds_hist", "http://hist", "tas")],
    }
    parents = {
        "http://ssp": ParentInfo(
            source_id="M", variant_label="vh", experiment_id="historical"
        ),
        "http://hist": ParentInfo(
            source_id="M", variant_label="vp", experiment_id="piControl"
        ),
    }

    links = resolve_parent_links(
        build_cells(records),
        via_parent={"historical": "ssp119", "piControl": "ssp119"},
        required_vars=("tas",),
        records=records,
        client=_client_with_files(files),
        reader=lambda url: parents[url],
    )

    assert links == {
        ("M", "v", "ssp119"): ("M", "vh", "historical"),
        ("M", "vh", "historical"): ("M", "vp", "piControl"),
    }


def test_conflict_is_recorded_and_skipped():
    records = _single_hop_records()
    client = _client_with_files(_single_hop_files())
    # The two variables disagree on the parent variant.
    parents = {
        "http://h/tas": ParentInfo(variant_label="r1", experiment_id="piControl"),
        "http://h/rsdt": ParentInfo(variant_label="r9", experiment_id="piControl"),
    }
    conflicts: list = []

    links = resolve_parent_links(
        build_cells(records),
        via_parent={"piControl": "abrupt-4xCO2"},
        required_vars=NEEDED,
        records=records,
        client=client,
        reader=lambda url: parents[url],
        conflicts=conflicts,
    )

    assert links == {}
    assert len(conflicts) == 1
    assert conflicts[0].cell == ("M", "r3", "abrupt-4xCO2")
    assert len(conflicts[0].infos) == 2


def test_no_links_when_base_not_covered():
    # abrupt-4xCO2 has only tas, not the full required set -> not a near-miss.
    records = [
        _rec("ab.tas", "r3", "abrupt-4xCO2", "tas"),
        _rec("pi.tas", "r1", "piControl", "tas"),
        _rec("pi.rsdt", "r1", "piControl", "rsdt"),
    ]
    links = resolve_parent_links(
        build_cells(records),
        via_parent={"piControl": "abrupt-4xCO2"},
        required_vars=NEEDED,
        records=records,
        client=_client_with_files({}),
        reader=lambda _url: ParentInfo(),
    )
    assert links == {}
