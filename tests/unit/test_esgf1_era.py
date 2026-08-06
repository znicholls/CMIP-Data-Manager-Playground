"""The ESGF1 backend applies a MIP era's facet-name translation (Phase 0.3)."""

from __future__ import annotations

import pytest

from cmip_data_manager.esgf.backends.base import Flavour, UnsupportedOnBackend
from cmip_data_manager.esgf.backends.detect import backend_for
from cmip_data_manager.esgf.backends.esgf1 import Esgf1Backend
from cmip_data_manager.esgf.client import ESGFSearchClient
from cmip_data_manager.esgf.eras import CMIP5_PROFILE, CMIP6_PROFILE
from cmip_data_manager.esgf.query import FacetQuery

# A real-shaped CMIP5 Amon table doc: one dataset carrying many variables.
_CMIP5_TABLE_DOC = {
    "id": "cmip5.output1.BNU.BNU-ESM.rcp45.mon.atmos.Amon.r1i1p1.v20120510|node",
    "master_id": ["cmip5.output1.BNU.BNU-ESM.rcp45.mon.atmos.Amon.r1i1p1"],
    "instance_id": ["cmip5.output1.BNU.BNU-ESM.rcp45.mon.atmos.Amon.r1i1p1.v20120510"],
    "model": ["BNU-ESM"],
    "institute": ["BNU"],
    "experiment": ["rcp45"],
    "ensemble": ["r1i1p1"],
    "variable": ["tas", "pr", "rlut", "zg"],
    "cmor_table": ["Amon"],
    "time_frequency": ["mon"],
    "realm": ["atmos"],
    "product": ["output1"],
    "version": ["20120510"],
    "data_node": "node",
}


def test_cmip6_backend_is_unchanged_identity():
    backend = Esgf1Backend()  # default CMIP6 profile
    _, params = backend.page_request(
        "https://x/search",
        FacetQuery(source_id=("CESM2",), variable_id=("tas",)),
        cursor=0,
        page_size=10,
    )
    assert params["source_id"] == "CESM2"
    assert params["variable_id"] == "tas"
    assert params["project"] == "CMIP6"


def test_cmip5_backend_renames_outbound_facets():
    backend = Esgf1Backend(era=CMIP5_PROFILE)
    _, params = backend.page_request(
        "https://x/search",
        FacetQuery(
            mip_era="CMIP5",
            source_id=("CESM1-CAM5",),
            experiment_id=("rcp45",),
            variant_label=("r1i1p1",),
            variable_id=("tas",),
            frequency=("mon",),
        ),
        cursor=0,
        page_size=10,
    )
    # Canonical facet keys are renamed to CMIP5-native ones, project forced to CMIP5.
    assert params["model"] == "CESM1-CAM5"
    assert params["experiment"] == "rcp45"
    assert params["ensemble"] == "r1i1p1"
    assert params["variable"] == "tas"
    assert params["time_frequency"] == "mon"
    assert params["project"] == "CMIP5"
    assert "source_id" not in params


def test_cmip5_backend_canonicalises_inbound_and_keeps_raw():
    backend = Esgf1Backend(era=CMIP5_PROFILE)
    doc = {
        "id": "cmip5.output1.NSF-DOE-NCAR.CESM1-CAM5.rcp45."
        "mon.atmos.Amon.r1i1p1.v20120601|aims3.llnl.gov",
        "model": ["CESM1-CAM5"],
        "experiment": ["rcp45"],
        "ensemble": ["r1i1p1"],
        "variable": ["tas"],
        "cmor_table": ["Amon"],
        "time_frequency": ["mon"],
        "product": ["output1"],
        "realm": ["atmos"],
        "data_node": "aims3.llnl.gov",
    }
    record = backend.parse_dataset(doc)
    # Canonical columns populate from the renamed keys...
    assert record.source_id == "CESM1-CAM5"
    assert record.experiment_id == "rcp45"
    assert record.variant_label == "r1i1p1"
    assert record.variable_id == "tas"
    assert record.table_id == "Amon"
    assert record.frequency == "mon"
    # ...and raw keeps the ORIGINAL CMIP5-native document verbatim.
    assert record.raw is doc
    assert record.raw["product"] == ["output1"]
    assert record.raw["realm"] == ["atmos"]


def test_backend_for_rejects_cmip5_on_ng():
    with pytest.raises(UnsupportedOnBackend):
        backend_for(Flavour.ESGF_NG_EAST, era=CMIP5_PROFILE)


def test_backend_for_allows_cmip6_on_all_and_cmip5_on_esgf1():
    assert isinstance(backend_for(Flavour.ESGF1, era=CMIP5_PROFILE), Esgf1Backend)
    assert backend_for(Flavour.ESGF_NG_EAST, era=CMIP6_PROFILE) is not None


def _fetch_one(doc):
    """A fetch returning a single-page Solr response holding `doc`."""

    def fetch(url, params):
        return {"response": {"numFound": 1, "docs": [doc]}}

    return fetch


def test_cmip5_search_expands_table_into_per_variable_records():
    client = ESGFSearchClient(
        "https://x/search",
        backend=Esgf1Backend(era=CMIP5_PROFILE),
        fetch=_fetch_one(_CMIP5_TABLE_DOC),
    )
    records = client.search(FacetQuery(mip_era="CMIP5", variable_id=("tas", "pr")))
    # One table doc -> two per-variable records (only the requested, present ones).
    assert sorted(r.variable_id for r in records) == ["pr", "tas"]
    # Each is a fully reconstructed, distinct per-variable master id.
    masters = {r.variable_id: r.master_id for r in records}
    assert masters["tas"].endswith(".tas.mon.Amon.BNU-ESM_rcp45_atmos")
    assert masters["pr"].endswith(".pr.mon.Amon.BNU-ESM_rcp45_atmos")
    assert masters["tas"] != masters["pr"]
    # Native table id (no variable) is preserved on each record for the file search.
    assert all(r.id.startswith("cmip5.output1.BNU.BNU-ESM") for r in records)


def test_cmip5_search_without_variable_raises_before_exploding():
    client = ESGFSearchClient(
        "https://x/search",
        backend=Esgf1Backend(era=CMIP5_PROFILE),
        fetch=_fetch_one(_CMIP5_TABLE_DOC),
    )
    with pytest.raises(ValueError, match="must be variable-scoped"):
        client.search(FacetQuery(mip_era="CMIP5"))


def test_cmip6_search_is_one_record_per_doc():
    doc = {"id": "d0|node", "variable_id": ["tas"], "source_id": ["CESM2"]}
    client = ESGFSearchClient(
        "https://x/search", backend=Esgf1Backend(), fetch=_fetch_one(doc)
    )
    records = client.search(FacetQuery(variable_id=("tas",)))
    assert len(records) == 1
    assert records[0].variable_id == "tas"
