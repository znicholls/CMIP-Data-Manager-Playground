"""
Tests for the CMIP7 (ESGF-NG / STAC) path: parsing, side table, layout, persistence.

CMIP7 needs no facet-name translation (its STAC properties already use canonical names
under a `cmip7:` prefix), so these tests focus on the CMIP7-specific behaviour: the
branding suffix folding into `table_id`, `mip_era` stamping, the `Cmip7VersionExtra`
promotion, and the DRS-id-rebuilt download layout.  A captured **real** CMIP7 item
(`tests/fixtures/cmip7_stac_item.json`) exercises the whole chain offline.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from sqlmodel import Session, select

from cmip_data_manager.db.repository import Repository
from cmip_data_manager.db.schema import Cmip7VersionExtra, Dataset, DatasetVersion
from cmip_data_manager.esgf.backends.esgf_ng import EsgfNgBackend
from cmip_data_manager.esgf.cmip7 import cmip7_extra_fields
from cmip_data_manager.esgf.eras import CMIP6_PROFILE, CMIP7_PROFILE
from cmip_data_manager.esgf.models import DatasetRecord
from cmip_data_manager.search.download import drs_path
from cmip_data_manager.search.files_ng import file_records_from_dataset

_FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "cmip7_stac_item.json"


def _real_feature() -> dict[str, Any]:
    """Load the captured live CMIP7 STAC item."""
    return json.loads(_FIXTURE.read_text())


def _cmip7_feature(**overrides: Any) -> dict[str, Any]:
    """Build a minimal synthetic CMIP7 STAC feature (`cmip7:`-prefixed properties)."""
    props: dict[str, Any] = {
        "cmip7:source_id": "CanESM5-1",
        "cmip7:experiment_id": "piControl",
        "cmip7:variant_label": "r1i1p2f1",
        "cmip7:variable_id": "tas",
        "cmip7:frequency": "mon",
        "cmip7:grid_label": "g120",
        "cmip7:variable_branding_suffix": "tavg-h2m-hxy-u",
        "cmip7:variable_branded_name": "tas_tavg-h2m-hxy-u",
        "cmip7:region": "glb",
        "cmip7:temporal_label": "tavg",
        "cmip7:license_id": "CC-BY-4.0",
        "cmip7:realm": ["atmos"],
        "version": "20190429",
        "latest": True,
    }
    props.update({k: v for k, v in overrides.items()})
    return {
        "type": "Feature",
        "id": "MIP-DRS7.CMIP7.CMIP.CCCma.CanESM5-1.piControl.r1i1p2f1.glb.mon.tas."
        "tavg-h2m-hxy-u.g120.v20190429",
        "collection": "CMIP7",
        "properties": props,
        "assets": {
            "tas_1.nc": {
                "href": "https://h/tas_1.nc",
                "roles": ["data"],
                "type": "application/netcdf",
                "file:size": 123,
                "cmip7:tracking_id": "hdl:1/a",
            }
        },
    }


def test_ng_backend_stamps_mip_era_from_its_era():
    # The STAC feature does not name the era in a column the record reads; the backend
    # stamps it from its bound profile.
    feature = _cmip7_feature()
    assert EsgfNgBackend(era=CMIP7_PROFILE).parse_dataset(feature).mip_era == "CMIP7"
    assert EsgfNgBackend(era=CMIP6_PROFILE).parse_dataset(feature).mip_era == "CMIP6"


def test_branding_suffix_folds_into_table_id():
    # CMIP7 has no table_id; the branding suffix is the variable-identity discriminator
    # and lands in the table_id column so downstream identity keeps working.
    record = EsgfNgBackend(era=CMIP7_PROFILE).parse_dataset(_cmip7_feature())
    assert record.table_id == "tavg-h2m-hxy-u"


def test_table_id_fallback_inert_when_real_table_present():
    # For a collection that *does* carry table_id (CMIP6), the branding fallback never
    # fires — the real table wins.
    feature = {
        "type": "Feature",
        "id": "CMIP6.X.v1",
        "collection": "CMIP6",
        "properties": {"cmip6:table_id": "Amon", "version": "1"},
    }
    assert DatasetRecord.from_stac(feature).table_id == "Amon"


def test_cmip7_extra_fields_extracted_from_properties():
    record = EsgfNgBackend(era=CMIP7_PROFILE).parse_dataset(_cmip7_feature())
    fields = cmip7_extra_fields(record)
    assert fields["branding_suffix"] == "tavg-h2m-hxy-u"
    assert fields["branded_variable"] == "tas_tavg-h2m-hxy-u"
    assert fields["region"] == "glb"
    assert fields["temporal_label"] == "tavg"
    assert fields["license_id"] == "CC-BY-4.0"
    assert fields["realm"] == "atmos"  # a 1-item list in the raw props, scalarised


def test_cmip7_download_layout_rebuilt_from_drs_id():
    record = EsgfNgBackend(era=CMIP7_PROFILE).parse_dataset(_cmip7_feature())
    path = drs_path(Path("/dl"), record, "tas_x.nc")
    # The versioned DRS id's dotted segments become the tree; branding + region appear.
    assert path == Path(
        "/dl/MIP-DRS7/CMIP7/CMIP/CCCma/CanESM5-1/piControl/r1i1p2f1/glb/mon/tas/"
        "tavg-h2m-hxy-u/g120/v20190429/tas_x.nc"
    )


def test_real_cmip7_item_parses_and_files_resolve():
    record = EsgfNgBackend(era=CMIP7_PROFILE).parse_dataset(_real_feature())
    assert record.mip_era == "CMIP7"
    assert record.source_id == "CanESM5-1"
    assert record.table_id == "tavg-h2m-hxy-u"
    assert record.version == "20190429"
    files = file_records_from_dataset(record)
    assert files, "the real CMIP7 item must yield http data assets"
    assert all(f.urls for f in files)


def test_record_run_persists_cmip7_side_table(repository: Repository):
    record = EsgfNgBackend(era=CMIP7_PROFILE).parse_dataset(_real_feature())
    repository.record_run(
        [record],
        endpoint_url="https://search.east.esgf.io/search",
        spec={"mip_era": "CMIP7"},
        tag="cmip7_test",
    )
    with Session(repository._engine) as session:
        dataset = session.exec(select(Dataset)).one()
        version = session.exec(select(DatasetVersion)).one()
        extra = session.exec(select(Cmip7VersionExtra)).one()
    assert dataset.mip_era == "CMIP7"
    assert version.mip_era == "CMIP7"
    assert extra.branding_suffix == "tavg-h2m-hxy-u"
    assert extra.region == "glb"
    assert extra.license_id == "CC-BY-4.0"
    # And the record round-trips with the branding preserved in table_id.
    [loaded] = repository.get_dataset_records("cmip7_test")
    assert loaded.mip_era == "CMIP7"
    assert loaded.table_id == "tavg-h2m-hxy-u"
