"""Tests for the ESGF-NG Step-2 assets->files transform (offline)."""

from __future__ import annotations

from typing import Any

from cmip_data_manager.esgf.backends import EsgfNgBackend
from cmip_data_manager.esgf.models import DatasetRecord
from cmip_data_manager.search.files_ng import (
    add_files_from_assets,
    file_records_from_dataset,
    is_stac_record,
)

VERSION_ID = "CMIP6.CMIP.X.MODEL.historical.r1i1p1f1.Amon.tas.gn.v20200101"


def _feature(assets: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "Feature",
        "id": VERSION_ID,
        "collection": "CMIP6",
        "properties": {
            "cmip6:source_id": "MODEL",
            "cmip6:experiment_id": "historical",
            "cmip6:variant_label": "r1i1p1f1",
            "version": "20200101",
            "latest": True,
        },
        "assets": assets,
    }


def _data_asset(name: str, host: str, **extra: Any) -> dict[str, Any]:
    return {
        "href": f"https://{host}/data/{name}",
        "type": "application/netcdf",
        "roles": ["data"],
        "file:size": 123,
        "file:checksum": "1220abcd",
        "cmip6:tracking_id": f"hdl:21.14100/{name}",
        **extra,
    }


def _record(feature: dict[str, Any]) -> DatasetRecord:
    return EsgfNgBackend().parse_dataset(feature)


# --- transform ------------------------------------------------------------------


def test_is_stac_record_detects_feature():
    assert is_stac_record(_record(_feature({}))) is True


def test_each_data_asset_becomes_one_file_record():
    feature = _feature(
        {
            "a.nc": _data_asset("a.nc", "dap.ceda.ac.uk"),
            "b.nc": _data_asset("b.nc", "dap.ceda.ac.uk"),
        }
    )
    files = file_records_from_dataset(_record(feature))
    assert {f.title for f in files} == {"a.nc", "b.nc"}
    one = next(f for f in files if f.title == "a.nc")
    assert one.size == 123
    assert one.checksum == "1220abcd"
    assert one.tracking_id == "hdl:21.14100/a.nc"
    assert one.urls == (
        "https://dap.ceda.ac.uk/data/a.nc|application/netcdf|HTTPServer",
    )


def test_alternate_assets_become_extra_access_urls():
    # A replicated file: primary href + an alternate host (the node/replica equivalent).
    asset = _data_asset(
        "a.nc",
        "dap.ceda.ac.uk",
        alternate={"esgf-node2": {"href": "https://node2.example.org/a.nc"}},
    )
    files = file_records_from_dataset(_record(_feature({"a.nc": asset})))
    assert files[0].urls == (
        "https://dap.ceda.ac.uk/data/a.nc|application/netcdf|HTTPServer",
        "https://node2.example.org/a.nc|application/netcdf|HTTPServer",
    )


def test_non_data_and_non_http_assets_are_skipped():
    feature = _feature(
        {
            "a.nc": _data_asset("a.nc", "dap.ceda.ac.uk"),
            "thumb": {"href": "https://h/t.png", "roles": ["thumbnail"]},
            "globus": {
                "href": "globus://abc/a.nc",
                "roles": ["data"],
                "type": "application/netcdf",
            },
        }
    )
    files = file_records_from_dataset(_record(feature))
    assert [f.title for f in files] == ["a.nc"]  # thumbnail + globus-only skipped


# --- store round-trip -----------------------------------------------------------


def test_add_files_from_assets_persists_and_is_readable(repository):
    feature = _feature(
        {
            "a.nc": _data_asset("a.nc", "dap.ceda.ac.uk"),
            "b.nc": _data_asset("b.nc", "dap.ceda.ac.uk"),
        }
    )
    record = _record(feature)
    # Step 1 must have stored the version first (the file FK target).
    repository.record_run([record], endpoint_url="https://x/search", spec={})

    result = add_files_from_assets([record], repository=repository)
    assert result.searched == 1
    assert result.files_stored == 2
    assert result.failed == []  # no network => no failures

    stored = repository.get_version_files(record.instance_key)
    assert {f.filename for f in stored} == {"a.nc", "b.nc"}
    # Byte-range-ready URLs are present for Step 3 (the header read).
    urls = {a.url for f in stored for a in f.accesses}
    assert "https://dap.ceda.ac.uk/data/a.nc" in urls
    hosts = {a.data_node for f in stored for a in f.accesses}
    assert hosts == {"dap.ceda.ac.uk"}


def test_add_files_from_assets_skips_cached(repository):
    feature = _feature({"a.nc": _data_asset("a.nc", "dap.ceda.ac.uk")})
    record = _record(feature)
    repository.record_run([record], endpoint_url="https://x/search", spec={})

    first = add_files_from_assets([record], repository=repository)
    second = add_files_from_assets([record], repository=repository)
    assert first.files_stored == 1
    assert second.searched == 0 and second.skipped_cached == 1
