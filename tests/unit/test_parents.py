"""Tests for resolving a dataset's parent from its files' netCDF headers."""

from __future__ import annotations

import pytest

from cmip_data_manager.esgf.models import FileRecord
from cmip_data_manager.esgf.parents import (
    ParentInfo,
    ParentMetadataConflictError,
    http_download_url,
    http_download_urls,
    resolve_dataset_parent,
)


def _file(file_id: str, *urls: str, variable_id: str | None = None) -> FileRecord:
    return FileRecord(
        id=file_id, dataset_id="d", variable_id=variable_id, urls=urls, raw={}
    )


def test_http_download_url_picks_the_httpserver_entry():
    file = _file(
        "f",
        "https://host/f.nc|application/netcdf|HTTPServer",
        "gsiftp://host/f.nc|application/gridftp|GridFTP",
    )
    assert http_download_url(file) == "https://host/f.nc"


def test_http_download_url_none_when_no_httpserver():
    file = _file("f", "gsiftp://host/f.nc|application/gridftp|GridFTP")
    assert http_download_url(file) is None


def test_http_download_urls_returns_all_mirrors():
    file = _file(
        "f",
        "http://a/f.nc|x|HTTPServer",
        "gsiftp://a/f.nc|x|GridFTP",
        "https://b/f.nc|x|HTTPServer",
    )
    assert http_download_urls(file) == ["http://a/f.nc", "https://b/f.nc"]


def test_resolve_returns_shared_parent_when_variables_agree():
    files = [
        _file("f1", "https://a/tas.nc|x|HTTPServer", variable_id="tas"),
        _file("f2", "https://b/rsdt.nc|x|HTTPServer", variable_id="rsdt"),
    ]
    parent = ParentInfo(source_id="M", variant_label="r1i1p1f1")

    result = resolve_dataset_parent(files, reader=lambda _url: parent)

    assert result == parent


def test_resolve_reads_one_header_per_variable():
    # Many time-chunks of one variable collapse to a single read.
    files = [
        _file("f1", "http://a/tas_1.nc|x|HTTPServer", variable_id="tas"),
        _file("f2", "http://a/tas_2.nc|x|HTTPServer", variable_id="tas"),
        _file("f3", "http://a/rsdt.nc|x|HTTPServer", variable_id="rsdt"),
    ]
    calls: list[str] = []

    def reader(url: str) -> ParentInfo:
        calls.append(url)
        return ParentInfo(variant_label="r1i1p1f1")

    resolve_dataset_parent(files, reader=reader)

    # One read for tas (first chunk), one for rsdt — not one per file.
    assert calls == ["http://a/tas_1.nc", "http://a/rsdt.nc"]


def test_resolve_raises_when_variables_disagree():
    files = [
        _file("f1", "https://a/tas.nc|x|HTTPServer", variable_id="tas"),
        _file("f2", "https://b/rsdt.nc|x|HTTPServer", variable_id="rsdt"),
    ]
    parents = {
        "https://a/tas.nc": ParentInfo(source_id="M", variant_label="r1i1p1f1"),
        "https://b/rsdt.nc": ParentInfo(source_id="M", variant_label="r2i1p1f1"),
    }

    with pytest.raises(ParentMetadataConflictError) as excinfo:
        resolve_dataset_parent(files, reader=lambda url: parents[url])

    assert excinfo.value.infos == set(parents.values())


def test_resolve_prefers_https_mirror():
    file = _file(
        "f",
        "http://a/f.nc|x|HTTPServer",
        "https://b/f.nc|x|HTTPServer",
        variable_id="tas",
    )
    calls: list[str] = []

    def reader(url: str) -> ParentInfo:
        calls.append(url)
        return ParentInfo(variant_label="r1i1p1f1")

    resolve_dataset_parent([file], reader=reader)

    assert calls == ["https://b/f.nc"]  # https tried first, http never needed


def test_resolve_falls_back_past_a_dead_mirror():
    # One variable available on two hosts; the first errors.
    files = [
        _file("f1", "https://dead/tas.nc|x|HTTPServer", variable_id="tas"),
        _file("f2", "https://ok/tas.nc|x|HTTPServer", variable_id="tas"),
    ]

    def reader(url: str) -> ParentInfo:
        if "dead" in url:
            raise OSError("connection timed out")
        return ParentInfo(variant_label="r1i1p1f1")

    result = resolve_dataset_parent(files, reader=reader)

    assert result.variant_label == "r1i1p1f1"


def test_resolve_honours_ignore_hosts():
    file = _file("f", "https://blocked/f.nc|x|HTTPServer", variable_id="tas")
    with pytest.raises(ValueError, match="No file exposed"):
        resolve_dataset_parent(
            [file],
            reader=lambda _url: ParentInfo(),
            ignore_hosts=frozenset({"blocked"}),
        )


def test_resolve_raises_when_nothing_readable():
    file = _file("f", "https://a/f.nc|x|HTTPServer", variable_id="tas")

    def reader(_url: str) -> ParentInfo:
        raise OSError("dead")

    with pytest.raises(ValueError, match="No file header could be read"):
        resolve_dataset_parent([file], reader=reader)


def test_resolve_raises_on_empty_files():
    with pytest.raises(ValueError, match="zero files"):
        resolve_dataset_parent([], reader=lambda _url: ParentInfo())


def test_resolve_raises_when_no_file_has_a_url():
    files = [_file("f1", "gsiftp://a/f1.nc|x|GridFTP", variable_id="tas")]
    with pytest.raises(ValueError, match="No file exposed"):
        resolve_dataset_parent(files, reader=lambda _url: ParentInfo())
