"""Tests for normalising raw ESGF documents."""

from __future__ import annotations

import pytest

from cmip_data_manager.esgf.models import (
    AmbiguousFieldError,
    DatasetRecord,
    FileRecord,
)


def test_dataset_from_solr_normalises_arrays():
    doc = {
        "id": "CMIP6.x.tas.v1|node",
        "master_id": ["CMIP6.x.tas"],
        "source_id": ["MODEL-A"],
        "experiment_id": ["ssp245"],
        "variant_label": ["r1i1p1f1"],
        "variable_id": ["tas"],
        "frequency": ["mon"],
        "version": [20220406],  # comes back as an int
        "replica": [True],
        "number_of_files": [86],
        "_timestamp": ["2023-11-10T15:19:29.144Z"],
    }
    record = DatasetRecord.from_solr(doc)
    assert record.id == "CMIP6.x.tas.v1|node"
    assert record.source_id == "MODEL-A"
    assert record.variable_id == "tas"
    assert record.version == "20220406"  # coerced to string
    assert record.replica is True
    assert record.number_of_files == 86
    assert record.esgf_timestamp == "2023-11-10T15:19:29.144Z"
    assert record.raw == doc


def test_repeated_identical_values_collapse():
    record = DatasetRecord.from_solr(
        {"id": "d1", "frequency": ["mon", "mon"], "variable_id": ["tas"]}
    )
    assert record.frequency == "mon"


def test_conflicting_values_raise():
    with pytest.raises(AmbiguousFieldError, match="variable_id"):
        DatasetRecord.from_solr({"id": "d1", "variable_id": ["tas", "pr"]})


def test_missing_fields_are_none():
    record = DatasetRecord.from_solr({"id": "d1"})
    assert record.source_id is None
    assert record.number_of_files is None


def test_file_from_solr():
    doc = {
        "id": "file-1",
        "dataset_id": ["CMIP6.x.tas.v1|node"],
        "title": ["tas_Amon.nc"],
        "size": [4722460],
        "checksum": ["abc"],
        "checksum_type": ["SHA256"],
        "url": ["http://host/a.nc|application/netcdf|HTTPServer", "gsiftp://x|y|z"],
    }
    record = FileRecord.from_solr(doc)
    assert record.id == "file-1"
    assert record.dataset_id == "CMIP6.x.tas.v1|node"
    assert record.title == "tas_Amon.nc"
    assert record.size == 4722460
    assert len(record.urls) == 2
