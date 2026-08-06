"""Tests for CMIP5 identity reconstruction (grid_label, base master id, extras)."""

from __future__ import annotations

from cmip_data_manager.esgf.cmip5 import (
    cmip5_base_master_id,
    cmip5_extra_fields,
    cmip5_grid_label,
    reconstruct_ids,
)
from cmip_data_manager.esgf.models import DatasetRecord

_NATIVE_MASTER = "cmip5.output1.NSF-DOE-NCAR.CESM1-CAM5.rcp45.mon.atmos.Amon.r1i1p1"
_BASE = "CMIP5.CESM1-CAM5.NSF-DOE-NCAR.rcp45.r1i1p1.tas.mon.Amon.CESM1-CAM5_rcp45_atmos"


def _cmip5_record(*, product="output1", realm="atmos", version="20120601"):
    """A CMIP5 record with canonical columns populated and native raw fields."""
    raw = {
        "id": f"{_NATIVE_MASTER}.v{version}|aims3.llnl.gov",
        "master_id": [_NATIVE_MASTER],
        "instance_id": [f"{_NATIVE_MASTER}.v{version}"],
        "product": [product],
        "realm": [realm],
        "version": [version],
    }
    return DatasetRecord(
        id=raw["id"],
        project="CMIP5",
        mip_era="CMIP5",
        source_id="CESM1-CAM5",
        institution_id="NSF-DOE-NCAR",
        experiment_id="rcp45",
        variant_label="r1i1p1",
        variable_id="tas",
        frequency="mon",
        table_id="Amon",
        version=version,
        data_node="aims3.llnl.gov",
        raw=raw,
    )


def test_grid_label_is_source_experiment_realm_composite():
    assert cmip5_grid_label(_cmip5_record()) == "CESM1-CAM5_rcp45_atmos"


def test_grid_label_handles_scalar_realm_not_only_lists():
    # ESGF usually returns lists, but a scalar realm must still work.
    rec = _cmip5_record()
    rec = rec.model_copy(update={"raw": {**rec.raw, "realm": "ocean"}})
    assert cmip5_grid_label(rec) == "CESM1-CAM5_rcp45_ocean"


def test_base_master_id_joins_dataset_facets_in_order():
    # project.source_id.institution_id.experiment_id.variant_label.variable_id
    # .frequency.table_id.grid_label  (no nominal_resolution).
    rec = reconstruct_ids(_cmip5_record())
    assert cmip5_base_master_id(rec) == _BASE


def test_reconstruct_sets_base_master_id_without_any_suffix():
    # Reconstruction never adds a product suffix; the id is the bare base.
    out1 = reconstruct_ids(_cmip5_record(product="output1"))
    out2 = reconstruct_ids(_cmip5_record(product="output2"))
    assert out1.master_id == _BASE
    assert out2.master_id == _BASE  # same base; disambiguation happens at write time


def test_instance_id_is_base_plus_v_normalised_version():
    out = reconstruct_ids(_cmip5_record(version="20120601"))
    assert out.version == "v20120601"  # bare YYYYMMDD gets a leading v
    assert out.instance_id == f"{_BASE}.v20120601"


def test_native_id_is_left_untouched_for_file_search():
    out = reconstruct_ids(_cmip5_record())
    assert out.id.startswith(_NATIVE_MASTER)
    assert out.raw["product"] == ["output1"]  # raw preserved


def test_extra_fields_carry_base_realm_and_native_table_ids():
    out = reconstruct_ids(_cmip5_record(product="output2"))
    fields = cmip5_extra_fields(out)
    assert fields["base_master_id"] == _BASE  # suffix-free, recomputed from columns
    assert fields["realm"] == "atmos"
    # Native ids are table-grained (no variable in them).
    assert fields["native_master_id"] == _NATIVE_MASTER
    assert fields["native_dataset_id"] == f"{_NATIVE_MASTER}.v20120601"


def test_bcc_model_with_dot_in_name_survives():
    # BCC-CSM1.1 has a '.' in the model name; it must not be confused with a suffix.
    rec = _cmip5_record()
    rec = rec.model_copy(update={"source_id": "BCC-CSM1.1"})
    out = reconstruct_ids(rec)
    # The '.1' lives inside the base; the id ends in the realm word, never a bare digit.
    assert out.master_id.endswith("_rcp45_atmos")
    assert "BCC-CSM1.1" in out.master_id


def test_no_version_anywhere_leaves_instance_equal_to_master():
    # With neither a version facet nor a version in the ids, instance_id == master_id.
    rec = _cmip5_record()
    # A trailing '.' means no version segment can be recovered from the id either.
    rec = rec.model_copy(
        update={"version": None, "id": "cmip5-no-version.", "instance_id": None}
    )
    out = reconstruct_ids(rec)
    assert out.version is None
    assert out.instance_id == out.master_id


def test_version_recovered_from_native_id_when_facet_absent():
    rec = _cmip5_record(version="20120601")
    # Drop the version facet; recover it from the native id's trailing segment.
    rec = rec.model_copy(update={"version": None})
    out = reconstruct_ids(rec)
    assert out.version == "v20120601"
    assert out.instance_id.endswith(".v20120601")
