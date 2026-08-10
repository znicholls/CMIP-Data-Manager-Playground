"""Tests for the MIP-era vocabulary profiles."""

from __future__ import annotations

import pytest

from cmip_data_manager.esgf.backends.base import Flavour
from cmip_data_manager.esgf.eras import (
    CMIP5_PROFILE,
    CMIP6_PROFILE,
    CMIP7_PROFILE,
    ERA_PROFILES,
    get_profile,
)


def test_cmip6_is_identity_translation():
    # CMIP6 is the canonical vocabulary: names never change in either direction.
    for facet in ("source_id", "experiment_id", "variable_id", "table_id"):
        assert CMIP6_PROFILE.native_facet(facet) == facet
        assert CMIP6_PROFILE.canonical_facet(facet) == facet
    params = {"project": "CMIP6", "source_id": "CESM2", "type": "Dataset"}
    assert CMIP6_PROFILE.to_native_params(params) == params
    doc = {"source_id": ["CESM2"], "id": "x"}
    assert CMIP6_PROFILE.to_canonical_doc(doc) == doc


def test_cmip5_name_translation_round_trips():
    # Every mapped canonical facet renames to its CMIP5-native name and back.
    for canonical, native in CMIP5_PROFILE.field_map.items():
        assert CMIP5_PROFILE.native_facet(canonical) == native
        assert CMIP5_PROFILE.canonical_facet(native) == canonical


def test_cmip5_outbound_params_renamed_and_project_set():
    params = {
        "format": "application/solr+json",
        "type": "Dataset",
        "project": "CMIP6",
        "source_id": "CESM2",
        "experiment_id": "rcp45",
        "variable_id": "tas",
        "frequency": "mon",
        "offset": "0",
        "limit": "10",
    }
    out = CMIP5_PROFILE.to_native_params(params)
    # Facet keys are renamed to CMIP5-native names...
    assert out["model"] == "CESM2"
    assert out["experiment"] == "rcp45"
    assert out["variable"] == "tas"
    assert out["time_frequency"] == "mon"
    # ...the canonical keys are gone...
    assert "source_id" not in out
    assert "experiment_id" not in out
    # ...project is forced to the era value, and non-facet keys pass through.
    assert out["project"] == "CMIP5"
    assert out["type"] == "Dataset"
    assert out["offset"] == "0"


def test_cmip5_inbound_doc_canonicalised_without_touching_original():
    doc = {
        "id": "cmip5...|node",
        "model": ["CESM2"],
        "experiment": ["rcp45"],
        "ensemble": ["r1i1p1"],
        "cmor_table": ["Amon"],
        "time_frequency": ["mon"],
        "product": ["output1"],
        "realm": ["atmos"],
    }
    canonical = CMIP5_PROFILE.to_canonical_doc(doc)
    assert canonical["source_id"] == ["CESM2"]
    assert canonical["experiment_id"] == ["rcp45"]
    assert canonical["variant_label"] == ["r1i1p1"]
    assert canonical["table_id"] == ["Amon"]
    assert canonical["frequency"] == ["mon"]
    # CMIP5-only facets that have no canonical name pass through untouched...
    assert canonical["product"] == ["output1"]
    assert canonical["realm"] == ["atmos"]
    # ...and the original document is not mutated (raw is preserved by the caller).
    assert "model" in doc
    assert "source_id" not in doc


def test_cmip5_supported_flavours_are_esgf1_only():
    assert CMIP5_PROFILE.supported_flavours == frozenset({Flavour.ESGF1})
    assert Flavour.ESGF_NG_EAST in CMIP6_PROFILE.supported_flavours


def test_cmip5_reconstructs_ids_cmip6_does_not():
    assert CMIP5_PROFILE.reconstruct is not None
    assert CMIP6_PROFILE.reconstruct is None


def test_get_profile_and_registry():
    assert get_profile("CMIP5") is CMIP5_PROFILE
    assert get_profile("CMIP6") is CMIP6_PROFILE
    assert get_profile("CMIP7") is CMIP7_PROFILE
    assert set(ERA_PROFILES) == {"CMIP5", "CMIP6", "CMIP7"}


def test_cmip7_is_esgf_ng_only_and_needs_no_name_translation():
    # CMIP7 is served only over ESGF-NG (STAC); never over ESGF1/Solr.
    assert CMIP7_PROFILE.supported_flavours == frozenset(
        {Flavour.ESGF_NG_EAST, Flavour.ESGF_NG_WEST}
    )
    assert Flavour.ESGF1 not in CMIP7_PROFILE.supported_flavours
    # The STAC properties already carry canonical names, so the field map is identity
    # and the ids need no reconstruction.
    assert CMIP7_PROFILE.field_map == {}
    assert CMIP7_PROFILE.reconstruct is None
    assert CMIP7_PROFILE.multi_variable is False
    # table_id has no CMIP7 equivalent (branding suffix takes its column) ...
    assert "table_id" in CMIP7_PROFILE.absent_facets
    # ... and parents are read from the record first (no header round-trip).
    assert CMIP7_PROFILE.parent_strategy[0] == "record"


def test_get_profile_unknown_era_raises_with_help():
    with pytest.raises(KeyError, match="Unknown mip_era"):
        get_profile("CMIP99")


def test_cmip6_expand_is_identity():
    assert CMIP6_PROFILE.multi_variable is False
    doc = {"variable_id": ["tas"], "id": "x"}
    assert CMIP6_PROFILE.expand(doc, ()) == [doc]
    # Even if a CMIP6 doc had many variables (it never does), it is left untouched.
    multi = {"variable_id": ["tas", "pr"]}
    assert CMIP6_PROFILE.expand(multi, ("tas",)) == [multi]


def test_cmip5_expand_projects_table_onto_requested_variables():
    assert CMIP5_PROFILE.multi_variable is True
    doc = {"variable": ["tas", "pr", "zg"], "model": ["BNU-ESM"]}
    docs = CMIP5_PROFILE.expand(doc, ("tas", "pr"))
    assert [d["variable"] for d in docs] == [["tas"], ["pr"]]
    # Non-variable fields are copied onto every projected doc.
    assert all(d["model"] == ["BNU-ESM"] for d in docs)
    # The original is not mutated.
    assert doc["variable"] == ["tas", "pr", "zg"]


def test_cmip5_expand_single_variable_table_passes_through():
    doc = {"variable": ["tas"], "model": ["M"]}
    assert CMIP5_PROFILE.expand(doc, ()) == [doc]


def test_cmip5_expand_raises_when_many_variables_and_none_requested():
    doc = {"variable": ["tas", "pr", "zg"]}
    with pytest.raises(ValueError, match="must be variable-scoped"):
        CMIP5_PROFILE.expand(doc, ())


def test_cmip5_expand_drops_variables_not_in_the_table():
    doc = {"variable": ["tas", "pr"]}
    # rlut isn't in this table -> only the present requested variables survive.
    docs = CMIP5_PROFILE.expand(doc, ("tas", "rlut"))
    assert [d["variable"] for d in docs] == [["tas"]]
