"""Tests for the client-side model-variant aggregation."""

from __future__ import annotations

from cmip_data_manager.esgf.models import DatasetRecord
from cmip_data_manager.search.aggregate import (
    Cell,
    ModelVariant,
    build_cells,
    pairs_all_experiments,
    pairs_any_experiment,
)


def _dict_resolver(links: dict[Cell, Cell]):
    return links.get


def _rec(source_id, variant_label, experiment_id, variable_id):
    return DatasetRecord(
        id=f"{source_id}.{experiment_id}.{variant_label}.{variable_id}",
        source_id=source_id,
        variant_label=variant_label,
        experiment_id=experiment_id,
        variable_id=variable_id,
        raw={},
    )


def test_build_cells_ignores_incomplete_records():
    records = [
        _rec("M", "r1", "piControl", "tas"),
        _rec("M", "r1", "piControl", "rsdt"),
        DatasetRecord(id="incomplete", source_id="M", raw={}),  # missing fields
    ]
    cells = build_cells(records)
    assert cells[("M", "r1", "piControl")] == {"tas", "rsdt"}
    assert len(cells) == 1


def test_pairs_all_experiments_strict_cross_product():
    needed = ["tas", "rsdt", "rlut", "rsut"]
    records = []
    # M/r1 has all vars in BOTH required experiments -> qualifies.
    for exp in ("abrupt-4xCO2", "piControl"):
        for var in needed:
            records.append(_rec("M", "r1", exp, var))
    # M/r2 has all vars only in piControl -> does NOT qualify.
    for var in needed:
        records.append(_rec("M", "r2", "piControl", var))
    # Optional experiment fully covered for M/r1.
    for var in needed:
        records.append(_rec("M", "r1", "abrupt-2xCO2", var))

    matches = pairs_all_experiments(
        build_cells(records),
        required_vars=needed,
        required_experiments=("abrupt-4xCO2", "piControl"),
        optional_experiments=("abrupt-2xCO2", "abrupt-0p5xCO2"),
    )
    assert [m.model_variant for m in matches] == [ModelVariant("M", "r1")]
    assert matches[0].optional_experiments == ("abrupt-2xCO2",)


def test_pairs_any_experiment_within_one_experiment():
    # M/r1 has tas+fgco2+nbp together in ssp126 -> qualifies.
    records = [
        _rec("M", "r1", "ssp126", "tas"),
        _rec("M", "r1", "ssp126", "fgco2"),
        _rec("M", "r1", "ssp126", "nbp"),
        _rec("M", "r1", "ssp126", "co2s"),
        # M/r2 has the vars but spread across experiments -> does NOT qualify.
        _rec("M", "r2", "historical", "tas"),
        _rec("M", "r2", "ssp585", "fgco2"),
        _rec("M", "r2", "ssp585", "nbp"),
    ]
    matches = pairs_any_experiment(
        build_cells(records),
        required_vars=("tas", "fgco2", "nbp"),
        optional_variable_preferences=("co2s", "co2"),
    )
    assert [m.model_variant for m in matches] == [ModelVariant("M", "r1")]
    assert matches[0].experiments == ("ssp126",)
    assert matches[0].optional_variables == ("co2s",)


def _forcing_records():
    # abrupt-4xCO2 under r1i1p1f3; its piControl parent is a different variant.
    needed = ["tas", "rsdt", "rlut", "rsut"]
    records = []
    for var in needed:
        records.append(_rec("HadGEM3", "r1i1p1f3", "abrupt-4xCO2", var))
        records.append(_rec("HadGEM3", "r1i1p1f1", "piControl", var))
    return needed, records


def test_pairs_all_experiments_matches_via_parent_single_hop():
    needed, records = _forcing_records()
    resolver = _dict_resolver(
        {("HadGEM3", "r1i1p1f3", "abrupt-4xCO2"): ("HadGEM3", "r1i1p1f1", "piControl")}
    )

    matches = pairs_all_experiments(
        build_cells(records),
        required_vars=needed,
        required_experiments=("abrupt-4xCO2", "piControl"),
        resolver=resolver,
        via_parent={"piControl": "abrupt-4xCO2"},
    )

    assert [m.model_variant for m in matches] == [ModelVariant("HadGEM3", "r1i1p1f3")]
    assert matches[0].parent_experiments == ("piControl",)


def test_via_parent_ignored_without_resolver():
    needed, records = _forcing_records()

    # No resolver: the abrupt variant has no piControl of its own -> no match.
    matches = pairs_all_experiments(
        build_cells(records),
        required_vars=needed,
        required_experiments=("abrupt-4xCO2", "piControl"),
        via_parent={"piControl": "abrupt-4xCO2"},
    )

    assert matches == []


def test_pairs_all_experiments_follows_parent_chain():
    # ssp245 -> historical -> piControl (two hops); historical has no datasets here.
    records = [
        _rec("M", "v", "ssp245", "tas"),
        _rec("M", "vp", "piControl", "tas"),
    ]
    resolver = _dict_resolver(
        {
            ("M", "v", "ssp245"): ("M", "v", "historical"),
            ("M", "v", "historical"): ("M", "vp", "piControl"),
        }
    )

    matches = pairs_all_experiments(
        build_cells(records),
        required_vars=("tas",),
        required_experiments=("ssp245", "piControl"),
        resolver=resolver,
        via_parent={"piControl": "ssp245"},
    )

    match = next(m for m in matches if m.model_variant == ModelVariant("M", "v"))
    assert match.parent_experiments == ("piControl",)


def test_via_parent_no_match_when_chain_dead_ends():
    needed, records = _forcing_records()
    # Resolver knows of no parent for the abrupt cell -> falls back to strict miss.
    matches = pairs_all_experiments(
        build_cells(records),
        required_vars=needed,
        required_experiments=("abrupt-4xCO2", "piControl"),
        resolver=_dict_resolver({}),
        via_parent={"piControl": "abrupt-4xCO2"},
    )
    assert matches == []


def test_via_parent_stops_on_cycle():
    needed, records = _forcing_records()
    # A self-referential parent link must not loop forever, and must not match.
    resolver = _dict_resolver(
        {
            ("HadGEM3", "r1i1p1f3", "abrupt-4xCO2"): (
                "HadGEM3",
                "r1i1p1f3",
                "abrupt-4xCO2",
            )
        }
    )
    matches = pairs_all_experiments(
        build_cells(records),
        required_vars=needed,
        required_experiments=("abrupt-4xCO2", "piControl"),
        resolver=resolver,
        via_parent={"piControl": "abrupt-4xCO2"},
    )
    assert matches == []


def test_optional_variable_falls_back_to_co2():
    records = [
        _rec("M", "r1", "historical", "tas"),
        _rec("M", "r1", "historical", "fgco2"),
        _rec("M", "r1", "historical", "nbp"),
        _rec("M", "r1", "historical", "co2"),  # co2s absent, co2 present
    ]
    matches = pairs_any_experiment(
        build_cells(records),
        required_vars=("tas", "fgco2", "nbp"),
        optional_variable_preferences=("co2s", "co2"),
    )
    assert matches[0].optional_variables == ("co2",)
