"""Tests for Step 4: the node-independent parent walk (per-parent search + errors)."""

from __future__ import annotations

import pytest

from cmip_data_manager.esgf.headers import HeaderMetadata
from cmip_data_manager.esgf.models import DatasetRecord, FileRecord
from cmip_data_manager.esgf.query import FacetQuery
from cmip_data_manager.search.parent_walk import (
    ParentExperimentMissingError,
    ParentNotAncestor,
    ParentResolutionError,
    resolve_parent_chains,
)

_V = "v20240101"
_NO_PARENT = {"parent_experiment_id": "no parent", "parent_variant_label": "no parent"}


def _rec(source, experiment, variant, variable, *, node="nci"):
    master = f"{source}.{experiment}.{variant}.{variable}"
    instance = f"{master}.{_V}"
    node_id = f"{instance}|{node}"
    return DatasetRecord(
        id=node_id,
        instance_id=instance,
        master_id=master,
        version=_V,
        data_node=node,
        source_id=source,
        experiment_id=experiment,
        variant_label=variant,
        variable_id=variable,
        table_id="Amon",
        grid_label="gn",
        raw={"id": node_id},
    )


def _parent_attrs(source, experiment, variant):
    return {
        "parent_source_id": source,
        "parent_experiment_id": experiment,
        "parent_variant_label": variant,
    }


def _url(record):
    return f"https://nci/{record.instance_id}/{record.variable_id}.nc"


class _World:
    """A fake index + data node driven by a `{sim: [records]}` world and headers."""

    base_url = "http://index/search"

    def __init__(self, records_by_sim, headers, existing_experiments):
        self._world = records_by_sim
        self._headers = headers
        self._existing = existing_experiments
        self.parent_searches: list[tuple[str, str, str]] = []
        self._by_id = {r.id: r for recs in records_by_sim.values() for r in recs}

    def count(self, query: FacetQuery) -> int:
        (experiment,) = query.experiment_id
        return 1 if experiment in self._existing else 0

    def search(self, query: FacetQuery) -> list[DatasetRecord]:
        sim = (query.source_id[0], query.experiment_id[0], query.variant_label[0])
        self.parent_searches.append(sim)
        return list(self._world.get(sim, []))

    def search_files(self, query: FacetQuery) -> list[FileRecord]:
        out: list[FileRecord] = []
        for dataset_id in query.dataset_id:
            record = self._by_id.get(dataset_id)
            if record is None:
                continue
            out.append(
                FileRecord(
                    id=f"f-{record.instance_id}",
                    dataset_id=dataset_id,
                    title=f"{record.variable_id}.nc",
                    urls=(f"{_url(record)}|application/netcdf|HTTPServer",),
                    raw={},
                )
            )
        return out

    def reader(self, url: str) -> HeaderMetadata:
        for sim, recs in self._world.items():
            for record in recs:
                if _url(record) == url:
                    attrs = dict(self._headers[sim])
                    return HeaderMetadata(attrs=attrs, source_url=url)
        raise OSError(f"no header at {url}")


def _walk(repository, world, roots, **kwargs):
    repository.record_run(roots, endpoint_url="u", spec={}, tag="uc")  # Step 1
    return resolve_parent_chains(
        roots,
        client=world,
        repository=repository,
        reader=world.reader,
        use_timeout=False,
        **kwargs,
    )


def test_resolves_and_links_a_correct_one_hop_parent(repository):
    child = _rec("M", "abrupt-4xCO2", "r1", "tas")
    parent = _rec("M", "piControl", "r1", "tas")
    world = _World(
        {
            ("M", "abrupt-4xCO2", "r1"): [child],
            ("M", "piControl", "r1"): [parent],
        },
        {
            ("M", "abrupt-4xCO2", "r1"): _parent_attrs("M", "piControl", "r1"),
            ("M", "piControl", "r1"): _NO_PARENT,
        },
        existing_experiments={"piControl"},
    )

    result = _walk(repository, world, [child], stopping_experiment="piControl")

    assert result.links == [(child.instance_key, parent.instance_key)]
    assert ("M", "piControl", "r1") in result.terminals
    assert world.parent_searches == [("M", "piControl", "r1")]  # one search, one parent
    assert (
        repository.latest_version_for("M", "piControl", "r1", "tas", "Amon", "gn")
        == parent.instance_key
    )


def test_missing_stopping_experiment_raises_before_walking(repository):
    child = _rec("M", "abrupt-4xCO2", "r1", "tas")
    world = _World(
        {("M", "abrupt-4xCO2", "r1"): [child]},
        {("M", "abrupt-4xCO2", "r1"): _parent_attrs("M", "piControl", "r1")},
        existing_experiments={"piControl"},  # 'picontrole' typo does not exist
    )

    with pytest.raises(ParentExperimentMissingError, match="picontrole"):
        _walk(repository, world, [child], stopping_experiment="picontrole")
    assert world.parent_searches == []  # never walked


def test_declared_parent_absent_from_index_raises(repository):
    child = _rec("M", "abrupt-4xCO2", "r1", "tas")
    world = _World(
        {("M", "abrupt-4xCO2", "r1"): [child]},  # piControl simulation NOT published
        {("M", "abrupt-4xCO2", "r1"): _parent_attrs("M", "piControl", "r1")},
        existing_experiments={"piControl"},  # the experiment exists, the sim does not
    )

    with pytest.raises(ParentResolutionError, match="no such dataset is published"):
        _walk(repository, world, [child], stopping_experiment="piControl")


def test_reaching_top_without_the_stopping_experiment_raises(repository):
    child = _rec("M", "G6solar", "r1", "tas")
    parent = _rec("M", "piControl", "r1", "tas")
    world = _World(
        {
            ("M", "G6solar", "r1"): [child],
            ("M", "piControl", "r1"): [parent],
        },
        {
            ("M", "G6solar", "r1"): _parent_attrs("M", "piControl", "r1"),
            ("M", "piControl", "r1"): _NO_PARENT,
        },
        existing_experiments={"ssp119", "piControl"},  # ssp119 exists, not on the chain
    )

    with pytest.raises(ParentResolutionError) as excinfo:
        _walk(repository, world, [child], stopping_experiment="ssp119")
    assert any(isinstance(f, ParentNotAncestor) for f in excinfo.value.failures)


def test_none_walks_to_the_no_parent_sentinel(repository):
    child = _rec("M", "G6solar", "r1", "tas")
    parent = _rec("M", "piControl", "r1", "tas")
    world = _World(
        {
            ("M", "G6solar", "r1"): [child],
            ("M", "piControl", "r1"): [parent],
        },
        {
            ("M", "G6solar", "r1"): _parent_attrs("M", "piControl", "r1"),
            ("M", "piControl", "r1"): _NO_PARENT,
        },
        existing_experiments=set(),
    )

    result = _walk(repository, world, [child], stopping_experiment=None)

    assert ("M", "piControl", "r1") in result.terminals
    assert result.links == [(child.instance_key, parent.instance_key)]
    assert result.hops == 2  # child hop, then piControl hop hits 'no parent'


def test_override_takes_precedence_over_the_header(repository):
    child = _rec("M", "abrupt-4xCO2", "r1", "tas")
    parent = _rec("M", "piControl", "r1", "tas")
    world = _World(
        {
            ("M", "abrupt-4xCO2", "r1"): [child],
            ("M", "piControl", "r1"): [parent],
        },
        # the child's header declares NO usable parent — only the override resolves it
        {
            ("M", "abrupt-4xCO2", "r1"): {},
            ("M", "piControl", "r1"): _NO_PARENT,
        },
        existing_experiments={"piControl"},
    )

    result = _walk(
        repository,
        world,
        [child],
        stopping_experiment="piControl",
        parent_overrides={("M", "abrupt-4xCO2", "r1"): ("M", "piControl", "r1")},
    )

    assert result.links == [(child.instance_key, parent.instance_key)]
