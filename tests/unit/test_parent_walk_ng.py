"""Step 4 (parent walk) on the ESGF-NG path: files from assets, search via the backend.

Mirrors the ESGF1 one-hop test but with STAC records — so the walk's per-hop Step 2
runs the assets transform (no `search_files`) and Step 3 reads headers from the asset
URLs, unchanged.
"""

from __future__ import annotations

from typing import Any

from cmip_data_manager.esgf.headers import HeaderMetadata
from cmip_data_manager.esgf.models import DatasetRecord
from cmip_data_manager.esgf.query import FacetQuery
from cmip_data_manager.search.parent_walk import resolve_parent_chains

_VERSION = "20240101"


def _asset_url(instance_id: str, variable: str) -> str:
    return f"https://dap.example.org/{instance_id}/{variable}.nc"


def _ng_rec(source: str, experiment: str, variant: str, variable: str) -> DatasetRecord:
    """Build a STAC-sourced dataset record with one data asset (its own href)."""
    master = f"CMIP6.CMIP.INST.{source}.{experiment}.{variant}.Amon.{variable}.gn"
    instance = f"{master}.v{_VERSION}"
    feature: dict[str, Any] = {
        "type": "Feature",
        "id": instance,
        "collection": "CMIP6",
        "properties": {
            "cmip6:source_id": source,
            "cmip6:experiment_id": experiment,
            "cmip6:variant_label": variant,
            "cmip6:variable_id": variable,
            "cmip6:table_id": "Amon",
            "cmip6:grid_label": "gn",
            "version": _VERSION,
            "latest": True,
        },
        "assets": {
            f"{variable}.nc": {
                "href": _asset_url(instance, variable),
                "type": "application/netcdf",
                "roles": ["data"],
                "file:size": 1,
                "cmip6:tracking_id": f"hdl:21.14100/{instance}",
            }
        },
    }
    return DatasetRecord.from_stac(feature)


def _parent_attrs(source: str, experiment: str, variant: str) -> dict[str, str]:
    return {
        "parent_source_id": source,
        "parent_experiment_id": experiment,
        "parent_variant_label": variant,
    }


_NO_PARENT = {"parent_experiment_id": "no parent", "parent_variant_label": "no parent"}


class _NgWorld:
    """A fake ESGF-NG index+data node: CQL2 search over sims, reader over asset URLs."""

    base_url = "https://search.east.esgf.io/search"

    def __init__(
        self,
        records_by_sim: dict[tuple[str, str, str], list[DatasetRecord]],
        headers: dict[tuple[str, str, str], dict[str, str]],
        existing_experiments: set[str],
    ) -> None:
        self._world = records_by_sim
        self._headers = headers
        self._existing = existing_experiments
        self.parent_searches: list[tuple[str, str, str]] = []

    def count(self, query: FacetQuery) -> int:
        (experiment,) = query.experiment_id
        return 1 if experiment in self._existing else 0

    def search(self, query: FacetQuery) -> list[DatasetRecord]:
        sim = (query.source_id[0], query.experiment_id[0], query.variant_label[0])
        self.parent_searches.append(sim)
        records = self._world.get(sim, [])
        if query.variable_id:
            records = [r for r in records if r.variable_id in query.variable_id]
        return list(records)

    def reader(self, url: str) -> HeaderMetadata:
        for sim, recs in self._world.items():
            for record in recs:
                if _asset_url(record.instance_id, record.variable_id or "") == url:
                    attrs = dict(self._headers[sim])
                    return HeaderMetadata(attrs=attrs, source_url=url)
        raise OSError(f"no header at {url}")


def test_ng_walk_resolves_one_hop_from_assets(repository):
    child = _ng_rec("M", "abrupt-4xCO2", "r1", "tas")
    parent = _ng_rec("M", "piControl", "r1", "tas")
    world = _NgWorld(
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
    repository.record_run([child], endpoint_url=world.base_url, spec={}, tag="uc")

    result = resolve_parent_chains(
        [child],
        client=world,
        repository=repository,
        reader=world.reader,
        use_timeout=False,
        stopping_experiment="piControl",
    )

    assert result.links == [(child.instance_key, parent.instance_key)]
    assert ("M", "piControl", "r1") in result.terminals
    assert world.parent_searches == [("M", "piControl", "r1")]
    # The link was made through a header read off the parent's STAC asset URL.
    assert (
        repository.latest_version_for("M", "piControl", "r1", "tas", "Amon", "gn")
        == parent.instance_key
    )
    # Files were populated from assets (no file search), so the child's file is stored.
    assert repository.version_has_files(child.instance_key)
