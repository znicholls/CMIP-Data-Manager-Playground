"""Tests for the multi-hop, header-verified parent walk (the G6solar case)."""

from __future__ import annotations

from urllib.parse import urlparse

from cmip_data_manager.esgf.headers import HeaderMetadata, SimulationKey
from cmip_data_manager.esgf.models import DatasetRecord, FileRecord
from cmip_data_manager.esgf.query import FacetQuery
from cmip_data_manager.search.parent_hop import declared_parent, enrich_parent_chains


class FakeClient:
    """A search client returning canned files, and canned datasets for re-fetches."""

    def __init__(
        self,
        files_by_dataset: dict[str, list[FileRecord]],
        refetchable: list[DatasetRecord] | None = None,
    ) -> None:
        self.files_by_dataset = files_by_dataset
        self.refetchable = refetchable or []
        self.search_calls = 0

    def search_files(self, query: FacetQuery) -> list[FileRecord]:
        out: list[FileRecord] = []
        for dsid in query.dataset_id:
            out.extend(self.files_by_dataset.get(dsid, []))
        return out

    def search(self, query: FacetQuery) -> list[DatasetRecord]:
        self.search_calls += 1
        return [
            record
            for record in self.refetchable
            if record.source_id in query.source_id
            and record.experiment_id in query.experiment_id
            and record.variant_label in query.variant_label
        ]

    def search_many(self, queries: list[FacetQuery]) -> list[list[DatasetRecord]]:
        return [self.search(query) for query in queries]


def _ds(
    experiment: str,
    variant: str,
    *,
    variable: str = "tas",
    source: str = "M",
    table: str = "Amon",
) -> DatasetRecord:
    dsid = f"{source}.{experiment}.{variant}.{variable}.{table}"
    return DatasetRecord(
        id=dsid,
        source_id=source,
        experiment_id=experiment,
        variant_label=variant,
        variable_id=variable,
        table_id=table,
        raw={},
    )


def _key(record: DatasetRecord) -> SimulationKey:
    return (record.source_id, record.experiment_id, record.variant_label)


def _file(record: DatasetRecord) -> FileRecord:
    url = f"https://nci/{record.id}.nc"
    return FileRecord(
        id=f"f.{record.id}",
        dataset_id=record.id,
        variable_id=record.variable_id,
        urls=(f"{url}|application/netcdf|HTTPServer",),
        raw={},
    )


def _attrs(
    record: DatasetRecord,
    *,
    parent: SimulationKey | None = None,
    branch: str = "10.0",
) -> dict:
    """A node's own identity attributes, plus `parent_*` when it declares a parent."""
    attrs = {
        "source_id": record.source_id,
        "experiment_id": record.experiment_id,
        "variant_label": record.variant_label,
    }
    if parent is not None:
        p_source, p_experiment, p_variant = parent
        attrs["parent_experiment_id"] = p_experiment
        attrs["parent_variant_label"] = p_variant
        if p_source != record.source_id:
            attrs["parent_source_id"] = p_source
        attrs["branch_time_in_parent"] = branch
    return attrs


def _reader(attrs_by_dataset: dict[str, dict], dead: frozenset[str] = frozenset()):
    """Return a reader mapping each dataset's mirror URL to canned header attrs."""

    def reader(url: str) -> HeaderMetadata:
        dataset_id = urlparse(url).path.lstrip("/").removesuffix(".nc")
        if dataset_id in dead:
            raise OSError(f"{dataset_id} refused")
        return HeaderMetadata(attrs=attrs_by_dataset[dataset_id], source_url=url)

    return reader


def _linear(steps: list[tuple[str, str]], *, source: str = "M"):
    """Build a linear chain (root first); each node declares the next as its parent."""
    records = [_ds(experiment, variant, source=source) for experiment, variant in steps]
    attrs: dict[str, dict] = {}
    for index, record in enumerate(records):
        parent = _key(records[index + 1]) if index + 1 < len(records) else None
        attrs[record.id] = _attrs(record, parent=parent)
    return records, attrs


def _walk(client, repository, records, attrs, *, dead=frozenset(), **overrides):
    return enrich_parent_chains(
        records,
        client=client,
        repository=repository,
        reader=_reader(attrs, dead=dead),
        use_timeout=False,
        max_attempts=1,
        **overrides,
    )


def _all_files(records):
    return {record.id: [_file(record)] for record in records}


def test_declared_parent_accepts_any_experiment_by_default():
    header = HeaderMetadata(
        attrs={
            "parent_experiment_id": "ssp585",
            "parent_variant_label": "r1i1p1f1",
            "branch_time_in_parent": "5.0",
        }
    )
    child = ("M", "G6solar", "r1i1p1f1")
    assert declared_parent(header, child).parent == ("M", "ssp585", "r1i1p1f1")
    # A fixed expected experiment still filters a non-matching parent out.
    assert declared_parent(header, child, "piControl").parent is None


def test_declared_parent_recognises_the_no_parent_sentinel():
    # CMIP6 marks the top of the tree with the literal string "no parent".
    header = HeaderMetadata(
        attrs={
            "parent_experiment_id": "no parent",
            "parent_variant_label": "no parent",
        }
    )
    assert declared_parent(header, ("M", "piControl-spinup", "r1i1p1f1")).parent is None


def test_chain_terminates_at_a_no_parent_node(repository):
    child = _ds("G6solar", "r1i1p1f1")
    pic = _ds("piControl", "r1i1p1f1")
    spinup = _ds("piControl-spinup", "r1i1p1f1")
    attrs = {
        child.id: _attrs(child, parent=_key(pic)),
        pic.id: _attrs(pic, parent=_key(spinup)),
        # The spinup declares the CMIP6 "no parent" sentinel: the true top.
        spinup.id: {
            "source_id": "M",
            "experiment_id": "piControl-spinup",
            "variant_label": "r1i1p1f1",
            "parent_experiment_id": "no parent",
            "parent_variant_label": "no parent",
        },
    }
    records = [child, pic, spinup]
    client = FakeClient(_all_files(records), refetchable=[pic, spinup])

    result = _walk(client, repository, [child], attrs)

    chain = result.chains[0]
    assert chain.complete
    assert chain.terminal == _key(spinup)
    assert chain.terminal_reason == "no_parent_metadata"
    assert [edge.parent[1] for edge in chain.edges] == ["piControl", "piControl-spinup"]


def test_full_chain_reaches_the_top_of_the_tree(repository):
    records, attrs = _linear(
        [
            ("G6solar", "r1i1p1f1"),
            ("ssp585", "r1i1p1f1"),
            ("historical", "r1i1p1f1"),
            ("piControl", "r1i1p1f1"),
        ]
    )
    child, *ancestors = records
    client = FakeClient(_all_files(records), refetchable=ancestors)

    result = _walk(client, repository, [child], attrs)

    assert len(result.chains) == 1
    chain = result.chains[0]
    assert chain.complete
    assert result.complete == result.chains
    assert chain.terminal == ("M", "piControl", "r1i1p1f1")
    assert chain.terminal_reason == "no_parent_metadata"
    assert [edge.parent[1] for edge in chain.edges] == [
        "ssp585",
        "historical",
        "piControl",
    ]
    assert chain.simulations == [_key(record) for record in records]
    assert result.hops == 4
    assert result.reads == 4  # one header per node, none re-read


def test_shared_ancestors_are_read_once(repository):
    child_a = _ds("G6solar", "r1i1p1f1")
    child_b = _ds("G6solar", "r2i1p1f1")
    ssp = _ds("ssp585", "r1i1p1f1")
    hist = _ds("historical", "r1i1p1f1")
    pic = _ds("piControl", "r1i1p1f1")
    attrs = {
        child_a.id: _attrs(child_a, parent=_key(ssp)),
        child_b.id: _attrs(child_b, parent=_key(ssp)),  # both share one ssp585
        ssp.id: _attrs(ssp, parent=_key(hist)),
        hist.id: _attrs(hist, parent=_key(pic)),
        pic.id: _attrs(pic),
    }
    records = [child_a, child_b, ssp, hist, pic]
    client = FakeClient(_all_files(records), refetchable=[ssp, hist, pic])

    result = _walk(client, repository, [child_a, child_b], attrs)

    assert len(result.chains) == 2
    assert all(chain.complete for chain in result.chains)
    # 2 children + one each of ssp585/historical/piControl (not once per branch).
    assert result.reads == 5
    assert {chain.edges[0].parent for chain in result.chains} == {_key(ssp)}


def test_only_absent_ancestors_are_refetched(repository):
    records, attrs = _linear(
        [
            ("G6solar", "r1i1p1f1"),
            ("ssp585", "r1i1p1f1"),
            ("historical", "r1i1p1f1"),
            ("piControl", "r1i1p1f1"),
        ]
    )
    child, ssp, hist, pic = records
    # ssp585 is already in the run; only historical and piControl need re-fetching.
    client = FakeClient(_all_files(records), refetchable=[hist, pic])

    result = _walk(client, repository, [child, ssp], attrs)

    assert {record.id for record in result.refetched} == {hist.id, pic.id}
    chain = result.chains[0]
    assert chain.complete
    refetched_by_experiment = {edge.parent[1]: edge.refetched for edge in chain.edges}
    assert refetched_by_experiment == {
        "ssp585": False,
        "historical": True,
        "piControl": True,
    }


def test_no_refetch_when_disabled(repository):
    records, attrs = _linear([("G6solar", "r1i1p1f1"), ("ssp585", "r1i1p1f1")])
    child, ssp = records
    client = FakeClient(_all_files([child]), refetchable=[ssp])

    result = _walk(client, repository, [child], attrs, refetch_missing=False)

    assert result.refetched == []
    assert client.search_calls == 0
    assert result.chains[0].terminal_reason == "parent_not_found"


def test_chain_stops_at_an_unlocatable_ancestor(repository):
    records, attrs = _linear(
        [("G6solar", "r1i1p1f1"), ("ssp585", "r1i1p1f1"), ("historical", "r1i1p1f1")]
    )
    child, ssp, _hist = records
    # historical is declared but is neither in the run nor re-fetchable.
    client = FakeClient(_all_files([child, ssp]), refetchable=[ssp])

    result = _walk(client, repository, [child], attrs)

    chain = result.chains[0]
    assert [edge.parent[1] for edge in chain.edges] == ["ssp585"]
    assert chain.terminal == _key(ssp)
    assert chain.terminal_reason == "parent_not_found"
    assert not chain.complete


def test_chain_stops_at_an_unreadable_ancestor(repository):
    records, attrs = _linear(
        [
            ("G6solar", "r1i1p1f1"),
            ("ssp585", "r1i1p1f1"),
            ("historical", "r1i1p1f1"),
            ("piControl", "r1i1p1f1"),
        ]
    )
    child, ssp, hist, pic = records
    client = FakeClient(_all_files(records), refetchable=[ssp, hist, pic])

    # historical is located but its mirror is dead, so its header cannot be read.
    result = _walk(client, repository, [child], attrs, dead=frozenset({hist.id}))

    chain = result.chains[0]
    assert [edge.parent[1] for edge in chain.edges] == ["ssp585"]
    assert chain.terminal == _key(ssp)
    assert chain.terminal_reason == "parent_unread"


def test_root_without_parent_metadata_is_a_single_node_chain(repository):
    child = _ds("G6solar", "r1i1p1f1")
    attrs = {child.id: _attrs(child)}  # no parent_* attributes
    client = FakeClient(_all_files([child]))

    result = _walk(client, repository, [child], attrs)

    chain = result.chains[0]
    assert chain.edges == []
    assert chain.terminal == _key(child)
    assert chain.terminal_reason == "no_parent_metadata"
    assert not chain.complete


def test_metadata_cycle_is_bounded(repository):
    node_a = _ds("expA", "r1i1p1f1")
    node_b = _ds("expB", "r1i1p1f1")
    attrs = {
        node_a.id: _attrs(node_a, parent=_key(node_b)),
        node_b.id: _attrs(node_b, parent=_key(node_a)),  # points back to the root
    }
    client = FakeClient(_all_files([node_a, node_b]), refetchable=[node_b])

    result = _walk(client, repository, [node_a], attrs, child_experiments=("expA",))

    chain = result.chains[0]
    assert len(chain.edges) == 2
    assert chain.terminal_reason == "max_hops"
    assert not chain.complete


def test_zero_max_hops_yields_bare_roots(repository):
    records, attrs = _linear([("G6solar", "r1i1p1f1"), ("ssp585", "r1i1p1f1")])
    child, ssp = records
    client = FakeClient(_all_files(records), refetchable=[ssp])

    result = _walk(client, repository, [child], attrs, max_hops=0)

    assert result.hops == 0
    assert result.enrichment == []
    chain = result.chains[0]
    assert chain.edges == []
    assert chain.terminal == _key(child)
    assert chain.terminal_reason == "max_hops"
