"""Tests for the one-hop, header-verified parent resolution (use case 2)."""

from __future__ import annotations

from urllib.parse import urlparse

from cmip_data_manager.esgf.headers import HeaderMetadata
from cmip_data_manager.esgf.models import DatasetRecord, FileRecord
from cmip_data_manager.esgf.query import FacetQuery
from cmip_data_manager.search.parent_hop import (
    _identity_confirms,
    declared_parent,
    enrich_with_parents,
)


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


def _file(record: DatasetRecord) -> FileRecord:
    url = f"https://nci/{record.id}.nc"
    return FileRecord(
        id=f"f.{record.id}",
        dataset_id=record.id,
        variable_id=record.variable_id,
        urls=(f"{url}|application/netcdf|HTTPServer",),
        raw={},
    )


def _child_attrs(parent_variant: str, *, parent_experiment: str = "piControl") -> dict:
    return {
        "parent_experiment_id": parent_experiment,
        "parent_variant_label": parent_variant,
        "branch_time_in_parent": "10.0",
    }


def _parent_attrs(variant: str, *, source: str = "M") -> dict:
    return {
        "source_id": source,
        "experiment_id": "piControl",
        "variant_label": variant,
    }


def _reader(attrs_by_dataset: dict[str, dict], dead: frozenset[str] = frozenset()):
    """Return a reader mapping each dataset's mirror URL to canned header attrs."""

    def reader(url: str) -> HeaderMetadata:
        # URL is https://nci/<dataset_id>.nc — recover the dataset id.
        dataset_id = urlparse(url).path.lstrip("/").removesuffix(".nc")
        if dataset_id in dead:
            raise OSError(f"{dataset_id} refused")
        return HeaderMetadata(attrs=attrs_by_dataset[dataset_id], source_url=url)

    return reader


def _kw(reader, **overrides):
    """Fast, offline enrich options: injected reader, no subprocess, no retries."""
    kwargs = {"reader": reader, "use_timeout": False, "max_attempts": 1}
    kwargs.update(overrides)
    return kwargs


def _run(client, repository, records, attrs, *, dead=frozenset(), **overrides):
    return enrich_with_parents(
        records,
        client=client,
        repository=repository,
        **_kw(_reader(attrs, dead=dead), **overrides),
    )


def test_declared_parent_projects_from_header():
    header = HeaderMetadata(attrs=_child_attrs("r1i1p1f1"))
    declared = declared_parent(header, ("M", "abrupt-4xCO2", "r1i1p1f2"), "piControl")
    assert declared.parent == ("M", "piControl", "r1i1p1f1")
    assert declared.branch_time_in_parent == "10.0"


def test_declared_parent_none_when_no_variant():
    header = HeaderMetadata(attrs={"parent_experiment_id": "piControl"})
    declared = declared_parent(header, ("M", "abrupt-4xCO2", "r1"), "piControl")
    assert declared.parent is None


def test_identity_confirms_is_lenient_on_absent_attrs():
    parent = ("M", "piControl", "r1i1p1f1")
    assert _identity_confirms(HeaderMetadata(attrs={"source_id": "M"}), parent)
    assert not _identity_confirms(HeaderMetadata(attrs={"source_id": "X"}), parent)


def test_verifies_parent_with_a_different_variant(repository):
    child = _ds("abrupt-4xCO2", "r1i1p1f2")
    parent = _ds("piControl", "r1i1p1f1")
    client = FakeClient({child.id: [_file(child)], parent.id: [_file(parent)]})
    attrs = {child.id: _child_attrs("r1i1p1f1"), parent.id: _parent_attrs("r1i1p1f1")}

    result = _run(client, repository, [child, parent], attrs)

    assert [link.status for link in result.links] == ["verified"]
    assert result.verified == result.links  # the only child verified
    link = result.links[0]
    assert link.parent == ("M", "piControl", "r1i1p1f1")
    assert link.same_variant is False
    assert link.branch_time_in_parent == "10.0"


def test_no_parent_metadata_when_parent_is_a_different_experiment(repository):
    child = _ds("abrupt-4xCO2", "r1i1p1f2")
    client = FakeClient({child.id: [_file(child)]})
    # The header names a parent, but not the piControl we are hopping to.
    attrs = {child.id: _child_attrs("r1i1p1f1", parent_experiment="piControl-spinup")}

    result = _run(client, repository, [child], attrs)

    assert result.links[0].status == "no_parent_metadata"
    assert result.links[0].parent is None


def test_shared_picontrol_is_read_once(repository):
    child_a = _ds("abrupt-4xCO2", "r1i1p1f2")
    child_b = _ds("abrupt-2xCO2", "r1i1p1f3")
    parent = _ds("piControl", "r1i1p1f1")
    client = FakeClient(
        {
            child_a.id: [_file(child_a)],
            child_b.id: [_file(child_b)],
            parent.id: [_file(parent)],
        }
    )
    attrs = {
        child_a.id: _child_attrs("r1i1p1f1"),
        child_b.id: _child_attrs("r1i1p1f1"),
        parent.id: _parent_attrs("r1i1p1f1"),
    }

    result = _run(client, repository, [child_a, child_b, parent], attrs)

    assert result.child_enrichment.read == 2
    assert result.parent_enrichment.read == 1  # the shared piControl read once
    assert {link.status for link in result.links} == {"verified"}


def test_same_variant_parent_is_still_read(repository):
    child = _ds("abrupt-4xCO2", "r1i1p1f1")
    parent = _ds("piControl", "r1i1p1f1")
    client = FakeClient({child.id: [_file(child)], parent.id: [_file(parent)]})
    attrs = {child.id: _child_attrs("r1i1p1f1"), parent.id: _parent_attrs("r1i1p1f1")}

    result = _run(client, repository, [child, parent], attrs)

    assert result.parent_enrichment.read == 1  # not skipped just because it matches
    assert result.links[0].same_variant is True
    assert result.links[0].verified is True


def test_refetches_a_parent_absent_from_the_run(repository):
    child = _ds("abrupt-4xCO2", "r1i1p1f2")
    parent = _ds("piControl", "r9i9p9f9")
    client = FakeClient({child.id: [_file(child)]}, refetchable=[parent])
    client.files_by_dataset[parent.id] = [_file(parent)]
    attrs = {child.id: _child_attrs("r9i9p9f9"), parent.id: _parent_attrs("r9i9p9f9")}

    result = _run(client, repository, [child], attrs)

    assert client.search_calls == 1
    assert [record.id for record in result.refetched] == [parent.id]
    link = result.links[0]
    assert link.status == "verified"
    assert link.refetched is True


def test_parent_not_found_without_refetch(repository):
    child = _ds("abrupt-4xCO2", "r1i1p1f2")
    client = FakeClient({child.id: [_file(child)]})
    attrs = {child.id: _child_attrs("r9i9p9f9")}

    result = _run(client, repository, [child], attrs, refetch_missing=False)

    assert result.links[0].status == "parent_not_found"
    assert result.refetched == []


def test_no_parent_metadata_when_header_absent(repository):
    child = _ds("abrupt-4xCO2", "r1i1p1f2")
    client = FakeClient({child.id: [_file(child)]})
    # Header carries no parent facets at all.
    attrs = {child.id: {"source_id": "M"}}

    result = _run(client, repository, [child], attrs)

    link = result.links[0]
    assert link.status == "no_parent_metadata"
    assert link.parent is None


def test_no_parent_metadata_when_child_header_unreadable(repository):
    child = _ds("abrupt-4xCO2", "r1i1p1f2")
    client = FakeClient({child.id: [_file(child)]})
    attrs = {child.id: _child_attrs("r1i1p1f1")}

    # The abrupt child's own mirror is dead, so no parent can be projected.
    result = _run(client, repository, [child], attrs, dead=frozenset({child.id}))

    assert result.child_enrichment.failed
    assert result.links[0].status == "no_parent_metadata"


def test_parent_unread_when_its_header_fails(repository):
    child = _ds("abrupt-4xCO2", "r1i1p1f2")
    parent = _ds("piControl", "r1i1p1f1")
    client = FakeClient({child.id: [_file(child)], parent.id: [_file(parent)]})
    attrs = {child.id: _child_attrs("r1i1p1f1"), parent.id: _parent_attrs("r1i1p1f1")}

    # The piControl mirror is dead, so its header can't be read to verify.
    result = _run(
        client, repository, [child, parent], attrs, dead=frozenset({parent.id})
    )

    assert result.links[0].status == "parent_unread"
