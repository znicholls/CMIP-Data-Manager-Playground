"""Tests for Step 4: the node-independent parent walk (per-parent search + errors)."""

from __future__ import annotations

import httpx
import pytest

from cmip_data_manager.esgf.headers import HeaderMetadata
from cmip_data_manager.esgf.models import DatasetRecord, FileRecord
from cmip_data_manager.esgf.preflight import ProbeCache, ProbeOutcome
from cmip_data_manager.esgf.query import FacetQuery
from cmip_data_manager.search.parent_walk import (
    ParentExperimentMissingError,
    ParentNotAncestor,
    ParentResolutionError,
    find_parent_datasets,
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
        self.search_variable_ids: list[tuple[str, ...]] = []
        self.file_search_variables: list[str] = []
        self._by_id = {r.id: r for recs in records_by_sim.values() for r in recs}

    def count(self, query: FacetQuery) -> int:
        (experiment,) = query.experiment_id
        return 1 if experiment in self._existing else 0

    def search(self, query: FacetQuery) -> list[DatasetRecord]:
        sim = (query.source_id[0], query.experiment_id[0], query.variant_label[0])
        self.parent_searches.append(sim)
        self.search_variable_ids.append(query.variable_id)
        records = self._world.get(sim, [])
        if query.variable_id:  # variable-scoped search: an OR over variable_id
            records = [r for r in records if r.variable_id in query.variable_id]
        return list(records)

    def search_files(self, query: FacetQuery) -> list[FileRecord]:
        out: list[FileRecord] = []
        for dataset_id in query.dataset_id:
            record = self._by_id.get(dataset_id)
            if record is None:
                continue
            self.file_search_variables.append(record.variable_id)
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


def test_walk_searches_parents_scoped_to_requested_variables(repository):
    # The per-parent index search is variable-scoped: it carries the requested
    # variable_id(s), so it only follows the lineage we care about.
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

    _walk(
        repository,
        world,
        [child],
        stopping_experiment="piControl",
        variables=("tas",),
    )

    assert world.search_variable_ids == [("tas",)]  # scoped to the requested variable


def test_variable_gap_does_not_break_a_multi_variable_climb(repository):
    # Multi-variable request: the parent publishes `pr` but not `tas`.  The OR search
    # still finds the parent (via `pr`), so the chain resolves; the missing `tas` is a
    # VariableGap (a lineage hole for that one variable), not a broken chain.
    child_tas = _rec("M", "abrupt-4xCO2", "r1", "tas")
    child_pr = _rec("M", "abrupt-4xCO2", "r1", "pr")
    parent_pr = _rec("M", "piControl", "r1", "pr")  # piControl publishes pr, not tas
    world = _World(
        {
            ("M", "abrupt-4xCO2", "r1"): [child_tas, child_pr],
            ("M", "piControl", "r1"): [parent_pr],
        },
        {
            ("M", "abrupt-4xCO2", "r1"): _parent_attrs("M", "piControl", "r1"),
            ("M", "piControl", "r1"): _NO_PARENT,
        },
        existing_experiments={"piControl"},
    )

    result = _walk(
        repository,
        world,
        [child_tas, child_pr],
        stopping_experiment="piControl",
        variables=("tas", "pr"),
    )

    assert result.links == [(child_pr.instance_key, parent_pr.instance_key)]
    assert ("M", "piControl", "r1") in result.terminals  # chain still resolves
    assert [
        (g.child_version, g.parent, g.variable_id) for g in result.variable_gaps
    ] == [(child_tas.instance_key, ("M", "piControl", "r1"), "tas")]


def test_variable_scoped_search_reports_a_missing_variable_as_not_found(repository):
    # The accepted consequence of a fully variable-scoped search: a parent that exists
    # but does not publish the (only) requested variable comes back empty and so raises
    # ParentNotFound — the walk cannot tell it apart from a parent that does not exist.
    child = _rec("M", "abrupt-4xCO2", "r1", "tas")
    parent_pr = _rec("M", "piControl", "r1", "pr")  # exists, but only for pr
    world = _World(
        {
            ("M", "abrupt-4xCO2", "r1"): [child],
            ("M", "piControl", "r1"): [parent_pr],
        },
        {
            ("M", "abrupt-4xCO2", "r1"): _parent_attrs("M", "piControl", "r1"),
            ("M", "piControl", "r1"): _NO_PARENT,
        },
        existing_experiments={"piControl"},
    )

    with pytest.raises(ParentResolutionError, match="piControl"):
        _walk(
            repository,
            world,
            [child],
            stopping_experiment="piControl",
            variables=("tas",),
        )


def test_file_search_stays_scoped_to_the_requested_variable(repository):
    # An intermediate parent publishes tas AND pr, but the variable-scoped search only
    # returns the requested `tas` dataset, and the read collapses to one file per sim,
    # so add_files never fans across every variable (the >50k file-access blow-up).
    child = _rec("M", "G6solar", "r1", "tas")
    ssp_tas = _rec("M", "ssp585", "r1", "tas")
    ssp_pr = _rec("M", "ssp585", "r1", "pr")
    pic = _rec("M", "piControl", "r1", "tas")
    world = _World(
        {
            ("M", "G6solar", "r1"): [child],
            ("M", "ssp585", "r1"): [ssp_tas, ssp_pr],  # two variables published
            ("M", "piControl", "r1"): [pic],
        },
        {
            ("M", "G6solar", "r1"): _parent_attrs("M", "ssp585", "r1"),
            ("M", "ssp585", "r1"): _parent_attrs("M", "piControl", "r1"),
            ("M", "piControl", "r1"): _NO_PARENT,
        },
        existing_experiments={"piControl"},
    )

    result = _walk(
        repository,
        world,
        [child],
        stopping_experiment="piControl",
        variables=("tas",),
    )

    assert ("M", "piControl", "r1") in result.terminals
    assert "pr" not in world.file_search_variables  # never fetched the extra variable
    assert world.file_search_variables.count("tas") >= 2  # G6solar + ssp585 headers


class _StubClient:
    """Minimal search client: returns preset records, or raises if `error` is set."""

    def __init__(self, records=None, *, error=None):
        self._records = list(records or [])
        self._error = error
        self.searched = False
        self.last_query: FacetQuery | None = None

    def search(self, query: FacetQuery) -> list[DatasetRecord]:
        self.searched = True
        self.last_query = query
        if self._error is not None:
            raise self._error
        return list(self._records)


def test_find_parent_datasets_falls_through_an_empty_endpoint():
    parent_rec = _rec("M", "piControl", "r2", "tas")
    ceda = _StubClient([])  # CEDA doesn't publish this piControl variant
    ornl = _StubClient([parent_rec])  # ORNL does
    west = _StubClient([_rec("X", "piControl", "r9", "tas")])

    found = find_parent_datasets((ceda, ornl, west), ("M", "piControl", "r2"))

    assert found == [parent_rec]  # first non-empty endpoint wins
    assert ceda.searched and ornl.searched
    assert not west.searched  # short-circuits once found


def test_find_parent_datasets_skips_an_erroring_endpoint():
    parent_rec = _rec("M", "piControl", "r2", "tas")
    down = _StubClient(error=OSError("index 500"))
    up = _StubClient([parent_rec])

    found = find_parent_datasets((down, up), ("M", "piControl", "r2"))

    assert found == [parent_rec]
    assert down.searched and up.searched


def test_find_parent_datasets_skips_a_5xx_http_endpoint():
    # Regression: a 504 from ORNL is an httpx.HTTPStatusError, not an OSError; the
    # fallback must still fall through to the next endpoint (and not abort the walk).
    request = httpx.Request("GET", "https://esgf-node.ornl.gov/proxy/search")
    response = httpx.Response(504, request=request)
    ornl = _StubClient(
        error=httpx.HTTPStatusError("504", request=request, response=response)
    )
    west = _StubClient([_rec("M", "piControl", "r2", "tas")])

    found = find_parent_datasets((ornl, west), ("M", "piControl", "r2"))

    assert [r.experiment_id for r in found] == ["piControl"]
    assert ornl.searched and west.searched


def test_find_parent_datasets_constrains_the_search_to_requested_variables():
    # The walk should only follow the requested variable's lineage: the per-parent
    # index search is constrained to variable_id, not fanned across every variable.
    client = _StubClient([_rec("M", "piControl", "r2", "tas")])

    find_parent_datasets((client,), ("M", "piControl", "r2"), variables=("tas",))

    assert client.last_query is not None
    assert client.last_query.variable_id == ("tas",)


def test_find_parent_datasets_searches_any_variable_by_default():
    client = _StubClient([_rec("M", "piControl", "r2", "tas")])

    find_parent_datasets((client,), ("M", "piControl", "r2"))

    assert client.last_query is not None
    assert client.last_query.variable_id == ()


def test_find_parent_datasets_empty_when_no_endpoint_has_it():
    a = _StubClient([])
    b = _StubClient(error=OSError("boom"))

    assert find_parent_datasets((a, b), ("M", "piControl", "r2")) == []


# --- pre-flight node probe threading ----------------------------------------
def _one_hop_world():
    """A minimal child -> piControl world on the single host `nci`."""
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
    return world, child, parent


def test_walk_force_alive_keeps_the_happy_path(repository):
    # With the probe on but the only host forced alive, the walk resolves exactly as
    # it would without a probe — and no network probe is issued (force-alive skips it).
    world, child, parent = _one_hop_world()

    result = _walk(
        repository,
        world,
        [child],
        stopping_experiment="piControl",
        preflight_probe=True,
        force_alive_hosts=frozenset({"nci"}),
    )

    assert result.links == [(child.instance_key, parent.instance_key)]
    assert ("M", "piControl", "r1") in result.terminals


def test_walk_shares_one_probe_cache_and_a_dead_node_blocks_reads(repository):
    # A shared, pre-seeded cache marks the only host dead: every hop's header read is
    # blocked, so the child's declared parent can never be read and the chain fails.
    world, child, _ = _one_hop_world()
    cache = ProbeCache()
    cache.record(
        ProbeOutcome(
            host="nci",
            alive=False,
            reason="black hole",
            seconds=90.0,
            url="https://nci/f.nc",
            attempts=1,
        )
    )

    with pytest.raises(ParentResolutionError):
        _walk(
            repository,
            world,
            [child],
            stopping_experiment="piControl",
            preflight_probe=True,
            probe_cache=cache,
        )

    # The walk used the very cache we passed (shared across hops), and never probed the
    # already-known host again.
    assert cache.dead_hosts() == frozenset({"nci"})
