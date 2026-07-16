"""
One-hop, header-verified parent resolution (the Gregory / forcing use case)

Use case 1 reads and caches one header per *simulation* through
`cmip_data_manager.search.headers.enrich_headers`.  Use case 2 is the first that
must *follow* a parent link discovered in a header rather than assume it: for each
abrupt-forcing run (`abrupt-4xCO2`, and when present `abrupt-2xCO2` /
`abrupt-0p5xCO2`) we read its header, take the declared `parent_*` metadata, and
verify the `piControl` it points at — **without** assuming the parent shares the
child's `variant_label` (it legitimately may not).

This module is a thin layer *on top of* `enrich_headers`, not a second way to read
headers: the abrupt children and their piControl parents are each enriched by the
same health-aware, timeout/retry-bounded, persisted pipeline, and the parent link
is projected out of the `parent_*` columns that pipeline already stores.  The flow
is:

1. enrich every abrupt child (one read per simulation, every variable's row saved);
2. project each child's declared `piControl` parent from its stored header;
3. de-duplicate the parents (a piControl shared by several abrupt variants of one
   model is read once) and, for a declared parent absent from the run's datasets,
   optionally issue a **targeted re-fetch** for that exact
   `(source_id, variant_label, piControl)`;
4. enrich the distinct parents once (already-cached parents are skipped);
5. verify each parent — its header must be readable and its own identity attributes
   must not contradict what the child declared.

`enrich_with_parents` is deliberately **one hop** (abrupt-* -> piControl).
`enrich_parent_chains` builds on the same pieces to walk an arbitrary chain up the
parent tree **without assuming** its experiments: starting from a set of children
(e.g. `G6solar`), it reads a header, follows whatever parent that header declares,
reads *that* parent's header, and repeats until a header declares no parent (the
true top of the tree, e.g. `piControl`).  A shared ancestor is read once and a
metadata cycle is bounded by a hop cap.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal

from cmip_data_manager.db.repository import Repository
from cmip_data_manager.esgf.client import ESGFSearchClient
from cmip_data_manager.esgf.headers import (
    HeaderMetadata,
    SimulationKey,
    simulation_key,
)
from cmip_data_manager.esgf.health import NodeHealth
from cmip_data_manager.esgf.models import DatasetRecord
from cmip_data_manager.esgf.query import FacetQuery
from cmip_data_manager.search.headers import EnrichResult, enrich_headers

DEFAULT_CHILD_EXPERIMENTS: tuple[str, ...] = (
    "abrupt-4xCO2",
    "abrupt-2xCO2",
    "abrupt-0p5xCO2",
)
"""Abrupt-forcing experiments whose piControl parent this use case resolves."""

DEFAULT_PARENT_EXPERIMENT = "piControl"
"""The experiment the children are expected to branch from."""

DEFAULT_REQUIRED_VARS: tuple[str, ...] = ("tas", "rsdt", "rsut", "rlut")
"""Variables the forcing (Gregory) calculation needs; used when re-fetching a parent."""

NO_PARENT_SENTINELS: frozenset[str] = frozenset({"no parent"})
"""
CMIP6 marker(s) for "this run branches from nothing"

A run at the top of the tree (typically a `piControl-spinup`) sets its `parent_*`
attributes to the controlled-vocabulary string `"no parent"` rather than leaving
them blank.  Treating that as a real parent named `"no parent"` would send a walk
chasing a simulation that cannot exist; recognising the sentinel lets the walk stop
where the metadata says the tree ends.
"""


def _is_no_parent(value: str | None) -> bool:
    """Whether a `parent_*` attribute means "no parent" (blank or a CMIP6 sentinel)."""
    if value is None or not value.strip():
        return True
    return value.strip().casefold() in NO_PARENT_SENTINELS


LinkStatus = Literal[
    "verified", "parent_not_found", "parent_unread", "no_parent_metadata"
]
"""
Outcome of resolving one child's parent

- `verified` — the declared piControl was located and its header read, with its own
  identity attributes not contradicting what the child declared;
- `parent_not_found` — the child declared a piControl parent, but it is not in the
  run's datasets (nor recoverable by a re-fetch);
- `parent_unread` — the parent was located but its header could not be read (or its
  identity contradicts the declaration);
- `no_parent_metadata` — the child's header was unreadable or declared no usable
  `piControl` parent (e.g. missing `parent_variant_label`).
"""


@dataclass(frozen=True)
class ParentLink:
    """One child simulation's resolved (and verified) parent."""

    child: SimulationKey
    """The abrupt-forcing simulation `(source_id, experiment_id, variant_label)`."""

    parent: SimulationKey | None
    """The declared piControl parent, or `None` when none could be projected."""

    status: LinkStatus
    """How resolution ended (see `LinkStatus`)."""

    branch_time_in_parent: str | None = None
    """The child's `branch_time_in_parent` (kept for the later Gregory regression)."""

    same_variant: bool = False
    """Whether the parent's `variant_label` equals the child's (never assumed)."""

    refetched: bool = False
    """Whether the parent was recovered by a targeted re-fetch, not the initial run."""

    @property
    def verified(self) -> bool:
        """Whether the parent was located and its header confirmed."""
        return self.status == "verified"


@dataclass(frozen=True)
class ParentHopResult:
    """The outcome of a one-hop parent-resolution pass."""

    child_enrichment: EnrichResult
    """Header enrichment of the abrupt-forcing children."""

    parent_enrichment: EnrichResult
    """Header enrichment of the (distinct) piControl parents."""

    links: list[ParentLink] = field(default_factory=list)
    """One entry per child simulation, sorted by child key."""

    refetched: list[DatasetRecord] = field(default_factory=list)
    """Datasets pulled in by targeted re-fetches of otherwise-absent parents."""

    @property
    def verified(self) -> list[ParentLink]:
        """The links whose parent was verified."""
        return [link for link in self.links if link.verified]


ChainTerminal = Literal[
    "no_parent_metadata", "parent_not_found", "parent_unread", "max_hops"
]
"""
Why a chain stopped walking up the parent tree

- `no_parent_metadata` — the last node reached declared no parent, i.e. the true
  top of the tree (e.g. `piControl`); the only reason a chain is `complete`;
- `parent_not_found` — a node's declared parent could not be located (nor
  re-fetched), so the walk could go no further;
- `parent_unread` — a node's declared parent was located but its header could not
  be read (or its identity contradicts the declaration);
- `max_hops` — the hop cap was hit (a safety net against a metadata cycle).
"""


@dataclass(frozen=True)
class ParentChain:
    """One child's resolved path up the parent tree, hop by hop."""

    root: SimulationKey
    """The starting child simulation `(source_id, experiment_id, variant_label)`."""

    edges: list[ParentLink] = field(default_factory=list)
    """The verified hops, in order from `root` upward (empty if none verified)."""

    terminal: SimulationKey | None = None
    """The last simulation reached (the node whose onward edge stopped the walk)."""

    terminal_reason: ChainTerminal = "no_parent_metadata"
    """Why the walk stopped at `terminal` (see `ChainTerminal`)."""

    @property
    def complete(self) -> bool:
        """Whether the walk made at least one hop and reached the top of the tree."""
        return bool(self.edges) and self.terminal_reason == "no_parent_metadata"

    @property
    def simulations(self) -> list[SimulationKey]:
        """The simulations on the chain, from `root` to `terminal` inclusive."""
        return [self.root, *(edge.parent for edge in self.edges if edge.parent)]


@dataclass(frozen=True)
class ParentChainResult:
    """The outcome of a multi-hop parent walk."""

    chains: list[ParentChain] = field(default_factory=list)
    """One entry per starting child simulation, sorted by child key."""

    enrichment: list[EnrichResult] = field(default_factory=list)
    """Header enrichment of each hop's frontier, in walk order."""

    refetched: list[DatasetRecord] = field(default_factory=list)
    """Datasets pulled in by targeted re-fetches of ancestors absent from the run."""

    hops: int = 0
    """How many frontier hops the walk performed."""

    @property
    def reads(self) -> int:
        """Total headers read across every hop (each simulation read at most once)."""
        return sum(result.read for result in self.enrichment)

    @property
    def complete(self) -> list[ParentChain]:
        """The chains that walked all the way to the top of the tree."""
        return [chain for chain in self.chains if chain.complete]


@dataclass(frozen=True)
class _Declared:
    """A child's projected parent declaration (before it is located/verified)."""

    parent: SimulationKey | None
    branch_time_in_parent: str | None


def _records_by_simulation(
    records: Sequence[DatasetRecord],
) -> dict[SimulationKey, list[DatasetRecord]]:
    """Bucket datasets by their `(source_id, experiment_id, variant_label)`."""
    grouped: dict[SimulationKey, list[DatasetRecord]] = defaultdict(list)
    for record in records:
        sim = simulation_key(record)
        if sim is not None:
            grouped[sim].append(record)
    return grouped


def declared_parent(
    header: HeaderMetadata | None,
    child: SimulationKey,
    parent_experiment: str | None = None,
) -> _Declared:
    """
    Project a child's declared parent from its header's `parent_*` attributes

    The parent's `source_id` defaults to the child's when the header omits
    `parent_source_id` (a CMIP6 run's parent is the same model).  A parent is only
    returned when the header names a `parent_experiment_id` *and* a
    `parent_variant_label` — the variant is never assumed to match the child's.

    Parameters
    ----------
    header
        The child's stored header, or `None` if it could not be read.

    child
        The child simulation key `(source_id, experiment_id, variant_label)`.

    parent_experiment
        The experiment the parent must be (e.g. `"piControl"`).  Pass `None` (the
        default) to accept **whatever** experiment the header declares — the mode a
        multi-hop walk uses, where the chain of experiments is not assumed and each
        parent's experiment is discovered from the header itself.

    Returns
    -------
    :
        The declared parent simulation (or `None`) and the child's
        `branch_time_in_parent`.

    Examples
    --------
    >>> header = HeaderMetadata(
    ...     attrs={
    ...         "parent_experiment_id": "piControl",
    ...         "parent_variant_label": "r1i1p1f1",
    ...         "branch_time_in_parent": "0.0",
    ...     }
    ... )
    >>> declared_parent(header, ("M", "abrupt-4xCO2", "r1i1p1f2"), "piControl")
    _Declared(parent=('M', 'piControl', 'r1i1p1f1'), branch_time_in_parent='0.0')
    """
    if header is None:
        return _Declared(parent=None, branch_time_in_parent=None)
    branch = header.get("branch_time_in_parent")
    experiment = header.get("parent_experiment_id")
    variant = header.get("parent_variant_label")
    if experiment is None or variant is None:
        return _Declared(parent=None, branch_time_in_parent=branch)
    # A blank or the CMIP6 `"no parent"` sentinel also means "no parent".
    if _is_no_parent(experiment) or _is_no_parent(variant):
        return _Declared(parent=None, branch_time_in_parent=branch)
    if parent_experiment is not None and experiment != parent_experiment:
        return _Declared(parent=None, branch_time_in_parent=branch)
    source = header.get("parent_source_id") or child[0]
    parent = (source, experiment, variant)
    return _Declared(parent=parent, branch_time_in_parent=branch)


def _refetch_query(
    parent: SimulationKey,
    *,
    required_vars: Sequence[str],
    frequency: Sequence[str],
    project: str,
) -> FacetQuery:
    """Build a targeted query for one exact parent simulation."""
    source_id, experiment_id, variant_label = parent
    return FacetQuery(
        project=project,
        source_id=(source_id,),
        experiment_id=(experiment_id,),
        variant_label=(variant_label,),
        variable_id=tuple(required_vars),
        frequency=tuple(frequency),
    )


def _identity_confirms(header: HeaderMetadata, parent: SimulationKey) -> bool:
    """
    Whether a parent header's own identity attributes match the declaration

    Lenient: an attribute the file does not carry cannot contradict, so only a
    *present* value that disagrees fails the check.
    """
    source_id, experiment_id, variant_label = parent
    expected = {
        "source_id": source_id,
        "experiment_id": experiment_id,
        "variant_label": variant_label,
    }
    for attr, want in expected.items():
        actual = header.get(attr)
        if actual is not None and actual != want:
            return False
    return True


def enrich_with_parents(  # noqa: PLR0913 - a DI seam; every parameter has a default
    records: Sequence[DatasetRecord],
    *,
    client: ESGFSearchClient,
    repository: Repository,
    child_experiments: Sequence[str] = DEFAULT_CHILD_EXPERIMENTS,
    parent_experiment: str = DEFAULT_PARENT_EXPERIMENT,
    required_vars: Sequence[str] = DEFAULT_REQUIRED_VARS,
    frequency: Sequence[str] = ("mon",),
    project: str = "CMIP6",
    refetch_missing: bool = True,
    health: NodeHealth | None = None,
    **enrich_options: Any,
) -> ParentHopResult:
    """
    Enrich abrupt-forcing children, then resolve and verify their piControl parents

    A single pass that reads one header per abrupt child, follows each child's
    declared `parent_*` up to `piControl`, and reads that parent's header to verify
    it — de-duplicating parents shared across a model's abrupt variants so each is
    read only once, and (optionally) re-fetching a declared parent that is not among
    the run's datasets.

    Both header-reading phases go through `enrich_headers`, so all its behaviour
    (health-aware node selection, per-read timeout/retry, attempt logging, storing a
    row per variable while reading one header per simulation, and skipping
    already-cached headers) applies to children and parents alike.

    Parameters
    ----------
    records
        The use case's datasets (all experiments and variables).  Children are the
        subset in `child_experiments`; parents are located among these too.

    client
        Search client, used to look up files and to re-fetch absent parents.

    repository
        Cache to enrich into and to read stored `parent_*`/identity attributes from.

    child_experiments
        Abrupt-forcing experiments to resolve parents for.

    parent_experiment
        The experiment the children branch from (`"piControl"`).

    required_vars
        Variables the calculation needs; used to scope a parent re-fetch.

    frequency
        Frequency to scope a parent re-fetch to (e.g. `("mon",)`).

    project
        Project to scope a parent re-fetch to.

    refetch_missing
        When `True`, issue a targeted query for any declared parent not present in
        `records` before giving up on it.

    health
        Node-health registry shared across both enrichment phases; defaults to the
        one persisted in `repository`.  Do **not** also pass `health` in
        `enrich_options`.

    enrich_options
        Extra keyword arguments forwarded verbatim to both `enrich_headers` calls
        (e.g. `preferred_hosts`, `ignore_hosts`, `max_workers`, `skip_cached`).

    Returns
    -------
    :
        The two enrichment summaries and one `ParentLink` per child simulation.
    """
    health = repository.load_node_health() if health is None else health
    child_set = set(child_experiments)
    child_records = [r for r in records if r.experiment_id in child_set]

    child_enrichment = enrich_headers(
        child_records,
        client=client,
        repository=repository,
        health=health,
        **enrich_options,
    )

    child_sims = sorted(
        {sim for r in child_records if (sim := simulation_key(r)) is not None}
    )
    declared = {
        child: declared_parent(
            _first_header(repository, child), child, parent_experiment
        )
        for child in child_sims
    }

    by_sim = _records_by_simulation(records)
    wanted = {d.parent for d in declared.values() if d.parent is not None}
    present_initially = {parent for parent in wanted if parent in by_sim}

    refetched: list[DatasetRecord] = []
    if refetch_missing and (missing := sorted(wanted - present_initially)):
        queries = [
            _refetch_query(
                parent,
                required_vars=required_vars,
                frequency=frequency,
                project=project,
            )
            for parent in missing
        ]
        for result in client.search_many(queries):
            refetched.extend(result)
        for record in refetched:
            sim = simulation_key(record)
            if sim is not None:
                by_sim[sim].append(record)

    parent_records = _distinct_records(
        record for parent in wanted for record in by_sim.get(parent, ())
    )
    parent_enrichment = enrich_headers(
        parent_records,
        client=client,
        repository=repository,
        health=health,
        **enrich_options,
    )

    links = [
        _verify(repository, child, declared[child], by_sim, present_initially)
        for child in child_sims
    ]
    return ParentHopResult(
        child_enrichment=child_enrichment,
        parent_enrichment=parent_enrichment,
        links=links,
        refetched=refetched,
    )


def _first_header(repository: Repository, sim: SimulationKey) -> HeaderMetadata | None:
    """Return any one stored header for a simulation (they share `parent_*`)."""
    headers = repository.get_simulation_headers(*sim)
    return headers[0] if headers else None


def _distinct_records(records: Iterable[DatasetRecord]) -> list[DatasetRecord]:
    """De-duplicate datasets by id, preserving first-seen order."""
    by_id: dict[str, DatasetRecord] = {}
    for record in records:
        by_id.setdefault(record.id, record)
    return list(by_id.values())


def _verify(
    repository: Repository,
    child: SimulationKey,
    declaration: _Declared,
    by_sim: dict[SimulationKey, list[DatasetRecord]],
    present_initially: set[SimulationKey],
) -> ParentLink:
    """Classify one child's parent as verified, unread, not-found or absent."""
    parent = declaration.parent
    branch = declaration.branch_time_in_parent
    if parent is None:
        return ParentLink(
            child=child,
            parent=None,
            status="no_parent_metadata",
            branch_time_in_parent=branch,
        )

    same_variant = child[2] == parent[2]
    refetched = parent not in present_initially and parent in by_sim
    if parent not in by_sim:
        status: LinkStatus = "parent_not_found"
    else:
        header = _first_header(repository, parent)
        status = (
            "verified"
            if header is not None and _identity_confirms(header, parent)
            else "parent_unread"
        )
    return ParentLink(
        child=child,
        parent=parent,
        status=status,
        branch_time_in_parent=branch,
        same_variant=same_variant,
        refetched=refetched,
    )


def enrich_parent_chains(  # noqa: PLR0913 - a DI seam; every parameter has a default
    records: Sequence[DatasetRecord],
    *,
    client: ESGFSearchClient,
    repository: Repository,
    child_experiments: Sequence[str] = ("G6solar",),
    required_vars: Sequence[str] = ("tas",),
    frequency: Sequence[str] = ("mon",),
    project: str = "CMIP6",
    refetch_missing: bool = True,
    max_hops: int = 8,
    health: NodeHealth | None = None,
    **enrich_options: Any,
) -> ParentChainResult:
    """
    Walk each child's parent chain up the tree, making no assumptions

    A breadth-first walk that starts from the children in `child_experiments`,
    reads a header per simulation, follows whatever parent that header declares
    (its experiment is **not** assumed — see `declared_parent`), reads that
    parent's header, and repeats until a header declares no parent.  Every hop goes
    through `enrich_headers`, so its health-aware reads, per-read timeout/retry,
    attempt logging, per-variable rows and `skip_cached` all apply.

    A shared ancestor is read only once: a global visited set collapses the tree
    (many children fold into one scenario run, many scenario runs into one
    `historical`, and so on), and a metadata cycle is bounded by `max_hops`.

    Because the child search may not include the ancestors, each newly-discovered
    parent is located among `records` or, failing that (when `refetch_missing`),
    pulled in with a targeted re-fetch before the next hop reads it.

    Parameters
    ----------
    records
        The starting datasets (typically only the children and their variables).

    client
        Search client, used to re-fetch ancestors absent from `records`.

    repository
        Cache to enrich into and to read stored `parent_*`/identity attributes from.

    child_experiments
        Experiments whose simulations start a chain (e.g. `("G6solar",)`).

    required_vars, frequency, project
        Scope of a targeted ancestor re-fetch.

    refetch_missing
        When `True`, re-fetch a declared ancestor not present in `records`.

    max_hops
        Safety cap on the number of frontier hops (guards against a metadata cycle).

    health
        Node-health registry shared across every hop; defaults to the one persisted
        in `repository`.  Do **not** also pass `health` in `enrich_options`.

    enrich_options
        Extra keyword arguments forwarded verbatim to every `enrich_headers` call.

    Returns
    -------
    :
        One `ParentChain` per starting child, the per-hop enrichment summaries, and
        any datasets pulled in by re-fetches.
    """
    health = repository.load_node_health() if health is None else health

    by_sim = _records_by_simulation(records)
    present_initially = set(by_sim)

    child_set = set(child_experiments)
    roots = sorted(
        {
            sim
            for r in records
            if r.experiment_id in child_set and (sim := simulation_key(r)) is not None
        }
    )

    visited: set[SimulationKey] = set()
    declared: dict[SimulationKey, _Declared] = {}
    enrichment: list[EnrichResult] = []
    refetched: list[DatasetRecord] = []

    frontier = list(roots)
    hops = 0
    while frontier and hops < max_hops:
        hops += 1
        frontier_records = _distinct_records(
            record for sim in frontier for record in by_sim.get(sim, ())
        )
        enrichment.append(
            enrich_headers(
                frontier_records,
                client=client,
                repository=repository,
                health=health,
                **enrich_options,
            )
        )
        for sim in frontier:
            visited.add(sim)
            declared[sim] = declared_parent(_first_header(repository, sim), sim)

        wanted = {
            d.parent for sim in frontier if (d := declared[sim]).parent is not None
        }
        next_frontier = sorted(parent for parent in wanted if parent not in visited)
        refetched.extend(
            _locate_missing(
                client,
                by_sim,
                [parent for parent in next_frontier if parent not in by_sim],
                required_vars=required_vars,
                frequency=frequency,
                project=project,
                refetch_missing=refetch_missing,
            )
        )
        frontier = [parent for parent in next_frontier if parent in by_sim]

    edge_of = {
        sim: _verify(repository, sim, declaration, by_sim, present_initially)
        for sim, declaration in declared.items()
    }
    chains = [_assemble_chain(root, edge_of, max_hops) for root in roots]
    return ParentChainResult(
        chains=chains, enrichment=enrichment, refetched=refetched, hops=hops
    )


def _locate_missing(  # noqa: PLR0913 - internal helper; call site passes them all
    client: ESGFSearchClient,
    by_sim: dict[SimulationKey, list[DatasetRecord]],
    missing: Sequence[SimulationKey],
    *,
    required_vars: Sequence[str],
    frequency: Sequence[str],
    project: str,
    refetch_missing: bool,
) -> list[DatasetRecord]:
    """Re-fetch declared ancestors absent from the pool, adding them to `by_sim`."""
    if not (refetch_missing and missing):
        return []
    queries = [
        _refetch_query(
            parent, required_vars=required_vars, frequency=frequency, project=project
        )
        for parent in missing
    ]
    refetched: list[DatasetRecord] = []
    for result in client.search_many(queries):
        for record in result:
            refetched.append(record)
            sim = simulation_key(record)
            if sim is not None:
                by_sim[sim].append(record)
    return refetched


def _assemble_chain(
    root: SimulationKey,
    edge_of: dict[SimulationKey, ParentLink],
    max_hops: int,
) -> ParentChain:
    """Follow the per-simulation parent edges from `root` up to a terminal."""
    edges: list[ParentLink] = []
    seen = {root}
    sim = root
    while True:
        link = edge_of.get(sim)
        if link is None:
            # Located but not enriched (the walk hit its hop cap first).
            return ParentChain(root, edges, terminal=sim, terminal_reason="max_hops")
        if link.status != "verified" or link.parent is None:
            # A verified link always has a parent, so the else is unreachable; it
            # only keeps `reason` a `ChainTerminal` (never the "verified" literal).
            reason: ChainTerminal = (
                link.status if link.status != "verified" else "parent_unread"
            )
            return ParentChain(root, edges, terminal=sim, terminal_reason=reason)
        edges.append(link)
        parent = link.parent
        if parent in seen or len(edges) >= max_hops:
            return ParentChain(root, edges, terminal=parent, terminal_reason="max_hops")
        seen.add(parent)
        sim = parent
