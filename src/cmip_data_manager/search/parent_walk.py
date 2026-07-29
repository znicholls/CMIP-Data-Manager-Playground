"""
Step 4 — walk the parent chain on the node-independent model, one search per parent

Starting from a set of child datasets, this reads each child's header-only metadata
(via Step 3), projects the declared parent `(source_id, experiment_id, variant_label)`,
and issues **one index search per distinct parent simulation** (never institution — a
parent's institution is unknown from the child, so it is discovered from the search;
`source_id` must match, and a missing `parent_source_id` defaults to the child's).  Each
found parent is stored, and the child *version* is linked to the parent *version* of the
same variable (`parent_version_key`); the walk repeats from the newly-found parents.

Stopping is **error-based, not warnings** (design decisions D5/D7/D8):

- **existence gate** — if a `stopping_experiment` is given, one search confirms it
  exists on the index node; if not, `ParentExperimentMissingError` is raised;
- reaching the `stopping_experiment` is success (that parent is a terminal; its header
  is not read and the walk does not go higher);
- `stopping_experiment=None` walks to the CMIP6 `"no parent"` sentinel (a natural top);
- a declared parent with **zero index results** is `ParentNotFound`; a chain that
  reaches the top **without** passing through a given `stopping_experiment` is
  `ParentNotAncestor`.  All chains are walked, then a single `ParentResolutionError`
  aggregates every failure.

A `parent_overrides` mapping (child simulation -> corrected parent simulation) takes
precedence over the header — the seam for user corrections / a future fixes library.
"""
# The per-parent search below (`find_parent_datasets`) is **variable-scoped**: it
# constrains `(source_id, experiment_id, variant_label)` to the requested `variables`
# (an OR across them).  A parent that publishes **none** of them yields an empty result
# and so a `ParentNotFound` — the search cannot tell that apart from a parent that does
# not exist at all.  (This reverses an earlier "variable-agnostic existence" design; the
# trade-off was made deliberately.)  The file/header work is collapsed to **one
# representative dataset per parent simulation** — a header describes the *run*, so any
# one file carries the same `parent_*` attributes and is enough to find the next
# parent — which is what keeps the file search from fanning out over every variable an
# ancestor publishes (the >50k file-access blow-up seen in the g6solar live test).  Each
# requested variable's child->parent version link is still made when the parent
# publishes it; a variable the found parent happens not to publish is recorded as a
# `VariableGap` (informational — the lineage resolved, that variable just has a hole
# here), not a chain-breaking error.

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

import httpx

from cmip_data_manager.db.repository import Repository
from cmip_data_manager.esgf.client import (
    DeepPaginationError,
    ESGFResponseError,
    ESGFSearchClient,
)
from cmip_data_manager.esgf.concurrency import MapFn, serial_map
from cmip_data_manager.esgf.dispatch import (
    DEFAULT_MAX_WORKERS,
    DEFAULT_NODE_CONCURRENCY,
)
from cmip_data_manager.esgf.headers import (
    DEFAULT_READ_TIMEOUT,
    HeaderMetadata,
    SimulationKey,
    read_header,
    simulation_key,
)
from cmip_data_manager.esgf.health import NodeHealth
from cmip_data_manager.esgf.models import DatasetRecord
from cmip_data_manager.esgf.preflight import DEFAULT_PROBE_READ_TIMEOUT, ProbeCache
from cmip_data_manager.esgf.query import FacetQuery
from cmip_data_manager.search.files import add_files
from cmip_data_manager.search.version_headers import (
    HeaderReader,
    enrich_version_headers,
)

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


@dataclass(frozen=True)
class _Declared:
    """A child's projected parent declaration (before it is located/verified)."""

    parent: SimulationKey | None
    branch_time_in_parent: str | None


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


class ParentExperimentMissingError(RuntimeError):
    """A user-declared stopping experiment does not exist on the index node."""

    def __init__(self, experiment: str) -> None:
        self.experiment = experiment
        super().__init__(
            f"The declared parent experiment {experiment!r} does not exist on the "
            f"index node."
        )


@dataclass(frozen=True)
class ParentNotFound:
    """A child declared a parent that has no datasets on the index node."""

    child: SimulationKey
    parent: SimulationKey

    def __str__(self) -> str:
        """Render the failure as the diagnostic message shown to the user."""
        c = " ".join(self.child)
        p = ", ".join(self.parent)
        return (
            f"{c} declares parent ({p}) but no such dataset is published on the "
            f"index node — the chain cannot be completed."
        )


@dataclass(frozen=True)
class ParentNotAncestor:
    """A chain reached the top without passing through the stopping experiment."""

    child: SimulationKey
    stopping_experiment: str

    def __str__(self) -> str:
        """Render the failure as the diagnostic message shown to the user."""
        c = " ".join(self.child)
        return (
            f"{c} reached the top of the parent tree ('no parent') without passing "
            f"through the declared stopping experiment "
            f"{self.stopping_experiment!r}."
        )


class ParentResolutionError(RuntimeError):
    """One or more chains could not be resolved (aggregates every failure)."""

    def __init__(self, failures: Sequence[ParentNotFound | ParentNotAncestor]) -> None:
        self.failures = list(failures)
        body = "\n".join(f"  - {failure}" for failure in failures)
        super().__init__(
            f"{len(failures)} parent chain(s) could not be resolved:\n{body}"
        )


@dataclass(frozen=True)
class VariableGap:
    """A parent simulation exists but does not publish a child version's variable

    The parent was found on the index (so the chain is **not** broken), but it
    does not publish the child's `variable_id`, so no same-variable version link can be
    made at this hop.  This is surfaced rather than raised: the *lineage* is intact, the
    requested *variable* simply is not available at that ancestor.
    """

    child_version: str
    """The child dataset version (`instance_key`) whose variable has no parent match."""

    parent: SimulationKey
    """The parent simulation `(source_id, experiment_id, variant_label)`."""

    variable_id: str
    """The variable the parent does not publish."""

    def __str__(self) -> str:
        """Render the gap as a human-readable diagnostic line."""
        p = ", ".join(self.parent)
        return (
            f"{self.child_version} has no parent version for variable "
            f"{self.variable_id!r} in parent ({p})"
        )


@dataclass(frozen=True)
class ParentWalkResult:
    """The outcome of a successful parent walk."""

    links: list[tuple[str, str]] = field(default_factory=list)
    """Resolved `(child_version_id, parent_version_id)` edges."""

    terminals: list[SimulationKey] = field(default_factory=list)
    """Simulations where a chain stopped (the stopping experiment or the true top)."""

    hops: int = 0
    """How many frontier hops the walk performed."""

    variable_gaps: list[VariableGap] = field(default_factory=list)
    """Child versions whose requested variable is absent from an *existing* parent — the
    simulation lineage resolved, but that variable's lineage has a hole at that ancestor
    (informational, never a failure)."""


# QUESTION: Removing default of CMIP6?
def parent_experiment_exists(
    client: ESGFSearchClient, experiment: str, *, project: str = "CMIP6"
) -> bool:
    """Whether any dataset for an experiment exists on the index node."""
    return client.count(FacetQuery(project=project, experiment_id=(experiment,))) > 0


def find_parent_datasets(
    clients: Sequence[ESGFSearchClient],
    parent: SimulationKey,
    *,
    project: str = "CMIP6",
    variables: Sequence[str] = (),
) -> list[DatasetRecord]:
    """
    Search the index for one parent simulation's datasets, with endpoint fallback

    Matches `(source_id, experiment_id, variant_label)`; a single simulation with too
    many replicas to page is retried for primary (non-replica) datasets only.
    Institution is never constrained.

    `variables` (e.g. `("tas",)`) constrains the search to those `variable_id`s (an OR
    across them).  The parent *walk* passes the requested variables here (see this
    module's header): a parent publishing none of them comes back empty and is reported
    as a `ParentNotFound`.  The empty default matches across *any* variable, for a
    caller that wants a pure existence check.

    `clients` are tried in **preference order** (e.g. CEDA, then ORNL, then
    metagrid-west): the first endpoint that returns datasets wins, and the search
    only falls through to the next endpoint when the current one finds **nothing** or
    errors.  This matters because a parent simulation (e.g. a specific `piControl`
    variant) can be published on one index node but not another — the query is by
    facet, not by a node-specific dataset id, so it is portable across endpoints.  An
    endpoint that raises is treated like an empty result and the next is tried; only
    if *every* endpoint is exhausted is the empty list returned.
    """
    source_id, experiment_id, variant_label = parent
    variable_id = tuple(variables)
    query = FacetQuery(
        project=project,
        source_id=(source_id,),
        experiment_id=(experiment_id,),
        variant_label=(variant_label,),
        variable_id=variable_id,
    )
    primary_only = FacetQuery(
        project=project,
        source_id=(source_id,),
        experiment_id=(experiment_id,),
        variant_label=(variant_label,),
        variable_id=variable_id,
        replica=False,
    )

    def _search_one(client: ESGFSearchClient) -> list[DatasetRecord]:
        try:
            return client.search(query)
        except DeepPaginationError:
            return client.search(primary_only)

    for client in clients:
        try:
            found = _search_one(client)
        except (httpx.HTTPError, ESGFResponseError, OSError):
            # endpoint down/erroring (a 5xx like ORNL's 504, a transport error, or a
            # bad payload) — treat like an empty result and fall through to the next.
            continue
        if found:
            return found
    return []


def resolve_parent_chains(  # noqa: PLR0913 - a DI seam; every parameter has a default
    root_records: Sequence[DatasetRecord],
    *,
    client: ESGFSearchClient,
    search_clients: Sequence[ESGFSearchClient] | None = None,
    repository: Repository,
    stopping_experiment: str | None = None,
    parent_overrides: Mapping[SimulationKey, SimulationKey] | None = None,
    project: str = "CMIP6",
    variables: Sequence[str] = (),
    max_hops: int = 8,
    map_fn: MapFn = serial_map,
    reader: HeaderReader = read_header,
    use_timeout: bool = True,
    health: NodeHealth | None = None,
    preferred_hosts: tuple[str, ...] = (),
    ignore_hosts: frozenset[str] = frozenset(),
    max_workers: int = DEFAULT_MAX_WORKERS,
    node_concurrency: int = DEFAULT_NODE_CONCURRENCY,
    timeout: float = DEFAULT_READ_TIMEOUT,
    skip_cached: bool = True,
    preflight_probe: bool = False,
    force_alive_hosts: frozenset[str] = frozenset(),
    probe_read_timeout: float = DEFAULT_PROBE_READ_TIMEOUT,
    probe_cache: ProbeCache | None = None,
) -> ParentWalkResult:
    """
    Walk each child's parent chain, linking versions and raising on any failure

    Parameters
    ----------
    root_records
        The starting child datasets (node-specific records).

    client, repository
        Search client and cache.  `client` runs the stopping-experiment existence gate
        and is the endpoint recorded against each hop's cached run.

    search_clients
        Preference-ordered clients for the walk's index searches, giving them endpoint
        fallback (backoff -> requeue -> next endpoint).  They drive **both** each hop's
        **Step 2** file search (via `add_files`) **and** each hop's per-parent dataset
        search (via `find_parent_datasets`), so a parent published on one index node
        but not another (e.g. a specific `piControl` variant) is still found.  Build
        them with `build_file_search_clients(...)`.  Defaults to `(client,)` — a single
        endpoint with no fallback — when omitted.

    stopping_experiment
        The experiment the chain should stop at; `None` walks to the `"no parent"`
        sentinel.  When given, it is confirmed to exist before walking.

    parent_overrides
        Child simulation -> corrected parent simulation; overrides the header.

    variables
        The originally-requested `variable_id`s (e.g. `("tas",)`).  They **scope** the
        per-parent index search (an OR across them): a parent publishing none of them
        comes back empty and so is a `ParentNotFound`.  Of a found parent's datasets,
        one representative per simulation carries into the next hop's header read (one
        file is enough — the header is variable-independent), while every requested
        variable a parent *does* publish is linked child->parent; a requested variable
        the parent lacks becomes a `VariableGap`, not a failure.  The empty default
        leaves the search variable-agnostic (a pure existence check).

    project, max_hops, map_fn, reader, use_timeout
        Search/read controls.

    health
        Node-health registry shared across every hop; defaults to the one persisted
        in `repository` (so health accumulates across runs).

    preferred_hosts, ignore_hosts, max_workers, node_concurrency, timeout
        Routing/read controls forwarded to each hop's `enrich_version_headers` (e.g.
        prefer NCI, avoid known-dead nodes); see that function.

    skip_cached
        Skip simulations whose versions already carry header metadata on each hop.

    preflight_probe, force_alive_hosts, probe_read_timeout
        Enable and tune the pre-flight node probe (see `enrich_version_headers`).  One
        `ProbeCache` is shared across **every hop**, so each data node is probed at most
        once per walk: hop 1 probes the roots' nodes, and a parent surfacing on a node
        no earlier hop touched is probed only when it appears.  Left **off** by default
        (the library stays network-free unless asked); the scripts turn it on.

    probe_cache
        The shared session `ProbeCache` the walk probes into; a fresh one is created
        when omitted.  Pass your own to seed verdicts or to read them back after the
        walk (the probe's alive/dead findings for every node it touched).

    Returns
    -------
    :
        The resolved edges, terminals and hop count.

    Raises
    ------
    ParentExperimentMissingError
        If `stopping_experiment` does not exist on the index node.

    ParentResolutionError
        If any chain declared a parent absent from the index, or reached the top
        without passing through `stopping_experiment`.
    """
    if stopping_experiment is not None and not parent_experiment_exists(
        client, stopping_experiment, project=project
    ):
        raise ParentExperimentMissingError(stopping_experiment)

    overrides = dict(parent_overrides or {})
    health = repository.load_node_health() if health is None else health
    resolved_search_clients = (
        (client,) if search_clients is None else tuple(search_clients)
    )
    state = _WalkState(
        client=client,
        search_clients=resolved_search_clients,
        repository=repository,
        stopping_experiment=stopping_experiment,
        overrides=overrides,
        project=project,
        variables=tuple(variables),
        map_fn=map_fn,
        reader=reader,
        use_timeout=use_timeout,
        health=health,
        preferred_hosts=preferred_hosts,
        ignore_hosts=ignore_hosts,
        max_workers=max_workers,
        node_concurrency=node_concurrency,
        timeout=timeout,
        skip_cached=skip_cached,
        preflight_probe=preflight_probe,
        force_alive_hosts=force_alive_hosts,
        probe_read_timeout=probe_read_timeout,
        probe_cache=ProbeCache() if probe_cache is None else probe_cache,
    )

    frontier = list(root_records)
    hops = 0
    while frontier and hops < max_hops:
        hops += 1
        frontier = state.hop(frontier)

    repository.save_node_health(health)
    if state.failures:
        raise ParentResolutionError(state.failures)
    return ParentWalkResult(
        links=state.links,
        terminals=state.terminals,
        hops=hops,
        variable_gaps=state.variable_gaps,
    )


@dataclass
class _WalkState:
    """Mutable state threaded through the walk's hops."""

    client: ESGFSearchClient
    search_clients: Sequence[ESGFSearchClient]
    repository: Repository
    stopping_experiment: str | None
    overrides: dict[SimulationKey, SimulationKey]
    project: str
    variables: Sequence[str]
    map_fn: MapFn
    reader: HeaderReader
    use_timeout: bool
    health: NodeHealth
    preferred_hosts: tuple[str, ...] = ()
    ignore_hosts: frozenset[str] = frozenset()
    max_workers: int = DEFAULT_MAX_WORKERS
    node_concurrency: int = DEFAULT_NODE_CONCURRENCY
    timeout: float = DEFAULT_READ_TIMEOUT
    skip_cached: bool = True
    preflight_probe: bool = False
    force_alive_hosts: frozenset[str] = frozenset()
    probe_read_timeout: float = DEFAULT_PROBE_READ_TIMEOUT
    probe_cache: ProbeCache = field(default_factory=ProbeCache)
    visited: set[SimulationKey] = field(default_factory=set)
    links: list[tuple[str, str]] = field(default_factory=list)
    terminals: list[SimulationKey] = field(default_factory=list)
    failures: list[ParentNotFound | ParentNotAncestor] = field(default_factory=list)
    variable_gaps: list[VariableGap] = field(default_factory=list)

    def hop(self, frontier: list[DatasetRecord]) -> list[DatasetRecord]:
        """Read a frontier's headers, resolve each declared parent, return the next."""
        by_sim: dict[SimulationKey, list[DatasetRecord]] = defaultdict(list)
        for record in frontier:
            sim = simulation_key(record)
            if sim is not None:
                by_sim[sim].append(record)

        self._read_headers(by_sim)

        declared = {sim: self._declared_parent(sim) for sim in by_sim}
        for sim in by_sim:
            self.visited.add(sim)

        wanted = sorted({p for p in declared.values() if p is not None})
        found = dict(self.map_fn(self._search, wanted))
        for parent, records in found.items():
            if records:
                self.repository.record_run(
                    records,
                    endpoint_url=self.client.base_url,
                    spec={"parent": list(parent)},
                )

        next_frontier: list[DatasetRecord] = []
        for sim, recs in by_sim.items():
            self._resolve(sim, recs, declared[sim], found, next_frontier)
        return [r for r in next_frontier if simulation_key(r) not in self.visited]

    def _read_headers(self, by_sim: dict[SimulationKey, list[DatasetRecord]]) -> None:
        """Ensure every frontier simulation has a header, reading only where needed.

        File access is **only** the substrate for the (variable-independent) header
        read, so it is gated to the minimum, while the header itself is promoted onto
        **every** variable's version of a simulation:

        - a simulation whose header is already stored — from *any* variable/version,
          including one read in an earlier run — needs **no file search**; the read
          step below copies that stored header onto its newly-searched versions;
        - a simulation that does need a read gets a file search for just **one
          representative dataset** — one file carries the whole run's `parent_*`, so
          there is no need to file-search every variable (the file-access blow-up
          Option B guarded against is avoided here, at the file gate).

        The header read/reuse then runs over the *whole* frontier, so both the
        just-read and the reused `parent_*` land on every version, not only the
        representative that was file-searched.
        """
        to_search: list[DatasetRecord] = []
        for sim, recs in by_sim.items():
            # File-search a representative unless the header is already stored and we
            # are honouring the cache (`skip_cached`); a forced re-read searches all.
            if not self.skip_cached or self.repository.simulation_header(*sim) is None:
                to_search.extend(self._read_representative(recs))
        if to_search:
            # Re-use Step 2 for the representatives' files, across the full preference-
            # ordered endpoint list so the walk gets the same backoff -> requeue ->
            # fallback resilience uc1's file step has.  A failed file search must not
            # hard-raise mid-walk (the read below handles a parent with no stored
            # files).
            add_files(
                to_search,
                clients=self.search_clients,
                repository=self.repository,
                map_fn=self.map_fn,
                raise_on_incomplete=False,
            )
        all_records = [record for recs in by_sim.values() for record in recs]
        enrich_version_headers(
            all_records,
            repository=self.repository,
            health=self.health,
            reader=self.reader,
            preferred_hosts=self.preferred_hosts,
            ignore_hosts=self.ignore_hosts,
            max_workers=self.max_workers,
            node_concurrency=self.node_concurrency,
            timeout=self.timeout,
            use_timeout=self.use_timeout,
            skip_cached=self.skip_cached,
            # Save-as-you-go: each header (and, throttled, the attempt log and node
            # health) is persisted the moment it is read, so killing a long walk keeps
            # every header already fetched.  persist_health stays False: the shared
            # health object is authoritatively saved once at the end of the walk.
            persist_health=False,
            persist_as_you_go=True,
            # Pre-flight probe (when enabled): the shared `probe_cache` spans every hop,
            # so a node is probed once per walk and parents on newly-seen nodes are
            # probed only when they appear.
            preflight_probe=self.preflight_probe,
            probe_cache=self.probe_cache,
            force_alive_hosts=self.force_alive_hosts,
            probe_read_timeout=self.probe_read_timeout,
        )

    def _search(
        self, parent: SimulationKey
    ) -> tuple[SimulationKey, list[DatasetRecord]]:
        """Search one parent simulation (the per-parent, parallelisable unit).

        **Variable-scoped**: the search is constrained to the requested `variables`
        (an OR across them), so it answers "does this parent publish any of the
        variables we care about?".  A parent that publishes **none** of them yields an
        empty result and so a `ParentNotFound` — the walk cannot cheaply tell that
        apart from a parent that does not exist at all.  Institution is never
        constrained (a parent's institution is unknown from the child).
        """
        return parent, find_parent_datasets(
            self.search_clients,
            parent,
            project=self.project,
            variables=self.variables,
        )

    def _declared_parent(self, sim: SimulationKey) -> SimulationKey | None:
        """Project a simulation's declared parent (override first, else its header).

        The header is looked up at the **simulation** grain (`simulation_header`) —
        header-only metadata is variable-independent, so whichever variable of the
        simulation was read (or reused from an earlier run) carries the same
        `parent_*`.
        """
        if sim in self.overrides:
            return self.overrides[sim]
        found = self.repository.simulation_header(*sim)
        header = found[0] if found is not None else None
        return declared_parent(header, sim, None).parent

    def _resolve(
        self,
        sim: SimulationKey,
        records: list[DatasetRecord],
        parent: SimulationKey | None,
        found: dict[SimulationKey, list[DatasetRecord]],
        next_frontier: list[DatasetRecord],
    ) -> None:
        """Link, terminate or record a failure for one simulation's declared parent."""
        if parent is None:  # reached the top of the tree
            if self.stopping_experiment is not None:
                self.failures.append(ParentNotAncestor(sim, self.stopping_experiment))
            else:
                self.terminals.append(sim)
            return
        if not found.get(parent):
            self.failures.append(ParentNotFound(sim, parent))
            return
        self._link(records, parent)
        if parent[1] == self.stopping_experiment:
            self.terminals.append(parent)  # reached target: stop, do not read higher
        elif parent not in self.visited:
            # Carry *all* the found parent's datasets forward so the next hop links
            # every requested variable; the file/header read is collapsed to one of
            # them inside `hop` (see `_read_representative`), so this does not re-open
            # the file-access blow-up.
            next_frontier.extend(found[parent])

    def _read_representative(self, records: list[DatasetRecord]) -> list[DatasetRecord]:
        """Pick one *version* of a simulation to read its (variable-independent) header.

        Only one file is needed to read a simulation's `parent_*` header — it describes
        the run, so every variable's file carries the same attributes — so collapsing to
        one version keeps the file search from fanning out over every variable an
        ancestor publishes (the >50k file-access blow-up).

        Crucially it returns **all** of the chosen version's node-specific records, not
        a single one: `add_files` searches a version by *every* node id it has, and
        dataset ids are node-specific, so a single arbitrary id may not be indexed by
        the file-search endpoints (a portability miss that leaves the sim with no files,
        so no readable header).  Prefer the **most-replicated** version — the most node
        ids gives both the best chance a portable id is found and the most live mirrors
        for the header read to fall back across.
        """
        if not records:
            return []
        by_version: dict[str, list[DatasetRecord]] = defaultdict(list)
        for record in records:
            by_version[record.instance_key].append(record)
        return max(
            by_version.values(),
            key=lambda recs: (len(recs), recs[0].instance_key),
        )

    def _link(self, records: list[DatasetRecord], parent: SimulationKey) -> None:
        """Link each child version to the parent version of the same variable.

        A requested child variable the parent does not publish is recorded as a
        `VariableGap` (the lineage is intact; only that variable is missing at this
        ancestor) instead of being silently dropped.
        """
        source_id, experiment_id, variant_label = parent
        for record in records:
            parent_version = self.repository.latest_version_for(
                source_id,
                experiment_id,
                variant_label,
                record.variable_id,
                record.table_id,
                record.grid_label,
            )
            if parent_version is not None:
                self.repository.set_parent_version(record.instance_key, parent_version)
                self.links.append((record.instance_key, parent_version))
            elif record.variable_id is not None and (
                not self.variables or record.variable_id in self.variables
            ):
                self.variable_gaps.append(
                    VariableGap(
                        child_version=record.instance_key,
                        parent=parent,
                        variable_id=record.variable_id,
                    )
                )
