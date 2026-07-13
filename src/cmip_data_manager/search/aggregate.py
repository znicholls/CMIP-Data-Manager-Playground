"""
Client-side aggregation of datasets into model-variant matches

Because a single ESGF query cannot require a model variant to hold several
variables across several experiments, we fetch the relevant datasets and combine
them here.  The core structure is a mapping

    (source_id, variant_label, experiment_id) -> {variable_id present}

which we call the *cells*.  Two matching strategies are built on top of it:

- `pairs_all_experiments` (strict cross-product): every required experiment's cell
  must contain all required variables;
- `pairs_any_experiment` (within one experiment): at least one experiment's cell
  must contain all required variables.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field

from cmip_data_manager.esgf.models import DatasetRecord

Cell = tuple[str, str, str]
"""A single `(source_id, variant_label, experiment_id)` coordinate."""

Cells = dict[Cell, set[str]]
"""`(source_id, variant_label, experiment_id) -> set of variable_id present`."""

ParentResolver = Callable[[Cell], "Cell | None"]
"""
Return a cell's immediate parent cell, or `None` if it has none (or is unresolvable)

This is the dependency-injection seam through which parent-aware matching reaches
the netCDF headers: the search layer supplies a resolver backed by a client (see
`cmip_data_manager.search.parentage.make_parent_resolver`), while tests supply a
plain dictionary lookup.  Matching itself stays free of any I/O.
"""

_MAX_PARENT_HOPS = 5
"""Guard against runaway/cyclic parent chains (e.g. ssp -> historical -> piControl)."""


@dataclass(frozen=True, order=True)
class ModelVariant:
    """A `(source_id, variant_label)` pair (a model plus one of its variants)."""

    source_id: str
    variant_label: str


@dataclass(frozen=True)
class PairMatch:
    """
    A model variant that satisfied a use case, with supporting detail

    `experiments` are the experiments that fully satisfied the requirement (all
    required experiments for the cross-product strategy, or the qualifying
    experiments for the any-experiment strategy).
    """

    model_variant: ModelVariant
    experiments: tuple[str, ...]
    optional_experiments: tuple[str, ...] = field(default_factory=tuple)
    optional_variables: tuple[str, ...] = field(default_factory=tuple)
    parent_experiments: tuple[str, ...] = field(default_factory=tuple)
    """Required experiments that were satisfied via a parent link, not directly."""


def build_cells(records: Iterable[DatasetRecord]) -> Cells:
    """
    Aggregate dataset records into cells

    Parameters
    ----------
    records
        Datasets to aggregate.  Records missing any of `source_id`,
        `variant_label`, `experiment_id` or `variable_id` are ignored.

    Returns
    -------
    :
        The cells mapping.
    """
    cells: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    for record in records:
        source_id = record.source_id
        variant_label = record.variant_label
        experiment_id = record.experiment_id
        variable_id = record.variable_id
        if (
            source_id is None
            or variant_label is None
            or experiment_id is None
            or variable_id is None
        ):
            continue
        cells[(source_id, variant_label, experiment_id)].add(variable_id)
    return dict(cells)


def _pairs(cells: Cells) -> list[ModelVariant]:
    """Return the sorted, distinct model variants present in the cells."""
    return sorted({ModelVariant(s, v) for (s, v, _e) in cells})


def _covered_via_parent(
    cells: Cells,
    resolver: ParentResolver,
    base_cell: Cell,
    target_experiment: str,
    needed: set[str],
) -> bool:
    """
    Follow parent links from `base_cell` up to `target_experiment` and check coverage

    Walks one hop at a time (e.g. `ssp245 -> historical -> piControl`), stopping when
    it reaches the target experiment, runs out of parents, or would revisit a cell.
    """
    seen: set[Cell] = {base_cell}
    cell = base_cell
    for _ in range(_MAX_PARENT_HOPS):
        parent = resolver(cell)
        if parent is None or parent in seen:
            return False
        seen.add(parent)
        if parent[2] == target_experiment:
            return needed <= cells.get(parent, set())
        cell = parent
    return False


def _experiment_covered(  # noqa: PLR0913 - a coverage check over several coordinates
    cells: Cells,
    resolver: ParentResolver | None,
    key: tuple[str, str],
    experiment: str,
    needed: set[str],
    via_parent: Mapping[str, str],
) -> tuple[bool, bool]:
    """
    Report whether a pair covers `experiment`, and whether it did so via a parent

    Coverage is checked directly first; only if that fails (and `experiment` has a
    base experiment in `via_parent`) is the parent chain walked.
    """
    if needed <= cells.get((*key, experiment), set()):
        return True, False
    base = via_parent.get(experiment)
    if base is None or resolver is None:
        return False, False
    reached = _covered_via_parent(cells, resolver, (*key, base), experiment, needed)
    return reached, reached


def pairs_all_experiments(  # noqa: PLR0913 - required/optional/parent matching knobs
    cells: Cells,
    required_vars: Sequence[str],
    required_experiments: Sequence[str],
    optional_experiments: Sequence[str] = (),
    *,
    resolver: ParentResolver | None = None,
    via_parent: Mapping[str, str] | None = None,
) -> list[PairMatch]:
    """
    Find model variants covering all required variables in all required experiments

    This is the strict cross-product rule: a pair qualifies only if, for *every*
    required experiment, that experiment's cell contains *all* required variables.

    A required experiment listed in `via_parent` may instead be satisfied through a
    parent link.  For example `via_parent={"piControl": "abrupt-4xCO2"}` means "if
    this variant has no `piControl` of its own, follow the parent chain from its
    `abrupt-4xCO2` run" — which is how CMIP6 experiments whose parent is a *different*
    variant (e.g. HadGEM3-GC31-LL abrupt `r1i1p1f3` / piControl `r1i1p1f1`) are
    matched.  With no `resolver`/`via_parent` this is the plain strict rule.

    Parameters
    ----------
    cells
        Aggregated cells (see `build_cells`).

    required_vars
        Variables that must be present.

    required_experiments
        Experiments that must each contain all required variables.

    optional_experiments
        Extra experiments to note when they also contain all required variables.
        These never affect whether a pair qualifies.

    resolver
        Resolves a cell to its parent cell, enabling `via_parent` matching.  When
        `None`, only direct coverage counts.

    via_parent
        Maps a required experiment to the base experiment to walk its parent chain
        from when it is not covered directly.

    Returns
    -------
    :
        Matching pairs, sorted by model variant.  `parent_experiments` records any
        required experiments that were satisfied via a parent link.
    """
    needed = set(required_vars)
    links = dict(via_parent or {})
    matches: list[PairMatch] = []
    for pair in _pairs(cells):
        key = (pair.source_id, pair.variant_label)
        # Short-circuit on the first uncovered experiment: this avoids resolving a
        # parent (a network read) for a pair that is going to fail anyway, so list
        # the cheap/base experiments first.
        via_used: list[str] = []
        covered = True
        for exp in required_experiments:
            ok, used_parent = _experiment_covered(
                cells, resolver, key, exp, needed, links
            )
            if not ok:
                covered = False
                break
            if used_parent:
                via_used.append(exp)
        if not covered:
            continue
        via = tuple(via_used)
        optional = tuple(
            exp
            for exp in optional_experiments
            if needed <= cells.get((*key, exp), set())
        )
        matches.append(
            PairMatch(
                model_variant=pair,
                experiments=tuple(required_experiments),
                optional_experiments=optional,
                parent_experiments=via,
            )
        )
    return matches


def pairs_any_experiment(
    cells: Cells,
    required_vars: Sequence[str],
    optional_variable_preferences: Sequence[str] = (),
) -> list[PairMatch]:
    """
    Find model variants covering all required variables in some single experiment

    A pair qualifies if there exists at least one experiment whose cell contains
    all required variables.  All such experiments are reported.

    Parameters
    ----------
    cells
        Aggregated cells (see `build_cells`).

    required_vars
        Variables that must all be present together in one experiment.

    optional_variable_preferences
        Optional variables in order of preference (e.g. `("co2s", "co2")`).  The
        first that appears in any qualifying experiment is recorded; it never
        affects whether a pair qualifies.

    Returns
    -------
    :
        Matching pairs, sorted by model variant.
    """
    needed = set(required_vars)
    per_pair: dict[ModelVariant, dict[str, set[str]]] = defaultdict(dict)
    for (source_id, variant_label, experiment_id), variables in cells.items():
        per_pair[ModelVariant(source_id, variant_label)][experiment_id] = variables

    matches: list[PairMatch] = []
    for pair in sorted(per_pair):
        experiments = per_pair[pair]
        qualifying = tuple(
            sorted(exp for exp, variables in experiments.items() if needed <= variables)
        )
        if not qualifying:
            continue
        present = {var for exp in qualifying for var in experiments[exp]}
        optional = tuple(
            pref for pref in optional_variable_preferences if pref in present
        )[:1]
        matches.append(
            PairMatch(
                model_variant=pair,
                experiments=qualifying,
                optional_variables=optional,
            )
        )
    return matches
