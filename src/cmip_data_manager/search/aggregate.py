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
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field

from cmip_data_manager.esgf.models import DatasetRecord

Cells = dict[tuple[str, str, str], set[str]]
"""`(source_id, variant_label, experiment_id) -> set of variable_id present`."""


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


def pairs_all_experiments(
    cells: Cells,
    required_vars: Sequence[str],
    required_experiments: Sequence[str],
    optional_experiments: Sequence[str] = (),
) -> list[PairMatch]:
    """
    Find model variants covering all required variables in all required experiments

    This is the strict cross-product rule: a pair qualifies only if, for *every*
    required experiment, that experiment's cell contains *all* required variables.

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

    Returns
    -------
    :
        Matching pairs, sorted by model variant.
    """
    needed = set(required_vars)
    matches: list[PairMatch] = []
    for pair in _pairs(cells):
        key = (pair.source_id, pair.variant_label)
        if not all(
            needed <= cells.get((*key, exp), set()) for exp in required_experiments
        ):
            continue
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
