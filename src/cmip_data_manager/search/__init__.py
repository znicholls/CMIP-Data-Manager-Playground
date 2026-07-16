"""
High-level search use cases and the aggregation that backs them

The ESGF API cannot answer "which model variants have all of these variables in
these experiments?" directly, so these helpers issue the necessary queries and
intersect the results at `(source_id, variant_label, experiment_id)` granularity.
"""

from __future__ import annotations

from cmip_data_manager.search.aggregate import (
    Cell,
    ModelVariant,
    PairMatch,
    ParentResolver,
    build_cells,
    pairs_all_experiments,
    pairs_any_experiment,
)
from cmip_data_manager.search.headers import EnrichResult, enrich_headers
from cmip_data_manager.search.parent_hop import (
    ParentChain,
    ParentChainResult,
    ParentHopResult,
    ParentLink,
    declared_parent,
    enrich_parent_chains,
    enrich_with_parents,
)
from cmip_data_manager.search.parentage import (
    ParentConflict,
    parent_cell_of,
    resolve_parent_links,
)
from cmip_data_manager.search.runner import (
    ParentSpec,
    UseCase,
    UseCaseResult,
    discover_experiments,
    fetch_records,
    per_variable_experiment,
    run_use_case,
)

__all__ = [
    "Cell",
    "EnrichResult",
    "ModelVariant",
    "PairMatch",
    "ParentChain",
    "ParentChainResult",
    "ParentConflict",
    "ParentHopResult",
    "ParentLink",
    "ParentResolver",
    "ParentSpec",
    "UseCase",
    "UseCaseResult",
    "build_cells",
    "declared_parent",
    "discover_experiments",
    "enrich_headers",
    "enrich_parent_chains",
    "enrich_with_parents",
    "fetch_records",
    "pairs_all_experiments",
    "pairs_any_experiment",
    "parent_cell_of",
    "per_variable_experiment",
    "resolve_parent_links",
    "run_use_case",
]
