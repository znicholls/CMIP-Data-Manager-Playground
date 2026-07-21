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
from cmip_data_manager.search.files import AddFilesResult, add_files
from cmip_data_manager.search.parent_walk import (
    ParentExperimentMissingError,
    ParentNotAncestor,
    ParentNotFound,
    ParentResolutionError,
    ParentWalkResult,
    declared_parent,
    resolve_parent_chains,
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
from cmip_data_manager.search.version_headers import (
    VersionEnrichResult,
    enrich_version_headers,
)

__all__ = [
    "AddFilesResult",
    "Cell",
    "ModelVariant",
    "PairMatch",
    "ParentConflict",
    "ParentExperimentMissingError",
    "ParentNotAncestor",
    "ParentNotFound",
    "ParentResolutionError",
    "ParentResolver",
    "ParentSpec",
    "ParentWalkResult",
    "UseCase",
    "UseCaseResult",
    "VersionEnrichResult",
    "add_files",
    "build_cells",
    "declared_parent",
    "discover_experiments",
    "enrich_version_headers",
    "fetch_records",
    "pairs_all_experiments",
    "pairs_any_experiment",
    "parent_cell_of",
    "per_variable_experiment",
    "resolve_parent_chains",
    "resolve_parent_links",
    "run_use_case",
]
