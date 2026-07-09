"""
High-level search use cases and the aggregation that backs them

The ESGF API cannot answer "which model variants have all of these variables in
these experiments?" directly, so these helpers issue the necessary queries and
intersect the results at `(source_id, variant_label, experiment_id)` granularity.
"""

from __future__ import annotations

from cmip_data_manager.search.aggregate import (
    ModelVariant,
    PairMatch,
    build_cells,
    pairs_all_experiments,
    pairs_any_experiment,
)
from cmip_data_manager.search.use_cases import (
    UseCase,
    UseCaseResult,
    discover_experiments,
    fetch_records,
    run_use_case,
    uc1_tas_ssp245,
    uc2_forcing,
    uc3_carbon,
    uc4_esm,
)

__all__ = [
    "ModelVariant",
    "PairMatch",
    "UseCase",
    "UseCaseResult",
    "build_cells",
    "discover_experiments",
    "fetch_records",
    "pairs_all_experiments",
    "pairs_any_experiment",
    "run_use_case",
    "uc1_tas_ssp245",
    "uc2_forcing",
    "uc3_carbon",
    "uc4_esm",
]
