"""
CMIP5-specific identity reconstruction

CMIP5's native `master_id`/`instance_id` are in CMIP5-DRS form (`cmip5.output1.
INSTITUTE.MODEL.experiment.time_frequency.realm.cmor_table.ensemble.vYYYYMMDD`), which
does not line up with the CMIP6-anchored canonical schema.  So for CMIP5 we **rebuild**
a CMIP6-shaped id from the canonical columns instead of trusting the raw ids.

Two user decisions are realised here (see the design interview):

1. **`grid_label`** — CMIP5 has none, but the reconstructed id needs that slot filled.
   We store the composite `"{source_id}_{experiment_id}_{realm}"` in `grid_label`
   (it deliberately repeats source/experiment; that is accepted).
2. **`master_id`** — the dotted join of the `Dataset` facet columns in schema order,
   `project.source_id.institution_id.experiment_id.variant_label.variable_id.frequency.table_id.grid_label`
   (**omitting** `nominal_resolution`, which is not a CMIP5 facet); `instance_id` is
   that plus `.version`.  This is the **base** id — it carries **no** product/collision
   suffix.  Disambiguating datasets that are identical across these columns but differ
   by an extra facet (CMIP5 `product`) is handled at DB-write time in the repository
   (it needs to see what has already been stored), never here.

This module is pure (reads `DatasetRecord` fields + the retained raw document, no I/O),
so it is unit-testable in isolation.  It assumes **one variable per record** — a CMIP5
table dataset carries many variables, so the search layer expands a table doc into one
record per requested variable *before* reconstruction runs.
"""

from __future__ import annotations

from typing import Any

from cmip_data_manager.esgf.models import DatasetRecord

# The Dataset facet columns, in schema order, that compose a reconstructed master_id.
# `nominal_resolution` is intentionally excluded (not a CMIP5 facet); `mip_era` is a
# discriminator, not part of the DRS-style id (and equals `project` for CMIP5).
_MASTER_ID_FACETS: tuple[str, ...] = (
    "project",
    "source_id",
    "institution_id",
    "experiment_id",
    "variant_label",
    "variable_id",
    "frequency",
    "table_id",
    "grid_label",
)


def cmip5_grid_label(record: DatasetRecord) -> str:
    """
    Build the CMIP5 `grid_label` composite `"{source_id}_{experiment_id}_{realm}"`

    CMIP5 has no grid label; this fills the slot with a composite of the model,
    experiment and (raw) realm.  Missing parts are dropped from the join.

    Parameters
    ----------
    record
        A CMIP5 `DatasetRecord` (canonical columns populated, `raw` holding the native
        document that carries `realm`).

    Returns
    -------
    :
        The composite grid label.

    Examples
    --------
    >>> rec = DatasetRecord(
    ...     id="i",
    ...     source_id="CESM1-CAM5",
    ...     experiment_id="rcp45",
    ...     raw={"realm": ["atmos"]},
    ... )
    >>> cmip5_grid_label(rec)
    'CESM1-CAM5_rcp45_atmos'
    """
    realm = _scalar(record.raw.get("realm"))
    return "_".join(
        part for part in (record.source_id, record.experiment_id, realm) if part
    )


def cmip5_base_master_id(record: DatasetRecord) -> str:
    """
    Join the `Dataset` facet columns into the base (suffix-free) CMIP5 master id

    Uses the record's canonical columns plus its `grid_label`; a missing column becomes
    an empty segment so the positional shape is preserved.  This is a **pure function of
    the columns**, so the same base is recomputed identically at reconstruction and at
    write time (the collision suffix is never part of it).

    Parameters
    ----------
    record
        A CMIP5 `DatasetRecord` whose `grid_label` is already the composite.

    Returns
    -------
    :
        The base master id (no product/collision suffix).

    Examples
    --------
    >>> rec = DatasetRecord(
    ...     id="i",
    ...     project="CMIP5",
    ...     source_id="CESM1-CAM5",
    ...     institution_id="NSF-DOE-NCAR",
    ...     experiment_id="rcp45",
    ...     variant_label="r1i1p1",
    ...     variable_id="tas",
    ...     frequency="mon",
    ...     table_id="Amon",
    ...     grid_label="CESM1-CAM5_rcp45_atmos",
    ...     raw={},
    ... )
    >>> cmip5_base_master_id(rec)
    'CMIP5.CESM1-CAM5.NSF-DOE-NCAR.rcp45.r1i1p1.tas.mon.Amon.CESM1-CAM5_rcp45_atmos'
    """
    values = record.model_dump()
    return ".".join(
        "" if values[name] is None else str(values[name]) for name in _MASTER_ID_FACETS
    )


def reconstruct_ids(record: DatasetRecord) -> DatasetRecord:
    """
    Rebuild a CMIP6-shaped base `master_id`/`instance_id`/`grid_label` for CMIP5

    Sets `grid_label` to the composite, `master_id` to the **base** (suffix-free) id and
    `instance_id` to that plus a `v`-normalised version.  The native CMIP5 `id` on
    `record.id` and the native ids in `record.raw` are left untouched — the file search
    (Step 2) and the CMIP5 side table still need them; the collision suffix (for
    datasets that differ only by `product`) is applied later, in the repository.

    Assumes the record already carries a **single** `variable_id` (the search layer
    expands a multi-variable CMIP5 table doc into one record per requested variable
    upstream).

    Parameters
    ----------
    record
        A CMIP5 `DatasetRecord` with canonical columns populated and `raw` holding the
        original CMIP5-native document.

    Returns
    -------
    :
        A copy with the reconstructed base identity fields.

    Examples
    --------
    >>> raw = {"realm": ["atmos"], "product": ["output2"], "version": ["20120601"]}
    >>> rec = DatasetRecord(
    ...     id="cmip5...|node",
    ...     project="CMIP5",
    ...     source_id="CESM1-CAM5",
    ...     institution_id="NSF-DOE-NCAR",
    ...     experiment_id="rcp45",
    ...     variant_label="r1i1p1",
    ...     variable_id="tas",
    ...     frequency="mon",
    ...     table_id="Amon",
    ...     version="20120601",
    ...     raw=raw,
    ... )
    >>> out = reconstruct_ids(rec)
    >>> out.grid_label
    'CESM1-CAM5_rcp45_atmos'
    >>> out.master_id  # base id, no product suffix here
    'CMIP5.CESM1-CAM5.NSF-DOE-NCAR.rcp45.r1i1p1.tas.mon.Amon.CESM1-CAM5_rcp45_atmos'
    >>> out.instance_id.endswith(".v20120601")
    True
    """
    grid_label = cmip5_grid_label(record)
    with_grid = record.model_copy(update={"grid_label": grid_label})
    master_id = cmip5_base_master_id(with_grid)
    version = _normalise_version(record.version or _native_version(record))
    instance_id = f"{master_id}.{version}" if version else master_id
    return record.model_copy(
        update={
            "grid_label": grid_label,
            "master_id": master_id,
            "instance_id": instance_id,
            "version": version,
        }
    )


def cmip5_extra_fields(record: DatasetRecord) -> dict[str, str | None]:
    """
    Extract the CMIP5-only side-table fields for a reconstructed record

    Returns the (recomputed) `base_master_id`, the native CMIP5 `realm`, and the native
    table ids from the retained raw document (which have **no** variable, because a
    CMIP5 dataset is table-grained).  The disambiguating `distinguishing_json` is added
    by the repository generically from the era's `distinguishing_facets`, not here.

    Parameters
    ----------
    record
        A reconstructed CMIP5 `DatasetRecord` (its `master_id` may already carry a
        collision suffix; `base_master_id` is recomputed from the columns, so it is
        suffix-free regardless).

    Returns
    -------
    :
        A mapping ready to merge onto a `Cmip5VersionExtra` row.

    Examples
    --------
    >>> raw = {
    ...     "realm": ["atmos"],
    ...     "master_id": ["cmip5.output2.NCAR.CESM1-CAM5.rcp45.mon.atmos.Amon.r1i1p1"],
    ...     "instance_id": ["cmip5.output2...r1i1p1.v20120601"],
    ... }
    >>> rec = DatasetRecord(
    ...     id="i",
    ...     project="CMIP5",
    ...     source_id="CESM1-CAM5",
    ...     institution_id="NSF-DOE-NCAR",
    ...     experiment_id="rcp45",
    ...     variant_label="r1i1p1",
    ...     variable_id="tas",
    ...     frequency="mon",
    ...     table_id="Amon",
    ...     grid_label="CESM1-CAM5_rcp45_atmos",
    ...     raw=raw,
    ... )
    >>> fields = cmip5_extra_fields(rec)
    >>> fields["realm"], fields["base_master_id"].endswith("atmos")
    ('atmos', True)
    >>> fields["native_master_id"].startswith("cmip5.output2")
    True
    """
    return {
        "base_master_id": cmip5_base_master_id(record),
        "realm": _scalar(record.raw.get("realm")),
        "native_master_id": _scalar(record.raw.get("master_id")),
        "native_dataset_id": _scalar(record.raw.get("instance_id")),
    }


def _scalar(value: Any) -> str | None:
    """Return the sole string for a raw field that arrives as a (1-item) list."""
    if isinstance(value, list):
        return None if not value else str(value[0])
    return None if value is None else str(value)


def _normalise_version(version: str | None) -> str | None:
    """Prefix a bare `YYYYMMDD` version with `v` to match the CMIP6 id shape."""
    if version is None:
        return None
    return version if version[:1].lower() == "v" else f"v{version}"


def _native_version(record: DatasetRecord) -> str | None:
    """Recover the version from the native ids when the `version` facet is absent."""
    key = record.instance_id or record.id.split("|", 1)[0]
    tail = key.rpartition(".")[2]
    return tail or None


__all__ = [
    "cmip5_base_master_id",
    "cmip5_extra_fields",
    "cmip5_grid_label",
    "reconstruct_ids",
]
