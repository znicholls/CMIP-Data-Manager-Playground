"""
CMIP7-specific side-table extraction

CMIP7 comes over ESGF-NG (STAC), so — unlike CMIP5 — its native ids are already in a
CMIP6-shaped DRS and need **no** reconstruction.  What it does need is the handful of
CMIP7-only facets that have no canonical `Dataset`/`DatasetVersion` column pulled out
of the retained STAC feature and onto a `Cmip7VersionExtra` row: the branding suffix
(also folded into `table_id`), the sampling `*_label`s that compose it, the `region`,
the full branded variable name and the licence / data-specs version.

The values live in the STAC feature's `properties` under the collection-facet prefix
(`cmip7:region`, …) — the same prefix `DatasetRecord.from_stac` derives — so this reads
them the same way, keeping the prefix **derived**, never hard-coded.  This module is
pure (reads the retained `raw` document, no I/O), so it is unit-testable in isolation.
"""

from __future__ import annotations

from typing import Any

from cmip_data_manager.esgf.models import DatasetRecord


def cmip7_extra_fields(record: DatasetRecord) -> dict[str, str | None]:
    """
    Extract the CMIP7-only side-table fields from a record's retained STAC feature

    Reads the `cmip7:`-prefixed facets that have no canonical column out of
    `record.raw["properties"]`.  The prefix is derived from the feature's `collection`
    (lower-cased), so the same helper serves any CMIP7-shaped collection.

    Parameters
    ----------
    record
        A CMIP7 `DatasetRecord` whose `raw` is the STAC feature (its `properties` carry
        the `cmip7:`-prefixed facets).

    Returns
    -------
    :
        A mapping ready to merge onto a `Cmip7VersionExtra` row (missing facets `None`).

    Examples
    --------
    >>> feature = {
    ...     "collection": "CMIP7",
    ...     "properties": {
    ...         "cmip7:variable_branding_suffix": "tavg-h2m-hxy-u",
    ...         "cmip7:variable_branded_name": "tas_tavg-h2m-hxy-u",
    ...         "cmip7:region": "glb",
    ...         "cmip7:temporal_label": "tavg",
    ...         "cmip7:area_label": "u",
    ...         "cmip7:license_id": "CC-BY-4.0",
    ...         "cmip7:realm": ["atmos"],
    ...     },
    ... }
    >>> rec = DatasetRecord(id="x", raw=feature)
    >>> fields = cmip7_extra_fields(rec)
    >>> fields["branding_suffix"], fields["region"], fields["realm"]
    ('tavg-h2m-hxy-u', 'glb', 'atmos')
    """
    collection = str(record.raw.get("collection", "") or "")
    prefix = f"{collection.lower()}:" if collection else ""
    props: dict[str, Any] = record.raw.get("properties", {}) or {}

    def facet(name: str) -> str | None:
        return _scalar(props.get(f"{prefix}{name}"))

    return {
        "branding_suffix": facet("variable_branding_suffix"),
        "branded_variable": facet("variable_branded_name"),
        "region": facet("region"),
        "temporal_label": facet("temporal_label"),
        "vertical_label": facet("vertical_label"),
        "horizontal_label": facet("horizontal_label"),
        "area_label": facet("area_label"),
        "license_id": facet("license_id"),
        "data_specs_version": facet("data_specs_version"),
        "realm": facet("realm"),
    }


def _scalar(value: Any) -> str | None:
    """Return a STAC property as a scalar (some, like `realm`, are 1-item lists)."""
    if isinstance(value, list):
        return None if not value else str(value[0])
    return None if value is None else str(value)


__all__ = ["cmip7_extra_fields"]
