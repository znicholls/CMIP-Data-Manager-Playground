"""
Select which dataset version(s) to carry from Step 1 into Steps 2-3

Step 1 now stores **every** published version of each dataset (the index search no
longer restricts to `latest`).  Steps 2 (files) and 3 (headers) act on a *chosen*
version per dataset — by default the true latest, decided from the `version` **date**
via `parse_version_date`, not from ESGF's `is_latest` flag (which data nodes disagree
about, so it is recorded but not trusted).

A caller may override the default:

- `"latest"` (default) — keep the newest version of each dataset;
- `"all"` — keep every version (no narrowing);
- an explicit `{master_id: version}` mapping — pin specific datasets to a chosen
  version (e.g. an older one), with any dataset not named in the mapping falling
  back to its latest.

The records handled here are node-specific `DatasetRecord`s (one per data node), so
all of a chosen version's records are kept — narrowing is by *version*, never by node.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from cmip_data_manager.db.schema import version_ordinal
from cmip_data_manager.esgf.models import DatasetRecord

VersionSelection = str | Mapping[str, str]
"""`"latest"`, `"all"`, or a `{master_id: version}` mapping of pinned versions."""


def latest_version(versions: Sequence[str]) -> str | None:
    """
    Return the newest version string by ordinal, or `None` if none is numeric

    "Newest" is decided by `version_ordinal` (versions are `[v]YYYYMMDD` dates or, for
    some CMIP5 datasets, plain integers), so it is robust to a mixed `v` prefix and to
    integer versions — unlike a plain string sort.  Non-numeric versions are ignored.

    Parameters
    ----------
    versions
        Candidate version strings for a single dataset.

    Returns
    -------
    :
        The version with the highest ordinal, or `None` if no candidate is numeric.

    Examples
    --------
    >>> latest_version(["v20200225", "20221112", "v20191115"])
    '20221112'
    >>> latest_version(["1", "2", "10"])
    '10'
    >>> latest_version(["not-a-date"]) is None
    True
    """
    ordinals: list[tuple[int, str]] = []
    for version in versions:
        try:
            ordinals.append((version_ordinal(version), version))
        except ValueError:
            continue
    if not ordinals:
        return None
    return max(ordinals, key=lambda pair: pair[0])[1]


def select_target_versions(
    records: Sequence[DatasetRecord],
    *,
    selection: VersionSelection = "latest",
) -> list[DatasetRecord]:
    """
    Keep only the records for each dataset's selected version

    Groups the node-specific `records` by dataset (`master_key`) and keeps those
    whose `version` matches the selection for that dataset.  See the module
    docstring for the `selection` modes.

    A dataset whose selected version cannot be determined (no date-parseable
    version, and no matching pin) is kept **in full** rather than silently dropped,
    so narrowing never loses a dataset.

    Parameters
    ----------
    records
        Node-specific dataset records from Step 1 (may span several versions).

    selection
        `"latest"`, `"all"`, or a `{master_id: version}` mapping of pinned versions.

    Returns
    -------
    :
        The subset of `records` for each dataset's selected version, in input order.
    """
    if selection == "all":
        return list(records)

    pins: Mapping[str, str] = selection if isinstance(selection, Mapping) else {}

    versions_by_master: dict[str, list[str]] = {}
    for record in records:
        if record.version is not None:
            versions_by_master.setdefault(record.master_key, []).append(record.version)

    # Resolve each dataset's target version once (a pin, else the latest by date).
    target_by_master: dict[str, str | None] = {}
    for master_id in {r.master_key for r in records}:
        target_by_master[master_id] = pins.get(master_id) or latest_version(
            versions_by_master.get(master_id, [])
        )

    # Single pass preserves input order.  An undetermined target keeps the whole
    # dataset rather than dropping it.
    return [
        record
        for record in records
        if target_by_master[record.master_key] in (None, record.version)
    ]
