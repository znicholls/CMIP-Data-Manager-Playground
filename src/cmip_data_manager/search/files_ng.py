"""
Step 2 on ESGF-NG — files come from the item's assets, not a file search

On ESGF1, Step 2 is a network *file search* per dataset version (with backoff,
requeue and endpoint fallback — see `search.files`).  On ESGF-NG there is **no file
search**: a STAC item already carries its files as `assets`, so Step 2 collapses to a
pure, in-process **transform** of the Step-1 records — no network, no
`IndexNodeHealth`, no endpoint fallback.

Each data-role asset becomes one logical `File`; its primary `href` plus any
alternate-asset hrefs (the STAC alternate-assets extension — one per host, the
node/replica equivalent) become that file's `FileAccess` rows.  The transform emits
the same `FileRecord`s the ESGF1 path produces, so `Repository.store_files` persists
them **unchanged** and Step 3 (the header read) then runs with no changes at all: the
candidate URLs it reads are the asset hrefs, host-ranked exactly as before.

The collection-facet prefix (`cmip6:tracking_id`, …) is **derived** from the item's
`collection`, never hard-coded, matching the rest of the ESGF-NG backend.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import Any
from urllib.parse import urlparse

from cmip_data_manager.db.repository import Repository
from cmip_data_manager.esgf.client import ESGFSearchClient
from cmip_data_manager.esgf.concurrency import MapFn, serial_map
from cmip_data_manager.esgf.models import DatasetRecord, FileRecord
from cmip_data_manager.search.files import AddFilesResult, add_files

_HTTP_SERVICE = "HTTPServer"
"""Service label for a byte-range-readable https asset (drives `fsspec_url`)."""

_DEFAULT_MIME = "application/netcdf"
"""MIME used when a STAC asset omits its `type` (CMIP6 assets are netCDF)."""


def is_stac_record(record: DatasetRecord) -> bool:
    """Whether a record came from an ESGF-NG (STAC) search — its raw is a Feature."""
    return record.raw.get("type") == "Feature" or "assets" in record.raw


def file_records_from_dataset(record: DatasetRecord) -> list[FileRecord]:
    """
    Turn one ESGF-NG dataset record's STAC `assets` into `FileRecord`s

    One `FileRecord` per data-role asset (a logical file), with its primary `href`
    and any alternate-asset hrefs rendered as the `url|mime|service` entries the
    storage layer expects — so a file served from several hosts collapses to one
    `File` with several `FileAccess` rows, just like an ESGF1 replica set.

    Parameters
    ----------
    record
        A dataset record from an ESGF-NG search (its `raw` is the STAC feature).

    Returns
    -------
    :
        One file record per readable data asset (assets with no http(s) href are
        skipped — e.g. Globus-only — since they cannot serve a byte-range header read).
    """
    feature = record.raw
    collection = str(feature.get("collection", "") or "")
    prefix = f"{collection.lower()}:" if collection else ""
    item_id = str(feature.get("id", "") or record.id)
    assets: dict[str, Any] = feature.get("assets", {}) or {}

    files: list[FileRecord] = []
    for name, asset in assets.items():
        if "data" not in (asset.get("roles") or []):
            continue
        urls = _asset_urls(asset)
        if not urls:  # no http(s) access (e.g. Globus-only) — cannot read a header
            continue
        tracking_id = asset.get(f"{prefix}tracking_id")
        files.append(
            FileRecord(
                id=str(tracking_id or name),
                dataset_id=item_id,
                title=name,
                size=asset.get("file:size"),
                checksum=asset.get("file:checksum"),
                checksum_type=None,
                tracking_id=tracking_id,
                urls=tuple(urls),
                raw=asset,
            )
        )
    return files


def add_files_from_assets(
    records: Sequence[DatasetRecord],
    *,
    repository: Repository,
    skip_cached: bool = True,
) -> AddFilesResult:
    """
    Populate each version's files from its STAC assets (no network), and store them

    A drop-in for `search.files.add_files` on the ESGF-NG path: it groups records by
    version, transforms each version's assets into `FileRecord`s and persists them via
    `Repository.store_files`, returning the same `AddFilesResult` shape.  There is no
    failure/overflow mode — the files are already in hand — so `failed`/`overflowed`
    are always empty.

    Parameters
    ----------
    records
        The Step-1 dataset records (ESGF-NG); each carries its files as STAC assets.

    repository
        Cache to check for already-stored files and to write results into.

    skip_cached
        Skip versions whose files are already stored (the caching behaviour).

    Returns
    -------
    :
        A summary of what was transformed, skipped and stored.
    """
    by_version: dict[str, list[DatasetRecord]] = defaultdict(list)
    for record in records:
        by_version[record.instance_key].append(record)

    files_by_version: dict[str, list[FileRecord]] = {}
    skipped = 0
    for version_key, recs in by_version.items():
        if skip_cached and repository.version_has_files(version_key):
            skipped += 1
            continue
        file_records: list[FileRecord] = []
        for record in recs:
            file_records.extend(file_records_from_dataset(record))
        files_by_version[version_key] = file_records

    files_stored = repository.store_files(files_by_version)
    return AddFilesResult(
        searched=len(files_by_version),
        skipped_cached=skipped,
        files_stored=files_stored,
    )


def add_files_auto(  # noqa: PLR0913 - a dispatch seam mirroring add_files' signature
    records: Sequence[DatasetRecord],
    *,
    repository: Repository,
    clients: Sequence[ESGFSearchClient] = (),
    map_fn: MapFn = serial_map,
    raise_on_incomplete: bool = True,
    skip_cached: bool = True,
) -> AddFilesResult:
    """
    Add each version's files, dispatching on dialect: assets transform vs file search

    A single Step-2 entry point that works for both backends.  When the records came
    from an ESGF-NG search (STAC), files are read from their `assets` in-process (no
    network, `clients` unused); otherwise the ESGF1 network file search runs with the
    given `clients` and its full backoff/requeue/endpoint-fallback resilience.  This
    lets the parent walk (and any caller) do Step 2 without knowing the dialect.

    Parameters
    ----------
    records
        The Step-1 dataset records to add files for.

    repository
        Cache to check for already-stored files and to write results into.

    clients
        Preference-ordered ESGF1 file-search clients (ignored on the ESGF-NG path).

    map_fn, raise_on_incomplete
        Forwarded to `search.files.add_files` on the ESGF1 path.

    skip_cached
        Skip versions whose files are already stored (both paths).

    Returns
    -------
    :
        The `AddFilesResult` from whichever path ran.
    """
    if records and all(is_stac_record(record) for record in records):
        return add_files_from_assets(
            records, repository=repository, skip_cached=skip_cached
        )
    return add_files(
        records,
        clients=clients,
        repository=repository,
        map_fn=map_fn,
        raise_on_incomplete=raise_on_incomplete,
        skip_cached=skip_cached,
    )


def _asset_urls(asset: dict[str, Any]) -> list[str]:
    """Render an asset's primary + alternate http(s) hrefs as `url|mime|service`."""
    mime = str(asset.get("type") or _DEFAULT_MIME)
    urls: list[str] = []
    seen: set[str] = set()
    hrefs = [asset.get("href")]
    for alt in (asset.get("alternate") or {}).values():
        hrefs.append(alt.get("href"))
    for href in hrefs:
        if isinstance(href, str) and _is_http(href) and href not in seen:
            urls.append(f"{href}|{mime}|{_HTTP_SERVICE}")
            seen.add(href)
    return urls


def _is_http(href: str) -> bool:
    """Whether an href is an http(s) URL (byte-range readable), not Globus/other."""
    return urlparse(href).scheme in ("http", "https")
