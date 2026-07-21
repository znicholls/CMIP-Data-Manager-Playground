"""
Step 2 — add files to each dataset version, one search per version

Where the old header pipeline OR-ed many datasets' ids into a few batched file
searches (needing a character budget and a bisect to dodge the index's 10000-result
retrieval cap), this issues **one file search per dataset version** and persists the
results immediately as `File` + `FileAccess` rows.  One search per version is:

- **traceable** — each query targets exactly one version, so "what was searched for
  this dataset?" has a single, obvious answer;
- **overflow-safe** — a single version's files (even across replicas) stay far below
  the retrieval cap, so no bisect is needed;
- **parallel** — the per-version searches fan out through the injected `MapFn`
  (search is HTTP I/O, so a thread pool is the right tool).

It is also a **caching step**: a version whose files are already stored is skipped.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

from cmip_data_manager.db.repository import Repository
from cmip_data_manager.esgf.client import DeepPaginationError, ESGFSearchClient
from cmip_data_manager.esgf.concurrency import MapFn, serial_map
from cmip_data_manager.esgf.models import DatasetRecord, FileRecord
from cmip_data_manager.esgf.query import FacetQuery


@dataclass(frozen=True)
class AddFilesResult:
    """Summary of a Step-2 file-adding pass."""

    searched: int = 0
    """Versions a file search was issued for."""

    skipped_cached: int = 0
    """Versions skipped because their files were already stored."""

    files_stored: int = 0
    """`File` rows written or updated."""

    overflowed: list[str] = field(default_factory=list)
    """Versions whose single-dataset file search still exceeded the retrieval cap."""


@dataclass(frozen=True)
class _VersionFiles:
    """A version's file-search outcome (carried through the `MapFn`)."""

    version_key: str
    files: list[FileRecord]
    overflowed: bool = False


def add_files(
    records: Sequence[DatasetRecord],
    *,
    client: ESGFSearchClient,
    repository: Repository,
    map_fn: MapFn = serial_map,
    skip_cached: bool = True,
) -> AddFilesResult:
    """
    Search each dataset version's files once and store them as `File`/`FileAccess`

    Parameters
    ----------
    records
        The datasets to add files for (node-specific records; grouped internally by
        version, and each version searched by its own node-specific dataset ids).

    client
        Search client used for the per-version file searches.

    repository
        Cache to check for already-stored files and to write results into.

    map_fn
        Strategy for running the per-version searches; defaults to serial.  Pass
        `thread_pool_map(...)` for parallelism (search is HTTP I/O).

    skip_cached
        Skip versions whose files are already stored (the caching behaviour).

    Returns
    -------
    :
        A summary of what was searched, skipped, stored and overflowed.
    """
    ids_by_version: dict[str, list[str]] = defaultdict(list)
    for record in records:
        ids_by_version[record.instance_key].append(record.id)

    todo: list[tuple[str, tuple[str, ...]]] = []
    skipped = 0
    for version_key, ids in ids_by_version.items():
        if skip_cached and repository.version_has_files(version_key):
            skipped += 1
            continue
        todo.append((version_key, tuple(ids)))

    results = list(map_fn(_lookup(client), todo)) if todo else []
    files_by_version = {vf.version_key: vf.files for vf in results if not vf.overflowed}
    overflowed = [vf.version_key for vf in results if vf.overflowed]

    stored = repository.store_files(files_by_version) if files_by_version else 0
    return AddFilesResult(
        searched=len(todo),
        skipped_cached=skipped,
        files_stored=stored,
        overflowed=overflowed,
    )


def _lookup(
    client: ESGFSearchClient,
) -> Callable[[tuple[str, tuple[str, ...]]], _VersionFiles]:
    """Build the per-version search worker bound to a client (for the `MapFn`)."""

    def search(item: tuple[str, tuple[str, ...]]) -> _VersionFiles:
        version_key, dataset_ids = item
        query = FacetQuery(type="File", dataset_id=dataset_ids)
        try:
            return _VersionFiles(version_key, client.search_files(query))
        except DeepPaginationError:
            return _VersionFiles(version_key, [], overflowed=True)

    return search
