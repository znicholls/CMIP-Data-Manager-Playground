"""
Step 2 — add files to each dataset version: parallel, with endpoint fallback

Each dataset version gets **one file search**, fanned out across `n_workers`
(through the injected `MapFn`).  Per version, resilience escalates in three levels
against a **preference-ordered** list of search-index endpoints (default
`(metagrid-west, esgf.ceda)`):

1. **in-request backoff** — the search retries with capped exponential backoff,
   `retries` times (default 5), against the current endpoint;
2. **requeue** — a version still failing after backoff is put back on the queue for
   one more full pass against the *same* endpoint (`requeue_rounds`, default 1);
3. **endpoint fallback** — a version still failing then moves to the *next* preferred
   endpoint and repeats levels 1-2.

Two properties matter:

- **Save-as-you-go.**  Each version's `File`/`FileAccess` rows are committed the moment
  its search succeeds, and index-node health is flushed as the run proceeds — a crash
  (or a final raise) keeps every completed dataset.  Because SQLite is single-writer,
  the network searches run in parallel but the DB writes are serialised behind a lock:
  *parallelise the I/O, serialise the commits*.
- **A per-version failure never aborts the pass.**  A worker catches everything and
  returns a failure/requeue outcome instead of propagating (which is what let one 500
  sink the whole pass before).  Only when **every** endpoint and requeue is exhausted
  does `add_files` raise `FileSearchIncompleteError` — and only *after* the successes
  are persisted, so re-running retries just the failures (the `version_has_files`
  cache gate skips everything already stored).

Index-node health (attempts, successes, failures, retries, 5xx, timeouts, latency) is
recorded per endpoint into `IndexNodeHealth` and persisted to `IndexNodeHealthStat`.
Alongside those aggregates, **every individual search call** — each backoff retry,
requeue and endpoint fallback — is logged as a `FileAccessAttempt` row (endpoint,
version, outcome, latency), so a version's whole search history can be reconstructed;
an `empty` outcome (HTTP 200 with zero files) is recorded distinctly from a `success`.
Endpoint *ordering* stays the caller's explicit preference; health is recorded for
observability, not to reorder the preference.
"""

from __future__ import annotations

import random
import time
from collections import defaultdict
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from threading import Lock

import httpx

from cmip_data_manager.db.repository import FileSearchAttempt, Repository
from cmip_data_manager.esgf.client import (
    DeepPaginationError,
    ESGFResponseError,
    ESGFSearchClient,
)
from cmip_data_manager.esgf.concurrency import MapFn, serial_map
from cmip_data_manager.esgf.index_health import (
    IndexNodeHealth,
    SearchOutcome,
    classify_search_error,
)
from cmip_data_manager.esgf.models import DatasetRecord, FileRecord
from cmip_data_manager.esgf.query import FacetQuery

DEFAULT_RETRIES = 5
"""In-request backoff retries per version, per endpoint (attempts = retries + 1)."""

DEFAULT_REQUEUE_ROUNDS = 1
"""Extra full passes over the still-failing versions on the *same* endpoint."""

DEFAULT_BACKOFF_BASE = 0.5
"""Base backoff delay in seconds; attempt `n` waits `base * 2**n`."""

DEFAULT_BACKOFF_CAP = 30.0
"""Maximum backoff delay between attempts, in seconds."""

DEFAULT_BACKOFF_JITTER = 0.1
"""Fractional random jitter added to each backoff delay, in `[0, jitter]`."""

_OK = "ok"
_FAILED = "failed"
_OVERFLOW = "overflow"

_Todo = tuple[str, tuple[str, ...]]
"""One unit of work: a version key and the node-specific dataset ids to search by."""


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

    failed: list[str] = field(default_factory=list)
    """Versions that could not be searched on any endpoint (also on the error)."""


class FileSearchIncompleteError(RuntimeError):
    """
    Raised when some versions could not be searched on any endpoint

    The successful searches are already persisted (save-as-you-go), so this is a loud
    "could not complete searches for X, Y, Z", not a data-loss event: re-running
    retries only the failures (the `version_has_files` cache gate skips the rest).
    The partial `AddFilesResult` and the endpoints tried are attached.
    """

    def __init__(self, result: AddFilesResult, endpoints: Sequence[str]) -> None:
        self.result = result
        self.endpoints = list(endpoints)
        failed = ", ".join(sorted(result.failed))
        super().__init__(
            f"Could not complete file searches for {len(result.failed)} dataset "
            f"version(s) after trying endpoint(s) {self.endpoints}: {failed}. "
            f"Successful searches are already cached, so re-running retries only "
            f"the failures."
        )


@dataclass(frozen=True)
class _Outcome:
    """A version's per-pass search outcome (carried back through the `MapFn`)."""

    version_key: str
    status: str
    files_stored: int = 0


def add_files(  # noqa: PLR0913 - a DI seam; every parameter has a default
    records: Sequence[DatasetRecord],
    *,
    clients: Sequence[ESGFSearchClient],
    repository: Repository,
    health: IndexNodeHealth | None = None,
    map_fn: MapFn = serial_map,
    retries: int = DEFAULT_RETRIES,
    requeue_rounds: int = DEFAULT_REQUEUE_ROUNDS,
    skip_cached: bool = True,
    backoff_base: float = DEFAULT_BACKOFF_BASE,
    backoff_cap: float = DEFAULT_BACKOFF_CAP,
    backoff_jitter: float = DEFAULT_BACKOFF_JITTER,
    sleep: Callable[[float], None] = time.sleep,
    raise_on_incomplete: bool = True,
) -> AddFilesResult:
    """
    Search each version's files once, with endpoint fallback and save-as-you-go

    Parameters
    ----------
    records
        The datasets to add files for (node-specific records; grouped internally by
        version, each searched by its own node-specific dataset ids).

    clients
        Search clients in **preference order** (e.g. CEDA, then ORNL, then
        metagrid-west), one per endpoint.  Build them with `no_retry` — retry is owned
        here so it can be
        counted and drive the fallback.  A version is tried on the next endpoint only
        after the current one's retries and requeues are exhausted.

    repository
        Cache to check for already-stored files and to write results into.

    health
        Index-node health registry; defaults to the one persisted in `repository`.

    map_fn
        Strategy for running the per-version searches; defaults to serial.  Pass
        `thread_pool_map(max_workers=n)` for `n_workers` parallelism (HTTP I/O).

    retries
        In-request backoff retries per version, per endpoint (default 5).

    requeue_rounds
        Extra full passes over the still-failing versions on the same endpoint before
        falling back (default 1).

    skip_cached
        Skip versions whose files are already stored (the caching behaviour).

    backoff_base, backoff_cap, backoff_jitter
        Capped-exponential-backoff parameters for the in-request retries.

    sleep
        Sleep function for the backoff, injectable so tests need not wait.

    raise_on_incomplete
        Raise `FileSearchIncompleteError` if any version could not be searched on any
        endpoint (default).  Pass `False` to return the partial result instead — e.g.
        the parent walk (Step 4) re-uses this step and handles missing files itself.

    Returns
    -------
    :
        A summary of what was searched, skipped, stored and overflowed.

    Raises
    ------
    ValueError
        If `clients` is empty.

    FileSearchIncompleteError
        If any version could not be searched on any endpoint.  Raised *after* the
        successes are persisted; the partial result is attached.
    """
    if not clients:
        msg = "add_files needs at least one endpoint client"
        raise ValueError(msg)
    health = repository.load_index_health() if health is None else health

    ids_by_version: dict[str, list[str]] = defaultdict(list)
    for record in records:
        ids_by_version[record.instance_key].append(record.id)

    todo: list[_Todo] = []
    skipped = 0
    for version_key, ids in ids_by_version.items():
        if skip_cached and repository.version_has_files(version_key):
            skipped += 1
            continue
        todo.append((version_key, tuple(ids)))

    write_lock = Lock()
    files_stored = 0
    overflowed: list[str] = []
    remaining = todo

    for client in clients:
        if not remaining:
            break
        if not client.supports_file_search:
            # A mixed-dialect ranked list may include an ESGF-NG client (files live on
            # STAC assets, not a file search); it cannot serve this step, so skip it —
            # the same tolerance the parent search already has for such a client.
            continue
        worker = _worker(
            client,
            repository,
            health,
            write_lock,
            retries=retries,
            base=backoff_base,
            cap=backoff_cap,
            jitter=backoff_jitter,
            sleep=sleep,
        )
        remaining, stored, over = _drain_endpoint(
            worker,
            remaining,
            requeue_rounds=requeue_rounds,
            map_fn=map_fn,
            repository=repository,
            health=health,
        )
        files_stored += stored
        overflowed.extend(over)

    result = AddFilesResult(
        searched=len(todo),
        skipped_cached=skipped,
        files_stored=files_stored,
        overflowed=overflowed,
        failed=[version_key for version_key, _ in remaining],
    )
    if result.failed and raise_on_incomplete:
        raise FileSearchIncompleteError(result, [client.base_url for client in clients])
    return result


def _drain_endpoint(  # noqa: PLR0913 - an internal helper; all args are threaded in
    worker: Callable[[_Todo], _Outcome],
    remaining: list[_Todo],
    *,
    requeue_rounds: int,
    map_fn: MapFn,
    repository: Repository,
    health: IndexNodeHealth,
) -> tuple[list[_Todo], int, list[str]]:
    """Run the initial pass plus `requeue_rounds` requeues against one endpoint.

    Returns the versions still failing (to carry to the next endpoint), the number
    of files stored, and the versions that overflowed.  Index-node health is
    persisted after each pass, so a long fallback still saves health as it goes.
    """
    files_stored = 0
    overflowed: list[str] = []
    for _ in range(1 + requeue_rounds):
        if not remaining:
            break
        outcomes = map_fn(worker, remaining)
        still_failing: list[_Todo] = []
        for item, outcome in zip(remaining, outcomes):
            if outcome.status == _OK:
                files_stored += outcome.files_stored
            elif outcome.status == _OVERFLOW:
                overflowed.append(outcome.version_key)
            else:
                still_failing.append(item)
        remaining = still_failing
        repository.save_index_health(health)  # persist as the run proceeds
    return remaining, files_stored, overflowed


def _worker(  # noqa: PLR0913 - a bound closure builder; all args are internal
    client: ESGFSearchClient,
    repository: Repository,
    health: IndexNodeHealth,
    write_lock: Lock,
    *,
    retries: int,
    base: float,
    cap: float,
    jitter: float,
    sleep: Callable[[float], None],
) -> Callable[[_Todo], _Outcome]:
    """Build the per-version search worker bound to one endpoint (for the `MapFn`)."""
    endpoint = client.base_url

    def run(item: _Todo) -> _Outcome:
        version_key, dataset_ids = item
        attempts: list[FileSearchAttempt] = []
        try:
            files, status = _search_with_backoff(
                client,
                endpoint,
                version_key,
                FacetQuery(type="File", dataset_id=dataset_ids),
                health,
                attempts,
                retries=retries,
                base=base,
                cap=cap,
                jitter=jitter,
                sleep=sleep,
            )
        except Exception as exc:
            # A worker must never abort the whole pass: any unexpected error becomes
            # a recorded failure that requeues / falls back like an ordinary one.
            attempts.append(
                FileSearchAttempt(
                    endpoint=endpoint,
                    version_key=version_key,
                    outcome=SearchOutcome.ERROR.value,
                    detail=str(exc),
                    attempt_no=len(attempts) + 1,
                )
            )
            with write_lock:
                repository.record_file_access_attempts(attempts)
            return _Outcome(version_key, _FAILED)

        # Serialise the write (SQLite is single-writer) and persist immediately,
        # flushing health and the attempt log alongside so a crash keeps this
        # dataset's progress and every search call it made.
        stored = 0
        with write_lock:
            if status == _OK:
                stored = repository.store_files({version_key: files})
                repository.save_index_health(health)
            repository.record_file_access_attempts(attempts)
        return _Outcome(version_key, status, stored)

    return run


def _search_with_backoff(  # noqa: PLR0913 - bound internal helper
    client: ESGFSearchClient,
    endpoint: str,
    version_key: str,
    query: FacetQuery,
    health: IndexNodeHealth,
    attempts: list[FileSearchAttempt],
    *,
    retries: int,
    base: float,
    cap: float,
    jitter: float,
    sleep: Callable[[float], None],
) -> tuple[list[FileRecord], str]:
    """Search one version's files with capped-exponential-backoff retries.

    Returns `(files, "ok")` on success, `([], "overflow")` if the single-version
    result exceeds the retrieval cap (retrying/falling back cannot help), or
    `([], "failed")` once the retries are exhausted.  Every call is recorded to
    `health` against `endpoint` (including retries and 5xx/timeout classification) and
    appended to `attempts` as a `FileSearchAttempt` (an `empty` outcome, HTTP 200 with
    zero files, is recorded distinctly from a `success` with files).
    """
    for attempt in range(retries + 1):
        started = time.perf_counter()
        try:
            files = client.search_files(query)
        except DeepPaginationError:
            attempts.append(
                FileSearchAttempt(
                    endpoint=endpoint,
                    version_key=version_key,
                    outcome=_OVERFLOW,
                    seconds=time.perf_counter() - started,
                    attempt_no=attempt + 1,
                )
            )
            return [], _OVERFLOW  # a query-shape problem, not an endpoint fault
        except (httpx.HTTPError, ESGFResponseError) as exc:
            outcome = classify_search_error(exc)
            elapsed = time.perf_counter() - started
            health.record(endpoint, outcome, elapsed, retry=attempt > 0)
            attempts.append(
                FileSearchAttempt(
                    endpoint=endpoint,
                    version_key=version_key,
                    outcome=outcome.value,
                    detail=str(exc),
                    seconds=elapsed,
                    attempt_no=attempt + 1,
                )
            )
            if attempt >= retries:
                return [], _FAILED
            sleep(_backoff_delay(attempt, base, cap, jitter))
        else:
            elapsed = time.perf_counter() - started
            health.record(endpoint, SearchOutcome.SUCCESS, elapsed, retry=attempt > 0)
            attempts.append(
                FileSearchAttempt(
                    endpoint=endpoint,
                    version_key=version_key,
                    outcome="success" if files else "empty",
                    files_found=len(files),
                    seconds=elapsed,
                    attempt_no=attempt + 1,
                )
            )
            return files, _OK
    return [], _FAILED  # unreachable, but keeps the return type total


def _backoff_delay(attempt: int, base: float, cap: float, jitter: float) -> float:
    """Return a capped exponential backoff delay with additive jitter."""
    delay = min(cap, base * (2**attempt))
    jittered = delay + random.uniform(0, jitter) * delay  # noqa: S311 - not crypto
    return float(jittered)
