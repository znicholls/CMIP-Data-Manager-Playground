"""
Persistence and change-tracking for cached search results

The `Repository` is the single entry point for writing runs and reading cached
data.  Recording a run does four things atomically:

1. group the returned records `master_id -> version (instance_id) -> data node`, and
   upsert one `Dataset` per master, one `DatasetVersion` per version, and one
   `DatasetNodeSpecificInfo` per data node (`first_seen`/`last_seen` bookkeeping);
2. record exactly which *versions* the run returned (`RunMembership`);
3. diff against the previous run **with the same query spec** (set difference on
   version ids, plus a representative `_timestamp` comparison for modifications);
4. write the resulting `DatasetChange` rows.

The diff *series* is keyed on the normalised query `spec`, not on any use-case name:
two runs of the same search form a series.  An optional `tag` labels a run for a
caller's convenience (e.g. a use-case name) and is what `get_dataset_records` reads
for the offline path, which reconstructs one `DatasetRecord` per stored location so
the downstream header pipeline still sees node-specific records.

Header-only metadata is stored on `File.header_attrs_json` and the dataset-applicable
subset promoted onto each `DatasetVersion` (see `store_files`/`promote_header`).
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

from sqlalchemy import Engine
from sqlmodel import Session, col, select

from cmip_data_manager.db.schema import (
    Cmip5VersionExtra,
    Cmip7VersionExtra,
    DataNodeHealthStat,
    Dataset,
    DatasetChange,
    DatasetNodeSpecificInfo,
    DatasetVersion,
    DownloadNodeHealthStat,
    File,
    FileAccess,
    FileAccessAttempt,
    FileDownload,
    FileDownloadAttempt,
    HeaderReadAttempt,
    IndexNodeHealthStat,
    RunMembership,
    SearchRun,
    version_ordinal,
)
from cmip_data_manager.esgf.cmip5 import cmip5_extra_fields
from cmip_data_manager.esgf.cmip7 import cmip7_extra_fields
from cmip_data_manager.esgf.download_health import DownloadNodeHealth, DownloadStat
from cmip_data_manager.esgf.eras import get_profile
from cmip_data_manager.esgf.headers import (
    HTTP_SERVICE,
    HeaderMetadata,
    https_twin,
)
from cmip_data_manager.esgf.health import NodeHealth, NodeStat
from cmip_data_manager.esgf.index_health import IndexNodeHealth, IndexNodeStat
from cmip_data_manager.esgf.models import DatasetRecord, FileRecord

# QUESTION: Is this an area that we may need to specify
# shared/re-written facets for project/esgf integration?
_DATASET_FACETS = (
    "mip_era",
    "project",
    "source_id",
    "institution_id",
    "experiment_id",
    "variant_label",
    "variable_id",
    "frequency",
    "table_id",
    "grid_label",
    "nominal_resolution",
)
"""Version-invariant columns stored on `Dataset` (keyed on `master_id`)."""

# QUESTION: see above - called the same? Not version promoted for
# CMIP7 (part of index node search not header data)
_VERSION_PROMOTED = (
    "parent_source_id",
    "parent_experiment_id",
    "parent_variant_label",
    "parent_activity_id",
    "branch_time_in_parent",
)
"""Header-only attributes promoted from a file's header onto its `DatasetVersion`."""


@dataclass(frozen=True)
class RunResult:
    """Summary of a recorded query run and the changes it produced."""

    run_id: int
    num_found: int
    """Number of distinct (version- and node-independent) datasets the run returned."""

    tag: str | None = None
    """The optional caller label recorded on the run."""

    added: list[str] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)
    modified: list[str] = field(default_factory=list)
    """Version ids added/removed/modified relative to the previous same-spec run."""

    @property
    def has_changes(self) -> bool:
        """Whether the run added, removed or modified anything."""
        return bool(self.added or self.removed or self.modified)


@dataclass(frozen=True)
class DisambiguationChoice:
    """
    Datasets that share a reconstructed base id but differ by an extra facet

    Some eras have facets outside the canonical `Dataset` columns that can make two
    otherwise-identical datasets distinct (CMIP5 `product`: `output1` vs `output2`).
    Those collide on one reconstructed base id and are disambiguated with a `.N` suffix.
    This groups the variants so a caller can see what differs and pick which to use
    (`Repository.cmip5_distinguishing_conflicts`).
    """

    base_master_id: str
    """The shared, suffix-free base master id (e.g. `CMIP5.…atmos`)."""

    options: list[tuple[str, str]]
    """`(distinguishing_json, master_id)` pairs, sorted, e.g.
    `[('{"product": "output1"}', 'CMIP5.…atmos'),
    ('{"product": "output2"}', 'CMIP5.…atmos.1')]`.  Always has at least two entries."""


@dataclass(frozen=True)
class HeaderAttempt:
    """
    One header-read attempt to persist to the `HeaderReadAttempt` log

    The write-side counterpart of the schema row: `enrich_version_headers` builds
    these from the `AttemptLog` (joining each URL back to its simulation, host and
    variable) and hands them to `record_header_attempts`.
    """

    source_id: str
    experiment_id: str
    variant_label: str
    outcome: str
    host: str | None = None
    url: str | None = None
    variable_id: str | None = None
    table_id: str | None = None
    seconds: float = 0.0
    attempt_no: int = 1
    detail: str | None = None
    """The error/exception text (or a note for a synthetic row); `None` on success."""


@dataclass(frozen=True)
class FileSearchAttempt:
    """
    One file-search attempt to persist to the `FileAccessAttempt` log

    The write-side counterpart of the schema row: `search.files.add_files` builds one
    of these per search call it issues for a version (each backoff retry, each requeue,
    each endpoint fallback) and hands them to `record_file_access_attempts`.
    """

    endpoint: str
    version_key: str
    outcome: str
    files_found: int = 0
    detail: str | None = None
    """The error/exception text for a failed attempt; `None` on `success`/`empty`."""
    seconds: float = 0.0
    attempt_no: int = 1


@dataclass(frozen=True)
class DownloadAttempt:
    """
    One file-download attempt to persist to the `FileDownloadAttempt` log

    The write-side counterpart of the schema row: the download orchestrator builds one
    of these per mirror URL it tries for a file (each `with_retry` sub-attempt, each
    resumed continuation, each file that fully failed) and hands them to
    `record_download_attempts`.
    """

    outcome: str
    file_id: int | None = None
    host: str | None = None
    url: str | None = None
    bytes_downloaded: int = 0
    seconds: float = 0.0
    throughput_mbps: float = 0.0
    attempt_no: int = 1
    resumed: bool = False
    detail: str | None = None
    """The error/exception text (or a note for a synthetic row); `None` on success."""


@dataclass(frozen=True)
class AttemptSummary:
    """A per-key roll-up of `HeaderReadAttempt` rows (see `header_attempt_summary`)."""

    key: str
    """The grouped value (a host or a `source_id`)."""

    attempts: int
    successes: int
    failures: int
    """Attempts that did not succeed (every non-`success` outcome)."""

    outcomes: dict[str, int] = field(default_factory=dict)
    """Count of each raw `outcome` value in this group."""


class Repository:
    """Read/write access to the local cache of ESGF search results."""

    def __init__(self, engine: Engine) -> None:
        """
        Initialise the repository

        Parameters
        ----------
        engine
            Engine for an already-initialised database (see `init_db`).
        """
        self._engine = engine

    def record_run(
        self,
        records: list[DatasetRecord],
        *,
        endpoint_url: str,
        spec: dict[str, Any],
        tag: str | None = None,
    ) -> RunResult:
        """
        Store a set of search results as a new run and compute the diff

        Parameters
        ----------
        records
            Datasets returned by this run (node-specific; grouped internally into
            `master_id -> version -> data node`).

        endpoint_url
            Endpoint that was queried (recorded for provenance).

        # QUESTION : What is this doing? Where does the spec get written? By user?
        spec
            JSON-serialisable description of the queries.  Diffs are computed against
            the previous run with the same normalised `spec`.

        tag
            Optional label for the run (e.g. a use-case name); read by
            `get_dataset_records`.  Never used for diffing.

        Returns
        -------
        :
            Summary of the run, including added/removed/modified version ids.
        """
        spec_json = json.dumps(spec, sort_keys=True)

        with Session(self._engine) as session:
            # Apply any era-specific collision suffix (CMIP5 product) *before* grouping,
            # so datasets that differ only by a distinguishing facet don't collapse.
            records = _disambiguate_records(session, records)
            # master_id -> instance_id (version) -> the records on each data node
            structure: dict[str, dict[str, list[DatasetRecord]]] = {}
            for record in records:
                versions = structure.setdefault(record.master_key, {})
                versions.setdefault(record.instance_key, []).append(record)

            run = SearchRun(
                endpoint_url=endpoint_url,
                spec_json=spec_json,
                num_found=len(records),
                num_stored=len(structure),
                tag=tag,
            )
            session.add(run)
            session.commit()
            session.refresh(run)
            run_id = run.id
            assert run_id is not None  # noqa: S101 - set by the database

            previous = self._previous_membership(session, spec_json, run_id)
            current: dict[str, str | None] = {
                version: _representative_timestamp(recs)
                for versions in structure.values()
                for version, recs in versions.items()
            }

            added = sorted(set(current) - set(previous))
            removed = sorted(set(previous) - set(current))
            modified = sorted(
                version
                for version in set(current) & set(previous)
                if current[version] != previous[version]
            )

            for master, versions in structure.items():
                self._upsert_dataset(
                    session, master, next(iter(versions.values()))[0], run_id
                )
                for version, vrecs in versions.items():
                    self._upsert_version(session, master, version, vrecs, run_id)
                    for record in vrecs:
                        self._upsert_location(session, version, record, run_id)
            session.flush()

            for version, timestamp in current.items():
                session.add(
                    RunMembership(
                        query_run_id=run_id,
                        version_key=version,
                        esgf_timestamp=timestamp,
                    )
                )
            for version in added:
                session.add(_change(run_id, version, "added"))
            for version in removed:
                session.add(_change(run_id, version, "removed"))
            for version in modified:
                session.add(
                    _change(
                        run_id,
                        version,
                        "modified",
                        detail={"old": previous[version], "new": current[version]},
                    )
                )

            session.add(run)
            session.commit()

            return RunResult(
                run_id=run_id,
                num_found=len(structure),
                tag=tag,
                added=added,
                removed=removed,
                modified=modified,
            )

    def save_node_health(self, health: NodeHealth) -> int:
        """
        Persist a node-health registry, upserting one row per host

        Writes the current in-memory counters back to `DataNodeHealthStat` so health
        accumulates across runs.  Load with `load_node_health` at the start of a
        run, record onto it, then save it here at the end.

        Parameters
        ----------
        health
            The registry to persist.

        Returns
        -------
        :
            Number of host rows written or updated.
        """
        snapshot = health.snapshot()
        with Session(self._engine) as session:
            for host, stat in snapshot.items():
                existing = session.get(DataNodeHealthStat, host)
                columns = _node_health_columns(stat)
                if existing is None:
                    session.add(DataNodeHealthStat(host=host, **columns))
                else:
                    for column, value in columns.items():
                        setattr(existing, column, value)
                    session.add(existing)
            session.commit()
        return len(snapshot)

    def load_node_health(self) -> NodeHealth:
        """
        Rebuild an in-memory node-health registry from persisted rows

        Returns
        -------
        :
            A `NodeHealth` seeded with every stored host's counters (empty if none
            have been persisted).
        """
        health = NodeHealth()
        with Session(self._engine) as session:
            for row in session.exec(select(DataNodeHealthStat)).all():
                health.restore(_stat_from_row(row))
        return health

    def cmip5_distinguishing_conflicts(self) -> list[DisambiguationChoice]:
        """
        Find CMIP5 datasets sharing a base id but split by an extra facet

        Groups the CMIP5 side-table rows by `base_master_id` and returns only the groups
        with **more than one** distinct `distinguishing_json` — the cases where the
        reconstructed `master_id` was disambiguated with a `.N` suffix and a user must
        choose (e.g. `product` `output1` vs `output2`).  Each option's concrete
        `master_id` is the owning version's `dataset_key`, so the caller sees exactly
        which key each choice maps to.

        Returns
        -------
        :
            One `DisambiguationChoice` per conflicted simulation, ordered by
            `base_master_id`; empty when no CMIP5 collisions exist.
        """
        with Session(self._engine) as session:
            rows = session.exec(
                select(
                    Cmip5VersionExtra.base_master_id,
                    Cmip5VersionExtra.distinguishing_json,
                    DatasetVersion.dataset_key,
                )
                .join(
                    DatasetVersion,
                    col(DatasetVersion.instance_id)
                    == col(Cmip5VersionExtra.version_key),
                )
                .where(col(Cmip5VersionExtra.base_master_id).is_not(None))
            ).all()
        by_base: dict[str, dict[str, str]] = {}
        for base, distinguishing, master_id in rows:
            if base is None or distinguishing is None:
                continue
            by_base.setdefault(base, {})[distinguishing] = master_id
        return [
            DisambiguationChoice(base_master_id=base, options=sorted(options.items()))
            for base, options in sorted(by_base.items())
            if len(options) > 1
        ]

    def rank_nodes_by_reliability(self) -> list[DataNodeHealthStat]:
        """
        Return persisted hosts best-to-worst by success rate

        Answers "rank the nodes by the share of header requests that failed" from
        the database directly.  Ordered by descending `successes/attempts`, with
        more-tried hosts winning ties.

        Returns
        -------
        :
            The stored host rows, most reliable first.
        """
        with Session(self._engine) as session:
            rows = session.exec(select(DataNodeHealthStat)).all()
        return sorted(
            rows,
            key=lambda r: (
                -(r.successes / r.attempts) if r.attempts else 0.0,
                -r.attempts,
                r.host,
            ),
        )

    def rank_nodes_by_speed(self) -> list[DataNodeHealthStat]:
        """
        Return persisted hosts fastest-to-slowest by mean successful-read time

        Answers "rank the nodes by speed of response".  Only hosts with at least
        one success are included (a host that never succeeded has no speed);
        ordered by ascending mean successful-read seconds.

        Returns
        -------
        :
            The stored host rows with successes, fastest first.
        """
        with Session(self._engine) as session:
            rows = session.exec(select(DataNodeHealthStat)).all()
        with_success = [row for row in rows if row.successes]
        return sorted(
            with_success,
            key=lambda r: (r.total_success_seconds / r.successes, r.host),
        )

    def save_index_health(self, health: IndexNodeHealth) -> int:
        """
        Persist a search-index health registry, upserting one row per endpoint

        The Step-2 twin of `save_node_health`: writes the current in-memory counters
        back to `IndexNodeHealthStat` so index-node health accumulates across runs.
        Upserting the whole snapshot is idempotent, so Step 2 can call this
        incrementally (save-as-you-go) — a crash keeps what was learned so far.

        Parameters
        ----------
        health
            The registry to persist.

        Returns
        -------
        :
            Number of endpoint rows written or updated.
        """
        snapshot = health.snapshot()
        with Session(self._engine) as session:
            for endpoint, stat in snapshot.items():
                existing = session.get(IndexNodeHealthStat, endpoint)
                columns = _index_health_columns(stat)
                if existing is None:
                    session.add(IndexNodeHealthStat(endpoint=endpoint, **columns))
                else:
                    for column, value in columns.items():
                        setattr(existing, column, value)
                    session.add(existing)
            session.commit()
        return len(snapshot)

    def load_index_health(self) -> IndexNodeHealth:
        """
        Rebuild an in-memory search-index health registry from persisted rows

        Returns
        -------
        :
            An `IndexNodeHealth` seeded with every stored endpoint's counters (empty
            if none have been persisted).
        """
        health = IndexNodeHealth()
        with Session(self._engine) as session:
            for row in session.exec(select(IndexNodeHealthStat)).all():
                health.restore(_index_stat_from_row(row))
        return health

    def rank_index_nodes_by_reliability(self) -> list[IndexNodeHealthStat]:
        """
        Return persisted search endpoints best-to-worst by success rate

        The Step-2 twin of `rank_nodes_by_reliability`.  Ordered by descending
        `successes/attempts`, with more-tried endpoints winning ties.

        Returns
        -------
        :
            The stored endpoint rows, most reliable first.
        """
        with Session(self._engine) as session:
            rows = session.exec(select(IndexNodeHealthStat)).all()
        return sorted(
            rows,
            key=lambda r: (
                -(r.successes / r.attempts) if r.attempts else 0.0,
                -r.attempts,
                r.endpoint,
            ),
        )

    def record_header_attempts(self, attempts: Sequence[HeaderAttempt]) -> int:
        """
        Append per-attempt header-read records to the log

        Append-only: every attempt (including retries and fully-failed
        simulations) becomes its own `HeaderReadAttempt` row, so the log builds a
        history across runs rather than being overwritten.

        Parameters
        ----------
        attempts
            The attempts to record.

        Returns
        -------
        :
            Number of rows written.
        """
        if not attempts:
            return 0
        with Session(self._engine) as session:
            for attempt in attempts:
                session.add(
                    HeaderReadAttempt(
                        source_id=attempt.source_id,
                        experiment_id=attempt.experiment_id,
                        variant_label=attempt.variant_label,
                        variable_id=attempt.variable_id,
                        table_id=attempt.table_id,
                        host=attempt.host,
                        url=attempt.url,
                        outcome=attempt.outcome,
                        seconds=attempt.seconds,
                        attempt_no=attempt.attempt_no,
                        detail=attempt.detail,
                    )
                )
            session.commit()
        return len(attempts)

    def get_header_attempts(  # noqa: PLR0913 - optional filters, all keyword-only
        self,
        *,
        host: str | None = None,
        source_id: str | None = None,
        experiment_id: str | None = None,
        variant_label: str | None = None,
        outcome: str | None = None,
        since: datetime | None = None,
        limit: int | None = None,
    ) -> list[HeaderReadAttempt]:
        """
        Return logged header-read attempts, filtered and newest-first

        The queryable window onto the attempt log: any combination of the
        (indexed) filters narrows it, so "how did node X do?" (`host=...`), "what
        happened to this model?" (`source_id=...`) or "today's failures"
        (`since=..., outcome="timeout"`) are each one call.

        Parameters
        ----------
        host, source_id, experiment_id, variant_label, outcome
            Exact-match filters; omit any to leave that dimension unconstrained.

        since
            Keep only attempts recorded at or after this time.

        limit
            Cap on rows returned (the newest ones); unbounded if omitted.

        Returns
        -------
        :
            Matching attempts, most recent first.
        """
        statement = select(HeaderReadAttempt)
        if host is not None:
            statement = statement.where(HeaderReadAttempt.host == host)
        if source_id is not None:
            statement = statement.where(HeaderReadAttempt.source_id == source_id)
        if experiment_id is not None:
            statement = statement.where(
                HeaderReadAttempt.experiment_id == experiment_id
            )
        if variant_label is not None:
            statement = statement.where(
                HeaderReadAttempt.variant_label == variant_label
            )
        if outcome is not None:
            statement = statement.where(HeaderReadAttempt.outcome == outcome)
        if since is not None:
            statement = statement.where(HeaderReadAttempt.created_at >= since)
        statement = statement.order_by(col(HeaderReadAttempt.id).desc())
        if limit is not None:
            statement = statement.limit(limit)
        with Session(self._engine) as session:
            return list(session.exec(statement).all())

    def record_file_access_attempts(self, attempts: Sequence[FileSearchAttempt]) -> int:
        """
        Append per-attempt file-search records to the log

        The Step-2 twin of `record_header_attempts`: append-only, so every search call
        (including backoff retries, requeues, endpoint fallbacks and versions that fully
        failed) becomes its own `FileAccessAttempt` row and the log builds a history
        across runs rather than being overwritten.

        Parameters
        ----------
        attempts
            The attempts to record.

        Returns
        -------
        :
            Number of rows written.
        """
        if not attempts:
            return 0
        with Session(self._engine) as session:
            for attempt in attempts:
                session.add(
                    FileAccessAttempt(
                        endpoint=attempt.endpoint,
                        version_key=attempt.version_key,
                        outcome=attempt.outcome,
                        files_found=attempt.files_found,
                        detail=attempt.detail,
                        seconds=attempt.seconds,
                        attempt_no=attempt.attempt_no,
                    )
                )
            session.commit()
        return len(attempts)

    def get_file_access_attempts(
        self,
        *,
        endpoint: str | None = None,
        version_key: str | None = None,
        outcome: str | None = None,
        since: datetime | None = None,
        limit: int | None = None,
    ) -> list[FileAccessAttempt]:
        """
        Return logged file-search attempts, filtered and newest-first

        The queryable window onto the Step-2 attempt log (the twin of
        `get_header_attempts`): any combination of the (indexed) filters narrows it, so
        "how did endpoint X do?" (`endpoint=...`), "what happened to this version?"
        (`version_key=...`) or "today's empty hits" (`since=..., outcome="empty"`) are
        each one call.

        Parameters
        ----------
        endpoint, version_key, outcome
            Exact-match filters; omit any to leave that dimension unconstrained.

        since
            Keep only attempts recorded at or after this time.

        limit
            Cap on rows returned (the newest ones); unbounded if omitted.

        Returns
        -------
        :
            Matching attempts, most recent first.
        """
        statement = select(FileAccessAttempt)
        if endpoint is not None:
            statement = statement.where(FileAccessAttempt.endpoint == endpoint)
        if version_key is not None:
            statement = statement.where(FileAccessAttempt.version_key == version_key)
        if outcome is not None:
            statement = statement.where(FileAccessAttempt.outcome == outcome)
        if since is not None:
            statement = statement.where(FileAccessAttempt.created_at >= since)
        statement = statement.order_by(col(FileAccessAttempt.id).desc())
        if limit is not None:
            statement = statement.limit(limit)
        with Session(self._engine) as session:
            return list(session.exec(statement).all())

    def header_attempt_summary(
        self, *, group_by: str = "host", since: datetime | None = None
    ) -> list[AttemptSummary]:
        """
        Roll up logged attempts by host or source_id

        Builds the "picture of the day" the raw log supports: for each host (or
        `source_id`), how many attempts were made, how many succeeded/failed, and
        the breakdown by outcome.  Pair with `since` for a single day's view.

        Parameters
        ----------
        group_by
            `"host"` or `"source_id"` — the dimension to roll up on.

        since
            Only include attempts recorded at or after this time.

        Returns
        -------
        :
            One summary per group, ordered by most attempts first.

        Raises
        ------
        ValueError
            If `group_by` is neither `"host"` nor `"source_id"`.
        """
        if group_by not in ("host", "source_id"):
            msg = "group_by must be 'host' or 'source_id'"
            raise ValueError(msg)
        statement = select(HeaderReadAttempt)
        if since is not None:
            statement = statement.where(HeaderReadAttempt.created_at >= since)
        with Session(self._engine) as session:
            rows = session.exec(statement).all()

        buckets: dict[str, dict[str, int]] = {}
        for row in rows:
            key = (row.host if group_by == "host" else row.source_id) or "(none)"
            outcomes = buckets.setdefault(key, {})
            outcomes[row.outcome] = outcomes.get(row.outcome, 0) + 1
        summaries = [
            AttemptSummary(
                key=key,
                attempts=sum(outcomes.values()),
                successes=outcomes.get("success", 0),
                failures=sum(outcomes.values()) - outcomes.get("success", 0),
                outcomes=dict(outcomes),
            )
            for key, outcomes in buckets.items()
        ]
        return sorted(summaries, key=lambda s: (-s.attempts, s.key))

    def record_download_attempts(self, attempts: Sequence[DownloadAttempt]) -> int:
        """
        Append per-attempt file-download records to the log

        The download twin of `record_header_attempts`: append-only, so every download
        attempt (including `with_retry` sub-attempts, resumed continuations, mirror
        fallbacks and files that fully failed) becomes its own `FileDownloadAttempt`
        row and the log builds a history across runs rather than being overwritten.

        Parameters
        ----------
        attempts
            The attempts to record.

        Returns
        -------
        :
            Number of rows written.
        """
        if not attempts:
            return 0
        with Session(self._engine) as session:
            for attempt in attempts:
                session.add(
                    FileDownloadAttempt(
                        file_id=attempt.file_id,
                        host=attempt.host,
                        url=attempt.url,
                        outcome=attempt.outcome,
                        bytes_downloaded=attempt.bytes_downloaded,
                        seconds=attempt.seconds,
                        throughput_mbps=attempt.throughput_mbps,
                        attempt_no=attempt.attempt_no,
                        resumed=attempt.resumed,
                        detail=attempt.detail,
                    )
                )
            session.commit()
        return len(attempts)

    def get_download_attempts(
        self,
        *,
        host: str | None = None,
        file_id: int | None = None,
        outcome: str | None = None,
        since: datetime | None = None,
        limit: int | None = None,
    ) -> list[FileDownloadAttempt]:
        """
        Return logged file-download attempts, filtered and newest-first

        The queryable window onto the download attempt log (the twin of
        `get_header_attempts`): any combination of the (indexed) filters narrows it, so
        "how did node X do?" (`host=...`), "what happened to this file?"
        (`file_id=...`) or "today's checksum failures" (`since=...,
        outcome="checksum_failed"`) are each one call.

        Parameters
        ----------
        host, file_id, outcome
            Exact-match filters; omit any to leave that dimension unconstrained.

        since
            Keep only attempts recorded at or after this time.

        limit
            Cap on rows returned (the newest ones); unbounded if omitted.

        Returns
        -------
        :
            Matching attempts, most recent first.
        """
        statement = select(FileDownloadAttempt)
        if host is not None:
            statement = statement.where(FileDownloadAttempt.host == host)
        if file_id is not None:
            statement = statement.where(FileDownloadAttempt.file_id == file_id)
        if outcome is not None:
            statement = statement.where(FileDownloadAttempt.outcome == outcome)
        if since is not None:
            statement = statement.where(FileDownloadAttempt.created_at >= since)
        statement = statement.order_by(col(FileDownloadAttempt.id).desc())
        if limit is not None:
            statement = statement.limit(limit)
        with Session(self._engine) as session:
            return list(session.exec(statement).all())

    def download_attempt_summary(
        self, *, since: datetime | None = None
    ) -> list[AttemptSummary]:
        """
        Roll up logged download attempts by host

        The download twin of `header_attempt_summary`: for each host, how many download
        attempts were made, how many succeeded/failed, and the breakdown by outcome.
        Pair with `since` for a single day's view.

        Parameters
        ----------
        since
            Only include attempts recorded at or after this time.

        Returns
        -------
        :
            One summary per host, ordered by most attempts first.
        """
        statement = select(FileDownloadAttempt)
        if since is not None:
            statement = statement.where(FileDownloadAttempt.created_at >= since)
        with Session(self._engine) as session:
            rows = session.exec(statement).all()
        buckets: dict[str, dict[str, int]] = {}
        for row in rows:
            key = row.host or "(none)"
            outcomes = buckets.setdefault(key, {})
            outcomes[row.outcome] = outcomes.get(row.outcome, 0) + 1
        summaries = [
            AttemptSummary(
                key=key,
                attempts=sum(outcomes.values()),
                successes=outcomes.get("success", 0),
                failures=sum(outcomes.values()) - outcomes.get("success", 0),
                outcomes=dict(outcomes),
            )
            for key, outcomes in buckets.items()
        ]
        return sorted(summaries, key=lambda s: (-s.attempts, s.key))

    def save_download_health(self, health: DownloadNodeHealth) -> int:
        """
        Persist a download-health registry, upserting one row per host

        The download twin of `save_node_health`: writes the current in-memory
        throughput counters back to `DownloadNodeHealthStat` so download health
        accumulates across runs.  Upserting the whole snapshot is idempotent, so the
        download step can call this incrementally (save-as-you-go) — a crash keeps what
        was learned so far.  Persisted download health is *ranking information only*: it
        is never turned into a cross-restart ignore list (see `DownloadNodeHealth`).

        Parameters
        ----------
        health
            The registry to persist.

        Returns
        -------
        :
            Number of host rows written or updated.
        """
        snapshot = health.snapshot()
        with Session(self._engine) as session:
            for host, stat in snapshot.items():
                existing = session.get(DownloadNodeHealthStat, host)
                columns = _download_health_columns(stat)
                if existing is None:
                    session.add(DownloadNodeHealthStat(host=host, **columns))
                else:
                    for column, value in columns.items():
                        setattr(existing, column, value)
                    session.add(existing)
            session.commit()
        return len(snapshot)

    def load_download_health(self) -> DownloadNodeHealth:
        """
        Rebuild an in-memory download-health registry from persisted rows

        Returns
        -------
        :
            A `DownloadNodeHealth` seeded with every stored host's counters (empty if
            none have been persisted).
        """
        health = DownloadNodeHealth()
        with Session(self._engine) as session:
            for row in session.exec(select(DownloadNodeHealthStat)).all():
                health.restore(_download_stat_from_row(row))
        return health

    def rank_download_nodes_by_throughput(self) -> list[DownloadNodeHealthStat]:
        """
        Return persisted hosts fastest-to-slowest by mean download throughput

        Answers "rank the nodes by download speed (MB/s)" from the database directly —
        the signal that matters for downloads, distinct from header-read latency.  Only
        hosts with at least one success are included (a host that never succeeded has no
        throughput); ordered by descending mean MB/s.

        Returns
        -------
        :
            The stored host rows with successes, fastest first.
        """
        with Session(self._engine) as session:
            rows = session.exec(select(DownloadNodeHealthStat)).all()
        with_throughput = [
            row for row in rows if row.successes and row.total_success_seconds
        ]
        return sorted(
            with_throughput,
            key=lambda r: (
                -(r.total_bytes / 1_000_000.0 / r.total_success_seconds),
                r.host,
            ),
        )

    def rank_download_nodes_by_reliability(self) -> list[DownloadNodeHealthStat]:
        """
        Return persisted hosts best-to-worst by download success rate

        The download twin of `rank_nodes_by_reliability`.  Ordered by descending
        `successes/attempts`, with more-tried hosts winning ties.

        Returns
        -------
        :
            The stored host rows, most reliable first.
        """
        with Session(self._engine) as session:
            rows = session.exec(select(DownloadNodeHealthStat)).all()
        return sorted(
            rows,
            key=lambda r: (
                -(r.successes / r.attempts) if r.attempts else 0.0,
                -r.attempts,
                r.host,
            ),
        )

    def mark_download(  # noqa: PLR0913 - a column per keyword; most default
        self,
        *,
        file_id: int,
        status: str,
        local_path: str | None = None,
        size_bytes: int | None = None,
        verified: bool = False,
        verified_algo: str | None = None,
        download_from_access_key: int | None = None,
        attempts: int = 0,
        completed_at: datetime | None = None,
    ) -> None:
        """
        Upsert the terminal download state of a file (`FileDownload`)

        Written the instant a file reaches a terminal state (save-as-you-go), so a
        killed run keeps every file it finished.  Re-marking the same `file_id`
        overwrites its row (e.g. a later run completing a previously failed file).

        Parameters
        ----------
        file_id
            The `File.id` this state belongs to.

        status
            `complete`, `unverified`, `failed` or `skipped` (see `FileDownload`).

        local_path
            Absolute path the file was written to; `None` for a `failed` row.

        size_bytes
            Size of the file on disk in bytes.

        verified, verified_algo
            Whether the bytes were checked against a published checksum, and the
            algorithm used.

        download_from_access_key
            Soft pointer to the `FileAccess.id` the winning download came from.

        attempts
            Total download attempts made for this file across all mirrors.

        completed_at
            When the file reached this state; defaults to now.
        """
        stamp = completed_at or datetime.now(timezone.utc)
        columns = {
            "local_path": local_path,
            "status": status,
            "size_bytes": size_bytes,
            "verified": verified,
            "verified_algo": verified_algo,
            "download_from_access_key": download_from_access_key,
            "attempts": attempts,
            "completed_at": stamp,
        }
        with Session(self._engine) as session:
            existing = session.get(FileDownload, file_id)
            if existing is None:
                session.add(FileDownload(file_id=file_id, **columns))
            else:
                for column, value in columns.items():
                    setattr(existing, column, value)
                session.add(existing)
            session.commit()

    def get_download_state(self, file_id: int) -> FileDownload | None:
        """
        Return the terminal download state for a file, or `None` if never attempted

        Parameters
        ----------
        file_id
            The `File.id` to look up.

        Returns
        -------
        :
            The stored `FileDownload` row, or `None`.
        """
        with Session(self._engine) as session:
            return session.get(FileDownload, file_id)

    def version_downloads(self, version_key: str) -> list[FileDownload]:
        """
        Return the download state of every file of a version that has one

        Joins `FileDownload` to `File` on the version, so a caller can derive
        version-level completeness (a version is complete when all its files have a
        `complete` row) without that being stored anywhere.

        Parameters
        ----------
        version_key
            The `DatasetVersion.instance_id` whose files' download state is wanted.

        Returns
        -------
        :
            The `FileDownload` rows for this version's files (empty if none downloaded).
        """
        statement = (
            select(FileDownload)
            .join(File, col(File.id) == col(FileDownload.file_id))
            .where(File.version_key == version_key)
        )
        with Session(self._engine) as session:
            return list(session.exec(statement).all())

    def get_dataset_records(self, tag: str) -> list[DatasetRecord]:
        """
        Return the datasets from the latest run with a given tag

        This is the offline search path: results are read straight from the
        database with no network access.  Each stored version is expanded back into
        one `DatasetRecord` per `DatasetNodeSpecificInfo`, so the caller sees the same
        node-specific records the online search produced.

        Parameters
        ----------
        tag
            Run label to look up (e.g. a use-case name).

        Returns
        -------
        :
            The datasets that run returned, as `DatasetRecord`s (one per location).
            Empty if no run carries that tag.
        """
        with Session(self._engine) as session:
            run = self._latest_run(session, tag)
            if run is None:
                return []
            memberships = session.exec(
                select(RunMembership).where(RunMembership.query_run_id == run.id)
            ).all()
            records: list[DatasetRecord] = []
            for membership in memberships:
                version = session.get(DatasetVersion, membership.version_key)
                if version is None:
                    continue
                dataset = session.get(Dataset, version.dataset_key)
                if dataset is None:
                    continue
                locations = session.exec(
                    select(DatasetNodeSpecificInfo).where(
                        DatasetNodeSpecificInfo.version_key == membership.version_key
                    )
                ).all()
                for location in locations:
                    records.append(_record_from(dataset, version, location))
            return records

    def get_changes(self, run_id: int) -> list[DatasetChange]:
        """
        Return the change log for a run

        Parameters
        ----------
        run_id
            Run whose changes to return.

        Returns
        -------
        :
            The recorded changes, in insertion order.
        """
        with Session(self._engine) as session:
            return list(
                session.exec(
                    select(DatasetChange)
                    .where(DatasetChange.query_run_id == run_id)
                    .order_by(col(DatasetChange.id))
                ).all()
            )

    def version_has_files(self, version_key: str) -> bool:
        """
        Whether any files are already cached for a dataset version

        The Step-2 cache check: a version whose files are already stored is skipped
        rather than re-searched.

        Parameters
        ----------
        version_key
            The `DatasetVersion.instance_id` to check.

        Returns
        -------
        :
            `True` if at least one `File` row exists for the version.
        """
        with Session(self._engine) as session:
            return (
                session.exec(
                    select(File.id).where(File.version_key == version_key)
                ).first()
                is not None
            )

    def get_version_files(self, version_key: str) -> list[File]:
        """
        Return the stored files (with their access options) for a dataset version

        Parameters
        ----------
        version_key
            The `DatasetVersion.instance_id` to look up.

        Returns
        -------
        :
            The `File` rows for the version, each with its `accesses` loaded; empty
            if none are stored.
        """
        with Session(self._engine) as session:
            files = session.exec(
                select(File).where(File.version_key == version_key)
            ).all()
            for file in files:
                _ = file.accesses  # load the relationship before the session closes
            return list(files)

    def latest_version_for(  # noqa: PLR0913 - a dataset is identified by its facets
        self,
        source_id: str,
        experiment_id: str,
        variant_label: str,
        variable_id: str | None,
        table_id: str | None = None,
        grid_label: str | None = None,
    ) -> str | None:
        """
        Return the latest stored version id for a dataset identified by its facets

        Used to resolve a child version's parent *version* once the parent
        simulation has been searched and stored: the parent dataset matching the
        child's `variable_id` (and, when given, `table_id`/`grid_label`) is looked
        up and its newest version returned.  "Newest" is the lexicographically
        greatest `version` (CMIP6 `vYYYYMMDD` dates sort chronologically).

        Parameters
        ----------
        source_id, experiment_id, variant_label, variable_id
            The dataset facets to match.

        table_id, grid_label
            Optional further facets to disambiguate.

        Returns
        -------
        :
            The matching `DatasetVersion.instance_id`, or `None` if none is stored.
        """
        statement = select(DatasetVersion.instance_id).where(
            DatasetVersion.dataset_key == Dataset.master_id,
            Dataset.source_id == source_id,
            Dataset.experiment_id == experiment_id,
            Dataset.variant_label == variant_label,
            Dataset.variable_id == variable_id,
        )
        if table_id is not None:
            statement = statement.where(Dataset.table_id == table_id)
        if grid_label is not None:
            statement = statement.where(Dataset.grid_label == grid_label)
        statement = statement.order_by(col(DatasetVersion.version).desc())
        with Session(self._engine) as session:
            return session.exec(statement).first()

    def set_parent_version(
        self, child_version_key: str, parent_version_key: str
    ) -> None:
        """
        Link a child dataset version to its parent version

        Sets `DatasetVersion.parent_version_key` (the real self-referential FK) on
        the child, recording the resolved child -> parent-version edge.

        Parameters
        ----------
        child_version_key
            The child `DatasetVersion.instance_id`.

        parent_version_key
            The parent `DatasetVersion.instance_id` to link to.
        """
        with Session(self._engine) as session:
            version = session.get(DatasetVersion, child_version_key)
            if version is None:
                return
            version.parent_version_key = parent_version_key
            session.add(version)
            session.commit()

    def version_has_header(self, version_key: str) -> bool:
        """Whether a version already has header-only metadata promoted onto it."""
        with Session(self._engine) as session:
            version = session.get(DatasetVersion, version_key)
            return version is not None and version.header_from_file_key is not None

    def version_header_file_id(self, version_key: str) -> int | None:
        """Return the `File.id` a version's promoted header was read from, if any."""
        with Session(self._engine) as session:
            version = session.get(DatasetVersion, version_key)
            return None if version is None else version.header_from_file_key

    def _header_from_version(
        self, session: Session, version: DatasetVersion
    ) -> tuple[HeaderMetadata, int] | None:
        """Reconstruct a version's promoted header and the `File.id` it came from.

        Returns the `HeaderMetadata` (its `attrs` from `File.header_attrs_json`, its
        `source_url` recovered from the file's recorded access) alongside that
        `File.id`, or `None` if the version has no header promoted onto it.
        """
        file_id = version.header_from_file_key
        if file_id is None:
            return None
        file = session.get(File, file_id)
        if file is None or file.header_attrs_json is None:
            return None
        attrs = json.loads(file.header_attrs_json)
        source_url: str | None = None
        if file.header_from_access_key is not None:
            access = session.get(FileAccess, file.header_from_access_key)
            source_url = access.url if access is not None else None
        return HeaderMetadata(attrs=attrs, source_url=source_url), file_id

    def version_header(self, version_key: str) -> HeaderMetadata | None:
        """
        Return the header-only metadata promoted onto a dataset version, if any

        Reconstructs a `HeaderMetadata` from the `File` the version's
        `header_from_file_key` points at (its full `header_attrs_json`), with the
        `source_url` recovered from that file's recorded access.  Header lookup is
        keyed at the version grain; use `simulation_header` to reuse a header across a
        simulation's variables.

        Parameters
        ----------
        version_key
            The `DatasetVersion.instance_id` to look up.

        Returns
        -------
        :
            The stored header, or `None` if none has been read for the version.
        """
        with Session(self._engine) as session:
            version = session.get(DatasetVersion, version_key)
            if version is None:
                return None
            result = self._header_from_version(session, version)
            return None if result is None else result[0]

    def simulation_header(
        self, source_id: str, experiment_id: str, variant_label: str
    ) -> tuple[HeaderMetadata, int] | None:
        """
        Return a stored header for a simulation, from any of its variables/versions

        Header-only metadata (the `parent_*` lineage) is **independent of variable**: a
        header read once for a simulation `(source_id, experiment_id, variant_label)`
        describes the whole run, so every variable of that simulation shares it.  This
        looks the header up at the **simulation** grain — any stored `DatasetVersion` of
        the simulation that already carries a promoted header, regardless of its
        `variable_id`, its version, or which earlier run read it — so a later search for
        a new variable can **copy** the header instead of re-reading it.

        Parameters
        ----------
        source_id, experiment_id, variant_label
            The simulation to look a header up for.

        Returns
        -------
        :
            The reconstructed `HeaderMetadata` and the `File.id` it was read from (so
            the caller can promote it onto the new versions), or `None` if no version of
            the simulation has a header yet.
        """
        statement = (
            select(DatasetVersion)
            .where(
                DatasetVersion.dataset_key == Dataset.master_id,
                Dataset.source_id == source_id,
                Dataset.experiment_id == experiment_id,
                Dataset.variant_label == variant_label,
                col(DatasetVersion.header_from_file_key).is_not(None),
            )
            .order_by(col(DatasetVersion.version).desc())
        )
        with Session(self._engine) as session:
            for version in session.exec(statement):
                result = self._header_from_version(session, version)
                if result is not None:
                    return result
            return None

    def promote_header(
        self,
        *,
        file_id: int,
        source_url: str | None,
        attrs: Mapping[str, str],
        version_keys: Sequence[str],
    ) -> int:
        """
        Store a header on the file it was read from and promote it onto versions

        Writes the full header onto `File.header_attrs_json` (with the access it came
        from and the read time), then, for each version in `version_keys`, sets
        `header_from_file_key` to that file and copies the promoted `parent_*` subset
        onto the version.  The file may belong to a *sibling* version of the
        simulation (the cross-variable header-reuse case), which is why the pointer
        is soft.

        Parameters
        ----------
        file_id
            The `File.id` the header was read from.

        source_url
            The exact URL read, used to record which access served the header.

        attrs
            The header's global attributes.

        version_keys
            The versions to promote the header-only metadata onto.

        Returns
        -------
        :
            Number of versions promoted.
        """
        with Session(self._engine) as session:
            file = session.get(File, file_id)
            if file is None:
                return 0
            file.header_attrs_json = json.dumps(dict(attrs), sort_keys=True)
            file.header_read_at = datetime.now(timezone.utc)
            file.header_from_access_key = _access_id_for_url(
                session, file_id, source_url
            )
            session.add(file)
            promoted = 0
            for version_key in version_keys:
                version = session.get(DatasetVersion, version_key)
                if version is None:
                    continue
                version.header_from_file_key = file_id
                for attr in _VERSION_PROMOTED:
                    setattr(version, attr, attrs.get(attr))
                session.add(version)
                promoted += 1
            session.commit()
        return promoted

    def store_files(self, files_by_version: Mapping[str, Sequence[FileRecord]]) -> int:
        """
        Upsert files and their per-node access options for dataset versions

        One `File` row per logical file (keyed by `(version_key, filename)`, so a
        file served from several nodes collapses to one row) and one `FileAccess`
        row per distinct access URL (the per-node download options, with an
        fsspec-openable form for `HTTPServer` URLs).  Files whose version is not
        cached are skipped (the foreign key could not be satisfied); store the
        datasets first.

        Parameters
        ----------
        files_by_version
            Mapping of `DatasetVersion.instance_id` to the file records found for it.

        Returns
        -------
        :
            Number of `File` rows written or updated.
        """
        stored = 0
        with Session(self._engine) as session:
            for version_key, records in files_by_version.items():
                version = session.get(DatasetVersion, version_key)
                if version is None:
                    continue
                by_filename: dict[str, list[FileRecord]] = {}
                for record in records:
                    by_filename.setdefault(record.title or record.id, []).append(record)
                for filename, frecs in by_filename.items():
                    self._upsert_file(session, version_key, filename, frecs)
                    stored += 1
                # Correct the file count to what was actually found for THIS version.
                # For CMIP5 the Step-1 count is the whole-table total (~57); the
                # variable-scoped Step-2 search narrows it to this variable's files.
                version.number_of_files = len(by_filename)
                session.add(version)
            session.commit()
        return stored

    def _upsert_file(
        self,
        session: Session,
        version_key: str,
        filename: str,
        records: Sequence[FileRecord],
    ) -> None:
        """Upsert one logical file and its access options across nodes."""
        columns = _file_columns(records[0])
        file = session.exec(
            select(File).where(
                File.version_key == version_key, File.filename == filename
            )
        ).first()
        if file is None:
            file = File(version_key=version_key, filename=filename, **columns)
            session.add(file)
            session.flush()
        else:
            for key, value in columns.items():
                setattr(file, key, value)
            session.add(file)
        file_id = file.id
        assert file_id is not None  # noqa: S101 - set by the flush/load above

        existing = {access.url: access for access in file.accesses}
        added: set[str | None] = set()
        for record in records:
            for access in _access_columns(record):
                url = access["url"]
                if url in existing:
                    for key, value in access.items():
                        setattr(existing[url], key, value)
                elif url not in added:
                    session.add(FileAccess(file_id=file_id, **access))
                    added.add(url)

    def _previous_membership(
        self, session: Session, spec_json: str, run_id: int
    ) -> dict[str, str | None]:
        """Return `{version_id: timestamp}` for the previous run of the same spec."""
        previous_run = session.exec(
            select(SearchRun)
            .where(SearchRun.spec_json == spec_json, SearchRun.id != run_id)
            .order_by(col(SearchRun.id).desc())
        ).first()
        if previous_run is None:
            return {}
        memberships = session.exec(
            select(RunMembership).where(RunMembership.query_run_id == previous_run.id)
        ).all()
        return {m.version_key: m.esgf_timestamp for m in memberships}

    def _latest_run(self, session: Session, tag: str) -> SearchRun | None:
        """Return the most recent run carrying a tag, if any."""
        return session.exec(
            select(SearchRun)
            .where(SearchRun.tag == tag)
            .order_by(col(SearchRun.id).desc())
        ).first()

    def _upsert_dataset(
        self,
        session: Session,
        master: str,
        record: DatasetRecord,
        run_id: int,
    ) -> None:
        """Upsert one version-invariant dataset (keyed on `master_id`)."""
        facets = {name: getattr(record, name) for name in _DATASET_FACETS}
        existing = session.get(Dataset, master)
        if existing is None:
            session.add(
                Dataset(
                    master_id=master,
                    first_seen_run_id=run_id,
                    last_seen_run_id=run_id,
                    **facets,
                )
            )
            return
        for key, value in facets.items():
            setattr(existing, key, value)
        existing.last_seen_run_id = run_id
        session.add(existing)

    def _upsert_version(
        self,
        session: Session,
        master: str,
        version_id: str,
        records: Sequence[DatasetRecord],
        run_id: int,
    ) -> None:
        """Upsert one dataset version (validating its version string is a date)."""
        version = _version_of(records[0])
        version_ordinal(version)  # raises ValueError on a non-numeric version
        columns = {
            "mip_era": records[0].mip_era,
            "version": version,
            "is_latest": records[0].latest,
            "size": records[0].size,
            "number_of_files": records[0].number_of_files,
        }
        existing = session.get(DatasetVersion, version_id)
        if existing is None:
            session.add(
                DatasetVersion(
                    instance_id=version_id,
                    dataset_key=master,
                    first_seen_run_id=run_id,
                    last_seen_run_id=run_id,
                    **columns,
                )
            )
        else:
            for key, value in columns.items():
                setattr(existing, key, value)
            existing.last_seen_run_id = run_id
            session.add(existing)
        if records[0].mip_era == "CMIP5":
            self._upsert_cmip5_extra(session, version_id, records[0])
        elif records[0].mip_era == "CMIP7":
            self._upsert_cmip7_extra(session, version_id, records[0])

    def _upsert_cmip7_extra(
        self, session: Session, version_id: str, record: DatasetRecord
    ) -> None:
        """Upsert the CMIP7-only side row (branding suffix, labels, region, licence)."""
        fields = cmip7_extra_fields(record)
        existing = session.get(Cmip7VersionExtra, version_id)
        if existing is None:
            session.add(Cmip7VersionExtra(version_key=version_id, **fields))
            return
        for key, value in fields.items():
            setattr(existing, key, value)
        session.add(existing)

    def _upsert_cmip5_extra(
        self, session: Session, version_id: str, record: DatasetRecord
    ) -> None:
        """Upsert the CMIP5-only side row (base id, native ids, realm, what differs)."""
        fields = cmip5_extra_fields(record)
        profile = get_profile("CMIP5")
        fields["distinguishing_json"] = _distinguishing_json(
            record.raw, profile.distinguishing_facets
        )
        existing = session.get(Cmip5VersionExtra, version_id)
        if existing is None:
            session.add(Cmip5VersionExtra(version_key=version_id, **fields))
            return
        for key, value in fields.items():
            setattr(existing, key, value)
        session.add(existing)

    def _upsert_location(
        self,
        session: Session,
        version_id: str,
        record: DatasetRecord,
        run_id: int,
    ) -> None:
        """Upsert the per-node location row for one record."""
        columns = {
            "esgf_dataset_id": record.id,
            "replica": record.replica,
            "esgf_timestamp": record.esgf_timestamp,
            "raw_json": json.dumps(record.raw, sort_keys=True),
        }
        existing = session.get(DatasetNodeSpecificInfo, (version_id, record.node_key))
        if existing is None:
            session.add(
                DatasetNodeSpecificInfo(
                    version_key=version_id,
                    data_node=record.node_key,
                    first_seen_run_id=run_id,
                    last_seen_run_id=run_id,
                    **columns,
                )
            )
            return
        for key, value in columns.items():
            setattr(existing, key, value)
        existing.last_seen_run_id = run_id
        session.add(existing)


def _distinguishing_json(raw: Mapping[str, Any], facets: tuple[str, ...]) -> str | None:
    """
    Render an era's distinguishing facet values from a raw doc as canonical JSON

    Returns `None` when the era declares no distinguishing facets (the CMIP6 case), so
    disambiguation never runs.  Otherwise a stable, sorted-key JSON string of
    `{facet: value}` (each value the sole entry of the raw list), which serves as both
    the collision key and the human-facing "what differs" record.
    """
    if not facets:
        return None
    values: dict[str, str | None] = {}
    for facet in facets:
        value = raw.get(facet)
        if isinstance(value, list):
            values[facet] = None if not value else str(value[0])
        else:
            values[facet] = None if value is None else str(value)
    return json.dumps(values, sort_keys=True)


class _Disambiguator:
    """
    Assigns a stable `.N` collision suffix per `(base_master_id, distinguishing_json)`

    Seeds each base's assignments from what is already stored (so re-runs are stable),
    then hands the first-seen variant the bare id (`""`) and each new variant the next
    ordinal.  Encounter-order, not a fixed priority: first seen keeps the bare id.
    """

    def __init__(self, session: Session) -> None:
        self._session = session
        self._assigned: dict[str, dict[str, str]] = {}

    def resolve(self, base_master_id: str, distinguishing_json: str) -> str:
        """Return the suffix (`""`, `".1"`, …) for this base + distinguishing value."""
        assigned = self._assigned.get(base_master_id)
        if assigned is None:
            assigned = self._load(base_master_id)
            self._assigned[base_master_id] = assigned
        if distinguishing_json in assigned:
            return assigned[distinguishing_json]
        suffix = "" if not assigned else f".{len(assigned)}"
        assigned[distinguishing_json] = suffix
        return suffix

    def _load(self, base_master_id: str) -> dict[str, str]:
        """Load `{distinguishing_json: suffix}` already persisted for this base."""
        rows = self._session.exec(
            select(Cmip5VersionExtra.distinguishing_json, DatasetVersion.dataset_key)
            .join(
                DatasetVersion,
                col(DatasetVersion.instance_id) == col(Cmip5VersionExtra.version_key),
            )
            .where(col(Cmip5VersionExtra.base_master_id) == base_master_id)
        ).all()
        assigned: dict[str, str] = {}
        for distinguishing, master_id in rows:
            if distinguishing is None:
                continue
            # The suffix is whatever the stored master id carries beyond the base.
            assigned[distinguishing] = master_id[len(base_master_id) :]
        return assigned


def _disambiguate_records(
    session: Session, records: list[DatasetRecord]
) -> list[DatasetRecord]:
    """
    Apply era collision suffixes to records before they are grouped into datasets

    For each record whose era declares `distinguishing_facets`, the record's `master_id`
    (set by reconstruction to the suffix-free base) and `instance_id` gain the resolved
    `.N` suffix, so two datasets identical on their columns but differing by e.g.
    `product` stay distinct.  Records of eras without distinguishing facets (CMIP6) pass
    through untouched.
    """
    disambiguator = _Disambiguator(session)
    out: list[DatasetRecord] = []
    for record in records:
        era = get_profile(record.mip_era) if record.mip_era else None
        base = record.master_id
        if era is None or not era.distinguishing_facets or base is None:
            out.append(record)
            continue
        distinguishing = _distinguishing_json(record.raw, era.distinguishing_facets)
        if distinguishing is None:
            out.append(record)
            continue
        suffix = disambiguator.resolve(base, distinguishing)
        if not suffix:
            out.append(record)
            continue
        master_id = base + suffix
        instance_id = f"{master_id}.{record.version}" if record.version else master_id
        out.append(
            record.model_copy(
                update={"master_id": master_id, "instance_id": instance_id}
            )
        )
    return out


def _version_of(record: DatasetRecord) -> str:
    """Return a record's version string, deriving it from the ids if unset."""
    if record.version:
        return record.version
    master, instance = record.master_key, record.instance_key
    if instance.startswith(f"{master}."):
        return instance[len(master) + 1 :]
    return instance.rpartition(".")[2]


def _representative_timestamp(records: Sequence[DatasetRecord]) -> str | None:
    """Pick a version's representative `_timestamp` (the latest across its nodes)."""
    stamps = [r.esgf_timestamp for r in records if r.esgf_timestamp is not None]
    return max(stamps) if stamps else None


def _change(
    run_id: int,
    version_key: str,
    change_type: str,
    detail: dict[str, Any] | None = None,
) -> DatasetChange:
    """Build a `DatasetChange` row (serialising `detail` to JSON if given)."""
    return DatasetChange(
        query_run_id=run_id,
        version_key=version_key,
        change_type=change_type,
        detail_json=None if detail is None else json.dumps(detail),
    )


def _record_from(
    dataset: Dataset, version: DatasetVersion, location: DatasetNodeSpecificInfo
) -> DatasetRecord:
    """Rebuild a node-specific `DatasetRecord` from stored dataset/version/location."""
    raw = json.loads(location.raw_json) if location.raw_json else {}
    facets = {name: getattr(dataset, name) for name in _DATASET_FACETS}
    dataset_id = (
        location.esgf_dataset_id or f"{version.instance_id}|{location.data_node}"
    )
    return DatasetRecord(
        id=dataset_id,
        instance_id=version.instance_id,
        master_id=dataset.master_id,
        version=version.version,
        latest=version.is_latest,
        data_node=location.data_node,
        replica=location.replica,
        size=version.size,
        number_of_files=version.number_of_files,
        esgf_timestamp=location.esgf_timestamp,
        raw=raw,
        **facets,
    )


def _access_id_for_url(
    session: Session, file_id: int, source_url: str | None
) -> int | None:
    """Find the `FileAccess.id` for a file whose `url` or `fsspec_url` was read."""
    if source_url is None:
        return None
    for column in (FileAccess.url, FileAccess.fsspec_url):
        access = session.exec(
            select(FileAccess).where(
                FileAccess.file_id == file_id, column == source_url
            )
        ).first()
        if access is not None:
            return access.id
    return None


def _file_columns(record: FileRecord) -> dict[str, Any]:
    """Return the storable `File` columns of a file record (excluding the keys)."""
    return {
        "size": record.size,
        "checksum": record.checksum,
        "checksum_type": record.checksum_type,
        "tracking_id": record.tracking_id,
    }


def _access_columns(record: FileRecord) -> list[dict[str, Any]]:
    """
    Turn a file record's `url|mime|service` entries into `FileAccess` column dicts

    One dict per distinct URL: the data node is the URL host, and `fsspec_url` is an
    fsspec-openable form of an `HTTPServer` URL (upgraded to `https://` when the index
    only listed `http://`); other services (OPeNDAP, Globus) leave it `None` for now.
    """
    accesses: list[dict[str, Any]] = []
    for entry in record.urls:
        parts = entry.split("|")
        if len(parts) != 3:  # noqa: PLR2004 - the fixed url|mime|service shape
            continue
        url, _mime, service = parts
        fsspec_url: str | None = None
        if service == HTTP_SERVICE:
            fsspec_url = url if url.startswith("https://") else https_twin(url)
        accesses.append(
            {
                "data_node": urlparse(url).hostname,
                "service": service,
                "url": url,
                "fsspec_url": fsspec_url,
                "esgf_file_id": record.id,
            }
        )
    return accesses


def _node_health_columns(stat: NodeStat) -> dict[str, Any]:
    """Return the storable columns of a node-health stat (excluding the host)."""
    return {
        "attempts": stat.attempts,
        "successes": stat.successes,
        "timeouts": stat.timeouts,
        "crashes": stat.crashes,
        "errors": stat.errors,
        "blocks": stat.blocks,
        "total_success_seconds": stat.total_success_seconds,
        "max_success_seconds": stat.max_success_seconds,
        "max_safe_concurrency": stat.max_safe_concurrency,
        "last_concurrency": stat.last_concurrency,
        "updated_at": datetime.now(timezone.utc),
    }


def _stat_from_row(row: DataNodeHealthStat) -> NodeStat:
    """Rebuild an in-memory `NodeStat` from a stored node-health row."""
    return NodeStat(
        host=row.host,
        attempts=row.attempts,
        successes=row.successes,
        timeouts=row.timeouts,
        crashes=row.crashes,
        errors=row.errors,
        blocks=row.blocks,
        total_success_seconds=row.total_success_seconds,
        max_success_seconds=row.max_success_seconds,
        max_safe_concurrency=row.max_safe_concurrency,
        last_concurrency=row.last_concurrency,
    )


def _index_health_columns(stat: IndexNodeStat) -> dict[str, Any]:
    """Return the storable columns of an index-health stat (excluding the endpoint)."""
    return {
        "attempts": stat.attempts,
        "successes": stat.successes,
        "failures": stat.failures,
        "retries": stat.retries,
        "server_errors": stat.server_errors,
        "timeouts": stat.timeouts,
        "total_success_seconds": stat.total_success_seconds,
        "max_success_seconds": stat.max_success_seconds,
        "updated_at": datetime.now(timezone.utc),
    }


def _index_stat_from_row(row: IndexNodeHealthStat) -> IndexNodeStat:
    """Rebuild an in-memory `IndexNodeStat` from a stored index-health row."""
    return IndexNodeStat(
        endpoint=row.endpoint,
        attempts=row.attempts,
        successes=row.successes,
        failures=row.failures,
        retries=row.retries,
        server_errors=row.server_errors,
        timeouts=row.timeouts,
        total_success_seconds=row.total_success_seconds,
        max_success_seconds=row.max_success_seconds,
    )


def _download_health_columns(stat: DownloadStat) -> dict[str, Any]:
    """Return the storable columns of a download-health stat (excluding the host)."""
    return {
        "attempts": stat.attempts,
        "successes": stat.successes,
        "timeouts": stat.timeouts,
        "blocks": stat.blocks,
        "host_faults": stat.host_faults,
        "checksum_failures": stat.checksum_failures,
        "errors": stat.errors,
        "total_bytes": stat.total_bytes,
        "total_success_seconds": stat.total_success_seconds,
        "max_success_seconds": stat.max_success_seconds,
        "max_safe_concurrency": stat.max_safe_concurrency,
        "last_concurrency": stat.last_concurrency,
        "updated_at": datetime.now(timezone.utc),
    }


def _download_stat_from_row(row: DownloadNodeHealthStat) -> DownloadStat:
    """Rebuild an in-memory `DownloadStat` from a stored download-health row."""
    return DownloadStat(
        host=row.host,
        attempts=row.attempts,
        successes=row.successes,
        timeouts=row.timeouts,
        blocks=row.blocks,
        host_faults=row.host_faults,
        checksum_failures=row.checksum_failures,
        errors=row.errors,
        total_bytes=row.total_bytes,
        total_success_seconds=row.total_success_seconds,
        max_success_seconds=row.max_success_seconds,
        max_safe_concurrency=row.max_safe_concurrency,
        last_concurrency=row.last_concurrency,
    )
