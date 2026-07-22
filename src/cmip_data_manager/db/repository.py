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
    Dataset,
    DatasetChange,
    DatasetNodeSpecificInfo,
    DatasetVersion,
    File,
    FileAccess,
    HeaderReadAttempt,
    IndexNodeHealthStat,
    NodeHealthStat,
    RunMembership,
    SearchRun,
    parse_version_date,
)
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
        # master_id -> instance_id (version) -> the records on each data node
        structure: dict[str, dict[str, list[DatasetRecord]]] = {}
        for record in records:
            versions = structure.setdefault(record.master_key, {})
            versions.setdefault(record.instance_key, []).append(record)

        with Session(self._engine) as session:
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

        Writes the current in-memory counters back to `NodeHealthStat` so health
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
                existing = session.get(NodeHealthStat, host)
                columns = _node_health_columns(stat)
                if existing is None:
                    session.add(NodeHealthStat(host=host, **columns))
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
            for row in session.exec(select(NodeHealthStat)).all():
                health.restore(_stat_from_row(row))
        return health

    def rank_nodes_by_reliability(self) -> list[NodeHealthStat]:
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
            rows = session.exec(select(NodeHealthStat)).all()
        return sorted(
            rows,
            key=lambda r: (
                -(r.successes / r.attempts) if r.attempts else 0.0,
                -r.attempts,
                r.host,
            ),
        )

    def rank_nodes_by_speed(self) -> list[NodeHealthStat]:
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
            rows = session.exec(select(NodeHealthStat)).all()
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

    def version_header(self, version_key: str) -> HeaderMetadata | None:
        """
        Return the header-only metadata promoted onto a dataset version, if any

        Reconstructs a `HeaderMetadata` from the `File` the version's
        `header_from_file_key` points at (its full `header_attrs_json`), with the
        `source_url` recovered from that file's recorded access.  Header lookup is
        keyed at the version grain.

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
            if version is None or version.header_from_file_key is None:
                return None
            file = session.get(File, version.header_from_file_key)
            if file is None or file.header_attrs_json is None:
                return None
            attrs = json.loads(file.header_attrs_json)
            source_url: str | None = None
            if file.header_from_access_key is not None:
                access = session.get(FileAccess, file.header_from_access_key)
                source_url = access.url if access is not None else None
            return HeaderMetadata(attrs=attrs, source_url=source_url)

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
                if session.get(DatasetVersion, version_key) is None:
                    continue
                by_filename: dict[str, list[FileRecord]] = {}
                for record in records:
                    by_filename.setdefault(record.title or record.id, []).append(record)
                for filename, frecs in by_filename.items():
                    self._upsert_file(session, version_key, filename, frecs)
                    stored += 1
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
        parse_version_date(version)  # raises ValueError on a non-date version
        columns = {
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
            return
        for key, value in columns.items():
            setattr(existing, key, value)
        existing.last_seen_run_id = run_id
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


def _stat_from_row(row: NodeHealthStat) -> NodeStat:
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
