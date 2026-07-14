"""
Persistence and change-tracking for cached search results

The `Repository` is the single entry point for writing runs and reading cached
data.  Recording a run does four things atomically:

1. upsert every returned dataset (`first_seen`/`last_seen` bookkeeping);
2. record exactly which datasets the run returned (`RunMembership`);
3. diff against the previous run of the same use case (set difference on ids,
   plus a `_timestamp` comparison for modifications);
4. write the resulting `DatasetChange` rows.

Offline search reuses cached entries: `get_dataset_records` returns the datasets
from the latest run of a use case, which can then be fed to the same aggregation
code that the online path uses.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

from sqlalchemy import Engine
from sqlmodel import Session, col, select

from cmip_data_manager.db.schema import (
    Dataset,
    DatasetChange,
    DatasetHeader,
    File,
    NodeHealthStat,
    QueryRun,
    RunMembership,
)
from cmip_data_manager.esgf.headers import PROMOTED_ATTRS, HeaderKey, HeaderMetadata
from cmip_data_manager.esgf.health import NodeHealth, NodeStat
from cmip_data_manager.esgf.models import DatasetRecord, FileRecord

_DATASET_SCALARS = (
    "master_id",
    "instance_id",
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
    "version",
    "data_node",
    "replica",
    "latest",
    "number_of_files",
    "size",
    "esgf_timestamp",
)


@dataclass(frozen=True)
class RunResult:
    """Summary of a recorded query run and the changes it produced."""

    run_id: int
    use_case: str
    num_found: int
    added: list[str] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)
    modified: list[str] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        """Whether the run added, removed or modified anything."""
        return bool(self.added or self.removed or self.modified)


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
        use_case: str,
        records: list[DatasetRecord],
        *,
        endpoint_url: str,
        spec: dict[str, Any],
    ) -> RunResult:
        """
        Store a set of search results as a new run and compute the diff

        Parameters
        ----------
        use_case
            Name of the use case; diffs are computed against the previous run
            with the same name.

        records
            Datasets returned by this run.

        endpoint_url
            Endpoint that was queried (recorded for provenance).

        spec
            JSON-serialisable description of the queries (recorded for provenance).

        Returns
        -------
        :
            Summary of the run, including added/removed/modified dataset ids.
        """
        with Session(self._engine) as session:
            run = QueryRun(
                use_case=use_case,
                endpoint_url=endpoint_url,
                spec_json=json.dumps(spec, sort_keys=True),
                num_found=len(records),
                num_stored=0,
            )
            session.add(run)
            session.commit()
            session.refresh(run)
            run_id = run.id
            assert run_id is not None  # noqa: S101 - set by the database

            previous = self._previous_membership(session, use_case, run_id)
            current = {r.id: r.esgf_timestamp for r in records}

            added = sorted(set(current) - set(previous))
            removed = sorted(set(previous) - set(current))
            modified = sorted(
                dsid
                for dsid in set(current) & set(previous)
                if current[dsid] != previous[dsid]
            )

            for record in records:
                self._upsert_dataset(session, record, run_id)
            session.flush()

            for record in records:
                session.add(
                    RunMembership(
                        query_run_id=run_id,
                        dataset_id=record.id,
                        esgf_timestamp=record.esgf_timestamp,
                    )
                )
            for dsid in added:
                session.add(_change(run_id, dsid, "added"))
            for dsid in removed:
                session.add(_change(run_id, dsid, "removed"))
            for dsid in modified:
                session.add(
                    _change(
                        run_id,
                        dsid,
                        "modified",
                        detail={"old": previous[dsid], "new": current[dsid]},
                    )
                )

            run.num_stored = len(records)
            session.add(run)
            session.commit()

            return RunResult(
                run_id=run_id,
                use_case=use_case,
                num_found=len(records),
                added=added,
                removed=removed,
                modified=modified,
            )

    def store_files(self, files: list[FileRecord]) -> int:
        """
        Upsert files, linking them to their parent datasets

        Files whose parent dataset is not cached are skipped (the foreign key
        could not be satisfied); store the datasets first.

        Parameters
        ----------
        files
            Files to store.

        Returns
        -------
        :
            Number of files written or updated.
        """
        stored = 0
        with Session(self._engine) as session:
            for record in files:
                if session.get(Dataset, record.dataset_id) is None:
                    continue
                existing = session.get(File, record.id)
                data = _file_columns(record)
                if existing is None:
                    session.add(File(**data))
                else:
                    for key, value in data.items():
                        setattr(existing, key, value)
                    session.add(existing)
                stored += 1
            session.commit()
        return stored

    def store_headers(self, headers: Mapping[HeaderKey, HeaderMetadata]) -> int:
        """
        Upsert cached header metadata, one row per dataset key

        Keys are `(source_id, experiment_id, variant_label, variable_id,
        table_id)`; the promoted `parent_*`/`tracking_id` columns are projected out
        of each header and the full attribute set is kept as JSON.

        Parameters
        ----------
        headers
            Mapping of dataset key to the header read for it.

        Returns
        -------
        :
            Number of header rows written or updated.
        """
        stored = 0
        with Session(self._engine) as session:
            for key, metadata in headers.items():
                columns = _header_columns(metadata)
                existing = session.get(DatasetHeader, key)
                if existing is None:
                    source_id, experiment_id, variant_label, variable_id, table_id = key
                    session.add(
                        DatasetHeader(
                            source_id=source_id,
                            experiment_id=experiment_id,
                            variant_label=variant_label,
                            variable_id=variable_id,
                            table_id=table_id,
                            **columns,
                        )
                    )
                else:
                    for column, value in columns.items():
                        setattr(existing, column, value)
                    session.add(existing)
                stored += 1
            session.commit()
        return stored

    def get_header(self, key: HeaderKey) -> HeaderMetadata | None:
        """
        Return the cached header for one dataset key, or `None` if absent

        Parameters
        ----------
        key
            `(source_id, experiment_id, variant_label, variable_id, table_id)`.

        Returns
        -------
        :
            The stored header, or `None`.
        """
        with Session(self._engine) as session:
            row = session.get(DatasetHeader, key)
            return None if row is None else _metadata_from_header(row)

    def get_simulation_headers(
        self, source_id: str, experiment_id: str, variant_label: str
    ) -> list[HeaderMetadata]:
        """
        Return every cached header for a simulation, across its variables/tables

        This is the seam for a future cross-variable "smart reader": a caller that
        wants `rsut` but has only ever read `tas` for the same `(source_id,
        experiment_id, variant_label)` can find the existing header here instead of
        re-reading.

        Parameters
        ----------
        source_id, experiment_id, variant_label
            The simulation to look up.

        Returns
        -------
        :
            The stored headers for that simulation (any variable/table); empty if
            none have been read.
        """
        with Session(self._engine) as session:
            rows = session.exec(
                select(DatasetHeader).where(
                    DatasetHeader.source_id == source_id,
                    DatasetHeader.experiment_id == experiment_id,
                    DatasetHeader.variant_label == variant_label,
                )
            ).all()
            return [_metadata_from_header(row) for row in rows]

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

    def get_dataset_records(self, use_case: str) -> list[DatasetRecord]:
        """
        Return the datasets from the latest cached run of a use case

        This is the offline search path: results are read straight from the
        database with no network access.

        Parameters
        ----------
        use_case
            Use case whose latest run should be read.

        Returns
        -------
        :
            The datasets that run returned, as `DatasetRecord`s.  Empty if the
            use case has never been run.
        """
        with Session(self._engine) as session:
            run = self._latest_run(session, use_case)
            if run is None:
                return []
            memberships = session.exec(
                select(RunMembership).where(RunMembership.query_run_id == run.id)
            ).all()
            records: list[DatasetRecord] = []
            for membership in memberships:
                dataset = session.get(Dataset, membership.dataset_id)
                if dataset is not None:
                    records.append(_record_from_dataset(dataset))
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

    def _previous_membership(
        self, session: Session, use_case: str, run_id: int
    ) -> dict[str, str | None]:
        """Return `{dataset_id: timestamp}` for the previous run of a use case."""
        previous_run = session.exec(
            select(QueryRun)
            .where(QueryRun.use_case == use_case, QueryRun.id != run_id)
            .order_by(col(QueryRun.id).desc())
        ).first()
        if previous_run is None:
            return {}
        memberships = session.exec(
            select(RunMembership).where(RunMembership.query_run_id == previous_run.id)
        ).all()
        return {m.dataset_id: m.esgf_timestamp for m in memberships}

    def _latest_run(self, session: Session, use_case: str) -> QueryRun | None:
        """Return the most recent run for a use case, if any."""
        return session.exec(
            select(QueryRun)
            .where(QueryRun.use_case == use_case)
            .order_by(col(QueryRun.id).desc())
        ).first()

    def _upsert_dataset(
        self, session: Session, record: DatasetRecord, run_id: int
    ) -> None:
        """Insert or update a dataset, maintaining first/last-seen bookkeeping."""
        columns = _dataset_columns(record)
        existing = session.get(Dataset, record.id)
        if existing is None:
            session.add(
                Dataset(
                    id=record.id,
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


def _change(
    run_id: int,
    dataset_id: str,
    change_type: str,
    detail: dict[str, Any] | None = None,
) -> DatasetChange:
    """Build a `DatasetChange` row (serialising `detail` to JSON if given)."""
    return DatasetChange(
        query_run_id=run_id,
        dataset_id=dataset_id,
        change_type=change_type,
        detail_json=None if detail is None else json.dumps(detail),
    )


def _dataset_columns(record: DatasetRecord) -> dict[str, Any]:
    """Return the storable columns of a dataset record (excluding the id)."""
    columns: dict[str, Any] = {name: getattr(record, name) for name in _DATASET_SCALARS}
    columns["raw_json"] = json.dumps(record.raw, sort_keys=True)
    return columns


def _record_from_dataset(dataset: Dataset) -> DatasetRecord:
    """Rebuild a `DatasetRecord` from a stored dataset row."""
    raw = json.loads(dataset.raw_json) if dataset.raw_json else {}
    scalars = {name: getattr(dataset, name) for name in _DATASET_SCALARS}
    return DatasetRecord(id=dataset.id, raw=raw, **scalars)


def _header_columns(metadata: HeaderMetadata) -> dict[str, Any]:
    """Return the storable columns of a header (excluding the key fields)."""
    attrs = metadata.attrs
    columns: dict[str, Any] = {name: attrs.get(name) for name in PROMOTED_ATTRS}
    columns["attrs_json"] = json.dumps(attrs, sort_keys=True)
    columns["source_url"] = metadata.source_url
    columns["data_node"] = (
        urlparse(metadata.source_url).hostname if metadata.source_url else None
    )
    columns["read_at"] = datetime.now(timezone.utc)
    return columns


def _metadata_from_header(row: DatasetHeader) -> HeaderMetadata:
    """Rebuild a `HeaderMetadata` from a stored header row."""
    attrs = json.loads(row.attrs_json) if row.attrs_json else {}
    return HeaderMetadata(attrs=attrs, source_url=row.source_url)


def _node_health_columns(stat: NodeStat) -> dict[str, Any]:
    """Return the storable columns of a node-health stat (excluding the host)."""
    return {
        "attempts": stat.attempts,
        "successes": stat.successes,
        "timeouts": stat.timeouts,
        "crashes": stat.crashes,
        "errors": stat.errors,
        "total_success_seconds": stat.total_success_seconds,
        "max_success_seconds": stat.max_success_seconds,
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
        total_success_seconds=row.total_success_seconds,
        max_success_seconds=row.max_success_seconds,
    )


def _file_columns(record: FileRecord) -> dict[str, Any]:
    """Return the storable columns of a file record (including the id)."""
    return {
        "id": record.id,
        "dataset_key": record.dataset_id,
        "dataset_id": record.dataset_id,
        "title": record.title,
        "size": record.size,
        "checksum": record.checksum,
        "checksum_type": record.checksum_type,
        "tracking_id": record.tracking_id,
        "variable_id": record.variable_id,
        "urls_json": json.dumps(list(record.urls)),
        "esgf_timestamp": record.esgf_timestamp,
        "raw_json": json.dumps(record.raw, sort_keys=True),
    }
