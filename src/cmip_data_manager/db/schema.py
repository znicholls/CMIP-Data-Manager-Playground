"""
Database schema for cached datasets, files and query history

Design notes:

- One persisted model per concept.  A `Dataset` owns many `File` rows through a
  standard one-to-many relationship (`Dataset.files` / `File.dataset`).  We model
  files from the start because downloads and completeness checks need them.
- We deliberately do **not** split into separate "database" and "API" classes.
  For these flat records that split buys nothing; the SQLModel classes are used
  directly as the Python API.  The one guard-rail is that relationship attributes
  (`Dataset.files`) are only safe to read inside an open session, so the
  repository eagerly loads them when returning detached objects.
- Query history is captured by `QueryRun` (one row per API call for a use case),
  `RunMembership` (exactly which datasets a run returned, enabling clean diffs)
  and `DatasetChange` (the computed added/removed/modified log).
"""

from datetime import datetime, timezone

from sqlmodel import Field, Relationship, SQLModel

# NOTE: this module intentionally does not use ``from __future__ import
# annotations``.  SQLModel resolves relationship targets from the annotations at
# runtime, and stringised annotations break the ``Dataset``/``File`` relationship.


def _utcnow() -> datetime:
    """Return the current time as a timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)


class QueryRun(SQLModel, table=True):
    """One execution of a use case's queries against the search API."""

    id: int | None = Field(default=None, primary_key=True)
    use_case: str = Field(index=True)
    """Name of the use case this run belongs to."""

    created_at: datetime = Field(default_factory=_utcnow)
    endpoint_url: str
    """Search endpoint that was queried."""

    spec_json: str
    """JSON description of the queries that were run."""

    num_found: int
    """Total number of datasets returned by the run."""

    num_stored: int
    """Number of datasets written/updated in the database by the run."""

    status: str = "ok"
    """Terminal status of the run (`"ok"` or an error marker)."""


class Dataset(SQLModel, table=True):
    """A cached ESGF dataset, uniquely identified by its versioned `id`."""

    id: str = Field(primary_key=True)
    master_id: str | None = Field(default=None, index=True)
    instance_id: str | None = Field(default=None, index=True)
    project: str | None = None
    source_id: str | None = Field(default=None, index=True)
    institution_id: str | None = None
    experiment_id: str | None = Field(default=None, index=True)
    variant_label: str | None = Field(default=None, index=True)
    variable_id: str | None = Field(default=None, index=True)
    frequency: str | None = None
    table_id: str | None = None
    grid_label: str | None = None
    nominal_resolution: str | None = None
    version: str | None = None
    data_node: str | None = None
    replica: bool | None = None
    latest: bool | None = None
    number_of_files: int | None = None
    size: int | None = None
    esgf_timestamp: str | None = None
    """Raw `_timestamp`; a change here marks the dataset as modified."""

    raw_json: str
    """The full raw search document, as JSON."""

    first_seen_run_id: int | None = Field(default=None, foreign_key="queryrun.id")
    last_seen_run_id: int | None = Field(default=None, foreign_key="queryrun.id")

    files: list["File"] = Relationship(
        back_populates="dataset",
        sa_relationship_kwargs={"cascade": "all, delete-orphan"},
    )


class File(SQLModel, table=True):
    """A cached ESGF file belonging to a `Dataset`."""

    id: str = Field(primary_key=True)
    dataset_key: str = Field(foreign_key="dataset.id", index=True)
    """Foreign key to the owning `Dataset.id`."""

    dataset_id: str
    """`dataset_id` as reported by ESGF (equals the parent dataset `id`)."""

    title: str | None = None
    size: int | None = None
    checksum: str | None = None
    checksum_type: str | None = None
    tracking_id: str | None = None
    variable_id: str | None = None
    urls_json: str | None = None
    """JSON list of the raw `url` entries (`url|mime-type|service`)."""

    esgf_timestamp: str | None = None
    raw_json: str

    dataset: Dataset | None = Relationship(back_populates="files")


class RunMembership(SQLModel, table=True):
    """Records that a given `QueryRun` returned a given dataset."""

    query_run_id: int = Field(foreign_key="queryrun.id", primary_key=True)
    dataset_id: str = Field(foreign_key="dataset.id", primary_key=True)
    esgf_timestamp: str | None = None
    """Dataset `_timestamp` at the time of this run (for modified detection)."""


class DatasetChange(SQLModel, table=True):
    """A single added/removed/modified event computed for a `QueryRun`."""

    id: int | None = Field(default=None, primary_key=True)
    query_run_id: int = Field(foreign_key="queryrun.id", index=True)
    dataset_id: str = Field(index=True)
    change_type: str
    """One of `"added"`, `"removed"` or `"modified"`."""

    detail_json: str | None = None
    """Optional JSON with extra context (e.g. old/new timestamps)."""

    created_at: datetime = Field(default_factory=_utcnow)
