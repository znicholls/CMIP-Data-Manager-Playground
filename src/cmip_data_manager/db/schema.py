"""
Database schema for cached datasets, files, access options and query history

The full workflow and the reasoning behind these grains live in
`design/search-workflow.md` (repo root); this module is its concrete realisation.

Design notes:

- **A dataset is node-independent.**  Its primary key is `instance_id` (version-
  specific but data-node-independent), so two replicas of the same dataset on
  different nodes are the *same* `Dataset` row, not two.  Everything node-specific
  moves to child tables.  A new *version* is a new `instance_id` (a separate row);
  `master_id` ties versions of the same dataset together.
- **`DatasetLocation`** captures dataset-search provenance: one row per data node
  that served the dataset, holding the raw per-node search document and per-node
  facts (`_timestamp`, `replica`).  This is where the raw JSON lives.
- **`File`** is node-independent (one row per logical file).  **`FileAccess`** is the
  per-node "where can I actually download this" table (fsspec-openable URLs).  A
  data node therefore appears at two grains — dataset (`DatasetLocation`, written
  when datasets are searched) and file (`FileAccess`, written when files are
  searched) — which are populated at different steps; neither is derived from the
  other.
- **Header-only metadata lives on the file that carries it.**  A netCDF header
  belongs to a *file*, so the full header is stored on `File.header_attrs_json`;
  the dataset-applicable subset (the `parent_*` link metadata) is *promoted* onto
  `Dataset`, with `header_from_file_key` recording exactly which file it came from.
- **Two soft (non-enforced) pointers.**  `Dataset.header_from_file_key` (-> `File`)
  and `File.header_from_access_key` (-> `FileAccess`) are indexed provenance columns
  but **not** foreign keys.  Making them real FKs would create insert-time cycles
  (`Dataset` <-> `File`, `File` <-> `FileAccess`) needing `post_update` machinery.
  They record "where this metadata was read from"; correctness never depends on
  them, and all writes go through the `Repository`, which keeps them consistent.
  The promoted metadata can legitimately point at a *sibling* dataset's file (the
  cross-variable header-reuse optimisation), which a per-dataset FK could not model
  anyway.  `parent_dataset_key` stays a real self-referential FK (a clean adjacency
  list, no cycle).
- Query history is captured by `SearchRun` (one row per search execution),
  `RunMembership` (which datasets a run returned, enabling clean diffs) and
  `DatasetChange` (the computed added/removed/modified log).  The diff *series* is
  keyed on the normalised query spec, not on any use-case name.

All parent/header columns are nullable: a use case that never touches parents (e.g.
`ssp245 tas`) produces datasets, locations, files and accesses with the parent and
header columns left `NULL`, and never runs the parent step.
"""

from datetime import datetime, timezone

from sqlalchemy import UniqueConstraint
from sqlmodel import Field, Relationship, SQLModel

# NOTE: this module intentionally does not use ``from __future__ import
# annotations``.  SQLModel resolves relationship targets from the annotations at
# runtime, and stringised annotations break the relationships below.


def _utcnow() -> datetime:
    """Return the current time as a timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)


class SearchRun(SQLModel, table=True):
    """
    One execution of a search against the index node

    A generic query-execution record.  It carries no use-case name: the diff
    *series* (added/removed/modified) is computed against previous runs with the
    same normalised query `spec_json`, so change tracking is a general-user feature
    rather than a testing artefact.
    """

    id: int | None = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=_utcnow)

    endpoint_url: str
    """Search endpoint that was queried."""

    spec_json: str
    """
    Normalised JSON description of the query that was run.

    Two runs sharing this value form a diff series, so an added/removed/modified log
    can be computed for "the same search over time".
    """

    num_found: int
    """Total number of datasets the run returned."""

    num_stored: int
    """Number of datasets written/updated in the database by the run."""

    status: str = "ok"
    """Terminal status of the run (`"ok"` or an error marker)."""

    tag: str | None = Field(default=None, index=True)
    """
    Optional freeform label for a run (e.g. a caller's use-case name).

    Never required and never used for diffing; it only exists so a caller can
    annotate a run for its own convenience.
    """


class Dataset(SQLModel, table=True):
    """
    A cached, node-independent ESGF dataset, keyed by its `instance_id`

    `instance_id` is version-specific but data-node-independent, so replicas
    collapse to one row and a new version becomes a new row.  Node-specific facts
    live on `DatasetLocation`; the dataset's files live on `File`.
    """

    instance_id: str = Field(primary_key=True)
    """Version-specific, node-independent identifier (the primary key)."""

    master_id: str | None = Field(default=None, index=True)
    """Version- and node-independent identifier; ties versions of a dataset together."""

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
    latest: bool | None = None

    # --- promoted header-only metadata (nullable; filled in the header step) ------
    parent_source_id: str | None = Field(default=None, index=True)
    parent_experiment_id: str | None = Field(default=None, index=True)
    parent_variant_label: str | None = Field(default=None, index=True)
    parent_activity_id: str | None = None
    branch_time_in_parent: str | None = None
    """Kept as text: attribute values are read as strings (e.g. `"60225.0"`)."""

    header_from_file_key: int | None = Field(default=None, index=True)
    """
    Soft pointer (indexed, **not** a foreign key) to the `File.id` whose header this
    dataset's promoted metadata was read from — which may be a *sibling* dataset's
    file (header reuse).  Provenance only; see the module docstring.
    """

    # --- parent link (a real self-referential FK; populated in the parent step) ---
    parent_dataset_key: str | None = Field(
        default=None, foreign_key="dataset.instance_id", index=True
    )
    """
    The `instance_id` of this dataset's parent, or `None`.

    Each dataset has at most one parent; a parent may have many children.  A use
    case that never resolves parents leaves this `NULL`.
    """

    first_seen_run_id: int | None = Field(default=None, foreign_key="searchrun.id")
    last_seen_run_id: int | None = Field(default=None, foreign_key="searchrun.id")

    locations: list["DatasetLocation"] = Relationship(
        back_populates="dataset",
        sa_relationship_kwargs={"cascade": "all, delete-orphan"},
    )
    files: list["File"] = Relationship(
        back_populates="dataset",
        sa_relationship_kwargs={"cascade": "all, delete-orphan"},
    )


class DatasetLocation(SQLModel, table=True):
    """
    One data node that serves a `Dataset` — dataset-search provenance

    Written when datasets are searched (Step 1): one row per `(dataset, data_node)`,
    preserving the raw per-node search document and the per-node dataset facts that
    the node-independent `Dataset` row cannot hold.
    """

    dataset_key: str = Field(foreign_key="dataset.instance_id", primary_key=True)
    """Foreign key to the owning `Dataset.instance_id`."""

    data_node: str = Field(primary_key=True)
    """Hostname of the data node serving this copy."""

    esgf_dataset_id: str | None = None
    """The node-specific ESGF dataset id (`instance_id|data_node`)."""

    replica: bool | None = None
    latest: bool | None = None
    esgf_timestamp: str | None = None
    """Raw per-node `_timestamp`; a change here marks this copy as modified."""

    size: int | None = None
    number_of_files: int | None = None

    raw_json: str
    """The full raw per-node dataset search document, as JSON."""

    first_seen_run_id: int | None = Field(default=None, foreign_key="searchrun.id")
    last_seen_run_id: int | None = Field(default=None, foreign_key="searchrun.id")

    dataset: Dataset | None = Relationship(back_populates="locations")


class File(SQLModel, table=True):
    """
    A cached, node-independent file belonging to a `Dataset`

    One row per logical file (identity is `(dataset_key, filename)`, which is stable
    across replicas); the per-node ways to download it live on `FileAccess`.  The
    file's netCDF header, once read, is stored here as `header_attrs_json`.
    """

    __table_args__ = (
        UniqueConstraint("dataset_key", "filename", name="uq_file_dataset_filename"),
    )

    id: int | None = Field(default=None, primary_key=True)
    """Surrogate primary key; the natural key is `(dataset_key, filename)`."""

    dataset_key: str = Field(foreign_key="dataset.instance_id", index=True)
    """Foreign key to the owning `Dataset.instance_id`."""

    filename: str
    """The file's name/title (identical across replicas)."""

    variable_id: str | None = None
    table_id: str | None = None
    size: int | None = None
    checksum: str | None = None
    checksum_type: str | None = None
    tracking_id: str | None = None
    """The file's PID/tracking id if published; stored, but not the row identity."""

    # --- header-only metadata (nullable until the header step reads this file) ----
    header_attrs_json: str | None = None
    """Every global attribute read from this file's netCDF header, as JSON."""

    header_from_access_key: int | None = Field(default=None, index=True)
    """
    Soft pointer (indexed, **not** a foreign key) to the `FileAccess.id` the header
    was actually read from — file-level provenance.  See the module docstring.
    """

    header_read_at: datetime | None = None

    dataset: Dataset | None = Relationship(back_populates="files")
    accesses: list["FileAccess"] = Relationship(
        back_populates="file",
        sa_relationship_kwargs={"cascade": "all, delete-orphan"},
    )


class FileAccess(SQLModel, table=True):
    """
    One place a `File` can be accessed from — a per-node access option

    Written when files are searched (Step 2): one row per way to reach a file (a
    given data node and service), carrying the concrete URL and an fsspec-openable
    form.  This is the node-availability view at file grain.
    """

    id: int | None = Field(default=None, primary_key=True)

    file_id: int = Field(foreign_key="file.id", index=True)
    """Foreign key to the owning `File.id`."""

    data_node: str = Field(index=True)
    """Hostname of the node this access reaches."""

    service: str | None = None
    """Access service, e.g. `"HTTPServer"`, `"OPENDAP"`, `"Globus"`."""

    url: str | None = None
    """The raw access URL."""

    fsspec_url: str | None = None
    """An fsspec-openable form of `url` (how to open the file with fsspec)."""

    replica: bool | None = None
    esgf_file_id: str | None = None
    """The node-specific ESGF file id."""

    raw_json: str | None = None
    """The raw per-node file search document, as JSON."""

    file: File | None = Relationship(back_populates="accesses")


class RunMembership(SQLModel, table=True):
    """Records that a given `SearchRun` returned a given dataset."""

    query_run_id: int = Field(foreign_key="searchrun.id", primary_key=True)
    dataset_key: str = Field(foreign_key="dataset.instance_id", primary_key=True)
    esgf_timestamp: str | None = None
    """Representative dataset `_timestamp` at run time (for modified detection)."""


class DatasetChange(SQLModel, table=True):
    """A single added/removed/modified event computed for a `SearchRun`."""

    id: int | None = Field(default=None, primary_key=True)
    query_run_id: int = Field(foreign_key="searchrun.id", index=True)
    dataset_key: str = Field(index=True)
    change_type: str
    """One of `"added"`, `"removed"` or `"modified"`."""

    detail_json: str | None = None
    """Optional JSON with extra context (e.g. old/new timestamps)."""

    created_at: datetime = Field(default_factory=_utcnow)


class DatasetHeader(SQLModel, table=True):
    """
    A netCDF file's cached global-attribute header — **transitional**

    Keyed at the `(source_id, experiment_id, variant_label, variable_id, table_id)`
    grain: one row per dataset.  This is the *old* simulation-grain header store; it
    is retained only so the not-yet-migrated header pipeline keeps working, and is
    **retired** once headers move onto `File.header_attrs_json` with the
    dataset-applicable subset promoted onto `Dataset` (Increment D).  Do not build
    new behaviour on it.
    """

    source_id: str = Field(primary_key=True)
    experiment_id: str = Field(primary_key=True)
    variant_label: str = Field(primary_key=True)
    variable_id: str = Field(primary_key=True)
    table_id: str = Field(primary_key=True)

    parent_source_id: str | None = Field(default=None, index=True)
    parent_experiment_id: str | None = Field(default=None, index=True)
    parent_variant_label: str | None = Field(default=None, index=True)
    parent_activity_id: str | None = None
    branch_time_in_parent: str | None = None
    """Kept as text: attribute values are read as strings (e.g. `"60225.0"`)."""

    tracking_id: str | None = None

    attrs_json: str
    """Every global attribute read, as JSON (the canonical copy)."""

    source_url: str | None = None
    """The exact mirror URL the header was read from (file-level provenance)."""

    data_node: str | None = Field(default=None, index=True)
    """Hostname of `source_url`; the data node that served the header."""

    read_at: datetime = Field(default_factory=_utcnow)


class HeaderReadAttempt(SQLModel, table=True):
    """
    One header-read attempt against a data node — an append-only log

    Where the promoted metadata keeps only the *winning* read and `NodeHealthStat`
    keeps per-host *aggregates*, this is the raw, timestamped per-attempt fact table:
    every URL tried for every simulation, in order, with its outcome and duration —
    including `with_retry` sub-attempts and simulations that fully failed.  It is
    append-only (never upserted), so it accumulates a history.

    It underpins two things the aggregates cannot:

    - **diagnosis** — for a simulation that failed, exactly which nodes and URLs were
      attempted and how each ended (timeout, crash, block, error);
    - **ad-hoc questions** — because `created_at`, `host`, `source_id`,
      `variable_id` and `outcome` are all indexed, a plain `GROUP BY` answers "how
      did node X do today?", "what happened to CanESM5's headers?", or "which nodes
      served `tas` on this day?", and a success-one-day / failure-the-next flip is
      visible by grouping a simulation's rows on `created_at`.
    """

    id: int | None = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=_utcnow, index=True)

    source_id: str = Field(index=True)
    experiment_id: str = Field(index=True)
    variant_label: str = Field(index=True)
    variable_id: str | None = Field(default=None, index=True)
    table_id: str | None = None

    host: str | None = Field(default=None, index=True)
    """Data node the attempt hit; `None` on a `no_candidate` record."""

    url: str | None = None
    """Exact mirror URL attempted; `None` on a `no_candidate` record."""

    outcome: str = Field(index=True)
    """`success`, `timeout`, `crash`, `blocked`, `error`, `no_candidate` (no mirror
    was indexed), or `stranded` (a mirror existed but was evicted before this
    simulation was tried)."""

    detail: str | None = None
    """The underlying error/exception message for a failed read (e.g. the netCDF or
    connection error text), or a short note for a synthetic `no_candidate` /
    `stranded` row; `None` on success.  Carried alongside `outcome` so a failure can
    be diagnosed without re-running it."""

    seconds: float = 0.0
    """Wall-clock duration of the attempt (`0` for a `no_candidate` record)."""

    attempt_no: int = 1
    """1-based ordinal of this attempt among retries of the same `(host, url)`."""


class NodeHealthStat(SQLModel, table=True):
    """
    Persisted per-data-node header-read outcomes

    One row per host, carrying the accumulated counters so node health survives
    across runs and can be ranked (by failure rate or response speed) with a plain
    `ORDER BY`.  A run loads these into an in-memory registry, records fresh
    outcomes against them, and writes them back.
    """

    host: str = Field(primary_key=True)
    attempts: int = 0
    successes: int = 0
    timeouts: int = 0
    crashes: int = 0
    errors: int = 0
    blocks: int = 0
    """Reads ending in a node-level block/rate-limit (HTTP 429/403/503)."""

    total_success_seconds: float = 0.0
    """Summed duration of successful reads (numerator of the mean)."""

    max_success_seconds: float = 0.0
    """Slowest successful read seen; informs a data-driven read timeout."""

    max_safe_concurrency: int = 0
    """Highest per-node connection count seen running cleanly (0 = not learned)."""

    last_concurrency: int = 0
    """Per-node connection count this host converged on last run (0 = not learned)."""

    updated_at: datetime = Field(default_factory=_utcnow)
