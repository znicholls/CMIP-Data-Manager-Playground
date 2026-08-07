"""
Database schema for cached datasets, versions, files, access options and history

The full workflow and the reasoning behind these grains live in
`design/search-workflow.md` (repo root); this module is its concrete realisation.

Design notes:

- **A dataset is version- and node-independent.**  `Dataset` is keyed on
  `master_id` and holds only the facets that never change between versions.  Each
  published *version* is a `DatasetVersion` (keyed on `instance_id` = master + the
  version), and everything that varies by version — files, node availability,
  header-only metadata and the parent link — hangs off the *version*, not the
  dataset.  This is the same "collapse the duplicated dimension into a child table"
  move used for data nodes, now applied to versions.
- **`DatasetVersion`** carries the `version` (validated parseable to a date, so
  versions sort chronologically), `is_latest`, size/file counts, the **parent link**
  (`parent_version_key`, a self-reference to the parent's *specific version*) and the
  promoted header-only metadata (read from *this version's* file).
- **`DatasetNodeSpecificInfo`** captures dataset-search provenance: one row per data
  node serving a *version*, holding the raw per-node search document and per-node
  facts (`_timestamp`, `replica`).  This is where the raw JSON lives.
- **`File`** is node-independent (one row per logical file of a version).
  **`FileAccess`** is the per-node "where can I actually download this" table
  (fsspec-openable URLs).  Node availability therefore exists at two grains — version
  (`DatasetNodeSpecificInfo`) and file (`FileAccess`) — populated at different steps.
- **Header-only metadata lives on the file that carries it.**  A netCDF header
  belongs to a *file*, so the full header is stored on `File.header_attrs_json`; the
  dataset-applicable subset (the `parent_*` link metadata) is *promoted* onto
  `DatasetVersion`, with `header_from_file_key` recording which file it came from.
- **Two soft (non-enforced) pointers.**  `DatasetVersion.header_from_file_key`
  (-> `File`) and `File.header_from_access_key` (-> `FileAccess`) are indexed
  provenance columns but **not** foreign keys, to avoid insert-time cycles
  (`DatasetVersion` <-> `File`, `File` <-> `FileAccess`).  Correctness never depends
  on them; all writes go through the `Repository`.  `parent_version_key` stays a real
  self-referential FK (a clean adjacency list, no cycle).
- Query history is captured by `SearchRun` (one row per search execution),
  `RunMembership` (which *versions* a run returned) and `DatasetChange` (the computed
  added/removed/modified log).  The diff *series* is keyed on the normalised query
  spec, not on any use-case name.

All parent/header columns are nullable: a use case that never touches parents (e.g.
`ssp245 tas`) produces datasets, versions, locations, files and accesses with the
parent and header columns left `NULL`, and never runs the parent step.
"""

from datetime import date, datetime, timezone

from sqlalchemy import UniqueConstraint
from sqlmodel import Field, Relationship, SQLModel

# NOTE: this module intentionally does not use ``from __future__ import
# annotations``.  SQLModel resolves relationship targets from the annotations at
# runtime, and stringised annotations break the relationships below.


def _utcnow() -> datetime:
    """Return the current time as a timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)


def parse_version_date(version: str) -> date:
    """
    Parse a CMIP6 dataset version string into a date, for chronological sorting

    Versions are published as dates (an optional leading `v`, then `YYYYMMDD`), e.g.
    `"v20191115"`.  This strips the optional `v` and parses the rest; it raises
    `ValueError` on anything that is not a date, which is how version validation is
    enforced (SQLModel table models skip pydantic validation, so the check is applied
    at the write boundary in the repository).

    Parameters
    ----------
    version
        The version string to parse.

    Returns
    -------
    :
        The version as a `date`.

    Raises
    ------
    ValueError
        If `version` is not a `[v]YYYYMMDD` date.

    Examples
    --------
    >>> parse_version_date("v20191115")
    datetime.date(2019, 11, 15)
    >>> parse_version_date("20200220")
    datetime.date(2020, 2, 20)
    """
    text = version[1:] if version[:1].lower() == "v" else version
    return datetime.strptime(text, "%Y%m%d").replace(tzinfo=timezone.utc).date()


def version_ordinal(version: str) -> int:
    """
    Parse a dataset version string into a sortable integer, for chronological ordering

    Versions are published as digit strings with an optional leading `v`: a `YYYYMMDD`
    date (CMIP6, most of CMIP5) **or** a plain incrementing integer (some CMIP5, e.g.
    `"1"`).  Both are ordered correctly by their integer value — `20120510` sorts after
    `20110101`, and a plain `1` sorts before any date — so this is the version validator
    and the "latest version" sort key.  It is looser than `parse_version_date` (it does
    not require a valid calendar date), which is what lets CMIP5's integer versions
    through; anything non-numeric still raises.

    Parameters
    ----------
    version
        The version string to parse (optionally `v`-prefixed).

    Returns
    -------
    :
        The version as an integer ordinal.

    Raises
    ------
    ValueError
        If `version` is not `[v]` followed by digits.

    Examples
    --------
    >>> version_ordinal("v20191115")
    20191115
    >>> version_ordinal("1")
    1
    """
    text = version[1:] if version[:1].lower() == "v" else version
    if not text.isdigit():
        msg = (
            f"version {version!r} is not numeric; expected a [v]YYYYMMDD date or an "
            f"integer version"
        )
        raise ValueError(msg)
    return int(text)


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

    # QUESTION: does the user specify this search spec?
    # Example is "note" "walkthrough uc1". This seems too specific to what our
    # current use cases are, not sure how this generalises
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

    # QUESTION: usefulness of status marker? Is this information meaningful?
    status: str = "ok"
    """Terminal status of the run (`"ok"` or an error marker)."""

    # QUESTION: usefulness of optional tag is for non-live searches
    # within pre-populated db?
    # Optional tag perhaps more useful than spec_json?
    # why have both?
    tag: str | None = Field(default=None, index=True)
    """
    Optional freeform label for a run (e.g. a caller's use-case name).

    Never required and never used for diffing; it only exists so a caller can
    annotate a run for its own convenience (and retrieve it via `get_dataset_records`).
    """


class Dataset(SQLModel, table=True):
    """
    A cached, version- and node-independent ESGF dataset, keyed by `master_id`

    Holds only the facets that never change between versions.  Each published version
    is a `DatasetVersion`; node-specific facts live on `DatasetNodeSpecificInfo`;
    files live on `File`.  `master_id` is the ESGF `instance_id` minus the version.
    """

    master_id: str = Field(primary_key=True)
    """Version- and node-independent identifier (the primary key)."""

    mip_era: str | None = Field(default=None, index=True)
    """The MIP era discriminator (`"CMIP5"`, `"CMIP6"`); indexed so any use case can
    filter by era.  Distinct from `project` (an ESGF-NG collection may differ)."""

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

    first_seen_run_id: int | None = Field(default=None, foreign_key="searchrun.id")
    last_seen_run_id: int | None = Field(default=None, foreign_key="searchrun.id")

    versions: list["DatasetVersion"] = Relationship(
        back_populates="dataset",
        sa_relationship_kwargs={"cascade": "all, delete-orphan"},
    )


class DatasetVersion(SQLModel, table=True):
    """
    One published version of a `Dataset`, keyed by its `instance_id`

    `instance_id` is `master_id` plus the version.  Everything version-specific lives
    here: the `version` string (validated parseable to a date — see
    `parse_version_date`), `is_latest`, size/file counts, the parent link
    (`parent_version_key`, pointing at the parent's *specific version*) and the
    promoted header-only metadata read from this version's file.
    """

    instance_id: str = Field(primary_key=True)
    """Version-specific, node-independent identifier (`master_id` + version)."""

    dataset_key: str = Field(foreign_key="dataset.master_id", index=True)
    """Foreign key to the owning `Dataset.master_id`."""

    mip_era: str | None = Field(default=None, index=True)
    """The MIP era discriminator, mirrored from the owning `Dataset` for
    version-scoped era filtering."""

    version: str
    """The version string (e.g. `"v20191115"`); validated parseable to a date."""

    is_latest: bool | None = None
    size: int | None = None
    number_of_files: int | None = None

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
    version's promoted metadata was read from — which may be a *sibling* version's
    file (header reuse).  Provenance only; see the module docstring.
    """

    # --- parent link (a real self-referential FK; populated in the parent step) ---
    parent_version_key: str | None = Field(
        default=None, foreign_key="datasetversion.instance_id", index=True
    )
    """
    The `instance_id` of this version's parent version, or `None`.

    Each version has at most one parent version; a parent may have many children.
    A use case that never resolves parents leaves this `NULL`.
    """

    first_seen_run_id: int | None = Field(default=None, foreign_key="searchrun.id")
    last_seen_run_id: int | None = Field(default=None, foreign_key="searchrun.id")

    dataset: Dataset | None = Relationship(back_populates="versions")
    locations: list["DatasetNodeSpecificInfo"] = Relationship(
        back_populates="version",
        sa_relationship_kwargs={"cascade": "all, delete-orphan"},
    )
    files: list["File"] = Relationship(
        back_populates="version",
        sa_relationship_kwargs={"cascade": "all, delete-orphan"},
    )


class DatasetNodeSpecificInfo(SQLModel, table=True):
    """
    One data node that serves a `DatasetVersion` — dataset-search provenance

    Written when datasets are searched (Step 1): one row per `(version, data_node)`,
    preserving the raw per-node search document and the per-node facts that the
    node-independent `DatasetVersion` row cannot hold.
    """

    version_key: str = Field(foreign_key="datasetversion.instance_id", primary_key=True)
    """Foreign key to the owning `DatasetVersion.instance_id`."""

    data_node: str = Field(primary_key=True)
    """Hostname of the data node serving this copy."""

    esgf_dataset_id: str | None = None
    """The node-specific ESGF dataset id (`instance_id|data_node`)."""

    replica: bool | None = None
    esgf_timestamp: str | None = None
    """Raw per-node `_timestamp`; a change here marks this copy as modified."""

    raw_json: str
    """The full raw per-node dataset search document, as JSON."""

    first_seen_run_id: int | None = Field(default=None, foreign_key="searchrun.id")
    last_seen_run_id: int | None = Field(default=None, foreign_key="searchrun.id")

    version: DatasetVersion | None = Relationship(back_populates="locations")


class Cmip5VersionExtra(SQLModel, table=True):
    """
    CMIP5-only facets promoted from raw JSON, one row per CMIP5 `DatasetVersion`

    A per-era 1:1 side table (the pattern from `design/multi-mip-era-search-plan.md`
    §6): the canonical `Dataset`/`DatasetVersion` stay era-agnostic, and CMIP5's extra
    facets that have no canonical column live here, keyed on the version's id.  Only
    written for `mip_era == "CMIP5"`.

    Two jobs.  **Provenance:** it keeps the native CMIP5-DRS ids (which are *table*-
    grained — no variable — because a CMIP5 dataset holds many variables) and the
    `realm` that was folded into `grid_label`.  **Disambiguation:** `base_master_id` is
    the reconstructed master id *without* any collision suffix, while
    `distinguishing_json` records the facet(s) that split this version (`product`).
    Grouping by `base_master_id` and reading each row's `distinguishing_json` is how the
    repository both assigns the `.N` suffix at write time and surfaces the user's choice
    (see `Repository.cmip5_distinguishing_conflicts`) — no mapping table needed.
    """

    version_key: str = Field(foreign_key="datasetversion.instance_id", primary_key=True)
    """Foreign key to the owning `DatasetVersion.instance_id` (1:1)."""

    base_master_id: str | None = Field(default=None, index=True)
    """The reconstructed `master_id` **without** any collision suffix; groups the
    variants (e.g. products) of one simulation so a `.N` suffix can be assigned and the
    choice surfaced."""

    distinguishing_json: str | None = None
    """JSON `{facet: value}` of the facet(s), outside the `Dataset` columns, that made
    this version distinct from a same-`base_master_id` sibling (e.g.
    `{"product": "output2"}`).  Generic, so a future era's facet needs no new column."""

    realm: str | None = None
    """The CMIP5 `realm` (`atmos`/`ocean`/…); folded into `grid_label` in the id."""

    native_master_id: str | None = None
    """The raw CMIP5-DRS `master_id` (table-grained, no variable), for provenance."""

    native_dataset_id: str | None = None
    """The raw CMIP5-DRS `instance_id` (table-grained, versioned), for provenance and to
    drive the variable-filtered Step-2 file search."""


class File(SQLModel, table=True):
    """
    A cached, node-independent file belonging to a `DatasetVersion`

    One row per logical file (identity is `(version_key, filename)`, stable across
    replicas); the per-node ways to download it live on `FileAccess`.  The file's
    netCDF header, once read, is stored here as `header_attrs_json`.
    """

    __table_args__ = (
        UniqueConstraint("version_key", "filename", name="uq_file_version_filename"),
    )

    id: int | None = Field(default=None, primary_key=True)
    """Surrogate primary key; the natural key is `(version_key, filename)`."""

    version_key: str = Field(foreign_key="datasetversion.instance_id", index=True)
    """Foreign key to the owning `DatasetVersion.instance_id`."""

    filename: str
    """The file's name/title (identical across replicas)."""

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

    version: DatasetVersion | None = Relationship(back_populates="files")
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

    data_node: str | None = Field(default=None, index=True)
    """Hostname of the node this access reaches (the URL host)."""

    service: str | None = None
    """Access service, e.g. `"HTTPServer"`, `"OPENDAP"`, `"Globus"`."""

    url: str | None = None
    """The raw access URL."""

    # QUESTION: does this have to start with fsspec?
    # Currently is just https:// URLs...
    fsspec_url: str | None = None
    """An fsspec-openable form of `url` (how to open the file with fsspec)."""

    # QUESTION: is replica important here?
    replica: bool | None = None
    esgf_file_id: str | None = None
    """The node-specific ESGF file id."""

    # QUESTION : this is null in tiny use-case1. Will this ever be populated?
    raw_json: str | None = None
    """The raw per-node file search document, as JSON."""

    file: File | None = Relationship(back_populates="accesses")


class RunMembership(SQLModel, table=True):
    """Records that a given `SearchRun` returned a given dataset version."""

    query_run_id: int = Field(foreign_key="searchrun.id", primary_key=True)
    version_key: str = Field(foreign_key="datasetversion.instance_id", primary_key=True)
    esgf_timestamp: str | None = None
    """Representative version `_timestamp` at run time (for modified detection)."""


class DatasetChange(SQLModel, table=True):
    """A single added/removed/modified event computed for a `SearchRun`."""

    id: int | None = Field(default=None, primary_key=True)
    query_run_id: int = Field(foreign_key="searchrun.id", index=True)
    version_key: str = Field(index=True)
    change_type: str
    """One of `"added"`, `"removed"` or `"modified"`."""

    # QUESTION: is this necessary? does the user provide detail for this, or
    # are there potential in-built
    # details depending on if changes?
    detail_json: str | None = None
    """Optional JSON with extra context (e.g. old/new timestamps)."""

    created_at: datetime = Field(default_factory=_utcnow)


class HeaderReadAttempt(SQLModel, table=True):
    """
    One header-read attempt against a data node — an append-only log

    Where the promoted metadata keeps only the *winning* read and `DataNodeHealthStat`
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


class DataNodeHealthStat(SQLModel, table=True):
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


class IndexNodeHealthStat(SQLModel, table=True):
    """
    Persisted per-search-index-endpoint file-search outcomes (Step 2)

    The Step-2 twin of `DataNodeHealthStat`: where that records *data node* header-read
    health, this records *search index* endpoint health — attempts, successes,
    failures, how many calls were **retries**, and the subset of failures that were
    **server errors** (5xx) or **timeouts**.  One row per endpoint, keyed on the full
    search URL (two proxies on the same host differ by path), carrying accumulated
    counters so index-node health survives across runs and can be ranked with a plain
    `ORDER BY`.  A run loads these, records fresh outcomes against them, and writes
    them back (incrementally, so a crash keeps what was learned).
    """

    endpoint: str = Field(primary_key=True)
    """Full search endpoint URL (e.g. `https://esgf.ceda.ac.uk/esg-search/search`)."""

    attempts: int = 0
    """Total search calls made to this endpoint, including retries."""

    successes: int = 0
    failures: int = 0
    """Calls that errored (`attempts == successes + failures`)."""

    retries: int = 0
    """Calls that were a retry of an earlier attempt (attempt number > 1)."""

    server_errors: int = 0
    """Failures that were an HTTP 5xx (e.g. the metagrid-west 500s)."""

    timeouts: int = 0
    """Failures that were a connect/read timeout."""

    total_success_seconds: float = 0.0
    """Summed duration of successful calls (numerator of the mean)."""

    max_success_seconds: float = 0.0
    """Slowest successful call seen; surfaces a pathologically slow-but-alive node."""

    updated_at: datetime = Field(default_factory=_utcnow)


class FileAccessAttempt(SQLModel, table=True):
    """
    One file-search attempt against a search-index endpoint (Step 2) — append-only

    The Step-2 twin of `HeaderReadAttempt`: where `IndexNodeHealthStat` keeps
    per-endpoint *aggregates*, this is the raw, timestamped per-attempt fact table —
    every file search issued for every dataset version, in order, with the endpoint it
    hit, its outcome and duration, including in-request backoff retries, requeue passes,
    endpoint fallbacks and versions that fully failed.  It is append-only (never
    upserted), so it accumulates a history across runs.

    It underpins two things the aggregate cannot:

    - **diagnosis** — for a version whose files could not be found, exactly which
      endpoints were tried and how each ended (`server_error`, `timeout`, `error`,
      `overflow`, or an **`empty`** 200 that returned no files — the case behind
      Step 3's `no_files`);
    - **ad-hoc questions** — because `created_at`, `endpoint`, `version_key` and
      `outcome` are all indexed, a plain `GROUP BY` answers "how did endpoint X do
      today?", "what happened to this version's file search?", or "which versions came
      back empty?".
    """

    id: int | None = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=_utcnow, index=True)

    endpoint: str = Field(index=True)
    """Search-index endpoint the attempt hit (the full search URL)."""

    version_key: str = Field(index=True)
    """`DatasetVersion.instance_id` whose files this attempt searched for."""

    outcome: str = Field(index=True)
    """`success`, `empty` (HTTP 200 but zero files), `server_error`, `timeout`, `error`,
    or `overflow` (the single-version result exceeded the retrieval cap)."""

    files_found: int = 0
    """Number of file records the search returned (0 on a failure or an `empty` hit)."""

    detail: str | None = None
    """Underlying error/exception text for a failed attempt; `None` on success/empty."""

    seconds: float = 0.0
    """Wall-clock duration of the attempt."""

    attempt_no: int = 1
    """1-based ordinal of this attempt among the backoff retries on this endpoint."""


class FileDownloadAttempt(SQLModel, table=True):
    """
    One file-download attempt against a data node — an append-only log

    The download twin of `HeaderReadAttempt`/`FileAccessAttempt`.  Where
    `DownloadNodeHealthStat` keeps per-host *aggregates* and `FileDownload` keeps only
    the *winning* download, this is the raw, timestamped per-attempt fact table — every
    mirror URL tried for every file, in order, with its outcome, bytes transferred,
    duration and measured throughput, including `with_retry` sub-attempts, resumed
    (`Range`) continuations and files that fully failed.  It is append-only (never
    upserted), so it accumulates a history across runs.

    It underpins two things the aggregate cannot:

    - **diagnosis** — for a file that could not be downloaded, exactly which nodes and
      URLs were attempted and how each ended (`timeout`, `blocked`, `host_fault`,
      `checksum_failed`, `error`);
    - **ad-hoc questions** — because `created_at`, `host`, `file_id` and `outcome` are
      all indexed, a plain `GROUP BY` answers "how fast did node X serve today?",
      "which files failed their checksum?", or "which nodes are worth preferring?".
    """

    id: int | None = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=_utcnow, index=True)

    file_id: int | None = Field(default=None, foreign_key="file.id", index=True)
    """`File.id` this attempt tried to download; `None` on a `no_candidate` record."""

    host: str | None = Field(default=None, index=True)
    """Data node the attempt hit; `None` on a `no_candidate` record."""

    url: str | None = None
    """Exact mirror URL attempted; `None` on a `no_candidate` record."""

    outcome: str = Field(index=True)
    """`success`, `timeout`, `blocked` (HTTP 429/403/503), `host_fault` (SSL/DNS/connect
    failure), `checksum_failed` (bytes arrived but the digest mismatched), `error`,
    `no_candidate` (no mirror was indexed), or `stranded` (a mirror existed but its host
    was evicted before this file was tried)."""

    detail: str | None = None
    """Underlying error/exception text for a failed attempt (connection error, an
    expected/actual checksum mismatch, …), or a short note for a synthetic
    `no_candidate` / `stranded` row; `None` on success."""

    bytes_downloaded: int = 0
    """Bytes transferred this attempt (a resumed attempt counts only the bytes it added
    on top of the pre-existing `.part`)."""

    seconds: float = 0.0
    """Wall-clock duration of the attempt (`0` for a `no_candidate` record)."""

    throughput_mbps: float = 0.0
    """Measured throughput in MB/s (`bytes_downloaded / seconds`, MB = 1e6 bytes) — the
    download-speed signal a header read cannot provide.  `0` when no bytes moved."""

    attempt_no: int = 1
    """1-based ordinal of this attempt among retries of the same `(host, url)`."""

    resumed: bool = False
    """Whether this attempt continued a partial `.part` file via an HTTP `Range` request
    rather than starting from byte 0."""


class DownloadNodeHealthStat(SQLModel, table=True):
    """
    Persisted per-data-node *download* outcomes — separate from header-read health

    The download twin of `DataNodeHealthStat`, kept deliberately separate because a
    header read and a full-file download measure different things: a header read moves a
    few KB and times *latency*, whereas a download moves the whole (often multi-GB) file
    and the signal that matters is *throughput* (MB/s).  A node that is "dead" for
    header reads can download perfectly well, so download health must **not** inherit
    header verdicts — it is learned from downloads alone.

    One row per host, carrying accumulated counters and byte/second totals so download
    node health survives across runs and can be ranked by throughput or reliability with
    a plain `ORDER BY`.  A run loads these into an in-memory registry, records fresh
    outcomes against them, and writes them back (incrementally, so a crash keeps what
    was learned).  Persisted health is *informational ranking only*: it is never turned
    into a cross-restart ignore list — every run re-probes every node from scratch,
    because data nodes are a moving target.
    """

    host: str = Field(primary_key=True)
    attempts: int = 0
    successes: int = 0
    timeouts: int = 0
    blocks: int = 0
    """Downloads ending in a node-level block/rate-limit (HTTP 429/403/503)."""

    host_faults: int = 0
    """Downloads ending in a host fault (SSL/DNS/connect failure — node unreachable)."""

    checksum_failures: int = 0
    """Downloads whose bytes arrived but failed checksum verification — a node-quality
    signal distinct from a transient transfer error."""

    errors: int = 0
    """Downloads ending in any other transient error."""

    total_bytes: int = 0
    """Summed bytes of successful downloads (numerator of the mean throughput)."""

    total_success_seconds: float = 0.0
    """Summed duration of successful downloads (denominator of the mean throughput)."""

    max_success_seconds: float = 0.0
    """Slowest successful download seen."""

    max_safe_concurrency: int = 0
    """Highest per-node connection count seen downloading cleanly (0 = not learned)."""

    last_concurrency: int = 0
    """Per-node connection count this host converged on last run (0 = not learned)."""

    updated_at: datetime = Field(default_factory=_utcnow)


class FileDownload(SQLModel, table=True):
    """
    Terminal download state for a `File`, one row per downloaded file (1:1 side table)

    The download step's state table, following the per-concern 1:1 side-table pattern of
    `Cmip5VersionExtra`: the node-independent `File` stays search-focused, and the "have
    we downloaded this, where did it land, did it verify" facts live here, keyed on
    `File.id`.  Written the instant a file completes (save-as-you-go), so a killed run
    keeps every file it finished.  Only the *winning* download is recorded here; every
    attempt (including failures) lives on the append-only `FileDownloadAttempt` log.

    Version-level completeness is **not** stored — it is *derived* (a version is
    complete when all its `File`s have a `complete` row here), so a failed file is just
    a failed file and never blocks its siblings.
    """

    file_id: int = Field(foreign_key="file.id", primary_key=True)
    """Foreign key to the owning `File.id` (1:1)."""

    local_path: str | None = None
    """Absolute path the file was written to (its DRS location under the download root);
    `None` until a download completes."""

    status: str = Field(index=True)
    """`complete` (downloaded and, where a checksum existed, verified), `unverified`
    (downloaded but no usable checksum to verify against), `failed` (all mirrors
    exhausted), or `skipped` (already present and verified before this run)."""

    size_bytes: int | None = None
    """Size of the downloaded file on disk, in bytes."""

    verified: bool = False
    """Whether the on-disk bytes were checked against a published checksum."""

    verified_algo: str | None = None
    """The digest algorithm actually used to verify (`md5`/`sha256`/…), resolved from
    `File.checksum_type` (ESGF1) or decoded from a STAC multihash (ESGF-NG); `None` when
    the file could not be verified."""

    download_from_access_key: int | None = Field(default=None, index=True)
    """
    Soft pointer (indexed, **not** a foreign key) to the `FileAccess.id` the winning
    download came from — which mirror/node served it.  Provenance only, mirroring
    `File.header_from_access_key`.
    """

    attempts: int = 0
    """Total download attempts made for this file across all mirrors (quick triage
    without scanning the `DownloadAttempt` log)."""

    completed_at: datetime | None = None
