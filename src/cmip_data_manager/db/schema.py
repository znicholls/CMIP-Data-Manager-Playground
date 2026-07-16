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
    # TODO: remove.
    # Let's just record queries as queries
    # with information about number found etc.
    # Let's store links betwen use cases
    # and queries somewhere else
    # (if at all).
    use_case: str = Field(index=True)
    """Name of the use case this run belongs to."""

    created_at: datetime = Field(default_factory=_utcnow)
    endpoint_url: str
    """Search endpoint that was queried."""

    # TODO: in docstring, please add a link back to the object
    # from which this JSON was created.
    # If there is a more robust way to make this link,
    # please add it.
    spec_json: str
    """JSON description of the queries that were run."""

    num_found: int
    """Total number of datasets returned by the run."""

    num_stored: int
    """Number of datasets written/updated in the database by the run."""

    # TODO: clarify docstring.
    # What does it mean by terminal status?
    # Where does status get defined?
    # Is it essentially just whether the search worked or not?
    # Can we turn this into an enum to have better visibility
    # of the possible outcomes
    # (and maybe have a status_string column to store full error
    # output if needed).
    status: str = "ok"
    """Terminal status of the run (`"ok"` or an error marker)."""


class Dataset(SQLModel, table=True):
    """A cached ESGF dataset, uniquely identified by its versioned `id`."""

    id: str = Field(primary_key=True)
    # TODO: check whether this is a thing for all ESGF responses.
    # It might only exist in ESGF1 and/or CMIP6.
    master_id: str | None = Field(default=None, index=True)
    # As above
    instance_id: str | None = Field(default=None, index=True)
    project: str | None = None
    # TODO: this is where things are going to get messy.
    # Do we have a dataset model for CMIP6, CMIP5 etc.
    # and then a high-level model?
    # Do just have one dataset model, that uses our 'harmonised' vocab
    # and then links (or stores) of the raw JSON
    # in the 'original' vocab (we always want to be able to retrieve the raw terms,
    # the question is just how)?
    source_id: str | None = Field(default=None, index=True)
    # As above
    institution_id: str | None = None
    # As above
    experiment_id: str | None = Field(default=None, index=True)
    # As above
    variant_label: str | None = Field(default=None, index=True)
    # As above (although this might actually be stable)
    variable_id: str | None = Field(default=None, index=True)
    # As above (although this might actually be stable)
    frequency: str | None = None
    # As above (doesn't exist in CMIP7)
    table_id: str | None = None
    # This has been changed to grid_id in CMIP7
    # (and the meaning is actually different, hopefully uniform grids
    # aren't important for us).
    grid_label: str | None = None
    # As above (although this might actually be stable)
    nominal_resolution: str | None = None
    # As above (not sure if in CMIP5)
    version: str | None = None
    # This should be on the files, it shouldn't exist at all on the dataset
    # (if it is part of the API response, drop it and either don't store it
    # or only have it in the full raw JSON response that we store)
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

    # TODO: add link to parent dataset
    # (each dataset can only have one parent
    # but a parent can have more than one child,
    # or a dataset can have no parents
    # (and a dataset can also have no children))
    #
    # TODO: add link to auxilliary datasets (e.g. areacella, 1:many
    # i.e. a single dataset can have multiple auxilliary datasets
    # e.g. cell area and land fraction).
    # Retrieiving this information will also require reading the header I believe.


class File(SQLModel, table=True):
    """A cached ESGF file belonging to a `Dataset`."""

    id: str = Field(primary_key=True)
    dataset_key: str = Field(foreign_key="dataset.id", index=True)
    """Foreign key to the owning `Dataset.id`."""

    # TODO: Why are we storing this, can't we just get it by looking up the dataset's ID
    # using the link back to dataset?
    dataset_id: str
    """`dataset_id` as reported by ESGF (equals the parent dataset `id`)."""

    # TODO: drop this
    title: str | None = None
    size: int | None = None
    checksum: str | None = None
    checksum_type: str | None = None
    tracking_id: str | None = None
    # TODO: Why are we storing this, can't we just get it by looking up the dataset
    # using the link back to dataset?
    variable_id: str | None = None
    # TODO: please break this out into a separate table.
    # I want that table to store 'access options' or some other name.
    # Each entry should link to a file.
    # Each file can have one or more access options,
    # via different ways e.g. url or service
    # and on different nodes.
    # Please include an fsspec column in this file,
    # which stores how to access the file in an fsspec-compliant way.
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


class DatasetHeader(SQLModel, table=True):
    """
    A netCDF file's cached global-attribute header

    Keyed at the `(source_id, experiment_id, variant_label, variable_id,
    table_id)` grain: one row per dataset.  `table_id` is part of the key because
    the same variable can be published at several frequencies (`Amon` vs `day`).
    The header describes the *simulation* and is assumed identical across a
    simulation's variables, so a later "smart reader" can reuse a sibling
    variable's row for the same `(source_id, experiment_id, variant_label)` rather
    than re-reading — but each read is still filed under the exact dataset it came
    from.

    Storage is hybrid: the frequently-queried CMIP6 `parent_*`/`tracking_id`
    attributes are promoted to indexed columns, while `attrs_json` retains the
    complete header so nothing read is ever lost and new attributes can be promoted
    later without a re-read.
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

    Where `DatasetHeader` keeps only the *winning* read and `NodeHealthStat` keeps
    per-host *aggregates*, this is the raw, timestamped per-attempt fact table:
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
