"""
Normalised records parsed from raw ESGF search responses

Every field in a raw esg-search document arrives as a (usually single-element)
list, e.g. `{"variable_id": ["tas"], "frequency": ["mon"], ...}`.  These models
flatten that into plain scalars while keeping the full raw document around, so
nothing is lost when we later persist or debug a record.

These records are intentionally decoupled from the database models: the ESGF
layer does not import the database layer.  The database layer knows how to turn a
`DatasetRecord`/`FileRecord` into a stored row.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict


class AmbiguousFieldError(ValueError):
    """
    Raised when a field expected to hold one value holds several

    ESGF returns every field as a list.  For fields that should be single-valued
    (e.g. `variable_id`, `source_id`, `frequency`) we would rather fail loudly on
    a record that carries conflicting values than silently keep only the first.
    """

    def __init__(self, key: str, values: list[Any]) -> None:
        self.key = key
        self.values = values
        super().__init__(
            f"Field {key!r} was expected to have a single value but had "
            f"{len(values)} distinct values: {values!r}"
        )


def _single(doc: dict[str, Any], key: str) -> Any | None:
    """
    Return the sole value for `key`, defending against unexpected multiples

    Identical repeated values are collapsed to one; genuinely conflicting values
    raise `AmbiguousFieldError` rather than being silently dropped.

    Parameters
    ----------
    doc
        Raw search document.

    key
        Field to look up.

    Returns
    -------
    :
        The scalar value, or `None` if the field is missing or empty.

    Raises
    ------
    AmbiguousFieldError
        If `key` holds more than one distinct value.
    """
    value = doc.get(key)
    if not isinstance(value, list):
        return value
    distinct = list(dict.fromkeys(value))
    if not distinct:
        return None
    if len(distinct) > 1:
        raise AmbiguousFieldError(key, distinct)
    return distinct[0]


def _single_str(doc: dict[str, Any], key: str) -> str | None:
    """Return the sole value for `key` coerced to a string (e.g. `version`)."""
    value = _single(doc, key)
    return None if value is None else str(value)


def _as_list(doc: dict[str, Any], key: str) -> list[Any]:
    """Return `key` as a list, wrapping a lone scalar and defaulting to `[]`."""
    value = doc.get(key)
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


class DatasetRecord(BaseModel):
    """
    A normalised ESGF dataset

    The `id` uniquely identifies a dataset version on a particular data node
    (it includes the version and the data node), which is why we use it as the
    primary key downstream.  `master_id` is version- and node-independent.
    """

    model_config = ConfigDict(frozen=True)

    id: str
    """Unique dataset id including version and data node."""

    master_id: str | None = None
    """Version- and node-independent identifier."""

    instance_id: str | None = None
    """Version-specific, node-independent identifier."""

    project: str | None = None
    source_id: str | None = None
    institution_id: str | None = None
    experiment_id: str | None = None
    variant_label: str | None = None
    variable_id: str | None = None
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
    """Value of the raw `_timestamp` field; used to detect modifications."""

    raw: dict[str, Any]
    """The full raw search document."""

    @classmethod
    def from_solr(cls, doc: dict[str, Any]) -> DatasetRecord:
        """
        Build a `DatasetRecord` from a raw Solr dataset document

        Parameters
        ----------
        doc
            Raw document from the `response.docs` array.

        Returns
        -------
        :
            The normalised record.
        """
        return cls(
            id=str(doc["id"]),
            master_id=_single_str(doc, "master_id"),
            instance_id=_single_str(doc, "instance_id"),
            project=_single_str(doc, "project"),
            source_id=_single_str(doc, "source_id"),
            institution_id=_single_str(doc, "institution_id"),
            experiment_id=_single_str(doc, "experiment_id"),
            variant_label=_single_str(doc, "variant_label"),
            variable_id=_single_str(doc, "variable_id"),
            frequency=_single_str(doc, "frequency"),
            table_id=_single_str(doc, "table_id"),
            grid_label=_single_str(doc, "grid_label"),
            nominal_resolution=_single_str(doc, "nominal_resolution"),
            version=_single_str(doc, "version"),
            data_node=_single_str(doc, "data_node"),
            replica=_single(doc, "replica"),
            latest=_single(doc, "latest"),
            number_of_files=_single(doc, "number_of_files"),
            size=_single(doc, "size"),
            esgf_timestamp=_single_str(doc, "_timestamp"),
            raw=doc,
        )


class FileRecord(BaseModel):
    """
    A normalised ESGF file belonging to a dataset

    `dataset_id` points at the parent dataset's `id` on the same data node, which
    is how files are linked back to datasets.
    """

    model_config = ConfigDict(frozen=True)

    id: str
    """Unique file id."""

    dataset_id: str
    """`id` of the parent dataset (same data node)."""

    title: str | None = None
    size: int | None = None
    checksum: str | None = None
    checksum_type: str | None = None
    tracking_id: str | None = None
    variable_id: str | None = None
    urls: tuple[str, ...] = ()
    """Raw `url` entries, each formatted `url|mime-type|service`."""

    esgf_timestamp: str | None = None
    raw: dict[str, Any]

    @classmethod
    def from_solr(cls, doc: dict[str, Any]) -> FileRecord:
        """
        Build a `FileRecord` from a raw Solr file document

        Parameters
        ----------
        doc
            Raw document from the `response.docs` array.

        Returns
        -------
        :
            The normalised record.
        """
        return cls(
            id=str(doc["id"]),
            dataset_id=str(_single(doc, "dataset_id")),
            title=_single_str(doc, "title"),
            size=_single(doc, "size"),
            checksum=_single_str(doc, "checksum"),
            checksum_type=_single_str(doc, "checksum_type"),
            tracking_id=_single_str(doc, "tracking_id"),
            variable_id=_single_str(doc, "variable_id"),
            urls=tuple(str(u) for u in _as_list(doc, "url")),
            esgf_timestamp=_single_str(doc, "_timestamp"),
            raw=doc,
        )
