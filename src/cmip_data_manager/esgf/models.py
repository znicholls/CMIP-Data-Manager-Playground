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

    @property
    def instance_key(self) -> str:
        """
        The version-specific, node-independent identity (a `DatasetVersion` key)

        `instance_id` when the search provided it, otherwise the `id` with any
        trailing `|data_node` stripped (the ESGF `id` is `instance_id|data_node`).
        """
        if self.instance_id:
            return self.instance_id
        return self.id.split("|", 1)[0]

    @property
    def master_key(self) -> str:
        """
        The version- and node-independent dataset identity (a `Dataset` key)

        `master_id` when the search provided it, otherwise the `instance_key` with a
        trailing `.[v]YYYYMMDD` version segment stripped (the ESGF `instance_id` is
        `master_id` + `.` + version).
        """
        if self.master_id:
            return self.master_id
        head, sep, tail = self.instance_key.rpartition(".")
        return head if sep and tail.lstrip("vV").isdigit() else self.instance_key

    @property
    def node_key(self) -> str:
        """
        The data node this record came from, for the per-node location row

        `data_node` when provided, otherwise the part of `id` after `|`, falling
        back to `"unknown"` when the id carries no node (e.g. in tests).
        """
        if self.data_node:
            return self.data_node
        _, _, node = self.id.partition("|")
        return node or "unknown"

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

    @classmethod
    def from_stac(cls, feature: dict[str, Any]) -> DatasetRecord:
        """
        Build a `DatasetRecord` from a raw ESGF-NG STAC feature

        The normalised columns are populated in the **same ESGF1 vocabulary** as
        `from_solr`, so the database schema and every downstream reader are unchanged
        — but `raw` keeps the **STAC feature verbatim** (not a Solr-shaped
        translation), so nothing the STAC/CQL2 API returned is lost.

        The collection-facet prefix (`cmip6:`, `cordex-cmip6:`, …) is **derived** from
        `feature["collection"]` (lower-cased), never hard-coded, so the same parser
        serves every collection.  A STAC item is a single, node-independent dataset
        version, so its `id` is the `instance_id`; there is no data node or replica
        dimension (`data_node`/`replica` are `None` — files/hosts live in `assets`).

        Parameters
        ----------
        feature
            One GeoJSON `Feature` from a STAC `FeatureCollection.features` array.

        Returns
        -------
        :
            The normalised record, with the raw STAC feature retained in `raw`.
        """
        collection = str(feature.get("collection", "") or "")
        prefix = f"{collection.lower()}:" if collection else ""
        props: dict[str, Any] = feature.get("properties", {}) or {}

        def facet(name: str) -> str | None:
            return _single_str(props, f"{prefix}{name}")

        assets = feature.get("assets", {}) or {}
        data_assets = sum(
            1 for a in assets.values() if "data" in (a.get("roles") or [])
        )
        return cls(
            id=str(feature["id"]),
            master_id=_single_str(props, "base_id"),
            instance_id=str(feature["id"]),
            project=collection or None,
            source_id=facet("source_id"),
            institution_id=facet("institution_id"),
            experiment_id=facet("experiment_id"),
            variant_label=facet("variant_label"),
            variable_id=facet("variable_id"),
            frequency=facet("frequency"),
            table_id=facet("table_id"),
            grid_label=facet("grid_label"),
            nominal_resolution=facet("nominal_resolution"),
            version=_single_str(props, "version"),
            data_node=None,
            replica=None,
            latest=_single(props, "latest"),
            number_of_files=data_assets or None,
            size=_single(props, "size"),
            esgf_timestamp=_single_str(props, "updated"),
            raw=feature,
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
