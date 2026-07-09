"""
Building blocks for ESGF search queries

The ESGF search API (esg-search) has two boolean rules that shape everything here:

- **different facets are combined with AND** (e.g. `variable_id=tas` *and*
  `experiment_id=ssp245`);
- **multiple values for the same facet are combined with OR**, expressed as a
  **comma-separated** list (e.g. `experiment_id=ssp245,historical`).  On the
  metagrid proxy, *repeating* a parameter is unreliable, so we always comma-join.

Crucially, a single query cannot express a per-variable/per-experiment
conjunction: `variable_id=tas,fgco2` matches datasets that are tas *or* fgco2
(each dataset has exactly one `variable_id`).  Requirements like "this model
variant has all of tas, fgco2 and nbp" are therefore satisfied by issuing several
queries and intersecting the results downstream.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

SOLR_JSON_FORMAT = "application/solr+json"
"""Value of the `format` parameter that makes the proxy return Solr JSON."""


class FacetQuery(BaseModel):
    """
    A single ESGF search request

    Facet fields hold tuples of values that are OR-ed together (comma-joined);
    different facets are AND-ed.  Free-text `query` is passed through untouched,
    which is how prefix searches such as `experiment_id:ssp*` are expressed.

    Examples
    --------
    >>> q = FacetQuery(
    ...     variable_id=("tas",), experiment_id=("ssp245",), frequency=("mon",)
    ... )
    >>> params = q.to_params(offset=0, limit=10)
    >>> params["experiment_id"], params["variable_id"], params["frequency"]
    ('ssp245', 'tas', 'mon')
    >>> params["type"], params["latest"], params["limit"], params["offset"]
    ('Dataset', 'true', '10', '0')

    Multiple values for one facet are comma-joined (logical OR):

    >>> FacetQuery(experiment_id=("ssp245", "historical")).to_params(0, 10)[
    ...     "experiment_id"
    ... ]
    'ssp245,historical'
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    project: str = "CMIP6"
    """Project facet (AND-ed with everything else)."""

    type: str = "Dataset"
    """Record type to search for: `"Dataset"` or `"File"`."""

    variable_id: tuple[str, ...] = ()
    """`variable_id` values to OR together."""

    experiment_id: tuple[str, ...] = ()
    """`experiment_id` values to OR together."""

    frequency: tuple[str, ...] = ()
    """`frequency` values to OR together (ESGF uses `"mon"` for monthly)."""

    source_id: tuple[str, ...] = ()
    """`source_id` (model) values to OR together."""

    variant_label: tuple[str, ...] = ()
    """`variant_label` values to OR together."""

    dataset_id: tuple[str, ...] = ()
    """Parent `dataset_id` values, used when searching for `type="File"`."""

    extra_facets: dict[str, tuple[str, ...]] = Field(default_factory=dict)
    """Any additional facets, keyed by facet name."""

    query: str | None = None
    """
    Free-text query in Apache Lucene syntax.

    Use this for things the facet parameters cannot express, e.g.
    `"experiment_id:ssp*"` or `"experiment_id:(historical OR esm-ssp*)"`.
    """

    latest: bool = True
    """Restrict to the latest version of each dataset."""

    replica: bool | None = None
    """If set, restrict to (`True`) or exclude (`False`) replicas."""

    distrib: bool | None = None
    """If set, control whether the search is distributed across index nodes."""

    fields: tuple[str, ...] = ()
    """If set, restrict the returned fields (`*` by default)."""

    def _facet_items(self) -> list[tuple[str, tuple[str, ...]]]:
        """Return the populated facet fields as (name, values) pairs."""
        named = {
            "variable_id": self.variable_id,
            "experiment_id": self.experiment_id,
            "frequency": self.frequency,
            "source_id": self.source_id,
            "variant_label": self.variant_label,
            "dataset_id": self.dataset_id,
        }
        items = [(k, v) for k, v in named.items() if v]
        items.extend((k, v) for k, v in self.extra_facets.items() if v)
        return items

    def to_params(self, offset: int, limit: int) -> dict[str, str]:
        """
        Render the query to esg-search URL parameters

        Parameters
        ----------
        offset
            Index of the first result to return.

        limit
            Maximum number of results to return in this page.

        Returns
        -------
        :
            Mapping of parameter name to value, ready to be URL-encoded.
        """
        params: dict[str, str] = {
            "format": SOLR_JSON_FORMAT,
            "type": self.type,
            "project": self.project,
            "offset": str(offset),
            "limit": str(limit),
            "latest": _bool_param(self.latest),
        }
        for name, values in self._facet_items():
            params[name] = ",".join(values)
        if self.replica is not None:
            params["replica"] = _bool_param(self.replica)
        if self.distrib is not None:
            params["distrib"] = _bool_param(self.distrib)
        if self.fields:
            params["fields"] = ",".join(self.fields)
        if self.query is not None:
            params["query"] = self.query
        return params

    def as_spec(self) -> dict[str, Any]:
        """
        Return a JSON-serialisable description of the query

        This is stored alongside each query run so a search can be reproduced or
        audited later.

        Returns
        -------
        :
            The query as a plain dictionary (facets and flags only, no paging).
        """
        return self.model_dump(exclude_defaults=True)


def _bool_param(value: bool) -> str:
    """Render a boolean as the lower-case string the API expects."""
    return "true" if value else "false"
