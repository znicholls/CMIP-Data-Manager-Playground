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


# TODO: complicated question.
# This data model works for ESGF1 and CMIP6 data.
# For other MIPs, it will vary.
# E.g. CMIP5 doesn't have the concept of source_id,
# I believe it is just simply called 'model'.
# It may also vary for other ESGF versions,
# e.g. ESGF-NG.
# How should we handle this?
# My suggestion would be to have multiple models,
# one for each MIP era - ESGF API combination
# (or just MIP era if all ESGF versions use the same query parameters).
# Then we have some high level class
# which uses a common vocabulary,
# then just translates out into the specific vocabulary
# expected by MIP era - ESGF API combination as required.
#
# To check ESGF-NG's API, please start with https://search.east.esgf.io/.
# There should already be CMIP6 data available via that API
# (specifically https://search.east.esgf.io/search).
# There will also be a 'west' equivalent, at
# https://search.west.esgf.io/search,
# but there is no data there yet.
# We suspect that east and west will have similar,
# but not identical behaviour so will each need their own, specific
# query and data access models/support.
# If you can read the docs and see differences already,
# we can confirm this.
# If not, we'll just have to wait until there is actually data
# on west to identify differences in behaviour.
#
# Some notes on testing this:
#
# Use cases:
# - different MIP eras and hitting different APIs
#     - using both the 'specific' query class and any general class we introduce
# - translating results back into our local search results database
#     - storing both 'raw' and 'translated' results (or we just translate in memory?)
#
# Testing:
# - offline unit/integration tests: runnable without an internet connection,
#   all responses are mocked
# - live tests, options:
#     a) record responses then make sure the API is stable
#        against those recorded responses
#        (defensive test of API because we don't trust it)
#        - very straightforward
#        - some responses should change over time (e.g. new data is added)
#        - ordering here can be tricky (need to avoid false failures)
#     b) online integration tests i.e. hit the API
#        and make sure we can handle the response
#        - hard to know what the 'correct' result is
#          normally you just say stuff like,
#          "I should end up with more than 1 and less than 1000 datasets,
#          the code shouldn't break"
# - no tests: just use this live and we'll figure it out as we go
#
# Other note
# If you want to use CMIP7 style names,
# then have a look at https://wcrp-cmip.github.io/cmip7-guidance/docs/CMIP7/Global_Attributes/
# to get them.
#
# Data access use case (which follows from this)
# - walking up the parent tree
#     - I don't know if this can be automated for CMIP5
#       i.e. the user might have to specify the tree
#     - for CMIP6, we want this to work with both ESGF1 and ESGF-NG.
#       This means solving the 'get the file header' issue
#     - for CMIP7, we might get this information in the initial response
#       i.e. not need to get file headers
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
    >>> params["type"], params["limit"], params["offset"]
    ('Dataset', '10', '0')

    By default `latest` is omitted, so **all** published versions are returned
    (Steps 2-3 then select a target version); pass ``latest=True`` to restrict to
    the latest at the index:

    >>> "latest" in params
    False
    >>> FacetQuery(latest=True).to_params(0, 10)["latest"]
    'true'

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

    # TODO: remove reference to parent in docstring here.
    # We can use this, but this ID isn't coupled to parents
    # (you could also just search for a dataset ID).
    # TODO: Is this only usable when type="File", or can it be used more generally?
    # If it is coupled to type, can we make that coupling clear
    # (e.g. by introducing a new facet query class or a validator or something else)?
    dataset_id: tuple[str, ...] = ()
    """Parent `dataset_id` values, used when searching for `type="File"`."""

    # TODO: can we get rid of this and just capture the full list of available fields?
    # Is there a reason not to do this?
    extra_facets: dict[str, tuple[str, ...]] = Field(default_factory=dict)
    """Any additional facets, keyed by facet name."""

    # TODO: do we actually want to support this?
    # It feels like it probably doesn't work and just confuses the data model.
    query: str | None = None
    """
    Free-text query in Apache Lucene syntax.

    Use this for things the facet parameters cannot express, e.g.
    `"experiment_id:ssp*"` or `"experiment_id:(historical OR esm-ssp*)"`.
    """

    latest: bool | None = None
    """
    Restrict to the latest version of each dataset (`True`) or to superseded
    versions only (`False`).

    Defaults to `None`, which **omits** the parameter so the search returns **all**
    published versions of each dataset.  The workflow stores every version at Step 1
    and then selects a target version for Steps 2-3
    (see `cmip_data_manager.search.versions.select_target_versions`); `is_latest`
    from ESGF is recorded but not trusted, because data nodes disagree about it.
    """

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
            # TODO: we're in our 'bottom-layer' here, this isn't a convenience.
            # Please, in general, avoid hard-coding and constants in the bottom-layer.
            # For example, here make format an input argument and just set the default
            # to SOLR_JSON_FORMAT, rather than using the global variable.
            "format": SOLR_JSON_FORMAT,
            "type": self.type,
            "project": self.project,
            "offset": str(offset),
            "limit": str(limit),
        }
        for name, values in self._facet_items():
            params[name] = ",".join(values)
        if self.latest is not None:
            params["latest"] = _bool_param(self.latest)
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
