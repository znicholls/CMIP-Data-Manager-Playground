"""
MIP-era vocabulary profiles: one canonical (CMIP6) vocabulary, translated per era

The search workflow speaks a single, **CMIP6-anchored** vocabulary everywhere
(`FacetQuery` facets, `DatasetRecord` columns, the database schema).  Each MIP era,
however, exposes its facets under different **names** on the ESGF index — CMIP5 calls
`source_id` `model`, `experiment_id` `experiment`, and so on.  An `EraProfile`
captures that per-era knowledge declaratively, in one place, so the workflow body
stays era-agnostic and the only era-specific logic is a pair of name renames plus (for
CMIP5) an id reconstruction.

The two rename directions are:

- **outbound** (`to_native_params`) — a canonical `FacetQuery` renders canonical param
  keys (`source_id=…`); the backend renames them to the era's native keys
  (`model=…`) just before the request goes out;
- **inbound** (`to_canonical_doc`) — a raw era-native search document (`{"model":
  ["CESM2"], …}`) is renamed back to canonical keys so `DatasetRecord.from_solr` reads
  it unchanged.  The **original** document is always retained verbatim as
  `DatasetRecord.raw`, so nothing era-specific is ever lost.

Only the *names* are translated; **values are era-native** (a CMIP5 caller passes
`rcp45`, `r1i1p1`), because cross-era value equivalences (`rcp45` ≟ `ssp245`) are
scientifically false and are never fabricated here.

Three profiles are shipped: `CMIP6_PROFILE` (the identity map — current behaviour,
now routed through the seam), `CMIP5_PROFILE`, and — later — a CMIP7 profile.  Look
one up by era with `get_profile`.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

from cmip_data_manager.esgf.backends.base import Flavour
from cmip_data_manager.esgf.cmip5 import reconstruct_ids as _reconstruct_cmip5_ids
from cmip_data_manager.esgf.models import DatasetRecord

# Canonical (CMIP6) facet names that never change between versions and that a query
# may translate.  Kept here so a profile's ``field_map`` can be validated against the
# vocabulary it claims to translate.
CANONICAL_FACETS: frozenset[str] = frozenset(
    {
        "source_id",
        "institution_id",
        "experiment_id",
        "variant_label",
        "variable_id",
        "frequency",
        "table_id",
        "activity_id",
        "grid_label",
        "nominal_resolution",
        "sub_experiment_id",
    }
)
"""The canonical (CMIP6) facet names an `EraProfile.field_map` may rename."""


@dataclass(frozen=True)
class EraProfile:
    """
    One MIP era's translation knowledge, as a declarative object

    An `EraProfile` is pure data plus the small rename helpers derived from it; it does
    no I/O and holds no state, so each profile is unit-testable in isolation.

    Attributes
    ----------
    mip_era
        The era label (`"CMIP5"`, `"CMIP6"`, `"CMIP7"`); the discriminator stored on
        each `Dataset`.

    project_facet
        The value to send for the `project` search parameter (`"CMIP5"`, …).

    field_map
        Canonical (CMIP6) facet name → era-native facet name.  Only entries that
        **differ** are listed; an empty map (CMIP6) means the identity translation.

    absent_facets
        Canonical facets that simply do not exist in this era (CMIP5 has no
        `activity_id`/`grid_label`).  They are dropped from outbound queries and left
        `NULL` on inbound records.

    parent_strategy
        Ordered names of the parent-resolution layers this era uses, tried in order
        (`"record"`, `"header"`, `"override"`).  Consumed by the parent resolver.

    supported_flavours
        Transport dialects this era can be served over; constructing the era against
        any other endpoint is an unsupported transport-and-era combination.

    reconstruct
        An optional hook that **rebuilds** `master_id`/`instance_id` (and any derived
        column such as `grid_label`) from the canonical columns, for eras whose native
        ids do not match the canonical schema.  `None` (CMIP6) trusts the raw ids;
        CMIP5 supplies a reconstruction that rebuilds a CMIP6-shaped id.

    distinguishing_facets
        Native facet names, **outside** the canonical `Dataset` columns, that can make
        two otherwise-identical datasets distinct (CMIP5 `product`: `output1` vs
        `output2`).  When two records share a reconstructed base id but differ on these,
        the repository disambiguates the second with a `.N` suffix and records which
        facet(s) differed, so a user can choose.  Empty (CMIP6) means every dataset is
        already unique on its columns — no disambiguation ever runs.

    multi_variable
        Whether one native dataset holds **many** variables (CMIP5, whose datasets are
        *table*-grained — an `Amon` dataset carries ~58 variables, the variable living
        only at the file grain).  When `True`, a search document is **expanded** into
        one record per *requested* variable it contains (see `expand`).  `False`
        (CMIP6/NG) means datasets are already single-variable — expansion is identity.
    """

    mip_era: str
    project_facet: str
    supported_flavours: frozenset[Flavour]
    field_map: Mapping[str, str] = field(default_factory=dict)
    absent_facets: frozenset[str] = frozenset()
    parent_strategy: tuple[str, ...] = ("header", "override")
    reconstruct: Callable[[DatasetRecord], DatasetRecord] | None = None
    distinguishing_facets: tuple[str, ...] = ()
    multi_variable: bool = False

    def expand(
        self, doc: Mapping[str, Any], requested_variables: tuple[str, ...]
    ) -> list[dict[str, Any]]:
        """
        Expand one raw search document into one document per requested variable

        For a single-variable era (CMIP6) this is the identity: `[dict(doc)]`.  For a
        multi-variable era (CMIP5) a *table* document lists many variables, so it is
        projected onto the **intersection** of its variables and the caller's requested
        ones, yielding one shallow-copied document per surviving variable (each with the
        native variable field set to that single value).  Downstream parsing then sees a
        clean single-variable document and never trips the many-values ambiguity guard.

        Because a table with many variables and **no** requested variable would explode
        into dozens of datasets, that case raises instead — a CMIP5 dataset search must
        be variable-scoped (existence checks that omit the variable use `count`, which
        never reaches here).

        Parameters
        ----------
        doc
            One raw (era-native) search document.

        requested_variables
            The variable values the query asked for (native values, e.g. `("tas",)`).

        Returns
        -------
        :
            One document per variable to build a record from (a copy each).

        Raises
        ------
        ValueError
            If the era is multi-variable, the document lists several variables, and no
            variable was requested to project onto.

        Examples
        --------
        >>> CMIP6_PROFILE.expand({"variable_id": ["tas"]}, ())
        [{'variable_id': ['tas']}]
        >>> docs = CMIP5_PROFILE.expand(
        ...     {"variable": ["tas", "pr", "zg"]}, ("tas", "pr")
        ... )
        >>> [d["variable"] for d in docs]
        [['tas'], ['pr']]
        """
        if not self.multi_variable:
            return [dict(doc)]
        var_field = self.native_facet("variable_id")
        raw_variables = doc.get(var_field)
        variables = (
            list(raw_variables)
            if isinstance(raw_variables, list)
            else ([raw_variables] if raw_variables else [])
        )
        if len(variables) <= 1:
            return [dict(doc)]
        if not requested_variables:
            msg = (
                f"A {self.mip_era} dataset search returned a table with "
                f"{len(variables)} variables but no variable was requested to project "
                f"onto; {self.mip_era} dataset searches must be variable-scoped."
            )
            raise ValueError(msg)
        wanted = set(requested_variables)
        return [
            {**doc, var_field: [variable]}
            for variable in variables
            if variable in wanted
        ]

    def native_facet(self, canonical: str) -> str:
        """
        Return the era-native name for a canonical facet

        Parameters
        ----------
        canonical
            A canonical (CMIP6) facet name.

        Returns
        -------
        :
            The era-native facet name, or `canonical` unchanged when the era uses the
            same name.

        Examples
        --------
        >>> CMIP5_PROFILE.native_facet("source_id")
        'model'
        >>> CMIP6_PROFILE.native_facet("source_id")
        'source_id'
        """
        return self.field_map.get(canonical, canonical)

    def canonical_facet(self, native: str) -> str:
        """
        Return the canonical name for an era-native facet (the inverse map)

        Parameters
        ----------
        native
            An era-native facet name as it appears in a raw search document.

        Returns
        -------
        :
            The canonical (CMIP6) facet name, or `native` unchanged when there is no
            mapping.

        Examples
        --------
        >>> CMIP5_PROFILE.canonical_facet("model")
        'source_id'
        """
        return self._inverse_field_map().get(native, native)

    def to_native_params(self, params: Mapping[str, str]) -> dict[str, str]:
        """
        Rename canonical facet keys in an outbound param map to their era-native names

        Non-facet keys (`format`, `type`, `offset`, …) and any facet the era shares
        pass through untouched; the `project` value is set to `project_facet`.

        Parameters
        ----------
        params
            The canonical `FacetQuery.to_params(...)` output.

        Returns
        -------
        :
            A new param map with facet keys renamed and `project` set for the era.

        Examples
        --------
        >>> out = CMIP5_PROFILE.to_native_params(
        ...     {"project": "CMIP6", "source_id": "CESM2", "type": "Dataset"}
        ... )
        >>> out["model"], out["project"], "source_id" in out
        ('CESM2', 'CMIP5', False)
        """
        renamed = {self.native_facet(key): value for key, value in params.items()}
        renamed["project"] = self.project_facet
        return renamed

    def to_canonical_doc(self, doc: Mapping[str, Any]) -> dict[str, Any]:
        """
        Rename era-native keys in a raw search document back to canonical names

        A **copy** is returned; the caller retains the original document verbatim as
        `DatasetRecord.raw`, so the era-native facets are never lost.

        Parameters
        ----------
        doc
            A raw era-native search document (values still era-native lists).

        Returns
        -------
        :
            A new document whose keys are canonical (CMIP6) names.

        Examples
        --------
        >>> canonical = CMIP5_PROFILE.to_canonical_doc(
        ...     {"model": ["CESM2"], "experiment": ["rcp45"], "id": "x"}
        ... )
        >>> canonical["source_id"], canonical["experiment_id"], canonical["id"]
        (['CESM2'], ['rcp45'], 'x')
        """
        inverse = self._inverse_field_map()
        return {inverse.get(key, key): value for key, value in doc.items()}

    def _inverse_field_map(self) -> dict[str, str]:
        """Return the native-to-canonical inverse of `field_map` (tiny; built here)."""
        return {native: canonical for canonical, native in self.field_map.items()}


CMIP6_PROFILE = EraProfile(
    mip_era="CMIP6",
    project_facet="CMIP6",
    supported_flavours=frozenset(
        {Flavour.ESGF1, Flavour.ESGF_NG_EAST, Flavour.ESGF_NG_WEST}
    ),
    field_map={},  # identity: CMIP6 is the canonical vocabulary
    parent_strategy=("header", "override"),
    reconstruct=None,
)
"""The canonical era: identity translation, current behaviour routed via the seam."""


CMIP5_PROFILE = EraProfile(
    mip_era="CMIP5",
    project_facet="CMIP5",
    # CMIP5 is served only over ESGF1/Solr; there is no CMIP5-on-ESGF-NG.
    supported_flavours=frozenset({Flavour.ESGF1}),
    field_map={
        "source_id": "model",
        "institution_id": "institute",
        "experiment_id": "experiment",
        "variant_label": "ensemble",
        "variable_id": "variable",
        "frequency": "time_frequency",
        "table_id": "cmor_table",
    },
    # CMIP5 has no activity/grid/resolution/sub-experiment concept.
    absent_facets=frozenset(
        {"activity_id", "grid_label", "nominal_resolution", "sub_experiment_id"}
    ),
    # Parent metadata in CMIP5 headers is notoriously inconsistent, so user-declared
    # overrides lead and a header read is only a best-effort fallback.
    parent_strategy=("override", "header"),
    # CMIP5's native ids are CMIP5-DRS shaped; rebuild a CMIP6-form id from the columns.
    reconstruct=_reconstruct_cmip5_ids,
    # `product` (output1/output2/…) can split two otherwise-identical CMIP5 datasets.
    distinguishing_facets=("product",),
    # CMIP5 datasets are table-grained: one dataset carries many variables.
    multi_variable=True,
)
"""CMIP5: ESGF1-only, CMIP5-native names, id reconstruction, override-led parents."""


CMIP7_PROFILE = EraProfile(
    mip_era="CMIP7",
    project_facet="CMIP7",
    # CMIP7 is served only over ESGF-NG (STAC); there is no CMIP7-on-ESGF1.
    supported_flavours=frozenset({Flavour.ESGF_NG_EAST, Flavour.ESGF_NG_WEST}),
    # No name translation: the STAC properties already use the canonical (CMIP6) facet
    # names under the collection prefix (`cmip7:source_id`), which `from_stac` strips.
    # (`field_map` only drives the ESGF1/Solr backend, which CMIP7 never uses.)
    field_map={},
    # CMIP7 drops `table_id` — variable identity is the branding suffix
    # (`variable_branding_suffix`), which `from_stac` folds into the `table_id` column —
    # and has no sub-experiment concept.
    absent_facets=frozenset({"table_id", "sub_experiment_id"}),
    # Parent metadata is carried in the STAC item properties (`cmip7:parent_*`), so it
    # is read straight from the search record with **no** file-header round-trip; the
    # `header`/`override` layers remain as fallbacks for the (deferred) parent walk.
    parent_strategy=("record", "header", "override"),
    reconstruct=None,
    multi_variable=False,
)
"""CMIP7: ESGF-NG-only, CMIP6-identical names, branding-suffix variable identity,
record-carried parents."""


ERA_PROFILES: dict[str, EraProfile] = {
    profile.mip_era: profile
    for profile in (CMIP5_PROFILE, CMIP6_PROFILE, CMIP7_PROFILE)
}
"""Registry of known era profiles, keyed by `mip_era`."""


def get_profile(mip_era: str) -> EraProfile:
    """
    Look up the `EraProfile` for a MIP era

    Parameters
    ----------
    mip_era
        The era label (`"CMIP5"`, `"CMIP6"`).

    Returns
    -------
    :
        The matching `EraProfile`.

    Raises
    ------
    KeyError
        If `mip_era` is not a known era.

    Examples
    --------
    >>> get_profile("CMIP6").project_facet
    'CMIP6'
    """
    try:
        return ERA_PROFILES[mip_era]
    except KeyError:
        known = ", ".join(sorted(ERA_PROFILES))
        msg = f"Unknown mip_era {mip_era!r}; known eras: {known}"
        raise KeyError(msg) from None


__all__ = [
    "CANONICAL_FACETS",
    "CMIP5_PROFILE",
    "CMIP6_PROFILE",
    "CMIP7_PROFILE",
    "ERA_PROFILES",
    "EraProfile",
    "get_profile",
]
