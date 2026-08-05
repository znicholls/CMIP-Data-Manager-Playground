# Multi-MIP-era search: integrating CMIP5 / CMIP6 / CMIP7

**Status:** plan only — not yet enacted.
**Author interview:** decisions below were made by the user on 2026-07-31/08-03 in a
one-decision-at-a-time design interview. Each decision records its rationale so a future
reader (or a re-anchor to CMIP7) can see *why*, not just *what*.

---

## 1. Goal

Extend the (successful, CMIP6-only) search workflow to run the **same MIP-equivalent
use cases** against **CMIP5** and **CMIP7** data, via a single high-level entry point
that speaks **one common vocabulary** and translates out to each era's ESGF facets.

## 2. The two orthogonal axes (one is already solved)

A search varies along **two independent axes**:

| Axis | What varies | Status |
|------|-------------|--------|
| **Transport dialect** (`Flavour`) | ESGF1 (esg-search / Solr) vs ESGF-NG (STAC / CQL2): request shape, paging, envelope | **Already solved** by the `SearchBackend` protocol seam (`esgf/backends/base.py`). Endpoint dialect is auto-detected from the URL (`esgf/backends/detect.py`). Both `from_solr` and `from_stac` already normalise into one CMIP6-vocab `DatasetRecord`. |
| **MIP era** (new) | CMIP5/6/7 facet *names* (`model` vs `source_id`), which fields exist, and how parent metadata is obtained | **This plan.** |

These axes are **not** the same thing: a *single* ESGF1 node (CEDA, ORNL) serves CMIP5,
CMIP6 **and** CMIP7 at once, so the era is a property of the **query**, not the endpoint.

### Live (transport × era) combinations we actually target

- ESGF1 × CMIP5 — live, testable
- ESGF1 × CMIP6 — live (current)
- ESGF-NG-east × CMIP6 — live (current)
- ESGF-NG × CMIP7 — future, **no data yet**
- (ESGF1 × CMIP7 — likely never; CMIP7 is NG-first)

Constructing a backend for an impossible combination (e.g. CMIP5 on ESGF-NG) should
raise `UnsupportedOnBackend` rather than silently misbehave.

---

## 3. Decisions (the interview outcome)

| # | Decision | Rationale |
|---|----------|-----------|
| D1 | **One common vocabulary**, translated per era (not per-era stacks). | The "high-level class, translate out" idea from `query.py`. Raw JSON is always retained, so a lossy canonical layer is safe. |
| D2 | **Canonical vocabulary is CMIP6-anchored.** Revisit if/when CMIP7 goes live. | Only era with live data; zero migration of existing schema/records; CMIP7 is nearly identical and still unpublished (spec can shift). Low-regret because raw JSON is retained → a re-anchor is a re-derivation, not a re-fetch. |
| D3 | **Era is folded into the backend instance** (backend = transport dialect + era), **not** into a separate composable axis. | User's choice. See §4 for the concrete, coherent realisation. |
| D4 | **Era is supplied by the user per search** (`mip_era`); transport dialect is still auto-detected from the endpoint URL. | The endpoint URL cannot reveal the era (shared nodes serve all eras); it must be an explicit input. |
| D5 | **One shared schema** + a `mip_era` discriminator column. Raw blob stays in the existing side table (`DatasetNodeSpecificInfo.raw_json`). Era-specific facets live in raw JSON until needed. | Retain-raw principle; least schema surface now; scans over canonical columns never pay for the blob (it's already off the hot table). |
| D6 | **Promote** an era-specific facet (when a use case must *filter* on it) into a **per-era 1:1 side table** (e.g. `Cmip7VersionExtra`), not wide nullable columns on `DatasetVersion`. | Keeps the hot table era-agnostic and lean (no cross-era NULL bloat); isolates each era's schema; scales to 100k+ datasets. |
| D7 | **Per-era parent resolver** with an ordered fallback chain: (a) parent_* on the search record → (b) file-header read → (c) user-declared / known-fixes override. Each era enables/orders the layers it needs. | Reuses the existing header + `parent_overrides` machinery; confines era differences to config; degrades gracefully under CMIP5/CMIP7 uncertainty. CMIP5 may collapse to user-supplied-only if header parent attrs prove too inconsistent. |
| D8 | **Reuse `FacetQuery` as the canonical carrier**; add an explicit `mip_era` field; derive `project` from `mip_era` (stop hardcoding `"CMIP6"`). Backend translates names in/out. | `FacetQuery` already *is* the neutral vocabulary; minimal new surface; `extra_facets` absorbs anything unmapped. |
| D9 | **Translate facet *names* only; facet *values* are era-native** (user passes `rcp45` for CMIP5, `ssp245` for CMIP6, `r1i1p1` for CMIP5 ensembles). | Never fabricate cross-era scientific equivalences (`rcp45`≠`ssp245`; stripping the CMIP5→CMIP6 forcing index would conflate `f1`/`f2`/`f3`). Safe and honest. |
| D10 | **CMIP5 first**, then CMIP7. | CMIP5 is the hardest vocabulary *and* has live data → validates the whole abstraction end-to-end. CMIP7 then becomes cheap config. |
| D11 | **Offline fixtures + opt-in live smoke.** | CI backbone is network-free (satisfies coverage/doctest/`mypy --strict`); a marker/env-gated live suite catches API drift; CMIP7 live tests are written but skipped-until-data. |

### Deliberately NOT doing

- No era-neutral rename of the existing CMIP6 schema/records (rejected in favour of D2).
- No auto cross-walk of experiment/ensemble **values** across eras (D9).
- No eager CMIP7 columns before data exists (D5/D6 — promote lazily).
- No CMIP7 end-to-end validation until data is published (D11).

---

## 4. Concrete realisation of "fold era into the backend" (D3 + D4)

Extending the `Flavour` **enum** to encode era (e.g. `ESGF1_CMIP5`) would break the
URL→`Flavour` detection in `detect.py` (a host cannot imply an era) and multiply the
enum. The coherent realisation of D3 that stays consistent with D4 ("transport
auto-detected, era user-supplied") is:

- **`Flavour` stays transport-only** and endpoint-detected, exactly as today.
- **The backend instance additionally carries an `EraProfile`** — i.e. the backend *is*
  `(transport dialect) + (era)`, which is "era folded into the backend" in the sense
  the user chose, without touching the detection axis.
- The `EraProfile` is selected from the user's `mip_era` at client-build time.

> **Flag for plan review:** if you literally meant *extending the `Flavour` enum*, say so
> and this section changes. The above is the interpretation that keeps D3 and D4
> mutually consistent.

### `EraProfile` — the era's knowledge, in one declarative object

```
EraProfile:
    mip_era: str                      # "CMIP5" | "CMIP6" | "CMIP7"
    project_facet: str                # value for the `project` param ("CMIP5", ...)
    field_map: dict[str, str]         # canonical (CMIP6) name -> era-native facet name
    variable_identity_field: str      # "table_id" (5/6) | "branding_suffix" (7)
    parent_strategy: tuple[ParentSource, ...]   # ordered fallback chain (D7)
    supported_flavours: frozenset[Flavour]      # guards impossible combos (§2)
```

Three instances: `CMIP6_PROFILE` (identity map — the current behaviour, refactored to go
through the seam), `CMIP5_PROFILE`, `CMIP7_PROFILE`. Declarative maps are unit-testable
in isolation and keep era conditionals out of the workflow body.

### Where the translation actually happens

Field-**name** translation is inserted at the two points confirmed in the code:

- **Outbound (request build):** `Esgf1Backend.page_request` calls `query.to_params(...)`,
  which currently writes canonical CMIP6 names verbatim. The era-aware backend renames
  the emitted param keys via `EraProfile.field_map` and sets `project=project_facet`
  before the request goes out (`source_id=…` → `model=…` for CMIP5). `FacetQuery` itself
  stays era-agnostic.
- **Inbound (parse):** `parse_dataset` → `DatasetRecord.from_solr(doc)` reads CMIP6 keys.
  The era-aware backend applies the **inverse** field map to a *copy* of the doc so the
  canonical columns populate, while `raw` keeps the **original** document verbatim. This
  mirrors the pattern `DatasetRecord.from_stac` already uses (read native-prefixed keys,
  store `raw=feature` unchanged).

---

## 5. Storage plan (D5 + D6)

- Add `mip_era: str` to `Dataset` (and mirror onto `DatasetVersion` if convenient for
  version-scoped queries). Indexed; every use case can filter by era.
- `DatasetNodeSpecificInfo.raw_json` continues to hold the full raw per-node document —
  **unchanged**. This is the loss-proof record of every era-specific facet.
- Canonical columns on `Dataset`/`DatasetVersion` are populated for whichever era-native
  fields map to them; fields absent in an era (CMIP5 has no `activity_id`/`grid_label`)
  are left `NULL`.
- **Promotion (only when a use case must filter on an era-specific field):** create a
  1:1 side table keyed on `DatasetVersion.instance_id`, e.g.
  `Cmip7VersionExtra(instance_id PK/FK, branding_suffix, branded_variable, region, …)`,
  and backfill it from raw JSON. `DatasetVersion` stays era-agnostic.
- **Scale check (measured):** in `uc1_full_ranked.sqlite`, `raw_json` averages **2.9 KB**
  (max 3.4 KB) per node-location row and is only **~8 %** of the DB; `fileaccess`
  dominates. Projected to 100k datasets: ~725 MB of raw JSON inside a ~8–9 GB SQLite DB —
  comfortably within SQLite's limits, and off the hot tables.

---

## 6. Parent resolution plan (D7)

A `ParentResolver` selected by era, trying an ordered chain and stopping at the first
that yields a parent link. The three sources map onto existing machinery:

| Source | Mechanism (existing) | Used by |
|--------|----------------------|---------|
| (a) parent_* on the **search record** | new: read `parent_*` from `DatasetRecord` / STAC `properties` — no header read | CMIP7 (primary, *if* STAC surfaces it) |
| (b) **file-header** read | `search/version_headers.py` (Step 3) + `search/parent_walk.py` (Step 4) | CMIP6 (primary); CMIP7 fallback; CMIP5 best-effort |
| (c) **user-declared / known-fixes** override | the `parent_overrides` mapping in `parent_walk.py` — already *"the seam for user corrections / a future fixes library"* | CMIP5 (primary/likely); override for any era |

Per-era chains:

- **CMIP6:** `(header, override)` — unchanged behaviour.
- **CMIP7:** `(record, header, override)` — robust whether or not STAC surfaces
  `parent_*`; new field `parent_mip_era` recorded.
- **CMIP5:** `(override, header?)` — user-declared primary; optional best-effort header
  attempt. CMIP5 header parent attrs are `parent_experiment_id` / `parent_experiment_rip`
  (the ensemble, → `parent_variant_label`) / `branch_time`, with **no** `parent_source_id`
  (defaults to the child's model). **Verify-item:** if these prove too inconsistent
  across CMIP5, drop (b) and make CMIP5 user-supplied-only.

---

## 7. Concrete field maps

### CMIP5 (`project=CMIP5`, ESGF1/Solr only) — canonical → era-native

| Canonical (CMIP6) | CMIP5 facet |
|---|---|
| `source_id` | `model` |
| `experiment_id` | `experiment` |
| `variant_label` | `ensemble` (`r1i1p1`, no forcing index) |
| `variable_id` | `variable` |
| `frequency` | `time_frequency` |
| `institution_id` | `institute` |
| `table_id` | `cmor_table` |
| `activity_id`, `grid_label`, `nominal_resolution`, `sub_experiment_id` | *(absent)* |

### CMIP7 (`project=CMIP7`, ESGF-NG/STAC-first) — canonical → era-native

Identical to CMIP6 for `source_id`, `experiment_id`, `variant_label`, `variable_id`,
`activity_id`, `grid_label`, `frequency`. Divergences:

- `table_id` → **eliminated**; variable identity is `branding_suffix` (+ `branded_variable`).
- New fields: `temporal_label`, `vertical_label`, `horizontal_label`, `area_label`,
  `region`, `license_id`, `data_specs_version` (`MIP-DS7.1.0.0`), `parent_mip_era`.
- STAC facets are collection-prefixed (`cmip7:…`); the `from_stac` prefix-derivation
  already handles this generically.

> All CMIP7 specifics are **unpublished / spec-derived** — treat as provisional and
> verify against the first real data (§9).

---

## 8. Phasing (D10)

**Phase 0 — scaffolding (CMIP6 stays the only wired era; main stays green):**
1. `EraProfile` + `CMIP6_PROFILE` (identity), the field-map rename seam in the ESGF1
   backend (in + out), `mip_era` on `FacetQuery`, `project` derived from era.
2. `mip_era` column + repository plumbing; `ParentResolver` seam wrapping the current
   header/override path as `CMIP6_PROFILE`'s chain.
3. Refactor is behaviour-preserving for CMIP6: existing tests + a UC1/UC2 live re-run
   must match today's results.

**Phase 1 — CMIP5 (validate the abstraction on live data):**
4. `CMIP5_PROFILE` (field map §7, `project=CMIP5`, parent chain `(override, header?)`).
5. Offline fixtures from real CMIP5 Solr docs; wire a CMIP5 script mirroring a UC1/UC2
   use case (RCP-equivalent of an SSP case).
6. Live smoke run; decide the CMIP5 parent verify-item (§6); write known-fixes entries as
   needed.

**Phase 2 — CMIP7 (cheap config, no live validation):**
7. `CMIP7_PROFILE`; `record`-first parent strategy reading `parent_*` from STAC
   properties with header fallback; synthetic STAC fixtures from the spec.
8. Live CMIP7 tests written but **skipped-until-data**; revisit D2 (re-anchor?) once data
   lands.

---

## 9. Open questions / verify-items

- **CMIP7 STAC `parent_*` in `properties`?** Determines whether CMIP7 skips header reads.
  Unverifiable until data exists → the `(record, header, …)` chain is robust either way.
- **CMIP5 header parent-attr reliability** → keep or drop parent source (b) for CMIP5 (§6).
- **CMIP7 spec churn** → all §7 CMIP7 names provisional; re-anchor decision (D2) deferred.
- **DRS identity across eras** — `DatasetRecord`'s `master_id`/`instance_id`/`version`
  splitting keys off a *trailing* `.vYYYYMMDD` and parses facets from doc *fields*, not
  DRS positions, so it *should* survive CMIP5/7 structural differences. Confirm on real
  CMIP5 ids during Phase 1.
- **Impossible (transport × era) combos** guarded via `EraProfile.supported_flavours`.

## 10. Testing (D11)

- **CI (network-free):** offline fixtures from real CMIP5/6 sample docs and CMIP7
  spec-derived STAC; unit tests for each `EraProfile` field map (in/out round-trip) and
  each `ParentResolver` chain; keep coverage ≥ 90 %, `mypy --strict`, `ruff`, and
  `--doctest-modules` (no network in docstrings).
- **Live smoke (opt-in, marker/env-gated, not in CI):** hit real CMIP5/CMIP6 endpoints
  with loose assertions (parses without error; `1 < n < N`); catches API drift.
- **CMIP7 live:** written, `skip`-marked until data is published.
