# Detailed workflow & call graph (CMIP6)

Status: **living document** — the *zoomed-in* companion to
[`search-workflow.md`](./search-workflow.md) (the ER model + the step-level flow)
and [`repository-layering.md`](./repository-layering.md) (who-may-call-whom by
layer). This file goes **down to the function names**: which concrete function
calls which, where the **injected seams** sit, where **parallelism** happens, and
which **tables** each write touches. It is deliberately busier than the other two —
the goal is to keep building a mental image, not to stay legible on one screen.

All Mermaid below is plain text (diff-able); GitHub renders it, or paste into
<https://mermaid.live>. Keep this file updated in the same commit as any change to
the call graph, seams, or parallelism.

> **Backend seam (ESGF1 / ESGF-NG).** The call graph below is drawn for ESGF1
> (esg-search / Solr). A fifth injected seam now sits under `ESGFSearchClient`: a
> **`SearchBackend`** (`esgf.backends`) resolved from the endpoint URL
> (`backends.detect`), so the same graph serves ESGF-NG (STAC / CQL2) with two
> substitutions — **Step 2** `files.add_files` becomes `files_ng.add_files_auto`,
> which for STAC records runs `add_files_from_assets` (an in-process transform of the
> item's `assets`, **no `search_files`, no `IndexNodeHealth`**) then the *unchanged*
> `store_files`; and every `client.search`/`count` renders to CQL2 instead of Solr
> params. Steps 3–4 (`enrich_version_headers`, `resolve_parent_chains`) are unchanged.
> See [`search-workflow.md`](./search-workflow.md#backends-esgf1-vs-esgf-ng) and
> [`esgf-ng-backend-adapter.md`](./esgf-ng-backend-adapter.md).

Legend for every diagram:

- **rounded box** = a function/callable you can grep for.
- **hexagon** `{{…}}` = an **injected seam** (dependency injection): the caller is
  handed *some* implementation, so behaviour (serial vs parallel, real vs fake) is
  swappable without touching the step. The four seams are `Fetch`, `MapFn`,
  `HeaderReader`, and `Repository`.
- **cylinder** `[( … )]` = a SQLite table (only `Repository` ever writes it).
- Edge labels say *what is passed* or *why the call happens*.

---

## 1. End-to-end call graph (all four steps)

The one picture: a `scripts/` entry point wires config, then drives the four
`search/` step functions in order. Each step reaches **left** to `esgf/` for the
network/read machinery and **down** to `Repository` for persistence. Nothing but
`Repository` touches a table.

```mermaid
flowchart TB
    %% ---------- entry ----------
    subgraph ENTRY["scripts/ — per-use-case wiring (UC-simple: Step 1 only; UC-chain: 1-4)"]
        SCRIPT["enrich_uc1_headers · enrich_uc2_headers<br/>enrich_g6solar_headers · esgf_search"]
        FAC["factory.build_client(Settings)<br/>config.Settings: base_url · project · page_size · timeout"]
        SCRIPT --> FAC
    end

    %% ---------- injected seams ----------
    FETCH{{"Fetch<br/>httpx_fetch / no_retry / exponential_backoff"}}
    MAP{{"MapFn<br/>serial_map (default) · thread_pool_map · process_pool_map"}}
    HR{{"HeaderReader<br/>read_header wrapped by with_timeout + with_retry"}}
    REPO{{"Repository<br/>(only layer that opens a DB session)"}}

    FAC -->|injects| FETCH
    SCRIPT -->|chooses| MAP

    %% ================= STEP 1 =================
    subgraph ST1["Step 1 — search index node (SEARCH API)"]
        R1["runner.fetch_records + _dedupe<br/>build FacetQuery per (var, experiment)"]
        C1["client.ESGFSearchClient.search / search_many<br/>guards: DeepPaginationError > 10k"]
        RR["Repository.record_run<br/>group master_id -> version -> node (ALL versions); diff vs prev spec"]
        R1 -->|"FacetQuery[]"| C1 -->|"DatasetRecord[]"| RR
    end

    %% ================= STEP 2 =================
    subgraph ST2["Step 2 — add files (SEARCH API, preference-ordered endpoints)"]
        F0{"Repository.version_has_files?<br/>(cache gate)"}
        F1["files.add_files<br/>ONE search_files per version; n_workers<br/>backoff(5) -> requeue -> next endpoint; worker never raises"]
        C2["client.search_files<br/>per endpoint: CEDA -> ORNL -> metagrid-west"]
        SF["Repository.store_files (save-as-you-go, lock-serialised)<br/>File (node-indep) + FileAccess (per-node, https twin)"]
        IH["Repository.save_index_health · record_file_access_attempts<br/>IndexNodeHealthStat aggregate + FileAccessAttempt per search call"]
        FX["all endpoints+requeues exhausted -><br/>FileSearchIncompleteError (after successes persisted)"]
        F0 -- "no" --> F1 -->|"FileRecord[]"| C2 --> SF
        F1 --> IH
        F1 -- "unresolved" --> FX
        F0 -- "yes" --> SKIP2["skip"]
    end

    %% ================= STEP 3 =================
    subgraph ST3["Step 3 — header-only metadata (DATA NODE byte-range)"]
        H0{"Repository.simulation_header?<br/>(sim-grain reuse gate: any variable / earlier run)"}
        H1["version_headers.enrich_version_headers<br/>_plan_headers: one file per simulation"]
        DISP["dispatch.dispatch_reads<br/>(see Diagram 2)"]
        PH["Repository.promote_header<br/>header_attrs_json on File; parent_* onto DatasetVersion"]
        NH["Repository.load_node_health / save_node_health<br/>record_header_attempts"]
        H0 -- "no" --> H1 --> DISP --> PH
        DISP --> NH
        H0 -- "yes" --> REUSE["copy stored header onto new versions<br/>(_reuse_sibling / _reuse_simulation; no read)"]
    end

    %% ================= STEP 4 =================
    subgraph ST4["Step 4 — parent walk (UC-chain ONLY, loops; see Diagram 3)"]
        W1["parent_walk.resolve_parent_chains<br/>declared_parent -> one search per parent -> link"]
    end

    %% step ordering
    FETCH --> C1
    FETCH --> C2
    MAP -->|"map over queries"| C1
    MAP -->|"map over versions"| F1
    MAP -->|"map over parents"| W1
    HR --> DISP

    RR -->|"UC-simple: DONE (Step 1 only — no files/header)"| DONE["Dataset + versions (catalog)"]
    RR -->|"UC-chain: files/header serve lineage"| VSEL["search.versions.select_target_versions<br/>narrow to target version per dataset (default: latest by version DATE)"]
    VSEL --> ST2
    SF --> ST3
    PH -->|"UC-chain"| ST4
    NH -.-> ST4
    W1 -->|"re-enter for each intermediate parent"| ST2
    W1 --> AGG{"any broken chain?"}
    AGG -- "no" --> DONE2["full linked chains"]
    AGG -- "yes" --> RAISE["ParentResolutionError (aggregated)"]

    %% ---------- tables ----------
    subgraph TABLES["db/schema.py — SQLite (Repository-only writes)"]
        T1[("SearchRun · RunMembership · DatasetChange")]
        T2[("Dataset · DatasetVersion · DatasetNodeSpecificInfo")]
        T3[("File · FileAccess")]
        T4[("DataNodeHealthStat · HeaderReadAttempt<br/>IndexNodeHealthStat · FileAccessAttempt")]
    end
    RR --> T1
    RR --> T2
    SF --> T3
    IH --> T4
    PH --> T3
    PH --> T2
    W1 --> T2
    NH --> T4
```

**Reading it:** every step is the same shape — *cache gate → esgf call (through a
seam) → `Repository` write → table*. Health is recorded wherever we hit a flaky
server: **data-node** health in Step 3 (`DataNodeHealthStat`, header reads — the only step
that talks to data nodes) and **index-node** health in Step 2 (`IndexNodeHealthStat`,
the file searches, with its own backoff → requeue → endpoint fallback).

---

## 2. Step 3 internals — the header-read dispatcher (where the *real* parallelism is)

Steps 1/2/4 parallelise trivially (HTTP search over threads). Step 3 is the
interesting one: netCDF is **not thread-safe** and a stalled read must be **killed**,
so each read runs in a **subprocess** under a wall-clock timeout, across a shared
worker budget *and* a per-host concurrency cap, with health-aware routing and retry.

```mermaid
flowchart TB
    IN["enrich_version_headers hands dispatch_reads:<br/>the simulations to read + routing controls"]

    subgraph ROUTE["routing.py — decide WHERE to read"]
        BC["build_candidates / simulation_candidates<br/>rank each sim's FileAccess mirrors"]
        AFF["source_id_affinity<br/>keep a model's reads on one learned-good host"]
        HL["order by NodeHealth: reliability, then speed<br/>preferred_hosts first, ignore_hosts dropped"]
        BC --> AFF --> HL
    end

    subgraph POOL["dispatch_reads — the two-level throttle"]
        BUDGET["shared worker budget<br/>DEFAULT_MAX_WORKERS = 12 (reads in flight ANYWHERE)"]
        CAP["per-host cap: concurrency_limit<br/>DEFAULT_NODE_CONCURRENCY = 2, AIMD up to CEILING = 8"]
        BUDGET --> CAP
    end

    subgraph ONE["per read (in a worker)"]
        WT["with_timeout: run read_header in a SUBPROCESS<br/>kill on stall (DEFAULT_READ_TIMEOUT = 90s)"]
        RH["read_header — byte-range GET of the netCDF header only<br/>_collapse_spaced_chars fixes char-array quirk"]
        CLS{"is_block_signal? (429/403/rate-limit)<br/>vs ordinary transient"}
        RETRY["with_retry: exponential_backoff<br/>(single reset = transient; burst = controller signal)"]
        WT --> RH --> CLS
        CLS -- "transient" --> RETRY --> WT
        CLS -- "block" --> EVICT["evict host (rate-based)<br/>reroute sim to next candidate"]
    end

    subgraph LEARN["health + audit (persisted)"]
        REC["NodeHealth.recording(host, ok?, latency)"]
        STAT[("DataNodeHealthStat — per-host aggregates")]
        ATT[("HeaderReadAttempt — append-only per-attempt log")]
        REC --> STAT
    end

    OUT["DispatchResult -> promote_header<br/>parent_* onto EVERY DatasetVersion of the sim"]

    IN --> ROUTE --> POOL --> ONE
    EVICT --> HL
    CLS -- "success" --> OUT
    ONE --> REC
    ONE -->|"every attempt incl. retries + total failures"| ATT
```

Key facts to remember from this diagram:

- **Two independent limits.** `DEFAULT_MAX_WORKERS` caps reads *everywhere*;
  `concurrency_limit` caps reads to *one host*. A host earns more parallelism
  (AIMD, up to the ceiling) only by succeeding, and loses it on block bursts.
- **Process, not thread.** `with_timeout` uses a subprocess purely so a hung
  `read_header` can be `_terminate`-d — `signal.alarm` and `.ncrc` tricks don't
  work here (that lesson is baked in).
- **Health persists across runs:** `load_node_health` at the start, `recording`
  per read, `save_node_health` at the end — so run *N+1* starts already knowing
  which mirrors are fast/dead.

---

## 3. Step 4 internals — the parent walk loop (function level)

The design doc has the *decision* flow; this is the *call* flow. Note it **re-uses**
Steps 2 and 3 for every hop — a parent is just another dataset that needs files and
a header read (to find *its* parent), until a terminal.

```mermaid
flowchart TB
    START["resolve_parent_chains(children, stopping_experiment, parent_overrides)"]

    GATE{"stopping_experiment given?"}
    EXIST["parent_experiment_exists<br/>one broad count() on the index"]
    RAISEG["raise ParentExperimentMissingError"]

    DECL["declared_parent(DatasetVersion.parent_*)<br/>apply parent_overrides if present<br/>source_id assumed same; institution_id NOT"]
    DEDUP["dedupe: many children -> one parent<br/>(DB + visited set: search each parent once)"]
    FIND["find_parent_datasets<br/>ONE search per parent via MapFn (VARIABLE-SCOPED: OR over requested vars)"]
    OUT{"search outcome"}
    PNF["ParentNotFound (terminal)<br/>declared (S,E,V) but zero index results"]

    LINK["Repository.set_parent_version<br/>child.parent_version_key -> parent version (same variable)<br/>parent lacks the requested variable -> VariableGap (informational)"]
    STOP{"parent.experiment_id == stopping_experiment?"}
    ENDCHAIN["end-of-chain parent: Step 1 only (stored by the search)<br/>NO files (only header substrate), NO header, NO further hop"]

    NEEDHDR["header cache miss -> collapse to ONE representative dataset per parent sim<br/>Step 2 (add_files, that one file) + Step 3 to read THIS parent's header;<br/>cache hit (any variable/earlier run) -> copy, skip files+read"]
    TOP{"header declares 'no parent' sentinel?"}
    NOTANC["ParentNotAncestor (terminal)<br/>hit top without the specified parent"]
    HOPGUARD{"hops < _MAX_PARENT_HOPS (5)?"}

    START --> GATE
    GATE -- "yes" --> EXIST
    EXIST -- "missing" --> RAISEG
    EXIST -- "exists" --> DECL
    GATE -- "no (None)" --> DECL
    DECL --> DEDUP --> FIND --> OUT
    OUT -- "zero" --> PNF
    OUT -- "found" --> LINK --> STOP
    STOP -- "yes" --> ENDCHAIN
    STOP -- "no" --> NEEDHDR --> TOP
    TOP -- "yes & parent specified" --> NOTANC
    TOP -- "yes & parent None" --> ENDCHAIN
    TOP -- "no" --> HOPGUARD
    HOPGUARD -- "yes" --> DECL
    HOPGUARD -- "no" --> NOTANC

    ENDCHAIN --> COLLECT["collect terminals + links"]
    PNF --> COLLECT
    NOTANC --> COLLECT
    COLLECT --> FINAL{"any terminal was an error?"}
    FINAL -- "no" --> OKDONE["ParentWalkResult: links + terminals + hops + variable_gaps"]
    FINAL -- "yes" --> AGGERR["raise ParentResolutionError<br/>(lists EVERY broken chain, after all attempted)"]
```

The **`parent_walk.py` variable question** is now settled as **fully variable-scoped**
(this reverses an earlier "variable-agnostic existence" design; the trade-off was taken
deliberately):

- **Existence / lineage** (`FIND`) is **variable-scoped**: it searches
  `(source_id, experiment_id, variant_label)` constrained to the requested variables (an
  OR). *Consequence:* a parent that publishes **none** of them comes back empty and so
  is a `ParentNotFound` — the walk cannot tell that apart from a parent that does not
  exist at all. (With a multi-variable request the OR is forgiving: the parent resolves
  as long as it publishes *at least one* requested variable.)
- **Files / header** (`NEEDHDR`) is **collapsed** to a single representative dataset per
  parent simulation (`_read_representative`) — a header describes the *run*, so any one
  file carries the same `parent_*` attributes and is enough to climb. This gate is what
  avoids the >50k file-access blow-up, and it is skipped entirely when the simulation's
  header is already stored (any variable, including an earlier run — `simulation_header`).

Each requested variable's child→parent version link is still made when the parent
publishes it; a requested variable the parent does *not* publish (while publishing
others) is reported as a `VariableGap` (the lineage resolved, that one variable's
lineage just has a hole at that ancestor) rather than failing the walk. The
**Gregory / uc2** multi-variable case therefore needs no special casing.

---

## 4. Dependency-injection map — what gets handed to what

The reason the diagrams above are swappable (real network in production, fakes in
tests) is that every step takes its collaborators as **parameters with defaults**,
not hard-coded imports. This is the whole seam story on one page.

```mermaid
flowchart LR
    SET["config.Settings"] --> FACT["factory.build_client"]
    FACT --> CLIENT["ESGFSearchClient<br/>(holds a Fetch)"]

    FETCH{{"Fetch = httpx_fetch<br/>(swap: recording double / no_retry)"}}
    MAP{{"MapFn = serial_map (default)<br/>(swap: thread_pool_map / process_pool_map / spy)"}}
    HR{{"HeaderReader = read_header + with_timeout/with_retry<br/>(swap: dict lookup in tests)"}}
    REPO{{"Repository<br/>(swap: in-memory SQLite for tests)"}}

    FETCH --> CLIENT

    CLIENT --> S1["Step 1: runner + record_run"]
    CLIENT --> S2["Step 2: add_files"]
    CLIENT --> S4["Step 4: resolve_parent_chains"]
    HR --> S3["Step 3: enrich_version_headers -> dispatch_reads"]

    MAP --> S1
    MAP --> S2
    MAP --> S4

    REPO --> S1
    REPO --> S2
    REPO --> S3
    REPO --> S4

    NOTE["Parallelism is a CHOICE the script makes:<br/>pass serial_map to debug one-at-a-time,<br/>thread_pool_map to go wide — same step code."]
    MAP -.-> NOTE
```

**Takeaway:** to change *how parallel* the whole pipeline runs, a script swaps one
argument (`MapFn`). To make any step testable, you pass a fake `Fetch` /
`HeaderReader` / `Repository` — the step function never knows the difference. That is
what lets the per-step tests double as living documentation (see the "Testing &
observability" section of `search-workflow.md`).

---

## How this doc relates to the other two

| Doc | Altitude | Answers |
|---|---|---|
| `repository-layering.md` | zoomed **out** | which *layer* may call which |
| `search-workflow.md` | mid | the ER model + the 4-step *decision* flow + per-step call map |
| **this doc** | zoomed **in** | *function-level* call graph, the read dispatcher, the walk loop, and the DI seams |


1. End-to-end call graph (all 4 steps) — every step shown as the same shape: cache gate → esgf call through a seam → Repository write → table, with the concrete function names (runner.fetch_records, client.search_many, files.add_files, store_files, enrich_version_headers, promote_header, resolve_parent_chains, etc.) and which of the 4 table groups each write hits.
2. Step-3 read dispatcher internals — the real parallelism: routing/affinity/health ordering → the two-level throttle (DEFAULT_MAX_WORKERS=12 shared budget vs per-host concurrency_limit AIMD) → per-read subprocess timeout + block/transient classification + retry → persisted DataNodeHealthStat / HeaderReadAttempt.
3. Step-4 parent-walk loop at function level — resolve_parent_chains → declared_parent → existence gate → find_parent_datasets (one search per parent) → set_parent_version, re-using Steps 2/3 each hop, with all three terminals and the aggregated error.
4. Dependency-injection seam map — how Settings/factory build the client and how the four seams (Fetch, MapFn, HeaderReader, Repository) are handed to each step, making "how parallel" a one-argument choice.
