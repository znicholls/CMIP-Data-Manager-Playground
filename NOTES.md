## 2026-07-09

### Initial notes

Let's try and get search going.
Let's do a couple of use cases we already know.

1. search for all tas monthly data for ssp245
1. search for all model-variants that have all the data required to do a gregory calculation
1. search for all model-variants that have fgco2, nbp and optionally co2s (and if no co2s, then co2, again optional) for historical and ssps

Slightly trickier pieces, particularly for the latter two cases:

1. do you start with one variable, then check the others, or just grab all of them and then figure out the combinations after?
    - assumption: you can't require all when searching ESGF (all or nothing is a thing we have to do)
    - is the cost of grabbing everything so high that we should avoid it and instead search for one variable,
      then for the next variable based on the model-variants we have, etc. until we've checked all variables.
      I think the tradeoff here will be that getting all results means we have results that we end up not using
      (because they're missing some other variable we need).
      Searching 'iteratively' means we have to ping off a lot of requests so we're going to need good parallelisation
      and backoff, which might be more trouble than it's worth.
1. grabbing parent data
    - we need to think about how we layer this appropriately to allow auto-search for parent data too
    - how do we define how 'far up the tree' to go
    - how do we inject known fixes along the way when searching for parent data

CMIP7 side quest: grab the URLs I gave you, and use them plus AI to design a similar search system
based on the API docs (you obviously can't test it because there's no data, but maybe the docs already tell you a lot).
Think about whether we can drop all the CMIP6 and CMIP7 data into the same database.
Pro: as a user, you can just grab data, doesn't matter which MIP era
Con: CMIP6 and CMIP7 (and definitely CMIP5) use slightly different names/facets (in ESGF language) for the same thing,
so we'll have to introduce a translation layer, which may complicate things significantly.

CMIP5: if you're happy and want to try something much harder, that will add a lot of complexity,
try and do the same thing for CMIP5 (just replace SSPs with RCPs)

### Initial prompt

There's a search API for climate model data distributed via the Earth System Grid Federation, known as ESGF.
We want to use the URL at https://metagrid.esgf-west.org/search for search.
As context, there are other URLs that should provide a similar, it not the same, search API.
Please don't hard-code this URL so we can swap in others if needed
(e.g. the esgf-west URL is down for maintenance),
but don't worry about trying other URLs right now.

Using this search API, I want to search for pieces of data.
Please help me write a script to perform searches for the following use cases.

1. all results for variable_id equal to "tas" and frequency equal to "monthly" (or "mon" might be the right value) and experiment_id equal to "ssp245"
1. all source_id - variant_label pairs (these are 'model-variant' pairs) that have all of the variables "tas", "rsdt", "rlut" and "rsut" and frequency equal to "monthly" (or "mon" might be the right value) and all of the experiment_id's "abrupt-4xCO2" and "piControl". Also include "abrupt-2xCO2" and "abrupt-0p5xCO2", but don't make those required.
1. all source_id - variant_label pairs (these are 'model-variant' pairs) that have all of the variables "tas", fgco2", "nbp" and optionally "co2s" (or if "co2s" isn't there, "co2") and frequency equal to "monthly" (or "mon" might be the right value) and any of the experiment_id's "historical" or an experiment_id that starst with "ssp".
1. all source_id - variant_label pairs (these are 'model-variant' pairs) that have all of the variables "tas" and frequency equal to "monthly" (or "mon" might be the right value) and any of the experiment_id's "esm-hist" or an experiment_id that starst with "esm-ssp".

Please put all 'core'/'supporting'/'reusable' code in `src` so that the code in `scripts` stays as small and clear as possible.
For querying the ESGF API, if we need to do multiple queries,
we will very likely need to support creating parallel or asynchronous queries
and specifying backoff/retry approaches.
Please make the specification of these approaches as flexible as possible using e.g. dependency injection,
as they may need to be quite user specific.

Save the results in a local database.
Use SQLite for this.
Let the user specify the file in which to save the SQLite database.
Please consider the possibility of switching out and using e.g. a local postgres server for the database,
but don't implement this yet.

Use pydantic and/or SQLModel for defining the database schemas.

Please also add support for just using the existing database entries for the search
and functionality to support updating the existing database entries by re-querying the ESGF API
and tracking the difference from the last time that the ESGF API was queried.

Some questions:

1. Can you figure out what the API's rules are for doing searches with AND/OR boolean logic and what it supports (in theory and in practice)? For example, can we search for tas from ssp245 and fgco2 from historical, or would this need to be split into multiple searches?
1. What are the limits on the number of search results? How can we handle cases where there are more results than the API provides in a single response. Do we need to support pagination? How can we best do this?
1. Should we have one class for the database model and another for the Python API, or can these just be the same? If you want a concrete example, do you think the approach used here makes sense or not? https://github.com/climate-resource/CMIP7-GHG-Concentration-Manuscripts/blob/main/src/local/esgf/models/esgf_dataset.py
1. How should we best track the changes between successive queries to the ESGF API? Do we just use a table in the database for this or should we e.g. set up our own local kafka queue?

### Prompt things to check

Does AI pick up that piControl and abrupt-4xCO2 don't have to have the same variant label, it should look at the parent metadata instead.

To handle this:

1. get AI to help you write a script that summarises the model-variants that had all the data for abrupt-4xCO2 but not all the data for piControl
1. pick an example where this should work, but it isn't because we're not using the parent information
1. use this example to help drive updates to the code
1. check that, with the updates, our script is able to correctly identify that this model-variant does have everything we need for a Gregory calculation
1. commit, push etc.

### Other use cases

1. Whatever search you need for your pattern effect analysis
1. ERF diagnosis. For this, we need `rsut,rlut,rsdt` for `piClim-control` and then any one of the following (ideally all): `piClim-4xCO2`, `piClim-CH4`, `piClim-N2O`, `piClim-histaer`, `piClim-histghg`, `piClim-histnat`. Please make `hist-nat`, `hist-GHG` and `hist-aer` (and their parents) optional
    - please also write a helper to print out a markdown table that has a row for each model-variant, then each experiment is a column (including the optional experiments) and an "X" in each cell for which the experiment is on ESGF for that model-variant. Sort from model-variants with the most to the least "X"'s in their row.

Updating logic to handle differences in data nodes not creating different datasets. The dataset is the same, it's just available from a different place.
Handle this during download support.


### Anna's first day

- Remove 'mon' hardcode -> frequency i
s user specific
- Test cases remove from src into scripts (scripts don't have type(?), intentional)
- Test cases will be user specific. I will want to test more/less cases
- parent/child variant_id differences : read NetCDF file header
- will need to read through multiple chains (ie from ssp to hist to piControl)

##### UC2 variant_id different between parent/child
- Will need to add flag - > opt in explicitly about parent metadata

HadGEM3-GC31-LL is the textbook case: abrupt-4xCO2 is r1i1p1f3, its parent piControl is r1i1p1f1. A Gregory calculation is perfectly valid here, but UC2 excludes it. This is exactly the concern flagged in NOTES.md:82 ("piControl and abrupt-4xCO2 don't have to have the same variant label, it should look at the parent metadata instead").

Important catch: the fix isn't trivial, because the ESGF dataset search records don't carry parent metadata. I checked — the raw Solr doc for these datasets has no parent_* keys at all:

keys mentioning parent: []

parent_variant_label / parent_experiment_id live in the netCDF file global attributes (or the CMIP6 errata/CV), not in the Solr facets stored in DatasetRecord. So factoring in the parent means bringing in a second data source (file global attributes or a known-fixes table) — which is the "inject known fixes / grab parent data" work sketched in NOTES.md:

### Anna's Second day
- Working on parent/child
- Threader doesn't work with netcdf? Seems slow to investigate headers
- Only investigate parents if experiment can't find parent which matches variant_id
- Testing all headers across all files is the massive bottlenck - now just testing first file header for each variable
    - Will need to come back to this for mislabeled data.
    - Eg EC-Earth3 1300 file reads down to 4 (for four variables)
    For full live run could take 30-60 minutes
    implement cross-cell parallelism (not sure what this is)
Still open (unchanged, all deferred)

- Per-read hard timeout — netcdf ignores the rc timeout for byte-range, so a stalled https mirror can still cost ~100s; the ~7 min is mostly those stalls + serial file searches. A real cap needs killing stuck worker processes.
- Phase 3 (DB persistence) — cache the links so it's one-time and offline runs are parent-aware. Highest-leverage next step now that resolution is fast-ish but not free.
- preferred_hosts (your future arg) and parallelising the per-cell file searches.

Your esgf_cache.sqlite was never touched — all live work went to a scratch DB. Want me to do Phase 3 next, or stop here?

- Another ssp119 chain assessment below
- For next week this could actually be useful because it will be important for download...
We've hit a different bottleneck now — and it's the real answer. Let me check process state before killing.

  Ran 1 shell command

The process has been stuck for 1h11m — and the cause is now clear from the curlcode: (28) lines: individual netCDF byte-range reads are blocking for 15–25 minutes each (899525 ms, 1545493 ms) on data nodes that accept the connection but never send bytes. netCDF4/libcurl has effectively no read timeout, so one stuck mirror pins a worker indefinitely. Let me kill it and assess.

### Third day (13/7)
 We have done test-case 1, searching for all data from ssp245 for the tas variable. You already have local search results in our SQLite database (the `esgf_cache.sqlite` file). Our next problem is this: for each file, we want to get information about its parent file. That information isn't in the search results, instead we have to get it from the actual files that sit underneath each dataset.

  What solution would you suggest for adding this 'file-only metadata' information to each dataset search record?

  Some context which will help.

  To get the file-only metadata, you have to open the file. Each dataset search record should provide information about how to access the files that sit underneath it. We will need to somehow represent and engage with that information. We believe it's possible to just get the metadata (aka the file header), without having to download the full file, please check this and see how well supported it is. There can be lots of files that belong to a given dataset. We suspect that reading all the files' metadata will be too slow, so we'll have to just read the first file's metadata and assume that it's the same across all files, please tell us if you thiompromise.
                                                                                                                                                                                                    Given how slow accessing metadata is likely to be, we suspect you'll need to paralstinct would be to use thread pools, but maybe we have to use process pools. Youcan tell us.
  Then the other piece of information is that different data nodes have different access speeds, reliability etc. We want to gather information on data node health/statistics during this process. We want to track things like response time, download speed, number of retries requuccess was recorded for this node (some nodes are just dead so will neversuceed). When trying to access file metadata information, the options seem to be as follows: 1. The data node connects, we access the metadata, and this is a success. 2. The data node connects, but 'stalls' and can stay connected for up to 25 minutes, this is not a success. Wmum wait time and ideally some way to measure whether that wait time is sensibleor not (e.g. waiting 5 seconds is too short, but 5 hours is too long, how do we determine the right value between these two)? 3. The data node does not connect, we retry the connection x times
  and the process is eventually successful, this is a success. Again, how many timesbefore we say it is a failure and how can we set this maximum retry number sensibly? 4. The data node does not connect, we try multiple times, it does not connect. This is a failure.


Answers to some questions:
1. How process isolation + kill-on-overrun fits parallelisation

The unit of work becomes: one header read = (spawn a child process, wait on it with a hard deadline, reap it — or kill it if it overruns). Parallelism is then a thread pool over those units:

- Threads are the right fan-out here precisely because the heavy work isn't in the thread. Each thread just starts a child, blocks on wait([pipe], timeout), and collects the result. The netCDF + libcurl work runs in the child process, so the GIL isn't held during the wait, and netCDF's thread-unsafety is moot — it never runs in the threads. This is what lets us drop the old process-pool-for-thread-safety requirement.
- Per-read kill is the whole point. Because each read owns its own process, we can terminate exactly the stalled one and leave the other 7 running. A shared ProcessPoolExecutor can't do this — you can't cancel a task already executing in a worker; one 25-minute stall poisons that worker and you'd have to tear the pool down.
- Concurrency = pool size = max simultaneous child processes (say 8). Each child costs ~0.5–1 s to spawn + import netCDF, amortised against the read. One caveat worth flagging: against a fast node like NCI (~1.2 s reads), that spawn overhead is proportionally large (~50%). So for an all-NCI run you'd either widen the pool or, later, move to a small pool of persistent worker processes with a supervisor that kills+replaces any worker exceeding the deadline — same gua'd start with spawn-per-read (simple, correct) and keep the persistent-workervariant as a known optimisation.
- Failures (HeaderReadTimeout, HeaderReadCrashed) are OSErrors, so the existing mirror-fallback catches them and tries the next mirror. Order is preserved by the thread map.
2. Preferred node — and it's cheap to check
                                                                                                                                                                                                 API: a preferred_hosts: tuple[str, ...] parameter threaded into the candidate-orde preferred → healthy-HTTPS → HTTPS → rest, minus ignore_hosts. You set("esgf.nci.org.au",); someone in Europe sets DKRZ/CEDA. NCI gets tried first whenever it's among a file's mirrors.
                                                                                                                                                                                                 Is checking "does NCI have this file" expensive? No — measured ~0.2 s, and it batc
- If the file search already returned an NCI mirror → free, it's just a list check.
- If not (the origin-pinned search didn't surface it) → query NCI's own index by instance_id: 0.20 s, and I can OR many instance_ids into one 0.20 s query. Negligible next to 1–20 s reads.
But one real finding: NCI coverage is partial. Of the 5 cached UC1 datasets I probed (EC-Earth3-CC, ACCESS-ESM1-5, CNRM-CM6-1, CanESM5), 0 were on NCI — even the Australian CSIRO ACCESS-ESM1-5 replica came from ORNL/ALCF — yet TaiESM1 was on NCI. So preferred-node must fall s the file (the ordering does this naturally: NCI-first if present, otherwise nextmirror). My recommendation: don't eagerly probe every file; use mirrors already in the search results, and optionally run one batched NCI probe per run to enrich mirror lists. Cheap and optional.
3. Storage — do both, keyed per (dataset, variable)
                                                                                                                                                                                                 Your instinct to worry about differing columns and cost is right, and the resoluti matters more than the format:

- Store per (dataset_id, variable_id), not per file. All time-chunks of a variable share the same global attributes, and you already read one header per variable — so one row per variable. For UC1 (one variable) that's one row per dataset. Storing per file would be redundant
- Full global-attrs JSON as the canonical column (attrs_json). A CMIP6 global-attr block is ~40–50 attributes, ~1–3 KB of text — genuinely cheap. At per-variable grain, thousands of datasets is single-digit MB. The JSON absorbs cross-model differences (your worry (a)) and serves future, unknown use cases without re-hitting the network (worry (b)).
- Plus a handful of promoted, indexed, nullable columns for the keys you actually filter/join on — parent_source_id, parent_variant_label, parent_experiment_id, parent_activity_id, tracking_id, branch_time_in_parent. These are a denormalised index over the JSON, not a replacement.
- Plus provenance: source_url (which mirror served it) and read_at (for staleness

Rule of thumb: JSON is the source of truth; promoted columns are a fast index; reamote what you query. This means a new use case that wants, say, forcing_index justreads it from the stored JSON — or you promote it to a column later — with zero extra network. Cost (worry (c)) is a non-issue at KB-per-variable.

---
That fully specifies the decoupled design. Concretely, what I'd build:

- esgf/headers.py — HeaderMetadata, general read_header(url, attrs=None) (all attrs by default), the mirror helpers + preferred_hosts ordering, and with_timeout. read_parent_info becomes a thin projection.
- esgf/health.py — NodeHealth + recording wrapper.
- db — a DatasetHeader table (JSON + promoted columns + provenance) with repository store_headers/get_headers.
- A general enrich_headers(datasets, client, repo, health, *, preferred_hosts, ...pec, that any use case can run; wire it for UC1 in the script.

Want me to start implementing this, beginning with esgf/headers.py + esgf/health.ption, no DB churn yet), then move to the DatasetHeader table and the UC1 enrichment wiring?

#### Day four (14/7) Goals
So yesterday I got Claude to build a headers.py to load and extract header metadata for experiments (making this independent of parent/child searches), and health.py to save data node success/failure stuff. Given the speed difference using NCI, we now include an option for a user to specify a preferred node (although I will double check that this is optional).

Today, I'm going to start with confirming what headers is saving (note the Claude recommendation screenshot below to save entire metadata as json in a table rather than extracting specific columns - also note that subsequently I requested that data be saved per source/experiment/variant to be independent of variable).

I also want to confirm what parallelisation is happening (and maybe double check that with you, because some of the limitations on where parallelisation can happen in this process goes a little over my head).

Confirming what health.py data is saving, and how this can subsequently be used in future searches.
A little or a lot of time could be spent on this? As we do more searches and connect to different nodes for different models, we will be building a picture (hopefully) of best -> bad nodes for that day. This will be different for a user sitting down with no cached data.
Do we want to spend time here building something related to data node health where a user can specify how long they want to attempt connections to different nodes to build a good picture before download (you had an example yesterday of 10 minutes vs an hour which isn't a lot of time if download will be really long).


Next:
Re-build that parentage set-up in order to search for parent experiments (from one to multiple hops - Gregory use-case through to g6solar).
This will be a good way to confirm that saved node health data (and saved header metadata) are being used in the way we want.

After all this:
Download data time?

First major point for the day
❯ Implement the DatasetHeader table and store/get methods. For the dataset header, is source_url related to the data node? Would it be worth retaining that information?
  health.py thoughts. I would like you to build in a retry method. Recall that these following are the potential use-cases: When trying to access file metadata information, the options seem to be as follows: 1. The data node connects, we access the metadata, and this is a success. 2. The data node connects, but 'stalls' and can stay connected for up to 25 minutes, this is not a success. Wmum wait time and ideally some way to measure whether that wait time is sensibleor not (e.g. waiting 5 seconds is too short, but 5 hours is too long, how do we determine the right value between these two)? 3. The data node does not connect, we retry the connection x times
    and the process is eventually successful, this is a success. Again, how many timesbefore we say it is a failure and how can we set this maximum retry number sensibly? 4. The data node does not connect, we try multiple times, it does not connect. This is a failure.
  You include all of them except the re-tries, given some nodes may fail multiple times then succeed, but I would need your thoughts on what is a sensible number of retries (and how to retry without being blocked from the node). The timeout method is currently at 90s, but I also want to make sure the time-to-success (if a node is deemed healthy) is saved, as this could help estimate a more appropriate timeout time. For example, if a max time for successful nodes is 45s, then we could shorten the timeout time from 90s to closer to 45s. Does this make sense?
  Also, in implementing the saved node health data, I would like your advice on if we use the saved health data as the health data is recorded, or if we trial a certain number of nodes/times to build a picture, before implementing the node preferences.

# Meeting notes
- Start live testing
- Will want to build up a picture of time taken to search index node, search and save headers (with and without caching) and for parent/child differences + different variables
- G6solar - paralellisation? How is this working?
- Also for data node access, likely will want to make preference list rather than single preferred node. Also will want to allow users to specify max workers or back-off to avoid blocking. We can create a helper function to assess this and write to config but also allow users to assert themselves.
- Markdown file - almost ready for prototyping
- Download step shouldn't be too much of a headache (even remote vs local download, just have fspec// to point to file path)
- Big challenge will be how well-coupled between CMIP generations. CMIP7 esgf next-gen API available, but no live data. Lively we go and test for CMIP5 and CMIP6 rather than CMIP6/7
- Another thought: potentially will want to make a "hybrid local path" e.g. for users who have data on large archive (like NCI) but want to see if there is additional data they need and how to save it

First live run after updates
- UC1: SSP245 'tas', single variant per model
- 11 mins for header (3 failed models)
- Should be quicker now have node health data
- Found bug in that an attrs (which should be str) was a netcdf character string
- In re-run, is it hard coding node health list for always failed data?

Bottom line

- You can stay connected to a node and pull other files — HTTP allows it, and we already do it within one file's read.
- We don't do it across files, on purpose: the per-read child process (our stall-kill mechanism) resets the connection every time.
- Recovering cross-file reuse means grouping reads by host and moving to a persistent-worker + supervisor model. The payoff (saved handshakes) is modest versus the read latency, so it's a "later, if the full 553 run shows it matters" optimization rather than an obvious win now.

Want me to note this as a documented future optimization (alongside the per-host cap), or move on to the full 553 run / from-scratch test?
For the future:
- Future optimizations noted to memory: (1) mid-run health learning, (2) per-host concurrency cap, (3) cross-file connection reuse.
- Answered your gating question: no, this run does not learn mid-run — health is read once up front for the ignore-set and ordering, then only recorded (for the next run). Correct for a clean cold baseline; static IGNORE_HOSTS still shields it from the 75s diasjp nodes.
- Wiped the cache (backed up to esgf_cache.sqlite.bak-20260714-145703 first — it held your uc2–uc5 work, restorable if needed).
- Script now does the from-scratch flow: SEARCH_SOURCE="api" runs the UC1 index search live and caches it, ONE_VARIANT_PER_MODEL=False for all 553.

For Claude

We have a currently successful workflow which searches for specific CMIP6 data via the ESGF metagrid index nodes, stores this data in esgf_cache.sqlite, then - to access specific header information, such as parent variant id - connects to data nodes to access the global attrs metadata to save in a local database. The data search (step 1) is very quick, only a few seconds for our test-case1 (SSP245, monthly, 'tas'). The second step, connecting to nodes, loading header, saving header information, is the slow step. My understanding of this workflow is as follows: spawn 8 parallel workers, then using the data from the search step, try nodes until find a successful connection (may need to have retries, may fail, may stall - which is a fail). Extract header data, save, disconnect from node, start again for next file. The node health data is also saved and could be useful to learn from for future runs (although currently during a specific run we do not learn from this health data).

We want to update this workflow in a few ways. What may be the most signficant update is that if we successfully connect to a 'healthy' node and extract header data from it, we should remain on this node and attempt to find and save files from other experiments. We do not want to be connecting/disconnecting/reconnecting. A few of my uncertainties with this, where I would like your help, are as follows: if we successfully connect to a node, will we only try to find model data on this node from models which have this node listed as a potential data source? How will we now handle stalling nodes? If a node has successfully connected (no stall), are the only two options that specific model data / files either are or are not available? I would like your thoughts on how to handle this in the workflow.

The second workflow update is that we want to include workers for each specific node. We do not want to have a large risk of being blocked by the node, but to be the most efficient we want to utilise multiple workers. How many workers would you suggest starting with? And at what connection rate (jitters/exponential back-off)? Additionally, is there a way to save information related to how many workers can successfully be used for each node before an error/block is thrown? Eventually, we may want to be able to let a user specify the number of workers per node.

If you have uncertainties, please ask.

#### Day 5 (15/7)
Goals:
- Updates to workflow
- Remain on a node for same source_id (wouldn't remain on single node indefinitely, slows process down)
- Multiple workers per node (node can handle this, but might be different per node)
- See how stalls (kill time) works in this framework
- Test on SSP, then Gregory (one hop), then G6solar (3 hops?)
- MM use-case with search: check that we can answer a 'Malte-style' workflow i.e. answer a question like "I want to do some analysis on historical. At maximum, I want to use these 20 variables. Show me a summary of the number of models depending on whether I require only these 5 variables (pick a random 5), these 10 variables (pick some set of 10) or all 20 variables"

Starting point from yesterday
"we should remain on this node and attempt to find and save files from other experiments" -> "we should remain on this node and attempt to find and save files from other experiments for the same source ID (model), this 'same' idea should be configurable by the user, set source_id as the default for now"

"We do not want to be connecting/disconnecting/reconnecting" -> "From experience, if we can get data for a given source ID from a node, it will also be able to provide other data for the same source ID"

Having read the response: ok, we give up on the idea of being able to reuse a TCP connection.

End-to-end workflow outline

1. Search + plan (unchanged): load cached datasets, group into simulations, skip cached, reuse sibling headers.
2. File lookup + invert (unchanged lookup, new step): batch-query files, then build host → {serviceable simulations} and each simulation's ranked host list.
3. Seed limits: load persisted NodeHealth; set each L_n from persisted last_concurrency, drop known-dead hosts into ignore.
4. Build per-node queues ordered by affinity_key (default source_id).
5. Dispatch loop over a pool of G workers, gated by per-host semaphores:
  - assign each sim to its best host under-cap (affinity-preferred), spill to next-best only to avoid idling a capable node;
  - each read stays with_retry(recording(with_timeout(read_header))) — the stall-kill is unchanged.
6. Outcome routing:
  - success → store, update health, AIMD-increase eligibility, advance the affinity group on that node;
  - file-level failure (404/crash/single stall) → requeue sim to its next-best host;
  - node-level failure / block signal → AIMD-decrease L_n; on repeated distress, circuit-break the node and drain its queue back to alternatives.
7. Persist: node health incl. learned L_n, and headers.

Q: preferencing nodes, three options on cold run: no preference, single node preference, ranked preference
- This already exists in the workflow to account for health data.
- No new workflow here.
- If warm run (have health data), and a user inputs a preferenced list of nodes, then the way to also include health data is "preference leads, health vetoes and fills"
## Implementation plan: node-centric header enrichment (best-node assignment + per-node pools)

Decided design (with Claude, 15/7). Turns the current **simulation-centric** header step
(one global 8-thread pool, each read independently picks the best mirror) into a
**node-centric** one: assign each simulation to its best healthy host, run per-node bounded
work queues in parallel, reassign on failure, and learn each node's safe concurrency.

### Mental model (three concurrency levels)

- **Local workers (G)** — one shared thread pool on our machine (start G ≈ 12–16). Total
  reads in flight *anywhere*. Bounds our own parallelism.
- **Nodes** — the ESGF data servers; many worked in parallel. Each hosts only *some* files.
- **Per-node connections (L_n)** — an adaptive cap per host, enforced by a
  `threading.Semaphore(L_n)` ("bag of L_n tickets"): a worker takes a ticket before reading
  from host *n*, returns it after. Invariant: `in_flight(n) ≤ L_n` ∀ n, and `Σ in_flight ≤ G`.
  ("in-flight" = read started but not returned yet.)

### Dispatcher / preferencing

- From the file lookup, invert into `host → {serviceable simulations}` (ground truth from
  the index — no probing needed). Each simulation keeps its ranked candidate-host list
  (`order_candidates`: preferred → learned health → https).
- **Dispatch rule:** keep G workers fed; assign each pending sim to its *best host that is
  under its cap* (walk the ranked list). Preferred/fast nodes (NCI) get first refusal up to
  their cap.
- **Anti-bottleneck (spill):** never leave a capable node idle. Spill a sim to its next-best
  host only when its preferred host is saturated **and** a capable alternative is idle. So NCI
  stays saturated (drains fast) while slower nodes work the overflow in parallel — no single
  node bottlenecks and none idles.
- **Affinity (soft):** within a node's queue, cluster by `affinity_key` (default `source_id`,
  because a model's datasets usually share one origin node). Once a node serves a group, prefer
  keeping that group's remaining sims on it — but never at the cost of idling/bottlenecking.
  New DI seam `affinity_key: Callable[[DatasetRecord], Hashable]`; users can pass
  `experiment_id`, `(source_id, experiment_id)`, or `None` (pure best-node fill). Ordering only,
  never correctness.

### Preference modes (all via existing `preferred_hosts`)

1. none → `()`  2. single → `("esgf.nci.org.au",)`  3. ranked → `("nci…", "ceda…", "dkrz…")`.
- **Cold run:** preference is the only steering signal; ranked list obeyed literally.
- **Warm re-run — "preference leads, health vetoes & fills":** (1) a preferred node health has
  *proven* unreliable (unreliable_hosts / circuit-broken) is dropped entirely; (2) a merely-slow
  preferred node still leads (user knows their context); health only orders the *unranked* nodes;
  (3) every node starts with its *learned* L_n from the DB, not the cold default.

### Outcome routing (once "connected" is not binary)

Separate three things: node reachable ≠ file present ≠ file readable.
- **success** → store, record health, advance the affinity group, count toward AIMD increase.
- **file-level failure** (404/index-mismatch, `HeaderReadCrashed`, a single stall) → requeue that
  sim to its *next-best* host; keep using the node for others.
- **node-level failure / block signal** (HTTP 429/403, connection-reset burst, latency cliff) →
  AIMD-decrease L_n; after `DEFAULT_MAX_ATTEMPTS` (3) consecutive node-level failures,
  **circuit-break** the node: evict it and drain its queue back to alternatives.
- Stall-kill mechanism (`with_timeout` subprocess kill) is unchanged — still needed.

### Adaptive per-node concurrency (AIMD)

- Start L_n = 2 (NCI 4), or seed from persisted `last_concurrency`.
- **Increase (+1)** after a streak of clean successes at current L_n with no block signals, up to
  a **hard ceiling of 6–8** (shared scientific infra — stay polite).
- **Decrease** on a block signal: `L_n = max(1, L_n // 2)`, then a cooldown before increasing again.
- Ordinary file failures (404 / one corrupt file) do **not** back off concurrency — only host
  distress does.
- **Persist** so learning survives runs (mirrors the `suggested_timeout` pattern).

### Data-model changes (`db/schema.py` `DataNodeHealthStat` + `health.py` `NodeStat`)

- `max_safe_concurrency: int` — highest L_n that ran clean.
- `last_concurrency: int` — where it converged; seeds next run.
- `block_events: int`, `last_block_at: datetime | None` — for cross-run cooldown.
- New `ReadOutcome.BLOCKED` (429/403/RST-burst) distinct from `ERROR`, so AIMD-decrease and
  the health veto only fire on genuine host distress.

### Config knobs (script-level, DI-injected)

- `preferred_hosts` (the 3 modes above), `affinity_key`, global `max_workers` (G),
  `default_node_concurrency` (2) + per-node overrides `node_concurrency: dict[str, int]`
  (pins L_n, disables AIMD for that host), `node_concurrency_ceiling` (6–8),
  `circuit_break_after` (= DEFAULT_MAX_ATTEMPTS).

### Suggested phasing

1. Schema + `NodeStat` fields + `ReadOutcome.BLOCKED` + block detection in `recording`.
2. `host → simulations` inversion + `affinity_key` seam (pure, unit-testable).
3. Dispatcher: per-host semaphores + best-host-under-cap + spill + requeue (replaces the single
   `read_map` over `plan.to_read` in `enrich_headers`).
4. Circuit breaker + AIMD controller (mid-run learning) + persistence.
5. Wire knobs into `scripts/enrich_uc1_headers.py`; live-test UC1 (SSP245 tas), compare timing
   vs the 191s baseline; then Gregory (1 hop) and G6solar (multi-hop).

### Testing

- Inject a fake reader (no network) that returns/raises per URL to exercise: spill when a node is
  capped, requeue on file-level failure, circuit-break on repeated node-level failure, AIMD
  up/down, and preference-vs-health ordering. Keep coverage ≥ 90%, mypy --strict, ruff.

### Open / deferred

- Cross-file *connection* reuse (persistent-worker + supervisor) still deferred — modest payoff vs
  read latency given subprocess-per-read; revisit only if the full 553 run shows it matters.
- Global inter-request throttle per node (small randomized spacing) — fold in with AIMD if block
  signals appear.

Starting point from yesterday
"we should remain on this node and attempt to find and save files from other experiments" -> "we should remain on this node and attempt to find and save files from other experiments for the same source ID (model), this 'same' idea should be configurable by the user, set source_id as the default for now"

"We do not want to be connecting/disconnecting/reconnecting" -> "From experience, if we can get data for a given source ID from a node, it will also be able to provide other data for the same source ID"

Having read the response: ok, we give up on the idea of being able to reuse a TCP connection.

End-to-end workflow outline

1. Search + plan (unchanged): load cached datasets, group into simulations, skip cached, reuse sibling headers.
2. File lookup + invert (unchanged lookup, new step): batch-query files, then build host → {serviceable simulations} and each simulation's ranked host list.
3. Seed limits: load persisted NodeHealth; set each L_n from persisted last_concurrency, drop known-dead hosts into ignore.
4. Build per-node queues ordered by affinity_key (default source_id).
5. Dispatch loop over a pool of G workers, gated by per-host semaphores:
  - assign each sim to its best host under-cap (affinity-preferred), spill to next-best only to avoid idling a capable node;
  - each read stays with_retry(recording(with_timeout(read_header))) — the stall-kill is unchanged.
6. Outcome routing:
  - success → store, update health, AIMD-increase eligibility, advance the affinity group on that node;
  - file-level failure (404/crash/single stall) → requeue sim to its next-best host;
  - node-level failure / block signal → AIMD-decrease L_n; on repeated distress, circuit-break the node and drain its queue back to alternatives.
7. Persist: node health incl. learned L_n, and headers.

Q: preferencing nodes, three options on cold run: no preference, single node preference, ranked preference
- This already exists in the workflow to account for health data.
- No new workflow here.
- If warm run (have health data), and a user inputs a preferenced list of nodes, then the way to also include health data is "preference leads, health vetoes and fills"

Fix after first live run:
Root problem: eviction fired on 3 consecutive failures, and affinity clustering makes consecutive dispatches hit the same model — so one model with missing files on a mirror evicted that mirror for everyone. It killed Globus (93% healthy, 228 successes) and stranded 214 sims.

The fix — rate-based eviction: a node is evicted only once it has ≥ evict_after_attempts (6) reads and an overall success rate ≤ evict_max_success_rate (0.2). The scheduler now tracks cumulative attempts/successes per host instead of a raw failure streak. Effect on last run's nodes:
- Dead nodes (ucar 0/12, gfdl 0/9, apcc21 0/9) → still evicted quickly. ✓
- Globus (228/244 = 93%) and ORNL (39/66 = 59%) → never cross 0.2, so they survive. ✓


failure cause for most of the models!! http vs https -> see https_twin in header.py
- This should fire regardless of if warm/cold run

### Thursday 16th July
A few zoom meetings
Gregory + G6solar parent/child hops
Monday (likely) implementing and starting to think about ESGF querying and integrating across multiple projects, and ESGF1/ESGFNG (likely will go back to recording and make notes from there about plan)

Before working on that, however...
fx, like areacella:
- Some models have a single areacella for piControl (assumed same for all other experiments), while other models have areacella for all experiments
- Speed vs safety -> Either we say for a model go and find where the areacella (or other) is for any experiment and call that truth, or we say go find the areacella for specific experiment (and identify fall-back options if not available). Is it an assumption that they are all the same for a single model across all experiments? Maybe go test this...

Parent/child branch failures
- We want the user to be able to specify the stop point. Enter a str or None and this tells the code how far to walk. If they include a typo then the program will walk as far up the chain until no_parent then will abort.
- Ultimately we want to create a plain text file of fixes (where e.g. there are errors in that historical points to a parent variant, but there is only one piControl run for that model that has a different variant, so we should just assume that is the variant). We will want to create a table of known fixes (to maybe one day publish on git) in plain text format to be input as plain text format, or specifically so a user can make a fix (or override) themselves, e.g. for this model, experiment, variant, THIS is the parent experiment/variant_id

Paralellise dataset bundle:
- Rather than searching for ~30 datasets at once (and having to back off for large files like piControl), we want to parallelise and do a single dataset at a time. Allows for more specificity, we should never hit limit, so won't have this issue, although it might be slower.

### End of week 1 (17 July)
End of week notes:
- Initial plan for the week was to take a range of CMIP6 use cases, search for the data, and start downloading
- We spent the whole week on the search step (which, for the most basic use-cases, occur in two steps), and now realise that downloading may be relatively straightforward, given we have learned efficient ways to connect to nodes, and with fspec can easily link to remote or local path)
- Next week the goal will be to start looking into how to integrate the current workflow across multiple MIP eras and ESGF portals.

Search index node and header learnings from the week
- Index node search step really fast, within seconds retrieves records (results) of datasets of all search results
    - Need to maybe refine columns here
    - CRITICAL: remove data_node from the dataset model to avoid multiple entries for same simulation
    - Current workflow works around this, but want to be very explicit with this. Take data node information into separate table (see notes in `anna-setup-zn` branch).
- Metadata - global attrs -> get header information
    - The headache for a lot of the week
    - Paralellising workers on local machine and setting multiple workers on individual nodes
    - Now successfully recording header read attempts and data node health
    - Challenge with this process is the multiple potential outcomes regarding node health (success, fail, retry+success, retry+fail, connect+stall+fail)
- Problem-solving throughout the week:
    - http vs https for some nodes (for failed nodes swapping these worked)
    - byte-reader vs OPEnDAP. byte-reader better, build in something for OPEnDAP too, or those nodes just assume dead?
        - just assume dead for now, I think it's fine
    - Cold run vs warm run
        - Cold run does not learn as it goes, does not preference node health, but still has a number of potential choices for user (e.g. max_stall_length = 90s, which then shortens on a warm run)
        - We will want to be very explicit about potential preferences for a user on both cold and warm run (especially ranked node preferences to use/not use)
        - Will want to know exactly what health data 'fixes' for a warm run, and how that integrates with user preferences
        - Make sure on warm run that all failed models are attempted again (even if on 'dead-node' list)
- Parent/child header
    - Multiple potential use cases.
        - A user knows what data they want explicitly (e.g. gregory method abrupt-nxCO2 and piControl). In this case all abrupt and piControl could be identified in an initial search index node step. However, note that sometimes multiple abrupt experiments point to a single piControl. In this case, we don't want to be duplicating header searchers where there is a many-to-one case. Right now the code (I think) is set up to search for both experiments in initial index node step, but maybe want to simplify this into workflow where a user can choose a parent (as below, so that it is less 'use-case specific').
        - A user knows the starting point but not the path to get to parent. i.e. SSPs, G6solar (use case). Multiple hops. Critical here with not duplicating header reads. For this use-case, where we don't assume path to parent (parent is either specified by user or we just walk all the way up the tree), we need to make multiple steps in search/header steps (as spoken about on zoom today). Files vs datasets, don't want to be searching for multiple files at once. Want to be able to track things, reduce potential bugs.
        - Question: if user specifies parent, do we still search header information for that parent (i.e. piControl)? Or do we just stop header search at one hop before parent (e.g. historical)
            - no don't search header information to find out the parent if the user already specified it. However, we may have to get the header anyway for other reasons, so we'll have to think about the right workflow.
        - Fixes/overrides: Some experiments do not have correct parent information. For example, an experiment might point to a piControl variant that doesn't exist. We want to build up a file (or file(s) effectively a plain text database) of known fixes that a user can optionally suggest as overrides for the data
            - Is this something we could use our current search/header workflow to start building up?
                - think of this the other way around. We build it up ourselves, but we can use it in our search/header workflow
            - At what level can we say "this is not an assumption anymore"? For example, if we find a model that has a single piControl variant, and we test all the experiments for that model, and we find that not all parent_varaint_id point to that single piControl, could we just say that that is a fix and we can list that in our fixes? Isn't that still an assumption?
                - we can only say, "This is not an assumption" if we a) confirm with the modelling centre directly or b) make some plots to show e.g. "This file lines up exactly with this other file, so it's fair to assume this is the parent, even though the metadata says otherwise". The existence of only one option isn't enough to guarantee that it is the parent (maybe the actual parent was just never published on ESGF)

Question of curiosity:
- Getting headers and learning about node health, super useful for download step, or not that useful? If node connects quickly to access headers, will that mean it connects quickly, works more quickly for download?
    - very likely super useful, but not guaranteed. For downloads, download speed is key and header access doesn't really test that because we download so little data. Obviously very helpful for identifying dead nodes (which also can't be used for download)

For next week:
- fx (e.g. areacella) stuff -> add into search (all experimentss or just single experiment for model?)
- (discussed above, i.e. smarter data_node table with column removed from core dataset, parent/child search-file workflow breakdown, user parent-data fixes, where to stop 'hopping' up parent tree?)
- Integrate across MIPs and ESGF portals! Re-watch zoom? Chat to Claude, talk through Claude's suggestions for how to do this. Esp. raw data vs renaming to common naming conventions.
- Continue with naming thinking? honestly esmporiun is still my favourite :/
    - I can live with esmporium, it's subtle enough
- create a schematic of the workflow: both for ourselves and future docs. We should be near enough to stable to be able to do this. Talk to Claude about options (raw ascii, a tool like mermaid, some other diagram making tool)


#### START WEEK 2 (20 July) #####

Claude prompt:

We have a successful workflow for a range of test cases which can search ESGF index nodes for potentially available data, then connect to data nodes to access and save header information which includes global attributes (such as parent information). This works for a range of use cases, ranging from a simple use case to access a single experiment and variable across models (ssp245 tas) through to more complicated where we start with an experiment (e.g. G6solar) and want to walk up the parent chain to an end point where we may not know what the intermediate 'hops' are. We are in a good place with this, but we need some refinement.

I will describe some of the refinements, but you may also look in schema.py and query.py under 'TODO'. Note that in query.py especially there are a lot of notes for integrating between cmip5/6/7 and between esgf1 and esgf-ng. Working on that will be my next step, but for right now please don't address this. I want to get the current workflow right for just cmip6, then we can start with integrating across mip generations and esgf portals.

First of all, I want to remove the data_node column from dataset. We do not want to have multiple rows for the same simulation in dataset, that only differ by data_node. See note from schema.py "    # This should be on the files, it shouldn't exist at all on the dataset
    # (if it is part of the API response, drop it and either don't store it
    # or only have it in the full raw JSON response that we store)"
Currently, our workflow has workarounds for a single simulation having multiple rows that only differ by data node. Please suggest how you would implement changes so that each simulation has a single row in dataset, and how/where that information may link to another table that could ahve multiple rows for a single simulation if the simualtion is available from multiple data nodes.

Second of all, I would like to go through the workflow for parent/child test cases. Specifically, in the search index node / header stages. I would like you to describe the differences between datasets and files for esgf, and exactly what the current workflow is doing. My understanding is that once we obtain parent information via child headers, we then need to search the index node for the parent experiments (files or datasets?), and these queeries are sent off in batches. We do not want this workflow in batches. We want to have a single search for each parent simulation. While this may be slighlty slower, we want to parallelise efficiently here, and also be able to track exactly what is being searched for to minimise bugs. I may have some more thigns to say about this set-up, so please tell me (visually, if possible) what the workflow is currenlty doing, and what you suggest based on my initial preferences.

Thirdly, an additional 'variable' that users may want to search for are the frequency (fx), such as areacella. This will depend on the model. Some models will ahve a specific areacella (fx) variable for every single other variable (e.g. tas, pressure, precip), but sometimes it will just be for a single one per experiment, or some models will just have a single areacella for a single model, perhaps for piControl (e.g. UKESM, i beleive). As such, this will need to be relatively flexible with how we implement this, as it will be very model specific.

Let's start with these fixes. Perhaps doing these separately? Let me know what you think.

Claude prompt - data workflow

Don't care about header for files, care about header for dataset, but to get header for dataset we need to go into one of the files of the dataset to get global attributes for specific dataset.
get header, save it to the file URL we obtained it from, and save that to the dataset for the file.

I want to talk through two use-cases so that we can be on the same page about searching index node + dataset + dataset file + header information. First of all, we can consider a simple case, such as ssp245 'tas', where we search the index node, then get the headers for each dataset. Secondly, we can think of the use cases where we know the child experiment, and the 'stopping' parent expeiment, but not the parent chain in between. Recall that in this parent/child use-case we initially only search the index node for the initial child experiment dataset, then use the header information obtained from the child datasets to search for the parent data and files. I want to talk through exactly what we want this workflow to look like for both use cases (you can suggest where they overlap, or diverge), I will want you to create a visual representation of this workflow (in a diagram whose source can be trackable in plain git, but which might link to somethign to truly visualise it), then in testing I will want to see exactly what function calls are happening at each step, what the inputs and outputs are, and where parallelisation is happening.

Step 1 - search for datasets on index node:
- This step will be common to our single use and parent/child use case. The only difference is that with the parent/child use case it is only the 'starting point' child that is being searched for.
- We understand at this step that certain AND/OR combinations aren't supported, and that this step will be (already is?) paralellised on the client side (our side). Hitting search API can use threads?
- Saving the information to the database. We note here that in the updated workflow that I have already provided, we are changing the primary key from id to instance_id. By doing this, we are saying now that resulsts that differ by data node are not actually different datasets. This is already a difference with the ESGF data model (as we will soon be investigating when we come to differenes in MIP and ESGF1/ESGF-NG). Where would you recommend saving the raw JSON results for each dataset in this case so that we can still obtain the raw dataset that the search result obtained?
Step 2 - Adding files to the datasets:
- Again, this step is shared between the two use-cases.
- We want to conduct a single search for files per dataset (no more combining across datasets to search for lists of files). This reduces changes of bugs, and avoids overflow (the 10000 file limit). We will have to make this paralell on our side (already is? or will have to be explicit because we are no longer lumping datasets into one search).
- From here, we want to immediately save informateion in a database. This immediately links datasets to files in access URL using fsspec links.
- We can consider this a caching step, to identify if we already ahve this information for a given dataset. We are breaking file results into file and access places.
Step 3 - get header information for datasets:
- For every dataset, we only want a single file retreival to obtain the header (this will be in parallel on our side - although want to maintain the potential paralel connections to nodes at once)
- one retrieval per dataset but using a common pool and config for workers
  (have to make parallel on our i.e. client side)
- real world: get header information for a single file
- also not really a header, because files have headers, datasets don't.
  Probably need to tweak language here, maybe
  'header-only metadata' or 'header-only dataset metadata'
- save header information to the file it applies to.
  extract the dataset applicable metadata and add that to the dataset too, making sure we also know exactly which file the metadata comes from
Step 4 - use dataset header information to drvie search for aprent dataset:
- Here our use cases differ. This is only relevant to the 'hopping' parent/child chain.
- We then need to start again in the search-index-node step 1, HOWEVER, we only do a single search per dataset (making sure only one search per parent - noting that multiple child datasets might point to a single parent, although perhaps we solve this by doing a database 'check' to avoid duplicates)
- one search per dataset (makes it simple to process results),
(have to make parallel on our i.e. client side, hitting search API so can use threads?)
- don't use fancy AND/OR to mean we only have to make one search
- save parent information to dataset (i.e. make child <-> parent links)


After this, we should end up with a dataset which includes file links, parent dataset link, and head-only metadata (eventually will also want auxilliary file link, such as areacella, but that is for future.)
We will also want a file entries dataset, which includes access links i.e. node availability (in separate table i.e. file - access info)
Entry for end of chain parent data. end of chain parent only needs to be based on search API, not headers, because don't need headers for final parent, only children and intermediate parents.

Let's map out a plan for all this, and how similar/different his is to the step 1 you were about to implement. please let me know if you have questions. REmember, i want first a plan including a diagram of how this workflow will go.

Claude prompt:

Parent/child stopping and hops. The three use cases (1. parent specified by user, 2. None specified, 3. typo/error). Your workflow for 3. currently has (3) User specifies the wrong stopping parent (typo, or an experiment the chain never passes through). Condition (1) never fires because nothing matches, but the "no parent" sentinel is always active as a fallback, so the walk still terminates cleanly at the true top instead of running forever. Then a post-walk validation checks: "did the chain actually pass through the experiment you asked to stop at?" If not, we surface it — I'd make this a warning by default with an opt-in strict mode that raises ("you asked to stop at 'picontrole' but the chain terminated at 'piControl' without ever reaching it"). That's your "stop and/or throw an error."
- An update to this is that we don't want to have warnings. We would prefer to throw an error. Two potential things to consider are that if a user has specified a parent (i.e. not None), we do a quick search on ESGF index node to see if the experiment exists. That is one confirmation for any not None use case that the experiment actually exists. If it doesnt, throw error. If the experiment exists, we walk up the chain. Now, either the chain will arrive correctly at the parent experiment and stop, or the parent experiment supplied by the user is not in the parent chain (e.g. user specifies starting point as g6solar, and parent as ssp119 - both experiments exist in esgf search, but incorrect parent). In this case we can walk all the way up the chain, once we hit no_parent, we suggest error.


Versions. I want to be very specific about having different tables for versions. You are correct that we should alter the workflow so that PK is now master_id (instead of instance_id). We should then create a new table such as
class DatasetVersion:
    dataset: Dataset  # link to version-invariant dataset definition
    version: str  # done as dates, so has to be a string. We should add validation that these can be passed to date time so we can sort them properly later
    parent_dataset: DatasetVersion  # link to parent dataset (including version)
    version_specific_attribute: float  # can't think of any other good examples right now, but you get the idea I hope
Note that we will need to shift any version-specific columns from dataset to datasetversion. Such as: parent information, version information, auxiliary (e.g. areacella) links (although we will think about this later). Then we will also have to have this table link to the Dataset Location. Please explain how these will all be linked. You will need to update the serach-worlfow.md please so that I can see the diagram and that I have provided clear isntructions. Thank you

### Week 2, day 2 (21/7)

I'm starting my day. Plans for the day:

Implement the new workflow which covers versions and the parent/child searching set up.
Important to understand what is happening throughout workflow (in terms of table connections and which functions link to each other)
In testing can be very explicit at each stage to confirm what's happening

Start implementing auxiliary variables - use Claude's initial suggestion of hierarchy in searching, but this will likely raise questions for me / will be a learning opportunity where I realise how little I knew about these fx variables.
Hoping to get up to here today, but we'll see.
Next steps will be multi-project/esgf integration.

Claude prompt leaving off from yesterday:
by 'parent does not exist', I mean that the parent information found in the child receives no search results on the index node, which in itself is different to a parent dataset existing but not being
available on any data nodes (i.e. dead nodes). I like this error "CanESM5 abrupt-4xCO2 r1i1p1f2 declares parent (CanESM5, piControl, r1i1p1f9) but no such dataset is published on the index node — the chain
cannot be completed." Note that for this use case, we want an option for the user to 'override' (either from the beginngin or after error?). If they know the specific child/parent link and that the global
metadata is wrong, then they should be able to implement a fix. We may also build a set of helper tools which offers our known fixes/overrides for parent information that a user could choose to implement.
None of this is built, but there should be room to adopt these.

Additionally, in this parent/child searching, we should not assume the institution_id is the same between parent and child. source_id has to be the same, but institution_id can sometimes vary. This should
be implemented now.

Going through repo workflow thoroughly to understand what is happening at each stage. Notes/questions
- db/schema.py: SearchRun : status, spec_json, tag??
- DatasetChange : detail_json (user input or error message??), necessary?
- db/repository.py _DATASET_FACETS tuple maybe area for change in integration?
  - record_run() uses spec. not sure where this is defined/written?
- db/esgf/client.py ESGFResponseError (shaped like solr result?) -> links to schema.py issues in TODO?
- schema.py: FileAccess questions
  - "Note the fsspec_url column: it's populated only for the two HTTPServer rows (an http→https upgrade so the URL is directly openable). The other services (OPENDAP/GridFTP/Globus) leave it NULL. Those two HTTPServer URLs are exactly the candidates Step 3 will try to byte-range read."

  - config.py 10,000 limit question

  - search/headers.py - a number of default choices written in here -> some of these will want to be user choice?

STep 4: parent walk
- search/parent_walk.py -> remove default of CMIP6?
- search/aggregate.py MAX_PARENT_HOPS = 5 -> reasonable assumption?

### Week 2, day 3 (22/7)

Responses to QUESTIONS throughout:

Group A — CMIP6 → CMIP7 / ESGF integration (the recurring one)

Five questions are really the same underlying question: "how much of the CMIP6-shaped assumptions survive ESGF/CMIP7 integration?"

- config.py:17 DEFAULT_PROJECT = "CMIP6" — [I understand / partly your call] This constant only supplies the default for Settings.project; it's already overridable via Settings(project=...). Nothing is hard-coded harmfully. The real question — does a single project facet even survive CMIP7 (which may merge activities differently) — is a forward-design decision that depends on what the CMIP7 index exposes. Safe to keep for now.
- client.py:30 "must the response be a Solr result?" — [I understand / client's call] The client parses the Solr/Globus-Search JSON shape (numFound, docs). It only has to look like that shape. If CMIP7/ESGF-next moves to STAC or another API, this contract changes. This is a genuine integration decision, not something I can resolve — flagging it as yours.
- repository.py:58 _DATASET_FACETS and :74 _VERSION_PROMOTED — [I understand] These two tuples are the schema contract: which facets are version-invariant vs. which are promoted from netCDF headers. Your note on :74 is the sharp one — for CMIP7 the parent linkage (parent_source_id, etc.) may come from the index-node search itself rather than from reading headers. If so, that whole header-read step for parents becomes optional. That's a real design fork worth deciding once we know CMIP7's index capabilities. Flag as client/CMIP7-dependent.
- parent_walk.py:218 "removing default of CMIP6?" — [I understand / recommend] Yes — I'd thread project from Settings rather than defaulting "CMIP6" in the function signature, so it can't drift from the configured project. Small code-hygiene fix, I can do it.

Group B — factual answers (I checked the code)

- config.py:22 "are MAX_PAGE_SIZE/MAX_RETRIEVABLE still used?" — [factual: yes, actively] Both are used in both the sync and async clients (client.py:117,181, async_client.py:129,150): they clamp page_size and raise DeepPaginationError when num_found > 10000. The 10000 limit applies to the index-node search (the Globus Search backend rejects offset >= 10000), not header reads. A narrow query (source_id, experiment_id, variable) won't hit it; a broad one (one experiment × one variable across all models/variants) can. So it's a live guard — keep it.
- schema.py:114 / repository.py:182 "does the user specify spec_json?" — [factual: no] It's machine-generated. runner.py:264 builds spec={"queries": [q.as_spec() ...]} and parent_walk.py:423 builds spec={"parent": ...}, then repository.py:196 does json.dumps(spec, sort_keys=True). The "note: walkthrough uc1" string you saw is the tag, not spec_json. So: spec_json = normalized machine identity used for diff-series matching; the user never types it.
- schema.py:131 "is the status marker meaningful?" — [factual: not currently] Grep finds no writer that ever sets status= to anything — it's always the default "ok". So today it's vestigial. It only earns its place if (a) a failed/partial run sets it to an error marker and (b) the diff logic skips diffing against a failed run (so a node outage doesn't look like mass "removed"). Decide whether to wire that up or drop the column.

Group C — change-tracking design (I understand these)

- schema.py:135 "tag vs spec_json — why both?" — [I understand] They do different jobs: spec_json = machine identity for "the same search over time" (diffing); tag = optional human label for retrieval convenience. Both justified; the docstrings could say this more bluntly.
- schema.py:380 detail_json on DatasetChange — [I understand] Code-filled (not user), holds old/new _timestamp for "modified" events so you can show what changed. Keep it for modified; it's null for add/remove.

Group D — data-model details

- schema.py:345 "must fsspec_url start with fsspec?" — [factual: no] It's a URL openable by fsspec (currently plain https://), not one literally prefixed fsspec. The name is misleading — openable_url would be clearer. Cosmetic.
- schema.py:350 "is replica important?" — [I understand] Marks primary vs. replica location. Not needed for data correctness, but useful for node ranking/provenance. Cheap to keep.
- schema.py:355 "raw_json null in uc1 — ever populated?" — [factual: partially] There are two raw_json fields. The one at :266 is required and written at repository.py:979 (record.raw). The location-level one at :356 is the optional debug escape-hatch that was null in uc1. It's provenance-only — either populate it always (storage cost) or drop it. I'd lean drop unless you want it for debugging.

Group E — tunables (I understand; the value is your/client's call)

- aggregate.py:42 _MAX_PARENT_HOPS = 5 — [I understand / recommend keep] Real chains are ≤3 (ssp→historical→piControl = 2 hops). 5 is a safe cycle guard. Reasonable.
- headers.py:51 user-specifiable promoted attrs — [I understand / your call] Right instinct: split into a required core (parent_source_id, etc. — the parent workflow breaks without them) plus a user-extensible set. Which extras to expose is a client-facing choice.
- headers.py:609 _BLOCK_SIGNATURES — [I understand / your call] Heuristic substrings (429, 403, "rate limit"). Correct to centralize and grow as we observe real node behavior. Exact list is a judgment call — keep it configurable.
- dispatch.py:54 DEFAULT_MAX_WORKERS = 12 — [I understand — one correction] A 64-CPU cruncher server does not imply more workers: header reads are network-I/O-bound, not CPU-bound. The right scaling axis is how much load shared scientific servers tolerate, not local CPUs. So it should stay a modest, per-node-aware budget (which the controller already learns), not scale with cores. It's already a DEFAULT_ param, so overridable.

Questions: is_latest (defined by us or cmip?)
get file -> kills all when receives 500 kill

Questions for Zeb:
- Running live use-case-1 (ssp245, tas). Expectation is 553 simulations returned from the initial search step. Version question, do we want to search and save all the versions available? Or do we only care about latest_version = True for the cold run, then on additional runs we want to know if more versions have been published? It looks like the code is currently written to search for latest_version = True, which I only picked up on becuase two model-variants have two versions, both of which are True for latest version. The latest_version is populated from the json data in the search step, meaning that we cannot necessarily rely on 'latest' data (unless it comes from the same node?). So, do we want to save 'latest' as a column? Do we want to get all available versions initially in Step 1, then we make a choice at a later step about which one to focus on?
- The Step 2 (search for file location) has a little bug. It searches for file location with default search proxy as metagrid.esgf-west.org (which is set in config.py Settings()). In the first live run attempt, about 50% of searches were returning as failures. We retry 5 times for each dataset before the entire step is killed ("HTTP 500" error), with nothing written into Files or FileAccess tables (even for successful models). So potential areas to fix this is to make this save as we go (somethign along those lines?) and also to attempt different mirrors (could you explain what a 'mirror' is? - claude uses it for the different nodes?), rather than our default esgf-west (such as CEDA, DKRZ, ORNL), although this risks not all of the datasets being available on these to link to files.

Updates based on above questions:
- Get all versions (Q: at what step do we not care about version information? Files? Headers? download?)
- "I also don't know what claude means here for mirrors. Does it mean different search API URLs? I would be more tempted to just back off our searches and do them more slowly/with fewer threads. The other option is to use different search API URLs. In theory, they should all give the same results..."

Before we continue and re-attempt the full use-case-1 (553 simulations), I want to make some updates based on the two issues found from the first attempt. Firstly, regarding the versions, in the Step 1 search step, we want to find all versions. In the datasetversion table, we want to keep the is_latest column, although we now know that we cannot rely on this information. For the Step 2, this is where we want to only get file (and for step 3, header) information for the latest version. We will need to build a little function which finds the true latest version based on the str version date. Please add these (in theory a user could also specify which version they want in the search step).
Secondly, for the Step 2, we will need to make some updates to the workflow (what do you mean when you refer to mirrors?) Is it possible to save data as we go? Potentially what would the impact be of using fewer threads as well, to back off our searches and do them slightly more slowly? How could we also incorporate different URL/APIs rather than the default if there are datasets that are failing?

Vocabulary I'll use (and stop abusing):
- search endpoint — an index server that answers search queries (metagrid-west, CEDA); configured by base_url (+ distrib). Not "mirror."
- data node — a server hosting file copies (FileAccess.data_node); a replica if it's a copy. This is the Step 3 concept.
- dataset — a Dataset row (master_id).
- dataset version — a DatasetVersion row (instance_id = master_id + version); its version is the vYYYYMMDD string.
- file search — the Step 2 query that finds a dataset version's files.
- So when a Step 2 query doesn't return, I'll say "the file search for a dataset version failed" — the search failed; the dataset version is just its subject. I won't call it a "failed version."

Step 2 prompt:

I want to make updates to the second step in the workflow: this step searches for the file URL in preparation for accessing the headers. This step populates File and FileAccess tables. Since completeley updating the repository a few days ago, we now are getting the 500 error for a number of datasets when attempting to call the metagrid-west API. I think this is related to our current chunking/retry/backoff methods, rather than due to other people attempting connections to the index node. In general, I would always like you to prefer paralelisation rather than chunking (except at the very initial step 1, index node search step). So, for Step 2, for each dataset, I would like you to start n_workers to paralelise, call a single URL, eg.g. metagrid-west, and save the file URL. If this is not successful, implement an exponentnial back-off strategy with default 3 retries. If this fails, then return to queue, and retry the URL again. An additional update is at this step we want to set a preference for URLs. Our recommendation is (metagrid-west, ceda) (first, second choice), and this should be used as the default but a user could specify at this step too. In addition to this, we want to retain all the index node health, similar to datanodehealth table. Retries, success, failure. Please create a detailed plan of this, then we can get started.

Claude suggestion based on prompt
Per-dataset, three escalating levels of resilience against a preference-ordered list of index endpoints (default (metagrid-west, ceda)):

- Level 1 — in-request backoff: each per-dataset search retries with exponential backoff, default 3 retries, against the current endpoint.
- Level 2 — requeue: datasets still failing after backoff go back on a queue and get one more full pass against the same endpoint (workers preserved).
- Level 3 — endpoint fallback: datasets still failing move to the next preferred endpoint (metagrid-west → ceda) and repeat levels 1–2.

Parallelism throughout via n_workers (thread pool — HTTP I/O), never aborting the pass: a per-dataset exception becomes a recorded failure + requeue, never propagates.

NOte for parent retries!!

Call-site updates made

- parent_walk.py → clients=(self.client,), raise_on_incomplete=False
- test_version_headers.py / test_files.py → new clients= API

One thing to flag: parent_walk now passes its (retry-wrapped) client as a single-endpoint list, so on a persistent failure during a parent hop it double-retries (orchestrator 5× on top of the client's own backoff). Harmless but wasteful; I'd clean it up when we wire endpoints properly. That's part of step 3 (config default (metagrid-west, esgf.ceda) + building no_retry per-endpoint clients + updating the scripts), which is next whenever you're ready.

### Week 2, day 4 (23/7)
Plan for today:
- Finish with step 2
  - Note that esgf-west down until 3/8? So preference ceda, then ornl, then metagrid-west.
  - But ultimately still prefer esgf-west because seems to have most acces to all data consistently
- Look into header step and if Step 2 changes affect header step at all.
- auxiliary
- Re-do headers so save as it goes.
- Fetch anna-setup-zn (NOTES-PROD.md)
- May need to fetch repo because path changed

Longer term plans:
- Start with ESGF1/NG querying. Potentially let's say we have an esgf-client currently set up. Let's rename that esgf1-client and create a new one for esgfng-client. The goal is that for the same search (e.g. tas ssp245) we will receive similar if not identical search results to be able to populate the database, but we may not know until we look into this and see what the esgf-ng api returns
- Then go ahead with CMIP5 and how/where those differences are. Overrides, user specifications.

Longer-longer term:
- Downloads
- Production phase

3. The no_files refinement, made specific

The one concrete stone I laid while here: the log now records an empty outcome (HTTP 200, zero files) distinctly from success — so the 7 problem versions are queryable today via get_file_access_attempts(outcome="empty"). No behaviour change.

The full refinement, precisely:

- Symptom: 7 latest versions (CESM2 ×3, MRI-ESM2-0 ×4) ended Step 3 as no_files.
- Root cause: CEDA's search returned 200 with zero File records — it indexes the dataset record (Step 1 found them) but not the file records for those models. add_files treats 200-empty as _OK, and escalation fires only on exceptions, so those versions were never tried on ORNL/west.
- The fix has two halves:
  a. Detect the soft-miss — 200-with-0 ≠ 200-with-N. (Built: the empty outcome.)
  b. Escalate — treat empty as a soft-failure that falls through to the next endpoint, but re-query the fallback by the node-independent instance_id facet, not the node-specific dataset_id. This is the crux: a naive fallback reusing CEDA's dataset_id on ORNL would also return empty (ORNL doesn't know CEDA's node-scoped ids — the cross-index portability limit). The instance_id requery is what lets any federated index answer, and it doubles as a disambiguator: found somewhere ⇒ index gap; found nowhere ⇒ genuinely retracted/unpublished.
- Guardrails: cap at "every endpoint tried once by instance_id", then record a terminal outcome (e.g. no_files_any_endpoint) and have the version_has_files cache gate treat it as resolved, so re-runs don't re-search fileless versions forever; only escalate empties, never successes, to avoid a fan-out storm. Cost in the observed run is ~14 extra calls.

Updates before uc2 gregory retry
- Attempted for 1h47m before killed session
- Now needing to update so header step save as it goes rather than all at the end
- Don't want to have this issue again
- Unrelated to esgf-west (index node search not institution data node connections)
Out of scope (flagging, not doing unless you want)

- Persisting parent links incrementally within a hop (currently hop-end) — small, separate.
- The dead-node tail itself (shorter read timeout / ignore list) — that's a config choice for the next run, independent of this change.❯ go ahead, implement it as specified, not the out of scope, I've got that in my notes. Although, the first out of scope is that related to parent use cases and this save on the go?

Good question — yes, that first out-of-scope item is specifically the parent-walk (uc2/Gregory) case, and here's how it relates:

In each hop the walk does three things in order: (1) add_files — already saves as-you-go; (2) enrich_version_headers — this change makes it save headers as-you-go; (3) after the hop's whole header batch finishes, it resolves declared parents, searches for them, and writes the child→parent links (parent_version_key).

So with this change, killing mid-hop preserves the expensive header reads, but the parent links for that hop are still written only at hop-end — that's the out-of-scope item. The reason it's low-cost to leave: links are cheap to regenerate. On a re-run, the now-cached headers are skipped (skip_cached), so the walk just re-does the fast parent search + linking. The 2-hour risk you're worried about is entirely in the header reads, and those are what we're protecting. Fully-incremental links would be a small separate follow-up (in your notes — good).

The extra time is retry × URL × timeout amplification on unreachable data nodes, and there's a specific classification gap driving it:

- with_retry is meant to skip stalls — its give_up_on list includes HeaderReadTimeout (our 90s subprocess kill). But curl's own connect timeout (~75s) fires first, and it surfaces as a plain OSError ("Failed to connect ... after 75003 ms"), which lands in retry_on=(OSError,). So a dead node is retried max_attempts=3 times, not given up.
- read_on_host tries each host's ranked URLs — the https twin + the http original = 2 URLs per host.
- So one dead host costs roughly 2 URLs × 3 attempts × ~75s ≈ 7.5 minutes per simulation, plus backoff sleeps.
- The circuit-breaker only evicts a node after 6 host-attempts (evict_after_attempts), and node_concurrency=2 throttles each dead host — so a node like CNRM takes ~6 × 7.5 min / 2 ≈ 20+ min just to get evicted.
- A simulation whose only mirrors are several dead EU nodes (CNRM → NSC → BSC) pays ~7.5 min per host before failing. ~30 such simulations across three concurrently-dead nodes stack up to ~2 hours.

The two stacked timeouts

Here's the crux of your question — there are two separate timeouts, at two different layers, and we only set one of them:

Timeout #1 — the OUTER one, which we DO set (90s).
with_timeout(read_header, seconds=90) runs the whole read_header call in a separate child process and, if that child hasn't returned after 90 seconds, kills the process (headers.py:28-31, 767). We have to do it this way because a stall isn't a Python error we can except — the netCDF call just sits there, so the only way to bound it is to kill the whole process from outside. When this fires, we raise HeaderReadTimeout.

Timeout #2 — the INNER one, which we do NOT set (~75s).
Down in libcurl/the OS, connecting to a host that never answers has its own timeout. When you try to open a TCP connection to a dead/firewalled node, your machine sends a SYN packet and waits for a reply. No reply → it retransmits, waits longer, retransmits again (exponential backoff)… and the operating system gives up on its own at about 75 seconds. On macOS that limit is literally net.inet.tcp.keepinit = 75000 ms. That's why every failure line in the log says the oddly-specific "after 75003 ms" — that's not a number we chose, it's the OS's built-in connect timeout leaking through.

So yes — to answer you directly: our 90s is in addition to, and wrapped around, curl's own ~75s connect timeout. They are independent. We never configured curl's connect timeout, so it falls back to the OS default of ~75s.


First, the terminology — you're right to flag it, because "child" means two completely different things here:

- "child dataset" / "parent dataset" = your science meaning. abrupt-4xCO2 is the child dataset, piControl is the parent dataset. I'll always say "child dataset" for this.
- "reader process" = an operating-system meaning. To read one file's header, the code launches a separate throwaway OS process to do that one network read. I'll call this a "reader process" (never "child process") so it doesn't collide with your dataset lineage. This has nothing to do with parent/child datasets — it's purely a computing mechanism.

Everything below is about reader processes, not datasets.

---
Why there are throwaway reader processes at all

Reading a header means opening a remote file over the network. Sometimes a data node accepts the connection and then just… hangs — sends nothing, forever. Python code stuck waiting for network bytes cannot be politely interrupted ("cancel" doesn't work on a frozen network read).

So the design does the only thing that reliably works: it runs each read in a separate throwaway OS process, and if that process takes too long, it force-kills the whole process from the outside — like Force-Quitting a frozen app. That's the with_timeout mechanism (now set to 25s).

The header step is organised like a small work crew:

   MANAGER (scheduler)  ── hands out read jobs ──►  a pool of ~12 WORKER THREADS
                                                      │      │            │
                                                      ▼      ▼            ▼
   each worker launches a throwaway  ───►         reader  reader  ...  reader
   READER PROCESS to do one network read          proc    proc         proc

- Healthy node: reader process returns the header in ~1s ✓ → the worker is free to grab the next job.
- Dead node: reader process hangs → after 25s the worker says "die" and force-kills it → then waits for it to finish dying → moves on.

---
The problem (the deadlock), visually

That last step — "wait for the killed reader process to finish dying" — has no time limit. Here's the normal vs. broken flow:

NORMAL kill:
  worker: "you're too slow" → SIGTERM → reader dies → worker waits (instant) → free ✓

BROKEN case (black-holed node):
  worker: SIGTERM → (ignored, reader is deep in a
  worker: SIGKILL "DIE NOW" → OS can't kill it yet (it's stuck mid-network-syscall)
  worker: wait for it to die ......... with NO ti

The subtlety: SIGKILL normally is instant, but when a reader process is stuck deep in the operating system waiting on network
bytes from a black-holed node, the OS won't actuak operation gives up — which can take a very longtime, or effectively never. So the worker thread that's "waiting for it to die" is stuck too.

One frozen worker isn't fatal — but there are only ~12. With several dead nodes in play (this cold run hit 3+ at once), worker after worker gets stuck on an un-killable reader process:

  worker1  🔒 (stuck on CNRM reader)
  worker2  🔒 (stuck on NSC reader)
  worker3  🔒 (stuck on BSC reader)
   ...
  worker12 🔒
        ▲
        └── MANAGER now has zero free workers, and nothing ever finishes
            → the whole header step freezes: 0% CPU, no progress, forever.  ← THE DEADLOCK

That's precisely what we saw: process alive, 0% CS "wait for process to die" call (waitpid), themanager blocked waiting for a worker to report back — none ever will.

---
Why uc1 (ssp245 / tas) never hit this

It comes down to which data nodes get touched — and it never got near the trap:

1. uc1's files live on healthy, fast nodes. Monthly tas for ssp245 is broadly mirrored (NCI, etc.). Reads connected and returned in ~1s, so a reader process almost never hung — the kill path was barely exercised, let alone the un-killable version of it.
2. uc2-Gregory drags in flaky nodes. The abrupt-4s like CNRM, EC-Earth (NSC), BSC live primarily onthose specific EU data nodes — exactly the ones that black-hole reads on a bad day. That's what produces un-killable reader processes.
3. The deadlock needs many stuck reads at once toarely had even one; uc2-Gregory on a cold run withseveral of those nodes down had dozens simultaneously — the perfect storm.

So it's not that uc1 was written differently — itprocess against a black-holing node, so it neversprang the trap.

---
The fix, and how big it is

The fix in one sentence: put a time limit on the "wait for the killed reader process to die" step; if it still won't die, stop
waiting for it — leave it as a harmless orphan fod janitor thread) to clean up later — mark the read as timed-out, and free the worker to move on.

BEFORE:  SIGKILL → wait forever for it to die
AFTER:   SIGKILL → wait ~2s → still alive? walk away,     ✓ worker free
                   hand it to a background janitor, move on

Analogy: right now, if you Force-Quit a frozen app and it doesn't close, the code stands there staring at it forever. The fix:  give it a couple seconds, then walk away and let  background — and get back to work. A single stuckreader process can then never freeze a worker, so the crew never grinds to a halt.
How intensive — will it change the workflow?
- Very small and very localized. It's essentially turning one unbounded "wait" into a bounded wait plus a "give up and detach"  path — on the order of ~10–20 lines, all inside t/with_timeout). Plus optionally a tiny backgroundreaper to mop up orphans.
- It does not change the workflow at all. Search → files → headers → parent-dataset resolution, the parallelism, save-as-you-go, and every result stay identical. Same headers read, same child→parent dataset links. It's purely a robustness patch to the kill mechanism — invisible to the rest of the pipeline
- Low risk. The only new behavior is: in the rare "won't die" case, we leave a short-lived orphan reader process (it's already a
daemon, so it dies with the run) instead of freez trade — a possible stray process vs. a total hang.
- The reproduction test I wrote flips from red to green with this change, so we'll have proof it's fixed.

### Week 3 (starting 27th July)

Claude prompt:
In uc2, we found some nodes that were dead, but allowed connection to try to read file headers. Firstly, could you please look into the database to find those dead nodes from the abrupt-4xCO2 experiment header read attempts? So the current set-up I want to implement is 90s stall on connecting to the data node. If the node connects and then stalls, then I want to implement a 25s stall. If that stall happens, we should just assume that the node is dead. We can try the http or https, but I do not then want to try otehr variables, or other files in the dataset. I want to implement quite a hard cut-off so that we don't waste time trying to connect to nodes that likely won't work. That will be for a user to decide if they want to try connecting to a dead node and waste a day attempting connection. Please let me know your plan. I want to still have the distinction between success and failiure and retries, for nodes that genuinely might work. For nodes we describe as dead for that query, we would retry them the next time someone attempts the same query.

┌────────────────────────────────────────────────────────────────┬─────────────────────────────────────────┬───────────────────────────────┐
│                       Connection failure                       │              Classified as              │           Retried?            │
├────────────────────────────────────────────────────────────────┼─────────────────────────────────────────┼───────────────────────────────┤
│ Connection refused (down but may recover / momentary overload) │ plain OSError                           │ ✅ Yes — full 3× with backoff │
├────────────────────────────────────────────────────────────────┼─────────────────────────────────────────┼───────────────────────────────┤
│ Connection reset / other transient OSError                     │ plain OSError                           │ ✅ Yes — 3×                   │
├────────────────────────────────────────────────────────────────┼─────────────────────────────────────────┼───────────────────────────────┤
│ Black-holed connect (no answer, hits 90s)                      │ HeaderReadHostFault                     │ ❌ No — one strike, next node │
├────────────────────────────────────────────────────────────────┼─────────────────────────────────────────┼───────────────────────────────┤
│ SSL / certificate error                                        │ HeaderReadHostFault                     │ ❌ No                         │
├────────────────────────────────────────────────────────────────┼─────────────────────────────────────────┼───────────────────────────────┤
│ DNS resolution failure                                         │ HeaderReadHostFault                     │ ❌ No                         │
├────────────────────────────────────────────────────────────────┼─────────────────────────────────────────┼───────────────────────────────┤
│ Connects, then read-stalls (25s)                               │ HeaderReadTimeout / HeaderReadHostFault │ ❌ No                         │
└────────────────────────────────────────────────────────────────┴─────────────────────────────────────────┴───────────────────────────────┘

### Day 2 (28/7)
 Earlier, we did a cold uc2 live run. This created a .json file which showed dead nodes. However, if you go to the .json file, you will see # working comments, because I tried the links and they successfully downloaded onto my computer. What could have caused this? why were they mis-identified as dead nodes? All the other ones seemed successfully dead. Please write a plan to investgiate this.

Byte range reader vs download
- Following connection with data node, can take up to 75s to access header (max stall time). We're conservative and just say if hits 75s we assume node is dead. To assess node health if we want to we can then investigate and download to see if truly dead. Or will just increase stall time.
┌────────────────────┬────────┬─────────────────┬────────────────────────────┬───────────────────────┐
│        Node        │  Size  │ Byte-range read │ Full download + local read │        Winner         │
├────────────────────┼────────┼─────────────────┼────────────────────────────┼───────────────────────┤
│ esg-dn1.nsc.liu.se │ 5.2 MB │ 58.8s           │ 27.8s (dl) + 0.0s = 27.8s  │ download, 2.1× faster │
├────────────────────┼────────┼─────────────────┼────────────────────────────┼───────────────────────┤
│ vesg.ipsl.upmc.fr  │ 46 MB  │ 39.8s           │ 10.1s (dl) + 0.0s = 10.1s  │ download, 3.9× faster │
└────────────────────┴────────┴─────────────────┴────────────────────────────┴───────────────────────┘
- THese were the two nodes that >25s original cut off, but download was super quick. Should be seen as 'healthy' though with raised to 75s

Parent/variable/hop information.
Could you please explore the parent hop / multi-hop system and variables. The ideal case would be to only search for (source, experiment, varaint) for parents, and not have to specify variables. However, if this is then implemented in the get file step, then this can lead to thousands of unecesssary file URLs being accessed. Please explore if there's a way for this step to work without having to specify the variable? Or could we search in an initial query then speicfy the variable for file URL? Please explain the current workflow (which may be just variable) and the potential debugging limitations of either not specifying variable, or specifying variable.

Following the >50k file access blowout in the g6solar multi-hop live test, I implemented a really strict 'only search for the required variables' in the parent step. So, this means that when parent information is obtained from a child experiment, we query (source, experiment,variant, variable) (step 1), then runs one file searh per dataset version (step 2), then reads one header per simulation (step 3).
It is logical that we could ease-up on Step 1, meaning that we only query the index node for (source, experiment, variant), however in the interests of time, for step 2 it feels like we should only search by variable (then step 3, obtain header information, only relies on a single simulation, regardless of the variable). This distinction would at least mean we will find in the search query stage "does the parent dataset exist for any variables", then step 2/3 ascertains are those variables available.
Would this workflow reduce/fix the bugs you're concerned about? Or would the ideal case (if time weren't an issue), be to also search for files of all variables in Step 2?

### Day 3 (29/7)
Claude prompt following ZN catch up:

I want to clarify the workflow of our set up please, and implement some changes before going to some live runs again. We have a number of different steps and a separation between simple use case (like ssp245 with no parent) compared to use cases with parent hops. Firstly, we consider a use case with no parent hops. For this case, we only need to implement step 1: dataset search. There is no need to get header information, so following the index node query for (variable, frequency), then we can stop the search there.
Then, we move to the next set of use cases which require parent information (which requires header information for cmip5/6, but not for cmip7, although we are only looking at cmip6 right now). We note that parent information may require one-to-multiple hops, but at some point the workflow repeats itself.
For the parent use case, we start with the initial child (as requested by the user, with one or multiple variables). We query ESGF index node and populate the relevant tables (such as dataset, for all requested varaible/s). Following this, (new step?) we want to check our local dataset/database for matching (source, experiment, variant) to see if there is already enriched metadata (i.e. header information). For example, if we already have searched for all tas abrupt-4xCO2 experiments then want to do another search for rsut, we would already have all the parent information for the (experiment, source, variant) because we would have found the header information based on tas already. In this case, we would not need to search for the header and could copy the information over. header information we say is independent for variable. If header metadata is not already available for a (experiment, source, variant), we move on to file/fileacess, which IS dependent on variable (i.e. dependent on what is in dataset), execute the query to obtain header information, saving results into the database. The header information, we only want to attempt access of one file for one variable for each (experiemnt, source, variant). We assume header information is the same across variables. Note here the steps which require specific variables, and the steps which are independent of variable. Use the header results to add the enriched metadata into the database.
Once we have the parent information, we either stop here (if only doing a single parent hop), or we use the parent information to create a new parent query in ESGF index node, dependent on the variable. We note as well that the parent and child may have different instituions, so shouldn't be dependent on institufion. Once we have queried ESGF, we also need to make sure that if we need to search for files and then headers, that if multiple children have the same parents, then we don't need to double up with file/header search. We then start form file/file access (per variable) then header (for a single file/variable), until we have reached the final parent specified by the user (in which case we don't need the header inforamtion).
A few additional notes: I want to maintain the workflow I have with workers and saving information at each step, plus not sending out file/header informatoin in batches, just one at a time. Secondly, this workflow may require some updates. Thirdly, some parent information won't be found in the ESGF search query. Here, we want a user to be able to override/fix if necessary to supply correct information. Fourthly, in the get header information step, we have a big workflow to determine is a node dead. We have a new method of potentially testing if a node is dead in addition to the current workflow, however maybe we make sure we are on the same page with the overall workflow before implementing that test? Please make a plan for how this description of the workflow is different to the current workflow and make a plan to update.

which requires header information for cmip5/6, but not for cmip7

Change to "which requires header information for cmip6, we don't know about cmip5 or cmip7 yet"

header information we say is independent for variable

We assume that this 'enriched' metadata about parent information is independent of variable. Please make this assumption clear in the docstring of the relevant function.

we move on to file/fileacess, which IS dependent on variable (i.e. dependent on what is in dataset)

we move on to file/fileacess, which we want to make variable specific so that we avoid getting 10 000 files results

Once we have the parent information, we either stop here (if only doing a single parent hop), or we use the parent information to create a new parent query in ESGF index node, dependent on the variable

Once we have the parent information we use the parent information to create a new parent query in ESGF index node, dependent on the variable

so shouldn't be dependent on institufion

so the generated query shouldn't include assumptions about the institution (i.e. any institution is ok)

We have a new method of potentially testing if a node is dead in addition to the current workflow, however maybe we make sure we are on the same page with the overall workflow before implementing that test?

I wouldn't mention that now. Let's just make it a completely separate, standalone task

serially (grouping on source/variant/experimt) -> multiple variables.

CLAUDE PROMPT Data node health.
 I would like to add a node-health test to my workflow pipeline. When we implement a search and populate the datanode, we obtain information on every potential data node for that search. If we need to do multiple parent hops (where we will need to connect to a node to access header information), I want to perform a test where, for every single node (can choose a random dataset to trial this on), we attempt to download a single chunk of data. If unsuccessful, consider this node dead for that session and put it in ignore_host. The user should be able to overrride this if they want, but otherwise assume it is dead. Otherwise, if a single chunk of data works for that node we should assume for that session it is alive. I would take recommendations on if we say a node is alive but still then 'fails' in header read (for example is our stall time too short, not enough retries, etc). Please make a plan of how this fits into our workflow, and how to implement this. Also offer your suggestions for if there are multiple parent hops and additional nodes become available through parents.
