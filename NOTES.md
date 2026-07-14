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
