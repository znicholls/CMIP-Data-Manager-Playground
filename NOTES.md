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
