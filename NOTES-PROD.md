## Search API

```python
class SearchResult:
    successes: tuple[SuccessfulSearchResult, ...]
    failures: tuple[FailedSearchResult, ...]

def search(
    searches: ESGFQuery | Iterable[ESGFQuery],
    search_config: SearchConfig,  # contains both parallel stuff like number of threads as well as preferences of search APIs to hit (include retry settings for each search API)
    db parameters,
) -> SearchResult:
    # Assumption: if you are doing a search, you want to hit the ESGF API.
    # No local caching of searches.
    #
    # Needs to work for both datasets and files
    put searches in a queue
    run searches in parallel using search_single, using config to control parallelism etc.
    save results to database within the parallel function (leave database manager to handle writing from multiple threads/processes; both retrieved Dataset or File entries and search results)
    OR get results in the main process as we go and save them in the main process as they are received
    compile results

    return

# Let claude write stuff in the middle

class SearchResultSingle:
    success: SuccessfulSearchResult | None
    failure: FailedSearchResult | None

    validate that one of success or failure is not None (but not both, and not neither)

def search_single(
    search: ESGFQuery,
    API/URL parameters,  # i.e. which URL to hit
    retry/backoff config/httpx or requests object to use for the search,
    db parameters
) -> SearchResultSingle:
    # Split this first bit out into yet another function,
    # which just does the search
    Hit the API
    If successful result
    If failure, let retry do its thing
    If still failing after retries, return as failure

    # Split out a function which stores search result in database as well as file/dataset entries if successful (we are ok having results potentially fall through the cracks between the 'pure search' function and database saving)

    return

def search_result_diff(
    new: ESGFQuery,
    base: ESGFQuery,
    db parameters
) -> SearchResultDiff:
    # Bottom layer
    Get results for new from database
    Get results for base from database

    Compile into result
    New results
    Results which have been changed - key by result, then just have dict of changes (entry: (old, new))
    Missing results (shouldn't be possible, but have a space just in case)
    Put a to_dataframe method on SearchResultDiff to allow for dropping this out to pandas as that is often more convenient

    return result


def get_search_result_diff_since(
    searches: ESGFQuery | Iterable[ESGFQuery],
    since: str | None = None,  # if provided, must be a date. Otherwise, just do since previous search
    db parameters
) -> SearchResult:
    for search in searches:
        # Break this block out into its own function,
        # previous_search = get_previous_search(on_or_before=since)
        # If no previous, raise NoPreviousSearchError in get_previous_search
        # (higher layers can use try except to catch this error if they want)
        if since is None:
            get previous search result
            if no previous search result, raise
            use this previous result for the diff

        else:
            parse since into a date
            get the latest search that was performed on or before this date
            if no search was performed on or before this date, raise
            use this latest search for the diff

        use search_result_diff to get the difference for this search
        store (for later compilation into a result)

    Compile the store into a SearchResultDiff
    (not sure how complicated this could be, e.g. contradictory diffs
    from overlapping searches...)

    return result
```

- Test that all this works both dataset and file searches

## Add files API

```python
class FileAdditionResult:
    successes: tuple[SuccessfulFileAdditionResult, ...]
    failures: tuple[FailedFileAdditionResult, ...]
    cached: tuple[Dataset, ...]  # datasets that we already had files for

def add_files_to_datasets(
    datasets: Iterable[Dataset],
    db params,
    search_config: SearchConfig,
) -> FileAdditionResult:
    put datasets in a queue
    run file retrieval in parallel using add_files_to_dataset, using config to control parallelisation and search URL preferencing options
    save results to database within the parallel function (both retrieved File entries and search results)
    compile results

    return


class FileAdditionResultSingle:
    success: SuccessfulFileAdditionResult | None
    failure: FailedFileAdditionResult | None

    validate that one of success or failure is not None (but not both, and not neither)


def add_files_to_dataset(
    datasets: Dataset,
    API/URL parameters,  # i.e. which URL to hit
    retry/backoff config/httpx or requests object to use for the search,
    db params,
) -> FileAdditionResultSingle:
    convert dataset to a file query
    then just use search_single
    have to bubble up success/failure and other errors

    return


We will also want a helper like the below (useful because you can use the same query object to drive search and load datasets that match from our local cache)

def get_datasets_matching_searches(
    searches: ESGFQuery | Iterable[ESGFQuery],
    db parameters
) -> tuple[Dataset, ...]:
    go through all the searches
    get all datasets in our database that match the search query

    return the datasets
```

## Enrich with header information API

```python

```
