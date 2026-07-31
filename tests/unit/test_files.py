"""Tests for Step 2: per-version file search with endpoint fallback + save-as-you-go."""

from __future__ import annotations

import httpx
import pytest

from cmip_data_manager.esgf.client import DeepPaginationError
from cmip_data_manager.esgf.models import DatasetRecord, FileRecord
from cmip_data_manager.esgf.query import FacetQuery
from cmip_data_manager.search.files import (
    FileSearchIncompleteError,
    add_files,
)

_V = "v20240101"
WEST = "https://metagrid.esgf-west.org/proxy/search"
CEDA = "https://esgf.ceda.ac.uk/esg-search/search"


def _noop(_seconds: float) -> None:
    """A sleep that does not wait (so backoff retries are instant in tests)."""


def _rec(master, *, node="esgf.nci.org.au"):
    """A node-specific dataset record for `master` at the default version."""
    instance = f"{master}.{_V}"
    node_id = f"{instance}|{node}"
    return DatasetRecord(
        id=node_id,
        instance_id=instance,
        master_id=master,
        version=_V,
        data_node=node,
        source_id="M",
        experiment_id="ssp245",
        variant_label="r1",
        variable_id="tas",
        raw={"id": node_id},
    )


def _file(fid, dataset_id, filename, *, host="esgf.nci.org.au", scheme="https"):
    """A file record with a single HTTPServer URL on `host`."""
    url = f"{scheme}://{host}/thredds/fileServer/{filename}"
    return FileRecord(
        id=fid,
        dataset_id=dataset_id,
        title=filename,
        size=10,
        checksum="abc",
        checksum_type="SHA256",
        tracking_id="hdl:1",
        urls=(f"{url}|application/netcdf|HTTPServer",),
        raw={},
    )


def _http_500() -> httpx.HTTPStatusError:
    request = httpx.Request("GET", WEST)
    return httpx.HTTPStatusError(
        "500", request=request, response=httpx.Response(500, request=request)
    )


class _FakeClient:
    """A client with a `base_url` that returns canned files or raises on demand.

    - `raises`: an exception to raise instead of returning files;
    - `raise_first`: raise only for the first N calls, then succeed (None = always);
    - `fail_ids`: raise only when the query targets one of these dataset ids.
    """

    def __init__(  # noqa: PLR0913 - a test fake; each flag drives one add_files case
        self,
        files_by_dataset_id=None,
        *,
        base_url=WEST,
        raises=None,
        raise_first=None,
        fail_ids=None,
        supports_file_search=True,
    ):
        self._files = files_by_dataset_id or {}
        self._base_url = base_url
        self._raises = raises
        self._raise_first = raise_first
        self._fail_ids = fail_ids
        self.supports_file_search = supports_file_search
        self.queries: list[FacetQuery] = []
        self.calls = 0

    @property
    def base_url(self) -> str:
        return self._base_url

    def search_files(self, query: FacetQuery) -> list[FileRecord]:
        self.calls += 1
        self.queries.append(query)
        targeted = self._fail_ids is None or any(
            d in self._fail_ids for d in query.dataset_id
        )
        within = self._raise_first is None or self.calls <= self._raise_first
        if self._raises is not None and targeted and within:
            raise self._raises
        out: list[FileRecord] = []
        for dataset_id in query.dataset_id:
            out.extend(self._files.get(dataset_id, []))
        return out


def _store_versions(repository, records):
    """Persist the versions (Step 1) so Step 2's foreign keys are satisfied."""
    repository.record_run(records, endpoint_url="u", spec={}, tag="uc")


def test_one_search_per_version_stores_files_and_access(repository):
    a, b = _rec("a"), _rec("b")
    _store_versions(repository, [a, b])
    client = _FakeClient(
        {a.id: [_file("fa", a.id, "a.nc")], b.id: [_file("fb", b.id, "b.nc")]}
    )

    result = add_files([a, b], clients=[client], repository=repository)

    assert len(client.queries) == 2  # one search per version, not one batched query
    assert all(q.type == "File" for q in client.queries)
    assert result.searched == 2
    assert result.files_stored == 2
    assert not result.failed
    assert {q.dataset_id for q in client.queries} == {(a.id,), (b.id,)}


def test_client_that_cannot_file_search_is_skipped(repository):
    # A ranked, mixed-dialect list: an ESGF-NG client (no file search) leads a real
    # ESGF1 one.  add_files must skip the NG client, not call it, and let ESGF1 serve.
    a = _rec("a")
    _store_versions(repository, [a])
    ng = _FakeClient(
        base_url="https://search.east.esgf.io/search", supports_file_search=False
    )
    esgf1 = _FakeClient({a.id: [_file("f", a.id, "a.nc")]})

    result = add_files([a], clients=[ng, esgf1], repository=repository)

    assert ng.calls == 0  # the NG client was skipped, never searched
    assert esgf1.calls == 1
    assert result.files_stored == 1
    assert not result.failed


def test_replicas_of_a_file_collapse_to_one_file_many_accesses(repository):
    nci = _rec("a", node="esgf.nci.org.au")
    llnl = _rec("a", node="aims3.llnl.gov")
    _store_versions(repository, [nci, llnl])
    client = _FakeClient(
        {
            nci.id: [_file("f1", nci.id, "a.nc", host="esgf.nci.org.au")],
            llnl.id: [_file("f2", llnl.id, "a.nc", host="aims3.llnl.gov")],
        }
    )

    result = add_files([nci, llnl], clients=[client], repository=repository)

    assert len(client.queries) == 1  # one version (two nodes) -> one search
    assert result.files_stored == 1  # one logical file
    files = repository.get_version_files(nci.instance_key)
    assert {a.data_node for a in files[0].accesses} == {
        "esgf.nci.org.au",
        "aims3.llnl.gov",
    }


def test_http_only_mirror_gets_an_https_fsspec_url(repository):
    a = _rec("a")
    _store_versions(repository, [a])
    client = _FakeClient({a.id: [_file("f", a.id, "a.nc", scheme="http")]})

    add_files([a], clients=[client], repository=repository)

    (file,) = repository.get_version_files(a.instance_key)
    (access,) = file.accesses
    assert access.fsspec_url == access.url.replace("http://", "https://")


def test_already_cached_versions_are_skipped(repository):
    a = _rec("a")
    _store_versions(repository, [a])
    client = _FakeClient({a.id: [_file("f", a.id, "a.nc")]})

    first = add_files([a], clients=[client], repository=repository)
    second = add_files([a], clients=[client], repository=repository)

    assert first.searched == 1 and first.skipped_cached == 0
    assert second.searched == 0 and second.skipped_cached == 1
    assert len(client.queries) == 1  # the cached version was not re-searched


def test_files_for_an_unstored_version_are_skipped(repository):
    a = _rec("a")  # no Step 1 for this version
    client = _FakeClient({a.id: [_file("f", a.id, "a.nc")]})

    result = add_files([a], clients=[client], repository=repository)

    assert result.searched == 1
    assert result.files_stored == 0  # foreign key unsatisfiable -> skipped


def test_falls_back_to_second_endpoint_on_server_error(repository):
    a = _rec("a")
    _store_versions(repository, [a])
    west = _FakeClient(base_url=WEST, raises=_http_500())  # always 500
    ceda = _FakeClient({a.id: [_file("f", a.id, "a.nc")]}, base_url=CEDA)

    result = add_files(
        [a], clients=[west, ceda], repository=repository, retries=1, sleep=_noop
    )

    assert not result.failed
    assert result.files_stored == 1
    assert repository.version_has_files(a.instance_key)
    # health: west accrued server errors, CEDA a success
    health = repository.load_index_health()
    assert health.stat(WEST).server_errors >= 1
    assert health.stat(WEST).successes == 0
    assert health.stat(CEDA).successes == 1


def test_in_request_backoff_retries_then_succeeds(repository):
    a = _rec("a")
    _store_versions(repository, [a])
    # 500 on the first two calls, then succeed on the third (same endpoint).
    client = _FakeClient(
        {a.id: [_file("f", a.id, "a.nc")]}, raises=_http_500(), raise_first=2
    )

    result = add_files(
        [a], clients=[client], repository=repository, retries=3, sleep=_noop
    )

    assert not result.failed
    assert result.files_stored == 1
    assert client.calls == 3
    stat = repository.load_index_health().stat(WEST)
    assert stat.attempts == 3
    assert stat.successes == 1
    assert stat.failures == 2
    assert stat.retries == 2  # calls 2 and 3 were retries of the first


def test_requeue_gives_a_second_pass_on_the_same_endpoint(repository):
    a = _rec("a")
    _store_versions(repository, [a])
    # No in-request retries; fail the initial pass's one call, succeed on the requeue.
    client = _FakeClient(
        {a.id: [_file("f", a.id, "a.nc")]}, raises=_http_500(), raise_first=1
    )

    result = add_files(
        [a],
        clients=[client],
        repository=repository,
        retries=0,
        requeue_rounds=1,
        sleep=_noop,
    )

    assert not result.failed
    assert result.files_stored == 1
    assert client.calls == 2  # one failed initial pass + one successful requeue


def test_raises_when_all_endpoints_exhausted_but_persists_successes(repository):
    good, bad = _rec("good"), _rec("bad")
    _store_versions(repository, [good, bad])
    # One endpoint; it serves `good` but always 500s for `bad`.
    client = _FakeClient(
        {good.id: [_file("f", good.id, "good.nc")]},
        raises=_http_500(),
        fail_ids={bad.id},
    )

    with pytest.raises(FileSearchIncompleteError) as excinfo:
        add_files(
            [good, bad],
            clients=[client],
            repository=repository,
            retries=0,
            requeue_rounds=0,
            sleep=_noop,
        )

    error = excinfo.value
    assert error.result.failed == [bad.instance_key]
    assert error.endpoints == [WEST]
    # save-as-you-go: the good version is already cached despite the raise
    assert repository.version_has_files(good.instance_key)
    assert not repository.version_has_files(bad.instance_key)


def test_incomplete_can_be_returned_instead_of_raised(repository):
    bad = _rec("bad")
    _store_versions(repository, [bad])
    client = _FakeClient(base_url=WEST, raises=_http_500())

    result = add_files(
        [bad],
        clients=[client],
        repository=repository,
        retries=0,
        requeue_rounds=0,
        sleep=_noop,
        raise_on_incomplete=False,
    )

    assert result.failed == [bad.instance_key]
    assert result.files_stored == 0


def test_overflow_is_recorded_not_failed(repository):
    a = _rec("a")
    _store_versions(repository, [a])
    client = _FakeClient(base_url=WEST, raises=DeepPaginationError(20000, 10000))

    result = add_files(
        [a], clients=[client], repository=repository, retries=3, sleep=_noop
    )

    assert result.overflowed == [a.instance_key]
    assert not result.failed
    assert client.calls == 1  # overflow is not retried
    # overflow is a query-shape issue, not an endpoint fault: no health failure logged
    assert repository.load_index_health().stat(WEST) is None


def test_unexpected_worker_error_does_not_abort_the_pass(repository):
    good, bad = _rec("good"), _rec("bad")
    _store_versions(repository, [good, bad])
    # A non-HTTP error for `bad` must be caught (not propagated) so `good` still stores.
    client = _FakeClient(
        {good.id: [_file("f", good.id, "good.nc")]},
        raises=ValueError("boom"),
        fail_ids={bad.id},
    )

    with pytest.raises(FileSearchIncompleteError):
        add_files(
            [good, bad],
            clients=[client],
            repository=repository,
            retries=0,
            requeue_rounds=0,
            sleep=_noop,
        )

    assert repository.version_has_files(good.instance_key)


def test_empty_clients_is_rejected(repository):
    a = _rec("a")
    _store_versions(repository, [a])
    with pytest.raises(ValueError, match="at least one endpoint"):
        add_files([a], clients=[], repository=repository)


def test_health_can_be_supplied_and_is_accumulated(repository):
    a = _rec("a")
    _store_versions(repository, [a])
    client = _FakeClient({a.id: [_file("f", a.id, "a.nc")]})
    health = repository.load_index_health()

    add_files([a], clients=[client], repository=repository, health=health)

    # the supplied registry is updated in place, and the run also persisted it
    assert health.stat(WEST).successes == 1
    assert repository.load_index_health().stat(WEST).successes == 1


def test_logs_a_success_attempt_per_version(repository):
    a = _rec("a")
    _store_versions(repository, [a])
    client = _FakeClient({a.id: [_file("f", a.id, "a.nc")]})

    add_files([a], clients=[client], repository=repository)

    (attempt,) = repository.get_file_access_attempts()
    assert attempt.endpoint == WEST
    assert attempt.version_key == a.instance_key
    assert attempt.outcome == "success"
    assert attempt.files_found == 1
    assert attempt.attempt_no == 1


def test_logs_empty_distinctly_from_success(repository):
    a = _rec("a")
    _store_versions(repository, [a])
    client = _FakeClient({})  # HTTP 200, but the index knows no files for this version

    result = add_files([a], clients=[client], repository=repository)

    assert not result.failed  # an empty hit is a successful (if fruitless) search
    assert result.files_stored == 0
    (attempt,) = repository.get_file_access_attempts(outcome="empty")
    assert attempt.version_key == a.instance_key
    assert attempt.files_found == 0


def test_logs_every_attempt_across_retries_and_fallback(repository):
    a = _rec("a")
    _store_versions(repository, [a])
    west = _FakeClient(base_url=WEST, raises=_http_500())  # always 500
    ceda = _FakeClient({a.id: [_file("f", a.id, "a.nc")]}, base_url=CEDA)

    add_files(
        [a],
        clients=[west, ceda],
        repository=repository,
        retries=1,
        requeue_rounds=0,
        sleep=_noop,
    )

    attempts = repository.get_file_access_attempts(version_key=a.instance_key)
    # two failed calls on WEST (initial + one retry), then one success on CEDA
    west_attempts = [x for x in attempts if x.endpoint == WEST]
    ceda_attempts = [x for x in attempts if x.endpoint == CEDA]
    assert [x.outcome for x in west_attempts] == ["server_error", "server_error"]
    assert all(x.detail for x in west_attempts)  # the 500 text is captured
    assert [x.outcome for x in ceda_attempts] == ["success"]


def test_logs_an_error_attempt_when_a_worker_raises_unexpectedly(repository):
    bad = _rec("bad")
    _store_versions(repository, [bad])
    client = _FakeClient(base_url=WEST, raises=ValueError("boom"))

    add_files(
        [bad],
        clients=[client],
        repository=repository,
        retries=0,
        requeue_rounds=0,
        sleep=_noop,
        raise_on_incomplete=False,
    )

    (attempt,) = repository.get_file_access_attempts(version_key=bad.instance_key)
    assert attempt.outcome == "error"
    assert "boom" in attempt.detail
