"""
Route simulations to the data nodes that can serve their headers

The node-centric header step needs two derived views of a search result, both
computable *offline* from what the index already told us — no data-node contact:

- **per-simulation candidates** (`SimulationCandidates`): for one simulation, the
  mirror hosts that can serve its header, ranked best-first (preferred node, then
  learned health, then HTTPS — the same ordering `order_candidates` produces), each
  with the concrete URLs to read on that host;
- **the inversion** (`hosts_to_simulations`): `host -> [simulations it can serve]`,
  the per-node work queues the dispatcher drains.

A simulation only appears under a host if the index lists that host among its file
mirrors, so "does this node have this model's data?" is answered here, for free,
rather than by trying the node.

Grouping is a soft **affinity**: an `affinity_key` (default `source_id`) tags each
simulation, and a host's queue clusters simulations that share a tag so a worker
stays with, say, one model before moving on — matching how ESGF publishes a model's
datasets to one origin node.  The key never changes *which* node can serve a
simulation, only the *order* a node's queue is worked; a caller can group more
coarsely (e.g. by `experiment_id`) or not at all.
"""

from __future__ import annotations

from collections.abc import Callable, Hashable, Mapping, Sequence
from dataclasses import dataclass
from typing import TypeVar
from urllib.parse import urlparse

from cmip_data_manager.esgf.headers import (
    SimulationKey,
    candidate_urls_for_files,
)
from cmip_data_manager.esgf.models import DatasetRecord, FileRecord

FileKey = TypeVar("FileKey", bound=Hashable)
"""A download work-item key (whatever the caller chooses, e.g. a `File.id`)."""

AffinityKey = Callable[[DatasetRecord], Hashable]
"""
Tag a dataset with the group its simulation belongs to (for queue clustering)

Applied to a representative dataset of a simulation.  The default,
`source_id_affinity`, groups by model; a caller can pass `experiment_id`, a tuple
key, or a constant to disable clustering.
"""


def source_id_affinity(record: DatasetRecord) -> Hashable:
    """
    Group a simulation by its `source_id` (the default affinity)

    A model's simulations are usually published together on one origin node, so
    clustering a node's queue by `source_id` keeps a worker on one model at a time.

    Parameters
    ----------
    record
        A dataset of the simulation to tag.

    Returns
    -------
    :
        The dataset's `source_id`.
    """
    return record.source_id


@dataclass(frozen=True)
class SimulationCandidates:
    """
    The data nodes that can serve one simulation's header, ranked best-first

    One readable file is enough for a whole simulation (the header describes the
    run, not the variable), so `urls_by_host` keeps, per host, only that host's
    **one representative file** — its `https` twin and `http` original — and the
    dispatcher reads the first that works, falling through to the next node.  A dead
    node therefore costs at most those two attempts, not one per variable and chunk.
    """

    simulation: SimulationKey
    """The `(source_id, experiment_id, variant_label)` this is for."""

    group: Hashable
    """The affinity tag (e.g. `source_id`) used to cluster a node's queue."""

    hosts: tuple[str, ...]
    """The candidate hosts, best-first (empty if no mirror is usable)."""

    urls_by_host: Mapping[str, tuple[str, ...]]
    """Each host's readable URLs, best-first (keys match `hosts`)."""


@dataclass(frozen=True)
class FileCandidates:
    """
    The data nodes that can serve one file's bytes, ranked best-first

    The download counterpart of `SimulationCandidates`.  Where a header read needs only
    *one* readable file per simulation — so each host is collapsed to a single
    representative file — a download needs *this* file, so `urls_by_host[h]` keeps every
    mirror URL for this file on host `h` (its `https` twin + `http` original) and there
    is **no** cross-file collapse.  Structurally it matches `dispatch.NodeCandidates`,
    so the same dispatcher drives it.
    """

    key: Hashable
    """The work-item key this is for (e.g. a `File.id`)."""

    group: Hashable
    """The affinity tag (e.g. `source_id`) used to cluster a node's queue."""

    hosts: tuple[str, ...]
    """The candidate hosts, best-first (empty if no mirror is usable)."""

    urls_by_host: Mapping[str, tuple[str, ...]]
    """Each host's URLs for this file, best-first (keys match `hosts`)."""


def _url_basename(url: str) -> str:
    """Return a URL's filename (last path segment), ignoring query and fragment."""
    return urlparse(url).path.rsplit("/", 1)[-1]


def _one_file(urls: Sequence[str]) -> list[str]:
    """
    Keep only the URLs naming the same file as the first (best-ranked) one

    One header serves a whole simulation, so a single readable file is enough;
    restricting a host to one file's URLs (its `https` twin + `http` original)
    stops a dead node from being probed once per variable and per time-chunk.

    Parameters
    ----------
    urls
        A host's candidate URLs, best-first.

    Returns
    -------
    :
        The best-first subset sharing the first URL's basename (empty if `urls` is).

    Examples
    --------
    >>> _one_file(
    ...     [
    ...         "https://n/tas_2000.nc",
    ...         "http://n/tas_2000.nc",
    ...         "https://n/tas_2001.nc",
    ...         "https://n/pr_2000.nc",
    ...     ]
    ... )
    ['https://n/tas_2000.nc', 'http://n/tas_2000.nc']
    """
    if not urls:
        return []
    keep = _url_basename(urls[0])
    return [url for url in urls if _url_basename(url) == keep]


def simulation_candidates(  # noqa: PLR0913 - ordering controls, keyword-only, defaulted
    simulation: SimulationKey,
    files: Sequence[FileRecord],
    *,
    group: Hashable,
    preferred_hosts: Sequence[str] = (),
    ignore_hosts: frozenset[str] = frozenset(),
    host_rank: Callable[[str], tuple[float, float]] | None = None,
) -> SimulationCandidates:
    """
    Rank one simulation's mirror hosts and bucket its URLs by host

    Pools the `HTTPServer` mirrors of `files` (any variable, chunk or replica of the
    simulation) and orders them with `candidate_urls_for_files`, then groups the
    ranked URLs by host and collapses each host to its **one representative file**
    (its `https` twin + `http` original) — so `hosts` is the best-first host order
    and `urls_by_host[h]` is that host's two URLs for a single readable file.

    Parameters
    ----------
    simulation
        The simulation these files belong to.

    files
        The simulation's files (their `HTTPServer` URLs are pooled).

    group
        The affinity tag for this simulation (see `AffinityKey`).

    preferred_hosts, ignore_hosts, host_rank
        Ordering controls forwarded to `candidate_urls_for_files`.

    Returns
    -------
    :
        The ranked candidates for this simulation.

    Examples
    --------
    >>> from cmip_data_manager.esgf.models import FileRecord
    >>> files = [
    ...     FileRecord(
    ...         id="f1",
    ...         dataset_id="d1",
    ...         urls=("https://nci/f1.nc|application/netcdf|HTTPServer",),
    ...         raw={},
    ...     ),
    ...     FileRecord(
    ...         id="f2",
    ...         dataset_id="d1",
    ...         urls=("https://ornl/f2.nc|application/netcdf|HTTPServer",),
    ...         raw={},
    ...     ),
    ... ]
    >>> cand = simulation_candidates(
    ...     ("ACCESS-ESM1-5", "ssp245", "r1i1p1f1"),
    ...     files,
    ...     group="ACCESS-ESM1-5",
    ...     preferred_hosts=("nci",),
    ... )
    >>> cand.hosts
    ('nci', 'ornl')
    >>> cand.urls_by_host["nci"]
    ('https://nci/f1.nc',)
    """
    ranked = candidate_urls_for_files(
        files,
        preferred_hosts=preferred_hosts,
        ignore_hosts=ignore_hosts,
        host_rank=host_rank,
    )
    urls_by_host: dict[str, list[str]] = {}
    for url in ranked:
        host = urlparse(url).hostname or url
        urls_by_host.setdefault(host, []).append(url)
    # Collapse each host to a single representative file (its https twin + http
    # original).  One readable file answers the whole simulation, so trying every
    # variable and time-chunk on a node only multiplies wasted attempts on a dead
    # one — uc2's 142-URL BSC death spiral.  The list is already best-first, so its
    # first URL names the representative file; keep only URLs sharing that basename.
    collapsed = {host: _one_file(urls) for host, urls in urls_by_host.items()}
    return SimulationCandidates(
        simulation=simulation,
        group=group,
        hosts=tuple(collapsed),
        urls_by_host={host: tuple(urls) for host, urls in collapsed.items()},
    )


def build_candidates(  # noqa: PLR0913 - ordering controls, keyword-only, defaulted
    records_by_sim: Mapping[SimulationKey, Sequence[DatasetRecord]],
    files_by_sim: Mapping[SimulationKey, Sequence[FileRecord]],
    *,
    preferred_hosts: Sequence[str] = (),
    ignore_hosts: frozenset[str] = frozenset(),
    host_rank: Callable[[str], tuple[float, float]] | None = None,
    affinity_key: AffinityKey = source_id_affinity,
) -> dict[SimulationKey, SimulationCandidates]:
    """
    Build ranked candidates for every simulation to be read

    Turns the (network-fetched) per-simulation datasets and files into the offline
    routing view the dispatcher consumes.  A simulation with no usable mirror is
    still included with empty `hosts`, so the caller can record it as failed rather
    than lose it.

    Parameters
    ----------
    records_by_sim
        The datasets of each simulation (used only to derive its affinity tag).

    files_by_sim
        The files of each simulation (their mirrors are ranked).

    preferred_hosts, ignore_hosts, host_rank
        Ordering controls forwarded to `simulation_candidates`.

    affinity_key
        Tags each simulation for queue clustering; defaults to `source_id`.

    Returns
    -------
    :
        One `SimulationCandidates` per simulation in `records_by_sim`.
    """
    candidates: dict[SimulationKey, SimulationCandidates] = {}
    for simulation, records in records_by_sim.items():
        group = affinity_key(records[0]) if records else None
        candidates[simulation] = simulation_candidates(
            simulation,
            files_by_sim.get(simulation, ()),
            group=group,
            preferred_hosts=preferred_hosts,
            ignore_hosts=ignore_hosts,
            host_rank=host_rank,
        )
    return candidates


def build_file_candidates(
    files_by_key: Mapping[FileKey, FileRecord],
    *,
    group_by_key: Mapping[FileKey, Hashable] | None = None,
    preferred_hosts: Sequence[str] = (),
    ignore_hosts: frozenset[str] = frozenset(),
    host_rank: Callable[[str], tuple[float, float]] | None = None,
) -> dict[FileKey, FileCandidates]:
    """
    Build ranked download candidates for every file to be fetched

    The file-grain counterpart of `build_candidates`: one work item per *file* (keyed
    however the caller chooses, e.g. by `File.id`), each ranked across the data nodes
    that mirror it.  A file with no usable mirror is still included with empty `hosts`,
    so the caller can record it as failed rather than lose it.  Unlike the header
    builder there is **no** one-file-per-host collapse — every file must be downloaded.

    Parameters
    ----------
    files_by_key
        The file to download for each work-item key (its `HTTPServer` mirrors are
        pooled and ranked, with `https` twins added).

    group_by_key
        Optional per-key affinity tag (e.g. `source_id`) so a node's queue keeps one
        model's files together; keys absent here get no group.

    preferred_hosts, ignore_hosts, host_rank
        Ordering controls forwarded to `candidate_urls_for_files`.

    Returns
    -------
    :
        One `FileCandidates` per key in `files_by_key`.

    Examples
    --------
    >>> from cmip_data_manager.esgf.models import FileRecord
    >>> files = {
    ...     7: FileRecord(
    ...         id="f7",
    ...         dataset_id="d1",
    ...         urls=(
    ...             "https://nci/tas.nc|application/netcdf|HTTPServer",
    ...             "https://ornl/tas.nc|application/netcdf|HTTPServer",
    ...         ),
    ...         raw={},
    ...     )
    ... }
    >>> cands = build_file_candidates(files, preferred_hosts=("nci",))
    >>> cands[7].hosts
    ('nci', 'ornl')
    >>> cands[7].urls_by_host["nci"]
    ('https://nci/tas.nc',)
    """
    groups = dict(group_by_key or {})
    candidates: dict[FileKey, FileCandidates] = {}
    for key, file in files_by_key.items():
        ranked = candidate_urls_for_files(
            [file],
            preferred_hosts=preferred_hosts,
            ignore_hosts=ignore_hosts,
            host_rank=host_rank,
        )
        urls_by_host: dict[str, list[str]] = {}
        for url in ranked:
            host = urlparse(url).hostname or url
            urls_by_host.setdefault(host, []).append(url)
        candidates[key] = FileCandidates(
            key=key,
            group=groups.get(key),
            hosts=tuple(urls_by_host),
            urls_by_host={host: tuple(urls) for host, urls in urls_by_host.items()},
        )
    return candidates


def hosts_to_simulations(
    candidates: Mapping[SimulationKey, SimulationCandidates],
) -> dict[str, list[SimulationKey]]:
    """
    Invert candidates into each host's work queue, clustered by affinity

    For every host, lists the simulations it can serve, ordered so that simulations
    sharing an affinity tag are adjacent (groups appear in first-seen order, and
    simulations are sorted within a group for determinism).  A simulation appears
    under a host only if that host is among its candidate mirrors.

    Parameters
    ----------
    candidates
        The per-simulation candidates (e.g. from `build_candidates`).

    Returns
    -------
    :
        `host -> [simulations]`, each list clustered by affinity tag.  Hosts with no
        servable simulation do not appear.

    Examples
    --------
    >>> a = SimulationCandidates(("A", "ssp245", "r1"), "A", ("nci",), {"nci": ()})
    >>> b = SimulationCandidates(("B", "ssp245", "r1"), "B", ("nci",), {"nci": ()})
    >>> a2 = SimulationCandidates(("A", "hist", "r1"), "A", ("nci",), {"nci": ()})
    >>> queues = hosts_to_simulations(
    ...     {a.simulation: a, b.simulation: b, a2.simulation: a2}
    ... )
    >>> queues["nci"]  # the two "A" simulations cluster together
    [('A', 'hist', 'r1'), ('A', 'ssp245', 'r1'), ('B', 'ssp245', 'r1')]
    """
    by_host: dict[str, list[tuple[SimulationKey, Hashable]]] = {}
    for simulation in sorted(candidates):
        candidate = candidates[simulation]
        for host in candidate.hosts:
            by_host.setdefault(host, []).append((simulation, candidate.group))
    return {host: _cluster_by_group(pairs) for host, pairs in by_host.items()}


def _cluster_by_group(
    pairs: Sequence[tuple[SimulationKey, Hashable]],
) -> list[SimulationKey]:
    """Order simulations so shared-affinity ones are adjacent (first-seen groups)."""
    buckets: dict[Hashable, list[SimulationKey]] = {}
    for simulation, group in pairs:
        buckets.setdefault(group, []).append(simulation)
    return [simulation for group in buckets for simulation in buckets[group]]
