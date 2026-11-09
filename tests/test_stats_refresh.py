"""The branches of the stats refresh. Its behaviour on real Neo4j is in tests/integration."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import graphinator.graphinator as g
from graphinator import maintenance_paging as mp
from tests.fake_paged_graph import FakePagedGraph


class _Session:
    """One-record stand-in for a driver session."""

    def __init__(self, record: dict[str, Any] | None) -> None:
        self.record = record
        self.cypher: list[str] = []

    async def __aenter__(self) -> _Session:
        return self

    async def __aexit__(self, *_args: Any) -> None:
        return None

    async def run(self, cypher: str, **_params: Any) -> MagicMock:
        self.cypher.append(cypher)
        result = MagicMock()
        result.single = AsyncMock(return_value=self.record)
        result.consume = AsyncMock()
        return result


def graph_returning(record: dict[str, Any] | None) -> MagicMock:
    graph = MagicMock()
    graph.session.return_value = _Session(record)
    return graph


@pytest.fixture(autouse=True)
def _idle_slots() -> Any:
    with (
        patch.object(g, "post_import_maintenance_task", None),
        patch.object(g, "stats_refresh_task", None),
        patch.object(g, "MAINTENANCE_SETTLE_SECONDS", 0.0),
        patch.object(g, "POST_IMPORT_MAINTENANCE_RETRY_DELAY_SECONDS", 0.0),
        patch.object(g, "message_counts", {"artists": 0, "labels": 0, "masters": 0, "releases": 0}),
    ):
        yield


def _finished_dump(*, signals: set[str] | None = None, maintained: bool = True) -> Any:
    return (
        patch.object(g, "_latest_extraction_version", AsyncMock(return_value="20261101")),
        patch.object(g, "_load_extraction_signals", AsyncMock(return_value=set(g.DATA_TYPES) if signals is None else signals)),
        patch.object(g, "_is_extraction_maintained", AsyncMock(return_value=maintained)),
    )


@pytest.mark.asyncio
async def test_a_dump_behind_the_stats_version_needs_a_refresh() -> None:
    latest, signals, maintained = _finished_dump()
    with patch.object(g, "graph", graph_returning({"stats_version": None})), latest, signals, maintained:
        assert await g._stats_refresh_version() == "20261101"


@pytest.mark.asyncio
async def test_a_current_dump_needs_no_refresh() -> None:
    latest, signals, maintained = _finished_dump()
    with patch.object(g, "graph", graph_returning({"stats_version": g.STATS_VERSION})), latest, signals, maintained:
        assert await g._stats_refresh_version() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("state", [{"signals": {"artists"}}, {"maintained": False}])
async def test_an_unfinished_dump_is_left_to_its_own_pass(state: dict[str, Any]) -> None:
    latest, signals, maintained = _finished_dump(**state)
    with patch.object(g, "graph", graph_returning({"stats_version": None})), latest, signals, maintained:
        assert await g._stats_refresh_version() is None


@pytest.mark.asyncio
async def test_no_driver_or_no_dump_needs_no_refresh() -> None:
    with patch.object(g, "graph", None):
        assert await g._stats_refresh_version() is None
    with patch.object(g, "graph", MagicMock()), patch.object(g, "_latest_extraction_version", AsyncMock(return_value=None)):
        assert await g._stats_refresh_version() is None


@pytest.mark.asyncio
async def test_resume_starts_a_refresh_only_when_one_is_needed() -> None:
    with patch.object(g, "_stats_refresh_version", AsyncMock(return_value=None)), patch.object(g, "start_stats_refresh") as start:
        assert await g.resume_stats_refresh() is False
    start.assert_not_called()
    with (
        patch.object(g, "_stats_refresh_version", AsyncMock(return_value="20261101")),
        patch.object(g, "start_stats_refresh", return_value=True) as start,
    ):
        assert await g.resume_stats_refresh() is True
    start.assert_called_once_with("20261101")


@pytest.mark.asyncio
async def test_a_second_refresh_is_refused_while_one_runs() -> None:
    release = asyncio.Event()

    async def blocked(_version: str) -> bool:
        await release.wait()
        return True

    with patch.object(g, "run_stats_refresh", blocked):
        assert g.start_stats_refresh("20261101") is True
        first = g.stats_refresh_task
        assert g.start_stats_refresh("20261101") is False
        assert g.stats_refresh_task is first
        release.set()
        assert await first is True


@pytest.mark.asyncio
async def test_a_maintenance_trigger_cancels_a_running_refresh() -> None:
    async def blocked(_version: str) -> bool:
        await asyncio.Event().wait()
        return True

    with patch.object(g, "run_stats_refresh", blocked), patch.object(g, "run_post_import_maintenance", AsyncMock(return_value=True)) as full_pass:
        g.start_stats_refresh("20261101")
        refresh = g.stats_refresh_task
        await asyncio.sleep(0)
        g.start_post_import_maintenance("20261201")
        assert await g.post_import_maintenance_task is True
    full_pass.assert_awaited_once_with("20261201")
    assert refresh.cancelled()


@pytest.mark.asyncio
async def test_the_guard_stops_on_an_arriving_message_without_asking_neo4j() -> None:
    latest = AsyncMock(return_value="20261101")
    with patch.object(g, "_latest_extraction_version", latest):
        guard = g._stop_if_superseded("20261101")
        g.message_counts["labels"] += 1
        with pytest.raises(mp.MaintenanceStopped, match="messages"):
            await guard()
    latest.assert_not_awaited()


@pytest.mark.asyncio
async def test_the_guard_stops_on_a_newer_dump_and_throttles_the_read() -> None:
    latest = AsyncMock(return_value="20261101")
    with patch.object(g, "_latest_extraction_version", latest):
        guard = g._stop_if_superseded("20261101")
        await guard()
        await guard()
        assert latest.await_count == 1, "the version read is throttled between pages"

    latest = AsyncMock(return_value="20261201")
    with patch.object(g, "_latest_extraction_version", latest):
        guard = g._stop_if_superseded("20261101")
        with pytest.raises(mp.MaintenanceStopped, match="20261201"):
            await guard()


@pytest.mark.asyncio
async def test_the_stats_stamp_needs_a_driver_and_a_node() -> None:
    with patch.object(g, "graph", None), pytest.raises(RuntimeError, match="not initialized"):
        await g._stamp_stats_version("20261101")
    with patch.object(g, "graph", graph_returning({"stamped": 0})), pytest.raises(RuntimeError, match="No ExtractionCompletion"):
        await g._stamp_stats_version("20261101")


@pytest.mark.asyncio
async def test_the_stats_stamp_never_touches_maintained_at() -> None:
    graph = graph_returning({"stamped": 1})
    with patch.object(g, "graph", graph):
        await g._stamp_stats_version("20261101")
    assert "maintained_at" not in graph.session.return_value.cypher[0]


def _sweeps(*, label: Any = None, artist: Any = None) -> tuple[Any, Any, AsyncMock]:
    """Stand in for the two refresh steps: Genre/Style/Label stats (``label``) and Artist stats."""
    stamp = AsyncMock()
    return (
        patch.object(g, "compute_genre_style_stats", label or AsyncMock(return_value=True)),
        patch.object(g, "compute_artist_stats", artist or AsyncMock(return_value=True)),
        stamp,
    )


@pytest.mark.asyncio
async def test_a_refresh_stamps_only_after_both_sweeps() -> None:
    label, artist, stamp = _sweeps()
    with label, artist, patch.object(g, "_stamp_stats_version", stamp):
        assert await g.run_stats_refresh("20261101") is True
    stamp.assert_awaited_once_with("20261101")


@pytest.mark.asyncio
async def test_a_refresh_sets_its_run_key_and_guard_only_while_it_runs() -> None:
    seen: dict[str, Any] = {}

    async def record() -> bool:
        seen.update(key=g._maintenance_run_key.get(), guard=g._maintenance_before_page.get())
        return True

    label, artist, stamp = _sweeps(label=record)
    with label, artist, patch.object(g, "_stamp_stats_version", stamp):
        await g.run_stats_refresh("20261101")
    assert seen["key"] == g.maintenance_run_key("20261101")
    assert seen["guard"] is not None
    assert g._maintenance_run_key.get() is None
    assert g._maintenance_before_page.get() is None


@pytest.mark.asyncio
async def test_a_failed_sweep_is_retried_then_left_unstamped() -> None:
    failing = AsyncMock(return_value=False)
    label, artist, stamp = _sweeps(artist=failing)
    with label, artist, patch.object(g, "_stamp_stats_version", stamp), patch.object(g, "logger"):
        assert await g.run_stats_refresh("20261101") is False
    assert failing.await_count == g.POST_IMPORT_MAINTENANCE_MAX_ATTEMPTS
    stamp.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_sweep_that_recovers_on_a_retry_is_stamped() -> None:
    label, artist, stamp = _sweeps(artist=AsyncMock(side_effect=[False, True]))
    with label, artist, patch.object(g, "_stamp_stats_version", stamp), patch.object(g, "logger"):
        assert await g.run_stats_refresh("20261101") is True
    stamp.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_stopped_refresh_ends_at_once_without_a_retry() -> None:
    stopped = AsyncMock(side_effect=mp.MaintenanceStopped("a newer dump has arrived"))
    label, artist, stamp = _sweeps(label=stopped)
    with label, artist, patch.object(g, "_stamp_stats_version", stamp):
        assert await g.run_stats_refresh("20261101") is False
    assert stopped.await_count == 1
    stamp.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_stamp_that_cannot_be_written_reports_failure() -> None:
    label, artist, _ = _sweeps()
    with label, artist, patch.object(g, "_stamp_stats_version", AsyncMock(side_effect=RuntimeError("down"))), patch.object(g, "logger"):
        assert await g.run_stats_refresh("20261101") is False


@pytest.mark.asyncio
async def test_a_refresh_writes_the_genre_and_style_series_and_every_other_stat() -> None:
    """STATS_VERSION 2 added the year series on Genre/Style, so a refresh has to sweep them too."""
    graph = FakePagedGraph(labels=["L1", "L2"], genres=["Rock"], styles=["Punk"], artists=["1"])
    stamp = AsyncMock()
    with patch.object(g, "graph", graph), patch.object(g, "_stamp_stats_version", stamp):
        assert await g.run_stats_refresh("20261101") is True
    stamp.assert_awaited_once_with("20261101")
    for key in ("Rock", "Punk"):
        assert {"release_years", "release_year_counts", "first_year"} <= {p for k, p in graph.metric_sets if k == key}
    assert graph.label_pages[0] == ["L1", "L2"]
    assert graph.artist_pages[0] == ["1"]
    assert set(graph.checkpoints) == {"genre_stats", "style_stats", "label_stats", "artist_stats"}


@pytest.mark.asyncio
async def test_a_stop_is_not_swallowed_as_a_failed_sweep() -> None:
    async def stop() -> None:
        raise mp.MaintenanceStopped("catalog messages are arriving")

    token = g._maintenance_before_page.set(stop)
    try:
        with patch.object(g, "graph", FakePagedGraph(artists=["1"], labels=["L1"])):
            with pytest.raises(mp.MaintenanceStopped):
                await g.compute_artist_stats()
            with pytest.raises(mp.MaintenanceStopped):
                await g.compute_genre_style_stats()
            with pytest.raises(mp.MaintenanceStopped):
                await g._run_maintenance_step("artist_stats", g.compute_artist_stats)
    finally:
        g._maintenance_before_page.reset(token)
