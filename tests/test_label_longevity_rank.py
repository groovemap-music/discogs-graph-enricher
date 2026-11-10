"""The label-longevity rank phase: what it writes, in what batches, and how it is wired in.

The ranking's agreement with catalog-api's live statement is proven on real Neo4j in
tests/integration/test_real_neo4j_writes.py; these tests cover the Python side.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

import graphinator.graphinator as g
from graphinator import label_longevity_rank, maintenance_paging
from tests.fake_paged_graph import FakePagedGraph


def row(label_id: str, *, ranked: bool = True, stored: int | None = None) -> dict[str, Any]:
    return {"id": label_id, "ranked": ranked, "longevity_rank": stored}


@pytest.mark.asyncio
async def test_ranks_are_stream_positions_and_only_changes_are_written() -> None:
    graph = FakePagedGraph()
    graph.rank_rows = [
        row("10", stored=1),  # already right
        row("9", stored=1),  # moved down
        row("7"),  # new
        row("1818", ranked=False, stored=4),  # lost eligibility
    ]

    result = await label_longevity_rank.compute_label_longevity_ranks(graph)

    assert graph.rank_writes == [[{"id": "9", "rank": 2}, {"id": "7", "rank": 3}, {"id": "1818", "rank": None}]]
    assert result == label_longevity_rank.RankResult(ranked=3, written=3, batches=1)


@pytest.mark.asyncio
async def test_an_unchanged_ranking_writes_nothing() -> None:
    graph = FakePagedGraph()
    graph.rank_rows = [row(str(i), stored=i) for i in range(1, 6)]

    result = await label_longevity_rank.compute_label_longevity_ranks(graph)

    assert graph.rank_writes == []
    assert result == label_longevity_rank.RankResult(ranked=5, written=0, batches=0)


@pytest.mark.asyncio
async def test_changes_are_written_in_bounded_batches(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(label_longevity_rank, "RANK_WRITE_BATCH_SIZE", 2)
    graph = FakePagedGraph()
    graph.rank_rows = [row(f"L{i}") for i in range(5)]
    seen_before: list[int] = []

    async def before_batch() -> None:
        seen_before.append(len(graph.rank_writes))

    result = await label_longevity_rank.compute_label_longevity_ranks(graph, before_batch=before_batch)

    assert [len(batch) for batch in graph.rank_writes] == [2, 2, 1]
    assert [r["rank"] for batch in graph.rank_writes for r in batch] == [1, 2, 3, 4, 5]
    # Once before the read, then before each of the three batches.
    assert seen_before == [0, 0, 1, 2]
    assert result.batches == 3


@pytest.mark.asyncio
async def test_a_stop_between_batches_keeps_the_written_batches(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(label_longevity_rank, "RANK_WRITE_BATCH_SIZE", 1)
    graph = FakePagedGraph()
    graph.rank_rows = [row("a"), row("b"), row("c")]
    calls = {"n": 0}

    async def stop_on_third_check() -> None:
        calls["n"] += 1
        if calls["n"] == 3:
            raise maintenance_paging.MaintenanceStopped("a newer dump has arrived")

    with pytest.raises(maintenance_paging.MaintenanceStopped):
        await label_longevity_rank.compute_label_longevity_ranks(graph, before_batch=stop_on_third_check)
    assert graph.rank_writes == [[{"id": "a", "rank": 1}]]


@pytest.mark.asyncio
async def test_the_read_passes_catalog_apis_placeholder_rule_and_the_timeout_backstop(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(maintenance_paging.MAINTENANCE_TX_TIMEOUT_ENV, "42")
    graph = FakePagedGraph()
    graph.rank_rows = [row("a")]
    recorded: list[dict[str, Any]] = []
    real_run = graph.run

    async def run(query: Any, **params: Any) -> Any:
        recorded.append(params)
        return await real_run(query, **params)

    monkeypatch.setattr(graph, "run", run)
    await label_longevity_rank.compute_label_longevity_ranks(graph)

    assert recorded[0] == {
        "placeholder_ids": ["1818"],
        "placeholder_name": label_longevity_rank.PLACEHOLDER_LABEL_NAME,
        "real_lookalike_ids": ["109707", "2614996"],
    }
    assert graph.timeouts_seen == [42.0, 42.0]


def test_the_read_orders_like_catalog_api() -> None:
    cypher = label_longevity_rank.RANKED_LABELS_CYPHER
    assert "ORDER BY ranked DESC, l.years_active DESC, l.total_releases DESC, l.id\n" in cypher
    assert "l.name IS NOT NULL AND l.total_releases > 0" in cypher
    # Never a numeric id: catalog-api orders the stored string.
    assert "toInteger" not in cypher


@pytest.mark.asyncio
async def test_a_versioned_run_stores_m_on_its_dump() -> None:
    graph = FakePagedGraph()
    graph.rank_rows = [row("a"), row("b"), row("x", ranked=False, stored=9)]
    token = g._maintenance_version.set("20261101")
    try:
        with patch.object(g, "graph", graph):
            assert await g.compute_label_longevity_ranks() is True
    finally:
        g._maintenance_version.reset(token)
    assert graph.rank_of == {"20261101": 2}


@pytest.mark.asyncio
async def test_an_unversioned_run_ranks_but_has_no_dump_to_store_m_on() -> None:
    graph = FakePagedGraph()
    graph.rank_rows = [row("a")]
    with patch.object(g, "graph", graph):
        assert await g.compute_label_longevity_ranks() is True
    assert graph.rank_writes == [[{"id": "a", "rank": 1}]]
    assert graph.rank_of == {}


@pytest.mark.asyncio
async def test_no_driver_is_a_no_op() -> None:
    with patch.object(g, "graph", None):
        assert await g.compute_label_longevity_ranks() is True


@pytest.mark.asyncio
async def test_a_failed_rank_phase_fails_the_stats_step() -> None:
    graph = FakePagedGraph(labels=["1"])
    graph.rank_rows = [row("1")]
    graph.fail = lambda kind, _params: RuntimeError("connection lost") if kind == "rank_write" else None
    with patch.object(g, "graph", graph), patch.object(g, "MAINTENANCE_SETTLE_SECONDS", 0.0), patch.object(g, "logger"):
        assert await g.compute_genre_style_stats() is False


@pytest.mark.asyncio
async def test_a_missing_dump_node_fails_the_rank_phase() -> None:
    graph = FakePagedGraph()
    graph.rank_rows = [row("a")]
    graph.missing_dumps = {"gone"}
    token = g._maintenance_version.set("gone")
    try:
        with patch.object(g, "graph", graph), patch.object(g, "logger") as logger:
            assert await g.compute_label_longevity_ranks() is False
    finally:
        g._maintenance_version.reset(token)
    assert "No ExtractionCompletion node" in logger.error.call_args.kwargs["error"]


@pytest.mark.asyncio
async def test_storing_m_needs_a_driver() -> None:
    with patch.object(g, "graph", None), pytest.raises(RuntimeError):
        await g._store_label_longevity_rank_of("20261101", 1)


@pytest.mark.asyncio
async def test_a_stop_propagates_out_of_the_rank_phase() -> None:
    with (
        patch.object(g, "graph", FakePagedGraph()),
        patch.object(
            g.label_longevity_rank, "compute_label_longevity_ranks", AsyncMock(side_effect=maintenance_paging.MaintenanceStopped("new dump"))
        ),
        pytest.raises(maintenance_paging.MaintenanceStopped),
    ):
        await g.compute_label_longevity_ranks()


@pytest.mark.asyncio
async def test_a_failed_label_sweep_skips_the_ranking() -> None:
    graph = FakePagedGraph(labels=["1"])
    graph.rank_rows = [row("1")]
    graph.fail = lambda kind, _params: RuntimeError("connection lost") if kind == "label" else None
    with patch.object(g, "graph", graph), patch.object(g, "MAINTENANCE_SETTLE_SECONDS", 0.0), patch.object(g, "logger"):
        assert await g.compute_genre_style_stats() is False
    assert graph.rank_writes == []
    assert not any("AS ranked_count" in c for c in graph.statements)


@pytest.mark.asyncio
async def test_the_ranking_runs_after_the_last_label_page() -> None:
    graph = FakePagedGraph(labels=["1", "2"], genres=["Rock"], styles=["Punk"])
    graph.rank_rows = [row("2"), row("1")]
    with patch.object(g, "graph", graph), patch.object(g, "MAINTENANCE_SETTLE_SECONDS", 0.0):
        assert await g.compute_genre_style_stats() is True

    rank_read = next(i for i, c in enumerate(graph.statements) if "AS ranked_count" in c)
    last_label_page = max(i for i, c in enumerate(graph.statements) if "WITH l ORDER BY l.id LIMIT $size" in c)
    assert rank_read > last_label_page
    assert graph.rank_writes == [[{"id": "2", "rank": 1}, {"id": "1", "rank": 2}]]


@pytest.mark.asyncio
async def test_a_refresh_stores_m_before_it_stamps_the_stats_version() -> None:
    graph = FakePagedGraph(labels=["1"], genres=["Rock"], styles=["Punk"])
    graph.rank_rows = [row("1")]
    order: list[str] = []

    async def store(version: str, rank_of: int) -> None:
        order.append(f"rank_of={rank_of}")
        graph.rank_of[version] = rank_of

    async def stamp(version: str) -> None:
        order.append(f"stamp {version}")

    with (
        patch.object(g, "graph", graph),
        patch.object(g, "MAINTENANCE_SETTLE_SECONDS", 0.0),
        patch.object(g, "_store_label_longevity_rank_of", store),
        patch.object(g, "_stamp_stats_version", stamp),
        patch.object(g, "compute_artist_stats", AsyncMock(return_value=True)),
        patch.object(g, "_stop_if_superseded", lambda _version: None),
    ):
        assert await g.run_stats_refresh("20261101") is True
    assert order == ["rank_of=1", "stamp 20261101"]
    assert g._maintenance_version.get() is None
