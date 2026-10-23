"""Discogs writes ``<master_id>0</master_id>`` for every masterless release.

``discogs-ingestion`` passes it through as the string ``"0"``, which is truthy, so the
enricher used to MERGE one fake ``(:Master {id: "0"})`` that every masterless release
pointed at. Both projection paths must treat ``"0"``, ``0``, ``""`` and a missing id as no
master: no Master node, no DERIVED_FROM edge, and the stale-edge prune must remove an
edge a previous version of the release asserted.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

import graphinator.graphinator as g
from graphinator.batch_processor import Neo4jBatchProcessor, PendingMessage
from graphinator.entity_projection import process_release
from graphinator.graphinator import cleanup_sentinel_master
from graphinator.master_reference import usable_master_id


NO_MASTER_FORMS = ["0", 0, "", " 0 ", "  ", None]


class TestUsableMasterId:
    @pytest.mark.parametrize("value", [*NO_MASTER_FORMS, False, True, 0.0, [], {}])
    def test_no_master_forms_are_dropped(self, value: Any) -> None:
        assert usable_master_id(value) is None

    @pytest.mark.parametrize(("value", "expected"), [("998877", "998877"), (998877, "998877"), (" 12 ", "12"), ("M1", "M1"), ("10", "10")])
    def test_real_master_ids_are_kept_as_text(self, value: Any, expected: str) -> None:
        assert usable_master_id(value) == expected


def _single_tx() -> tuple[MagicMock, list[tuple[str, dict[str, Any]]]]:
    executed: list[tuple[str, dict[str, Any]]] = []

    async def track(query: str, **params: Any) -> MagicMock:
        executed.append((query, params))
        result = MagicMock()
        result.single = AsyncMock(return_value=None)
        return result

    tx = MagicMock()
    tx.run = AsyncMock(side_effect=track)
    return tx, executed


def _master_prune_keep(executed: list[tuple[str, dict[str, Any]]]) -> list[Any]:
    prunes = [params for query, params in executed if "DELETE rel" in query and "[rel:DERIVED_FROM]" in query]
    assert len(prunes) == 1
    return prunes[0]["keep"]


class TestEntityProjectionMasterSentinel:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("master_id", NO_MASTER_FORMS)
    async def test_sentinel_creates_no_master_and_prunes_the_edge(self, master_id: Any) -> None:
        tx, executed = _single_tx()

        assert await process_release(tx, {"id": "r1", "sha256": "h", "master_id": master_id}) is True

        assert not [q for q, _ in executed if "MERGE (m_r:Master" in q]
        assert _master_prune_keep(executed) == []

    @pytest.mark.asyncio
    async def test_missing_key_creates_no_master(self) -> None:
        tx, executed = _single_tx()

        await process_release(tx, {"id": "r1", "sha256": "h"})

        assert not [q for q, _ in executed if "MERGE (m_r:Master" in q]
        assert _master_prune_keep(executed) == []

    @pytest.mark.asyncio
    async def test_real_master_is_still_projected(self) -> None:
        tx, executed = _single_tx()

        await process_release(tx, {"id": "r1", "sha256": "h", "master_id": "998877"})

        merges = [params for q, params in executed if "MERGE (m_r:Master" in q]
        assert merges == [{"id": "r1", "m_id": "998877"}]
        assert _master_prune_keep(executed) == ["998877"]


class TestBatchProjectionMasterSentinel:
    @staticmethod
    def _tracking_driver() -> tuple[Any, list[tuple[str, dict[str, Any]]]]:
        executed: list[tuple[str, dict[str, Any]]] = []

        async def track(query: str, **params: Any) -> None:
            executed.append((query, params))

        tx = AsyncMock()
        tx.run.side_effect = track

        session = AsyncMock()
        result = MagicMock()
        result.__aiter__ = lambda self: self
        result.__anext__ = AsyncMock(side_effect=StopAsyncIteration)
        session.run = AsyncMock(return_value=result)

        async def execute_write(fn: Any) -> None:
            await fn(tx)

        session.execute_write = AsyncMock(side_effect=execute_write)

        context = AsyncMock()
        context.__aenter__.return_value = session
        context.__aexit__.return_value = None
        driver = MagicMock()
        driver.session = MagicMock(return_value=context)
        return driver, executed

    @staticmethod
    def _release(master_id: Any, *, include: bool = True) -> PendingMessage:
        data: dict[str, Any] = {"id": "1", "title": "R", "sha256": "newhash"}
        if include:
            data["master_id"] = master_id
        return PendingMessage("releases", data, AsyncMock(), AsyncMock())

    @pytest.mark.asyncio
    @pytest.mark.parametrize("master_id", NO_MASTER_FORMS)
    async def test_sentinel_creates_no_master_and_prunes_the_edge(self, master_id: Any) -> None:
        driver, executed = self._tracking_driver()

        await Neo4jBatchProcessor(driver)._process_releases_batch([self._release(master_id)])

        assert not [q for q, _ in executed if "MERGE (m:Master" in q]
        prunes = [p for q, p in executed if "DELETE rel" in q and "[rel:DERIVED_FROM]" in q]
        assert [p["records"] for p in prunes] == [[{"key": "1", "keep": []}]]

    @pytest.mark.asyncio
    async def test_missing_key_creates_no_master(self) -> None:
        driver, executed = self._tracking_driver()

        await Neo4jBatchProcessor(driver)._process_releases_batch([self._release(None, include=False)])

        assert not [q for q, _ in executed if "MERGE (m:Master" in q]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(("master_id", "expected"), [("998877", "998877"), (998877, "998877")])
    async def test_real_master_is_still_projected(self, master_id: Any, expected: str) -> None:
        driver, executed = self._tracking_driver()

        await Neo4jBatchProcessor(driver)._process_releases_batch([self._release(master_id)])

        merges = [p for q, p in executed if "MERGE (m:Master" in q]
        assert merges == [{"masters": [{"release_id": "1", "master_id": expected}]}]
        prunes = [p for q, p in executed if "DELETE rel" in q and "[rel:DERIVED_FROM]" in q]
        assert [p["records"] for p in prunes] == [[{"key": "1", "keep": [expected]}]]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("master_id", ["0", 0, ""])
    async def test_master_stream_nacks_a_sentinel_master_id(self, master_id: Any) -> None:
        driver, executed = self._tracking_driver()
        message = PendingMessage("masters", {"id": master_id, "title": "M", "sha256": "h"}, AsyncMock(), AsyncMock())

        nacked = await Neo4jBatchProcessor(driver)._process_masters_batch([message])

        assert nacked == {0}
        assert not [q for q, _ in executed if "MERGE (m:Master" in q]


class TestSentinelMasterCleanup:
    @staticmethod
    def _graph(fail: bool = False) -> tuple[MagicMock, list[str]]:
        run_calls: list[str] = []

        class _Result:
            async def consume(self) -> MagicMock:
                summary = MagicMock()
                summary.counters.nodes_deleted = 1
                summary.counters.relationships_deleted = 3
                return summary

        class _Session:
            async def __aenter__(self) -> _Session:
                return self

            async def __aexit__(self, *args: Any) -> None:
                return None

            async def run(self, cypher: str, *_args: Any, **_kwargs: Any) -> _Result:
                run_calls.append(cypher)
                if fail:
                    raise RuntimeError("boom")
                return _Result()

        graph = MagicMock()
        graph.session = MagicMock(side_effect=lambda **_: _Session())
        return graph, run_calls

    @pytest.mark.asyncio
    async def test_deletes_edges_in_chunks_then_the_node(self) -> None:
        graph, run_calls = self._graph()
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(g, "graph", graph)
            assert await cleanup_sentinel_master() is True

        assert len(run_calls) == 2
        edges, node = run_calls
        assert "Master {id: '0'}" in edges and "IN TRANSACTIONS OF" in edges and "DELETE rel" in edges
        assert "Master {id: '0'}" in node and "DETACH DELETE m" in node

    @pytest.mark.asyncio
    async def test_reports_failure(self) -> None:
        graph, _ = self._graph(fail=True)
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(g, "graph", graph)
            assert await cleanup_sentinel_master() is False

    @pytest.mark.asyncio
    async def test_noop_without_a_driver(self) -> None:
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(g, "graph", None)
            assert await cleanup_sentinel_master() is True
