"""Tests for the durable ExtractionCompletion.maintained_at signal.

Downstream gates need "the graph is final for this dump". The fourth
extraction_complete is acked before the detached maintenance runs, so the stamp is
the only durable record that the pass finished. It is written as the pass's last
step, a failed pass records maintenance_error instead, and a restart resumes an
unfinished pass. Each version's node is independent.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aio_pika.abc import AbstractIncomingMessage

import graphinator.graphinator as g


class FakeNodes:
    """In-memory stand-in for the :ExtractionCompletion nodes, one per version."""

    def __init__(self, nodes: dict[str, dict[str, Any]] | None = None) -> None:
        self.nodes = nodes or {}

    async def load(self, version: str) -> set[str]:
        return set(self.nodes.get(version, {}).get("signals", set()))

    async def persist(self, version: str, signals: set[str]) -> None:
        self.nodes.setdefault(version, {}).setdefault("signals", set()).update(signals)

    async def is_maintained(self, version: str) -> bool:
        return "maintained_at" in self.nodes.get(version, {})

    async def latest(self) -> str | None:
        return list(self.nodes)[-1] if self.nodes else None

    async def stamp(self, version: str) -> None:
        node = self.nodes[version]
        node.setdefault("maintained_at", "t0")
        node.pop("maintenance_error", None)

    async def fail(self, version: str, error: str) -> None:
        node = self.nodes[version]
        if "maintained_at" not in node:
            node["maintenance_error"] = error


def full(**extra: Any) -> dict[str, Any]:
    return {"signals": set(g.DATA_TYPES), **extra}


@pytest.fixture
def store(monkeypatch: pytest.MonkeyPatch) -> FakeNodes:
    fake = FakeNodes()
    monkeypatch.setattr(g, "_load_extraction_signals", fake.load)
    monkeypatch.setattr(g, "_persist_extraction_signals", fake.persist)
    monkeypatch.setattr(g, "_is_extraction_maintained", fake.is_maintained)
    monkeypatch.setattr(g, "_latest_extraction_version", fake.latest)
    monkeypatch.setattr(g, "_stamp_maintenance_success", fake.stamp)
    monkeypatch.setattr(g, "_record_maintenance_failure", fake.fail)
    monkeypatch.setattr(g, "graph", AsyncMock())
    monkeypatch.setattr(g, "batch_processor", None)
    monkeypatch.setattr(g, "POST_IMPORT_MAINTENANCE_RETRY_DELAY_SECONDS", 0.0)
    monkeypatch.setattr(g, "extraction_complete_signals", set())
    monkeypatch.setattr(g, "extraction_complete_version", None)
    return fake


def steps(monkeypatch: pytest.MonkeyPatch, *, stats_ok: Any = True) -> AsyncMock:
    monkeypatch.setattr(g, "cleanup_all_stub_nodes", AsyncMock(return_value=True))
    monkeypatch.setattr(g, "cleanup_sentinel_master", AsyncMock(return_value=True))
    stats = AsyncMock(side_effect=stats_ok) if isinstance(stats_ok, BaseException) else AsyncMock(return_value=stats_ok)
    monkeypatch.setattr(g, "compute_genre_style_stats", stats)
    monkeypatch.setattr(g, "compute_person_credit_counts", AsyncMock(return_value=True))
    return stats


@pytest.mark.asyncio
async def test_success_stamps_maintained_at_after_all_four_signals(store: FakeNodes, monkeypatch: pytest.MonkeyPatch) -> None:
    steps(monkeypatch)
    started: list[Any] = []
    monkeypatch.setattr(g, "start_post_import_maintenance", lambda version=None: started.append(version))

    for data_type in g.DATA_TYPES:
        message = AsyncMock(spec=AbstractIncomingMessage)
        await g.check_file_completion({"type": "extraction_complete", "version": "v1"}, data_type, message)
        message.ack.assert_awaited_once()
    assert started == ["v1"]
    assert "maintained_at" not in store.nodes["v1"]

    assert await g.run_post_import_maintenance("v1") is True
    assert "maintained_at" in store.nodes["v1"]
    assert "maintenance_error" not in store.nodes["v1"]


@pytest.mark.asyncio
async def test_stamp_is_written_after_every_sweep(store: FakeNodes, monkeypatch: pytest.MonkeyPatch) -> None:
    store.nodes["v1"] = full()
    seen: list[bool] = []

    async def stats() -> bool:
        seen.append("maintained_at" in store.nodes["v1"])
        return True

    steps(monkeypatch)
    monkeypatch.setattr(g, "compute_genre_style_stats", stats)
    assert await g.run_post_import_maintenance("v1") is True
    assert seen == [False]
    assert "maintained_at" in store.nodes["v1"]


@pytest.mark.asyncio
async def test_failure_records_error_and_no_stamp(store: FakeNodes, monkeypatch: pytest.MonkeyPatch) -> None:
    store.nodes["v1"] = full()
    steps(monkeypatch, stats_ok=RuntimeError("boom"))

    assert await g.run_post_import_maintenance("v1") is False
    assert "maintained_at" not in store.nodes["v1"]
    assert "boom" in store.nodes["v1"]["maintenance_error"]


@pytest.mark.asyncio
async def test_unwritable_stamp_fails_the_attempt_and_retries(store: FakeNodes, monkeypatch: pytest.MonkeyPatch) -> None:
    store.nodes["v1"] = full()
    steps(monkeypatch)
    calls = {"n": 0}

    async def flaky(version: str) -> None:
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("neo4j unavailable")
        await store.stamp(version)

    monkeypatch.setattr(g, "_stamp_maintenance_success", flaky)
    assert await g.run_post_import_maintenance("v1") is True
    assert calls["n"] == 2
    assert "maintained_at" in store.nodes["v1"]


@pytest.mark.asyncio
async def test_later_success_clears_earlier_error(store: FakeNodes, monkeypatch: pytest.MonkeyPatch) -> None:
    store.nodes["v1"] = full(maintenance_error="old")
    steps(monkeypatch)
    assert await g.run_post_import_maintenance("v1") is True
    assert "maintenance_error" not in store.nodes["v1"]
    assert "maintained_at" in store.nodes["v1"]


@pytest.mark.asyncio
async def test_restart_mid_maintenance_resumes_and_stamps(store: FakeNodes, monkeypatch: pytest.MonkeyPatch) -> None:
    """The trigger was acked, then the process died: only the node remembers."""
    store.nodes["v1"] = full()
    steps(monkeypatch)
    ran: list[bool] = []

    def start(version: str | None = None) -> None:
        ran.append(True)
        assert version == "v1"

    monkeypatch.setattr(g, "start_post_import_maintenance", start)
    assert await g.resume_pending_maintenance() is True
    assert ran == [True]

    assert await g.run_post_import_maintenance("v1") is True
    assert "maintained_at" in store.nodes["v1"]
    # A second restart finds nothing left to do.
    assert await g.resume_pending_maintenance() is False


@pytest.mark.asyncio
async def test_resume_retries_a_previously_failed_pass(store: FakeNodes, monkeypatch: pytest.MonkeyPatch) -> None:
    store.nodes["v1"] = full(maintenance_error="boom")
    start = MagicMock()
    monkeypatch.setattr(g, "start_post_import_maintenance", start)
    assert await g.resume_pending_maintenance() is True
    start.assert_called_once_with("v1")


@pytest.mark.asyncio
async def test_resume_ignores_incomplete_latch_and_stamped_nodes(store: FakeNodes, monkeypatch: pytest.MonkeyPatch) -> None:
    start = MagicMock()
    monkeypatch.setattr(g, "start_post_import_maintenance", start)
    assert await g.resume_pending_maintenance() is False  # no nodes at all

    store.nodes["v2"] = {"signals": {"artists"}}
    assert await g.resume_pending_maintenance() is False  # still importing

    store.nodes["v2"] = full(maintained_at="t")
    assert await g.resume_pending_maintenance() is False  # done
    start.assert_not_called()


@pytest.mark.asyncio
async def test_versions_are_independent(store: FakeNodes, monkeypatch: pytest.MonkeyPatch) -> None:
    """An earlier dump's stamp never satisfies, or is disturbed by, a later dump."""
    store.nodes["v1"] = full(maintained_at="t1")
    store.nodes["v2"] = full()
    steps(monkeypatch, stats_ok=False)

    assert await g.run_post_import_maintenance("v2") is False
    assert store.nodes["v1"] == full(maintained_at="t1")
    assert "maintained_at" not in store.nodes["v2"]
    assert "maintenance_error" in store.nodes["v2"]

    start = MagicMock()
    monkeypatch.setattr(g, "start_post_import_maintenance", start)
    assert await g.resume_pending_maintenance() is True
    start.assert_called_once_with("v2")


@pytest.mark.asyncio
async def test_failure_cannot_unstamp_a_finished_version(store: FakeNodes) -> None:
    store.nodes["v1"] = full(maintained_at="t1")
    await store.fail("v1", "late failure")
    assert "maintenance_error" not in store.nodes["v1"]
    assert store.nodes["v1"]["maintained_at"] == "t1"


@pytest.mark.asyncio
async def test_redelivered_final_signal_skips_finished_maintenance(store: FakeNodes, monkeypatch: pytest.MonkeyPatch) -> None:
    store.nodes["v1"] = full(maintained_at="t1")
    start = MagicMock()
    monkeypatch.setattr(g, "start_post_import_maintenance", start)
    message = AsyncMock(spec=AbstractIncomingMessage)

    await g.check_file_completion({"type": "extraction_complete", "version": "v1"}, "releases", message)

    start.assert_not_called()
    message.ack.assert_awaited_once()


@pytest.mark.asyncio
async def test_unreadable_maintenance_state_requeues_final_signal(store: FakeNodes, monkeypatch: pytest.MonkeyPatch) -> None:
    store.nodes["v1"] = full()
    monkeypatch.setattr(g, "_is_extraction_maintained", AsyncMock(side_effect=RuntimeError("down")))
    start = MagicMock()
    monkeypatch.setattr(g, "start_post_import_maintenance", start)
    message = AsyncMock(spec=AbstractIncomingMessage)

    await g.check_file_completion({"type": "extraction_complete", "version": "v1"}, "releases", message)

    message.nack.assert_awaited_once_with(requeue=True)
    message.ack.assert_not_awaited()
    start.assert_not_called()


@pytest.mark.asyncio
async def test_shutdown_records_neither_stamp_nor_error(store: FakeNodes, monkeypatch: pytest.MonkeyPatch) -> None:
    store.nodes["v1"] = full()
    steps(monkeypatch)
    with patch.object(g, "shutdown_requested", True):
        assert await g.run_post_import_maintenance("v1") is False
    assert "maintained_at" not in store.nodes["v1"]
    assert "maintenance_error" not in store.nodes["v1"]
