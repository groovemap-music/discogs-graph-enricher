"""Pytest configuration for graphinator tests."""

from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aio_pika.abc import AbstractChannel, AbstractConnection, AbstractQueue


@pytest.fixture
def mock_amqp_connection() -> AsyncMock:
    """Return a connection with asynchronous channel and queue boundaries."""
    connection = AsyncMock(spec=AbstractConnection)
    channel = AsyncMock(spec=AbstractChannel)
    queue = AsyncMock(spec=AbstractQueue)
    connection.channel.return_value = channel
    channel.declare_queue.return_value = queue
    channel.declare_exchange.return_value = AsyncMock()
    return connection


@pytest.fixture
def mock_neo4j_driver() -> MagicMock:
    """Return a driver whose session follows the runtime async context contract."""
    driver = MagicMock()
    session = AsyncMock()
    context = AsyncMock()
    context.__aenter__ = AsyncMock(return_value=session)
    context.__aexit__ = AsyncMock(return_value=None)
    driver.session = MagicMock(return_value=context)
    session.execute_write = AsyncMock(return_value=True)
    session.run.return_value.single.return_value = None
    session.close = AsyncMock()
    driver.close = AsyncMock()
    return driver


@pytest.fixture
def temp_dir(tmp_path: Path) -> Path:
    """Return an isolated filesystem location."""
    return tmp_path


@pytest.fixture
def sample_artist_data() -> dict[str, Any]:
    """Return representative Discogs artist data."""
    return {
        "id": "123456",
        "name": "Test Artist",
        "sha256": "abc123def456",
        "members": [{"id": "234567", "name": "Member 1"}, {"id": "345678", "name": "Member 2"}],
        "aliases": [{"id": "456789", "name": "Alias 1"}],
    }


@pytest.fixture
def sample_label_data() -> dict[str, Any]:
    """Return representative Discogs label data."""
    return {
        "id": "987654",
        "name": "Test Label",
        "sha256": "fed321cba654",
        "parentLabel": {"id": "876543"},
        "sublabels": [{"id": "765432"}],
    }


@pytest.fixture
def sample_release_data() -> dict[str, Any]:
    """Return representative Discogs release data."""
    return {
        "id": "112233",
        "title": "Test Release",
        "sha256": "112233445566",
        "artists": [{"id": "123456", "name": "Test Artist"}],
        "labels": [{"id": "987654", "name": "Test Label"}],
        "genres": ["Rock", "Pop"],
        "styles": ["Alternative Rock", "Indie Pop"],
        "master_id": "998877",
    }


@pytest.fixture
def sample_master_data() -> dict[str, Any]:
    """Return representative Discogs master data."""
    return {
        "id": "998877",
        "title": "Test Master",
        "year": 2023,
        "sha256": "998877665544",
        "artists": [{"id": "123456", "name": "Test Artist"}],
        "genres": ["Rock"],
        "styles": ["Alternative Rock"],
    }


@pytest.fixture(autouse=True)
def setup_test_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Provide non-secret local defaults for configuration tests."""
    values = {
        "RABBITMQ_USERNAME": "test",
        "RABBITMQ_PASSWORD": "test",
        "RABBITMQ_HOST": "localhost",
        "RABBITMQ_PORT": "5672",
        "DISCOGS_ROOT": str(tmp_path / "test-discogs"),
        "NEO4J_HOST": "localhost",
        "NEO4J_USERNAME": "test",
        "NEO4J_PASSWORD": "test",
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value)


@pytest.fixture(autouse=True)
def reset_global_state() -> Iterator[None]:
    """Prevent mutable service state from leaking between tests."""
    import graphinator.graphinator as g

    def reset() -> None:
        g.shutdown_requested = False
        g.graph = None
        g.config = None
        g.message_counts = {"artists": 0, "labels": 0, "masters": 0, "releases": 0}
        g.last_message_time = {"artists": 0.0, "labels": 0.0, "masters": 0.0, "releases": 0.0}
        g.progress_interval = 100
        g.current_task = None
        g.current_progress = 0.0
        g.consumer_tags = {}
        g.completed_files = set()
        g.queues = {}
        g.idle_mode = False

    reset()
    yield
    reset()


@pytest.fixture(autouse=True)
def disable_batch_mode() -> Iterator[None]:
    """Disable batch mode for all graphinator tests.

    The tests mock the old per-message processing flow, so we need to
    disable batch mode to use that code path.
    """
    with patch("graphinator.graphinator.BATCH_MODE", False), patch("graphinator.graphinator.batch_processor", None):
        yield


@pytest.fixture(autouse=True)
def reset_extraction_complete_signals() -> Iterator[None]:
    """Reset the extraction_complete signal latch between tests.

    check_file_completion defers stub cleanup until every data type has
    signalled extraction_complete, tracked in a module-level set. Leaking that
    set across tests would let one test's partial signals trigger (or suppress)
    cleanup in another.
    """
    import graphinator.graphinator as g

    saved = set(g.extraction_complete_signals)
    g.extraction_complete_signals = set()
    yield
    g.extraction_complete_signals = saved


@pytest.fixture(autouse=True)
def instant_maintenance_delays() -> Iterator[None]:
    """Collapse post-import maintenance wall-clock delays to zero.

    Maintenance staggers heavy queries (MAINTENANCE_SETTLE_SECONDS) and backs
    off on Neo4j transaction-memory pressure (MAINTENANCE_RETRY_BASE_DELAY_SECONDS).
    Those are production pacing values; tests assert the control flow, not the
    clock, so waiting on them would only make the suite slow.
    """
    with (
        patch("graphinator.graphinator.MAINTENANCE_SETTLE_SECONDS", 0.0),
        patch("graphinator.graphinator.MAINTENANCE_RETRY_BASE_DELAY_SECONDS", 0.0),
        patch("graphinator.graphinator.POST_IMPORT_MAINTENANCE_RETRY_DELAY_SECONDS", 0.0),
    ):
        yield


@pytest.fixture(autouse=True)
def reset_post_import_maintenance_task() -> Iterator[None]:
    """Clear the detached post-import maintenance task between tests.

    Post-import maintenance runs in its own task so the extraction_complete
    delivery can be acked immediately (discogsography-zjja). The task handle is
    module-level and single-flight, so a leaked handle from one test would make
    the next test's trigger a no-op — or have it await the wrong task.
    """
    import graphinator.graphinator as g

    g.post_import_maintenance_task = None
    g.maintenance_tasks = set()


@pytest.fixture(autouse=True)
def fast_outage_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    """Preserve backoff accounting without adding wall-clock delay."""
    from common.outage_backoff import OutageBackoff

    async def wait(backoff: OutageBackoff) -> float:
        return backoff.next_delay()

    monkeypatch.setattr(OutageBackoff, "wait", wait)


@pytest.fixture(autouse=True)
def in_memory_extraction_latch(monkeypatch: pytest.MonkeyPatch) -> None:
    """Use an in-memory latch except in tests that explicitly replace it."""
    import graphinator.graphinator as g

    async def load(_version: str) -> set[str]:
        return set(g.extraction_complete_signals)

    async def persist(_version: str, _signals: set[str]) -> None:
        return None

    monkeypatch.setattr(g, "_load_extraction_signals", load)
    monkeypatch.setattr(g, "_persist_extraction_signals", persist)
    monkeypatch.setattr(g, "extraction_complete_version", None)
    yield
    task = g.post_import_maintenance_task
    if task is not None and not task.done():
        task.cancel()
    g.post_import_maintenance_task = None
    g.maintenance_tasks = set()
