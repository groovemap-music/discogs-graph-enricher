"""Exit-status tests: cli() must exit 1 whenever startup gives up and 0 on a signal-driven stop.

Docker's ``restart: on-failure`` only restarts a non-zero exit, so these run the real cli()
entry point and assert the status it exits with.
"""

import asyncio
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import graphinator.graphinator as gm


if TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.fixture(autouse=True)
def _isolated_process(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Keep cli() from touching real logging, telemetry, signals, or module shutdown state."""
    monkeypatch.setenv("STARTUP_DELAY", "0")
    monkeypatch.setenv("NEO4J_HOST", "localhost")
    monkeypatch.setenv("NEO4J_USERNAME", "neo4j")
    monkeypatch.setenv("NEO4J_PASSWORD", "password")
    monkeypatch.setattr(gm, "shutdown_requested", False)
    with (
        patch.object(gm, "setup_logging"),
        patch.object(gm, "setup_telemetry"),
        patch.object(gm, "shutdown_telemetry") as mock_shutdown_telemetry,
        patch.object(gm.signal, "signal"),
        patch.object(gm, "HealthServer"),
        patch.object(gm, "BATCH_MODE", False),
    ):
        yield
        # Telemetry must be flushed on every exit path, failure or not.
        mock_shutdown_telemetry.assert_called_once()


def _run_cli() -> int | str | None:
    """Run cli() and return the SystemExit code (None when it returned normally)."""
    try:
        gm.cli()
    except SystemExit as exc:
        return exc.code
    return None


def _neo4j_session_ok() -> MagicMock:
    """A Neo4j driver double whose connectivity probe succeeds."""
    session = AsyncMock()
    result = AsyncMock()
    result.single = AsyncMock(return_value={"test": 1})
    session.run = AsyncMock(return_value=result)
    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=session)
    ctx.__aexit__ = AsyncMock(return_value=None)
    driver = MagicMock()
    driver.session = MagicMock(return_value=ctx)
    driver.close = AsyncMock()
    return driver


class TestStartupGiveUpExitsOne:
    def test_missing_required_env_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("NEO4J_HOST")
        assert _run_cli() == 1

    def test_invalid_health_port(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("HEALTH_PORT", "abc")
        assert _run_cli() == 1

    def test_neo4j_unreachable(self) -> None:
        driver = MagicMock()
        driver.session = MagicMock(side_effect=Exception("connection refused"))
        with patch.object(gm, "AsyncResilientNeo4jDriver", return_value=driver):
            assert _run_cli() == 1

    def test_amqp_unreachable_after_retries(self) -> None:
        rabbit = AsyncMock()
        rabbit.connect = AsyncMock(side_effect=Exception("broker down"))
        with (
            patch.object(gm, "AsyncResilientNeo4jDriver", return_value=_neo4j_session_ok()),
            patch.object(gm, "AsyncResilientRabbitMQ", return_value=rabbit),
            patch.object(gm.asyncio, "sleep", AsyncMock()),
        ):
            assert _run_cli() == 1
        assert rabbit.connect.await_count == 5

    def test_no_amqp_connection_without_shutdown_request(self) -> None:
        rabbit = AsyncMock()
        rabbit.connect = AsyncMock(return_value=None)
        with (
            patch.object(gm, "AsyncResilientNeo4jDriver", return_value=_neo4j_session_ok()),
            patch.object(gm, "AsyncResilientRabbitMQ", return_value=rabbit),
        ):
            # connect() "succeeds" with None: the loop exits with no connection and no signal.
            assert _run_cli() == 1

    def test_unexpected_error_in_main(self) -> None:
        with patch.object(gm, "main", AsyncMock(side_effect=RuntimeError("boom"))):
            assert _run_cli() == 1


class TestCleanShutdownExitsZero:
    def test_signal_before_amqp_connects(self) -> None:
        rabbit = AsyncMock()
        rabbit.connect = AsyncMock(return_value=None)
        with (
            patch.object(gm, "AsyncResilientNeo4jDriver", return_value=_neo4j_session_ok()),
            patch.object(gm, "AsyncResilientRabbitMQ", return_value=rabbit),
            patch.object(gm, "shutdown_requested", True),
        ):
            assert _run_cli() is None

    def test_signal_after_startup(self) -> None:
        connection = AsyncMock()
        channel = AsyncMock()
        channel.declare_exchange = AsyncMock(return_value=AsyncMock())
        channel.declare_queue = AsyncMock(return_value=AsyncMock())
        connection.channel = AsyncMock(return_value=channel)
        connection.__aenter__ = AsyncMock(return_value=connection)
        connection.__aexit__ = AsyncMock(return_value=None)
        rabbit = AsyncMock()
        rabbit.connect = AsyncMock(return_value=connection)

        async def signalled(coro: Any, **_kwargs: Any) -> None:
            # The SIGTERM handler's effect: flag the shutdown, then let the loop wind down.
            coro.close()
            gm.shutdown_requested = True
            raise TimeoutError

        with (
            patch.object(gm, "AsyncResilientNeo4jDriver", return_value=_neo4j_session_ok()),
            patch.object(gm, "AsyncResilientRabbitMQ", return_value=rabbit),
            patch.object(gm, "cancel_all_consumers", AsyncMock()),
            patch.object(gm, "close_rabbitmq_connection", AsyncMock()),
            patch.object(asyncio, "wait_for", signalled),
        ):
            assert _run_cli() is None
