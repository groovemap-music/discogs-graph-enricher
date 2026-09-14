"""Tests for batch_processor module."""

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from graphinator import telemetry as gm_telemetry
from graphinator.batch_processor import (
    BatchConfig,
    Neo4jBatchProcessor,
    PendingMessage,
)


def create_async_session_mock() -> tuple[MagicMock, AsyncMock]:
    """Create mock driver and session for async context manager testing.

    Returns:
        Tuple of (mock_driver, mock_session)
    """
    mock_session = AsyncMock()
    mock_session_context = AsyncMock()
    mock_session_context.__aenter__.return_value = mock_session
    mock_session_context.__aexit__.return_value = None

    mock_driver = MagicMock()
    # driver.session() is an @asynccontextmanager, returns context manager directly
    mock_driver.session = MagicMock(return_value=mock_session_context)

    return mock_driver, mock_session


class AsyncIteratorMock:
    """Mock async iterator that yields items from a list."""

    def __init__(self, items: list[Any]):
        """Initialize with items to yield."""
        self.items = items
        self.index = 0

    def __aiter__(self) -> AsyncIteratorMock:
        """Return self as the async iterator."""
        return self

    async def __anext__(self) -> Any:
        """Return the next item or raise StopAsyncIteration."""
        if self.index >= len(self.items):
            raise StopAsyncIteration
        item = self.items[self.index]
        self.index += 1
        return item


def create_async_result_mock(records: list[dict[str, Any]]) -> AsyncIteratorMock:
    """Create a mock result that can be used with async for.

    Args:
        records: List of record dicts to return

    Returns:
        Mock result object
    """
    return AsyncIteratorMock(records)


class TestBatchConfig:
    """Test BatchConfig dataclass."""

    def test_default_config(self) -> None:
        """Test default configuration values."""
        config = BatchConfig()
        assert config.batch_size == 100
        assert config.flush_interval == 5.0
        assert config.max_pending == 1000

    def test_custom_config(self) -> None:
        """Test custom configuration values."""
        config = BatchConfig(batch_size=50, flush_interval=10.0, max_pending=500)
        assert config.batch_size == 50
        assert config.flush_interval == 10.0
        assert config.max_pending == 500


class TestPendingMessage:
    """Test PendingMessage dataclass."""

    def test_pending_message_creation(self) -> None:
        """Test creating a pending message."""
        ack_callback = MagicMock()
        nack_callback = MagicMock()
        data = {"id": "123", "name": "Test"}

        msg = PendingMessage(data_type="artists", data=data, ack_callback=ack_callback, nack_callback=nack_callback)

        assert msg.data_type == "artists"
        assert msg.data == data
        assert msg.ack_callback == ack_callback
        assert msg.nack_callback == nack_callback
        assert msg.received_at > 0


class TestSharedBatchRuntimeAdapter:
    """Owner policy plugs into common.batch without reimplementing its lifecycle."""

    @pytest.mark.asyncio
    async def test_success_settles_each_member_once_and_updates_owner_stats(self) -> None:
        processor = Neo4jBatchProcessor(MagicMock(), BatchConfig(batch_size=2))
        processor._process_artists_batch = AsyncMock(return_value={1})  # type: ignore[method-assign]
        ack_one, ack_two, reject_two = AsyncMock(), AsyncMock(), AsyncMock()

        assert await processor.add_message("artists", {"id": "1"}, ack_one, AsyncMock())
        assert await processor.add_message("artists", {"id": "2"}, ack_two, reject_two)

        ack_one.assert_awaited_once()
        ack_two.assert_not_awaited()
        reject_two.assert_awaited_once()
        assert processor.get_stats()["processed"]["artists"] == 1
        assert processor.get_stats()["batches"]["artists"] == 1

    @pytest.mark.asyncio
    async def test_transient_failure_is_retained_without_poison_or_settlement(self) -> None:
        from common.db_resilience import DatabaseUnavailableError

        processor = Neo4jBatchProcessor(
            MagicMock(),
            BatchConfig(batch_size=1, min_batch_size=1, backoff_initial=0.001),
        )
        processor._process_artists_batch = AsyncMock(  # type: ignore[method-assign]
            side_effect=DatabaseUnavailableError("neo4j down")
        )
        ack, nack = AsyncMock(), AsyncMock()

        assert await processor.add_message("artists", {"id": "1"}, ack, nack)

        snapshot = processor._engine.snapshot()
        assert snapshot["pending"]["artists"] == 1  # type: ignore[index]
        assert snapshot["transient_attempts"]["artists"] == 1  # type: ignore[index]
        assert snapshot["poison_attempts"]["artists"] == 0  # type: ignore[index]
        ack.assert_not_awaited()
        nack.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_deterministic_poison_isolated_and_rejected_once(self) -> None:
        processor = Neo4jBatchProcessor(
            MagicMock(),
            BatchConfig(batch_size=1, min_batch_size=1, max_flush_retries=2, max_poison_retries=2),
        )
        processor._process_artists_batch = AsyncMock(side_effect=ValueError("poison"))  # type: ignore[method-assign]
        ack, nack = AsyncMock(), AsyncMock()

        assert await processor.add_message("artists", {"id": "1"}, ack, nack)

        ack.assert_not_awaited()
        nack.assert_awaited_once()
        assert processor._engine.snapshot()["pending"]["artists"] == 0  # type: ignore[index]

    @pytest.mark.asyncio
    async def test_bounded_drain_retains_deterministic_work_without_settlement(self) -> None:
        processor = Neo4jBatchProcessor(
            MagicMock(),
            BatchConfig(batch_size=1, min_batch_size=1, max_flush_retries=2, max_poison_retries=10),
        )
        processor._process_artists_batch = AsyncMock(side_effect=ValueError("still poison"))  # type: ignore[method-assign]
        delivery = PendingMessage("artists", {"id": "1"}, AsyncMock(), AsyncMock())
        await processor._engine.submit("artists", delivery, delivery)

        assert await processor.flush_queue("artists") is False
        assert processor._engine.snapshot()["pending"]["artists"] == 1  # type: ignore[index]
        delivery.ack_callback.assert_not_awaited()
        delivery.nack_callback.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_retry_restores_original_order(self) -> None:
        from common.db_resilience import DatabaseUnavailableError

        seen: list[list[str]] = []

        async def process(messages: list[PendingMessage]) -> set[int]:
            seen.append([str(message.data["id"]) for message in messages])
            if len(seen) == 1:
                raise DatabaseUnavailableError("retry")
            return set()

        processor = Neo4jBatchProcessor(
            MagicMock(),
            BatchConfig(batch_size=3, min_batch_size=1, backoff_initial=0.001),
        )
        processor._process_artists_batch = process  # type: ignore[method-assign]
        messages = [PendingMessage("artists", {"id": str(index)}, AsyncMock(), AsyncMock()) for index in range(3)]
        for message in messages:
            await processor._engine.submit("artists", message, message)

        assert await processor.flush_queue("artists") is False
        await asyncio.sleep(0.002)
        assert await processor.flush_queue("artists") is True
        assert seen == [["0", "1", "2"], ["0"], ["1", "2"]]
        assert all(message.ack_callback.await_count == 1 for message in messages)

    @pytest.mark.asyncio
    async def test_cancellation_restores_unsettled_delivery(self) -> None:
        entered = asyncio.Event()

        async def block(_messages: list[PendingMessage]) -> set[int]:
            entered.set()
            await asyncio.Future()
            return set()

        processor = Neo4jBatchProcessor(MagicMock(), BatchConfig(batch_size=1))
        processor._process_artists_batch = block  # type: ignore[method-assign]
        ack, nack = AsyncMock(), AsyncMock()
        task = asyncio.create_task(processor.add_message("artists", {"id": "1"}, ack, nack))
        await entered.wait()

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert processor._engine.snapshot()["pending"]["artists"] == 1  # type: ignore[index]
        ack.assert_not_awaited()
        nack.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_observer_failure_cannot_change_settlement(self) -> None:
        processor = Neo4jBatchProcessor(MagicMock(), BatchConfig(batch_size=1))
        processor._process_artists_batch = AsyncMock(return_value=set())  # type: ignore[method-assign]
        ack = AsyncMock()

        with patch.object(gm_telemetry, "record_batch_flush", side_effect=RuntimeError("metrics down")):
            assert await processor.add_message("artists", {"id": "1"}, ack, AsyncMock())

        ack.assert_awaited_once()

    @pytest.mark.parametrize(
        "error",
        [
            pytest.param(__import__("neo4j").exceptions.ServiceUnavailable("down"), id="service-unavailable"),
            pytest.param(__import__("neo4j").exceptions.SessionExpired("expired"), id="session-expired"),
            pytest.param(
                __import__("neo4j").exceptions.TransientError("Neo.TransientError.General.DatabaseUnavailable", "retry"), id="transient-error"
            ),
        ],
    )
    def test_neo4j_driver_failures_are_transient(self, error: BaseException) -> None:
        from common.delivery import FailureKind

        from graphinator.batch_processor import Neo4jFailureClassifier

        assert Neo4jFailureClassifier()(error) is FailureKind.TRANSIENT

    def test_owner_sink_maps_projection_indices_to_terminal_results(self) -> None:
        from common.delivery import Settlement

        async def exercise() -> None:
            processor = Neo4jBatchProcessor(MagicMock())
            processor._process_artists_batch = AsyncMock(return_value={1})  # type: ignore[method-assign]
            messages = [
                PendingMessage("artists", {"id": "1"}, AsyncMock(), AsyncMock()),
                PendingMessage("artists", {"id": "2"}, AsyncMock(), AsyncMock()),
            ]
            results = await processor.write("artists", messages)
            assert [result.settlement for result in results] == [Settlement.ACK, Settlement.REJECT]

        asyncio.run(exercise())


class TestProcessArtistsBatch:
    """Test _process_artists_batch functionality."""

    @pytest.mark.asyncio
    async def test_process_artists_with_no_updates_needed(self) -> None:
        """Test skipping artists that are already up to date."""
        mock_driver, mock_session = create_async_session_mock()

        # Mock hash check to return matching hashes
        mock_result = create_async_result_mock([{"id": "1", "hash": "hash1"}])
        mock_session.run = AsyncMock(return_value=mock_result)

        processor = Neo4jBatchProcessor(mock_driver)

        messages = [PendingMessage("artists", {"id": "1", "name": "Artist 1", "sha256": "hash1"}, AsyncMock(), AsyncMock())]

        nack_indices = await processor._process_artists_batch(messages)

        # Should only run hash check query, not updates
        assert mock_session.run.call_count == 1
        assert nack_indices == set()

    @pytest.mark.asyncio
    async def test_process_artists_with_updates(self) -> None:
        """Test processing artists that need updates."""
        mock_driver, mock_session = create_async_session_mock()

        # Mock hash check to return no existing hash
        mock_result = create_async_result_mock([{"id": "1", "hash": None}])
        mock_session.run = AsyncMock(return_value=mock_result)

        processor = Neo4jBatchProcessor(mock_driver)

        messages = [
            PendingMessage(
                "artists",
                {"id": "1", "name": "Artist 1", "sha256": "hash1", "members": [], "groups": [], "aliases": []},
                AsyncMock(),
                AsyncMock(),
            )
        ]

        nack_indices = await processor._process_artists_batch(messages)

        # Should have called execute_write
        mock_session.execute_write.assert_called_once()
        assert nack_indices == set()

    @pytest.mark.asyncio
    async def test_process_artists_with_relationships(self) -> None:
        """Test processing artists with members, groups, and aliases."""
        mock_driver, mock_session = create_async_session_mock()

        # Mock hash check
        mock_result = create_async_result_mock([{"id": "1", "hash": None}])
        mock_session.run = AsyncMock(return_value=mock_result)

        processor = Neo4jBatchProcessor(mock_driver)

        messages = [
            PendingMessage(
                "artists",
                {
                    "id": "1",
                    "name": "Artist 1",
                    "sha256": "hash1",
                    "members": [{"id": "2"}, {"id": "3"}],
                    "groups": [{"id": "4"}],
                    "aliases": [{"id": "5"}],
                },
                AsyncMock(),
                AsyncMock(),
            )
        ]

        nack_indices = await processor._process_artists_batch(messages)

        mock_session.execute_write.assert_called_once()
        assert nack_indices == set()

    @pytest.mark.asyncio
    async def test_process_artists_skips_messages_with_missing_id(self) -> None:
        """Test that messages without 'id' field are skipped with a warning."""
        mock_driver, mock_session = create_async_session_mock()
        mock_result = create_async_result_mock([{"id": "1", "hash": None}])
        mock_session.run = AsyncMock(return_value=mock_result)

        processor = Neo4jBatchProcessor(mock_driver)

        messages = [
            PendingMessage("artists", {"name": "No ID Artist"}, AsyncMock(), AsyncMock()),
            PendingMessage("artists", {"id": "1", "name": "Artist 1", "sha256": "h1"}, AsyncMock(), AsyncMock()),
        ]

        with patch("graphinator.batch_processor.logger") as mock_logger:
            nack_indices = await processor._process_artists_batch(messages)

        mock_logger.warning.assert_called()
        mock_session.execute_write.assert_called_once()
        assert nack_indices == {0}

    @pytest.mark.asyncio
    async def test_process_artists_all_missing_id_returns_early(self) -> None:
        """Test early return when all messages lack an 'id' field."""
        mock_driver, _mock_session = create_async_session_mock()
        processor = Neo4jBatchProcessor(mock_driver)

        messages = [
            PendingMessage("artists", {"name": "No ID 1"}, AsyncMock(), AsyncMock()),
            PendingMessage("artists", {"name": "No ID 2"}, AsyncMock(), AsyncMock()),
        ]

        with patch("graphinator.batch_processor.logger"):
            nack_indices = await processor._process_artists_batch(messages)

        mock_driver.session.assert_not_called()
        assert nack_indices == {0, 1}


class TestProcessLabelsBatch:
    """Test _process_labels_batch functionality."""

    @pytest.mark.asyncio
    async def test_process_labels_with_parent_and_sublabels(self) -> None:
        """Test processing labels with parent and sublabel relationships."""
        mock_driver, mock_session = create_async_session_mock()

        # Mock hash check
        mock_result = create_async_result_mock([{"id": "1", "hash": None}])
        mock_session.run = AsyncMock(return_value=mock_result)

        processor = Neo4jBatchProcessor(mock_driver)

        messages = [
            PendingMessage(
                "labels",
                {
                    "id": "1",
                    "name": "Label 1",
                    "sha256": "hash1",
                    "parentLabel": {"id": "2"},
                    "sublabels": [{"id": "3"}, {"id": "4"}],
                },
                AsyncMock(),
                AsyncMock(),
            )
        ]

        await processor._process_labels_batch(messages)

        mock_session.execute_write.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_labels_skips_messages_with_missing_id(self) -> None:
        """Test that label messages without 'id' are skipped."""
        mock_driver, mock_session = create_async_session_mock()
        mock_result = create_async_result_mock([{"id": "1", "hash": None}])
        mock_session.run = AsyncMock(return_value=mock_result)
        processor = Neo4jBatchProcessor(mock_driver)

        messages = [
            PendingMessage("labels", {"name": "No ID Label"}, AsyncMock(), AsyncMock()),
            PendingMessage("labels", {"id": "1", "name": "Label 1", "sha256": "h1"}, AsyncMock(), AsyncMock()),
        ]
        with patch("graphinator.batch_processor.logger"):
            await processor._process_labels_batch(messages)
        mock_session.execute_write.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_labels_all_missing_id_returns_early(self) -> None:
        """Test early return when all label messages lack an 'id' field."""
        mock_driver, _mock_session = create_async_session_mock()
        processor = Neo4jBatchProcessor(mock_driver)
        messages = [PendingMessage("labels", {"name": "No ID"}, AsyncMock(), AsyncMock())]
        with patch("graphinator.batch_processor.logger"):
            await processor._process_labels_batch(messages)
        mock_driver.session.assert_not_called()


class TestProcessMastersBatch:
    """Test _process_masters_batch functionality."""

    @pytest.mark.asyncio
    async def test_process_masters_with_genres_and_styles(self) -> None:
        """Test processing masters with genres and styles."""
        mock_driver, mock_session = create_async_session_mock()

        # Mock hash check
        mock_result = create_async_result_mock([{"id": "1", "hash": None}])
        mock_session.run = AsyncMock(return_value=mock_result)

        processor = Neo4jBatchProcessor(mock_driver)

        messages = [
            PendingMessage(
                "masters",
                {
                    "id": "1",
                    "title": "Master 1",
                    "year": 2023,
                    "sha256": "hash1",
                    "artists": [{"id": "A1"}],
                    "genres": ["Rock", "Pop"],
                    "styles": ["Alternative", "Indie"],
                },
                AsyncMock(),
                AsyncMock(),
            )
        ]

        await processor._process_masters_batch(messages)

        mock_session.execute_write.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_masters_skips_messages_with_missing_id(self) -> None:
        """Test that master messages without 'id' are skipped."""
        mock_driver, mock_session = create_async_session_mock()
        mock_result = create_async_result_mock([{"id": "1", "hash": None}])
        mock_session.run = AsyncMock(return_value=mock_result)
        processor = Neo4jBatchProcessor(mock_driver)

        messages = [
            PendingMessage("masters", {"title": "No ID"}, AsyncMock(), AsyncMock()),
            PendingMessage("masters", {"id": "1", "title": "Master 1", "sha256": "h1"}, AsyncMock(), AsyncMock()),
        ]
        with patch("graphinator.batch_processor.logger"):
            await processor._process_masters_batch(messages)
        mock_session.execute_write.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_masters_all_missing_id_returns_early(self) -> None:
        """Test early return when all master messages lack an 'id' field."""
        mock_driver, _mock_session = create_async_session_mock()
        processor = Neo4jBatchProcessor(mock_driver)
        messages = [PendingMessage("masters", {"title": "No ID"}, AsyncMock(), AsyncMock())]
        with patch("graphinator.batch_processor.logger"):
            await processor._process_masters_batch(messages)
        mock_driver.session.assert_not_called()

    @pytest.mark.asyncio
    async def test_masters_multi_genre_skips_part_of(self) -> None:
        """discogsography-sy5k: a master with >1 genre must not cartesian-link every
        style to every genre — the assertion is only unambiguous for a single genre."""
        mock_driver, mock_session = create_async_session_mock()
        mock_result = create_async_result_mock([{"id": "1", "hash": None}])
        mock_session.run = AsyncMock(return_value=mock_result)

        executed_queries: list[str] = []

        async def track_query(query: str, **_params: Any) -> None:
            executed_queries.append(query)

        mock_tx = AsyncMock()
        mock_tx.run.side_effect = track_query

        async def execute_write_mock(tx_func: Any) -> None:
            await tx_func(mock_tx)

        mock_session.execute_write = AsyncMock(side_effect=execute_write_mock)

        processor = Neo4jBatchProcessor(mock_driver)
        messages = [
            PendingMessage(
                "masters",
                {
                    "id": "1",
                    "title": "Master 1",
                    "sha256": "hash1",
                    "genres": ["Electronic", "Rock"],
                    "styles": ["House"],
                },
                AsyncMock(),
                AsyncMock(),
            )
        ]

        await processor._process_masters_batch(messages)

        part_of_queries = [q for q in executed_queries if "PART_OF" in q]
        assert part_of_queries == []

    @pytest.mark.asyncio
    async def test_masters_single_genre_creates_part_of(self) -> None:
        """A single-genre master is an unambiguous style->genre assertion and should
        still create the PART_OF edge."""
        mock_driver, mock_session = create_async_session_mock()
        mock_result = create_async_result_mock([{"id": "1", "hash": None}])
        mock_session.run = AsyncMock(return_value=mock_result)

        executed_queries: list[tuple[str, dict[str, Any]]] = []

        async def track_query(query: str, **params: Any) -> None:
            executed_queries.append((query, params))

        mock_tx = AsyncMock()
        mock_tx.run.side_effect = track_query

        async def execute_write_mock(tx_func: Any) -> None:
            await tx_func(mock_tx)

        mock_session.execute_write = AsyncMock(side_effect=execute_write_mock)

        processor = Neo4jBatchProcessor(mock_driver)
        messages = [
            PendingMessage(
                "masters",
                {
                    "id": "1",
                    "title": "Master 1",
                    "sha256": "hash1",
                    "genres": ["Electronic"],
                    "styles": ["House", "Techno"],
                },
                AsyncMock(),
                AsyncMock(),
            )
        ]

        await processor._process_masters_batch(messages)

        part_of_queries = [(q, p) for q, p in executed_queries if "PART_OF" in q]
        assert len(part_of_queries) == 1
        pairs = part_of_queries[0][1]["pairs"]
        assert sorted(pairs, key=lambda pair: pair["style"]) == [
            {"genre": "Electronic", "style": "House"},
            {"genre": "Electronic", "style": "Techno"},
        ]


class TestStaleEdgePruning:
    """discogsography-bd0u: relationship writes are MERGE-only and therefore additive,
    but Discogs records are mutable. A release corrected from genres=["Rock"] to
    genres=["Jazz"] gets a new sha256, passes the hash gate, MERGEs the Jazz edge — and
    used to keep the Rock one forever. compute_genre_style_stats counts those stale
    edges, so Genre/Style/Label counts were permanently over-stated and explore
    endpoints listed the release under a genre it no longer has.
    """

    @staticmethod
    def _tracking_session() -> tuple[Any, Any, list[tuple[str, dict[str, Any]]]]:
        mock_driver, mock_session = create_async_session_mock()
        mock_session.run = AsyncMock(return_value=create_async_result_mock([{"id": "1", "hash": "oldhash"}]))

        executed: list[tuple[str, dict[str, Any]]] = []

        async def track_query(query: str, **params: Any) -> None:
            executed.append((query, params))

        mock_tx = AsyncMock()
        mock_tx.run.side_effect = track_query

        async def execute_write_mock(tx_func: Any) -> None:
            await tx_func(mock_tx)

        mock_session.execute_write = AsyncMock(side_effect=execute_write_mock)
        return mock_driver, mock_session, executed

    @staticmethod
    def _prunes(executed: list[tuple[str, dict[str, Any]]], rel_type: str, target: str) -> list[dict[str, Any]]:
        return [params for query, params in executed if "DELETE rel" in query and f"[rel:{rel_type}]" in query and f":{target})" in query]

    @pytest.mark.asyncio
    async def test_release_prunes_dropped_genre_edges(self) -> None:
        mock_driver, _session, executed = self._tracking_session()
        processor = Neo4jBatchProcessor(mock_driver)

        await processor._process_releases_batch(
            [
                PendingMessage(
                    "releases",
                    {"id": "1", "title": "R", "sha256": "newhash", "genres": ["Jazz"], "styles": []},
                    AsyncMock(),
                    AsyncMock(),
                )
            ]
        )

        genre_prunes = self._prunes(executed, "IS", "Genre")
        assert len(genre_prunes) == 1
        # Only the genres the NEW version asserts are kept; "Rock" no longer matches
        # the keep-list, so its edge is deleted.
        assert genre_prunes[0]["records"] == [{"key": "1", "keep": ["Jazz"]}]

    @pytest.mark.asyncio
    async def test_release_prunes_every_managed_edge_type(self) -> None:
        mock_driver, _session, executed = self._tracking_session()
        processor = Neo4jBatchProcessor(mock_driver)

        await processor._process_releases_batch(
            [
                PendingMessage(
                    "releases",
                    {
                        "id": "1",
                        "title": "R",
                        "sha256": "newhash",
                        "artists": [{"id": "A1"}],
                        "labels": [{"id": "L1"}],
                        "master_id": "M1",
                        "genres": ["Jazz"],
                        "styles": ["Bebop"],
                    },
                    AsyncMock(),
                    AsyncMock(),
                )
            ]
        )

        assert self._prunes(executed, "BY", "Artist")[0]["records"] == [{"key": "1", "keep": ["A1"]}]
        assert self._prunes(executed, "ON", "Label")[0]["records"] == [{"key": "1", "keep": ["L1"]}]
        assert self._prunes(executed, "DERIVED_FROM", "Master")[0]["records"] == [{"key": "1", "keep": ["M1"]}]
        assert self._prunes(executed, "IS", "Style")[0]["records"] == [{"key": "1", "keep": ["Bebop"]}]

    @pytest.mark.asyncio
    async def test_release_with_no_associations_prunes_them_all(self) -> None:
        """The "every association was removed" case: an empty keep-list must still run,
        or the old edges survive with nothing to contradict them."""
        mock_driver, _session, executed = self._tracking_session()
        processor = Neo4jBatchProcessor(mock_driver)

        await processor._process_releases_batch(
            [PendingMessage("releases", {"id": "1", "title": "R", "sha256": "newhash"}, AsyncMock(), AsyncMock())]
        )

        for rel_type, target in (("BY", "Artist"), ("ON", "Label"), ("DERIVED_FROM", "Master"), ("IS", "Genre"), ("IS", "Style")):
            prunes = self._prunes(executed, rel_type, target)
            assert len(prunes) == 1, f"{rel_type}->{target} prune must run even with nothing to keep"
            assert prunes[0]["records"] == [{"key": "1", "keep": []}]

    @pytest.mark.asyncio
    async def test_master_prunes_dropped_genre_and_style_edges(self) -> None:
        mock_driver, _session, executed = self._tracking_session()
        processor = Neo4jBatchProcessor(mock_driver)

        await processor._process_masters_batch(
            [
                PendingMessage(
                    "masters",
                    {"id": "1", "title": "M", "sha256": "newhash", "genres": ["Jazz"], "styles": ["Bebop"]},
                    AsyncMock(),
                    AsyncMock(),
                )
            ]
        )

        assert self._prunes(executed, "IS", "Genre")[0]["records"] == [{"key": "1", "keep": ["Jazz"]}]
        assert self._prunes(executed, "IS", "Style")[0]["records"] == [{"key": "1", "keep": ["Bebop"]}]
        assert self._prunes(executed, "BY", "Artist")[0]["records"] == [{"key": "1", "keep": []}]

    @pytest.mark.asyncio
    async def test_unchanged_records_are_not_pruned(self) -> None:
        """A record whose hash matches is never reprocessed, so its edges must be left
        strictly alone — pruning there would delete edges nothing is about to rewrite."""
        mock_driver, _session, executed = self._tracking_session()
        processor = Neo4jBatchProcessor(mock_driver)

        await processor._process_releases_batch(
            [
                PendingMessage(
                    "releases",
                    {"id": "1", "title": "R", "sha256": "oldhash", "genres": ["Rock"]},
                    AsyncMock(),
                    AsyncMock(),
                )
            ]
        )

        assert not [q for q, _ in executed if "DELETE rel" in q]


class TestProcessReleasesBatch:
    """Test _process_releases_batch functionality."""

    @pytest.mark.asyncio
    async def test_process_releases_with_all_relationships(self) -> None:
        """Test processing releases with all relationship types."""
        mock_driver, mock_session = create_async_session_mock()

        # Mock hash check
        mock_result = create_async_result_mock([{"id": "1", "hash": None}])
        mock_session.run = AsyncMock(return_value=mock_result)

        processor = Neo4jBatchProcessor(mock_driver)

        messages = [
            PendingMessage(
                "releases",
                {
                    "id": "1",
                    "title": "Release 1",
                    "year": 1997,
                    "sha256": "hash1",
                    "artists": [{"id": "A1"}],
                    "labels": [{"id": "L1"}],
                    "master_id": "M1",
                    "genres": ["Rock"],
                    "styles": ["Alternative"],
                },
                AsyncMock(),
                AsyncMock(),
            )
        ]

        await processor._process_releases_batch(messages)

        mock_session.execute_write.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_releases_with_credits(self) -> None:
        """Test processing releases with extraartists (credits) data."""
        mock_driver, mock_session = create_async_session_mock()

        # Mock hash check — no existing hash means needs processing
        mock_result = create_async_result_mock([{"id": "1", "hash": None}])
        mock_session.run = AsyncMock(return_value=mock_result)

        processor = Neo4jBatchProcessor(mock_driver)

        messages = [
            PendingMessage(
                "releases",
                {
                    "id": "1",
                    "title": "Release With Credits",
                    "year": 1995,
                    "sha256": "hash_credits",
                    "artists": [{"id": "A1"}],
                    "labels": [],
                    "extraartists": [
                        {"name": "Bob Ludwig", "role": "Mastered By", "id": "500"},
                        {"name": "Flood", "role": "Producer"},
                    ],
                },
                AsyncMock(),
                AsyncMock(),
            )
        ]

        await processor._process_releases_batch(messages)

        # execute_write should be called — the batch_write function processes credits
        mock_session.execute_write.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_releases_with_credits_no_artist_id(self) -> None:
        """Test credits without artist IDs (no SAME_AS relationship)."""
        mock_driver, mock_session = create_async_session_mock()

        mock_result = create_async_result_mock([{"id": "2", "hash": None}])
        mock_session.run = AsyncMock(return_value=mock_result)

        processor = Neo4jBatchProcessor(mock_driver)

        messages = [
            PendingMessage(
                "releases",
                {
                    "id": "2",
                    "title": "Release Credits No ID",
                    "year": 2000,
                    "sha256": "hash_no_id",
                    "extraartists": [
                        {"name": "Unknown Engineer", "role": "Engineer"},
                    ],
                },
                AsyncMock(),
                AsyncMock(),
            )
        ]

        await processor._process_releases_batch(messages)
        mock_session.execute_write.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_releases_credits_empty_name_skipped(self) -> None:
        """Test that credits with missing name or role are skipped."""
        mock_driver, mock_session = create_async_session_mock()

        mock_result = create_async_result_mock([{"id": "3", "hash": None}])
        mock_session.run = AsyncMock(return_value=mock_result)

        processor = Neo4jBatchProcessor(mock_driver)

        messages = [
            PendingMessage(
                "releases",
                {
                    "id": "3",
                    "title": "Release Bad Credits",
                    "year": 2005,
                    "sha256": "hash_bad",
                    "extraartists": [
                        {"name": "", "role": "Producer"},  # empty name
                        {"name": "Valid", "role": ""},  # empty role
                        {"role": "Engineer"},  # missing name
                    ],
                },
                AsyncMock(),
                AsyncMock(),
            )
        ]

        await processor._process_releases_batch(messages)
        mock_session.execute_write.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_releases_skips_messages_with_missing_id(self) -> None:
        """Test that release messages without 'id' are skipped."""
        mock_driver, mock_session = create_async_session_mock()
        mock_result = create_async_result_mock([{"id": "1", "hash": None}])
        mock_session.run = AsyncMock(return_value=mock_result)
        processor = Neo4jBatchProcessor(mock_driver)

        messages = [
            PendingMessage("releases", {"title": "No ID"}, AsyncMock(), AsyncMock()),
            PendingMessage("releases", {"id": "1", "title": "Release 1", "sha256": "h1"}, AsyncMock(), AsyncMock()),
        ]
        with patch("graphinator.batch_processor.logger"):
            await processor._process_releases_batch(messages)
        mock_session.execute_write.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_releases_all_missing_id_returns_early(self) -> None:
        """Test early return when all release messages lack an 'id' field."""
        mock_driver, _mock_session = create_async_session_mock()
        processor = Neo4jBatchProcessor(mock_driver)
        messages = [PendingMessage("releases", {"title": "No ID"}, AsyncMock(), AsyncMock())]
        with patch("graphinator.batch_processor.logger"):
            await processor._process_releases_batch(messages)
        mock_driver.session.assert_not_called()

    @pytest.mark.asyncio
    async def test_releases_multi_genre_skips_part_of(self) -> None:
        """discogsography-sy5k: a release with >1 genre must not cartesian-link every
        style to every genre — the assertion is only unambiguous for a single genre."""
        mock_driver, mock_session = create_async_session_mock()
        mock_result = create_async_result_mock([{"id": "1", "hash": None}])
        mock_session.run = AsyncMock(return_value=mock_result)

        executed_queries: list[str] = []

        async def track_query(query: str, **_params: Any) -> None:
            executed_queries.append(query)

        mock_tx = AsyncMock()
        mock_tx.run.side_effect = track_query

        async def execute_write_mock(tx_func: Any) -> None:
            await tx_func(mock_tx)

        mock_session.execute_write = AsyncMock(side_effect=execute_write_mock)

        processor = Neo4jBatchProcessor(mock_driver)
        messages = [
            PendingMessage(
                "releases",
                {
                    "id": "1",
                    "title": "Release 1",
                    "sha256": "hash1",
                    "genres": ["Electronic", "Rock"],
                    "styles": ["House"],
                },
                AsyncMock(),
                AsyncMock(),
            )
        ]

        await processor._process_releases_batch(messages)

        part_of_queries = [q for q in executed_queries if "PART_OF" in q]
        assert part_of_queries == []


class TestBatchTransactionLogic:
    """Test batch transaction logic for all data types."""

    @pytest.mark.asyncio
    async def test_artists_batch_transaction_creates_all_relationships(self) -> None:
        """Test artist batch transaction creates all relationship types."""
        mock_driver, mock_session = create_async_session_mock()

        # Track cypher queries executed
        executed_queries: list[str] = []

        async def track_query(query: str, **_params: Any) -> None:
            executed_queries.append(query)

        mock_tx = AsyncMock()
        mock_tx.run.side_effect = track_query

        async def execute_write_mock(tx_func: Any) -> None:
            await tx_func(mock_tx)

        mock_session.execute_write = AsyncMock(side_effect=execute_write_mock)

        # Mock hash check to return no hashes
        mock_result = create_async_result_mock([{"id": "1", "hash": None}])
        mock_session.run = AsyncMock(return_value=mock_result)

        processor = Neo4jBatchProcessor(mock_driver)

        messages = [
            PendingMessage(
                "artists",
                {
                    "id": "1",
                    "name": "Artist 1",
                    "sha256": "hash1",
                    "members": [{"id": "M1"}],
                    "groups": [{"id": "G1"}],
                    "aliases": [{"id": "A1"}],
                },
                AsyncMock(),
                AsyncMock(),
            )
        ]

        await processor._process_artists_batch(messages)

        # Should have created artist node and all relationships
        assert len(executed_queries) >= 4  # Artist node + members + groups + aliases

    @pytest.mark.asyncio
    async def test_artists_batch_all_up_to_date_returns_early(self) -> None:
        """When all artists already have matching hashes, returns early without writing (acking is done by _flush_queue)."""
        mock_driver, mock_session = create_async_session_mock()

        mock_result = create_async_result_mock([{"id": "1", "hash": "hash1"}])
        mock_session.run = AsyncMock(return_value=mock_result)

        processor = Neo4jBatchProcessor(mock_driver)

        ack_cb = AsyncMock()
        nack_cb = AsyncMock()
        messages = [PendingMessage("artists", {"id": "1", "name": "Artist 1", "sha256": "hash1"}, ack_cb, nack_cb)]

        await processor._process_artists_batch(messages)

        mock_session.execute_write.assert_not_called()
        # Acking is handled by _flush_queue, not by _process_artists_batch
        ack_cb.assert_not_awaited()
        nack_cb.assert_not_called()

    @pytest.mark.asyncio
    async def test_labels_batch_all_up_to_date_early_return(self) -> None:
        """Test early return when all labels already have matching hashes — acking is done by _flush_queue."""
        mock_driver, mock_session = create_async_session_mock()

        # Return hash that matches the message hash
        mock_result = create_async_result_mock([{"id": "1", "hash": "hash1"}])
        mock_session.run = AsyncMock(return_value=mock_result)

        processor = Neo4jBatchProcessor(mock_driver)

        ack_cb = AsyncMock()
        nack_cb = AsyncMock()
        messages = [PendingMessage("labels", {"id": "1", "name": "Label 1", "sha256": "hash1"}, ack_cb, nack_cb)]

        await processor._process_labels_batch(messages)

        # execute_write should NOT be called (returned early)
        mock_session.execute_write.assert_not_called()
        # Acking is handled by _flush_queue, not by _process_labels_batch
        ack_cb.assert_not_awaited()
        nack_cb.assert_not_called()

    @pytest.mark.asyncio
    async def test_labels_batch_transaction_executes_parent_and_sublabels(self) -> None:
        """Test label batch_write closure executes parent and sublabel logic (lines 409, 416, 430-432, 439)."""
        mock_driver, mock_session = create_async_session_mock()

        mock_result = create_async_result_mock([{"id": "1", "hash": None}])
        mock_session.run = AsyncMock(return_value=mock_result)

        executed_queries: list[str] = []

        async def track_query(query: str, **_params: Any) -> None:
            executed_queries.append(query)

        mock_tx = AsyncMock()
        mock_tx.run.side_effect = track_query

        async def execute_write_mock(tx_func: Any) -> None:
            await tx_func(mock_tx)

        mock_session.execute_write = AsyncMock(side_effect=execute_write_mock)

        processor = Neo4jBatchProcessor(mock_driver)

        messages = [
            PendingMessage(
                "labels",
                {
                    "id": "1",
                    "name": "Label 1",
                    "sha256": "hash1",
                    "parentLabel": {"id": "2"},
                    "sublabels": [{"id": "3"}, {"id": "4"}],
                },
                AsyncMock(),
                AsyncMock(),
            )
        ]

        await processor._process_labels_batch(messages)

        # Should have executed label node + parent + sublabel queries
        assert len(executed_queries) >= 3

    @pytest.mark.asyncio
    async def test_masters_batch_all_up_to_date_early_return(self) -> None:
        """Test early return when all masters already have matching hashes — acking is done by _flush_queue."""
        mock_driver, mock_session = create_async_session_mock()

        mock_result = create_async_result_mock([{"id": "1", "hash": "hash1"}])
        mock_session.run = AsyncMock(return_value=mock_result)

        processor = Neo4jBatchProcessor(mock_driver)

        ack_cb = AsyncMock()
        nack_cb = AsyncMock()
        messages = [PendingMessage("masters", {"id": "1", "title": "Master 1", "sha256": "hash1"}, ack_cb, nack_cb)]

        await processor._process_masters_batch(messages)

        # execute_write should NOT be called (returned early)
        mock_session.execute_write.assert_not_called()
        # Acking is handled by _flush_queue, not by _process_masters_batch
        ack_cb.assert_not_awaited()
        nack_cb.assert_not_called()

    @pytest.mark.asyncio
    async def test_masters_batch_transaction_executes_all_relationships(self) -> None:
        """Test master batch_write closure executes artist/genre/style logic (lines 498-500, 507, 521-523, 530, 544-546, 553, 569-571, 578)."""
        mock_driver, mock_session = create_async_session_mock()

        mock_result = create_async_result_mock([{"id": "1", "hash": None}])
        mock_session.run = AsyncMock(return_value=mock_result)

        executed_queries: list[str] = []

        async def track_query(query: str, **_params: Any) -> None:
            executed_queries.append(query)

        mock_tx = AsyncMock()
        mock_tx.run.side_effect = track_query

        async def execute_write_mock(tx_func: Any) -> None:
            await tx_func(mock_tx)

        mock_session.execute_write = AsyncMock(side_effect=execute_write_mock)

        processor = Neo4jBatchProcessor(mock_driver)

        messages = [
            PendingMessage(
                "masters",
                {
                    "id": "1",
                    "title": "Master 1",
                    "year": 2023,
                    "sha256": "hash1",
                    "artists": [{"id": "A1"}],
                    "genres": ["Rock"],
                    "styles": ["Alternative"],
                },
                AsyncMock(),
                AsyncMock(),
            )
        ]

        await processor._process_masters_batch(messages)

        # master node + artists + genres + styles + genre-style pairs
        assert len(executed_queries) >= 5

    @pytest.mark.asyncio
    async def test_releases_batch_all_up_to_date_early_return(self) -> None:
        """Test early return when all releases already have matching hashes — acking is done by _flush_queue."""
        mock_driver, mock_session = create_async_session_mock()

        mock_result = create_async_result_mock([{"id": "1", "hash": "hash1"}])
        mock_session.run = AsyncMock(return_value=mock_result)

        processor = Neo4jBatchProcessor(mock_driver)

        ack_cb = AsyncMock()
        nack_cb = AsyncMock()
        messages = [PendingMessage("releases", {"id": "1", "title": "Release 1", "year": None, "sha256": "hash1"}, ack_cb, nack_cb)]

        await processor._process_releases_batch(messages)

        # execute_write should NOT be called (returned early)
        mock_session.execute_write.assert_not_called()
        # Acking is handled by _flush_queue, not by _process_releases_batch
        ack_cb.assert_not_awaited()
        nack_cb.assert_not_called()

    @pytest.mark.asyncio
    async def test_releases_batch_transaction_executes_all_relationships(self) -> None:
        """Test release batch_write closure executes artist/label/master/genre/style logic."""
        mock_driver, mock_session = create_async_session_mock()

        mock_result = create_async_result_mock([{"id": "1", "hash": None}])
        mock_session.run = AsyncMock(return_value=mock_result)

        executed_queries: list[str] = []

        async def track_query(query: str, **_params: Any) -> None:
            executed_queries.append(query)

        mock_tx = AsyncMock()
        mock_tx.run.side_effect = track_query

        async def execute_write_mock(tx_func: Any) -> None:
            await tx_func(mock_tx)

        mock_session.execute_write = AsyncMock(side_effect=execute_write_mock)

        processor = Neo4jBatchProcessor(mock_driver)

        messages = [
            PendingMessage(
                "releases",
                {
                    "id": "1",
                    "title": "Release 1",
                    "year": 1997,
                    "sha256": "hash1",
                    "artists": [{"id": "A1"}],
                    "labels": [{"id": "L1"}],
                    "master_id": "M1",
                    "genres": ["Rock"],
                    "styles": ["Alternative"],
                },
                AsyncMock(),
                AsyncMock(),
            )
        ]

        await processor._process_releases_batch(messages)

        # release node (with year) + artists + labels + master + genres + styles + genre-style pairs
        assert len(executed_queries) >= 7

    @pytest.mark.asyncio
    async def test_releases_batch_year_written_to_node(self) -> None:
        """Test that release.year is included in the Release node SET clause."""
        mock_driver, mock_session = create_async_session_mock()

        mock_result = create_async_result_mock([{"id": "1", "hash": None}])
        mock_session.run = AsyncMock(return_value=mock_result)

        captured_queries: list[tuple[str, Any]] = []

        async def capture_query(query: str, **params: Any) -> None:
            captured_queries.append((query, params))

        mock_tx = AsyncMock()
        mock_tx.run.side_effect = capture_query

        async def execute_write_mock(tx_func: Any) -> None:
            await tx_func(mock_tx)

        mock_session.execute_write = AsyncMock(side_effect=execute_write_mock)

        processor = Neo4jBatchProcessor(mock_driver)

        messages = [
            PendingMessage(
                "releases",
                {"id": "1", "title": "Release 1", "year": 1997, "sha256": "hash1"},
                AsyncMock(),
                AsyncMock(),
            )
        ]

        await processor._process_releases_batch(messages)

        # The first query should set r.year
        first_query = captured_queries[0][0]
        assert "r.year" in first_query

    @pytest.mark.asyncio
    async def test_releases_batch_write_closure_credits_with_artist_id(self) -> None:
        """Test credits with artist IDs produce both CREDITED_ON and SAME_AS queries."""
        mock_driver, mock_session = create_async_session_mock()

        mock_result = create_async_result_mock([{"id": "1", "hash": None}])
        mock_session.run = AsyncMock(return_value=mock_result)

        executed_queries: list[str] = []

        async def track_query(query: str, **_params: Any) -> None:
            executed_queries.append(query)

        mock_tx = AsyncMock()
        mock_tx.run.side_effect = track_query

        async def execute_write_mock(tx_func: Any) -> None:
            await tx_func(mock_tx)

        mock_session.execute_write = AsyncMock(side_effect=execute_write_mock)

        processor = Neo4jBatchProcessor(mock_driver)

        messages = [
            PendingMessage(
                "releases",
                {
                    "id": "1",
                    "title": "Release With Credits",
                    "year": 1995,
                    "sha256": "hash_credits",
                    "artists": [{"id": "A1"}],
                    "labels": [],
                    "extraartists": [
                        {"name": "Bob Ludwig", "role": "Mastered By", "id": "500"},
                        {"name": "Flood", "role": "Producer", "id": "600"},
                    ],
                },
                AsyncMock(),
                AsyncMock(),
            )
        ]

        await processor._process_releases_batch(messages)

        # Should contain CREDITED_ON query for Person nodes
        credited_queries = [q for q in executed_queries if "CREDITED_ON" in q]
        assert len(credited_queries) == 1
        assert "Person" in credited_queries[0]

        # Should contain SAME_AS query linking Person to Artist (both credits have IDs)
        same_as_queries = [q for q in executed_queries if "SAME_AS" in q]
        assert len(same_as_queries) == 1

    @pytest.mark.asyncio
    async def test_releases_batch_write_closure_credits_without_artist_id(self) -> None:
        """Test credits without artist IDs produce CREDITED_ON but NOT SAME_AS queries."""
        mock_driver, mock_session = create_async_session_mock()

        mock_result = create_async_result_mock([{"id": "2", "hash": None}])
        mock_session.run = AsyncMock(return_value=mock_result)

        executed_queries: list[str] = []

        async def track_query(query: str, **_params: Any) -> None:
            executed_queries.append(query)

        mock_tx = AsyncMock()
        mock_tx.run.side_effect = track_query

        async def execute_write_mock(tx_func: Any) -> None:
            await tx_func(mock_tx)

        mock_session.execute_write = AsyncMock(side_effect=execute_write_mock)

        processor = Neo4jBatchProcessor(mock_driver)

        messages = [
            PendingMessage(
                "releases",
                {
                    "id": "2",
                    "title": "Release Credits No ID",
                    "year": 2000,
                    "sha256": "hash_no_id",
                    "extraartists": [
                        {"name": "Unknown Engineer", "role": "Engineer"},
                        {"name": "Anonymous Mixer", "role": "Mixed By"},
                    ],
                },
                AsyncMock(),
                AsyncMock(),
            )
        ]

        await processor._process_releases_batch(messages)

        # Should contain CREDITED_ON query for Person nodes
        credited_queries = [q for q in executed_queries if "CREDITED_ON" in q]
        assert len(credited_queries) == 1
        assert "Person" in credited_queries[0]

        # Should NOT contain SAME_AS query (no artist IDs provided)
        same_as_queries = [q for q in executed_queries if "SAME_AS" in q]
        assert len(same_as_queries) == 0

    @pytest.mark.asyncio
    async def test_credited_on_merge_key_excludes_derived_category(self) -> None:
        """discogsography-9k6i: CREDITED_ON must MERGE on {role} only and SET category,
        so a later categorize_role() taxonomy change updates the existing edge in place
        instead of forking a parallel duplicate edge."""
        mock_driver, mock_session = create_async_session_mock()

        mock_result = create_async_result_mock([{"id": "1", "hash": None}])
        mock_session.run = AsyncMock(return_value=mock_result)

        executed_queries: list[str] = []

        async def track_query(query: str, **_params: Any) -> None:
            executed_queries.append(query)

        mock_tx = AsyncMock()
        mock_tx.run.side_effect = track_query

        async def execute_write_mock(tx_func: Any) -> None:
            await tx_func(mock_tx)

        mock_session.execute_write = AsyncMock(side_effect=execute_write_mock)

        processor = Neo4jBatchProcessor(mock_driver)

        messages = [
            PendingMessage(
                "releases",
                {
                    "id": "1",
                    "title": "Release With Credits",
                    "year": 1995,
                    "sha256": "hash_credits",
                    "extraartists": [{"name": "Bob Ludwig", "role": "Mastered By"}],
                },
                AsyncMock(),
                AsyncMock(),
            )
        ]

        await processor._process_releases_batch(messages)

        credited_queries = [q for q in executed_queries if "CREDITED_ON" in q]
        assert len(credited_queries) == 1
        query = credited_queries[0]
        # The MERGE match key must be {role} only — category must not appear inside it.
        assert "MERGE (p)-[c:CREDITED_ON {role: credit.role}]->(r)" in query
        assert "category" not in query.split("MERGE (p)-[c:CREDITED_ON")[1].split("]->(r)")[0]
        # category is applied as a SET after the MERGE so re-ingest updates in place.
        assert "SET c.category = credit.category" in query


class TestReleaseCatalogNumber:
    """Verify the bulk graphinator pipeline propagates labels[0].catno → Release.catalog_number."""

    def _capture_tx_runs(self, mock_session: AsyncMock) -> list[tuple[str, dict[str, Any]]]:
        """Wire mock_session.execute_write so we can inspect every tx.run call inside batch_write."""
        captured: list[tuple[str, dict[str, Any]]] = []

        mock_tx = AsyncMock()

        async def capture(cypher: str, **kwargs: Any) -> AsyncMock:
            captured.append((cypher, kwargs))
            return AsyncMock()

        mock_tx.run = capture

        async def call_tx_func(tx_func: Any) -> None:
            await tx_func(mock_tx)

        mock_session.execute_write.side_effect = call_tx_func
        return captured

    def _make_release_msg(self, release_id: str, labels: list[dict[str, Any]] | None) -> PendingMessage:
        data: dict[str, Any] = {
            "id": release_id,
            "title": f"Release {release_id}",
            "year": 2020,
            "sha256": f"hash-{release_id}",
        }
        if labels is not None:
            data["labels"] = labels
        return PendingMessage("releases", data, AsyncMock(), AsyncMock())

    @pytest.mark.asyncio
    async def test_release_cypher_uses_metadata_bag(self) -> None:
        """The MERGE cypher must merge `r += release.metadata`; the bag carries catalog_number."""
        mock_driver, mock_session = create_async_session_mock()
        # Hash check returns no existing release → needs processing
        mock_result = create_async_result_mock([{"id": "R1", "hash": None}])
        mock_session.run = AsyncMock(return_value=mock_result)

        captured = self._capture_tx_runs(mock_session)
        processor = Neo4jBatchProcessor(mock_driver)
        msg = self._make_release_msg("R1", [{"id": "L1", "name": "Some Label", "catno": "ABC-123"}])

        await processor._process_releases_batch([msg])

        # First tx.run is the MERGE on Release nodes
        first_cypher, first_kwargs = captured[0]
        assert "r += release.metadata" in first_cypher
        assert first_kwargs["releases"][0]["metadata"] == {"catalog_number": "ABC-123"}

    @pytest.mark.asyncio
    async def test_release_metadata_empty_when_labels_empty(self) -> None:
        """labels=[] → metadata is {} on the payload (SET r += {} is a no-op)."""
        mock_driver, mock_session = create_async_session_mock()
        mock_result = create_async_result_mock([{"id": "R2", "hash": None}])
        mock_session.run = AsyncMock(return_value=mock_result)

        captured = self._capture_tx_runs(mock_session)
        processor = Neo4jBatchProcessor(mock_driver)
        msg = self._make_release_msg("R2", [])

        await processor._process_releases_batch([msg])

        _, first_kwargs = captured[0]
        assert first_kwargs["releases"][0]["metadata"] == {}

    @pytest.mark.asyncio
    async def test_release_metadata_empty_when_labels_missing(self) -> None:
        """`labels` key entirely absent → metadata is {} on the payload."""
        mock_driver, mock_session = create_async_session_mock()
        mock_result = create_async_result_mock([{"id": "R3", "hash": None}])
        mock_session.run = AsyncMock(return_value=mock_result)

        captured = self._capture_tx_runs(mock_session)
        processor = Neo4jBatchProcessor(mock_driver)
        msg = self._make_release_msg("R3", None)

        await processor._process_releases_batch([msg])

        _, first_kwargs = captured[0]
        assert first_kwargs["releases"][0]["metadata"] == {}

    @pytest.mark.asyncio
    async def test_release_metadata_empty_when_first_label_lacks_catno(self) -> None:
        """labels[0] without `catno` key → metadata is {} (no KeyError, catalog_number key absent)."""
        mock_driver, mock_session = create_async_session_mock()
        mock_result = create_async_result_mock([{"id": "R4", "hash": None}])
        mock_session.run = AsyncMock(return_value=mock_result)

        captured = self._capture_tx_runs(mock_session)
        processor = Neo4jBatchProcessor(mock_driver)
        msg = self._make_release_msg("R4", [{"id": "L1", "name": "Catno-less Label"}])

        await processor._process_releases_batch([msg])

        _, first_kwargs = captured[0]
        assert first_kwargs["releases"][0]["metadata"] == {}

    @pytest.mark.asyncio
    async def test_release_uses_first_label_catno(self) -> None:
        """Multiple labels — only labels[0].catno is taken (canonical pressing)."""
        mock_driver, mock_session = create_async_session_mock()
        mock_result = create_async_result_mock([{"id": "R5", "hash": None}])
        mock_session.run = AsyncMock(return_value=mock_result)

        captured = self._capture_tx_runs(mock_session)
        processor = Neo4jBatchProcessor(mock_driver)
        msg = self._make_release_msg(
            "R5",
            [
                {"id": "L1", "name": "Primary", "catno": "PRI-001"},
                {"id": "L2", "name": "Reissue", "catno": "REI-999"},
            ],
        )

        await processor._process_releases_batch([msg])

        _, first_kwargs = captured[0]
        assert first_kwargs["releases"][0]["metadata"] == {"catalog_number": "PRI-001"}
