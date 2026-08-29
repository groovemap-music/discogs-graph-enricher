"""Regression tests for terminal file-completion deliveries."""

from unittest.mock import AsyncMock, patch

import pytest

import graphinator.graphinator as service


@pytest.mark.asyncio
async def test_file_complete_flushes_marks_and_acknowledges() -> None:
    message = AsyncMock()
    processor = AsyncMock()
    processor.flush_queue.return_value = True
    completed: set[str] = set()
    with (
        patch.object(service, "batch_processor", processor),
        patch.object(service, "completed_files", completed),
        patch.object(service, "CONSUMER_CANCEL_DELAY", 0),
    ):
        handled = await service.check_file_completion(
            {"type": "file_complete", "data_type": "artists", "total_processed": 12_345},
            "artists",
            message,
        )
    assert handled is True
    processor.flush_queue.assert_awaited_once_with("artists")
    assert completed == {"artists"}
    message.ack.assert_awaited_once()
    message.nack.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_drain_requeues_marker_without_marking_complete() -> None:
    message = AsyncMock()
    processor = AsyncMock()
    processor.flush_queue.return_value = False
    completed: set[str] = set()
    with patch.object(service, "batch_processor", processor), patch.object(service, "completed_files", completed):
        handled = await service.check_file_completion({"type": "file_complete"}, "artists", message)
    assert handled is True
    assert completed == set()
    message.nack.assert_awaited_once_with(requeue=True)
    message.ack.assert_not_awaited()


@pytest.mark.asyncio
async def test_normal_record_is_not_a_completion_marker() -> None:
    message = AsyncMock()
    assert await service.check_file_completion({"id": "123", "name": "Artist"}, "artists", message) is False
    message.ack.assert_not_awaited()
    message.nack.assert_not_awaited()


@pytest.mark.asyncio
async def test_artist_handler_returns_after_completion_marker() -> None:
    message = AsyncMock(body=b'{"type":"file_complete","data_type":"artists"}')
    with (
        patch.object(service, "shutdown_requested", False),
        patch.object(service, "check_file_completion", new_callable=AsyncMock, return_value=True) as completion,
        patch.object(service, "process_artist", new_callable=AsyncMock) as process,
    ):
        await service.on_artist_message(message)
    completion.assert_awaited_once()
    process.assert_not_awaited()
