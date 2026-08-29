"""Performance regression tests for the graph batch boundary."""

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from graphinator.batch_processor import BatchConfig, Neo4jBatchProcessor


def _driver() -> MagicMock:
    driver = MagicMock()
    session = AsyncMock()
    context = AsyncMock()
    context.__aenter__.return_value = session
    driver.session.return_value = context

    async def execute_write(function):
        transaction = AsyncMock()
        transaction.run.return_value.single.return_value = None
        await function(transaction)

    session.execute_write.side_effect = execute_write

    class EmptyResults:
        def __aiter__(self):
            return self

        async def __anext__(self):
            raise StopAsyncIteration

    session.run.return_value = EmptyResults()
    return driver


async def _add(processor: Neo4jBatchProcessor, data_type: str, count: int) -> None:
    for index in range(count):
        await processor.add_message(
            data_type,
            {"id": f"{data_type}-{index}", "name": f"Record {index}", "sha256": f"hash-{index}"},
            AsyncMock(),
            AsyncMock(),
        )


@pytest.mark.asyncio
async def test_batch_size_500_processes_1000_records_with_two_writes() -> None:
    processor = Neo4jBatchProcessor(_driver(), BatchConfig(batch_size=500, flush_interval=2, max_pending=5000))
    started = time.perf_counter()
    await _add(processor, "artists", 1000)
    await processor.flush_all()
    assert time.perf_counter() - started < 2.0
    assert processor.processed_counts["artists"] == 1000
    assert processor.batch_counts["artists"] == 2


@pytest.mark.asyncio
async def test_four_entity_streams_flush_concurrently() -> None:
    processor = Neo4jBatchProcessor(_driver(), BatchConfig(batch_size=500, flush_interval=2, max_pending=5000))
    started = time.perf_counter()
    await asyncio.gather(*(_add(processor, data_type, 500) for data_type in ("artists", "labels", "masters", "releases")))
    await processor.flush_all()
    assert time.perf_counter() - started < 3.0
    assert processor.processed_counts == dict.fromkeys(("artists", "labels", "masters", "releases"), 500)
