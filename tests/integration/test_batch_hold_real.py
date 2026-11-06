"""Bounded batch holds through a long Neo4j outage, on a real RabbitMQ and a real Neo4j.

Before this, the enricher's batch engine held a delivery in memory for as long as Neo4j stayed down,
so an outage longer than the broker's ``consumer_timeout`` got the consumer cancelled and the
delivery requeued without being counted. With ``max_hold_s`` the engine returns each held delivery
to RabbitMQ with ``requeue=True`` inside the bound, and the broker's ``x-acquired-count`` counts the
trips, so the outage a record survives is ``max_requeues`` holds and no longer.

The outage is injected at the batch write (``ServiceUnavailable``, what the driver raises when
Neo4j is unreachable). Everything else is real: the queue, the handler, the settlement, and the Neo4j
the record lands in once the outage ends.
"""

from __future__ import annotations

import asyncio
import itertools
import os
import time
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import aio_pika
import pytest
import pytest_asyncio
from common.neo4j_resilient import AsyncResilientNeo4jDriver
from neo4j.exceptions import ServiceUnavailable
from orjson import dumps, loads

import graphinator.graphinator as graphinator
from graphinator.batch_processor import BatchConfig, Neo4jBatchProcessor
from graphinator.entity_projection import process_artist


if TYPE_CHECKING:
    from collections.abc import AsyncIterator


pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.integration,
    pytest.mark.timeout(90),
    pytest.mark.skipif(not os.environ.get("RABBITMQ_URL"), reason="RABBITMQ_URL not set"),
]

MAX_HOLD_S = 0.4
FLUSH_INTERVAL_S = 0.05
BACKOFF_MAX_S = 0.1
# The engine checks holds at each flush entry, so a hold can run past the bound by one flush tick, one
# backoff sleep, and scheduling.
HOLD_CEILING_S = MAX_HOLD_S + FLUSH_INTERVAL_S + BACKOFF_MAX_S + 0.3
IDS = ("hold-1", "hold-2", "hold-3")


@dataclass
class Stack:
    channel: aio_pika.abc.AbstractChannel
    queue: aio_pika.abc.AbstractQueue
    dlq: aio_pika.abc.AbstractQueue
    driver: AsyncResilientNeo4jDriver
    processor: Neo4jBatchProcessor
    outage: bool = True
    # (record id, x-acquired-count, monotonic arrival) for every delivery the consumer receives.
    deliveries: list[tuple[str, int, float]] = field(default_factory=list)

    async def publish(self, record_id: str) -> None:
        body = dumps({"id": record_id, "name": f"Artist {record_id}", "sha256": "a" * 64})
        await self.channel.default_exchange.publish(
            aio_pika.Message(body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT), routing_key=self.queue.name
        )

    async def stored(self) -> set[str]:
        async with self.driver.session(database="neo4j") as session:
            result = await session.run("MATCH (a:Artist) WHERE a.id IN $ids RETURN a.id AS id", ids=list(IDS))
            return {record["id"] async for record in result}

    async def broker_counts(self) -> tuple[int, int]:
        queue = await self.channel.declare_queue(self.queue.name, passive=True)
        dlq = await self.channel.declare_queue(self.dlq.name, passive=True)
        return queue.declaration_result.message_count or 0, dlq.declaration_result.message_count or 0

    def longest_hold(self) -> float:
        """Longest gap between a record's consecutive deliveries: how long the loader held it."""
        arrivals: dict[str, list[float]] = {}
        for record_id, _count, at in self.deliveries:
            arrivals.setdefault(record_id, []).append(at)
        gaps = [later - earlier for times in arrivals.values() for earlier, later in itertools.pairwise(times)]
        return max(gaps, default=0.0)


async def until(predicate: Any, timeout: float, what: str) -> None:
    deadline = time.monotonic() + timeout
    while not await predicate():
        assert time.monotonic() < deadline, f"timed out after {timeout:g}s waiting for {what}"
        await asyncio.sleep(0.05)


async def build(monkeypatch: pytest.MonkeyPatch, *, max_requeues: int | None) -> AsyncIterator[Stack]:
    driver = AsyncResilientNeo4jDriver(
        uri=os.environ["NEO4J_URI"], auth=(os.environ.get("NEO4J_INTEGRATION_USER", "neo4j"), os.environ["NEO4J_INTEGRATION_PASSWORD"])
    )
    config = BatchConfig(
        batch_size=10,
        min_batch_size=1,
        flush_interval=FLUSH_INTERVAL_S,
        backoff_initial=0.05,
        backoff_max=BACKOFF_MAX_S,
        max_hold_s=MAX_HOLD_S,
        max_requeues=max_requeues,
    )
    processor = Neo4jBatchProcessor(driver, config)
    monkeypatch.setattr(graphinator, "graph", driver)
    monkeypatch.setattr(graphinator, "BATCH_MODE", True)
    monkeypatch.setattr(graphinator, "batch_processor", processor)
    monkeypatch.setattr(graphinator, "shutdown_requested", False)

    connection = await aio_pika.connect(os.environ["RABBITMQ_URL"])
    channel = await connection.channel(publisher_confirms=True)
    await channel.set_qos(prefetch_count=10)
    name = f"gm-zo65-{uuid.uuid4().hex[:8]}"
    exchange = await channel.declare_exchange(f"{name}.dlx", aio_pika.ExchangeType.FANOUT, durable=True)
    dlq = await channel.declare_queue(f"{name}.dlq", durable=True, arguments={"x-queue-type": "classic"})
    await dlq.bind(exchange)
    queue = await channel.declare_queue(
        name, durable=True, arguments={"x-queue-type": "quorum", "x-delivery-limit": 20, "x-dead-letter-exchange": f"{name}.dlx"}
    )
    stack = Stack(channel, queue, dlq, driver, processor)

    real_write = processor._process_artists_batch

    async def write(messages: Any) -> Any:
        if stack.outage:
            raise ServiceUnavailable("Unable to retrieve routing information")
        return await real_write(messages)

    monkeypatch.setattr(processor, "_process_artists_batch", write)
    handler = graphinator.make_message_handler("artists", "name", "Unknown Artist", process_artist)

    async def handle(message: aio_pika.abc.AbstractIncomingMessage) -> None:
        record_id = str(loads(message.body)["id"])
        stack.deliveries.append((record_id, int((message.headers or {}).get("x-acquired-count", 0)), time.monotonic()))
        await handler(message)

    periodic = asyncio.create_task(processor.periodic_flush())
    tag = await queue.consume(handle)
    try:
        yield stack
    finally:
        processor.shutdown()
        await asyncio.wait_for(asyncio.gather(periodic, return_exceptions=True), 10)
        await queue.cancel(tag)
        async with driver.session(database="neo4j") as session:
            await session.run("MATCH (a:Artist) WHERE a.id IN $ids DETACH DELETE a", ids=list(IDS))
        await queue.delete(if_unused=False, if_empty=False)
        await dlq.delete(if_unused=False, if_empty=False)
        await exchange.delete()
        await connection.close()
        await driver.close()


@pytest_asyncio.fixture
async def stack(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[Stack]:
    async for built in build(monkeypatch, max_requeues=None):
        yield built


@pytest_asyncio.fixture
async def budgeted_stack(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[Stack]:
    async for built in build(monkeypatch, max_requeues=2):
        yield built


async def test_a_long_outage_holds_within_the_bound_and_loses_no_healthy_record(stack: Stack) -> None:
    outage_s = 3.0
    started = time.monotonic()
    for record_id in IDS:
        await stack.publish(record_id)

    await asyncio.sleep(outage_s)
    trips = len(stack.deliveries) - len(IDS)
    longest = stack.longest_hold()
    print(f"\nOUTAGE: {outage_s:g}s down, {len(IDS)} records, {trips} broker requeues, longest hold {longest:.2f}s (bound {HOLD_CEILING_S:.2f}s)")
    assert trips >= len(IDS) * 2, "held deliveries must have gone back to the broker repeatedly during the outage"
    assert longest <= HOLD_CEILING_S
    assert await stack.stored() == set(), "nothing may be written while Neo4j is down"

    stack.outage = False

    async def landed() -> bool:
        return await stack.stored() == set(IDS)

    await until(landed, 30, "every held record to be written once Neo4j is back")

    async def drained() -> bool:
        return await stack.broker_counts() == (0, 0)

    await until(drained, 10, "the queue to drain with nothing dead-lettered")
    print(f"RECOVERY: all {len(IDS)} records stored {time.monotonic() - started:.1f}s after the first publish; queue and DLQ empty")


async def test_a_record_held_past_the_requeue_budget_is_dead_lettered_not_lost(budgeted_stack: Stack) -> None:
    await budgeted_stack.publish(IDS[0])

    async def dead_lettered() -> bool:
        return (await budgeted_stack.broker_counts())[1] == 1

    await until(dead_lettered, 30, "the record to be dead-lettered when its requeue budget is spent")
    counts = [count for _id, count, _at in budgeted_stack.deliveries]
    assert counts == [0, 1, 2], "the broker's x-acquired-count must show each trip, and the budget must end at 2"
    message = await budgeted_stack.dlq.get(fail=True, timeout=5)
    await message.ack()
    assert (message.headers or {}).get("x-first-death-reason") == "rejected"
