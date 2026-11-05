"""A record that cannot succeed, on real RabbitMQ and real Neo4j.

The queue is declared like production. RabbitMQ does not spend ``x-delivery-limit`` on
``nack(requeue=True)``, so before this a record that raised looped hundreds of times a second,
forever. A body that cannot be read now ends on its first delivery; anything else is paced, then
ends at the requeue budget, counted from the broker's ``x-acquired-count``.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import aio_pika
import pytest
import pytest_asyncio
from common.neo4j_resilient import AsyncResilientNeo4jDriver
from common.outage_backoff import OutageBackoff
from orjson import dumps

import graphinator.graphinator as graphinator
from graphinator.batch_processor import BatchConfig, Neo4jBatchProcessor


if TYPE_CHECKING:
    from collections.abc import AsyncIterator


pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.integration,
    pytest.mark.timeout(60),
    pytest.mark.skipif(not os.environ.get("RABBITMQ_URL"), reason="RABBITMQ_URL not set"),
]

# Preserved before the default suite fixture removes the sleep: pacing is what these tests prove.
_OUTAGE_WAIT = OutageBackoff.wait


async def broken(_tx: Any, _record: dict[str, Any]) -> bool:
    raise ValueError("a record the projection cannot handle")


@dataclass
class Stack:
    channel: aio_pika.abc.AbstractChannel
    queue: aio_pika.abc.AbstractQueue
    dlq: aio_pika.abc.AbstractQueue
    deliveries: list[dict[str, Any]] = field(default_factory=list)

    async def publish(self, body: bytes) -> None:
        await self.channel.default_exchange.publish(
            aio_pika.Message(body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT), routing_key=self.queue.name
        )

    async def dead_letter_reason(self, wait_s: float = 5.0) -> str | None:
        deadline = asyncio.get_running_loop().time() + wait_s
        while (message := await self.dlq.get(fail=False, timeout=5)) is None:
            if asyncio.get_running_loop().time() >= deadline:
                return None
            await asyncio.sleep(0.05)
        await message.ack()
        reason = (message.headers or {}).get("x-first-death-reason")
        return reason if isinstance(reason, str) else None


@pytest_asyncio.fixture(params=[True, False], ids=["batch", "non-batch"])
async def stack(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[Stack]:
    driver = AsyncResilientNeo4jDriver(
        uri=os.environ["NEO4J_URI"], auth=(os.environ.get("NEO4J_INTEGRATION_USER", "neo4j"), os.environ["NEO4J_INTEGRATION_PASSWORD"])
    )
    monkeypatch.setattr(OutageBackoff, "wait", _OUTAGE_WAIT)
    monkeypatch.setattr(graphinator, "graph", driver)
    monkeypatch.setattr(graphinator, "BATCH_MODE", request.param)
    monkeypatch.setattr(graphinator, "batch_processor", Neo4jBatchProcessor(driver, BatchConfig()) if request.param else None)
    monkeypatch.setattr(graphinator, "shutdown_requested", False)
    graphinator.outage_backoff.reset()

    connection = await aio_pika.connect(os.environ["RABBITMQ_URL"])
    channel = await connection.channel(publisher_confirms=True)
    await channel.set_qos(prefetch_count=4)
    name = f"gm-g3r3-{uuid.uuid4().hex[:8]}"
    exchange = await channel.declare_exchange(f"{name}.dlx", aio_pika.ExchangeType.FANOUT, durable=True)
    dlq = await channel.declare_queue(f"{name}.dlq", durable=True, arguments={"x-queue-type": "classic"})
    await dlq.bind(exchange)
    queue = await channel.declare_queue(
        name, durable=True, arguments={"x-queue-type": "quorum", "x-delivery-limit": 20, "x-dead-letter-exchange": f"{name}.dlx"}
    )
    built = Stack(channel, queue, dlq)
    handler = graphinator.make_message_handler("artists", "name", "Unknown Artist", broken)

    async def handle(message: aio_pika.abc.AbstractIncomingMessage) -> None:
        built.deliveries.append(dict(message.headers or {}))
        await handler(message)

    tag = await queue.consume(handle)
    try:
        yield built
    finally:
        await queue.cancel(tag)
        await queue.delete(if_unused=False, if_empty=False)
        await dlq.delete(if_unused=False, if_empty=False)
        await exchange.delete()
        await connection.close()
        await driver.close()


@pytest.mark.parametrize("body", [b"not json", b"[1, 2]", b'"text"', b"\xff\xfe"], ids=["garbage", "array", "string", "bytes"])
async def test_an_unreadable_body_dead_letters_on_its_first_delivery(stack: Stack, body: bytes) -> None:
    await stack.publish(body)

    assert await stack.dead_letter_reason() == "rejected"
    assert len(stack.deliveries) == 1


async def test_a_record_that_keeps_raising_is_paced_and_ends_at_the_budget(stack: Stack, monkeypatch: pytest.MonkeyPatch) -> None:
    if graphinator.BATCH_MODE:
        pytest.skip("in batch mode the projection does not raise through the handler")
    monkeypatch.setattr(graphinator, "RECORD_MAX_REQUEUES", 2)
    started = asyncio.get_running_loop().time()
    await stack.publish(dumps({"id": "1", "name": "Poison", "sha256": "a" * 64}))

    assert await stack.dead_letter_reason(wait_s=20.0) == "rejected"
    assert len(stack.deliveries) == 3
    assert stack.deliveries[-1]["x-acquired-count"] == 2
    assert asyncio.get_running_loop().time() - started >= 6.0  # 2 s, then 4 s, of backoff
