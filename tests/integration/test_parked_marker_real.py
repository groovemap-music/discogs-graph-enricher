"""extraction_complete parked on cancelled consumers, on real RabbitMQ 4.4 quorum queues and real Neo4j.

The 2026-11-06 ingest: artists, labels and masters finish and their consumers are cancelled while
releases still runs; the extractor then publishes extraction_complete to all four, and the three
signals sat on queues with no consumer until the idle reconnect (up to QUEUE_CHECK_INTERVAL, 3600 s).
The queues, dead-letter arguments, handlers, cancellation, checker and Neo4j latch are the real ones;
only the post-import maintenance sweep is replaced by a recorder, because the latch is what is timed.

LATENCY_BASELINE=1 disables the new probe to measure the old behaviour on the same stack.
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import TYPE_CHECKING, Any

import aio_pika
import aiormq
import pytest
import pytest_asyncio
from common import AsyncResilientRabbitMQ
from common.neo4j_resilient import AsyncResilientNeo4jDriver
from neo4j import AsyncDriver, AsyncGraphDatabase
from orjson import dumps, loads

import graphinator.graphinator as graphinator


if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable


pytestmark = [pytest.mark.asyncio, pytest.mark.integration, pytest.mark.skipif(not os.environ.get("RABBITMQ_URL"), reason="RABBITMQ_URL not set")]

# Preserved before the default suite fixture replaces them: the real latch is what is timed.
_LOAD_EXTRACTION_SIGNALS = graphinator._load_extraction_signals
_PERSIST_EXTRACTION_SIGNALS = graphinator._persist_extraction_signals

VERSION = "20261106"
PARKED = ("artists", "labels", "masters")
LONG_RUNNING = "releases"
CHECK_INTERVAL = 1
MARKER_INTERVAL = 2
# One probe tick, a resubscribe, the Neo4j latch write and the ack, with margin. The incident waited up to 3600 s.
PARKED_MARKER_BOUND = MARKER_INTERVAL + 6
BASELINE_QUEUE_CHECK_INTERVAL = 60
BASELINE_DEADLINE = 150


class Stack:
    def __init__(self, url: str, broker: aiormq.abc.AbstractChannel, publisher: aio_pika.abc.AbstractChannel) -> None:
        self.url = url
        self.broker = broker
        self.publisher = publisher
        self.recoveries = 0
        self.handled: list[tuple[str, str | None]] = []

    async def consumers(self, data_type: str) -> int:
        result = await self.broker.queue_declare(graphinator.catalog_queue_name("graphinator", data_type), passive=True)
        return int(result.consumer_count)

    async def ready(self, data_type: str) -> int:
        result = await self.broker.queue_declare(graphinator.catalog_queue_name("graphinator", data_type), passive=True)
        return int(result.message_count)

    async def publish(self, data_type: str, **body: Any) -> None:
        exchange = await self.publisher.get_exchange(graphinator.catalog_exchange_name(data_type))
        await exchange.publish(aio_pika.Message(dumps(body), delivery_mode=aio_pika.DeliveryMode.PERSISTENT), routing_key="")

    def was_handled(self, data_type: str, message_type: str) -> bool:
        return (data_type, message_type) in self.handled

    async def attach(self) -> None:
        """Connect and consume every type exactly as the recovery path leaves the service."""
        graphinator.active_connection = await graphinator.rabbitmq_manager.connect()
        graphinator.active_channel = await graphinator.active_connection.channel()
        await graphinator.active_channel.set_qos(prefetch_count=graphinator.channel_prefetch())
        graphinator.queues = {}
        for data_type in graphinator.DATA_TYPES:
            dlx_name = graphinator.catalog_dead_letter_exchange_name("graphinator", data_type)
            exchange = await graphinator.active_channel.declare_exchange(
                graphinator.catalog_exchange_name(data_type), graphinator.AMQP_EXCHANGE_TYPE, durable=True, auto_delete=False
            )
            dlx = await graphinator.active_channel.declare_exchange(dlx_name, graphinator.AMQP_EXCHANGE_TYPE, durable=True, auto_delete=False)
            dlq = await graphinator.active_channel.declare_queue(
                name=graphinator.catalog_dead_letter_queue_name("graphinator", data_type),
                durable=True,
                auto_delete=False,
                arguments={"x-queue-type": "classic"},
            )
            await dlq.bind(dlx)
            queue = await graphinator.active_channel.declare_queue(
                name=graphinator.catalog_queue_name("graphinator", data_type),
                durable=True,
                auto_delete=False,
                arguments={"x-queue-type": "quorum", "x-dead-letter-exchange": dlx_name, "x-delivery-limit": 20},
            )
            await queue.bind(exchange)
            graphinator.queues[data_type] = queue
            graphinator.consumer_tags[data_type] = await queue.consume(graphinator.HANDLERS[data_type], consumer_tag=f"graphinator-{data_type}")


async def until(predicate: Callable[[], Awaitable[bool]], timeout: float, what: str) -> None:
    deadline = time.monotonic() + timeout
    while not await predicate():
        assert time.monotonic() < deadline, f"timed out waiting for {what}"
        await asyncio.sleep(0.1)


def direct_driver() -> AsyncDriver:
    return AsyncGraphDatabase.driver(
        os.environ["NEO4J_URI"], auth=(os.environ.get("NEO4J_INTEGRATION_USER", "neo4j"), os.environ["NEO4J_INTEGRATION_PASSWORD"])
    )


async def latched_signals() -> list[str]:
    driver = direct_driver()
    try:
        async with driver.session(database="neo4j") as session:
            row = await (await session.run("MATCH (m:ExtractionCompletion {version: $v}) RETURN m.signals AS signals", v=VERSION)).single()
    finally:
        await driver.close()
    return sorted(row["signals"]) if row else []


@pytest_asyncio.fixture
async def stack(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[Stack]:
    url = os.environ["RABBITMQ_URL"]
    observer = await aiormq.connect(url)
    publishing = await aio_pika.connect(url)
    state = Stack(url, await observer.channel(), await publishing.channel())

    driver = AsyncResilientNeo4jDriver(
        uri=os.environ["NEO4J_URI"],
        auth=(os.environ.get("NEO4J_INTEGRATION_USER", "neo4j"), os.environ["NEO4J_INTEGRATION_PASSWORD"]),
        max_retries=1,
    )
    maintenance: list[str | None] = []
    real_recover = graphinator._recover_consumers

    async def recover() -> None:
        state.recoveries += 1
        await real_recover()

    for data_type in graphinator.DATA_TYPES:
        real_handler = graphinator.HANDLERS[data_type]

        def wrap(handler: Any, name: str) -> Any:
            async def handle(message: Any) -> None:
                kind = loads(message.body).get("type")
                await handler(message)
                state.handled.append((name, kind))

            return handle

        monkeypatch.setitem(graphinator.HANDLERS, data_type, wrap(real_handler, data_type))

    monkeypatch.setattr(graphinator, "graph", driver)
    monkeypatch.setattr(graphinator, "BATCH_MODE", False)
    monkeypatch.setattr(graphinator, "batch_processor", None)
    monkeypatch.setattr(graphinator, "_load_extraction_signals", _LOAD_EXTRACTION_SIGNALS)
    monkeypatch.setattr(graphinator, "_persist_extraction_signals", _PERSIST_EXTRACTION_SIGNALS)
    monkeypatch.setattr(graphinator, "extraction_complete_version", None)
    monkeypatch.setattr(graphinator, "extraction_complete_signals", set())
    monkeypatch.setattr(graphinator, "shutdown_requested", False)
    monkeypatch.setattr(graphinator, "_recover_consumers", recover)
    monkeypatch.setattr(graphinator, "STUCK_CHECK_INTERVAL", CHECK_INTERVAL)
    monkeypatch.setattr(graphinator, "start_post_import_maintenance", lambda version=None: maintenance.append(version))
    state.maintenance = maintenance  # type: ignore[attr-defined]
    monkeypatch.setattr(
        graphinator,
        "rabbitmq_manager",
        AsyncResilientRabbitMQ(connection_url=url, max_retries=3, heartbeat=600, connection_attempts=3, retry_delay=1.0),
    )
    graphinator.marker_backoff.reset()
    await state.attach()
    checker = asyncio.create_task(graphinator.periodic_queue_checker())
    try:
        yield state
    finally:
        graphinator.shutdown_requested = True
        checker.cancel()
        await asyncio.gather(checker, return_exceptions=True)
        for task in list(graphinator.consumer_cancel_tasks.values()):
            task.cancel()
        graphinator.consumer_cancel_tasks.clear()
        if graphinator.active_connection is not None:
            await asyncio.wait_for(graphinator.active_connection.close(), timeout=10)
        for data_type in graphinator.DATA_TYPES:
            await state.broker.queue_delete(graphinator.catalog_queue_name("graphinator", data_type))
            await state.broker.queue_delete(graphinator.catalog_dead_letter_queue_name("graphinator", data_type))
        cleanup = direct_driver()
        async with cleanup.session(database="neo4j") as session:
            await session.run("MATCH (node) DETACH DELETE node")
        await cleanup.close()
        await driver.close()
        await publishing.close()
        await observer.close()


@pytest.mark.timeout(BASELINE_DEADLINE + 30)
async def test_extraction_complete_parked_on_cancelled_consumers_lands_within_the_probe_interval(
    stack: Stack, monkeypatch: pytest.MonkeyPatch
) -> None:
    baseline = os.environ.get("LATENCY_BASELINE") == "1"
    monkeypatch.setattr(graphinator, "CONSUMER_CANCEL_DELAY", 1)
    monkeypatch.setattr(graphinator, "MARKER_CHECK_INTERVAL", MARKER_INTERVAL)
    monkeypatch.setattr(graphinator, "QUEUE_CHECK_INTERVAL", BASELINE_QUEUE_CHECK_INTERVAL)
    if baseline:

        async def disabled() -> bool:
            return False

        monkeypatch.setattr(graphinator, "recover_parked_markers", disabled)

    # The checker's first idle check fires at once (its last-check clock starts at 0). Spend it, so the run
    # starts like a production ingest, hours after the last full check: all complete, broker closed.
    graphinator.completed_files.update(graphinator.DATA_TYPES)
    graphinator.extraction_signalled.update(graphinator.DATA_TYPES)
    await graphinator.close_rabbitmq_connection()
    await until(lambda: _recovered(stack), 10, "the checker's first idle check")
    await asyncio.sleep(2 * CHECK_INTERVAL)
    assert graphinator.active_connection is None
    graphinator.completed_files.clear()
    graphinator.extraction_signalled.clear()
    await stack.attach()

    for data_type in PARKED:
        await stack.publish(data_type, type="file_complete", total_processed=0)

    async def parked_consumers_cancelled() -> bool:
        return not any([await stack.consumers(data_type) for data_type in PARKED])

    await until(parked_consumers_cancelled, 10, "the three finished types to have their consumers cancelled")
    assert await stack.consumers(LONG_RUNNING) == 1, "the long-running type keeps its consumer"
    # An idle cancelled consumer holds nothing: the queues are empty and no tag is tracked.
    assert not any([await stack.ready(data_type) for data_type in PARKED])
    assert not any(graphinator.consumer_tags.get(data_type) for data_type in PARKED)

    await stack.publish(LONG_RUNNING, type="file_complete", total_processed=0)
    started = time.monotonic()
    for data_type in graphinator.DATA_TYPES:
        await stack.publish(data_type, type="extraction_complete", version=VERSION, started_at="2026-11-06T00:00:00Z")

    async def latched() -> bool:
        return await latched_signals() == sorted(graphinator.DATA_TYPES) and bool(stack.maintenance)  # type: ignore[attr-defined]

    await until(latched, BASELINE_DEADLINE if baseline else PARKED_MARKER_BOUND, "all four signals latched in Neo4j")
    elapsed = time.monotonic() - started
    print(f"\nPARKED_MARKER_LATENCY mode={'baseline-no-probe' if baseline else 'probe'} seconds={elapsed:.2f}")

    assert set(graphinator.extraction_signalled) == set(graphinator.DATA_TYPES)
    health = graphinator.get_health_data()
    assert "awaiting_extraction_complete" in health
    # The resubscribed consumers cancel again after the marker: the bound on what they hold is unchanged.
    await until(parked_consumers_cancelled, 10, "the resubscribed consumers to be cancelled again")
    assert not any([await stack.ready(data_type) for data_type in graphinator.DATA_TYPES])


async def _recovered(stack: Stack) -> bool:
    return stack.recoveries > 0
