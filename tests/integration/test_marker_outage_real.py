"""End-of-type markers through a Neo4j failure, on real RabbitMQ and real Neo4j.

The broker queue is declared like production: quorum, ``x-delivery-limit`` 20, with a dead-letter
exchange. Neo4j is reached through a TCP proxy this test can cut, so the driver sees a real outage
without the test needing control of the database container.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import time
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

import aio_pika
import pytest
import pytest_asyncio
from common.neo4j_resilient import AsyncResilientNeo4jDriver
from common.outage_backoff import OutageBackoff
from neo4j import AsyncDriver, AsyncGraphDatabase
from orjson import dumps

import graphinator.graphinator as graphinator
from graphinator.batch_processor import BatchConfig, Neo4jBatchProcessor


if TYPE_CHECKING:
    from collections.abc import AsyncIterator


pytestmark = [pytest.mark.asyncio, pytest.mark.integration, pytest.mark.skipif(not os.environ.get("RABBITMQ_URL"), reason="RABBITMQ_URL not set")]

# Preserved before the default suite fixture removes the sleep: pacing is what these tests prove.
_OUTAGE_WAIT = OutageBackoff.wait
_LOAD_EXTRACTION_SIGNALS = graphinator._load_extraction_signals
_PERSIST_EXTRACTION_SIGNALS = graphinator._persist_extraction_signals

VERSION = "20261105"
RECORDS = 40


class BoltProxy:
    """A TCP proxy to Neo4j that can drop every connection and refuse new ones."""

    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self.listen_port = 0
        self._server: asyncio.Server | None = None
        self._writers: set[asyncio.StreamWriter] = set()

    async def open(self) -> None:
        self._server = await asyncio.start_server(self._accept, "127.0.0.1", self.listen_port)
        self.listen_port = self._server.sockets[0].getsockname()[1]

    async def cut(self) -> None:
        if self._server is not None:
            self._server.close()
            self._server = None
        for writer in list(self._writers):
            writer.transport.abort()
        self._writers.clear()

    async def _accept(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        upstream_reader, upstream_writer = await asyncio.open_connection(self.host, self.port)
        self._writers.update((writer, upstream_writer))
        await asyncio.gather(self._pipe(reader, upstream_writer), self._pipe(upstream_reader, writer))

    async def _pipe(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        with contextlib.suppress(OSError, RuntimeError):
            while data := await reader.read(65536):
                writer.write(data)
                await writer.drain()
        writer.close()
        self._writers.discard(writer)


@dataclass
class Stack:
    proxy: BoltProxy
    channel: aio_pika.abc.AbstractChannel
    queue_name: str
    dlq: aio_pika.abc.AbstractQueue
    marker_deliveries: list[float] = field(default_factory=list)

    async def publish(self, body: dict[str, Any]) -> None:
        await self.channel.default_exchange.publish(
            aio_pika.Message(dumps(body), delivery_mode=aio_pika.DeliveryMode.PERSISTENT), routing_key=self.queue_name
        )

    async def depth(self, name: str) -> int:
        queue = await self.channel.declare_queue(name, passive=True)
        assert queue.declaration_result.message_count is not None
        return queue.declaration_result.message_count


async def until(predicate: Any, timeout: float) -> None:
    async with asyncio.timeout(timeout):
        while not await predicate():
            await asyncio.sleep(0.05)


@pytest_asyncio.fixture
async def stack(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[Stack]:
    """The real artists handler consuming a production-shaped queue, with Neo4j behind the proxy."""
    uri = urlsplit(os.environ["NEO4J_URI"])
    assert uri.hostname is not None
    assert uri.port is not None
    proxy = BoltProxy(uri.hostname, uri.port)
    await proxy.open()
    driver = AsyncResilientNeo4jDriver(
        uri=f"bolt://127.0.0.1:{proxy.listen_port}",
        auth=(os.environ.get("NEO4J_INTEGRATION_USER", "neo4j"), os.environ["NEO4J_INTEGRATION_PASSWORD"]),
        max_retries=1,
        connection_acquisition_timeout=2.0,
        connection_timeout=2.0,
    )
    processor = Neo4jBatchProcessor(driver, BatchConfig(batch_size=RECORDS * 2, flush_interval=600.0))
    processor.last_flush["artists"] = time.time()
    monkeypatch.setattr(OutageBackoff, "wait", _OUTAGE_WAIT)
    monkeypatch.setattr(graphinator, "BATCH_MODE", True)
    monkeypatch.setattr(graphinator, "graph", driver)
    monkeypatch.setattr(graphinator, "batch_processor", processor)
    monkeypatch.setattr(graphinator, "CONSUMER_CANCEL_DELAY", 0)
    monkeypatch.setattr(graphinator, "_load_extraction_signals", _LOAD_EXTRACTION_SIGNALS)
    monkeypatch.setattr(graphinator, "_persist_extraction_signals", _PERSIST_EXTRACTION_SIGNALS)
    monkeypatch.setattr(graphinator, "extraction_complete_version", None)
    monkeypatch.setattr(graphinator, "extraction_complete_signals", set())
    monkeypatch.setattr(graphinator, "shutdown_requested", False)
    monkeypatch.setattr(graphinator, "completed_files", set())
    graphinator.marker_backoff.reset()

    connection = await aio_pika.connect(os.environ["RABBITMQ_URL"])
    channel = await connection.channel(publisher_confirms=True)
    await channel.set_qos(prefetch_count=RECORDS * 2)
    name = f"gm-t6ut-{uuid.uuid4().hex[:8]}"
    exchange = await channel.declare_exchange(f"{name}.dlx", aio_pika.ExchangeType.FANOUT, durable=True)
    dlq = await channel.declare_queue(f"{name}.dlq", durable=True, arguments={"x-queue-type": "classic"})
    await dlq.bind(exchange)
    queue = await channel.declare_queue(
        name, durable=True, arguments={"x-queue-type": "quorum", "x-delivery-limit": 20, "x-dead-letter-exchange": f"{name}.dlx"}
    )
    built = Stack(proxy, channel, name, dlq)
    loop = asyncio.get_running_loop()

    async def handle(message: aio_pika.abc.AbstractIncomingMessage) -> None:
        if b'"extraction_complete"' in message.body or b'"file_complete"' in message.body:
            built.marker_deliveries.append(loop.time())
        await graphinator.on_artist_message(message)

    tag = await queue.consume(handle)
    try:
        yield built
    finally:
        with contextlib.suppress(Exception):
            await queue.cancel(tag)
        await proxy.cut()
        cleanup = direct_driver()
        async with cleanup.session(database="neo4j") as session:
            await session.run("MATCH (node) DETACH DELETE node")
        await cleanup.close()
        await driver.close()
        await queue.delete(if_unused=False, if_empty=False)
        await dlq.delete(if_unused=False, if_empty=False)
        await exchange.delete()
        await connection.close()


def artist(index: int) -> dict[str, Any]:
    return {"id": str(index), "name": f"Artist {index}", "sha256": f"{index:064d}"}


def direct_driver() -> AsyncDriver:
    """Past the proxy and the resilient driver's breaker, for test assertions and cleanup."""
    return AsyncGraphDatabase.driver(
        os.environ["NEO4J_URI"], auth=(os.environ.get("NEO4J_INTEGRATION_USER", "neo4j"), os.environ["NEO4J_INTEGRATION_PASSWORD"])
    )


async def stored() -> tuple[int, list[str]]:
    driver = direct_driver()
    try:
        async with driver.session(database="neo4j") as session:
            artists = await (await session.run("MATCH (a:Artist) RETURN count(a) AS c")).single()
            latch = await (await session.run("MATCH (m:ExtractionCompletion {version: $v}) RETURN m.signals AS signals", v=VERSION)).single()
    finally:
        await driver.close()
    return artists["c"], sorted(latch["signals"]) if latch else []


async def load_records(stack: Stack) -> None:
    for index in range(RECORDS):
        await stack.publish(artist(index))

    async def held() -> bool:
        assert graphinator.batch_processor is not None
        return graphinator.batch_processor._engine.snapshot()["pending"]["artists"] == RECORDS

    await until(held, 15)


@pytest.mark.timeout(180)
@pytest.mark.parametrize("marker", ["file_complete", "extraction_complete"])
async def test_a_short_failure_at_the_end_of_a_type_loses_no_marker(stack: Stack, marker: str) -> None:
    """Records pending, Neo4j down for 15 s: the marker waits it out and the signal lands."""
    await load_records(stack)
    await stack.proxy.cut()
    await stack.publish({"type": marker, "version": VERSION, "total_processed": RECORDS})
    await asyncio.sleep(15)
    during_outage = len(stack.marker_deliveries)
    await stack.proxy.open()

    async def landed() -> bool:
        artists, signals = await stored()
        done = signals == ["artists"] if marker == "extraction_complete" else "artists" in graphinator.completed_files
        return done and artists == RECORDS

    await until(landed, 120)
    assert await stack.depth(f"{stack.queue_name}.dlq") == 0
    assert during_outage <= 5, f"marker redelivered {during_outage} times in a 15 s outage"


@pytest.mark.timeout(180)
async def test_a_failure_writing_the_signal_is_paced_and_the_signal_lands(stack: Stack) -> None:
    """Nothing pending, so the flush passes and the latch write itself is what fails."""
    await stack.proxy.cut()
    await stack.publish({"type": "extraction_complete", "version": VERSION})
    await asyncio.sleep(15)
    during_outage = len(stack.marker_deliveries)
    await stack.proxy.open()

    async def landed() -> bool:
        return (await stored())[1] == ["artists"]

    await until(landed, 120)
    assert await stack.depth(f"{stack.queue_name}.dlq") == 0
    assert during_outage <= 8, f"marker redelivered {during_outage} times in a 15 s outage"


@pytest.mark.timeout(180)
async def test_a_persistent_failure_dead_letters_the_marker(stack: Stack, monkeypatch: pytest.MonkeyPatch) -> None:
    """The real broker's x-acquired-count ends it: exactly budget + 1 deliveries, then the DLQ."""
    monkeypatch.setattr(graphinator, "MARKER_MAX_REQUEUES", 3)
    await load_records(stack)
    await stack.proxy.cut()
    await stack.publish({"type": "extraction_complete", "version": VERSION})

    async def dead() -> bool:
        return await stack.depth(f"{stack.queue_name}.dlq") == 1

    await until(dead, 300)
    assert len(stack.marker_deliveries) == 4
    message = await stack.dlq.get(fail=True, timeout=5)
    assert message.headers["x-first-death-reason"] == "rejected"
    await message.ack()
