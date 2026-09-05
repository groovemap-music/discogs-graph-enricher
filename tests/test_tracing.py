"""End-to-end tracing tests: the consumer span joins the extractor's trace, and a batch flush
is one span linked to the deliveries it wrote.

These drive the real handler and the real batch processor rather than the span helpers on
their own, because the acceptance is about where the spans sit in the pipeline: a delivery
must be *processed inside* its CONSUMER span, and the Neo4j writes must nest under the flush
span that covers them.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from common.tracing import MAX_FLUSH_LINKS, db_span
from opentelemetry.trace import SpanKind

from graphinator import telemetry as gm_telemetry
from graphinator.batch_processor import BatchConfig, Neo4jBatchProcessor, PendingMessage
from graphinator.graphinator import on_artist_message


if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from tests.conftest import SpanCollector


# A fixed upstream context, exactly as discogs-ingestion's `publish` span would have injected
# it into the AMQP headers. Version 00, sampled.
UPSTREAM_TRACE_ID = 0x4BF92F3577B34DA6A3CE929D0E0E4736
UPSTREAM_SPAN_ID = 0x00F067AA0BA902B7
TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

CONSUMER_TAG = "graphinator-artists"
QUEUE_NAME = "groovemap-discogs-graphinator-artists"


class FakeIncomingMessage:
    """The delivery surface the handler touches: a body, AMQP headers, and settlement."""

    def __init__(self, body: bytes, headers: Mapping[str, Any] | None, consumer_tag: str) -> None:
        self.body = body
        self.headers = dict(headers) if headers is not None else {}
        self.consumer_tag = consumer_tag
        self.ack = AsyncMock()
        self.nack = AsyncMock()


class FakeQueue:
    """A queue that hands deliveries straight to the registered consumer callback."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.consumer_tag = ""
        self._handler: Callable[[FakeIncomingMessage], Any] | None = None

    def subscribe(self, handler: Callable[[FakeIncomingMessage], Any], consumer_tag: str = "") -> str:
        """Register the consumer callback, as ``aio_pika`` queue.consume() would."""
        self._handler = handler
        self.consumer_tag = consumer_tag
        return consumer_tag

    async def consume(self, handler: Callable[[FakeIncomingMessage], Any], consumer_tag: str = "") -> str:
        return self.subscribe(handler, consumer_tag)

    async def publish(self, record: dict[str, Any], headers: Mapping[str, Any] | None = None) -> FakeIncomingMessage:
        """Deliver one record to the consumer, as the broker would."""
        assert self._handler is not None, "no consumer registered"
        message = FakeIncomingMessage(json.dumps(record).encode(), headers, self.consumer_tag)
        await self._handler(message)
        return message


class FakeChannel:
    """The channel surface these tests need: declaring a queue to consume from."""

    def __init__(self) -> None:
        self.queues: dict[str, FakeQueue] = {}

    def declare(self, name: str) -> FakeQueue:
        """Declare a queue on this channel and return it."""
        queue = FakeQueue(name)
        self.queues[name] = queue
        return queue

    async def declare_queue(self, name: str, **_kwargs: Any) -> FakeQueue:
        return self.declare(name)


@pytest.fixture
def artist_record() -> dict[str, Any]:
    return {"id": "123456", "name": "Test Artist", "sha256": "abc123"}


@pytest.fixture
def writing_neo4j_driver() -> MagicMock:
    """A driver whose execute_write actually runs the transaction function."""
    driver = MagicMock()
    session = AsyncMock()
    context = AsyncMock()
    context.__aenter__ = AsyncMock(return_value=session)
    context.__aexit__ = AsyncMock(return_value=None)
    driver.session = MagicMock(return_value=context)

    async def execute_write(func: Any) -> Any:
        transaction = MagicMock()
        transaction.run = AsyncMock()
        transaction.run.return_value.single = AsyncMock(return_value=None)
        return await func(transaction)

    session.execute_write.side_effect = execute_write
    return driver


@pytest.fixture
def artists_queue() -> FakeQueue:
    """A fake channel with the artists queue declared and the real handler consuming it."""
    queue = FakeChannel().declare(QUEUE_NAME)
    queue.subscribe(on_artist_message, consumer_tag=CONSUMER_TAG)
    return queue


class TestConsumerSpanThroughAFakeChannel:
    """Every consumed message is processed inside `process {queue}`, joined to its publisher."""

    @pytest.mark.asyncio
    async def test_the_span_joins_the_trace_carried_in_the_message_headers(
        self,
        spans: SpanCollector,
        artists_queue: FakeQueue,
        artist_record: dict[str, Any],
        writing_neo4j_driver: MagicMock,
    ) -> None:
        with patch("graphinator.graphinator.graph", writing_neo4j_driver):
            message = await artists_queue.publish(artist_record, headers={"traceparent": TRACEPARENT})

        message.ack.assert_awaited_once()
        span = spans.only(f"process {CONSUMER_TAG}")
        assert span.kind is SpanKind.CONSUMER
        assert span.context.trace_id == UPSTREAM_TRACE_ID
        assert span.parent is not None
        assert span.parent.span_id == UPSTREAM_SPAN_ID
        assert dict(span.attributes) == {
            "messaging.system": "rabbitmq",
            "messaging.destination.name": CONSUMER_TAG,
            "messaging.operation.name": "process",
        }

    @pytest.mark.asyncio
    async def test_a_message_without_headers_starts_a_new_trace(
        self,
        spans: SpanCollector,
        artists_queue: FakeQueue,
        artist_record: dict[str, Any],
        writing_neo4j_driver: MagicMock,
    ) -> None:
        with patch("graphinator.graphinator.graph", writing_neo4j_driver):
            await artists_queue.publish(artist_record)

        span = spans.only(f"process {CONSUMER_TAG}")
        assert span.parent is None
        assert span.context.trace_id != UPSTREAM_TRACE_ID

    @pytest.mark.asyncio
    async def test_the_neo4j_write_happens_inside_the_consumer_span(
        self,
        spans: SpanCollector,
        artists_queue: FakeQueue,
        artist_record: dict[str, Any],
        writing_neo4j_driver: MagicMock,
    ) -> None:
        """The wrapper's db spans nest under the delivery, so one trace shows the whole path.

        The driver is mocked here, so this stands in for the wrapper's own `session neo4j`
        span by opening one from the same helper at the point the handler enters the session.
        """
        opened: list[Any] = []

        def session(**_kwargs: Any) -> Any:
            with db_span("neo4j", "session") as span:
                opened.append(span)
            return writing_neo4j_driver.session.return_value

        driver = MagicMock()
        driver.session = MagicMock(side_effect=session)

        with patch("graphinator.graphinator.graph", driver):
            await artists_queue.publish(artist_record, headers={"traceparent": TRACEPARENT})

        consumer = spans.only(f"process {CONSUMER_TAG}")
        database = spans.only("session neo4j")
        assert database.parent is not None
        assert database.parent.span_id == consumer.context.span_id
        assert database.context.trace_id == UPSTREAM_TRACE_ID

    @pytest.mark.asyncio
    async def test_a_rejected_delivery_fails_its_span_with_error_type_only(
        self,
        spans: SpanCollector,
        artists_queue: FakeQueue,
    ) -> None:
        message = await artists_queue.publish({"name": "No Identifier"}, headers={"traceparent": TRACEPARENT})

        message.nack.assert_awaited_once()
        span = spans.only(f"process {CONSUMER_TAG}")
        assert span.attributes["error.type"] == "MissingIdError"
        assert span.events == ()


class TestFlushSpan:
    """Every neo4j batch flush runs inside `flush neo4j {entity}`, linked to its deliveries."""

    @staticmethod
    def _pending(span_contexts: list[Any]) -> list[PendingMessage]:
        return [
            PendingMessage(
                data_type="artists",
                data={"id": str(index), "name": f"Artist {index}", "sha256": "hash"},
                ack_callback=AsyncMock(),
                nack_callback=AsyncMock(),
                span_context=context,
            )
            for index, context in enumerate(span_contexts)
        ]

    @staticmethod
    def _processor(process: Any) -> Neo4jBatchProcessor:
        processor = Neo4jBatchProcessor(MagicMock(), BatchConfig(batch_size=1000))
        processor._process_artists_batch = process  # type: ignore[method-assign]
        return processor

    @pytest.mark.asyncio
    async def test_links_every_member_delivery_and_records_the_processed_outcome(self, spans: SpanCollector) -> None:
        contexts = []
        for _ in range(3):
            with gm_telemetry.consume_span(CONSUMER_TAG) as span:
                contexts.append(gm_telemetry.span_context_of(span))

        processor = self._processor(AsyncMock(return_value=set()))
        processor.queues["artists"].extend(self._pending(contexts))
        await processor._flush_queue("artists")

        flush = spans.only("flush neo4j artist")
        assert flush.kind is SpanKind.INTERNAL
        assert flush.attributes["db.system.name"] == "neo4j"
        assert flush.attributes["groovemap.entity"] == "artist"
        assert flush.attributes["outcome"] == "processed"
        assert [link.context.span_id for link in flush.links] == [context.span_id for context in contexts]

    @pytest.mark.asyncio
    async def test_links_are_capped_so_a_large_batch_cannot_flood_the_collector(self, spans: SpanCollector) -> None:
        contexts = []
        for _ in range(MAX_FLUSH_LINKS + 25):
            with gm_telemetry.consume_span(CONSUMER_TAG) as span:
                contexts.append(gm_telemetry.span_context_of(span))

        processor = self._processor(AsyncMock(return_value=set()))
        processor.queues["artists"].extend(self._pending(contexts))
        await processor._flush_queue("artists")

        assert len(spans.only("flush neo4j artist").links) == MAX_FLUSH_LINKS

    @pytest.mark.asyncio
    async def test_the_neo4j_writes_nest_under_the_flush_span(self, spans: SpanCollector) -> None:
        async def process(_messages: list[PendingMessage]) -> set[int]:
            with db_span("neo4j", "execute"):
                pass
            return set()

        processor = self._processor(process)
        processor.queues["artists"].extend(self._pending([None]))
        await processor._flush_queue("artists")

        flush = spans.only("flush neo4j artist")
        write = spans.only("execute neo4j")
        assert write.parent is not None
        assert write.parent.span_id == flush.context.span_id

    @pytest.mark.asyncio
    async def test_a_transient_failure_records_the_failed_outcome_and_error_type(self, spans: SpanCollector) -> None:
        from common.db_resilience import DatabaseUnavailableError

        processor = self._processor(AsyncMock(side_effect=DatabaseUnavailableError("neo4j is down")))
        processor.queues["artists"].extend(self._pending([None]))
        await processor._flush_queue("artists")

        flush = spans.only("flush neo4j artist")
        assert flush.attributes["outcome"] == "failed"
        assert flush.attributes["error.type"] == "DatabaseUnavailableError"
        assert flush.events == ()

    @pytest.mark.asyncio
    async def test_a_poison_batch_records_the_failed_outcome(self, spans: SpanCollector) -> None:
        processor = self._processor(AsyncMock(side_effect=ValueError("poison")))
        processor.config.max_poison_retries = 1
        processor.queues["artists"].extend(self._pending([None]))
        await processor._flush_queue("artists")

        flush = spans.only("flush neo4j artist")
        assert flush.attributes["outcome"] == "failed"
        assert flush.attributes["error.type"] == "ValueError"

    @pytest.mark.asyncio
    async def test_an_empty_queue_opens_no_span(self, spans: SpanCollector) -> None:
        processor = self._processor(AsyncMock(return_value=set()))
        await processor._flush_queue("artists")

        assert spans.names() == []


class TestBatchModeCarriesTheDeliveryContext:
    """In batch mode the delivery span has ended before the flush, so its context rides along."""

    @pytest.mark.asyncio
    async def test_the_handler_hands_the_span_context_to_the_batch_processor(
        self,
        spans: SpanCollector,
        artists_queue: FakeQueue,
        artist_record: dict[str, Any],
    ) -> None:
        captured: dict[str, Any] = {}

        async def add_message(_data_type: str, _data: dict[str, Any], _ack: Any, _nack: Any, span_context: Any = None) -> bool:
            captured["span_context"] = span_context
            return True

        processor = MagicMock()
        processor.add_message = AsyncMock(side_effect=add_message)

        with (
            patch("graphinator.graphinator.BATCH_MODE", True),
            patch("graphinator.graphinator.batch_processor", processor),
        ):
            await artists_queue.publish(artist_record, headers={"traceparent": TRACEPARENT})

        delivery = spans.only(f"process {CONSUMER_TAG}")
        assert captured["span_context"] is not None
        assert captured["span_context"].span_id == delivery.context.span_id
        assert captured["span_context"].trace_id == UPSTREAM_TRACE_ID

    @pytest.mark.asyncio
    async def test_the_flush_span_links_back_to_the_delivery_that_produced_the_record(
        self,
        spans: SpanCollector,
        artists_queue: FakeQueue,
        artist_record: dict[str, Any],
    ) -> None:
        """The whole batch path: delivery span -> pending message -> linked flush span."""
        processor = Neo4jBatchProcessor(MagicMock(), BatchConfig(batch_size=1))
        processor._process_artists_batch = AsyncMock(return_value=set())  # type: ignore[method-assign]

        with (
            patch("graphinator.graphinator.BATCH_MODE", True),
            patch("graphinator.graphinator.batch_processor", processor),
        ):
            await artists_queue.publish(artist_record, headers={"traceparent": TRACEPARENT})

        delivery = spans.only(f"process {CONSUMER_TAG}")
        flush = spans.only("flush neo4j artist")
        assert [link.context.span_id for link in flush.links] == [delivery.context.span_id]

    @pytest.mark.asyncio
    async def test_no_links_are_attached_when_tracing_is_off(
        self,
        artists_queue: FakeQueue,
        artist_record: dict[str, Any],
    ) -> None:
        """Without a live tracer nothing is recorded, and the flush still writes its batch."""
        processor = Neo4jBatchProcessor(MagicMock(), BatchConfig(batch_size=1))
        process = AsyncMock(return_value=set())
        processor._process_artists_batch = process  # type: ignore[method-assign]

        with (
            patch("graphinator.graphinator.BATCH_MODE", True),
            patch("graphinator.graphinator.batch_processor", processor),
        ):
            message = await artists_queue.publish(artist_record)

        process.assert_awaited_once()
        message.ack.assert_awaited_once()


class TestEventLoopMonitor:
    """start_event_loop_monitor() runs from the consumer's own loop, right after setup."""

    @pytest.mark.asyncio
    async def test_main_starts_the_monitor_after_setup_telemetry(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The monitor samples the loop main() is already running on, so it starts inside
        main() rather than at import: before the loop exists there is nothing to sample.
        """
        from graphinator.graphinator import main

        monkeypatch.setenv("STARTUP_DELAY", "0")
        calls: list[str] = []

        with (
            patch("graphinator.graphinator.setup_logging", side_effect=lambda *_a, **_k: calls.append("setup_logging")),
            patch("graphinator.graphinator.setup_telemetry", side_effect=lambda *_a, **_k: calls.append("setup_telemetry")),
            patch(
                "graphinator.graphinator.start_event_loop_monitor",
                side_effect=lambda *_a, **_k: calls.append("start_event_loop_monitor"),
            ) as mock_monitor,
            patch("graphinator.graphinator.HealthServer"),
            patch("graphinator.graphinator.GraphinatorConfig") as mock_config,
        ):
            mock_config.from_env.side_effect = ValueError("Missing required environment variables: NEO4J_HOST")
            await main()

        assert calls[:3] == ["setup_logging", "setup_telemetry", "start_event_loop_monitor"]
        mock_monitor.assert_called_once_with()
