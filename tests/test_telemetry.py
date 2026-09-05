"""Tests for graphinator's domain OpenTelemetry instruments and spans.

Every assertion here is about the shape the collector and dashboards depend on: instrument
name, unit, span name and kind, and the closed attribute set defined by the GrooveMap
OpenTelemetry conventions.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from common import telemetry
from common.tracing import flush_span
from opentelemetry.sdk.metrics import MeterProvider as SdkMeterProvider
from opentelemetry.sdk.trace import TracerProvider as SdkTracerProvider
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind, StatusCode

from graphinator import telemetry as gm_telemetry


if TYPE_CHECKING:
    from tests.conftest import Collector, SpanCollector


# A fixed upstream context, as an extractor's publish span would have written it into the
# AMQP headers. Version 00, sampled; the ids are what a joined span must report.
UPSTREAM_TRACE_ID = 0x4BF92F3577B34DA6A3CE929D0E0E4736
UPSTREAM_SPAN_ID = 0x00F067AA0BA902B7
TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"


class FakeMessage:
    """Minimal stand-in for an aio-pika incoming message's routing attributes."""

    def __init__(self, consumer_tag: str | None = None, routing_key: str | None = None, exchange: str | None = None) -> None:
        if consumer_tag is not None:
            self.consumer_tag = consumer_tag
        if routing_key is not None:
            self.routing_key = routing_key
        if exchange is not None:
            self.exchange = exchange


class TestEntityFor:
    """entity_for maps the plural queue names to the singular `entity` attribute value."""

    def test_maps_every_known_data_type(self) -> None:
        assert gm_telemetry.entity_for("artists") == "artist"
        assert gm_telemetry.entity_for("labels") == "label"
        assert gm_telemetry.entity_for("masters") == "master"
        assert gm_telemetry.entity_for("releases") == "release"

    def test_falls_back_to_the_input_for_an_unknown_type(self) -> None:
        assert gm_telemetry.entity_for("widgets") == "widgets"


class TestRecordMessage:
    """groovemap.pipeline.messages / groovemap.pipeline.message.duration."""

    def test_records_counter_and_duration_with_the_conventions_attributes(self, collector: Collector) -> None:
        gm_telemetry.record_message("artist", "processed", 0.25)

        [attrs] = collector.attributes(gm_telemetry.PIPELINE_MESSAGES)
        assert attrs == {"source": "discogs", "entity": "artist", "outcome": "processed"}

        [duration_attrs] = collector.attributes(gm_telemetry.PIPELINE_MESSAGE_DURATION)
        assert duration_attrs == {"source": "discogs", "entity": "artist"}
        [point] = collector.points(gm_telemetry.PIPELINE_MESSAGE_DURATION)
        assert point.sum == pytest.approx(0.25)

    def test_outcome_is_a_closed_set_of_three_values(self, collector: Collector) -> None:
        for outcome in ("processed", "skipped", "failed"):
            gm_telemetry.record_message("release", outcome, 0.1)

        outcomes = {attrs["outcome"] for attrs in collector.attributes(gm_telemetry.PIPELINE_MESSAGES)}
        assert outcomes == {"processed", "skipped", "failed"}


class TestRecordBatchFlush:
    """groovemap.pipeline.batch.size / groovemap.pipeline.batch.flush.duration."""

    def test_records_size_and_duration_with_the_conventions_attributes(self, collector: Collector) -> None:
        gm_telemetry.record_batch_flush("release", "processed", 42, 1.5)

        [size_attrs] = collector.attributes(gm_telemetry.PIPELINE_BATCH_SIZE)
        assert size_attrs == {"store": "neo4j", "entity": "release", "outcome": "processed"}
        [size_point] = collector.points(gm_telemetry.PIPELINE_BATCH_SIZE)
        assert size_point.sum == 42

        [duration_attrs] = collector.attributes(gm_telemetry.PIPELINE_BATCH_FLUSH_DURATION)
        assert duration_attrs == {"store": "neo4j", "entity": "release", "outcome": "processed"}
        [duration_point] = collector.points(gm_telemetry.PIPELINE_BATCH_FLUSH_DURATION)
        assert duration_point.sum == pytest.approx(1.5)

    def test_failed_outcome_is_distinguished_from_processed(self, collector: Collector) -> None:
        gm_telemetry.record_batch_flush("artist", "processed", 10, 0.2)
        gm_telemetry.record_batch_flush("artist", "failed", 10, 0.2)

        outcomes = {attrs["outcome"] for attrs in collector.attributes(gm_telemetry.PIPELINE_BATCH_SIZE)}
        assert outcomes == {"processed", "failed"}


class TestConsumersActive:
    """groovemap.pipeline.consumers.active tracks consumer start/stop as an up-down counter."""

    def test_started_and_stopped_are_symmetric_deltas(self, collector: Collector) -> None:
        gm_telemetry.record_consumer_started()
        gm_telemetry.record_consumer_started()
        gm_telemetry.record_consumer_stopped()

        [point] = collector.points(gm_telemetry.PIPELINE_CONSUMERS_ACTIVE)
        assert point.value == 1
        assert dict(point.attributes) == {"source": "discogs"}

    def test_net_zero_after_matching_start_and_stop(self, collector: Collector) -> None:
        gm_telemetry.record_consumer_started()
        gm_telemetry.record_consumer_stopped()

        [point] = collector.points(gm_telemetry.PIPELINE_CONSUMERS_ACTIVE)
        assert point.value == 0


class TestConsumedDestination:
    """consumed_destination mirrors common.rabbitmq_resilient._destination_name."""

    def test_prefers_consumer_tag(self) -> None:
        message = FakeMessage(consumer_tag="graphinator-artists", routing_key="artist.1", exchange="discogs.artists")
        assert gm_telemetry.consumed_destination(message) == "graphinator-artists"

    def test_falls_back_to_routing_key(self) -> None:
        message = FakeMessage(routing_key="artist.1", exchange="discogs.artists")
        assert gm_telemetry.consumed_destination(message) == "artist.1"

    def test_falls_back_to_exchange(self) -> None:
        message = FakeMessage(exchange="discogs.artists")
        assert gm_telemetry.consumed_destination(message) == "discogs.artists"

    def test_falls_back_to_unknown_with_no_attributes(self) -> None:
        assert gm_telemetry.consumed_destination(FakeMessage()) == "unknown"

    def test_falls_back_to_unknown_for_a_blank_value(self) -> None:
        message = FakeMessage(consumer_tag="", routing_key="", exchange="")
        assert gm_telemetry.consumed_destination(message) == "unknown"


class TestRecordConsumedMessage:
    """messaging.client.consumed.messages, recorded locally for the code path here that
    acks/nacks directly instead of going through process_message_with_retry."""

    def test_records_success_without_error_type(self, collector: Collector) -> None:
        gm_telemetry.record_consumed_message("graphinator-artists", None)

        [attrs] = collector.attributes(gm_telemetry.MESSAGING_CONSUMED_MESSAGES)
        assert attrs == {
            "messaging.system": "rabbitmq",
            "messaging.destination.name": "graphinator-artists",
            "messaging.operation.name": "process",
        }

    def test_records_failure_with_error_type(self, collector: Collector) -> None:
        gm_telemetry.record_consumed_message("graphinator-artists", "ValueError")

        [attrs] = collector.attributes(gm_telemetry.MESSAGING_CONSUMED_MESSAGES)
        assert attrs["error.type"] == "ValueError"


class TestNoOpSafety:
    """Telemetry must never fail the pipeline, with or without a live provider installed."""

    def test_every_recorder_is_safe_before_setup_telemetry(self) -> None:
        gm_telemetry.reset_instruments()
        try:
            gm_telemetry.record_message("artist", "processed", 0.1)
            gm_telemetry.record_batch_flush("artist", "processed", 1, 0.1)
            gm_telemetry.record_consumer_started()
            gm_telemetry.record_consumer_stopped()
            gm_telemetry.record_consumed_message("unknown", None)
        finally:
            gm_telemetry.reset_instruments()


class TestConsumeSpan:
    """`process {queue}`, the CONSUMER span this service opens for every delivery."""

    def test_span_name_kind_and_attributes_follow_the_conventions(self, spans: SpanCollector) -> None:
        with gm_telemetry.consume_span("graphinator-artists"):
            pass

        span = spans.only("process graphinator-artists")
        assert span.kind is SpanKind.CONSUMER
        assert dict(span.attributes) == {
            "messaging.system": "rabbitmq",
            "messaging.destination.name": "graphinator-artists",
            "messaging.operation.name": "process",
        }

    def test_joins_the_trace_carried_in_the_headers(self, spans: SpanCollector) -> None:
        with gm_telemetry.consume_span("graphinator-artists", {"traceparent": TRACEPARENT}):
            pass

        span = spans.only("process graphinator-artists")
        assert span.context.trace_id == UPSTREAM_TRACE_ID
        assert span.parent is not None
        assert span.parent.span_id == UPSTREAM_SPAN_ID

    def test_accepts_a_bytes_header_value(self, spans: SpanCollector) -> None:
        """aio-pika hands back whatever the broker delivered, which can be bytes."""
        with gm_telemetry.consume_span("graphinator-artists", {"traceparent": TRACEPARENT.encode()}):
            pass

        assert spans.only("process graphinator-artists").context.trace_id == UPSTREAM_TRACE_ID

    def test_starts_a_new_trace_without_headers(self, spans: SpanCollector) -> None:
        with gm_telemetry.consume_span("graphinator-artists"):
            pass

        span = spans.only("process graphinator-artists")
        assert span.parent is None
        assert span.context.trace_id != UPSTREAM_TRACE_ID

    def test_a_malformed_traceparent_starts_a_new_trace(self, spans: SpanCollector) -> None:
        """A broken trace context must never fail the message that delivered it."""
        with gm_telemetry.consume_span("graphinator-artists", {"traceparent": "not-a-traceparent"}):
            pass

        span = spans.only("process graphinator-artists")
        assert span.parent is None

    def test_mark_consumed_error_records_error_type_only(self, spans: SpanCollector) -> None:
        with gm_telemetry.consume_span("graphinator-artists") as span:
            gm_telemetry.mark_consumed_error(span, "MissingIdError")

        finished = spans.only("process graphinator-artists")
        assert finished.attributes["error.type"] == "MissingIdError"
        assert finished.status.status_code is StatusCode.ERROR
        assert finished.status.description is None
        assert finished.events == ()

    def test_mark_consumed_error_leaves_a_successful_delivery_alone(self, spans: SpanCollector) -> None:
        with gm_telemetry.consume_span("graphinator-artists") as span:
            gm_telemetry.mark_consumed_error(span, None)

        finished = spans.only("process graphinator-artists")
        assert "error.type" not in dict(finished.attributes)
        assert finished.status.status_code is not StatusCode.ERROR

    def test_a_propagating_exception_fails_the_span_without_a_payload(self, spans: SpanCollector) -> None:
        with pytest.raises(ValueError), gm_telemetry.consume_span("graphinator-artists"):
            raise ValueError("record 12345 is broken")

        finished = spans.only("process graphinator-artists")
        assert finished.attributes["error.type"] == "ValueError"
        assert finished.status.status_code is StatusCode.ERROR
        assert finished.events == ()

    def test_is_a_no_op_before_setup_telemetry(self) -> None:
        gm_telemetry.reset_instruments()
        with gm_telemetry.consume_span("graphinator-artists", {"traceparent": TRACEPARENT}) as span:
            assert span is None or not span.is_recording()


class TestSpanContextOf:
    """span_context_of captures what a later flush span can link back to."""

    def test_returns_the_context_of_a_recording_span(self, spans: SpanCollector) -> None:
        with gm_telemetry.consume_span("graphinator-artists") as span:
            context = gm_telemetry.span_context_of(span)

        assert context is not None
        assert context.span_id == spans.only("process graphinator-artists").context.span_id

    def test_returns_none_for_no_span(self) -> None:
        assert gm_telemetry.span_context_of(None) is None

    def test_drops_a_non_recording_span(self) -> None:
        """A link to an unsampled span tells an operator nothing and still costs a payload."""
        with gm_telemetry.consume_span("graphinator-artists") as span:
            assert gm_telemetry.span_context_of(span) is None


class TestMarkFlushOutcome:
    """The outcome attribute a flush span carries alongside its metric twin."""

    def test_records_the_processed_outcome(self, spans: SpanCollector) -> None:
        with flush_span("neo4j", "artist") as span:
            gm_telemetry.mark_flush_outcome(span, "processed")

        assert spans.only("flush neo4j artist").attributes["outcome"] == "processed"

    def test_records_a_failure_with_error_type_only(self, spans: SpanCollector) -> None:
        with flush_span("neo4j", "artist") as span:
            gm_telemetry.mark_flush_outcome(span, "failed", RuntimeError("neo4j said no"))

        finished = spans.only("flush neo4j artist")
        assert finished.attributes["outcome"] == "failed"
        assert finished.attributes["error.type"] == "RuntimeError"
        assert finished.status.status_code is StatusCode.ERROR
        assert finished.events == ()

    def test_is_safe_without_a_span(self) -> None:
        gm_telemetry.mark_flush_outcome(None, "processed")


class TestTracingDisabled:
    """OTEL_TRACES_EXPORTER=none turns tracing off while metrics keep flowing."""

    def test_metrics_export_with_no_spans_created(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4318")
        monkeypatch.setenv("OTEL_TRACES_EXPORTER", "none")
        monkeypatch.setenv("OTEL_METRIC_EXPORT_INTERVAL", "600000")

        exporter = InMemorySpanExporter()
        try:
            provider = telemetry.setup_telemetry("graphinator")

            # Metrics got the real SDK: the endpoint is configured and the exporter is on.
            assert isinstance(provider, SdkMeterProvider)

            # Tracing did not. Whatever no-op provider is installed, nothing it hands out
            # records, so neither of this service's spans reaches an exporter.
            tracer_provider = telemetry.tracer_provider()
            assert not isinstance(tracer_provider, SdkTracerProvider)
            with gm_telemetry.consume_span("graphinator-artists", {"traceparent": TRACEPARENT}) as span:
                assert span is None or not span.is_recording()
            with flush_span("neo4j", "artist") as flush:
                assert flush is None or not flush.is_recording()
            assert exporter.get_finished_spans() == ()
        finally:
            telemetry.shutdown_telemetry()
