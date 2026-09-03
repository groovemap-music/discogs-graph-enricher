"""Tests for graphinator's domain OpenTelemetry instruments.

Every assertion here is about the shape the collector and dashboards depend on: instrument
name, unit, and the closed attribute set defined by the GrooveMap OpenTelemetry conventions.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from graphinator import telemetry as gm_telemetry


if TYPE_CHECKING:
    from tests.conftest import Collector


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
