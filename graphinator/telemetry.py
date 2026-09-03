"""Domain OpenTelemetry instruments for discogs-graph-enricher (graphinator).

Instruments are built lazily from ``get_meter("groovemap.graphinator")`` on first use and
cached until the installed provider changes (mirrors
``common.runtime_metrics``), so a process that never calls ``common.telemetry.setup_telemetry``
pays only for one no-op instrument per metric. Every recording helper swallows its own errors:
telemetry must never turn a working pipeline into a failure.

Metric names, units, and attribute keys follow the GrooveMap OpenTelemetry conventions
(see the ``gm-discogs-graph-enricher-kr0`` epic design). All attribute values are closed,
low-cardinality sets — never ids, record contents, or free text.

``messaging.client.consumed.messages`` is normally emitted by
``common.rabbitmq_resilient.process_message_with_retry``, but this service acks/nacks messages
itself instead of going through that wrapper, so :func:`record_consumed_message` reproduces the
same metric name and attribute shape locally.
"""

from __future__ import annotations

import logging
from threading import RLock
from typing import Any

from common.telemetry import get_meter, provider_generation


logger = logging.getLogger(__name__)

INSTRUMENTATION_SCOPE = "groovemap.graphinator"

SOURCE = "discogs"
STORE = "neo4j"
MESSAGING_SYSTEM = "rabbitmq"

PIPELINE_MESSAGES = "groovemap.pipeline.messages"
PIPELINE_MESSAGE_DURATION = "groovemap.pipeline.message.duration"
PIPELINE_BATCH_SIZE = "groovemap.pipeline.batch.size"
PIPELINE_BATCH_FLUSH_DURATION = "groovemap.pipeline.batch.flush.duration"
PIPELINE_CONSUMERS_ACTIVE = "groovemap.pipeline.consumers.active"
MESSAGING_CONSUMED_MESSAGES = "messaging.client.consumed.messages"

# Maps the plural queue/data-type names used throughout graphinator to the singular entity
# name used in metric attributes, matching the shared `entity` vocabulary.
ENTITY_SINGULAR: dict[str, str] = {
    "artists": "artist",
    "labels": "label",
    "masters": "master",
    "releases": "release",
}

_lock = RLock()
_instruments: dict[str, Any] = {}
_instrument_generation = -1


def _build_instruments() -> dict[str, Any]:
    """Create one instrument per domain metric from the current provider."""
    meter = get_meter(INSTRUMENTATION_SCOPE)
    return {
        PIPELINE_MESSAGES: meter.create_counter(
            PIPELINE_MESSAGES,
            description="Catalog messages handled by the pipeline.",
        ),
        PIPELINE_MESSAGE_DURATION: meter.create_histogram(
            PIPELINE_MESSAGE_DURATION,
            unit="s",
            description="Duration of handling one catalog message.",
        ),
        PIPELINE_BATCH_SIZE: meter.create_histogram(
            PIPELINE_BATCH_SIZE,
            unit="{items}",
            description="Number of records in a batch flush attempt.",
        ),
        PIPELINE_BATCH_FLUSH_DURATION: meter.create_histogram(
            PIPELINE_BATCH_FLUSH_DURATION,
            unit="s",
            description="Duration of a batch flush attempt.",
        ),
        PIPELINE_CONSUMERS_ACTIVE: meter.create_up_down_counter(
            PIPELINE_CONSUMERS_ACTIVE,
            description="Number of active RabbitMQ consumers.",
        ),
        MESSAGING_CONSUMED_MESSAGES: meter.create_counter(
            MESSAGING_CONSUMED_MESSAGES,
            description="Messages consumed from the broker.",
        ),
    }


def _instrument(name: str) -> Any:
    """Return one cached instrument, rebuilding the cache when the provider changed."""
    global _instrument_generation

    generation = provider_generation()
    with _lock:
        if _instrument_generation != generation or not _instruments:
            _instruments.clear()
            _instruments.update(_build_instruments())
            _instrument_generation = generation
        return _instruments[name]


def reset_instruments() -> None:
    """Drop the instrument cache. Test seam; production relies on the generation check."""
    global _instrument_generation

    with _lock:
        _instruments.clear()
        _instrument_generation = -1


def entity_for(data_type: str) -> str:
    """Return the singular entity attribute value for a plural data-type name."""
    return ENTITY_SINGULAR.get(data_type, data_type)


def record_message(entity: str, outcome: str, duration_s: float) -> None:
    """Record one per-message pipeline handling outcome and its duration.

    ``outcome`` is one of ``processed`` (write applied), ``skipped`` (no change needed), or
    ``failed`` (nacked / raised).
    """
    try:
        _instrument(PIPELINE_MESSAGES).add(1, {"source": SOURCE, "entity": entity, "outcome": outcome})
        _instrument(PIPELINE_MESSAGE_DURATION).record(duration_s, {"source": SOURCE, "entity": entity})
    except Exception:  # pragma: no cover - defensive
        logger.debug("Could not record %s", PIPELINE_MESSAGES, exc_info=True)


def record_batch_flush(entity: str, outcome: str, size: int, duration_s: float) -> None:
    """Record one batch-processor flush attempt: how many records and how long it took.

    ``outcome`` is ``processed`` (the write succeeded, regardless of any per-record nacks) or
    ``failed`` (transient retry or a poison batch nacked to the DLQ).
    """
    try:
        attributes = {"store": STORE, "entity": entity, "outcome": outcome}
        _instrument(PIPELINE_BATCH_SIZE).record(size, attributes)
        _instrument(PIPELINE_BATCH_FLUSH_DURATION).record(duration_s, attributes)
    except Exception:  # pragma: no cover - defensive
        logger.debug("Could not record %s", PIPELINE_BATCH_SIZE, exc_info=True)


def record_consumer_started() -> None:
    """Count one RabbitMQ consumer starting to receive deliveries."""
    try:
        _instrument(PIPELINE_CONSUMERS_ACTIVE).add(1, {"source": SOURCE})
    except Exception:  # pragma: no cover - defensive
        logger.debug("Could not record %s", PIPELINE_CONSUMERS_ACTIVE, exc_info=True)


def record_consumer_stopped() -> None:
    """Count one RabbitMQ consumer no longer receiving deliveries."""
    try:
        _instrument(PIPELINE_CONSUMERS_ACTIVE).add(-1, {"source": SOURCE})
    except Exception:  # pragma: no cover - defensive
        logger.debug("Could not record %s", PIPELINE_CONSUMERS_ACTIVE, exc_info=True)


def consumed_destination(message: Any) -> str:
    """Return the low-cardinality queue name a message was consumed from.

    Mirrors ``common.rabbitmq_resilient._destination_name``: a routing key can carry ids, so
    the consumer tag (bound to this service's own queue name) is preferred, then the routing
    key, then the exchange; "unknown" otherwise. Kept local because the source is a private
    helper with no compatibility guarantee.
    """
    for attribute in ("consumer_tag", "routing_key", "exchange"):
        value = getattr(message, attribute, None)
        if isinstance(value, str) and value:
            return value
    return "unknown"


def record_consumed_message(destination: str, error_type: str | None = None) -> None:
    """Count one message consumed from the broker.

    Reproduces ``messaging.client.consumed.messages`` as
    ``common.runtime_metrics.record_consumed_message`` would, for the code path here that
    acks/nacks directly instead of going through ``process_message_with_retry``.
    """
    try:
        attributes: dict[str, str] = {
            "messaging.system": MESSAGING_SYSTEM,
            "messaging.destination.name": destination,
            "messaging.operation.name": "process",
        }
        if error_type is not None:
            attributes["error.type"] = error_type
        _instrument(MESSAGING_CONSUMED_MESSAGES).add(1, attributes)
    except Exception:  # pragma: no cover - defensive
        logger.debug("Could not record %s", MESSAGING_CONSUMED_MESSAGES, exc_info=True)
