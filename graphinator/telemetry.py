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
same metric name and attribute shape locally. :func:`consume_span` does the same for the
CONSUMER span that wrapper would otherwise open, built from the stable ``common`` tracing
surface (``get_tracer`` and ``extract_context``) rather than from the wrapper's own private
helper, which carries no compatibility promise.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from threading import RLock
from typing import TYPE_CHECKING, Any

from common.telemetry import get_meter, provider_generation
from common.tracing import extract_context, get_tracer


if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping


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

# The attribute a flush span carries alongside its metric twin; the same closed set
# `record_batch_flush` writes onto groovemap.pipeline.batch.*.
OUTCOME_ATTRIBUTE = "outcome"

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


def _mark_error(span: Any, error_type: str) -> None:
    """Fail a span with ``error.type`` only — never a message, a stack trace, or a payload.

    The span helpers here switch the SDK's own exception recording off, so this is what
    replaces it: the exception's class name and a status, and nothing that could carry a
    record id or a Cypher statement into the collector.
    """
    if span is None:
        return
    try:
        from opentelemetry.trace import Status, StatusCode  # noqa: PLC0415

        span.set_attribute("error.type", error_type)
        span.set_status(Status(StatusCode.ERROR))
    except Exception:  # pragma: no cover - defensive
        logger.debug("Could not mark a span as failed", exc_info=True)


def mark_consumed_error(span: Any, error_type: str | None) -> None:
    """Fail the CONSUMER span for a delivery this handler rejected or could not process.

    The handler settles its own deliveries and swallows the exception, so the span never sees
    one propagate; ``error_type`` is the same value that goes onto
    ``messaging.client.consumed.messages``. ``None`` leaves the span successful.
    """
    if error_type is not None:
        _mark_error(span, error_type)


def mark_flush_outcome(span: Any, outcome: str, error: BaseException | None = None) -> None:
    """Record ``outcome`` on a flush span, failing it with ``error.type`` when one is given.

    ``outcome`` is the closed set :func:`record_batch_flush` uses — ``processed`` or
    ``failed`` — so a flush span and its duration histogram carry the identical value.
    """
    if span is None:
        return
    try:
        span.set_attribute(OUTCOME_ATTRIBUTE, outcome)
    except Exception:  # pragma: no cover - defensive
        logger.debug("Could not record the flush span outcome", exc_info=True)
    if error is not None:
        _mark_error(span, type(error).__name__)


@contextmanager
def consume_span(destination: str, headers: Mapping[str, Any] | None = None) -> Iterator[Any]:
    """Open the CONSUMER span for one delivery: ``process {destination}``.

    The span is a child of the W3C trace context carried in the AMQP ``headers``, which is what
    puts the extractor's ``publish`` span and this service's processing in one trace. Headers
    that carry no readable context simply start a new trace: a broken ``traceparent`` must
    never fail the message that delivered it.

    ``destination`` is :func:`consumed_destination`'s low-cardinality queue name, the same
    value ``messaging.client.consumed.messages`` carries. Yields ``None`` when the tracer could
    not start a span, so callers must tolerate it; the helpers here already do.
    """
    attributes = {
        "messaging.system": MESSAGING_SYSTEM,
        "messaging.destination.name": destination,
        "messaging.operation.name": "process",
    }
    try:
        from opentelemetry.trace import SpanKind  # noqa: PLC0415

        manager = get_tracer(INSTRUMENTATION_SCOPE).start_as_current_span(
            f"process {destination}",
            context=extract_context(headers) if headers else None,
            kind=SpanKind.CONSUMER,
            attributes=attributes,
            # The conventions allow a status and an `error.type`, not an exception event
            # carrying the message and the traceback, so both SDK defaults are switched off
            # and `mark_consumed_error` writes what is allowed instead.
            record_exception=False,
            set_status_on_exception=False,
        )
    except Exception:  # pragma: no cover - defensive
        logger.debug("Could not start the process span", exc_info=True)
        yield None
        return

    with manager as span:
        try:
            yield span
        except BaseException as exc:
            _mark_error(span, type(exc).__name__)
            raise


def span_context_of(span: Any) -> Any:
    """Return a span's context for later linking, or None when there is nothing to link.

    A batch flush happens long after the delivery span it covers has ended, so the context is
    captured when the message is queued and carried on the pending message. A non-recording
    span is dropped here rather than linked, because a link to an unsampled span tells an
    operator nothing and still costs the collector a payload.
    """
    if span is None:
        return None
    try:
        if not span.is_recording():
            return None
        return span.get_span_context()
    except Exception:  # pragma: no cover - defensive
        logger.debug("Could not read a span context for linking", exc_info=True)
        return None
