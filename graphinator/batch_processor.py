"""Discogs-to-Neo4j policy adapters for the shared batch runtime.

The transport-neutral queue, concurrency, retry, drain, cancellation, and
settlement lifecycle lives in :mod:`common.batch`. This module deliberately
keeps the owner-specific policy here: Discogs normalization, Neo4j projection
and exception classification, telemetry, and the service-facing facade.
"""

from __future__ import annotations

import contextlib
import os
import time
from contextlib import nullcontext
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from time import perf_counter
from typing import TYPE_CHECKING, Any

import structlog
from common import normalize_record
from common.batch import AsyncBatchEngine, BatchItemResult, BatchPolicy
from common.db_resilience import DatabaseUnavailableError
from common.delivery import DeliveryResult, FailureKind, Settlement
from common.tracing import flush_span
from neo4j.exceptions import ServiceUnavailable, SessionExpired, TransientError

from graphinator import telemetry as gm_telemetry
from graphinator.batch_projection import Neo4jBatchProjector


if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence
    from types import TracebackType


logger = structlog.get_logger(__name__)
DATA_TYPES = ("artists", "labels", "masters", "releases")
_CURRENT_FLUSH: ContextVar[_FlushObservation | None] = ContextVar("discogs_graph_flush", default=None)


@dataclass
class BatchConfig:
    """Owner configuration translated one-for-one to ``BatchPolicy``."""

    batch_size: int = 100
    flush_interval: float = 5.0
    max_pending: int = 1000
    max_concurrent_flushes: int = 2
    min_batch_size: int = 10
    backoff_initial: float = 1.0
    backoff_max: float = 30.0
    backoff_multiplier: float = 2.0
    max_flush_retries: int = 5
    max_poison_retries: int = 5

    def runtime_policy(self) -> BatchPolicy:
        """Return the dependency-light runtime policy for this service."""
        return BatchPolicy(
            batch_size=self.batch_size,
            flush_interval_s=self.flush_interval,
            max_pending=self.max_pending,
            max_concurrent_flushes=self.max_concurrent_flushes,
            min_batch_size=min(self.min_batch_size, self.batch_size),
            backoff_initial_s=self.backoff_initial,
            backoff_max_s=self.backoff_max,
            backoff_multiplier=self.backoff_multiplier,
            max_drain_retries=self.max_flush_retries,
            max_poison_retries=self.max_poison_retries,
        )


@dataclass
class PendingMessage:
    """Normalized owner payload plus the broker settlement callbacks."""

    data_type: str
    data: dict[str, Any]
    ack_callback: Callable[[], Awaitable[None]]
    nack_callback: Callable[[], Awaitable[None]]
    received_at: float = field(default_factory=time.time)
    span_context: Any = None

    async def ack(self) -> None:
        await self.ack_callback()

    async def nack(self, *, requeue: bool) -> None:
        if requeue:
            raise ValueError("batch deliveries may only be rejected without requeue")
        await self.nack_callback()


class Neo4jFailureClassifier:
    """Map concrete Neo4j/wrapper outages without leaking them into the runtime."""

    _transient = (ServiceUnavailable, SessionExpired, TransientError, DatabaseUnavailableError)

    def __call__(self, error: BaseException) -> FailureKind:
        observation = _CURRENT_FLUSH.get()
        if observation is not None:
            observation.mark_failed(error)
        if isinstance(error, self._transient):
            return FailureKind.TRANSIENT
        return FailureKind.DETERMINISTIC


class _FlushObservation:
    """One owner-side metric/span around a shared-engine flush attempt."""

    def __init__(self, observer: Neo4jBatchObserver, key: str, size: int, links: Sequence[object]) -> None:
        self._observer = observer
        self.key = key
        self.size = size
        self._started = perf_counter()
        self._context = flush_span(gm_telemetry.STORE, gm_telemetry.entity_for(key), links=links)
        self.span: Any = None
        self.failed = False
        self.error: BaseException | None = None
        self._token: Token[_FlushObservation | None] | None = None

    def __enter__(self) -> _FlushObservation:
        self.span = self._context.__enter__()
        self._token = _CURRENT_FLUSH.set(self)
        return self

    def mark_failed(self, error: BaseException | None = None) -> None:
        self.failed = True
        if error is not None:
            self.error = error

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        if self._token is not None:
            _CURRENT_FLUSH.reset(self._token)
        if exc is not None:
            self.mark_failed(exc)
        outcome = "failed" if self.failed else "processed"
        with contextlib.suppress(Exception):
            gm_telemetry.mark_flush_outcome(self.span, outcome, self.error)
        with contextlib.suppress(Exception):
            gm_telemetry.record_batch_flush(
                gm_telemetry.entity_for(self.key),
                outcome,
                self.size,
                perf_counter() - self._started,
            )
        if not self.failed:
            self._observer.batch_counts[self.key] += 1
            self._observer.last_flush[self.key] = time.time()
        return bool(self._context.__exit__(exc_type, exc, traceback))


class Neo4jBatchObserver:
    """Preserve GrooveMap metrics and flush spans behind runtime protocols."""

    def __init__(self) -> None:
        self.processed_counts = dict.fromkeys(DATA_TYPES, 0)
        self.batch_counts = dict.fromkeys(DATA_TYPES, 0)
        self.last_flush = dict.fromkeys(DATA_TYPES, time.time())

    def consume(self, _destination: str, _headers: object | None) -> Any:
        """Batch delivery spans are opened by the owner-hive handler."""
        return nullcontext()

    def flush(self, key: str, size: int, links: Sequence[object]) -> _FlushObservation:
        return _FlushObservation(self, key, size, links)

    def retry(
        self,
        *,
        key: str,
        kind: FailureKind,
        attempt: int,
        delay_s: float,
        span: Any,
    ) -> None:
        if isinstance(span, _FlushObservation):
            span.mark_failed()
        logger.warning(
            "Batch write retained for retry",
            data_type=key,
            failure_kind=kind.value,
            attempt=attempt,
            delay_seconds=round(delay_s, 3),
        )

    def settled(
        self,
        *,
        entity: str,
        result: DeliveryResult,
        duration_s: float,
        span: Any,
    ) -> None:
        del duration_s
        if result.settlement is Settlement.ACK:
            self.processed_counts[entity] += 1
        elif result.outcome == "poison" and isinstance(span, _FlushObservation):
            span.mark_failed()


class Neo4jBatchProcessor:
    """Service facade over ``AsyncBatchEngine`` with Neo4j-owned adapters."""

    def __init__(self, driver: Any, config: BatchConfig | None = None) -> None:
        self.driver = driver
        self.config = config or BatchConfig()
        self._apply_batch_size_override()
        self._projector = Neo4jBatchProjector(driver)
        self._classifier = Neo4jFailureClassifier()
        self._observer = Neo4jBatchObserver()
        self._engine: AsyncBatchEngine[str, PendingMessage] = AsyncBatchEngine(
            DATA_TYPES,
            policy=self.config.runtime_policy(),
            sink=self,
            classifier=self._classifier,
            observer=self._observer,
        )

        # Health/progress reporting reads these counters; lifecycle state stays
        # private to the shared engine.
        self.processed_counts = self._observer.processed_counts
        self.batch_counts = self._observer.batch_counts
        self.last_flush = self._observer.last_flush

    def _apply_batch_size_override(self) -> None:
        value = os.environ.get("NEO4J_BATCH_SIZE")
        if value is None:
            return
        try:
            size = int(value)
            if size <= 0:
                raise ValueError
        except ValueError:
            logger.warning("Invalid NEO4J_BATCH_SIZE, using default", value=value, default=self.config.batch_size)
            return
        self.config.batch_size = size
        self.config.min_batch_size = min(self.config.min_batch_size, size)
        logger.info("Using batch size from environment", batch_size=size)

    async def add_message(
        self,
        data_type: str,
        data: dict[str, Any],
        ack_callback: Callable[[], Awaitable[None]],
        nack_callback: Callable[[], Awaitable[None]],
        span_context: Any = None,
    ) -> bool:
        if data_type not in DATA_TYPES:
            logger.error("Unknown data type", data_type=data_type)
            await nack_callback()
            return False
        if not data.get("id"):
            logger.error("Message missing 'id' field", data_type=data_type)
            await nack_callback()
            return False
        try:
            normalized = normalize_record(data_type, data)
        except Exception as error:
            logger.error("Failed to normalize data", data_type=data_type, error=str(error))
            await nack_callback()
            return False

        pending = PendingMessage(data_type, normalized, ack_callback, nack_callback, span_context=span_context)
        await self._engine.submit(data_type, pending, pending, span_context=span_context)
        snapshot = self._engine.snapshot()
        pending_count = snapshot["pending"][data_type]  # type: ignore[index]
        batch_size = snapshot["batch_sizes"][data_type]  # type: ignore[index]
        if pending_count >= batch_size or time.time() - self.last_flush[data_type] >= self.config.flush_interval:
            await self._engine.flush(data_type)
        return True

    async def flush_queue(self, data_type: str) -> bool:
        return await self._engine.flush(data_type)

    async def write(self, key: str, payloads: Sequence[PendingMessage]) -> Sequence[BatchItemResult]:
        """Implement BatchSink by delegating to the local Neo4j projector."""
        messages = list(payloads)
        processors = {
            "artists": self._process_artists_batch,
            "labels": self._process_labels_batch,
            "masters": self._process_masters_batch,
            "releases": self._process_releases_batch,
        }
        try:
            processor = processors[key]
        except KeyError:
            raise ValueError(f"unsupported Discogs graph batch key: {key}") from None
        rejected = await processor(messages)
        return [
            BatchItemResult(Settlement.REJECT, "failed") if index in rejected else BatchItemResult(Settlement.ACK, "processed")
            for index in range(len(messages))
        ]

    async def flush_all(self) -> bool:
        return await self._engine.flush_all()

    async def periodic_flush(self) -> None:
        await self._engine.run_periodic()

    def shutdown(self) -> None:
        self._engine.shutdown()

    def get_stats(self) -> dict[str, Any]:
        snapshot = self._engine.snapshot()
        return {
            "processed": self.processed_counts.copy(),
            "batches": self.batch_counts.copy(),
            "pending": snapshot["pending"],  # type: ignore[index]
            "effective_batch_size": snapshot["batch_sizes"],  # type: ignore[index]
            "configured_batch_size": self.config.batch_size,
            "consecutive_failures": snapshot["poison_attempts"],  # type: ignore[index]
            "transient_failures": snapshot["transient_attempts"],  # type: ignore[index]
        }

    # Projection compatibility for focused owner tests. Queueing and settlement
    # do not pass through these methods; the shared engine invokes sink.write.
    async def _process_artists_batch(self, messages: list[PendingMessage]) -> set[int]:
        self._projector.logger = logger
        return await self._projector._process_artists_batch(messages)

    async def _process_labels_batch(self, messages: list[PendingMessage]) -> set[int]:
        self._projector.logger = logger
        return await self._projector._process_labels_batch(messages)

    async def _process_masters_batch(self, messages: list[PendingMessage]) -> set[int]:
        self._projector.logger = logger
        return await self._projector._process_masters_batch(messages)

    async def _process_releases_batch(self, messages: list[PendingMessage]) -> set[int]:
        self._projector.logger = logger
        return await self._projector._process_releases_batch(messages)

    @staticmethod
    async def _prune_stale_edges(
        tx: Any,
        label: str,
        records: list[dict[str, Any]],
        *,
        rel_type: str,
        target_label: str,
        target_key: str,
        desired: Callable[[dict[str, Any]], list[Any]],
        outgoing: bool = True,
    ) -> None:
        await Neo4jBatchProjector._prune_stale_edges(
            tx,
            label,
            records,
            rel_type=rel_type,
            target_label=target_label,
            target_key=target_key,
            desired=desired,
            outgoing=outgoing,
        )
