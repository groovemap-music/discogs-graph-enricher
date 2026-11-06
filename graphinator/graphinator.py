import asyncio
import contextlib
import contextvars
import os
import signal
import sys
import time
from asyncio import run
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import structlog
from common import (
    DEFAULT_MAX_REQUEUES,
    AsyncResilientNeo4jDriver,
    AsyncResilientRabbitMQ,
    HealthServer,
    OutageBackoff,
    broker_handouts,
    neo4j_security_kwargs,
    normalize_record,
    requeue_or_dead_letter,
    setup_logging,
)
from common.db_resilience import DatabaseUnavailableError
from common.telemetry import setup_telemetry, shutdown_telemetry, start_event_loop_monitor
from neo4j.exceptions import ServiceUnavailable, SessionExpired, TransientError
from orjson import loads

from graphinator import maintenance_paging
from graphinator import telemetry as gm_telemetry
from graphinator.batch_processor import DEFAULT_MAX_HOLD_S, BatchConfig, Neo4jBatchProcessor
from graphinator.batch_processor import DEFAULT_MAX_REQUEUES as BATCH_DEFAULT_MAX_REQUEUES
from graphinator.config import GraphinatorConfig
from graphinator.entity_projection import (
    process_artist,
    process_label,
    process_master,
    process_release,
)
from graphinator.master_reference import NO_MASTER_ID
from graphinator.person_credits import CREDIT_COUNT_PROPERTIES, build_credit_count_page_cypher
from graphinator.queue_names import (
    AMQP_EXCHANGE_TYPE,
    DATA_TYPES,
)
from graphinator.queue_names import (
    dead_letter_exchange_name as catalog_dead_letter_exchange_name,
)
from graphinator.queue_names import (
    dead_letter_queue_name as catalog_dead_letter_queue_name,
)
from graphinator.queue_names import (
    exchange_name as catalog_exchange_name,
)
from graphinator.queue_names import (
    queue_name as catalog_queue_name,
)


if TYPE_CHECKING:
    from aio_pika.abc import AbstractIncomingMessage


logger = structlog.get_logger(__name__)

SERVICE_NAME = "discogs-graph-enricher"

# service.name default for setup_telemetry(): the docker-compose service key, not the
# distribution/repo name above. OTEL_SERVICE_NAME overrides this in every environment.
TELEMETRY_SERVICE_NAME = "graphinator"

STARTUP_BANNER = r"""
    _ _                                            _                    _    _
 __| (_)___ __ ___  __ _ ______ __ _ _ _ __ _ _ __| |_ ___ ___ _ _  _ _(_)__| |_  ___ _ _
/ _` | (_-</ _/ _ \/ _` (_-<___/ _` | '_/ _` | '_ \ ' \___/ -_) ' \| '_| / _| ' \/ -_) '_|
\__,_|_/__/\__\___/\__, /__/   \__, |_| \__,_| .__/_||_|  \___|_||_|_| |_\__|_||_\___|_|
                   |___/       |___/         |_|
                           GrooveMap / discogs-graph-enricher
""".strip("\n")

# Config will be initialized in main
config: GraphinatorConfig | None = None

# Progress tracking
message_counts = {"artists": 0, "labels": 0, "masters": 0, "releases": 0}
progress_interval = 100  # Log progress every 100 messages
last_message_time = {"artists": 0.0, "labels": 0.0, "masters": 0.0, "releases": 0.0}
completed_files: set[str] = set()  # Track which files have completed processing

# Paces every record requeue, because nothing else does: RabbitMQ 4.3.5 and 4.4.0 do not
# spend x-delivery-limit on nack(requeue=True), so a nacked record comes straight back
# forever. Batch mode holds records in memory instead, and its failures do not nack.
outage_backoff = OutageBackoff(SERVICE_NAME)
# Requeues a record may use before it is dead-lettered, read from x-acquired-count. At the
# 60 s backoff cap this is about 8.3 hours, so an outage does not dead-letter good records.
RECORD_MAX_REQUEUES = DEFAULT_MAX_REQUEUES
# An end-of-type marker waits out a Neo4j failure here before each requeue. RabbitMQ 4.3.5
# and 4.4.0 do not spend x-delivery-limit on nack(requeue=True), so nothing else slows a
# marker down or ever stops it: x-delivery-count stays absent and it never dead-letters.
marker_backoff = OutageBackoff(f"{SERVICE_NAME}-marker")
# Hand-outs a marker may use, read from x-acquired-count, which counts every kind of return.
# At the 60 s backoff cap this is about 8.2 hours of failure before the flush retries each
# cycle adds, provided only nacks return it: see docs/file-completion-tracking.md. A marker
# blocks nothing while it waits, and losing one means that type's signal never lands, so
# the budget is wide.
MARKER_MAX_REQUEUES = 500
# Track which data types have delivered an extraction_complete signal. Stub
# cleanup and aggregate stats are deferred until EVERY type has signalled, so
# cross-type stub creation (release batches MERGE Artist/Label/Master stubs)
# has fully stopped before we DETACH DELETE.
#
# This set is only a CACHE of the durable latch stored in Neo4j (see
# _load_extraction_signals). Acking a signal destroys the only other copy — the
# queued message — so a restart between the first and last signal used to lose
# them all: the final signal then found 1-of-4, deferred forever, and post-import
# maintenance silently never ran. The latch is also keyed by extraction VERSION,
# so a long-lived process cannot carry a completed run's signals into the next
# dump and fire maintenance while other queues are still importing.
extraction_complete_signals: set[str] = set()
extraction_complete_version: str | None = None
# Four consumers share this singleton cache. Serialize the entire read/commit/
# publish operation, including version switches, not just the database write.
extraction_latch_lock = asyncio.Lock()
EXTRACTION_LATCH_UNKNOWN_VERSION = "unknown"
# Post-import maintenance runs DETACHED from the AMQP delivery that triggers it
# (see check_file_completion). Strong references are held here so the loop cannot
# garbage-collect the task mid-flight.
post_import_maintenance_task: asyncio.Task[bool] | None = None
stats_refresh_task: asyncio.Task[bool] | None = None
maintenance_tasks: set[asyncio.Task[bool]] = set()
current_task = None
current_progress = 0.0

# Consumer management
consumer_tags: dict[str, str] = {}  # {"artists": "consumer-tag-123", ...}
consumer_cancel_tasks: dict[str, asyncio.Task[None]] = {}  # {"artists": asyncio.Task, ...}
queues: dict[str, Any] = {}  # {"artists": queue_object, ...}
CONSUMER_CANCEL_DELAY = int(os.environ.get("CONSUMER_CANCEL_DELAY", "300"))  # Default 5 minutes
CONSUMER_CANCEL_TIMEOUT = 5.0  # Bound broker confirmation and teardown within the shutdown grace period.
consumer_recovery_requested = False

# Periodic queue checking settings
QUEUE_CHECK_INTERVAL = int(
    os.environ.get("QUEUE_CHECK_INTERVAL", "3600")
)  # Default 1 hour - how often to check for new messages when connection is closed

# Interval for checking stuck state (consumers died unexpectedly)
STUCK_CHECK_INTERVAL = int(os.environ.get("STUCK_CHECK_INTERVAL", "30"))  # Default 30 seconds - how often to check for stuck state

# Idle mode settings - reduce log noise when no messages arrive after startup
STARTUP_IDLE_TIMEOUT = int(os.environ.get("STARTUP_IDLE_TIMEOUT", "30"))  # Seconds after startup with no messages before entering idle mode
IDLE_LOG_INTERVAL = int(os.environ.get("IDLE_LOG_INTERVAL", "300"))  # 5 min between idle status logs

# Idle mode state
idle_mode = False

# Driver will be initialized in main
graph: AsyncResilientNeo4jDriver | None = None

# Batch processor (optional, enabled via BATCH_MODE env var)
batch_processor: Neo4jBatchProcessor | None = None
BATCH_MODE = os.environ.get("NEO4J_BATCH_MODE", "true").lower() == "true"
BATCH_SIZE = int(os.environ.get("NEO4J_BATCH_SIZE", "100"))
BATCH_FLUSH_INTERVAL = float(os.environ.get("NEO4J_BATCH_FLUSH_INTERVAL", "5.0"))
# Held-delivery bound: deliveries waiting out Neo4j backoff longer than this go back to RabbitMQ with
# requeue=True. BatchConfig rejects values that could reach the broker consumer_timeout (lux: 30 minutes).
BATCH_MAX_HOLD_S = float(os.environ.get("NEO4J_BATCH_MAX_HOLD_S", str(DEFAULT_MAX_HOLD_S)))
# Times a held delivery may go back to RabbitMQ before it is dead-lettered instead.
BATCH_MAX_REQUEUES = int(os.environ.get("NEO4J_BATCH_MAX_REQUEUES", str(BATCH_DEFAULT_MAX_REQUEUES)))

# Connection state tracking
rabbitmq_manager: Any = None  # Will hold AsyncResilientRabbitMQ instance
active_connection: Any = None  # Current active connection
active_channel: Any = None  # Current active channel
connection_check_task: asyncio.Task[None] | None = None  # Background task for periodic queue checks

# Global shutdown flag
shutdown_requested = False


def get_health_data() -> dict[str, Any]:
    """Get current health data for monitoring."""
    # Determine current task based on active consumers and recent activity
    active_task = None
    current_time = time.time()

    # Check for recent message activity (within last 10 seconds)
    for data_type, last_time in last_message_time.items():
        if last_time > 0 and (current_time - last_time) < 10:
            active_task = f"Processing {data_type}"
            break

    # If no recent activity but consumers exist, show as idle
    if active_task is None and len(consumer_tags) > 0:
        active_task = "Idle - waiting for messages"

    # Check for stuck state: no consumers but work remains (files not completed)
    no_active_consumers = len(consumer_tags) == 0
    files_incomplete = len(completed_files) < len(DATA_TYPES)
    has_processed_messages = any(count > 0 for count in message_counts.values())
    is_stuck = no_active_consumers and files_incomplete and has_processed_messages

    if is_stuck:
        active_task = "STUCK - consumers died, awaiting recovery"

    # Determine health status:
    # - "starting" if graph driver not yet initialized (startup in progress)
    # - "unhealthy" if graph was initialized but is now None (connection lost)
    # - "unhealthy" if in stuck state (consumers died unexpectedly)
    # - "healthy" if graph is initialized and ready
    if graph is None:
        # Check if we're still in startup (no consumers registered yet)
        if len(consumer_tags) == 0 and all(c == 0 for c in message_counts.values()):
            status = "starting"
            active_task = "Initializing Neo4j connection"
        else:
            status = "unhealthy"
    elif is_stuck or consumer_recovery_requested:
        status = "unhealthy"
    else:
        status = "healthy"

    return {
        "status": status,
        "service": SERVICE_NAME,
        "current_task": active_task,
        "progress": current_progress,
        "message_counts": message_counts.copy(),
        "last_message_time": last_message_time.copy(),
        "active_consumers": list(consumer_tags.keys()),
        "completed_files": list(completed_files),
        "timestamp": datetime.now(UTC).isoformat(),
    }


class StartupError(Exception):
    """Startup gave up; cli() turns this into a non-zero exit so Docker's restart policy sees a failure."""


def signal_handler(signum: int, _frame: Any) -> None:
    """Handle shutdown signals gracefully."""
    global shutdown_requested
    logger.info("🛑 Received signal, initiating graceful shutdown...", signum=signum)
    shutdown_requested = True


async def _cancel_consumer(data_type: str, consumer_tag: str, queue: Any) -> bool:
    """Remove a consumer only after CancelOk; ambiguous outcomes require recovery."""
    global consumer_recovery_requested

    try:
        if queue is None:
            raise RuntimeError("consumer queue handle missing")
        async with asyncio.timeout(CONSUMER_CANCEL_TIMEOUT):
            await queue.cancel(consumer_tag, nowait=False, timeout=CONSUMER_CANCEL_TIMEOUT)
    except asyncio.CancelledError:
        # A timer can be replaced while its RPC is already in flight. The
        # interrupted waiter cannot establish whether the broker cancelled it.
        consumer_recovery_requested = True
        raise
    except Exception as e:
        consumer_recovery_requested = True
        logger.warning(
            "⚠️ Consumer cancel failed; broker confirmation required before removing tag",
            data_type=data_type,
            timed_out=isinstance(e, TimeoutError),
            error_type=type(e).__name__,
        )
        return False

    if consumer_tags.get(data_type) == consumer_tag:
        consumer_tags.pop(data_type)
        gm_telemetry.record_consumer_stopped()
    return True


async def schedule_consumer_cancellation(data_type: str, queue: Any) -> None:
    """Schedule cancellation of a consumer after a delay."""

    async def cancel_after_delay() -> None:
        try:
            await asyncio.sleep(CONSUMER_CANCEL_DELAY)

            if data_type in consumer_tags:
                consumer_tag = consumer_tags[data_type]
                logger.info(f"🔧 Canceling consumer for {data_type} after {CONSUMER_CANCEL_DELAY}s grace period")

                if not await _cancel_consumer(data_type, consumer_tag, queue):
                    return

                logger.info(
                    "✅ Consumer successfully canceled",
                    data_type=data_type,
                )

                # Check if all consumers are now idle
                if await check_all_consumers_idle():
                    logger.info("🔧 All consumers idle, closing RabbitMQ connection")
                    await close_rabbitmq_connection()
        except Exception as e:
            logger.error(
                "❌ Failed to cancel consumer",
                data_type=data_type,
                error=str(e),
            )
        finally:
            # A replaced timer must not erase the new timer's reference.
            if consumer_cancel_tasks.get(data_type) is asyncio.current_task():
                consumer_cancel_tasks.pop(data_type, None)

    # Cancel any existing scheduled cancellation
    if data_type in consumer_cancel_tasks:
        consumer_cancel_tasks[data_type].cancel()

    # Schedule new cancellation
    consumer_cancel_tasks[data_type] = asyncio.create_task(cancel_after_delay())


async def cancel_all_consumers() -> None:
    """Stop new deliveries at shutdown by cancelling every consumer.

    Shutdown previously had no deregistration phase at all: the flag flipped, the
    consumers stayed subscribed, and the per-message guard nacked whatever the
    broker kept pushing. Cancelling here closes the delivery tap BEFORE the
    seconds-long flush/teardown sequence, so nothing is redelivered into a
    service that is on its way out. Best-effort: teardown continues regardless.
    """
    # Stop delayed cancels before the immediate shutdown cancels compete on the
    # shared channel. Concurrent requests share one overall timeout budget.
    pending = list(consumer_cancel_tasks.values())
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)
    consumer_cancel_tasks.clear()
    results = await asyncio.gather(*(_cancel_consumer(data_type, tag, queues.get(data_type)) for data_type, tag in list(consumer_tags.items())))
    logger.info("Consumer shutdown cancels finished", confirmed=sum(results), failed=len(results) - sum(results))


async def close_rabbitmq_connection() -> None:
    """Close the RabbitMQ connection and channel when all consumers are idle."""
    global active_connection, active_channel, consumer_recovery_requested

    had_transport = bool(active_channel or active_connection)
    try:
        async with asyncio.timeout(CONSUMER_CANCEL_TIMEOUT):
            if active_channel:
                try:
                    await active_channel.close()
                    active_channel = None
                except Exception as e:
                    logger.warning("⚠️ Channel close failed; closing connection", error_type=type(e).__name__)
            if active_connection:
                await active_connection.close()
                active_connection = None
                active_channel = None
        if active_channel:
            consumer_recovery_requested = True
            return
        if not had_transport:
            return
        # Confirmed channel/connection teardown also ends any consumers whose
        # individual cancellation could not be confirmed.
        for _ in consumer_tags:
            gm_telemetry.record_consumer_stopped()
        consumer_tags.clear()
        logger.info("✅ RabbitMQ connection closed", check_interval=f"{QUEUE_CHECK_INTERVAL}s")
    except Exception as e:
        consumer_recovery_requested = True
        logger.warning("⚠️ RabbitMQ teardown failed", error_type=type(e).__name__)


async def check_all_consumers_idle() -> bool:
    """Check if all consumers are cancelled (idle) AND all files completed."""
    return len(consumer_tags) == 0 and len(DATA_TYPES) == len(completed_files)


async def check_consumers_unexpectedly_dead() -> bool:
    """Check if consumers have died unexpectedly (no consumers but files not completed).

    This detects the stuck state where:
    - No consumers are active (consumer_tags is empty)
    - Not all files are completed (some work remains)
    - We've processed at least some messages (not just starting up)

    Returns:
        True if consumers appear to have died unexpectedly
    """
    no_active_consumers = len(consumer_tags) == 0
    files_incomplete = len(completed_files) < len(DATA_TYPES)
    has_processed_messages = any(count > 0 for count in message_counts.values())

    return no_active_consumers and files_incomplete and has_processed_messages


async def periodic_queue_checker() -> None:
    """Periodically check queue health and recover from stuck states.

    This task handles two scenarios:
    1. Normal idle state: All files completed, check for new messages periodically
    2. Stuck state: Consumers died unexpectedly, need immediate recovery

    The stuck state check runs frequently (every STUCK_CHECK_INTERVAL seconds) to
    detect and recover quickly. The normal idle check runs at QUEUE_CHECK_INTERVAL.
    """

    last_full_check = 0.0  # Track when we last did a full queue check

    while not shutdown_requested:
        try:
            await asyncio.sleep(STUCK_CHECK_INTERVAL)

            current_time = time.time()

            # A failed cancel leaves a truthful tag but an uncertain broker
            # subscription; recover even when the ordinary zero-tag guard fails.
            if consumer_recovery_requested:
                await _recover_consumers()
                continue

            # Check for stuck state (consumers died but work remains)
            if await check_consumers_unexpectedly_dead():
                logger.warning(
                    "⚠️ Detected stuck state: consumers died but files not completed. Attempting recovery...",
                    active_consumers=len(consumer_tags),
                    completed_files=list(completed_files),
                    message_counts=message_counts,
                )
                await _recover_consumers()
                continue

            # Normal idle check: only run at QUEUE_CHECK_INTERVAL
            time_since_last_check = current_time - last_full_check
            if time_since_last_check < QUEUE_CHECK_INTERVAL:
                continue

            # Only do full queue check if no active consumers and connection is closed
            if active_connection or len(consumer_tags) > 0:
                continue

            last_full_check = current_time
            logger.info("🔄 Checking all queues for new messages...")
            await _recover_consumers()

        except asyncio.CancelledError:
            logger.info("🛑 Queue checker task cancelled")
            break
        except Exception as e:
            logger.error("❌ Error in periodic queue checker", error=str(e))
            # Continue running despite errors


async def _recover_consumers() -> None:
    """Recover consumers by reconnecting to RabbitMQ and restarting consumption.

    This function handles the actual recovery logic for both:
    - Normal recovery after idle period
    - Emergency recovery after unexpected consumer death
    """
    global active_connection, active_channel, queues, idle_mode, consumer_recovery_requested

    # Do not reconnect until old subscriptions are confirmed gone. Their tags
    # must not prevent registering consumers on the replacement channel.
    if active_connection or active_channel:
        await close_rabbitmq_connection()
        if active_connection or active_channel:
            return
    for data_type, task in list(consumer_cancel_tasks.items()):
        task.cancel()
        consumer_cancel_tasks.pop(data_type, None)
    consumer_recovery_requested = False

    # Temporarily connect to check queue depths
    try:
        temp_connection = await rabbitmq_manager.connect()
        temp_channel = await temp_connection.channel()
    except Exception as e:
        consumer_recovery_requested = True
        logger.error("❌ Failed to connect to RabbitMQ for recovery", error=str(e))
        return

    try:
        # Check each queue for pending messages
        queues_with_messages = []
        for data_type in DATA_TYPES:
            queue_name = catalog_queue_name("graphinator", data_type)

            # Use queue.declare with passive=True to get message count without affecting the queue
            declared_queue = await temp_channel.declare_queue(name=queue_name, passive=True)

            if declared_queue.declaration_result.message_count > 0:
                queues_with_messages.append((data_type, declared_queue.declaration_result.message_count))

        if queues_with_messages:
            total_messages = sum(count for _, count in queues_with_messages)
            logger.info(f"📬 Found messages in queues, restarting consumers: {queues_with_messages} (total: {total_messages})")

            # Re-establish full connection and start consuming
            active_connection = temp_connection
            active_channel = temp_channel

            # Set QoS - scale with batch_size for efficient batch processing
            prefetch_count = max(200, BATCH_SIZE * 2) if BATCH_MODE else 200
            await active_channel.set_qos(prefetch_count=prefetch_count)

            # Declare per-data-type fanout exchanges and consumer-owned queues
            queues = {}
            for data_type in DATA_TYPES:
                exchange_name = catalog_exchange_name(data_type)
                queue_name = catalog_queue_name("graphinator", data_type)
                dlx_name = catalog_dead_letter_exchange_name("graphinator", data_type)
                dlq_name = catalog_dead_letter_queue_name("graphinator", data_type)

                # Declare fanout exchange (must match extractor)
                exchange = await active_channel.declare_exchange(
                    exchange_name,
                    AMQP_EXCHANGE_TYPE,
                    durable=True,
                    auto_delete=False,
                )

                # Declare consumer-owned dead-letter exchange
                dlx_exchange = await active_channel.declare_exchange(
                    dlx_name,
                    AMQP_EXCHANGE_TYPE,
                    durable=True,
                    auto_delete=False,
                )

                # Declare DLQ (classic queue for dead letters)
                dlq = await active_channel.declare_queue(
                    auto_delete=False,
                    durable=True,
                    name=dlq_name,
                    arguments={"x-queue-type": "classic"},
                )
                await dlq.bind(dlx_exchange)

                # Declare main quorum queue with consumer-owned DLX
                queue_args = {
                    "x-queue-type": "quorum",
                    "x-dead-letter-exchange": dlx_name,
                    "x-delivery-limit": 20,
                }
                queue = await active_channel.declare_queue(
                    auto_delete=False,
                    durable=True,
                    name=queue_name,
                    arguments=queue_args,
                )
                await queue.bind(exchange)
                queues[data_type] = queue

            # Start consumers for ALL data types lacking one — not just those
            # with a current backlog. A type whose queue was empty at the
            # passive-declare instant still needs a consumer; otherwise messages
            # that arrive later are never consumed, because once active_connection
            # is set and consumer_tags is non-empty both periodic recovery routes
            # are permanently gated off, silently starving that data type.
            pending_counts = dict(queues_with_messages)
            for data_type in DATA_TYPES:
                if data_type in queues and data_type not in consumer_tags:
                    handler = HANDLERS.get(data_type)
                    if handler:
                        consumer_tag = await queues[data_type].consume(handler, consumer_tag=f"{SERVICE_NAME}-{data_type}")
                        consumer_tags[data_type] = consumer_tag
                        gm_telemetry.record_consumer_started()
                        # Only un-complete a type that actually has a backlog, so
                        # genuinely-finished types stay marked complete.
                        if data_type in pending_counts:
                            completed_files.discard(data_type)
                        last_message_time[data_type] = time.time()
                        logger.info(f"✅ Started consumer for {data_type} (pending: {pending_counts.get(data_type, 0)})")

            logger.info(f"✅ Recovery complete - consumers restarted: {list(consumer_tags.keys())}")
            # Clear idle mode since we have active consumers again
            idle_mode = False
            # Don't close temp_connection since we're using it as active_connection
        else:
            logger.info("⏳ No messages in any queue, connection remains closed")
            # Close the temporary connection
            await temp_channel.close()
            await temp_connection.close()

    except Exception as e:
        consumer_recovery_requested = True
        logger.error("❌ Error during consumer recovery", error=str(e))
        # Retain handles/tags until teardown confirms the partial registrations
        # are gone; the explicit recovery flag re-arms retries even with tags.
        active_connection = temp_connection
        active_channel = temp_channel
        queues = {}
        await close_rabbitmq_connection()


async def _load_extraction_signals(version: str) -> set[str]:
    """Read the durable extraction_complete latch for this version from Neo4j.

    Returns the set of data types that have already signalled. Raises if Neo4j
    cannot answer — the caller must requeue rather than assume "none signalled",
    which would re-run maintenance state from scratch.
    """
    if graph is None:
        return set()

    async with graph.session(database="neo4j") as session:
        result = await session.run(
            """
            MATCH (m:ExtractionCompletion {version: $version})
            UNWIND coalesce(m.signals, []) AS signal
            RETURN collect(DISTINCT signal) AS signals
            """,
            version=version,
        )
        record = await result.single()

    # Older concurrent first writes could create duplicate version nodes before
    # the process lock existed. Recover every committed signal without deleting
    # historical nodes or requiring a runtime uniqueness migration.
    if record is None or record["signals"] is None:
        return set()
    return {str(signal) for signal in record["signals"]}


async def _persist_extraction_signals(version: str, signals: set[str]) -> None:
    """Write the extraction_complete latch for this version to Neo4j.

    Union with the stored property rather than replace it with a cache snapshot.
    Reading the property in its own SET expression lets Neo4j acquire its write
    lock before that read, preserving overlapping updates to an existing node.
    First creation is serialized by extraction_latch_lock in the supported
    single-service process; this does not require runtime schema changes.
    """
    if graph is None:
        raise RuntimeError("Neo4j driver is not initialized")

    async with graph.session(database="neo4j") as session:
        result = await session.run(
            """
            MERGE (m:ExtractionCompletion {version: $version})
            ON CREATE SET m.created_at = datetime()
            SET m.signals = reduce(recorded = coalesce(m.signals, []), signal IN $signals |
                CASE WHEN signal IN recorded THEN recorded ELSE recorded + [signal] END),
                m.updated_at = datetime()
            """,
            version=version,
            signals=sorted(signals),
        )
        # Auto-commit queries are lazy: consume the result and surface commit
        # failures before publishing cache state or destroying the AMQP delivery.
        await result.consume()


async def _record_extraction_signal(version: str, data_type: str) -> set[str]:
    """Commit one signal, then publish and return this version's cache snapshot.

    Keep the candidate private until persistence succeeds. A failed duplicate
    must not discard a previously committed signal, and a failed first delivery
    must not let another consumer see completion that was never committed.
    """
    global extraction_complete_signals, extraction_complete_version

    async with extraction_latch_lock:
        changed_version = extraction_complete_version != version
        persisted = await _load_extraction_signals(version) if changed_version else extraction_complete_signals
        candidate = persisted | {data_type}
        await _persist_extraction_signals(version, candidate)
        if changed_version:
            if extraction_complete_version is not None:
                logger.info(
                    "🔄 New extraction version — resetting the completion latch",
                    previous_version=extraction_complete_version,
                    version=version,
                )
            if persisted:
                logger.info("📥 Restored extraction_complete signals", version=version, received=sorted(persisted))
        extraction_complete_signals = candidate
        extraction_complete_version = version
        return set(candidate)


MAINTENANCE_ERROR_MAX_CHARS = 500


async def _is_extraction_maintained(version: str) -> bool:
    """Return True when this version's ExtractionCompletion node carries maintained_at."""
    if graph is None:
        return False

    async with graph.session(database="neo4j") as session:
        result = await session.run(
            """
            MATCH (m:ExtractionCompletion {version: $version})
            RETURN any(stamped IN collect(m.maintained_at IS NOT NULL) WHERE stamped) AS maintained
            """,
            version=version,
        )
        record = await result.single()
    return bool(record and record["maintained"])


async def _latest_extraction_version() -> str | None:
    """Return the version of the most recently created ExtractionCompletion node."""
    if graph is None:
        return None

    async with graph.session(database="neo4j") as session:
        result = await session.run(
            """
            MATCH (m:ExtractionCompletion)
            RETURN m.version AS version
            ORDER BY coalesce(m.created_at, datetime({epochMillis: 0})) DESC
            LIMIT 1
            """
        )
        record = await result.single()
    return None if record is None or record["version"] is None else str(record["version"])


async def _stamp_maintenance_success(version: str) -> None:
    """Stamp maintained_at on this version's node: the durable "graph is final" signal.

    Idempotent: the first stamp wins (coalesce), so a re-run never moves it, and any
    earlier maintenance_error is cleared. Only this version's node is touched, and only
    maintenance_* properties and stats_version — never `signals`, which stays owned by the
    latch writer. Runs under extraction_latch_lock like every other write to this node.
    """
    if graph is None:
        raise RuntimeError("Neo4j driver is not initialized")

    async with extraction_latch_lock, graph.session(database="neo4j") as session:
        result = await session.run(
            """
            MATCH (m:ExtractionCompletion {version: $version})
            SET m.maintained_at = coalesce(m.maintained_at, datetime()),
                m.maintenance_status = 'succeeded',
                m.stats_version = $stats_version
            REMOVE m.maintenance_error, m.maintenance_failed_at
            RETURN count(m) AS stamped
            """,
            version=version,
            stats_version=STATS_VERSION,
        )
        record = await result.single()
        await result.consume()
    if record is None or not record["stamped"]:
        raise RuntimeError(f"No ExtractionCompletion node for version {version!r} to stamp")


async def _record_maintenance_failure(version: str, error: str) -> None:
    """Record a failed pass as maintenance_error. Never creates or moves maintained_at.

    A node that is already stamped is left untouched: a later failure must not
    contradict a completed run.
    """
    if graph is None:
        raise RuntimeError("Neo4j driver is not initialized")

    async with extraction_latch_lock, graph.session(database="neo4j") as session:
        result = await session.run(
            """
            MATCH (m:ExtractionCompletion {version: $version})
            WHERE m.maintained_at IS NULL
            SET m.maintenance_status = 'failed',
                m.maintenance_error = $error,
                m.maintenance_failed_at = datetime()
            """,
            version=version,
            error=error[:MAINTENANCE_ERROR_MAX_CHARS],
        )
        await result.consume()


async def resume_pending_maintenance() -> bool:
    """Re-run maintenance after a restart interrupted it. Returns True if one was started.

    The fourth extraction_complete is acked before maintenance runs, so a restart
    mid-maintenance leaves no queued message to retrigger it. The durable node is the
    only record: if the NEWEST version has all four signals but no maintained_at,
    maintenance never finished. Older versions are superseded by a newer dump and are
    deliberately not resumed (maintenance sweeps the whole graph, so running it for a
    stale version could race the newer import).
    """
    if graph is None:
        return False
    try:
        version = await _latest_extraction_version()
        if version is None:
            return False
        signals = await _load_extraction_signals(version)
        if not signals.issuperset(DATA_TYPES) or await _is_extraction_maintained(version):
            return False
    except Exception as e:
        logger.error("❌ Could not check for interrupted post-import maintenance", error=str(e))
        return False

    logger.info("♻️ Resuming interrupted post-import maintenance", version=version)
    start_post_import_maintenance(version)
    return True


async def _stats_refresh_version() -> str | None:
    """Return the newest dump's version when its stored stats are behind STATS_VERSION.

    Only a finished dump qualifies. One still importing, or stamped by neither a pass nor a
    refresh, is left to the maintenance pass that follows it, which writes every stat.
    """
    if graph is None:
        return None
    version = await _latest_extraction_version()
    if version is None:
        return None
    signals = await _load_extraction_signals(version)
    if not signals.issuperset(DATA_TYPES) or not await _is_extraction_maintained(version):
        return None
    async with graph.session(database="neo4j") as session:
        result = await session.run(
            "MATCH (m:ExtractionCompletion {version: $version}) RETURN max(m.stats_version) AS stats_version",
            version=version,
        )
        record = await result.single()
    return None if record is not None and record["stats_version"] == STATS_VERSION else version


async def resume_stats_refresh() -> bool:
    """Start a stats refresh if the newest dump needs one. Returns True if one was started.

    A dump stamped by older code never gets the stats that code did not write: its
    maintained_at stops resume_pending_maintenance, and the next dump can be a month away.
    This runs at every start, so deploying a new STATS_VERSION is the whole trigger, a
    finished refresh is never repeated, and an interrupted one continues after its last
    committed page.
    """
    try:
        version = await _stats_refresh_version()
    except Exception as e:
        logger.error("❌ Could not check whether the stored stats are current", error=str(e))
        return False
    if version is None:
        return False
    return start_stats_refresh(version)


def start_stats_refresh(version: str) -> bool:
    """Launch a stats refresh as a detached task unless maintenance or a refresh is running.

    It has its own slot because start_post_import_maintenance drops a trigger while its slot
    is busy, and a real pass's trigger cannot be replayed: that function cancels this task.
    """
    global stats_refresh_task

    if post_import_maintenance_task is not None and not post_import_maintenance_task.done():
        logger.info("⏳ Post-import maintenance is running — it writes every stat, so no refresh is needed")
        return False
    if stats_refresh_task is not None and not stats_refresh_task.done():
        logger.info("⏳ Stats refresh already running — ignoring duplicate trigger")
        return False

    logger.info("♻️ Refreshing stats on the newest dump", version=version, stats_version=STATS_VERSION)
    task = asyncio.create_task(run_stats_refresh(version))
    stats_refresh_task = task
    maintenance_tasks.add(task)
    task.add_done_callback(maintenance_tasks.discard)
    return True


def _stop_if_superseded(version: str) -> maintenance_paging.BeforePage:
    """Build the check that stops a refresh once a new dump has started.

    A new dump writes long before its first ExtractionCompletion node exists, so an arriving
    message counts as well as a newer node. Sweeping alongside those writers would contend
    for the locks on Style nodes, and the dump's own maintenance pass replaces the result.
    """
    messages_at_start = sum(message_counts.values())
    next_version_check = 0.0

    async def before_page() -> None:
        nonlocal next_version_check
        if sum(message_counts.values()) != messages_at_start:
            raise maintenance_paging.MaintenanceStopped("catalog messages are arriving")
        now = time.monotonic()
        if now >= next_version_check:
            next_version_check = now + STATS_REFRESH_VERSION_CHECK_SECONDS
            latest = await _latest_extraction_version()
            if latest != version:
                raise maintenance_paging.MaintenanceStopped(f"a newer dump has arrived: {latest}")

    return before_page


async def _stamp_stats_version(version: str) -> None:
    """Record that ``version`` carries the current stats. Never touches maintained_at."""
    if graph is None:
        raise RuntimeError("Neo4j driver is not initialized")

    async with extraction_latch_lock, graph.session(database="neo4j") as session:
        result = await session.run(
            """
            MATCH (m:ExtractionCompletion {version: $version})
            SET m.stats_version = $stats_version
            RETURN count(m) AS stamped
            """,
            version=version,
            stats_version=STATS_VERSION,
        )
        record = await result.single()
        await result.consume()
    if record is None or not record["stamped"]:
        raise RuntimeError(f"No ExtractionCompletion node for version {version!r} to stamp")


async def run_stats_refresh(version: str) -> bool:
    """Bring the stats of a stamped dump up to STATS_VERSION, retrying on failure.

    Runs only the Label and Artist sweeps, under the same run key as a pass of this stats
    version, so the checkpoints from older code do not skip them and a restart continues
    after the last committed page. It leaves maintained_at, maintenance_status and
    maintenance_error alone: the graph stays final for readers, and a failure is logged
    and retried at the next start rather than recorded against a stamped dump.

    Returns:
        True if the stats are current and stamped; False if the refresh failed or stopped.
    """
    run_key_token = _maintenance_run_key.set(maintenance_run_key(version))
    guard_token = _maintenance_before_page.set(_stop_if_superseded(version))
    try:
        for attempt in range(1, POST_IMPORT_MAINTENANCE_MAX_ATTEMPTS + 1):
            failed_steps: list[str] = []
            for name, step in (("label_stats", compute_label_stats), ("artist_stats", compute_artist_stats)):
                if (step_failure := await _run_maintenance_step(name, step)) is not None:
                    failed_steps.append(step_failure)
            if not failed_steps:
                await _stamp_stats_version(version)
                logger.info("✅ Stats refresh complete", version=version, stats_version=STATS_VERSION)
                return True
            if attempt < POST_IMPORT_MAINTENANCE_MAX_ATTEMPTS:
                delay = POST_IMPORT_MAINTENANCE_RETRY_DELAY_SECONDS * (2 ** (attempt - 1))
                logger.warning("⚠️ Stats refresh failed — retrying", attempt=attempt, failed_steps=failed_steps, retry_in_seconds=delay)
                await asyncio.sleep(delay)
        logger.error("❌ Stats refresh failed after every attempt — it continues at the next start", version=version, failed_steps=failed_steps)
    except maintenance_paging.MaintenanceStopped as e:
        logger.warning("🛑 Stats refresh stopped — the new dump's maintenance pass writes these stats", version=version, reason=str(e))
    except Exception as e:
        logger.error("❌ Stats refresh could not complete — it continues at the next start", version=version, error=str(e))
    finally:
        _maintenance_before_page.reset(guard_token)
        _maintenance_run_key.reset(run_key_token)
    return False


async def requeue_marker(message: AbstractIncomingMessage, data_type: str, reason: str) -> None:
    """Return a marker to the broker after a pause, or dead-letter it once its budget is spent."""
    handouts = broker_handouts(message)
    if handouts >= MARKER_MAX_REQUEUES:
        logger.critical(
            "🚨 End-of-type marker dead-lettered: its requeue budget is spent, so this type's signal has NOT landed",
            data_type=data_type,
            reason=reason,
            deliveries=handouts + 1,
        )
    else:
        await marker_backoff.wait()
    await requeue_or_dead_letter(message, max_requeues=MARKER_MAX_REQUEUES)


async def requeue_record(message: AbstractIncomingMessage) -> None:
    """Return a record to the broker after a pause, or dead-letter it once its budget is spent."""
    if broker_handouts(message) < RECORD_MAX_REQUEUES:
        await outage_backoff.wait()
    await requeue_or_dead_letter(message, max_requeues=RECORD_MAX_REQUEUES)


async def check_file_completion(data: dict[str, Any], data_type: str, message: AbstractIncomingMessage) -> bool:
    """Check if message is a file completion or extraction completion message."""
    if data.get("type") == "file_complete":
        total_processed = data.get("total_processed", 0)
        logger.info(f"✅ File processing complete for {data_type}! Total records processed: {total_processed}")

        # Flush remaining batches for this data type before cancellation
        if batch_processor is not None and not await batch_processor.flush_queue(data_type):
            # The drain gave up with records still pending (typically a database
            # outage). Marking the file complete here would cancel the consumer
            # and let post-import maintenance run over an incomplete graph while
            # the service reports success. Requeue the marker instead — the
            # pending records stay queued and periodic_flush retries them.
            logger.error(
                "❌ Flush incomplete — requeueing file_complete instead of marking the file done",
                data_type=data_type,
            )
            await requeue_marker(message, data_type, "flush_incomplete")
            return True

        # Mark complete only after flush succeeds
        completed_files.add(data_type)

        # Schedule consumer cancellation if enabled
        if CONSUMER_CANCEL_DELAY > 0 and data_type in queues:
            await schedule_consumer_cancellation(data_type, queues[data_type])

        marker_backoff.reset()
        await message.ack()
        return True

    if data.get("type") == "extraction_complete":
        logger.info(
            "🏁 Received extraction_complete signal",
            data_type=data_type,
            version=data.get("version"),
        )

        # The grace timer file_complete armed must not fire while this marker waits out a
        # failure: it would come back to a queue with no consumer. Landing re-arms the timer.
        pending_cancel = consumer_cancel_tasks.pop(data_type, None)
        if pending_cancel is not None:
            pending_cancel.cancel()

        # Flush remaining batches for this data type before cleanup
        if batch_processor is not None and not await batch_processor.flush_queue(data_type):
            # Records are still pending, so this type is NOT complete. Recording
            # the signal would let stub cleanup and stats run over a graph that is
            # still missing rows. Requeue and retry.
            logger.error(
                "❌ Flush incomplete — requeueing extraction_complete instead of recording the signal",
                data_type=data_type,
            )
            await requeue_marker(message, data_type, "flush_incomplete")
            return True

        # Record this type's completion signal (idempotent — safe under
        # nack/requeue redelivery) in Neo4j BEFORE acking. The ack destroys the
        # queued message, which was otherwise the only durable copy of this
        # coordination state.
        version = str(data.get("version") or EXTRACTION_LATCH_UNKNOWN_VERSION)
        try:
            signals = await _record_extraction_signal(version, data_type)
        except Exception as e:
            logger.error(
                "❌ Failed to persist extraction_complete latch — requeueing the signal",
                data_type=data_type,
                version=version,
                error=str(e),
            )
            await requeue_marker(message, data_type, "latch_write_failed")
            return True

        # extraction_complete is this type's terminal signal, so it must also
        # (re-)mark the type complete. completed_files is otherwise written only by
        # file_complete and ERASED by the recovery path for any type whose queue still
        # holds messages — and when the only pending message IS this signal, nothing
        # ever restored the flag: "Stalled consumers detected" then logged at ERROR
        # every 30s forever and check_all_consumers_idle() could never return True, so
        # the connection and four idle consumers were held open until restart. A plain
        # restart between the file_complete ack and this delivery reaches the same
        # terminal state.
        completed_files.add(data_type)
        if CONSUMER_CANCEL_DELAY > 0 and data_type in queues:
            await schedule_consumer_cancellation(data_type, queues[data_type])

        # Defer stub cleanup and stats until EVERY data type has signalled
        # extraction_complete. The four fanout queues drain at very different
        # rates (releases is by far the largest and finishes last), and every
        # release batch MERGEs sha256-less Artist/Label/Master stubs for
        # dangling references. Cleaning per-type as each queue completes would
        # race those still-running release writers: stubs re-created after an
        # early cleanup persist forever, and DETACH DELETE can interleave with
        # freshly written release edges. Running once — after all four signals
        # — guarantees no consumer is still creating stubs.
        if not signals.issuperset(DATA_TYPES):
            logger.info(
                "⏳ Deferring stub cleanup until all data types complete",
                data_type=data_type,
                received=sorted(signals),
                pending=sorted(set(DATA_TYPES) - signals),
            )
            marker_backoff.reset()
            await message.ack()
            return True

        logger.info(
            "🏁 All data types complete — running post-import maintenance",
            received=sorted(signals),
        )

        # A redelivered final signal for a dump whose maintenance already finished
        # must not re-sweep the graph. Check while the delivery is still unacked so a
        # database error requeues it (the latch write is idempotent).
        try:
            already_maintained = await _is_extraction_maintained(version)
        except Exception as e:
            logger.error(
                "❌ Could not read maintenance state — requeueing the signal",
                version=version,
                error=str(e),
            )
            await requeue_marker(message, data_type, "maintenance_state_unreadable")
            return True
        if already_maintained:
            logger.info("✅ Post-import maintenance already stamped for this version — skipping", version=version)
            marker_backoff.reset()
            await message.ack()
            return True

        # Ack the trigger BEFORE maintenance starts, never after. Post-import
        # maintenance is unbounded work — genre/style stats alone run ~10-30s per
        # node over 16 genres + ~757 styles, label stats cover ~2.3M labels, and
        # stub cleanup DETACH DELETEs millions of nodes — routinely hours. Holding
        # the delivery unacked across it trips RabbitMQ's 30-minute consumer ack
        # timeout. The broker then cancels that consumer and requeues the delivery,
        # with the channel left open and no x-delivery-count spent, so the signal is
        # redelivered, maintenance restarts from scratch, and nothing on the broker
        # ever ends the loop.
        marker_backoff.reset()
        await message.ack()

        # Maintenance now runs detached with its own retry, because the AMQP
        # delivery is no longer available as a retry token.
        start_post_import_maintenance(version)
        return True

    return False


def start_post_import_maintenance(version: str | None = None) -> None:
    """Launch post-import maintenance as a detached, single-flight task.

    Single-flight matters: extraction_complete can be redelivered (consumer
    restart, recovery loop) and every step is a heavy Neo4j sweep — two
    concurrent runs would contend for the same transaction pool the retry logic
    is trying to relieve.
    """
    global post_import_maintenance_task

    if post_import_maintenance_task is not None and not post_import_maintenance_task.done():
        logger.info("⏳ Post-import maintenance already running — ignoring duplicate trigger")
        return

    # A full pass covers everything a refresh would do, and its trigger is acked and cannot
    # be replayed, so it always wins. Dropping it for a running refresh would lose the pass.
    if stats_refresh_task is not None and not stats_refresh_task.done():
        logger.info("⏭️ Stopping the stats refresh — full post-import maintenance takes over")
        stats_refresh_task.cancel()

    task = asyncio.create_task(run_post_import_maintenance(version))
    post_import_maintenance_task = task
    maintenance_tasks.add(task)
    task.add_done_callback(maintenance_tasks.discard)


async def run_post_import_maintenance(version: str | None = None) -> bool:
    """Run stub cleanup, aggregate stats, and Person credit counts, retrying on failure.

    Detached from the extraction_complete delivery (which is acked immediately),
    so this owns the retry the nack/requeue used to provide. Every step is
    idempotent, so a retry re-runs the whole sweep safely.

    When ``version`` is given, a successful pass stamps ``maintained_at`` on that
    version's ExtractionCompletion node as its final step (a stamp that cannot be
    written fails the attempt, so it is retried), and a run that fails every attempt
    records ``maintenance_error`` instead. Shutdown records neither: the work is
    simply unfinished and resume_pending_maintenance picks it up on restart.

    Returns:
        True if a full pass succeeded, False if every attempt failed.
    """
    failure = "post-import maintenance failed"
    # Identifies this run to the per-step checkpoints. A versioned run is resumable
    # across retries AND restarts (resume_pending_maintenance passes the same version);
    # an unversioned manual run is only resumable within this call. STATS_VERSION is part
    # of the key so a step the previous code finished is not skipped and then stamped as
    # current.
    run_key = maintenance_run_key(version) if version is not None else f"adhoc-{time.time_ns()}"
    token = _maintenance_run_key.set(run_key)
    try:
        return await _run_post_import_maintenance_attempts(version, failure)
    finally:
        _maintenance_run_key.reset(token)


async def _run_maintenance_step(name: str, step: Any) -> str | None:
    """Run one maintenance step. Returns ``None`` on success, else a description of the failure.

    A raise or a False is logged under the step's name so a failed step is never anonymous.
    """
    try:
        ok = await step()
    except maintenance_paging.MaintenanceStopped:
        raise
    except Exception as e:
        logger.error(f"❌ Maintenance step {name} raised", step=name, error=str(e))
        return f"{name}: {type(e).__name__}: {e}"
    if not ok:
        logger.error(f"❌ Maintenance step {name} did not complete", step=name)
        return name
    return None


async def _run_post_import_maintenance_attempts(version: str | None, failure: str) -> bool:
    for attempt in range(1, POST_IMPORT_MAINTENANCE_MAX_ATTEMPTS + 1):
        if shutdown_requested:
            logger.warning(
                "🛑 Shutdown requested — abandoning post-import maintenance",
                attempt=attempt,
            )
            return False

        failed_steps: list[str] = []
        try:
            # Drain EVERY batch queue first, not just the signalling type's. The
            # extraction_complete branch flushes only its own data type, and a type
            # whose signal was acked earlier can still hold pending messages:
            # _flush_queue re-enqueues a transiently-failed batch at the front of that
            # type's deque, so writes reappear after flush_queue already saw it empty.
            # Cleanup then DETACH DELETEs sha256-less stubs of every label while those
            # batches MERGE fresh stubs — the stubs survive forever with no sha256, or
            # the sweep deletes a stub together with a just-written edge. The gate is
            # "all four signals received"; this makes it "all four queues quiescent"
            # too, which is the invariant the deferral comment actually claims.
            # flush_all is a no-op on empty queues and waits out in-flight batches per type.
            drained = batch_processor is None or await batch_processor.flush_all()
            if not drained:
                # Writers are still creating stubs and edges: NOTHING below may run, and
                # that is a deferral, not a per-step failure.
                logger.error(
                    "❌ Batch queues did not drain — deferring every maintenance step rather than sweeping while writers are still active",
                    skipped_steps=["stub_cleanup", "sentinel_master_cleanup", "genre_style_label_stats", "artist_stats", "person_credit_counts"],
                )
                failed_steps.append("batch_queue_drain")
            elif graph is not None:
                # The steps are independent of each other, so one failing must not
                # silently skip the rest. Each failure is logged by step name and the
                # stamp below is withheld unless EVERY step succeeded. Steps with a
                # checkpoint skip their completed pages on the retry.
                steps: list[tuple[str, Any]] = [
                    # Stub nodes created by cross-type MERGE operations, across EVERY entity
                    # label — release batches create Artist/Label/Master stubs regardless of
                    # which signal arrived last.
                    ("stub_cleanup", cleanup_all_stub_nodes),
                    # Drops the fake (:Master {id: "0"}) a graph loaded before the sentinel
                    # fix may still carry. Idempotent, a no-op on a graph built since.
                    ("sentinel_master_cleanup", cleanup_sentinel_master),
                    # Aggregate stats on Genre/Style/Label nodes, over a graph that is no
                    # longer being written.
                    ("genre_style_label_stats", compute_genre_style_stats),
                    # Release and label counts and the primary genre on every Artist.
                    ("artist_stats", compute_artist_stats),
                    # Recount every Person's credits from the CREDITED_ON edges as they
                    # stand after the import (re-imported releases have already pruned the
                    # credits Discogs removed), so removals are reflected as well as additions.
                    ("person_credit_counts", compute_person_credit_counts),
                ]
                for name, step in steps:
                    if (step_failure := await _run_maintenance_step(name, step)) is not None:
                        failed_steps.append(step_failure)

            maintenance_ok = not failed_steps
            if failed_steps:
                failure = f"maintenance step(s) did not complete: {', '.join(failed_steps)} (see service logs)"

            # Final step: the durable "graph is final for this dump" stamp. Written
            # only after every step above succeeded.
            if maintenance_ok and version is not None and graph is not None:
                await _stamp_maintenance_success(version)
        except Exception as e:
            logger.error("❌ Post-import maintenance raised", attempt=attempt, error=str(e))
            failure = f"{type(e).__name__}: {e}"
            maintenance_ok = False

        if maintenance_ok:
            logger.info("✅ Post-import maintenance complete", attempt=attempt)
            return True

        if attempt == POST_IMPORT_MAINTENANCE_MAX_ATTEMPTS:
            logger.error(
                "❌ Post-import maintenance failed after every attempt — stub nodes, aggregate stats, and Person credit counts are stale until it is re-run",
                attempts=attempt,
            )
            if version is not None and graph is not None:
                try:
                    await _record_maintenance_failure(version, failure)
                except Exception as e:
                    logger.error("❌ Could not record maintenance_error", version=version, error=str(e))
            return False

        delay = POST_IMPORT_MAINTENANCE_RETRY_DELAY_SECONDS * (2 ** (attempt - 1))
        logger.warning(
            "⚠️ Post-import maintenance failed — retrying",
            attempt=attempt,
            retry_in_seconds=delay,
        )
        await asyncio.sleep(delay)

    return False


# Pages per statement. Genre/Style are low-cardinality but heavy per node, so each
# metric of a node is its own statement and a page is a handful of nodes; Label and
# Person are high-cardinality, so a page is a bounded slice of them. Every size halves
# on a timeout / memory pressure and the working size is remembered in the checkpoint.
GENRE_STYLE_STATS_PAGE_SIZE = 10
LABEL_STATS_PAGE_SIZE = 100
ARTIST_STATS_PAGE_SIZE = 1000

# The version of what the stats sweeps write. Bump it when a sweep's output changes:
# ExtractionCompletion.stats_version then differs from this on the newest dump, and the
# next start refreshes it (see resume_stats_refresh) instead of leaving the new values
# missing until the next dump. 1 added the Artist stats and the HAS_STYLE edges.
STATS_VERSION = 1

# How often a running refresh asks Neo4j whether a newer dump has arrived. Arriving
# messages are checked before every page; this read is throttled to keep pages cheap.
STATS_REFRESH_VERSION_CHECK_SECONDS = 30.0

# Identifies the run being maintained to the checkpoints (the dump version). ``None``
# outside run_post_import_maintenance, which disables checkpoint persistence.
_maintenance_run_key: contextvars.ContextVar[str | None] = contextvars.ContextVar("maintenance_run_key", default=None)

# Runs before every page of a sweep. Set only by a stats refresh, which has to stop if a
# new dump starts writing; a full maintenance pass runs after the writers have drained.
_maintenance_before_page: contextvars.ContextVar[maintenance_paging.BeforePage | None] = contextvars.ContextVar(
    "maintenance_before_page", default=None
)


def maintenance_run_key(version: str) -> str:
    """Return the checkpoint run key of ``version`` under the current stats version."""
    return f"{version}@stats-{STATS_VERSION}"


def _node_stat_metrics(node: str) -> list[tuple[tuple[str, ...], str]]:
    """Return ``(properties, Cypher body)`` per statement of a Genre/Style node ``n``.

    Each body is a subquery returning ``v0, v1, ...``, one column per property, and
    each becomes its own statement and transaction. The node is Genre or Style, and the
    "other" taxonomy is counted.

    ``release_count`` is ``count(r)``, not ``count(DISTINCT r)``: the ``IS`` edge is
    ``MERGE``d, so a release has at most one edge to a node (checked live on Rock, Pop,
    Electronic, Folk/World/Country and Jazz: edges equal distinct releases), and the
    distinct set only cost memory (286 MB for Rock). ``artist_count``, ``label_count``
    and the other taxonomy's count share ONE expansion of each release: the cost of a
    release is walking its relationship chain, which three separate statements did three
    times over.
    """
    other_label, other_prop = ("Style", "style_count") if node == "Genre" else ("Genre", "genre_count")
    return [
        (("release_count",), "MATCH (n)<-[:IS]-(r:Release) RETURN count(r) AS v0"),
        (
            ("artist_count", "label_count", other_prop),
            f"""MATCH (n)<-[:IS]-(r:Release)
                OPTIONAL MATCH (r)-[x:BY|ON|IS]->(t)
                RETURN count(DISTINCT CASE WHEN type(x) = 'BY' AND t:Artist THEN t END) AS v0,
                    count(DISTINCT CASE WHEN type(x) = 'ON' AND t:Label THEN t END) AS v1,
                    count(DISTINCT CASE WHEN type(x) = 'IS' AND t:{other_label} THEN t END) AS v2""",
        ),
        (("first_year",), "MATCH (n)<-[:IS]-(r:Release) WHERE r.year > 0 RETURN min(r.year) AS v0"),
    ]


def build_node_stat_statements(node: str) -> list[tuple[str, dict[str, Any]]]:
    """Return one ``(cypher, params)`` statement per group of aggregates of a Genre/Style node."""
    return [
        (
            f"""
            MATCH (n:{node} {{name: $key}})
            CALL {{
                WITH n
                {body}
            }}
            SET {", ".join(f"n.{prop} = v{i}" for i, prop in enumerate(props))}
            """,
            {},
        )
        for props, body in _node_stat_metrics(node)
    ]


def build_label_stats_page_cypher() -> str:
    """Keyed page of Label aggregates: the ``$size`` labels with ``id > $cursor``.

    Returns ``last`` (next cursor) and ``processed``. Bounded by ``$size``, not by the
    ~millions of labels in the graph; most labels have few releases so a page is quick.
    Each property is written only when it differs, so a rerun over unchanged data writes nothing.

    Writes ``release_count``, ``artist_count`` and ``genre_count`` on the Label, and one
    ``(:Label)-[:HAS_STYLE {release_count}]->(:Style)`` per style of its releases, with no
    cap on styles. The style counts are an edge and not list properties because
    ``/api/label/{id}/similar`` has to go from a style to the labels carrying it: on lux
    Pop Rock reaches 74,053 of the 2.43M labels, which an edge reads directly and a list
    property could only find by scanning every Label. An edge whose style the label no
    longer has is deleted, so the edges always equal a live count.

    The artists, genres and styles share ONE expansion of each release, which on lux
    takes the largest label (1818, 762,594 releases) from 21.4M db hits to 14.4M, in 2.7 s
    and 64 MB. ``count(r)`` per target needs no ``DISTINCT``: ``BY`` and ``IS`` are
    ``MERGE``d, so a release has at most one edge to a target.
    """
    return """
    MATCH (l:Label)
    WHERE l.id > $cursor
    WITH l ORDER BY l.id LIMIT $size
    CALL (l) {
        CALL (l) {
            OPTIONAL MATCH (l)<-[:ON]-(r:Release)
            RETURN count(DISTINCT r) AS rc
        }
        CALL (l) {
            OPTIONAL MATCH (l)<-[:ON]-(r:Release)-[:BY|IS]->(t)
            WHERE t:Artist OR t:Genre OR t:Style
            WITH t, count(r) AS n
            RETURN count(CASE WHEN t:Artist THEN 1 END) AS ac,
                count(CASE WHEN t:Genre THEN 1 END) AS gc,
                collect(CASE WHEN t:Style THEN {style: t, releases: n} END) AS styles
        }
        FOREACH (_ IN CASE WHEN l.release_count IS NULL OR l.release_count <> rc THEN [1] ELSE [] END | SET l.release_count = rc)
        FOREACH (_ IN CASE WHEN l.artist_count IS NULL OR l.artist_count <> ac THEN [1] ELSE [] END | SET l.artist_count = ac)
        FOREACH (_ IN CASE WHEN l.genre_count IS NULL OR l.genre_count <> gc THEN [1] ELSE [] END | SET l.genre_count = gc)
        WITH l, styles
        CALL (l, styles) {
            MATCH (l)-[stale:HAS_STYLE]->(old:Style)
            WHERE NOT old IN [row IN styles | row.style]
            DELETE stale
        }
        CALL (l, styles) {
            UNWIND styles AS row
            WITH l, row.style AS style, row.releases AS releases
            MERGE (l)-[h:HAS_STYLE]->(style)
            FOREACH (_ IN CASE WHEN h.release_count IS NULL OR h.release_count <> releases THEN [1] ELSE [] END | SET h.release_count = releases)
        }
    }
    RETURN max(l.id) AS last, count(l) AS processed
    """


def build_artist_stats_page_cypher() -> str:
    """Keyed page of Artist aggregates: the ``$size`` artists with ``id > $cursor``.

    Writes three properties, each only when it differs, so a rerun over unchanged data
    writes nothing:

    - ``release_count``: the distinct releases ``BY`` the artist. Only Releases count, and
      Masters are ``BY`` the artist too.
    - ``label_count``: the distinct labels those releases are ``ON``.
    - ``primary_genre``: the Genre on the most of those releases, every release counted.
      A tie goes to the alphabetically first genre name, so the answer never depends on
      storage order. An artist with no genre has no property, and a genre lost since the
      last pass removes it.

    Every artist is computed the same way, placeholders included: Various (id 194,
    1,361,622 releases) takes 4.0 s, 29M db hits and 49 MB on lux, inside the 600 s
    transaction budget. Labels and genres share ONE expansion of each release. The
    ``ORDER BY`` sits before the ``collect``, which keeps that order.
    """
    return """
    MATCH (a:Artist)
    WHERE a.id > $cursor
    WITH a ORDER BY a.id LIMIT $size
    CALL (a) {
        CALL (a) {
            OPTIONAL MATCH (a)<-[:BY]-(r:Release)
            RETURN count(DISTINCT r) AS rc
        }
        CALL (a) {
            OPTIONAL MATCH (a)<-[:BY]-(r:Release)-[:ON|IS]->(t)
            WHERE t:Label OR t:Genre
            WITH t, count(r) AS n
            ORDER BY n DESC, CASE WHEN t:Genre THEN t.name END
            RETURN count(CASE WHEN t:Label THEN 1 END) AS lc,
                head(collect(CASE WHEN t:Genre THEN t.name END)) AS pg
        }
        FOREACH (_ IN CASE WHEN a.release_count IS NULL OR a.release_count <> rc THEN [1] ELSE [] END | SET a.release_count = rc)
        FOREACH (_ IN CASE WHEN a.label_count IS NULL OR a.label_count <> lc THEN [1] ELSE [] END | SET a.label_count = lc)
        FOREACH (_ IN CASE WHEN coalesce(a.primary_genre, '') <> coalesce(pg, '') THEN [1] ELSE [] END | SET a.primary_genre = pg)
    }
    RETURN max(a.id) AS last, count(a) AS processed
    """


async def compute_genre_style_stats() -> bool:
    """Pre-compute aggregate counts and first_year on Genre, Style, and Label nodes.

    Sets release_count, artist_count, label_count, style_count/genre_count,
    and first_year properties on each Genre and Style node.  Also sets
    release_count, artist_count, genre_count on Label nodes, and the label's
    per-style release counts on its HAS_STYLE edges.

    - Genre/Style counts are read by explore/genre and explore/style endpoints,
      replacing 4 expensive traversal queries (~200M DB hits for Rock)
      with a single property read (~3 DB hits).
    - first_year is read by genre-emergence, replacing a full IS edge scan
      (~184M DB hits) with an index seek (~100 DB hits).
    - Label counts are read by explore/label, replacing a 1.2M DB-hit
      traversal (for Reprise Records with 55K releases) with ~3 DB hits.
    - HAS_STYLE edges are read by /api/label/{id}/similar, which otherwise walks
      every release of a style (19.6M DB hits for Pop Rock).

    Resumable: each of Genre, Style and Label is a keyed-page sweep checkpointed after
    every page (see :mod:`graphinator.maintenance_paging`), so no single transaction is
    bounded by the size of the graph, a transaction timeout halves the page rather than
    repeating the same work, and a retry continues after the last committed page.

    Should be called after all releases have been imported.

    Returns:
        True if all stats were computed (or there is no driver / no work),
        False if the computation failed. Every failing sweep is logged with its
        step name; a failure does not skip the sweeps after it.
    """
    return await _run_stat_sweeps(("Genre", "Style", "Label"))


async def compute_label_stats() -> bool:
    """Run only the Label sweep of :func:`compute_genre_style_stats`, for a stats refresh."""
    return await _run_stat_sweeps(("Label",))


async def _run_stat_sweeps(node_labels: tuple[str, ...]) -> bool:
    """Run the named Genre, Style and Label sweeps, each checkpointed, in the order given."""
    if graph is None:
        return True

    run_key = _maintenance_run_key.get()
    before_page = _maintenance_before_page.get()
    available: dict[str, tuple[Any, int]] = {
        "Genre": (
            maintenance_paging.per_key_page_processor(
                graph,
                list_keys_cypher="MATCH (n:Genre) WHERE n.name > $cursor RETURN n.name AS key ORDER BY key LIMIT $size",
                statements_for_key=lambda _key: build_node_stat_statements("Genre"),
            ),
            GENRE_STYLE_STATS_PAGE_SIZE,
        ),
        "Style": (
            maintenance_paging.per_key_page_processor(
                graph,
                list_keys_cypher="MATCH (n:Style) WHERE n.name > $cursor RETURN n.name AS key ORDER BY key LIMIT $size",
                statements_for_key=lambda _key: build_node_stat_statements("Style"),
            ),
            GENRE_STYLE_STATS_PAGE_SIZE,
        ),
        "Label": (
            maintenance_paging.keyed_page_processor(graph, build_label_stats_page_cypher()),
            LABEL_STATS_PAGE_SIZE,
        ),
    }

    ok = True
    for index, node_label in enumerate(node_labels):
        process_page, page_size = available[node_label]
        if index:
            await asyncio.sleep(MAINTENANCE_SETTLE_SECONDS)
        step = f"{node_label.lower()}_stats"
        logger.info(f"📊 Computing aggregate stats on {node_label} nodes...", step=step)
        try:
            processed = await maintenance_paging.run_checkpointed_step(
                graph,
                step=step,
                run_key=run_key,
                process_page=process_page,
                page_size=page_size,
                before_page=before_page,
            )
            logger.info(f"✅ {node_label} stats computed", step=step, processed=processed)
        except maintenance_paging.MaintenanceStopped:
            raise
        except Exception as e:
            # Keep going: the other sweeps are independent, and the checkpoint keeps
            # this one's progress for the retry.
            logger.error(f"❌ Failed to compute {node_label} stats", step=step, error=str(e))
            ok = False

    return ok


async def compute_artist_stats() -> bool:
    """Pre-compute ``release_count``, ``label_count`` and ``primary_genre`` on every Artist.

    See :func:`build_artist_stats_page_cypher` for what each property holds. Explore reads
    the two counts, and ``/api/network/cluster`` reads ``primary_genre``, instead of
    walking each artist's whole discography per request.

    Artists are processed in keyed pages of ``ARTIST_STATS_PAGE_SIZE``, checkpointed after
    each page and halved on a timeout or transaction-memory pressure, so the sweep resumes
    after its last committed page instead of restarting.

    Returns:
        True if the stats were written (or there is no driver), False if the sweep failed
        (logged with the step name), so the detached maintenance task retries it.
    """
    if graph is None:
        return True

    step = "artist_stats"
    try:
        await asyncio.sleep(MAINTENANCE_SETTLE_SECONDS)
        logger.info("📊 Computing aggregate stats on Artist nodes...", step=step)
        processed = await maintenance_paging.run_checkpointed_step(
            graph,
            step=step,
            run_key=_maintenance_run_key.get(),
            process_page=maintenance_paging.keyed_page_processor(graph, build_artist_stats_page_cypher()),
            page_size=ARTIST_STATS_PAGE_SIZE,
            before_page=_maintenance_before_page.get(),
        )
        logger.info("✅ Artist stats computed", step=step, processed=processed)
    except maintenance_paging.MaintenanceStopped:
        raise
    except Exception as e:
        logger.error("❌ Failed to compute Artist stats", step=step, error=str(e))
        return False

    return True


async def compute_person_credit_counts() -> bool:
    """Recompute ``credit_count`` and ``credit_count_<category>`` on every Person.

    The property contract (shared with the database-schema indexes and the catalog-api
    role leaderboard) is documented in :mod:`graphinator.person_credits`: per category in
    ``common.credit_roles.ALL_CATEGORIES``, the number of DISTINCT releases the person is
    CREDITED_ON with that category, plus the distinct-release total. Every property is
    rewritten on every Person, and a category the person no longer has reads ``0``.

    A full recount rather than an incremental one, so the result does not depend on the
    previous value: a re-run is idempotent, and a credit removed since the last pass is
    simply no longer counted. Persons are processed in keyed pages of
    ``PERSON_CREDIT_COUNT_BATCH_SIZE`` (by name), checkpointed after each page and
    halved on a timeout or transaction-memory pressure, so the sweep resumes after its
    last committed page instead of restarting.

    Returns:
        True if the counts were written (or there is no driver), False if the recompute
        failed (logged with the step name) — so the detached maintenance task retries it.
    """
    if graph is None:
        return True

    step = "person_credit_counts"
    try:
        # Runs straight after the Genre/Style/Label sweep: let the shared transaction
        # pool drain first rather than claiming it back to back.
        await asyncio.sleep(MAINTENANCE_SETTLE_SECONDS)
        logger.info("📊 Computing credit counts on Person nodes...", step=step)
        processed = await maintenance_paging.run_checkpointed_step(
            graph,
            step=step,
            run_key=_maintenance_run_key.get(),
            process_page=maintenance_paging.keyed_page_processor(graph, build_credit_count_page_cypher()),
            page_size=PERSON_CREDIT_COUNT_BATCH_SIZE,
        )
        logger.info("✅ Person credit counts computed", step=step, properties=list(CREDIT_COUNT_PROPERTIES), processed=processed)
    except Exception as e:
        logger.error("❌ Failed to compute Person credit counts", step=step, error=str(e))
        return False

    return True


# ── Post-import maintenance under transaction-memory pressure ──────────────
#
# Post-import maintenance shares Neo4j's single transaction-memory pool
# (dbms.memory.transaction.total.max, 16 GiB in production) with ingestion and
# the insights background scans. Jul 5-9 that pool was exhausted repeatedly
# (Neo.TransientError.General.MemoryPoolOutOfMemoryError), which killed
# stub-Master cleanup twice — silently skipping the maintenance — and broke
# seven dashboard health checks.
#
# Two defences, applied to EVERY batched maintenance query in this module:
#
#  1. Bound the working set. Smaller `IN TRANSACTIONS OF n ROWS` chunks keep a
#     single inner transaction cheap even when the chunk contains high-degree
#     nodes whose DETACH DELETE touches many relationships.
#  2. Retry with backoff AND a halved chunk size. Pool exhaustion is transient
#     and contention-driven, so waiting and then asking for less is the correct
#     response. Every query here is idempotent and `IN TRANSACTIONS` commits per
#     chunk, so a retry resumes from where the failed attempt stopped instead of
#     redoing its work.
#
# Reduced from 10000: a chunk of 10000 DETACH DELETEs over high-degree stubs was
# still large enough to contend for the shared pool.
STUB_CLEANUP_BATCH_SIZE = 1000

# Persons per inner transaction for the credit-count recompute. Each person's working
# set is one row per release it is credited on, so a chunk stays cheap even when it
# holds a prolific mastering engineer; the halving retry covers the rest.
PERSON_CREDIT_COUNT_BATCH_SIZE = 1000

# Floor for the halving backoff — below this the round-trip overhead dominates.
MAINTENANCE_MIN_BATCH_SIZE = 50

MAINTENANCE_MAX_ATTEMPTS = 4
MAINTENANCE_RETRY_BASE_DELAY_SECONDS = 30.0

# Whole-sweep retries for the detached post-import maintenance task. The AMQP
# delivery is acked before maintenance starts, so this — not
# nack/requeue — is what retries a failed sweep. Kept small: each pass is hours of
# Neo4j work, and every step is idempotent so a later manual re-run is always safe.
POST_IMPORT_MAINTENANCE_MAX_ATTEMPTS = 3
POST_IMPORT_MAINTENANCE_RETRY_DELAY_SECONDS = 60.0

# Pause between consecutive heavy maintenance queries so the transaction pool
# can drain before the next one claims it, rather than running them back to back.
MAINTENANCE_SETTLE_SECONDS = 5.0


async def _run_maintenance_query(
    build_cypher: Any,
    description: str,
    batch_size: int | None = None,
    **log_fields: Any,
) -> Any | None:
    """Run a batched maintenance query, retrying on transaction-memory exhaustion.

    Args:
        build_cypher: Callable taking the current batch size and returning the
            Cypher to run. Called again with a smaller size on each retry.
        description: Human-readable name used in log messages.
        batch_size: Starting `IN TRANSACTIONS OF n ROWS` size, or ``None`` for
            queries that are not batch-size parameterised.
        **log_fields: Extra structured log fields.

    Returns:
        The query summary on success, or ``None`` if every attempt failed.
    """
    if graph is None:
        return None

    current_batch = batch_size
    for attempt in range(1, MAINTENANCE_MAX_ATTEMPTS + 1):
        try:
            async with graph.session(database="neo4j") as session:
                result = await session.run(build_cypher(current_batch))
                return await result.consume()
        except TransientError as e:
            # Pool exhaustion / contention — retryable. Anything else is a real
            # error and propagates to the caller's handler.
            if attempt == MAINTENANCE_MAX_ATTEMPTS:
                logger.error(
                    f"❌ {description} exhausted retries under transaction-memory pressure",
                    attempts=attempt,
                    batch_size=current_batch,
                    error=str(e),
                    **log_fields,
                )
                return None
            delay = MAINTENANCE_RETRY_BASE_DELAY_SECONDS * (2 ** (attempt - 1))
            if current_batch is not None:
                current_batch = max(MAINTENANCE_MIN_BATCH_SIZE, current_batch // 2)
            logger.warning(
                f"⚠️ {description} hit transaction-memory pressure — retrying with a smaller batch",
                attempt=attempt,
                next_batch_size=current_batch,
                retry_in_seconds=delay,
                error=str(e),
                **log_fields,
            )
            await asyncio.sleep(delay)
    return None


async def cleanup_all_stub_nodes() -> bool:
    """Clean up stub nodes for every entity label once all data is imported.

    Called only after every data type has delivered extraction_complete, so no
    consumer is still MERGEing cross-type stubs. Runs cleanup for all labels
    (Artist, Label, Master, Release) rather than a single type, because release
    batches create stubs of every other type.

    Returns:
        True if all label cleanups succeeded, False if any failed — so the
        caller can nack and retry (each cleanup is idempotent).
    """
    ok = True
    for index, data_type in enumerate(DATA_TYPES):
        if index:
            # Stagger: let the shared transaction pool drain between labels
            # instead of firing four DETACH DELETE sweeps back to back.
            await asyncio.sleep(MAINTENANCE_SETTLE_SECONDS)
        if not await cleanup_stub_nodes(data_type):
            ok = False
    return ok


async def cleanup_stub_nodes(data_type: str) -> bool:
    """Delete ISOLATED stub nodes that have no sha256 property.

    During extraction, MERGE operations in relationship queries create
    skeleton nodes for cross-referenced entities (e.g., a release referencing
    an artist that hasn't been processed yet). Primary records always set
    sha256, so nodes without it are stubs that were never filled.

    A stub that still has relationships is NOT collected: its edges belong to
    fully-imported records and deleting them is unrecoverable under the
    content-hash write-skip.

    Returns:
        True if cleanup succeeded (or there is nothing to do), False if the
        DETACH DELETE failed — so the caller can nack and retry (idempotent).
    """
    if graph is None:
        return True

    # Map data types to their Neo4j labels
    label_map = {
        "artists": "Artist",
        "labels": "Label",
        "masters": "Master",
        "releases": "Release",
    }

    label = label_map.get(data_type)
    if not label:
        return True

    # Batch the deletion with CALL {} IN TRANSACTIONS so a large stub-node set
    # (cross-type MERGEs can leave hundreds of thousands to millions of
    # property-less stubs after a full import) does not run in one implicit
    # transaction — a single unbatched DETACH DELETE blows the Neo4j
    # transaction memory limit (dbms.memory.transaction.total.max) or the
    # default 120s timeout, and the swallowed error would leave the stubs
    # forever. Mirrors the compute_genre_style_stats batching in this file: the
    # driving MATCH sits OUTSIDE the transactional subquery and the node is
    # re-imported via WITH, so each inner transaction deletes at most one chunk.
    #
    # The chunk size is a parameter of the retry loop, not a constant in the
    # query: under transaction-memory pressure _run_maintenance_query re-runs
    # this with progressively smaller chunks. IN TRANSACTIONS commits per chunk,
    # so a retry only has to delete what the failed attempt did not.
    # Only ISOLATED stubs are deleted. A stub that still carries relationships is a
    # genuine dangling reference: a fully-imported release/artist MERGEd it plus a
    # BY/ON/DERIVED_FROM/MEMBER_OF edge because the referenced entity was absent from
    # this dump. DETACH DELETE destroyed those edges along with the stub — and the
    # damage was permanent, because sha256 is purely content-derived: when the entity
    # appears in a later dump the referencing record is byte-identical, the hash-skip
    # in process_release/process_artist (and both batch processors) returns before the
    # relationship blocks, and the edge is never rebuilt. Keeping the stub preserves
    # both the edge and the id, and a later dump upgrades it in place — the fill-in
    # path MERGEs on id, so the stub simply gains its properties and sha256.
    # Truly orphaned stubs still get collected here. HAS_STYLE does not hold a stub: it is
    # derived from the ON edges by the Label stats sweep, which runs after this one, so a
    # label left with no releases must not be kept alive by last pass's derived edges.
    def _build_cleanup_cypher(batch_size: int | None) -> str:
        return f"""
    MATCH (n:{label})
    WHERE n.sha256 IS NULL AND NOT EXISTS {{ (n)-[e]-() WHERE NOT e:HAS_STYLE }}
    CALL {{
        WITH n
        DETACH DELETE n
    }} IN TRANSACTIONS OF {batch_size} ROWS
    """

    try:
        summary = await _run_maintenance_query(
            _build_cleanup_cypher,
            f"Stub {label} cleanup",
            batch_size=STUB_CLEANUP_BATCH_SIZE,
            data_type=data_type,
        )
        if summary is None:
            # Retries exhausted — already logged. Report failure so the caller
            # nacks and the cleanup is retried on the next cycle rather than
            # being silently skipped.
            return False

        deleted = summary.counters.nodes_deleted
        if deleted > 0:
            logger.info(
                f"🧹 Cleaned up {deleted} stub {label} nodes (no sha256 property)",
                data_type=data_type,
                deleted=deleted,
            )
        else:
            logger.info(
                f"✅ No stub {label} nodes to clean up",
                data_type=data_type,
            )
    except Exception as e:
        logger.error(
            f"❌ Failed to clean up stub {label} nodes",
            data_type=data_type,
            error=str(e),
        )
        return False

    return True


async def cleanup_sentinel_master() -> bool:
    """Delete the fake ``(:Master {id: "0"})`` projected from Discogs' "no master" sentinel.

    Backstop only: the projection no longer creates it (see
    :mod:`graphinator.master_reference`). A graph loaded before that fix has one node
    that every masterless release points at, millions of ``DERIVED_FROM`` edges deep,
    which :func:`cleanup_stub_nodes` never collects because it only deletes ISOLATED
    stubs. The edges go first, in chunks, so no single transaction has to detach a
    node that large; the now-edgeless node is deleted last. Idempotent.

    Returns:
        True if the sweep succeeded (or there is nothing to do), False if it failed —
        so the caller can retry.
    """
    if graph is None:
        return True

    def _build_edge_cypher(batch_size: int | None) -> str:
        return f"""
    MATCH (:Master {{id: '{NO_MASTER_ID}'}})-[rel]-()
    CALL {{
        WITH rel
        DELETE rel
    }} IN TRANSACTIONS OF {batch_size} ROWS
    """

    def _build_node_cypher(_batch_size: int | None) -> str:
        return f"MATCH (m:Master {{id: '{NO_MASTER_ID}'}}) DETACH DELETE m"

    try:
        edge_summary = await _run_maintenance_query(
            _build_edge_cypher,
            "Sentinel Master edge cleanup",
            batch_size=STUB_CLEANUP_BATCH_SIZE,
        )
        if edge_summary is None:
            return False
        node_summary = await _run_maintenance_query(_build_node_cypher, "Sentinel Master node cleanup")
        if node_summary is None:
            return False

        if node_summary.counters.nodes_deleted > 0:
            logger.info(
                "🧹 Removed the sentinel Master node projected from Discogs master_id 0",
                edges_deleted=edge_summary.counters.relationships_deleted,
            )
        else:
            logger.info("✅ No sentinel Master node to clean up")
    except Exception as e:
        logger.error("❌ Failed to clean up the sentinel Master node", error=str(e))
        return False

    return True


def make_message_handler(
    data_type: str,
    name_field: str,
    default_name: str,
    process_fn: Any,
) -> Any:
    """Create a RabbitMQ message handler for the given data type."""

    entity = gm_telemetry.entity_for(data_type)

    async def handler(message: AbstractIncomingMessage) -> None:
        if shutdown_requested:
            # Leave the delivery UNACKED — never nack(requeue=True) here. The
            # consumer is still subscribed at this point, so a requeue is
            # redelivered within milliseconds and nacked again, in a hot loop that
            # nothing ends. Returning without settling lets the connection close
            # requeue them exactly once, which spends one x-delivery-count.
            logger.debug("🛑 Shutdown requested, leaving message unacked for redelivery")
            return

        record_id = "unknown"
        # messaging.client.consumed.messages normally comes from
        # common.rabbitmq_resilient.process_message_with_retry — this service acks/nacks
        # directly instead, so it is recorded locally in the `finally` below, once per
        # delivery regardless of which branch handled it (mirrors that wrapper: `error_type`
        # stays None unless this handler itself raised or rejected the record).
        destination = gm_telemetry.consumed_destination(message)
        consumed_error_type: str | None = None
        started = time.perf_counter()
        # The CONSUMER span for this delivery. It joins the trace the extractor's publish
        # span started, through the W3C context in the AMQP headers, so a record's whole
        # path is one trace; headers carrying no readable context start a new one. Every
        # db span the Neo4j wrapper opens below nests inside it, and in batch mode the
        # span's context is carried on the pending message so the eventual flush span can
        # link back to it.
        with gm_telemetry.consume_span(destination, getattr(message, "headers", None)) as span:
            try:
                logger.debug("🔄 Received message", data_type=data_type[:-1])
                try:
                    parsed = loads(message.body)
                except ValueError:
                    parsed = None
                if not isinstance(parsed, dict):
                    # Unreadable now means unreadable forever, so no retry can help.
                    logger.error("❌ Malformed message body, dead-lettering", data_type=data_type)
                    consumed_error_type = "MalformedRecord"
                    gm_telemetry.record_message(entity, "failed", time.perf_counter() - started)
                    await message.nack(requeue=False)
                    return
                record: dict[str, Any] = parsed

                if await check_file_completion(record, data_type, message):
                    return

                # Records after a terminal marker start fresh per-type work.
                # Re-arm stuck detection and prevent the old grace timer from
                # cancelling a consumer while the next extraction is importing.
                completed_files.discard(data_type)
                pending_cancel = consumer_cancel_tasks.pop(data_type, None)
                if pending_cancel is not None:
                    pending_cancel.cancel()

                if BATCH_MODE and batch_processor is not None:
                    accepted = await batch_processor.add_message(
                        data_type,
                        record,
                        message.ack,
                        # requeue=False: this callback nacks permanently-invalid
                        # input (unknown data_type, missing 'id', normalize failure,
                        # poison batch) — send it straight to the DLQ instead of
                        # retrying what cannot succeed. Transient failures are held
                        # in memory and retried, and never nack.
                        lambda: message.nack(requeue=False),
                        # This delivery's span has ended by the time the batch is written, so
                        # its context rides along and the flush span links back to it.
                        span_context=gm_telemetry.span_context_of(span),
                        # A delivery held in Neo4j backoff past BATCH_MAX_HOLD_S goes back to RabbitMQ
                        # before consumer_timeout can cancel this consumer, and the engine counts
                        # each trip through the broker's headers.
                        requeue_callback=lambda: message.nack(requeue=True),
                        headers=getattr(message, "headers", None),
                    )
                    if accepted:
                        # Note: message_counts tracks received (not processed) messages in
                        # batch mode. Actual processed count is in batch_processor.processed_counts.
                        # Messages nacked later by _process_*_batch (e.g. missing 'id') are included.
                        message_counts[data_type] += 1
                        last_message_time[data_type] = time.time()
                    # groovemap.pipeline.messages is not recorded here: in batch mode the
                    # per-record outcome (processed/skipped/failed) is only known once the batch
                    # is flushed, so Neo4jBatchProcessor records groovemap.pipeline.batch.* for
                    # this data instead.
                    return

                try:
                    record = normalize_record(data_type, record)
                except Exception as e:
                    # The same verdict batch mode reaches: bad input, not a failure to retry.
                    logger.error("❌ Failed to normalize record, dead-lettering", data_type=data_type, error=str(e))
                    consumed_error_type = type(e).__name__
                    gm_telemetry.record_message(entity, "failed", time.perf_counter() - started)
                    await message.nack(requeue=False)
                    return

                # Validate required 'id' field — nack with requeue=False to avoid
                # infinite requeue loop for malformed messages (matches tableinator).
                # Check both missing key and None value since normalize_record
                # sets id=None when the raw message lacks an id field.
                if not record.get("id"):
                    logger.error("❌ Message missing 'id' field", data_type=data_type)
                    consumed_error_type = "MissingIdError"
                    gm_telemetry.record_message(entity, "failed", time.perf_counter() - started)
                    await message.nack(requeue=False)
                    return

                record_id = record.get("id", "unknown")
                record_name = record.get(name_field, default_name)

                logger.debug(
                    f"🔄 Processing {data_type[:-1]}",
                    record_id=record_id,
                    record_name=record_name,
                )

                if graph is None:
                    raise RuntimeError("Neo4j driver not initialized")

                async with graph.session(database="neo4j") as session:

                    async def tx_fn(tx: Any) -> bool:
                        return bool(await process_fn(tx, record))

                    updated = await session.execute_write(tx_fn)

                # Ack first, then increment counters — avoids double-ack-then-nack
                # if ack raises (exception handler would attempt nack on already-acked msg)
                await message.ack()

                # Neo4j answered — clear the outage backoff.
                outage_backoff.reset()

                message_counts[data_type] += 1
                last_message_time[data_type] = time.time()
                gm_telemetry.record_message(entity, "processed" if updated else "skipped", time.perf_counter() - started)
                if message_counts[data_type] % progress_interval == 0:
                    logger.info(
                        f"📊 Processed {data_type} in Neo4j",
                        message_counts=message_counts[data_type],
                    )

                if updated:
                    logger.debug(
                        f"💾 Updated {data_type[:-1]} in Neo4j",
                        record_id=record_id,
                    )
                else:
                    logger.debug(
                        f"🔄 Skipped {data_type[:-1]} (no changes needed)",
                        record_id=record_id,
                    )
            except (ServiceUnavailable, SessionExpired, DatabaseUnavailableError) as e:
                consumed_error_type = type(e).__name__
                gm_telemetry.record_message(entity, "failed", time.perf_counter() - started)
                logger.warning(
                    f"⚠️ Neo4j unavailable, will retry {data_type[:-1]} message",
                    error=str(e),
                )
                try:
                    await requeue_record(message)
                except Exception as nack_error:
                    logger.warning("⚠️ Failed to nack message", error=str(nack_error))
            except Exception as e:
                consumed_error_type = type(e).__name__
                gm_telemetry.record_message(entity, "failed", time.perf_counter() - started)
                logger.error(
                    f"❌ Failed to process {data_type[:-1]} message",
                    record_id=record_id,
                    error=str(e),
                )
                try:
                    await requeue_record(message)
                except Exception as nack_error:
                    logger.warning("⚠️ Failed to nack message", error=str(nack_error))
            finally:
                gm_telemetry.mark_consumed_error(span, consumed_error_type)
                gm_telemetry.record_consumed_message(destination, consumed_error_type)

    return handler


on_artist_message = make_message_handler("artists", "name", "Unknown Artist", process_artist)
on_label_message = make_message_handler("labels", "name", "Unknown Label", process_label)
on_master_message = make_message_handler("masters", "title", "Unknown Master", process_master)
on_release_message = make_message_handler("releases", "title", "Unknown Release", process_release)

# Handler lookup by data type for consumer registration and recovery
HANDLERS: dict[str, Any] = {
    "artists": on_artist_message,
    "labels": on_label_message,
    "masters": on_master_message,
    "releases": on_release_message,
}


async def progress_reporter() -> None:
    """Periodically report processing progress and manage idle mode."""
    global idle_mode

    report_count = 0
    startup_time = time.time()
    last_idle_log = 0.0

    while not shutdown_requested:
        # More frequent reports initially, then every 30 seconds
        if report_count < 3:
            await asyncio.sleep(10)  # First 3 reports every 10 seconds
        else:
            await asyncio.sleep(30)  # Then every 30 seconds
        report_count += 1

        # Skip all logging if all files are complete
        if len(completed_files) == len(DATA_TYPES):
            continue

        total = sum(message_counts.values())
        current_time = time.time()

        # Idle mode detection: no messages received after STARTUP_IDLE_TIMEOUT
        # Idle mode only suppresses reporting - consumers stay connected
        if not idle_mode and total == 0 and (current_time - startup_time) >= STARTUP_IDLE_TIMEOUT:
            idle_mode = True
            last_idle_log = current_time
            logger.info(
                f"😴 No messages received after {STARTUP_IDLE_TIMEOUT}s, entering idle mode. Consumers remain connected, reporting paused.",
                startup_idle_timeout=STARTUP_IDLE_TIMEOUT,
            )
            continue

        # While in idle mode, only log briefly every IDLE_LOG_INTERVAL
        if idle_mode:
            if total > 0:
                # Messages started flowing, exit idle mode
                idle_mode = False
                logger.info("🔄 Messages detected, resuming normal operation")
            elif (current_time - last_idle_log) >= IDLE_LOG_INTERVAL:
                last_idle_log = current_time
                logger.info(
                    "😴 Idle mode - waiting for messages. Consumers connected.",
                )
            continue

        # Check for stalled consumers (skip completed files)
        stalled_consumers = []
        for data_type, last_time in last_message_time.items():
            if data_type not in completed_files and last_time > 0 and (current_time - last_time) > 120:  # No messages for 2 minutes
                stalled_consumers.append(data_type)

        if stalled_consumers:
            logger.error(f"⚠️ Stalled consumers detected: {stalled_consumers}. No messages processed for >2 minutes.")

        # Always show progress, even if no messages processed yet
        # Build progress string with completion emojis
        progress_parts = []
        for data_type in ["artists", "labels", "masters", "releases"]:
            emoji = "✅ " if data_type in completed_files else ""
            progress_parts.append(f"{emoji}{data_type.capitalize()}: {message_counts[data_type]}")

        logger.info(f"📊 Neo4j Progress: {total} total messages processed ({', '.join(progress_parts)})")

        # Log current processing state
        if total == 0:
            logger.info("⏳ Waiting for messages to process...")
        elif all(current_time - last_time < 5 for last_time in last_message_time.values() if last_time > 0):
            logger.info("✅ All consumers actively processing")
        elif any(last_time > 0 and 5 < current_time - last_time < 120 for last_time in last_message_time.values()):
            slow_consumers = [dt for dt, lt in last_message_time.items() if lt > 0 and 5 < current_time - lt < 120]
            logger.warning(
                f"⚠️ Slow consumers detected: {slow_consumers}",
                slow_consumers=slow_consumers,
            )

        # Log consumer status
        active_consumers = list(consumer_tags.keys())
        canceled_consumers = [dt for dt in DATA_TYPES if dt not in consumer_tags and dt in completed_files]

        if canceled_consumers:
            logger.info(
                f"🔧 Canceled consumers: {canceled_consumers}",
                canceled_consumers=canceled_consumers,
            )
        if active_consumers:
            logger.info(
                f"✅ Active consumers: {active_consumers}",
                active_consumers=active_consumers,
            )


async def main() -> None:
    global config, graph, queues, rabbitmq_manager, active_connection, active_channel, connection_check_task, batch_processor

    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    setup_logging(SERVICE_NAME, log_file=Path(f"/logs/{SERVICE_NAME}.log"))
    setup_telemetry(TELEMETRY_SERVICE_NAME)
    # Sample event-loop lag from this loop. Every consumer, flush, and maintenance task runs
    # on it, so a loop that stops scheduling promptly is the one signal that explains a
    # pipeline slowing down while Neo4j and RabbitMQ both look healthy. Returns None (and
    # costs nothing) when metrics export is off.
    start_event_loop_monitor()
    logger.info("🚀 Starting GrooveMap discogs-graph-enricher service")

    # Add startup delay for dependent services
    startup_delay = int(os.environ.get("STARTUP_DELAY", "5"))
    if startup_delay > 0:
        logger.info(f"⏳ Waiting {startup_delay} seconds for dependent services to start...")
        await asyncio.sleep(startup_delay)

    # Initialize configuration (before the health server, which binds config.health_port)
    try:
        config = GraphinatorConfig.from_env()
    except ValueError as e:
        logger.error("❌ Configuration error", error=str(e))
        raise StartupError("configuration error") from e

    # Start health server
    health_server = HealthServer(config.health_port, get_health_data)
    health_server.start_background()
    logger.info(f"🏥 Health server started on port {config.health_port}")

    # Initialize async resilient Neo4j driver
    graph = AsyncResilientNeo4jDriver(
        uri=config.neo4j_host,
        auth=(config.neo4j_username, config.neo4j_password),
        max_retries=5,
        **neo4j_security_kwargs(),
    )

    # Test Neo4j connectivity using async operations
    try:
        async with graph.session(database="neo4j") as session:
            result = await session.run("RETURN 1 as test")
            await result.single()
            logger.info("✅ Neo4j connectivity verified (async)")

        # Initialize batch processor if enabled
        if BATCH_MODE:
            batch_config = BatchConfig(
                batch_size=BATCH_SIZE,
                flush_interval=BATCH_FLUSH_INTERVAL,
                max_hold_s=BATCH_MAX_HOLD_S,
                max_requeues=BATCH_MAX_REQUEUES,
            )
            batch_processor = Neo4jBatchProcessor(graph, batch_config)
            logger.info(
                "🚀 Batch processing enabled",
                batch_size=BATCH_SIZE,
                flush_interval=BATCH_FLUSH_INTERVAL,
            )
        else:
            logger.info("📝 Using per-message processing (batch mode disabled)")

    except Exception as e:
        logger.error("❌ Failed to connect to Neo4j", error=str(e))
        raise StartupError("Neo4j connection failed") from e
    print(STARTUP_BANNER)

    # Initialize resilient RabbitMQ connection manager (not connecting yet)
    rabbitmq_manager = AsyncResilientRabbitMQ(
        connection_url=config.amqp_connection,
        max_retries=10,  # Startup needs extra attempts while the broker becomes ready.
        heartbeat=600,
        connection_attempts=10,
        retry_delay=5.0,
    )

    # Try to connect with additional retry logic for startup
    max_startup_retries = 5
    startup_retry = 0
    amqp_connection = None

    while startup_retry < max_startup_retries and not shutdown_requested:
        try:
            logger.info(f"🐰 Attempting to connect to RabbitMQ (attempt {startup_retry + 1}/{max_startup_retries})")
            amqp_connection = await rabbitmq_manager.connect()
            active_connection = amqp_connection
            break
        except Exception as e:
            startup_retry += 1
            if startup_retry < max_startup_retries:
                wait_time = min(30, 5 * startup_retry)  # Exponential backoff up to 30s
                logger.warning(f"⚠️ RabbitMQ connection failed: {e}. Retrying in {wait_time} seconds...")
                await asyncio.sleep(wait_time)
            else:
                logger.error(f"❌ Failed to connect to AMQP broker after {max_startup_retries} attempts: {e}")
                raise StartupError("AMQP connection failed") from e

    if amqp_connection is None:
        if shutdown_requested:
            # A signal arrived before the broker connected: a requested stop, not a failure.
            logger.info("🛑 Shutdown requested before AMQP connection was established")
            return
        logger.error("❌ No AMQP connection available")
        raise StartupError("no AMQP connection available")

    async with amqp_connection:
        channel = await amqp_connection.channel()
        active_channel = channel

        # Set QoS to allow concurrent batch processing for better throughput
        # prefetch_count must be >= batch_size to allow batches to fill before flushing.
        # QoS is deliberately per-consumer (aio-pika's global_=False default) so each
        # data type can fill its own batch; the resulting channel-wide ceiling is
        # prefetch_count x len(DATA_TYPES) and is logged so the multiplier is explicit
        # rather than silent. Unlike the Discogs SQL loader's non-batch mode,
        # graphinator writes through the Neo4j driver's own pool, so there is no small
        # fixed connection budget to couple the prefetch to.
        prefetch_count = max(200, BATCH_SIZE * 2) if BATCH_MODE else 200
        await channel.set_qos(prefetch_count=prefetch_count)
        logger.info(
            "🔧 QoS prefetch configured",
            prefetch_count=prefetch_count,
            channel_wide_max=prefetch_count * len(DATA_TYPES),
            batch_size=BATCH_SIZE if BATCH_MODE else "N/A",
        )

        # Declare per-data-type fanout exchanges and consumer-owned queues
        queues = {}
        for data_type in DATA_TYPES:
            exchange_name = catalog_exchange_name(data_type)
            queue_name = catalog_queue_name("graphinator", data_type)
            dlx_name = catalog_dead_letter_exchange_name("graphinator", data_type)
            dlq_name = catalog_dead_letter_queue_name("graphinator", data_type)

            # Declare fanout exchange (must match extractor)
            exchange = await channel.declare_exchange(exchange_name, AMQP_EXCHANGE_TYPE, durable=True, auto_delete=False)

            # Declare consumer-owned dead-letter exchange
            dlx_exchange = await channel.declare_exchange(dlx_name, AMQP_EXCHANGE_TYPE, durable=True, auto_delete=False)

            # Declare DLQ (classic queue for dead letters)
            dlq = await channel.declare_queue(
                auto_delete=False,
                durable=True,
                name=dlq_name,
                arguments={"x-queue-type": "classic"},
            )
            await dlq.bind(dlx_exchange)

            # Declare main quorum queue with consumer-owned DLX
            queue_args = {
                "x-queue-type": "quorum",
                "x-dead-letter-exchange": dlx_name,
                "x-delivery-limit": 20,
            }
            queue = await channel.declare_queue(
                auto_delete=False,
                durable=True,
                name=queue_name,
                arguments=queue_args,
            )
            await queue.bind(exchange)
            queues[data_type] = queue

        # Start consumers for all data types
        for data_type, handler in HANDLERS.items():
            consumer_tags[data_type] = await queues[data_type].consume(handler, consumer_tag=f"{SERVICE_NAME}-{data_type}")
            gm_telemetry.record_consumer_started()

        logger.info(
            f"🚀 GrooveMap {SERVICE_NAME} started! Connected to AMQP broker ({len(DATA_TYPES)} fanout exchanges). "
            f"Consuming from {len(DATA_TYPES)} queues. "
            "Ready to process messages into Neo4j. Press CTRL+C to exit"
        )

        progress_task = asyncio.create_task(progress_reporter())

        # Start batch flush task if using batch mode
        batch_flush_task = None
        if BATCH_MODE and batch_processor is not None:
            batch_flush_task = asyncio.create_task(batch_processor.periodic_flush())
            logger.info("🔄 Started batch periodic flush task")

        # A restart mid-maintenance loses the (already acked) trigger; the durable
        # node says whether the newest dump still needs its maintenance pass. If it does
        # not, a dump stamped by older code may still need the current stats.
        if not await resume_pending_maintenance():
            await resume_stats_refresh()

        # Start periodic queue checker task
        connection_check_task = asyncio.create_task(periodic_queue_checker())
        logger.info(f"🔄 Started periodic queue checker (interval: {QUEUE_CHECK_INTERVAL}s)")

        try:
            # Create a shutdown event that can be triggered by signal handler
            shutdown_event = asyncio.Event()

            # Check for shutdown periodically
            while not shutdown_requested:
                try:
                    await asyncio.wait_for(shutdown_event.wait(), timeout=1.0)
                    break
                except TimeoutError:
                    continue

        except KeyboardInterrupt:
            logger.info("🛑 Received interrupt signal, shutting down gracefully")
        finally:
            # Stop new deliveries FIRST, before the multi-second flush/teardown
            # below: a still-subscribed consumer keeps being handed messages it
            # can only leave unacked.
            await cancel_all_consumers()

            # Cancel progress reporting
            progress_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await progress_task

            # Cancel connection check task
            if connection_check_task:
                connection_check_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await connection_check_task
                logger.info("✅ Queue checker task stopped")

            # Cancel batch flush task and flush remaining messages
            if batch_flush_task:
                batch_flush_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await batch_flush_task
            if batch_processor:
                batch_processor.shutdown()
                try:
                    await batch_processor.flush_all()
                    logger.info("✅ Batch processor flushed and stopped")
                except Exception as e:
                    logger.error("❌ Error flushing batch processor", error=str(e))

            # Cancel any pending consumer cancellation tasks
            for task in list(consumer_cancel_tasks.values()):
                task.cancel()

            # Cancel detached post-import maintenance. Its trigger was already
            # acked, and every step is idempotent — an interrupted sweep is
            # re-run by the next extraction_complete or manually.
            for maintenance_task in list(maintenance_tasks):
                maintenance_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await maintenance_task
            if maintenance_tasks:
                logger.info("✅ Post-import maintenance task stopped")

            # Close RabbitMQ connection if still active
            await close_rabbitmq_connection()

            # Close async Neo4j driver
            try:
                if graph is not None:
                    await graph.close()
                logger.info("✅ Async Neo4j driver closed")
            except Exception as e:
                logger.warning("⚠️ Error closing Neo4j driver", error=str(e))

        # Stop health server
        health_server.stop()


def cli() -> None:
    """Run the async service from a console-script entry point.

    Exits 1 when startup gives up (StartupError) or the service dies on an unexpected error, so
    Docker's ``restart: on-failure`` restarts it; a signal-driven shutdown exits 0.
    """
    exit_code = 0
    try:
        run(main())
    except KeyboardInterrupt:
        logger.warning("⚠️ Application interrupted")
    except StartupError as e:
        # The specific cause was already logged where it was raised.
        logger.error("❌ Startup failed", error=str(e))
        exit_code = 1
    except Exception as e:
        logger.error("❌ Application error", error=str(e))
        exit_code = 1
    finally:
        # Called here rather than inside main(): main() raises on its early give-up paths (config
        # error, Neo4j/AMQP connection failure) before its own try/finally block, so
        # this is the one place reached on every shutdown path. shutdown_telemetry() is a plain
        # synchronous call (safe outside the closed event loop) and never raises.
        shutdown_telemetry()
        logger.info("✅ GrooveMap discogs-graph-enricher shutdown complete")
    if exit_code:
        sys.exit(exit_code)


if __name__ == "__main__":
    cli()
