"""Resumable, keyed-pagination plumbing for the post-import aggregate sweeps.

Why this exists
---------------
The Genre/Style/Label stats and the Person credit-count recompute used to be one
Cypher statement each (``CALL { } IN TRANSACTIONS``). ``IN TRANSACTIONS`` commits the
inner work in chunks, but the statement as a whole is still ONE query on ONE outer
transaction, so the server's ``db.transaction.timeout`` bounded the entire sweep. It is
set on the server in ``deployment/docker-compose*.yml`` (``120s`` base file, ``600s``
production overlay, which lux runs); nothing in this repo or ``python-libraries`` sets a
client-side transaction timeout.

Measured on the real graph on lux (600s limit), the old single statements took: Genre
2m16s, Style about 9m35s (close to the limit), Label 2m12s (7.3M properties set), and
Person credit counts hit the limit at exactly 600s on both attempts
(``Neo.ClientError.Transaction.TransactionTimedOutClientConfiguration``). So Person is
the step that failed outright and Style was a near miss that grows with the graph; a
retry of the identical statement can only fail the same way.

The sweep is now shaped as many short statements instead:

* each statement handles one *page* of nodes after a key cursor (``n.id > $cursor
  ORDER BY n.id LIMIT $size``), so it is bounded by the page size, not the graph size;
* after every page the cursor is committed to a ``(:MaintenanceCheckpoint)`` node, so a
  retry, a crash or a restart continues after the last committed page and never redoes
  completed work;
* a page that times out or hits transaction-memory pressure is retried at half the size,
  and the size that worked is remembered in the checkpoint;
* a finished step is marked ``done`` and skipped by later passes of the same run.

A checkpoint belongs to one *run key* (the dump version being maintained). A different
run key starts the step from the beginning, so counts are recomputed for every new dump.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import structlog
from neo4j import Query
from neo4j.exceptions import Neo4jError, TransientError


logger = structlog.get_logger(__name__)

CHECKPOINT_LABEL = "MaintenanceCheckpoint"

# Optional per-statement client transaction timeout, in seconds. Unset (the default)
# leaves the server's db.transaction.timeout in charge. This is a documented BACKSTOP
# only: the paging above is what keeps every statement inside the server budget.
MAINTENANCE_TX_TIMEOUT_ENV = "MAINTENANCE_TRANSACTION_TIMEOUT_SECONDS"

# Floor for page halving: below this the round-trip overhead dominates.
MIN_PAGE_SIZE = 1

# Consecutive failed attempts at the smallest page before the step gives up.
MAX_FAILURES_AT_MIN_PAGE = 3
PAGE_RETRY_BASE_DELAY_SECONDS = 30.0

# Highest cursor sorting below every key: all real keys compare greater.
START_CURSOR = ""


def tx_timeout_seconds() -> float | None:
    """Return the optional client transaction timeout backstop, or ``None``."""
    raw = os.environ.get(MAINTENANCE_TX_TIMEOUT_ENV, "").strip()
    if not raw:
        return None
    try:
        value = float(raw)
    except ValueError:
        logger.warning("⚠️ Ignoring non-numeric maintenance transaction timeout", value=raw)
        return None
    return value if value > 0 else None


def is_page_retryable(error: BaseException) -> bool:
    """Whether ``error`` means "this page was too much", i.e. retry with a smaller one.

    Transient errors cover memory-pool exhaustion and lock contention; the timeout codes
    cover a statement that outlived the transaction timeout (server- or client-set).
    """
    if isinstance(error, TransientError):
        return True
    if isinstance(error, Neo4jError):
        code = getattr(error, "code", None) or ""
        return "TransactionTimedOut" in code
    return False


async def run_statement(graph: Any, cypher: str, **params: Any) -> dict[str, Any] | None:
    """Run one statement in its own session and return its single record (or ``None``)."""
    timeout = tx_timeout_seconds()
    query: Any = Query(cypher, timeout=timeout) if timeout else cypher
    async with graph.session(database="neo4j") as session:
        result = await session.run(query, **params)
        record = await result.single()
        await result.consume()
    return dict(record) if record is not None else None


async def run_rows(graph: Any, cypher: str, **params: Any) -> list[dict[str, Any]]:
    """Run one statement in its own session and return all its records."""
    timeout = tx_timeout_seconds()
    query: Any = Query(cypher, timeout=timeout) if timeout else cypher
    async with graph.session(database="neo4j") as session:
        result = await session.run(query, **params)
        rows = [dict(record) async for record in result]
        await result.consume()
    return rows


@dataclass
class Checkpoint:
    """Persisted progress of one paged step."""

    cursor: str = START_CURSOR
    size: int | None = None
    done: bool = False


async def read_checkpoint(graph: Any, step: str, run_key: str) -> Checkpoint:
    """Load the checkpoint for ``step``; a different ``run_key`` reads as fresh."""
    record = await run_statement(
        graph,
        f"""
        OPTIONAL MATCH (c:{CHECKPOINT_LABEL} {{step: $step}})
        RETURN c.run_key AS run_key, c.cursor AS cursor, c.size AS size, c.done AS done
        """,
        step=step,
    )
    if record is None or record.get("run_key") != run_key:
        return Checkpoint()
    return Checkpoint(
        cursor=record.get("cursor") or START_CURSOR,
        size=record.get("size"),
        done=bool(record.get("done")),
    )


async def write_checkpoint(graph: Any, step: str, run_key: str, checkpoint: Checkpoint) -> None:
    """Commit ``checkpoint`` for ``step`` under ``run_key``."""
    await run_statement(
        graph,
        f"""
        MERGE (c:{CHECKPOINT_LABEL} {{step: $step}})
        SET c.run_key = $run_key, c.cursor = $cursor, c.size = $size, c.done = $done,
            c.updated_at = datetime()
        RETURN count(c) AS written
        """,
        step=step,
        run_key=run_key,
        cursor=checkpoint.cursor,
        size=checkpoint.size,
        done=checkpoint.done,
    )


# A page processor takes (cursor, size) and returns (last_key, processed). processed == 0
# means the key space after ``cursor`` is exhausted.
PageProcessor = Callable[[str, int], Awaitable[tuple[str | None, int]]]


async def run_checkpointed_step(
    graph: Any,
    *,
    step: str,
    run_key: str | None,
    process_page: PageProcessor,
    page_size: int,
    min_page_size: int = MIN_PAGE_SIZE,
) -> int:
    """Drive ``process_page`` across the key space, committing a checkpoint per page.

    Args:
        graph: Neo4j driver.
        step: Stable step name; the checkpoint key and the name in every log line.
        run_key: Identity of the run being maintained. ``None`` disables persistence
            (a direct call outside a maintenance run): progress then lives in memory
            for this call only.
        process_page: Processes the page after ``cursor`` and reports where it ended.
        page_size: Starting page size, unless the checkpoint remembers a smaller one.
        min_page_size: Page-size floor for the halving backoff.

    Returns:
        The number of items processed by THIS call (0 if the step was already done).

    Raises:
        Whatever ``process_page`` raises that is not page-retryable, and the last
        retryable error once ``MAX_FAILURES_AT_MIN_PAGE`` attempts at the minimum page
        size have failed. The checkpoint keeps every committed page either way.
    """
    checkpoint = Checkpoint()
    if run_key is not None:
        checkpoint = await read_checkpoint(graph, step, run_key)
        if checkpoint.done:
            logger.info("⏭️ Maintenance step already complete for this run — skipping", step=step, run_key=run_key)
            return 0
        if checkpoint.cursor != START_CURSOR:
            logger.info("▶️ Resuming maintenance step after its last committed page", step=step, cursor=checkpoint.cursor)

    size = max(min_page_size, min(page_size, checkpoint.size or page_size))
    cursor = checkpoint.cursor
    total = 0
    failures_at_min = 0

    while True:
        try:
            last, processed = await process_page(cursor, size)
        except Exception as e:
            if not is_page_retryable(e):
                logger.error("❌ Maintenance step failed", step=step, cursor=cursor, page_size=size, error=str(e))
                raise
            if size > min_page_size:
                size = max(min_page_size, size // 2)
                logger.warning(
                    "⚠️ Maintenance page too large for the transaction budget — retrying smaller",
                    step=step,
                    cursor=cursor,
                    next_page_size=size,
                    error=str(e),
                )
                continue
            failures_at_min += 1
            if failures_at_min >= MAX_FAILURES_AT_MIN_PAGE:
                logger.error(
                    "❌ Maintenance step cannot make progress even at the minimum page size",
                    step=step,
                    cursor=cursor,
                    page_size=size,
                    error=str(e),
                )
                raise
            delay = PAGE_RETRY_BASE_DELAY_SECONDS * (2 ** (failures_at_min - 1))
            logger.warning("⚠️ Maintenance page failed at the minimum size — backing off", step=step, retry_in_seconds=delay, error=str(e))
            await asyncio.sleep(delay)
            continue

        failures_at_min = 0
        if processed == 0 or last is None:
            if run_key is not None:
                await write_checkpoint(graph, step, run_key, Checkpoint(cursor=cursor, size=size, done=True))
            logger.info("✅ Maintenance step complete", step=step, processed_this_call=total)
            return total

        cursor = last
        total += processed
        if run_key is not None:
            await write_checkpoint(graph, step, run_key, Checkpoint(cursor=cursor, size=size))
        logger.info("📊 Maintenance page committed", step=step, cursor=cursor, page_size=size, processed_this_call=total)


def keyed_page_processor(graph: Any, page_cypher: str) -> PageProcessor:
    """Build a page processor from a Cypher page statement.

    ``page_cypher`` takes ``$cursor`` and ``$size`` and returns ``last`` (the greatest
    key it handled) and ``processed`` (how many nodes it handled).
    """

    async def process_page(cursor: str, size: int) -> tuple[str | None, int]:
        record = await run_statement(graph, page_cypher, cursor=cursor, size=size)
        if record is None:
            return None, 0
        return record.get("last"), int(record.get("processed") or 0)

    return process_page


def per_key_page_processor(
    graph: Any,
    *,
    list_keys_cypher: str,
    statements_for_key: Callable[[str], list[tuple[str, dict[str, Any]]]],
) -> PageProcessor:
    """Build a page processor that lists keys, then runs several statements per key.

    For low-cardinality nodes (Genre, Style) whose per-node work is heavy: each metric is
    its OWN statement, so no single transaction carries more than one metric of one node.
    ``list_keys_cypher`` takes ``$cursor`` and ``$size`` and returns rows with ``key`` in
    ascending order. The page is only reported as processed once every statement of every
    key in it has run, and the checkpoint is written after the page, so a crash redoes at
    most that page (every statement is an idempotent SET).
    """

    async def process_page(cursor: str, size: int) -> tuple[str | None, int]:
        rows = await run_rows(graph, list_keys_cypher, cursor=cursor, size=size)
        keys = [row["key"] for row in rows]
        for key in keys:
            for cypher, params in statements_for_key(key):
                await run_statement(graph, cypher, key=key, **params)
        return (keys[-1] if keys else None), len(keys)

    return process_page
