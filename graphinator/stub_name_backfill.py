"""Backfill names onto Artist and Label stubs created before releases carried them.

Before the projection copied a release's reference name onto the stub it creates, a label or
artist that has no record of its own was left with ``name = null`` for good: the stub has
edges, so ``cleanup_stub_nodes`` keeps it, and the release hash gate skips the unchanged
releases on a re-import. The name exists only in the release data, so this reads it from an
NDJSON file of ``{"kind": "Label"|"Artist", "id": "...", "name": "..."}`` lines (one per
distinct entity; see ``docs/stub-name-backfill.md`` for the Postgres query that produces it).

Each batch is its own transaction and only fills a node whose name is still missing, so a
real entity name is never overwritten, a repeat run is a no-op, and a crash resumes from
the checkpoint file's line offset (or simply re-runs from line 0 without harm).

Usage: ``python -m graphinator.stub_name_backfill FILE [--batch-size N] [--checkpoint PATH] [--dry-run]``
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path
from typing import Any

import structlog
from orjson import JSONDecodeError, loads

from graphinator.reference_name import reference_name


logger = structlog.get_logger(__name__)

DEFAULT_BATCH_SIZE = 5000

# One query per kind: labels cannot be parameterised in Cypher. Fill-only by construction.
FILL_NAME_CYPHER = {
    "Label": "UNWIND $rows AS row MATCH (n:Label {id: row.id}) WHERE n.name IS NULL SET n.name = row.name RETURN count(n) AS filled",
    "Artist": "UNWIND $rows AS row MATCH (n:Artist {id: row.id}) WHERE n.name IS NULL SET n.name = row.name RETURN count(n) AS filled",
}


def parse_line(line: str) -> tuple[str, dict[str, str]] | None:
    """Return (kind, {"id", "name"}) for a usable line, or None for a blank/invalid one."""
    line = line.strip()
    if not line:
        return None
    try:
        data = loads(line)
    except JSONDecodeError:
        return None
    if not isinstance(data, dict) or data.get("kind") not in FILL_NAME_CYPHER:
        return None
    name = reference_name(data)
    entity_id = data.get("id")
    if name is None or entity_id is None or isinstance(entity_id, bool):
        return None
    return data["kind"], {"id": str(entity_id), "name": name}


def read_checkpoint(path: Path | None) -> int:
    """Return the number of input lines already applied (0 when none)."""
    if path is None or not path.exists():
        return 0
    try:
        return max(0, int(path.read_text().strip() or "0"))
    except ValueError:
        return 0


def write_checkpoint(path: Path | None, lines_done: int) -> None:
    if path is not None:
        path.write_text(f"{lines_done}\n")


async def backfill(
    driver: Any,
    source: Path,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    checkpoint: Path | None = None,
    dry_run: bool = False,
) -> dict[str, int]:
    """Apply the file in batches; return counts of lines read, skipped and nodes filled."""
    start = read_checkpoint(checkpoint)
    totals = {"lines": 0, "skipped": 0, "filled": 0}
    batch: dict[str, list[dict[str, str]]] = {kind: [] for kind in FILL_NAME_CYPHER}
    pending_lines = 0

    async def flush(position: int) -> None:
        nonlocal pending_lines
        for kind, rows in batch.items():
            if rows and not dry_run:
                async with driver.session(database="neo4j") as session:

                    async def work(tx: Any, cypher: str = FILL_NAME_CYPHER[kind], rows: list[dict[str, str]] = rows) -> int:
                        result = await tx.run(cypher, rows=rows)
                        record = await result.single()
                        return int(record["filled"]) if record else 0

                    totals["filled"] += await session.execute_write(work)
            rows.clear()
        if not dry_run:
            write_checkpoint(checkpoint, position)
        pending_lines = 0
        logger.info("stub name backfill progress", lines=position, filled=totals["filled"], skipped=totals["skipped"])

    with source.open(encoding="utf-8") as handle:
        position = 0
        for position, line in enumerate(handle, start=1):
            if position <= start:
                continue
            totals["lines"] += 1
            parsed = parse_line(line)
            if parsed is None:
                totals["skipped"] += 1
                continue
            kind, row = parsed
            batch[kind].append(row)
            pending_lines += 1
            if pending_lines >= batch_size:
                await flush(position)
        if pending_lines or position > start:
            await flush(position)
    return totals


async def count_missing(driver: Any) -> dict[str, int]:
    """Count nameless Artist and Label nodes that still have edges (the stubs this fills)."""
    counts: dict[str, int] = {}
    async with driver.session(database="neo4j") as session:
        for kind in FILL_NAME_CYPHER:
            result = await session.run(f"MATCH (n:{kind}) WHERE n.name IS NULL RETURN count(n) AS missing")
            record = await result.single()
            counts[kind] = int(record["missing"]) if record else 0
    return counts


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m graphinator.stub_name_backfill", description=__doc__.split("\n\n")[0])
    parser.add_argument("source", type=Path, help="NDJSON file of {kind, id, name} lines")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="rows per transaction (default %(default)s)")
    parser.add_argument("--checkpoint", type=Path, default=None, help="file holding the count of lines applied, to resume from")
    parser.add_argument("--dry-run", action="store_true", help="parse and count only; write nothing")
    return parser


async def run(args: argparse.Namespace) -> int:
    from common import AsyncResilientNeo4jDriver, neo4j_security_kwargs  # noqa: PLC0415

    from graphinator.config import GraphinatorConfig  # noqa: PLC0415

    if args.batch_size < 1:
        logger.error("batch size must be at least 1")
        return 2
    config = GraphinatorConfig.from_env()
    driver = AsyncResilientNeo4jDriver(
        uri=config.neo4j_host,
        auth=(config.neo4j_username, config.neo4j_password),
        max_retries=5,
        **neo4j_security_kwargs(),
    )
    try:
        before = await count_missing(driver)
        totals = await backfill(driver, args.source, batch_size=args.batch_size, checkpoint=args.checkpoint, dry_run=args.dry_run)
        after = await count_missing(driver)
    finally:
        await driver.close()
    logger.info("stub name backfill done", nameless_before=before, nameless_after=after, **totals)
    return 0


def main() -> None:
    sys.exit(asyncio.run(run(build_parser().parse_args())))


if __name__ == "__main__":
    main()
