"""Each real label's place in catalog-api's label-longevity ranking, stored as ``l.longevity_rank``.

graph-explorer shows "#N of M" on every label page, and catalog-api answers that from the
graph instead of ranking 1.2M labels per request. The rank is that of catalog-api's
``_LABEL_LONGEVITY_CYPHER`` (``api/queries/insights_neo4j_queries.py``): the same eligible
labels, in the same order, numbered 1..M with no gaps and no shared places.

- **Eligible:** the label has a name, at least one dated release (``total_releases > 0``),
  and is not a placeholder by catalog-api's rule (the constants below).
- **Order:** ``years_active`` descending, then ``total_releases`` descending, then ``l.id``
  ascending. ``l.id`` is a string and is compared as one, in Cypher, so ``'10'`` ranks before
  ``'9'`` exactly as catalog-api's ``ORDER BY ... l.id`` does.

The ranking is global, so it cannot be worked out by the keyed Label pages: it runs once,
after every page has written ``years_active`` and ``total_releases``, and before the dump's
``stats_version`` is stamped. One statement sorts the eligible labels (then the ineligible
ones still carrying a rank), numbers them, and returns only the labels whose stored rank is
wrong, after a first row that carries M. Only those are written, in batches of
:data:`RANK_WRITE_BATCH_SIZE` per transaction; a label that is no longer eligible loses its
rank. A rerun over unchanged stats returns one row and writes nothing.

Numbering in Cypher keeps the unchanged labels off the wire: on 2.4M labels (1.1M ranked) the
Python driver takes about 20 s just to decode 1.1M records, where the server's scan, sort and
comparison take about 6 s on a laptop container (lux sorted every label in 1.3 s).

Memory: the read holds about 420 MB of transaction memory at that size (418 MB measured:
386 MB for the sort, 32 MB for the collected labels). Lux caps transaction memory at 3g in total
(``dbms.memory.transaction.total.max``), and that cap is shared by every transaction running
at the time, so the read takes about a seventh of it. The pass runs after the import has
drained, but catalog-api's reads still use the same pool. A read refused for memory fails
the step, and the pass retries it.

Timeout: the writes run while the read is still streaming, each batch in its own session, so
client memory stays at one batch whatever the number of labels. The read's transaction
therefore stays open for the whole phase, writes included, and the whole phase has to finish
inside the server's transaction timeout (600 s on lux, 120 s in the base compose file). On
the laptop container a first run writing all 1.1M ranks took 42.5 s, a 5% shift 14 s, and a
rerun 4.9 s. The read takes no locks, and each batch writes only labels the stream has
already passed, so the read and the writes never touch the same label. If the read
does outlive the timeout, the step fails and the next attempt starts again. Every batch
already written is correct and is not written again, so each attempt gets further.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import structlog
from neo4j import Query

from graphinator import maintenance_paging


logger = structlog.get_logger(__name__)

# catalog-api's placeholder rule (api/queries/insights_neo4j_queries.py). They must equal
# catalog-api's, or a label ranks here that catalog-api leaves out (or the reverse) and every
# rank below it is off by one. scripts/check-contracts.py fails `just check` when they differ
# from contracts/label-placeholders/v1/source.json, which pins them to a catalog-api commit.
PLACEHOLDER_LABEL_IDS = ("1818",)
PLACEHOLDER_LABEL_NAME = r"(?is)not on label *[(\[{].*|.*not on label.*self.?release.*"
REAL_LABELS_NAMED_LIKE_PLACEHOLDERS = ("109707", "2614996")

# Changed ranks per write transaction. A monthly dump can move most of the ~1.2M ranks.
RANK_WRITE_BATCH_SIZE = 10_000

# Eligible labels first, in catalog-api's order, then the ineligible labels that still carry
# a rank. ``ranked`` is never null: a label with no stats (total_releases absent) or no name is
# simply not ranked. Position 0 is a header row carrying M (``ranked_count``), so M comes back
# even when no rank changed; every other row is a label whose stored rank differs from its
# position (or that must lose its rank, ``rank`` null).
RANKED_LABELS_CYPHER = """
MATCH (l:Label)
WITH l, coalesce(
        l.name IS NOT NULL AND l.total_releases > 0
        AND NOT (l.id IN $placeholder_ids OR (l.name =~ $placeholder_name AND NOT l.id IN $real_lookalike_ids)),
        false) AS ranked
WHERE ranked OR l.longevity_rank IS NOT NULL
WITH l, ranked
ORDER BY ranked DESC, l.years_active DESC, l.total_releases DESC, l.id
WITH collect(l) AS labels, count(CASE WHEN ranked THEN 1 END) AS ranked_count
UNWIND range(0, size(labels)) AS position
WITH ranked_count, position, CASE WHEN position > 0 THEN labels[position - 1] END AS l
WITH ranked_count, position, l, CASE WHEN position > 0 AND position <= ranked_count THEN position END AS rank
WHERE position = 0 OR coalesce(l.longevity_rank, 0) <> coalesce(rank, 0)
RETURN position, l.id AS id, rank, ranked_count
"""

# A null rank removes the property. The guard repeats the read's comparison so a batch never
# rewrites a value that is already right.
WRITE_RANKS_CYPHER = """
UNWIND $rows AS row
MATCH (l:Label {id: row.id})
WITH l, row
WHERE coalesce(l.longevity_rank, 0) <> coalesce(row.rank, 0)
SET l.longevity_rank = row.rank
"""

BeforeBatch = Callable[[], Awaitable[None]]


@dataclass(frozen=True)
class RankResult:
    """What one rank phase did."""

    ranked: int
    """M: how many labels carry a rank, 1..M."""
    written: int
    """Properties the write batches set or removed (Neo4j's counters). 0 on a rerun."""
    batches: int
    """Write transactions run."""


def _query(cypher: str) -> Any:
    timeout = maintenance_paging.tx_timeout_seconds()
    return Query(cypher, timeout=timeout) if timeout else cypher


async def _write(graph: Any, rows: list[dict[str, Any]]) -> int:
    async with graph.session(database="neo4j") as session:
        result = await session.run(_query(WRITE_RANKS_CYPHER), rows=rows)
        summary = await result.consume()
    return int(summary.counters.properties_set)


async def compute_label_longevity_ranks(graph: Any, *, before_batch: BeforeBatch | None = None) -> RankResult:
    """Write ``longevity_rank`` on every eligible Label and remove it from the rest.

    Args:
        graph: Neo4j driver.
        before_batch: Called before the read and before every write batch; raising
            :class:`~graphinator.maintenance_paging.MaintenanceStopped` stops the phase
            (a stats refresh uses it to give way to a new dump).

    Returns:
        M, the properties written and the write transactions run.
    """
    ranked = 0
    written = 0
    batches = 0
    pending: list[dict[str, Any]] = []

    async def flush() -> None:
        nonlocal written, batches
        if before_batch is not None:
            await before_batch()
        written += await _write(graph, pending)
        batches += 1
        pending.clear()

    if before_batch is not None:
        await before_batch()
    async with graph.session(database="neo4j") as session:
        result = await session.run(
            _query(RANKED_LABELS_CYPHER),
            placeholder_ids=list(PLACEHOLDER_LABEL_IDS),
            placeholder_name=PLACEHOLDER_LABEL_NAME,
            real_lookalike_ids=list(REAL_LABELS_NAMED_LIKE_PLACEHOLDERS),
        )
        async for record in result:
            if record["position"] == 0:
                ranked = int(record["ranked_count"])
                continue
            pending.append({"id": record["id"], "rank": record["rank"]})
            if len(pending) >= RANK_WRITE_BATCH_SIZE:
                await flush()
        await result.consume()
    if pending:
        await flush()

    return RankResult(ranked=ranked, written=written, batches=batches)
