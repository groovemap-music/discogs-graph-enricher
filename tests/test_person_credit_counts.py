"""Person credit counts: the CREDITED_ON prune and the post-import recompute.

catalog-api's role leaderboard reads Person.credit_count and
Person.credit_count_<category> (indexed by database-schema) instead of counting
CREDITED_ON edges at query time. Nothing wrote those properties, and the MERGE-only
credit writes never removed a credit Discogs had dropped, so even a recount would have
kept counting it. These tests pin the contract: per category in the shared
ALL_CATEGORIES vocabulary, the number of DISTINCT releases; credit_count is the
distinct-release total; a lost category reads 0; the recompute is batched and retried
under transaction-memory pressure like every other maintenance sweep. The counts
themselves are evaluated by a real engine in tests/integration.
"""

from __future__ import annotations

import re
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from common.credit_roles import ALL_CATEGORIES
from neo4j.exceptions import Neo4jError

import graphinator.graphinator as g
from graphinator import person_credits
from graphinator.batch_processor import Neo4jBatchProcessor, PendingMessage
from graphinator.person_credits import (
    CREDIT_COUNT_PROPERTIES,
    PRUNE_CREDITED_ON_CYPHER,
    build_credit_count_cypher,
    credit_count_property,
    credited_on_prune_record,
    person_credit_rows,
)
from tests.test_batch_processor import create_async_result_mock, create_async_session_mock


# ── Property contract ───────────────────────────────────────────────────────


def test_properties_follow_the_shared_category_vocabulary() -> None:
    """One count per ALL_CATEGORIES entry plus the total — the names database-schema indexes."""
    assert person_credits.ALL_CATEGORIES is ALL_CATEGORIES, "the vocabulary must be imported, not copied"
    assert ("credit_count", *(f"credit_count_{category}" for category in ALL_CATEGORIES)) == CREDIT_COUNT_PROPERTIES
    assert credit_count_property("mastering") == "credit_count_mastering"


def test_recompute_writes_every_property_on_every_person() -> None:
    """A lost category is rewritten to 0, never left at its old value or skipped."""
    cypher = build_credit_count_cypher(1000)
    for prop in CREDIT_COUNT_PROPERTIES:
        assert re.search(rf"\bp\.{prop} = ", cypher), f"{prop} must be SET on every person"
    # OPTIONAL MATCH: a person whose credits were all pruned still gets its zeros.
    assert "OPTIONAL MATCH (p)-[c:CREDITED_ON]->(r:Release)" in cypher
    for category in ALL_CATEGORIES:
        assert f"sum(CASE WHEN '{category}' IN categories THEN 1 ELSE 0 END) AS count_{category}" in cypher
    assert "REMOVE" not in cypher


def test_recompute_counts_distinct_releases_not_edges() -> None:
    """Two roles on one release (Producer + Executive Producer, or Producer + Mixed By)
    are two CREDITED_ON edges. The edges are grouped per release before counting, so the
    release counts once in the total and once in each category it carries."""
    cypher = build_credit_count_cypher(1000)
    group = cypher.index("WITH p, r, collect(DISTINCT c.category) AS categories")
    total = cypher.index("count(r) AS total")
    assert group < total, "edges must be grouped per release before they are counted"
    assert "count(c)" not in cypher


def test_recompute_driving_match_is_outside_the_transactional_subquery() -> None:
    """Same regression as the Genre/Style/Label stats: a driving MATCH inside the CALL
    gives the subquery a single input row, so the whole recompute runs in ONE
    transaction — exactly the unbounded working set the batching exists to prevent."""
    cypher = build_credit_count_cypher(250)
    assert cypher.index("MATCH (p:Person)") < cypher.index("CALL {")
    after_call = cypher[cypher.index("CALL {") + len("CALL {") :]
    assert after_call.strip().startswith("WITH p")
    assert re.search(r"\} IN TRANSACTIONS OF 250 ROWS", cypher)


def test_recompute_batch_size_is_bounded() -> None:
    assert g.MAINTENANCE_MIN_BATCH_SIZE <= g.PERSON_CREDIT_COUNT_BATCH_SIZE <= 1000


# ── Credit rows and the CREDITED_ON prune ─────────────────────────────────────


def test_credit_rows_categorize_and_skip_unusable_credits() -> None:
    rows = person_credit_rows(
        "R1",
        [
            {"name": "Flood", "role": "Producer", "id": "600"},
            {"name": "Flood", "role": "Mixed By"},
            {"name": "", "role": "Bass"},
            {"name": "No Role"},
        ],
    )
    assert rows == [
        {"name": "Flood", "role": "Producer", "category": "production", "release_id": "R1", "artist_id": "600"},
        {"name": "Flood", "role": "Mixed By", "category": "engineering", "release_id": "R1"},
    ]
    assert person_credit_rows("R1", None) == []


def test_prune_record_keeps_name_and_role_pairs() -> None:
    """The pair mirrors the merge key, so one of two roles can be dropped on its own."""
    rows = person_credit_rows("R1", [{"name": "Flood", "role": "Producer"}, {"name": "Flood", "role": "Mixed By"}])
    assert credited_on_prune_record("R1", rows) == {"key": "R1", "keep": [["Flood", "Producer"], ["Flood", "Mixed By"]]}
    assert credited_on_prune_record("R2", []) == {"key": "R2", "keep": []}
    assert "WHERE NOT [p.name, rel.role] IN record.keep" in PRUNE_CREDITED_ON_CYPHER
    assert "DELETE rel" in PRUNE_CREDITED_ON_CYPHER


def _batch_tx_capture() -> tuple[Neo4jBatchProcessor, list[tuple[str, dict[str, Any]]]]:
    """A batch processor whose hash lookup finds nothing and whose writes are captured."""
    captured: list[tuple[str, dict[str, Any]]] = []

    async def capture(cypher: str, **params: Any) -> AsyncMock:
        captured.append((cypher, params))
        return AsyncMock()

    mock_tx = AsyncMock()
    mock_tx.run = AsyncMock(side_effect=capture)

    async def execute_write(tx_func: Any) -> None:
        await tx_func(mock_tx)

    driver, session = create_async_session_mock()
    session.run = AsyncMock(return_value=create_async_result_mock([]))
    session.execute_write = AsyncMock(side_effect=execute_write)
    return Neo4jBatchProcessor(driver), captured


@pytest.mark.asyncio
async def test_batch_path_prunes_credits_before_merging_them() -> None:
    """Every reprocessed release takes part — one that now asserts no credits with an
    empty keep-list, which is the "every credit was removed" case."""
    processor, captured = _batch_tx_capture()
    messages = [
        PendingMessage(
            "releases",
            {"id": "R1", "title": "Kept", "sha256": "h1", "extraartists": [{"name": "Flood", "role": "Producer"}]},
            AsyncMock(),
            AsyncMock(),
        ),
        PendingMessage("releases", {"id": "R2", "title": "Stripped", "sha256": "h2"}, AsyncMock(), AsyncMock()),
    ]

    await processor._process_releases_batch(messages)

    cyphers = [cypher for cypher, _ in captured]
    prune_index = cyphers.index(PRUNE_CREDITED_ON_CYPHER)
    merge_index = next(i for i, cypher in enumerate(cyphers) if "MERGE (p)-[c:CREDITED_ON" in cypher)
    assert prune_index < merge_index
    assert captured[prune_index][1]["records"] == [
        {"key": "R1", "keep": [["Flood", "Producer"]]},
        {"key": "R2", "keep": []},
    ]


@pytest.mark.asyncio
async def test_single_record_path_prunes_a_release_that_lost_every_credit() -> None:
    from graphinator.entity_projection import process_release

    tx = MagicMock()
    tx.run = AsyncMock()
    tx.run.return_value.single = AsyncMock(return_value=None)

    await process_release(tx, {"id": "R9", "title": "Stripped", "sha256": "h9"})

    prune_calls = [call for call in tx.run.call_args_list if call.args[0] == PRUNE_CREDITED_ON_CYPHER]
    assert len(prune_calls) == 1
    assert prune_calls[0].kwargs["records"] == [{"key": "R9", "keep": []}]
    assert not [call for call in tx.run.call_args_list if "MERGE (p)-[c:CREDITED_ON" in call.args[0]]


# ── The maintenance step ─────────────────────────────────────────────────────


def _pressure_graph(fail_times: int) -> tuple[MagicMock, list[str]]:
    """Graph mock whose first ``fail_times`` runs raise MemoryPoolOutOfMemoryError."""
    run_calls: list[str] = []

    class _Result:
        async def consume(self) -> MagicMock:
            summary = MagicMock()
            summary.counters = "properties_set=8"
            return summary

    class _Session:
        async def __aenter__(self) -> _Session:
            return self

        async def __aexit__(self, *args: Any) -> None:
            return None

        async def run(self, cypher: str, *_args: Any, **_kwargs: Any) -> _Result:
            run_calls.append(cypher)
            if len(run_calls) <= fail_times:
                raise Neo4jError._hydrate_neo4j(
                    code="Neo.TransientError.General.MemoryPoolOutOfMemoryError",
                    message="using 16.0 GiB of 16.0 GiB dbms.memory.transaction.total.max",
                )
            return _Result()

    graph_mock = MagicMock()
    graph_mock.session = MagicMock(return_value=_Session())
    return graph_mock, run_calls


def _batch_sizes(run_calls: list[str]) -> list[int]:
    return [int(re.search(r"IN TRANSACTIONS OF (\d+) ROWS", c).group(1)) for c in run_calls]  # type: ignore[union-attr]


@pytest.fixture
def no_waits() -> Any:
    with (
        patch.object(g, "MAINTENANCE_SETTLE_SECONDS", 0.0),
        patch.object(g, "MAINTENANCE_RETRY_BASE_DELAY_SECONDS", 0.0),
    ):
        yield


@pytest.mark.asyncio
@pytest.mark.usefixtures("no_waits")
async def test_compute_runs_the_batched_recompute() -> None:
    graph_mock, run_calls = _pressure_graph(fail_times=0)
    with patch.object(g, "graph", graph_mock):
        assert await g.compute_person_credit_counts() is True

    assert len(run_calls) == 1
    assert run_calls[0] == build_credit_count_cypher(g.PERSON_CREDIT_COUNT_BATCH_SIZE)


@pytest.mark.asyncio
@pytest.mark.usefixtures("no_waits")
async def test_compute_retries_with_halved_batches_under_memory_pressure() -> None:
    graph_mock, run_calls = _pressure_graph(fail_times=2)
    with patch.object(g, "graph", graph_mock):
        assert await g.compute_person_credit_counts() is True

    size = g.PERSON_CREDIT_COUNT_BATCH_SIZE
    assert _batch_sizes(run_calls) == [size, size // 2, size // 4]


@pytest.mark.asyncio
@pytest.mark.usefixtures("no_waits")
async def test_compute_reports_failure_when_retries_exhaust() -> None:
    graph_mock, run_calls = _pressure_graph(fail_times=99)
    with patch.object(g, "graph", graph_mock):
        assert await g.compute_person_credit_counts() is False

    assert len(run_calls) == g.MAINTENANCE_MAX_ATTEMPTS
    assert all(size >= g.MAINTENANCE_MIN_BATCH_SIZE for size in _batch_sizes(run_calls))


@pytest.mark.asyncio
@pytest.mark.usefixtures("no_waits")
async def test_compute_reports_a_non_transient_error_without_retrying() -> None:
    session = AsyncMock()
    session.run = AsyncMock(side_effect=RuntimeError("syntax error"))
    session_ctx = AsyncMock()
    session_ctx.__aenter__ = AsyncMock(return_value=session)
    session_ctx.__aexit__ = AsyncMock(return_value=False)
    graph_mock = MagicMock()
    graph_mock.session = MagicMock(return_value=session_ctx)

    with patch.object(g, "graph", graph_mock), patch.object(g, "logger"):
        assert await g.compute_person_credit_counts() is False

    assert session.run.await_count == 1


@pytest.mark.asyncio
async def test_compute_is_a_no_op_without_a_driver() -> None:
    with patch.object(g, "graph", None):
        assert await g.compute_person_credit_counts() is True


@pytest.mark.asyncio
async def test_compute_lets_the_pool_settle_after_the_stats_sweep() -> None:
    graph_mock, _ = _pressure_graph(fail_times=0)
    sleeps: list[float] = []

    async def record_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    with (
        patch.object(g, "graph", graph_mock),
        patch.object(g, "MAINTENANCE_SETTLE_SECONDS", 7.5),
        patch.object(g.asyncio, "sleep", record_sleep),
    ):
        assert await g.compute_person_credit_counts() is True

    assert sleeps == [7.5]


@pytest.mark.asyncio
async def test_maintenance_recounts_credits_after_the_aggregate_stats() -> None:
    """The recount runs on the graph the import and cleanup left behind."""
    order: list[str] = []

    def step(name: str) -> AsyncMock:
        async def run() -> bool:
            order.append(name)
            return True

        return AsyncMock(side_effect=run)

    with (
        patch.object(g, "graph", AsyncMock()),
        patch.object(g, "cleanup_all_stub_nodes", step("cleanup")),
        patch.object(g, "cleanup_sentinel_master", step("sentinel")),
        patch.object(g, "compute_genre_style_stats", step("stats")),
        patch.object(g, "compute_person_credit_counts", step("credits")),
    ):
        assert await g.run_post_import_maintenance() is True

    assert order == ["cleanup", "sentinel", "stats", "credits"]


@pytest.mark.asyncio
async def test_failed_credit_recount_fails_the_pass_and_is_retried() -> None:
    """A failed recount must not be reported as complete maintenance — the detached task
    retries the whole idempotent pass, as it does for every other step."""
    credits = AsyncMock(return_value=False)
    with (
        patch.object(g, "graph", AsyncMock()),
        patch.object(g, "cleanup_all_stub_nodes", AsyncMock(return_value=True)),
        patch.object(g, "cleanup_sentinel_master", AsyncMock(return_value=True)),
        patch.object(g, "compute_genre_style_stats", AsyncMock(return_value=True)),
        patch.object(g, "compute_person_credit_counts", credits),
        patch.object(g, "POST_IMPORT_MAINTENANCE_RETRY_DELAY_SECONDS", 0.0),
    ):
        assert await g.run_post_import_maintenance() is False

    assert credits.await_count == g.POST_IMPORT_MAINTENANCE_MAX_ATTEMPTS


@pytest.mark.asyncio
async def test_credit_recount_is_skipped_when_the_stats_sweep_fails() -> None:
    credits = AsyncMock(return_value=True)
    with (
        patch.object(g, "graph", AsyncMock()),
        patch.object(g, "cleanup_all_stub_nodes", AsyncMock(return_value=True)),
        patch.object(g, "cleanup_sentinel_master", AsyncMock(return_value=True)),
        patch.object(g, "compute_genre_style_stats", AsyncMock(return_value=False)),
        patch.object(g, "compute_person_credit_counts", credits),
        patch.object(g, "POST_IMPORT_MAINTENANCE_RETRY_DELAY_SECONDS", 0.0),
    ):
        assert await g.run_post_import_maintenance() is False

    credits.assert_not_awaited()
