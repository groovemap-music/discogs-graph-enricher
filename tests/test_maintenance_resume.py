"""Resumable paged maintenance: bounded statements, checkpoints, resume, step failure.

On lux every post-import attempt died with TransactionTimedOutClientConfiguration because
the Genre/Style/Label stats and the Person credit counts were each ONE statement bounded
by the whole graph. These tests pin the replacement shape: many short keyed pages, a
checkpoint after each, resume without redoing a committed page, a smaller page after a
timeout, and a failed step that is named, does not skip its siblings, and withholds the
maintained_at stamp.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import graphinator.graphinator as g
from graphinator import maintenance_paging as mp
from graphinator.person_credits import build_credit_count_page_cypher
from tests.fake_paged_graph import FakePagedGraph, timeout_error


NAMES = [f"person-{i:04d}" for i in range(25)]


@pytest.fixture(autouse=True)
def _no_waits() -> Any:
    with (
        patch.object(g, "MAINTENANCE_SETTLE_SECONDS", 0.0),
        patch.object(mp, "PAGE_RETRY_BASE_DELAY_SECONDS", 0.0),
        patch.object(g, "PERSON_CREDIT_COUNT_BATCH_SIZE", 10),
        patch.object(g, "LABEL_STATS_PAGE_SIZE", 10),
    ):
        yield


def _run_key(key: str | None) -> Any:
    return patch.object(g, "_maintenance_run_key", _Var(key))


class _Var:
    def __init__(self, value: str | None) -> None:
        self.value = value

    def get(self) -> str | None:
        return self.value


# ── Shape: bounded pages ───────────────────────────────────────────────────────


def test_person_page_is_bounded_and_keyed() -> None:
    """The statement is a keyed page: cursor, ordering, LIMIT — never a whole-graph sweep."""
    cypher = build_credit_count_page_cypher()
    assert "WHERE p.name > $cursor" in cypher
    assert "ORDER BY p.name LIMIT $size" in cypher
    assert "IN TRANSACTIONS" not in cypher
    assert "RETURN max(p.name) AS last, count(p) AS processed" in cypher


def test_label_page_is_bounded_and_keyed() -> None:
    cypher = g.build_label_stats_page_cypher()
    assert "WHERE l.id > $cursor" in cypher
    assert "ORDER BY l.id LIMIT $size" in cypher
    assert "IN TRANSACTIONS" not in cypher


def test_genre_style_metrics_are_grouped_by_what_they_share() -> None:
    """The three counts that walk the same release chain share one statement; the others stay apart."""
    for node in ("Genre", "Style"):
        other = "style_count" if node == "Genre" else "genre_count"
        statements = g.build_node_stat_statements(node)
        sets = [cypher.split("SET ")[1].strip() for cypher, _ in statements]
        assert sets == [
            "n.release_count = v0",
            f"n.artist_count = v0, n.label_count = v1, n.{other} = v2",
            "n.first_year = v0",
        ]
        # release_count needs no DISTINCT set: the IS edge is MERGEd, one per release/node pair.
        assert "count(r) AS v0" in statements[0][0]
        assert "DISTINCT" not in statements[0][0]


@pytest.mark.asyncio
async def test_person_sweep_covers_everyone_in_bounded_pages() -> None:
    graph = FakePagedGraph(persons=NAMES)
    with patch.object(g, "graph", graph), _run_key("2026-11"):
        assert await g.compute_person_credit_counts() is True

    assert [len(p) for p in graph.person_pages] == [10, 10, 5, 0]
    assert [n for page in graph.person_pages for n in page] == NAMES
    assert max(graph.page_sizes) <= 10
    assert graph.checkpoints["person_credit_counts"]["done"] is True


# ── Resume ─────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_interrupted_person_sweep_resumes_without_redoing_committed_pages() -> None:
    graph = FakePagedGraph(persons=NAMES)
    calls = {"n": 0}

    def crash_on_third_page(_kind: str, _params: dict[str, Any]) -> Exception | None:
        calls["n"] += 1
        return RuntimeError("connection lost") if calls["n"] == 3 else None

    graph.fail = crash_on_third_page
    with patch.object(g, "graph", graph), _run_key("2026-11"), patch.object(g, "logger"):
        assert await g.compute_person_credit_counts() is False
        assert [n for page in graph.person_pages for n in page] == NAMES[:20]
        assert graph.checkpoints["person_credit_counts"]["cursor"] == NAMES[19]

        graph.fail = None
        assert await g.compute_person_credit_counts() is True

    seen = [n for page in graph.person_pages for n in page]
    assert seen == NAMES, "every person processed exactly once across the interrupted and resumed runs"


@pytest.mark.asyncio
async def test_resume_survives_a_restart_because_the_cursor_lives_in_the_graph() -> None:
    graph = FakePagedGraph(persons=NAMES)
    graph.checkpoints["person_credit_counts"] = {"run_key": "2026-11", "cursor": NAMES[14], "size": 10, "done": False}
    with patch.object(g, "graph", graph), _run_key("2026-11"):
        assert await g.compute_person_credit_counts() is True

    assert [n for page in graph.person_pages for n in page] == NAMES[15:]


@pytest.mark.asyncio
async def test_a_finished_step_is_skipped_on_the_next_pass() -> None:
    graph = FakePagedGraph(persons=NAMES)
    with patch.object(g, "graph", graph), _run_key("2026-11"):
        assert await g.compute_person_credit_counts() is True
        pages_after_first = len(graph.person_pages)
        assert await g.compute_person_credit_counts() is True

    assert len(graph.person_pages) == pages_after_first


@pytest.mark.asyncio
async def test_a_new_dump_version_recomputes_from_the_start() -> None:
    graph = FakePagedGraph(persons=NAMES)
    graph.checkpoints["person_credit_counts"] = {"run_key": "2026-10", "cursor": NAMES[-1], "size": 10, "done": True}
    with patch.object(g, "graph", graph), _run_key("2026-11"):
        assert await g.compute_person_credit_counts() is True

    assert [n for page in graph.person_pages for n in page] == NAMES


@pytest.mark.asyncio
async def test_label_stats_resume_after_a_failure_in_the_label_sweep() -> None:
    graph = FakePagedGraph(labels=[f"{i:04d}" for i in range(30)], genres=["Rock"], styles=["Punk"])
    state = {"fail": True}

    def fail_second_label_page(kind: str, _params: dict[str, Any]) -> Exception | None:
        if kind == "label" and state["fail"] and len(graph.label_pages) == 1:
            return RuntimeError("boom")
        return None

    graph.fail = fail_second_label_page
    with patch.object(g, "graph", graph), _run_key("2026-11"), patch.object(g, "logger"):
        assert await g.compute_genre_style_stats() is False
        state["fail"] = False
        assert await g.compute_genre_style_stats() is True

    assert [n for page in graph.label_pages for n in page] == [f"{i:04d}" for i in range(30)]
    # Genre and Style were finished by the first pass and not redone by the second.
    assert [k for k, p in graph.metric_sets if p == "release_count"] == ["Rock", "Punk"]


# ── Timeout: smaller page, not the same statement again ───────────────────────


@pytest.mark.asyncio
async def test_timeout_halves_the_page_and_the_size_is_remembered() -> None:
    graph = FakePagedGraph(persons=NAMES)

    def time_out_big_pages(_kind: str, params: dict[str, Any]) -> Exception | None:
        return timeout_error() if params["size"] > 3 else None

    graph.fail = time_out_big_pages
    with patch.object(g, "graph", graph), _run_key("2026-11"), patch.object(g, "logger"):
        assert await g.compute_person_credit_counts() is True

    assert graph.page_sizes[:3] == [10, 5, 2]
    assert [n for page in graph.person_pages for n in page] == NAMES
    assert graph.checkpoints["person_credit_counts"]["size"] == 2


@pytest.mark.asyncio
async def test_a_retry_does_not_repeat_the_statement_that_timed_out() -> None:
    graph = FakePagedGraph(persons=NAMES)
    graph.fail = lambda _kind, params: timeout_error() if params["size"] > 3 else None
    with patch.object(g, "graph", graph), _run_key("2026-11"), patch.object(g, "logger"):
        await g.compute_person_credit_counts()
        sizes_first = list(graph.page_sizes)
        graph.page_sizes.clear()
        graph.checkpoints["person_credit_counts"].update(done=False, cursor="")
        await g.compute_person_credit_counts()

    assert max(graph.page_sizes) <= 2, f"resumed at the remembered size, not {sizes_first[0]} again"


@pytest.mark.asyncio
async def test_a_step_that_cannot_progress_even_at_page_size_one_fails() -> None:
    graph = FakePagedGraph(persons=NAMES)
    graph.fail = lambda _kind, _params: timeout_error()
    with patch.object(g, "graph", graph), _run_key("2026-11"), patch.object(g, "logger"):
        assert await g.compute_person_credit_counts() is False

    assert graph.page_sizes[-3:] == [1, 1, 1]


@pytest.mark.asyncio
async def test_backstop_timeout_is_off_by_default_and_set_by_env(monkeypatch: pytest.MonkeyPatch) -> None:
    graph = FakePagedGraph(persons=NAMES[:3])
    monkeypatch.delenv(mp.MAINTENANCE_TX_TIMEOUT_ENV, raising=False)
    with patch.object(g, "graph", graph), _run_key("k"):
        await g.compute_person_credit_counts()
    assert set(graph.timeouts_seen) == {None}

    graph = FakePagedGraph(persons=NAMES[:3])
    monkeypatch.setenv(mp.MAINTENANCE_TX_TIMEOUT_ENV, "900")
    with patch.object(g, "graph", graph), _run_key("k"):
        await g.compute_person_credit_counts()
    assert set(graph.timeouts_seen) == {900.0}


@pytest.mark.asyncio
async def test_unversioned_direct_call_does_not_persist_checkpoints() -> None:
    graph = FakePagedGraph(persons=NAMES)
    with patch.object(g, "graph", graph):
        assert await g.compute_person_credit_counts() is True
    assert graph.checkpoints == {}


# ── Genre / Style ──────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_genre_and_style_stats_run_every_metric_for_every_node() -> None:
    graph = FakePagedGraph(genres=["Jazz", "Rock"], styles=["Punk"], labels=["1"])
    with patch.object(g, "graph", graph), _run_key("2026-11"):
        assert await g.compute_genre_style_stats() is True

    assert sorted({k for k, _ in graph.metric_sets}) == ["Jazz", "Punk", "Rock"]
    assert sorted(p for k, p in graph.metric_sets if k == "Rock") == sorted(
        ["release_count", "artist_count", "label_count", "style_count", "first_year"]
    )
    assert set(graph.checkpoints) == {"genre_stats", "style_stats", "label_stats"}


# ── Step failure: named, siblings still run, stamp withheld ───────────────────


def _step(ok: bool = True) -> AsyncMock:
    return AsyncMock(return_value=ok)


@pytest.mark.asyncio
async def test_failed_step_is_named_does_not_skip_siblings_and_leaves_stamp_unset() -> None:
    stats = _step(False)
    credits = _step(True)
    stamp = AsyncMock()
    record_failure = AsyncMock()
    log = MagicMock()
    with (
        patch.object(g, "graph", AsyncMock()),
        patch.object(g, "batch_processor", None),
        patch.object(g, "cleanup_all_stub_nodes", _step()),
        patch.object(g, "cleanup_sentinel_master", _step()),
        patch.object(g, "compute_genre_style_stats", stats),
        patch.object(g, "compute_person_credit_counts", credits),
        patch.object(g, "_stamp_maintenance_success", stamp),
        patch.object(g, "_record_maintenance_failure", record_failure),
        patch.object(g, "POST_IMPORT_MAINTENANCE_RETRY_DELAY_SECONDS", 0.0),
        patch.object(g, "logger") as logger,
    ):
        logger.error = log
        assert await g.run_post_import_maintenance("2026-11") is False

    assert credits.await_count == g.POST_IMPORT_MAINTENANCE_MAX_ATTEMPTS, "the later step still ran after the failure"
    stamp.assert_not_awaited()
    steps_logged = [call.kwargs.get("step") for call in log.call_args_list]
    assert "genre_style_label_stats" in steps_logged
    assert "person_credit_counts" not in steps_logged
    assert "genre_style_label_stats" in record_failure.await_args.args[1]


@pytest.mark.asyncio
async def test_a_raising_step_is_named_and_the_stamp_is_withheld() -> None:
    stamp = AsyncMock()
    with (
        patch.object(g, "graph", AsyncMock()),
        patch.object(g, "batch_processor", None),
        patch.object(g, "cleanup_all_stub_nodes", AsyncMock(side_effect=RuntimeError("kaput"))),
        patch.object(g, "cleanup_sentinel_master", _step()),
        patch.object(g, "compute_genre_style_stats", _step()),
        patch.object(g, "compute_person_credit_counts", _step()),
        patch.object(g, "_stamp_maintenance_success", stamp),
        patch.object(g, "_record_maintenance_failure", AsyncMock()) as record_failure,
        patch.object(g, "POST_IMPORT_MAINTENANCE_RETRY_DELAY_SECONDS", 0.0),
        patch.object(g, "logger"),
    ):
        assert await g.run_post_import_maintenance("2026-11") is False

    stamp.assert_not_awaited()
    assert "stub_cleanup" in record_failure.await_args.args[1]


@pytest.mark.asyncio
async def test_stamp_is_written_when_every_step_succeeds() -> None:
    stamp = AsyncMock()
    with (
        patch.object(g, "graph", AsyncMock()),
        patch.object(g, "batch_processor", None),
        patch.object(g, "cleanup_all_stub_nodes", _step()),
        patch.object(g, "cleanup_sentinel_master", _step()),
        patch.object(g, "compute_genre_style_stats", _step()),
        patch.object(g, "compute_person_credit_counts", _step()),
        patch.object(g, "_stamp_maintenance_success", stamp),
    ):
        assert await g.run_post_import_maintenance("2026-11") is True

    stamp.assert_awaited_once_with("2026-11")


@pytest.mark.asyncio
async def test_retry_after_a_failed_step_resumes_instead_of_restarting() -> None:
    """End to end through the real sweeps: pass 1 dies mid-Person-sweep, pass 2 finishes it."""
    graph = FakePagedGraph(persons=NAMES, labels=["1", "2"], genres=["Rock"], styles=["Punk"])
    calls = {"n": 0}

    def crash_once_on_second_person_page(kind: str, _params: dict[str, Any]) -> Exception | None:
        if kind == "person":
            calls["n"] += 1
            if calls["n"] == 2:
                return RuntimeError("connection lost")
        return None

    graph.fail = crash_once_on_second_person_page
    stamp = AsyncMock()
    with (
        patch.object(g, "graph", graph),
        patch.object(g, "batch_processor", None),
        patch.object(g, "cleanup_all_stub_nodes", _step()),
        patch.object(g, "cleanup_sentinel_master", _step()),
        patch.object(g, "_stamp_maintenance_success", stamp),
        patch.object(g, "POST_IMPORT_MAINTENANCE_RETRY_DELAY_SECONDS", 0.0),
        patch.object(g, "logger"),
    ):
        assert await g.run_post_import_maintenance("2026-11") is True

    assert [n for page in graph.person_pages for n in page] == NAMES, "no person redone, none missed"
    assert [k for k, p in graph.metric_sets if p == "release_count"] == ["Rock", "Punk"], "stats were not recomputed on the retry"
    stamp.assert_awaited_once_with("2026-11")


@pytest.mark.asyncio
async def test_non_retryable_error_fails_the_step_at_once_without_halving_or_backoff() -> None:
    """A syntax error or lost connection is not a page-size problem: no halving, no sleep."""
    graph = FakePagedGraph(persons=NAMES)
    graph.fail = lambda _kind, _params: RuntimeError("syntax error")
    sleeps: list[float] = []

    async def record_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    with (
        patch.object(g, "graph", graph),
        _run_key("2026-11"),
        patch.object(g, "logger"),
        patch.object(mp.asyncio, "sleep", record_sleep),
    ):
        assert await g.compute_person_credit_counts() is False

    assert graph.page_sizes == [10], "exactly one attempt at the starting size"
    assert [d for d in sleeps if d > 0] == [], "no retry backoff"
