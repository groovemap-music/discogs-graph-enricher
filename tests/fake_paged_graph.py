"""An in-memory stand-in for the Neo4j driver that understands the paged maintenance statements.

It recognises each statement by its shape (checkpoint read/write, keyed Person/Label page,
Genre/Style key listing and per-metric SET) so tests can assert on exactly what a sweep
committed, in what order, and what a retry did or did not redo.
"""

from __future__ import annotations

import re
from typing import Any
from unittest.mock import MagicMock

from neo4j.exceptions import Neo4jError


def timeout_error() -> Neo4jError:
    return Neo4jError._hydrate_neo4j(
        code="Neo.ClientError.Transaction.TransactionTimedOutClientConfiguration",
        message="The transaction has been terminated: timeout",
    )


class _Result:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    async def single(self) -> dict[str, Any] | None:
        return self._rows[0] if self._rows else None

    def __aiter__(self) -> Any:
        async def gen() -> Any:
            for row in self._rows:
                yield row

        return gen()

    async def consume(self) -> MagicMock:
        return MagicMock()


class FakePagedGraph:
    """Records every page and checkpoint; ``fail`` lets a test raise on chosen statements."""

    def __init__(
        self, persons: list[str] | None = None, labels: list[str] | None = None, genres: list[str] | None = None, styles: list[str] | None = None
    ):
        self.persons = sorted(persons or [])
        self.labels = sorted(labels or [])
        self.genres = sorted(genres or [])
        self.styles = sorted(styles or [])
        self.checkpoints: dict[str, dict[str, Any]] = {}
        self.person_pages: list[list[str]] = []
        self.label_pages: list[list[str]] = []
        self.metric_sets: list[tuple[str, str]] = []  # (key, property)
        self.page_sizes: list[int] = []
        self.statements: list[str] = []
        self.fail: Any = None  # callable(kind, params) -> exception | None
        self.timeouts_seen: list[Any] = []

    def session(self, **_kwargs: Any) -> FakePagedGraph:
        return self

    async def __aenter__(self) -> FakePagedGraph:
        return self

    async def __aexit__(self, *_args: Any) -> None:
        return None

    def _maybe_fail(self, kind: str, params: dict[str, Any]) -> None:
        if self.fail is not None and (error := self.fail(kind, params)) is not None:
            raise error

    async def run(self, query: Any, **params: Any) -> _Result:
        cypher = getattr(query, "text", query)
        self.timeouts_seen.append(getattr(query, "timeout", None))
        self.statements.append(cypher)
        if "OPTIONAL MATCH (c:MaintenanceCheckpoint" in cypher:
            stored = self.checkpoints.get(params["step"])
            if stored is None:
                return _Result([{"run_key": None, "cursor": None, "size": None, "done": None}])
            return _Result([stored])
        if "MERGE (c:MaintenanceCheckpoint" in cypher:
            self.checkpoints[params["step"]] = {
                "run_key": params["run_key"],
                "cursor": params["cursor"],
                "size": params["size"],
                "done": params["done"],
            }
            return _Result([{"written": 1}])
        if "MATCH (p:Person)" in cypher:
            return self._page("person", self.persons, self.person_pages, params)
        if "MATCH (l:Label)" in cypher:
            return self._page("label", self.labels, self.label_pages, params)
        if "RETURN n.name AS key" in cypher:
            self._maybe_fail("keys", params)
            source = self.genres if "(n:Genre)" in cypher else self.styles
            keys = [k for k in source if k > params["cursor"]][: params["size"]]
            return _Result([{"key": k} for k in keys])
        if "SET n." in cypher:
            self._maybe_fail("metric", params)
            # A statement may set several properties: record each one.
            for prop in re.findall(r"n\.(\w+) = v\d", cypher.split("SET ", 1)[1]):
                self.metric_sets.append((params["key"], prop))
            return _Result([])
        raise AssertionError(f"unexpected statement: {cypher}")

    def _page(self, kind: str, keys: list[str], pages: list[list[str]], params: dict[str, Any]) -> _Result:
        self.page_sizes.append(params["size"])
        self._maybe_fail(kind, params)
        page = [k for k in keys if k > params["cursor"]][: params["size"]]
        pages.append(page)
        return _Result([{"last": page[-1] if page else None, "processed": len(page)}])
