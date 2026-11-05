"""Real-engine checks for the Cypher write paths mocks cannot validate."""

from __future__ import annotations

import asyncio
import os
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from common.credit_roles import ALL_CATEGORIES
from neo4j import AsyncDriver, AsyncGraphDatabase
from neo4j.exceptions import ServiceUnavailable

import graphinator.graphinator as graphinator
from graphinator.batch_processor import BatchConfig, Neo4jBatchProcessor, PendingMessage
from graphinator.entity_projection import process_artist, process_label, process_release
from graphinator.person_credits import CREDIT_COUNT_PROPERTIES, build_credit_count_page_cypher


pytestmark = pytest.mark.integration

# Preserve real helpers before the default suite fixture substitutes an in-memory latch.
_LOAD_EXTRACTION_SIGNALS = graphinator._load_extraction_signals
_PERSIST_EXTRACTION_SIGNALS = graphinator._persist_extraction_signals


def pending(data_type: str, data: dict[str, Any]) -> PendingMessage:
    """Build one batch message; settlement belongs to the queue-level tests."""
    return PendingMessage(data_type, data, AsyncMock(), AsyncMock())


async def enqueue(processor: Neo4jBatchProcessor, message: PendingMessage) -> None:
    await processor._engine.submit(message.data_type, message, message)


@pytest_asyncio.fixture
async def neo4j_driver() -> AsyncDriver:
    """Connect to the disposable Neo4j started by ``just test-integration``."""
    driver = AsyncGraphDatabase.driver(
        os.environ["NEO4J_URI"],
        auth=(os.environ.get("NEO4J_INTEGRATION_USER", "neo4j"), os.environ["NEO4J_INTEGRATION_PASSWORD"]),
    )
    await driver.verify_connectivity()
    try:
        yield driver
    finally:
        async with driver.session(database="neo4j") as session:
            await session.run("MATCH (node) DETACH DELETE node")
        await driver.close()


async def one(driver: AsyncDriver, query: str, **parameters: Any) -> dict[str, Any]:
    """Run a read assertion and return its one record as a plain mapping."""
    async with driver.session(database="neo4j") as session:
        result = await session.run(query, **parameters)
        record = await result.single(strict=True)
        return dict(record)


@pytest.mark.asyncio
async def test_concurrent_first_signals_are_durable_at_ack(neo4j_driver: AsyncDriver, monkeypatch: pytest.MonkeyPatch) -> None:
    """Use real load/commit awaits through the four consumer completion handlers."""
    monkeypatch.setattr(graphinator, "graph", neo4j_driver)
    monkeypatch.setattr(graphinator, "batch_processor", None)
    monkeypatch.setattr(graphinator, "_load_extraction_signals", _LOAD_EXTRACTION_SIGNALS)
    monkeypatch.setattr(graphinator, "_persist_extraction_signals", _PERSIST_EXTRACTION_SIGNALS)
    acknowledged: set[str] = set()
    ack_lock = asyncio.Lock()
    messages = []
    for data_type in graphinator.DATA_TYPES:
        message = AsyncMock()

        async def ack(data_type: str = data_type) -> None:
            async with ack_lock:
                acknowledged.add(data_type)
                stored = await _LOAD_EXTRACTION_SIGNALS("concurrent-first")
                assert stored.issuperset(acknowledged)

        message.ack.side_effect = ack
        messages.append((data_type, message))

    with patch.object(graphinator, "start_post_import_maintenance") as start:
        await asyncio.gather(
            *(
                graphinator.check_file_completion({"type": "extraction_complete", "version": "concurrent-first"}, data_type, message)
                for data_type, message in messages
            )
        )
    assert await _LOAD_EXTRACTION_SIGNALS("concurrent-first") == set(graphinator.DATA_TYPES)
    assert all(message.ack.await_count == 1 and message.nack.await_count == 0 for _, message in messages)
    start.assert_called_once()


@pytest.mark.asyncio
async def test_overlapping_latch_writes_union_existing_signals(neo4j_driver: AsyncDriver, monkeypatch: pytest.MonkeyPatch) -> None:
    """Bypass the process lock: overlapping stale snapshots must remain monotonic in Neo4j."""
    monkeypatch.setattr(graphinator, "graph", neo4j_driver)
    await _PERSIST_EXTRACTION_SIGNALS("overlapping-writes", {"already-committed"})
    # Each write intentionally knows only its own type, never the committed seed
    # or other writers' signals. Repetition also checks duplicate idempotency.
    await asyncio.gather(*(_PERSIST_EXTRACTION_SIGNALS("overlapping-writes", {data_type}) for _ in range(8) for data_type in graphinator.DATA_TYPES))
    stored = await one(
        neo4j_driver,
        "MATCH (m:ExtractionCompletion {version: $version}) RETURN count(m) AS nodes, head(collect(m.signals)) AS signals",
        version="overlapping-writes",
    )
    expected = {"already-committed", *graphinator.DATA_TYPES}
    assert stored["nodes"] == 1
    assert set(stored["signals"]) == expected
    assert len(stored["signals"]) == len(expected)


@pytest.mark.asyncio
async def test_historical_duplicate_version_nodes_restore_every_committed_signal(neo4j_driver: AsyncDriver, monkeypatch: pytest.MonkeyPatch) -> None:
    """Recover old first-MERGE races without DDL, deletion, or cross-version leakage."""
    monkeypatch.setattr(graphinator, "graph", neo4j_driver)
    monkeypatch.setattr(graphinator, "batch_processor", None)
    monkeypatch.setattr(graphinator, "_load_extraction_signals", _LOAD_EXTRACTION_SIGNALS)
    monkeypatch.setattr(graphinator, "_persist_extraction_signals", _PERSIST_EXTRACTION_SIGNALS)
    await one(
        neo4j_driver,
        """
        CREATE (:ExtractionCompletion {version: 'historical', signals: ['artists', 'labels']}),
               (:ExtractionCompletion {version: 'historical', signals: ['masters']}),
               (:ExtractionCompletion {version: 'other-version', signals: ['releases']})
        RETURN 1 AS seeded
        """,
    )
    assert await _LOAD_EXTRACTION_SIGNALS("historical") == {"artists", "labels", "masters"}
    assert await _LOAD_EXTRACTION_SIGNALS("other-version") == {"releases"}
    assert await _LOAD_EXTRACTION_SIGNALS("missing-version") == set()
    message = AsyncMock()

    async def ack() -> None:
        assert await _LOAD_EXTRACTION_SIGNALS("historical") == set(graphinator.DATA_TYPES)

    message.ack.side_effect = ack
    with patch.object(graphinator, "start_post_import_maintenance") as start:
        await graphinator.check_file_completion({"type": "extraction_complete", "version": "historical"}, "releases", message)
    message.ack.assert_awaited_once()
    message.nack.assert_not_awaited()
    start.assert_called_once()
    stored = await one(
        neo4j_driver,
        "MATCH (m:ExtractionCompletion {version: 'historical'}) RETURN collect(m.signals) AS rows",
    )
    assert len(stored["rows"]) == 2
    assert all(set(row) == set(graphinator.DATA_TYPES) for row in stored["rows"])
    assert await _LOAD_EXTRACTION_SIGNALS("other-version") == {"releases"}


@pytest.mark.asyncio
async def test_artist_batch_writes_nodes_and_membership_edges(neo4j_driver: AsyncDriver) -> None:
    processor = Neo4jBatchProcessor(neo4j_driver)
    message = pending(
        "artists",
        {
            "id": "artist-main",
            "name": "Main Artist",
            "sha256": "artist-hash",
            "members": [{"id": "artist-member"}],
            "groups": [{"id": "artist-group"}],
            "aliases": [{"id": "artist-alias"}],
        },
    )
    await enqueue(processor, message)
    assert await processor.flush_queue("artists") is True
    message.ack_callback.assert_awaited_once()
    message.nack_callback.assert_not_awaited()
    assert await one(
        neo4j_driver,
        """
        MATCH (artist:Artist {id: $id})
        OPTIONAL MATCH (:Artist {id: 'artist-member'})-[member:MEMBER_OF]->(artist)
        OPTIONAL MATCH (artist)-[group:MEMBER_OF]->(:Artist {id: 'artist-group'})
        OPTIONAL MATCH (:Artist {id: 'artist-alias'})-[alias:ALIAS_OF]->(artist)
        RETURN artist.name AS name, count(DISTINCT member) AS members,
               count(DISTINCT group) AS groups, count(DISTINCT alias) AS aliases
        """,
        id="artist-main",
    ) == {"name": "Main Artist", "members": 1, "groups": 1, "aliases": 1}


@pytest.mark.asyncio
async def test_release_batch_writes_representative_catalog_edges(neo4j_driver: AsyncDriver) -> None:
    processor = Neo4jBatchProcessor(neo4j_driver)
    message = pending(
        "releases",
        {
            "id": "release-one",
            "title": "Release One",
            "year": 2001,
            "sha256": "release-hash",
            "artists": [{"id": "artist-one"}],
            "labels": [{"id": "label-one"}],
            "master_id": "master-one",
            "genres": ["Electronic"],
            "styles": ["House"],
            "formats": [],
        },
    )
    await enqueue(processor, message)
    assert await processor.flush_queue("releases") is True
    message.ack_callback.assert_awaited_once()
    assert await one(
        neo4j_driver,
        """
        MATCH (release:Release {id: $id})
        OPTIONAL MATCH (release)-[by:BY]->(:Artist {id: 'artist-one'})
        OPTIONAL MATCH (release)-[on:ON]->(:Label {id: 'label-one'})
        OPTIONAL MATCH (release)-[derived:DERIVED_FROM]->(:Master {id: 'master-one'})
        OPTIONAL MATCH (release)-[genre:IS]->(:Genre {name: 'Electronic'})
        OPTIONAL MATCH (release)-[style:IS]->(:Style {name: 'House'})
        RETURN release.title AS title, count(DISTINCT by) AS artists,
               count(DISTINCT on) AS labels, count(DISTINCT derived) AS masters,
               count(DISTINCT genre) AS genres, count(DISTINCT style) AS styles
        """,
        id="release-one",
    ) == {"title": "Release One", "artists": 1, "labels": 1, "masters": 1, "genres": 1, "styles": 1}


def prune_release(version: str, artists: list[str], labels: list[str], master: str, genres: list[str], styles: list[str]) -> PendingMessage:
    return pending(
        "releases",
        {
            "id": "P1",
            "title": "P1",
            "sha256": version,
            "artists": [{"id": a} for a in artists],
            "labels": [{"id": x} for x in labels],
            "master_id": master,
            "genres": genres,
            "styles": styles,
            "formats": [],
        },
    )


@pytest.mark.asyncio
async def test_one_prune_statement_removes_exactly_the_edges_a_reimport_drops(neo4j_driver: AsyncDriver) -> None:
    """BY, ON, DERIVED_FROM and IS (Genre and Style apart) are pruned together, each by its own keep-list."""
    processor = Neo4jBatchProcessor(neo4j_driver)
    await import_releases(
        processor,
        pending("releases", {"id": "BYSTANDER", "title": "B", "sha256": "b", "artists": [{"id": "A2"}], "genres": ["Rock"], "formats": []}),
        prune_release("v1", ["A1", "A2"], ["L1", "L2"], "M1", ["Rock", "Jazz"], ["Punk", "Bebop"]),
    )
    await import_releases(processor, prune_release("v2", ["A1"], ["L2"], "M2", ["Jazz"], ["Bebop"]))

    edges = await one(
        neo4j_driver,
        """
        MATCH (:Release {id: 'P1'})-[e:BY|ON|DERIVED_FROM|IS]->(t)
        RETURN collect(type(e) + ':' + coalesce(t.id, t.name)) AS edges
        """,
    )
    assert sorted(edges["edges"]) == ["BY:A1", "DERIVED_FROM:M2", "IS:Bebop", "IS:Jazz", "ON:L2"]
    # A release outside the batch is untouched, and the shared nodes survive the pruned edges.
    bystander = await one(neo4j_driver, "MATCH (:Release {id: 'BYSTANDER'})-[e:BY|IS]->() RETURN count(e) AS edges")
    assert bystander == {"edges": 2}
    assert (await one(neo4j_driver, "MATCH (n) WHERE n:Artist OR n:Label OR n:Genre OR n:Style RETURN count(n) AS nodes"))["nodes"] == 2 + 2 + 2 + 2

    # Dropping everything leaves no edge of those types.
    await import_releases(processor, prune_release("v3", [], [], "0", [], []))
    assert await one(neo4j_driver, "MATCH (:Release {id: 'P1'})-[e:BY|ON|DERIVED_FROM|IS]->() RETURN count(e) AS edges") == {"edges": 0}


@pytest.mark.asyncio
async def test_repeated_genres_and_styles_in_one_batch_project_one_node_and_edge_each(neo4j_driver: AsyncDriver) -> None:
    """Genre/Style/PART_OF are merged once per distinct value, with every release edge kept."""
    messages = [
        pending(
            "releases",
            {
                "id": f"dup-{n}",
                "title": f"Dup {n}",
                "sha256": f"dup-{n}",
                "genres": genres,
                "styles": styles,
                "formats": [],
            },
        )
        for n, (genres, styles) in enumerate(
            [(["Rock"], ["Punk", "Garage"]), (["Rock"], ["Punk"]), (["Rock", "Pop"], ["Punk"]), (["Jazz"], [])],
        )
    ]
    processor = Neo4jBatchProcessor(neo4j_driver)
    await import_releases(processor, *messages)
    # A second import of the same data must leave the graph unchanged.
    await import_releases(processor, *[pending("releases", {**m.data, "sha256": m.data["sha256"] + "-v2"}) for m in messages])

    shape = await one(
        neo4j_driver,
        """
        MATCH (g:Genre) WITH count(g) AS genres
        MATCH (s:Style) WITH genres, count(s) AS styles
        MATCH (:Release)-[i:IS]->() WITH genres, styles, count(i) AS is_edges
        MATCH (:Style)-[p:PART_OF]->(:Genre) RETURN genres, styles, is_edges, count(p) AS part_of
        """,
    )
    assert shape == {"genres": 3, "styles": 2, "is_edges": 9, "part_of": 2}


@pytest.mark.asyncio
async def test_multi_genre_release_does_not_create_cartesian_part_of_edges(neo4j_driver: AsyncDriver) -> None:
    """Genre/style co-occurrence on a multi-genre release must not be persisted as taxonomy."""
    processor = Neo4jBatchProcessor(neo4j_driver)
    messages = [
        pending(
            "releases",
            {
                "id": "release-multi-genre",
                "title": "Ambiguous Taxonomy",
                "sha256": "multi-hash",
                "genres": ["Electronic", "Rock"],
                "styles": ["House"],
                "formats": [],
            },
        ),
        pending(
            "releases",
            {
                "id": "release-single-genre",
                "title": "Unambiguous Taxonomy",
                "sha256": "single-hash",
                "genres": ["Electronic"],
                "styles": ["Techno"],
                "formats": [],
            },
        ),
    ]
    for message in messages:
        await enqueue(processor, message)
    assert await processor.flush_queue("releases") is True
    assert all(message.ack_callback.await_count == 1 for message in messages)
    assert await one(
        neo4j_driver,
        """
        MATCH (style:Style)-[:PART_OF]->(genre:Genre)
        RETURN collect(DISTINCT [style.name, genre.name]) AS pairs
        """,
    ) == {"pairs": [["Techno", "Electronic"]]}


def companies(*items: tuple[str, int | None, str, str]) -> dict[str, Any]:
    """Build a canonical companies block from (name, discogs id, role, category) tuples."""
    return {
        "companies_version": "1",
        "items": [
            {
                "name": name,
                "discogs_id": discogs_id,
                "role": role,
                "role_category": category,
                "catno": None,
                "source": {"provider": "discogs", "entity_type": None},
            }
            for name, discogs_id, role, category in items
        ],
        "role_categories": sorted({category for *_, category in items}),
        "unmapped": {"roles": []},
    }


@pytest.mark.asyncio
async def test_company_credits_survive_correction_by_company_and_role_pair(neo4j_driver: AsyncDriver) -> None:
    """The prune's `[co.id, e.role] IN record.keep` is list-pair Cypher a mock cannot check.

    The corrected record keeps Damont as its distributor and drops it as its plant, so the
    prune has to delete exactly one of the two edges to that one node. It also drops the
    cutting room entirely, and it names a plant Discogs gives no id — the derived
    `name:`-keyed node the uniqueness constraint has to accept.
    """
    processor = Neo4jBatchProcessor(neo4j_driver)
    original = pending(
        "releases",
        {
            "id": "release-credits",
            "title": "Pressed Somewhere",
            "sha256": "credits-hash-1",
            "country": "UK",
            "formats": [],
            "companies": companies(
                ("Damont", 12345, "Pressed By", "pressing"),
                ("Damont", 12345, "Distributed By", "distribution"),
                ("Utopia Studios", 266218, "Lacquer Cut At", "lacquer"),
            ),
        },
    )
    await enqueue(processor, original)
    assert await processor.flush_queue("releases") is True

    # `collect` has no order guarantee, so every credit assertion below sorts its rows.
    written = await one(
        neo4j_driver,
        """
        MATCH (:Release {id: $id})-[e:CREDITED_TO]->(co:Company)
        RETURN collect([co.id, co.name, e.role, e.role_category, e.source]) AS credits
        """,
        id="release-credits",
    )
    assert sorted(written["credits"]) == [
        ["12345", "Damont", "Distributed By", "distribution", "discogs"],
        ["12345", "Damont", "Pressed By", "pressing", "discogs"],
        ["266218", "Utopia Studios", "Lacquer Cut At", "lacquer", "discogs"],
    ]

    corrected = pending(
        "releases",
        {
            "id": "release-credits",
            "title": "Pressed Somewhere",
            "sha256": "credits-hash-2",
            "country": "US",
            "formats": [],
            "companies": companies(
                ("Damont", 12345, "Distributed By", "distribution"),
                ("Sound Performance", None, "Pressed By", "pressing"),
            ),
        },
    )
    await enqueue(processor, corrected)
    assert await processor.flush_queue("releases") is True

    surviving = await one(
        neo4j_driver,
        """
        MATCH (release:Release {id: $id})
        OPTIONAL MATCH (release)-[e:CREDITED_TO]->(co:Company)
        RETURN release.country AS country, collect([co.id, e.role]) AS credits
        """,
        id="release-credits",
    )
    assert surviving["country"] == "US"
    assert sorted(surviving["credits"]) == [["12345", "Distributed By"], ["name:sound performance", "Pressed By"]]


@pytest.mark.asyncio
async def test_a_release_with_no_companies_block_keeps_the_credits_already_written(neo4j_driver: AsyncDriver) -> None:
    """A pre-cutover record is silent about credits, so neither statement runs for it."""
    processor = Neo4jBatchProcessor(neo4j_driver)
    await enqueue(
        processor,
        pending(
            "releases",
            {
                "id": "release-silent",
                "title": "Already Credited",
                "sha256": "silent-hash-1",
                "country": "UK",
                "formats": [],
                "companies": companies(("Damont", 12345, "Pressed By", "pressing")),
            },
        ),
    )
    assert await processor.flush_queue("releases") is True

    await enqueue(
        processor,
        pending(
            "releases",
            {
                "id": "release-silent",
                "title": "Already Credited",
                "sha256": "silent-hash-2",
                "formats": [],
                "companies": [{"id": 12345, "name": "Damont", "entity_type_name": "Pressed By"}],
            },
        ),
    )
    assert await processor.flush_queue("releases") is True

    kept = await one(
        neo4j_driver,
        """
        MATCH (release:Release {id: $id})
        OPTIONAL MATCH (release)-[e:CREDITED_TO]->(co:Company)
        RETURN release.country AS country, collect([co.id, e.role]) AS credits
        """,
        id="release-silent",
    )
    assert kept == {"country": None, "credits": [["12345", "Pressed By"]]}


@pytest.mark.asyncio
async def test_real_engine_retries_transient_then_writes_once(neo4j_driver: AsyncDriver) -> None:
    processor = Neo4jBatchProcessor(
        neo4j_driver,
        BatchConfig(batch_size=1, min_batch_size=1, backoff_initial=0.001),
    )
    project = processor._process_artists_batch
    attempts = 0

    async def transient_once(messages: list[PendingMessage]) -> set[int]:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ServiceUnavailable("temporary outage")
        return await project(messages)

    processor._process_artists_batch = transient_once  # type: ignore[method-assign]
    message = pending("artists", {"id": "retry-artist", "name": "Retry Artist", "sha256": "retry-hash"})
    await enqueue(processor, message)

    assert await processor.flush_queue("artists") is False
    await asyncio.sleep(0.002)
    assert await processor.flush_queue("artists") is True
    message.ack_callback.assert_awaited_once()
    message.nack_callback.assert_not_awaited()
    assert (await one(neo4j_driver, "MATCH (a:Artist {id: $id}) RETURN a.name AS name", id="retry-artist"))["name"] == "Retry Artist"


@pytest.mark.asyncio
async def test_real_engine_isolates_poison_and_writes_healthy_tail(neo4j_driver: AsyncDriver) -> None:
    processor = Neo4jBatchProcessor(
        neo4j_driver,
        BatchConfig(batch_size=2, min_batch_size=1, max_flush_retries=3, max_poison_retries=2),
    )
    project = processor._process_artists_batch

    async def reject_named_poison(messages: list[PendingMessage]) -> set[int]:
        if messages[0].data["id"] == "poison-artist":
            raise ValueError("deterministic poison")
        return await project(messages)

    processor._process_artists_batch = reject_named_poison  # type: ignore[method-assign]
    poison = pending("artists", {"id": "poison-artist", "name": "Poison", "sha256": "poison-hash"})
    healthy = pending("artists", {"id": "healthy-artist", "name": "Healthy", "sha256": "healthy-hash"})
    await enqueue(processor, poison)
    await enqueue(processor, healthy)

    assert await processor.flush_queue("artists") is True
    poison.nack_callback.assert_awaited_once()
    poison.ack_callback.assert_not_awaited()
    healthy.ack_callback.assert_awaited_once()
    assert (await one(neo4j_driver, "MATCH (a:Artist {id: $id}) RETURN a.name AS name", id="healthy-artist"))["name"] == "Healthy"


@pytest.mark.asyncio
async def test_shutdown_drains_real_engine_and_settles_each_member_once(neo4j_driver: AsyncDriver) -> None:
    processor = Neo4jBatchProcessor(neo4j_driver, BatchConfig(batch_size=10))
    messages = [pending("artists", {"id": f"shutdown-{index}", "name": f"Artist {index}", "sha256": f"hash-{index}"}) for index in range(2)]
    for message in messages:
        await enqueue(processor, message)

    processor.shutdown()
    assert await processor.flush_all() is True
    assert all(message.ack_callback.await_count == 1 for message in messages)
    assert all(message.nack_callback.await_count == 0 for message in messages)
    assert (await one(neo4j_driver, "MATCH (a:Artist) WHERE a.id STARTS WITH 'shutdown-' RETURN count(a) AS count"))["count"] == 2


def credited_release(release_id: str, sha256: str, *credits: tuple[str, str]) -> PendingMessage:
    """Build one release message carrying (name, role) extraartists credits."""
    return pending(
        "releases",
        {
            "id": release_id,
            "title": release_id,
            "sha256": sha256,
            "formats": [],
            "extraartists": [{"name": name, "role": role} for name, role in credits],
        },
    )


async def import_releases(processor: Neo4jBatchProcessor, *messages: PendingMessage) -> None:
    for message in messages:
        await enqueue(processor, message)
    assert await processor.flush_queue("releases") is True


async def recount_credits(driver: AsyncDriver, batch_size: int | None = None) -> None:
    """Run the post-import maintenance step against the disposable engine."""
    with (
        patch.object(graphinator, "graph", driver),
        patch.object(graphinator, "MAINTENANCE_SETTLE_SECONDS", 0.0),
        patch.object(graphinator, "PERSON_CREDIT_COUNT_BATCH_SIZE", batch_size or graphinator.PERSON_CREDIT_COUNT_BATCH_SIZE),
    ):
        assert await graphinator.compute_person_credit_counts() is True


async def credit_counts(driver: AsyncDriver, name: str) -> dict[str, Any]:
    """Every credit-count property of one person, by property name."""
    person = await one(driver, "MATCH (p:Person {name: $name}) RETURN properties(p) AS props", name=name)
    return {prop: person["props"].get(prop) for prop in CREDIT_COUNT_PROPERTIES}


def expected(total: int, **categories: int) -> dict[str, int]:
    """The full property map: every category not named reads 0."""
    return {"credit_count": total, **{f"credit_count_{category}": categories.get(category, 0) for category in ALL_CATEGORIES}}


@pytest.mark.asyncio
async def test_person_credit_counts_are_distinct_releases_per_category(neo4j_driver: AsyncDriver) -> None:
    """Multi-category people, and releases credited under several roles of one category.

    Flood produces and mixes R1 (two categories, one release), produces and executive-
    produces R2 (two edges, one category, one release), and plays bass on R3. Run with a
    batch of one person per inner transaction, so the IN TRANSACTIONS batching itself is
    exercised by the real engine.
    """
    await import_releases(
        Neo4jBatchProcessor(neo4j_driver),
        credited_release("R1", "r1-a", ("Flood", "Producer"), ("Flood", "Mixed By"), ("Bob Ludwig", "Mastered By")),
        credited_release("R2", "r2-a", ("Flood", "Producer"), ("Flood", "Executive Producer")),
        credited_release("R3", "r3-a", ("Flood", "Bass"), ("Bob Ludwig", "Mastered By"), ("Bob Ludwig", "Lacquer Cut By")),
    )

    await recount_credits(neo4j_driver, batch_size=1)

    assert await credit_counts(neo4j_driver, "Flood") == expected(3, production=2, engineering=1, session=1)
    assert await credit_counts(neo4j_driver, "Bob Ludwig") == expected(2, mastering=2)


@pytest.mark.asyncio
async def test_person_credit_counts_reflect_removed_credits(neo4j_driver: AsyncDriver) -> None:
    """A re-imported release prunes the credits Discogs dropped, and the recount reads 0
    for a category — and for a person — that no longer has any."""
    processor = Neo4jBatchProcessor(neo4j_driver)
    await import_releases(
        processor,
        credited_release("R1", "r1-a", ("Flood", "Producer"), ("Flood", "Mixed By"), ("Alan Moulder", "Mixed By")),
        credited_release("R2", "r2-a", ("Flood", "Producer")),
    )
    await recount_credits(neo4j_driver)
    assert await credit_counts(neo4j_driver, "Flood") == expected(2, production=2, engineering=1)
    assert await credit_counts(neo4j_driver, "Alan Moulder") == expected(1, engineering=1)

    # The corrected R1 drops Flood's mixing credit and Alan Moulder entirely; the
    # corrected R2 drops every credit.
    await import_releases(
        processor,
        credited_release("R1", "r1-b", ("Flood", "Producer")),
        credited_release("R2", "r2-b"),
    )
    await recount_credits(neo4j_driver)

    assert await credit_counts(neo4j_driver, "Flood") == expected(1, production=1)
    assert await credit_counts(neo4j_driver, "Alan Moulder") == expected(0)
    edges = await one(neo4j_driver, "MATCH (p:Person)-[c:CREDITED_ON]->(r:Release) RETURN collect([p.name, c.role, r.id]) AS edges")
    assert edges == {"edges": [["Flood", "Producer", "R1"]]}


@pytest.mark.asyncio
async def test_single_record_path_prunes_removed_credits(neo4j_driver: AsyncDriver) -> None:
    async def project(record: dict[str, Any]) -> None:
        async with neo4j_driver.session(database="neo4j") as session:
            await session.execute_write(process_release, record)

    await project(
        {"id": "S1", "title": "S1", "sha256": "s1-a", "extraartists": [{"name": "Flood", "role": "Producer"}, {"name": "Flood", "role": "Mixed By"}]}
    )
    await project({"id": "S1", "title": "S1", "sha256": "s1-b", "extraartists": [{"name": "Flood", "role": "Producer"}]})
    await recount_credits(neo4j_driver)

    assert await credit_counts(neo4j_driver, "Flood") == expected(1, production=1)


_REAL_MAINTENANCE_HELPERS = {
    name: getattr(graphinator, name)
    for name in (
        "_is_extraction_maintained",
        "_latest_extraction_version",
        "_stamp_maintenance_success",
        "_record_maintenance_failure",
    )
}


async def run_page(driver: AsyncDriver, cypher: str) -> int:
    """Run one whole-graph page of a sweep statement; return the properties it set."""
    async with driver.session(database="neo4j") as session:
        result = await session.run(cypher, cursor="", size=1000)
        summary = await result.consume()
        return summary.counters.properties_set


@pytest.mark.asyncio
async def test_sweeps_write_only_properties_that_changed(neo4j_driver: AsyncDriver) -> None:
    """Person and Label pages compare before writing: a rerun sets nothing, a change sets only itself."""
    await import_releases(
        Neo4jBatchProcessor(neo4j_driver),
        credited_release("R1", "r1-a", ("Flood", "Producer"), ("Flood", "Mixed By")),
        credited_release("R2", "r2-a", ("Flood", "Bass")),
    )
    async with neo4j_driver.session(database="neo4j") as session:
        await session.run(
            "CREATE (l:Label {id: 'L1'}), (a:Artist {id: 'A1'}), (g:Genre {name: 'Rock'}) "
            "WITH l, a, g MATCH (r:Release {id: 'R1'}) CREATE (r)-[:ON]->(l), (r)-[:BY]->(a), (r)-[:IS]->(g)"
        )
    person_page = build_credit_count_page_cypher()
    label_page = graphinator.build_label_stats_page_cypher()

    # First pass writes every property: 8 on the one person, 3 on the one label.
    assert await run_page(neo4j_driver, person_page) == len(CREDIT_COUNT_PROPERTIES)
    assert await run_page(neo4j_driver, label_page) == 3
    flood = await credit_counts(neo4j_driver, "Flood")
    assert flood == expected(2, production=1, engineering=1, session=1)

    # Unchanged data: nothing is written, and the values are identical.
    assert await run_page(neo4j_driver, person_page) == 0
    assert await run_page(neo4j_driver, label_page) == 0
    assert await credit_counts(neo4j_driver, "Flood") == flood

    # One stale value (and one lost category read as stale) is rewritten, alone.
    async with neo4j_driver.session(database="neo4j") as session:
        await session.run("MATCH (p:Person {name: 'Flood'}) SET p.credit_count_session = 9")
        await session.run("MATCH (l:Label {id: 'L1'}) SET l.release_count = 9")
    assert await run_page(neo4j_driver, person_page) == 1
    assert await run_page(neo4j_driver, label_page) == 1
    assert await credit_counts(neo4j_driver, "Flood") == flood
    assert (await one(neo4j_driver, "MATCH (l:Label {id: 'L1'}) RETURN l.release_count AS rc"))["rc"] == 1


@pytest.mark.asyncio
async def test_an_over_limit_person_name_cannot_lose_the_release(neo4j_driver: AsyncDriver, monkeypatch: pytest.MonkeyPatch) -> None:
    """With the real unique index on Person.name, a name past its 8,164-byte key limit is bounded, not fatal."""
    async with neo4j_driver.session(database="neo4j") as session:
        await session.run("CREATE CONSTRAINT person_name_key IF NOT EXISTS FOR (p:Person) REQUIRE p.name IS UNIQUE")
    fits = "a" * 8000  # the largest name left as received
    ascii_long = "b" * 9000
    wide_long = "€" * 4000  # 12,000 bytes in 4,000 characters

    def release(sha: str) -> PendingMessage:
        return credited_release("LONG1", sha, (fits, "Producer"), (ascii_long, "Mixed By"), (wide_long, "Bass"), ("Flood", "Producer"))

    # Control: with the guard off the real index refuses the batch and the release is nacked.
    with monkeypatch.context() as unguarded:
        unguarded.setattr("graphinator.person_credits.PERSON_NAME_MAX_BYTES", 10**9)
        control = release("control")
        config = BatchConfig(batch_size=1, min_batch_size=1, max_flush_retries=1, max_poison_retries=1, backoff_initial=0.01, backoff_max=0.01)
        unguarded_processor = Neo4jBatchProcessor(neo4j_driver, config)
        await enqueue(unguarded_processor, control)
        await unguarded_processor.flush_queue("releases")
        control.nack_callback.assert_awaited()
        control.ack_callback.assert_not_awaited()
    assert (await one(neo4j_driver, "MATCH (r:Release {id: 'LONG1'}) RETURN count(r) AS n"))["n"] == 0

    processor = Neo4jBatchProcessor(neo4j_driver)
    await import_releases(processor, release("v1"))
    # Re-importing the same data is idempotent: one Person per distinct name.
    await import_releases(processor, release("v2"))

    people = await one(
        neo4j_driver,
        """
        MATCH (p:Person)-[c:CREDITED_ON]->(:Release {id: 'LONG1'})
        RETURN count(DISTINCT p) AS people, count(c) AS credits, max(size(p.name)) AS longest
        """,
    )
    assert people["people"] == 4
    assert people["credits"] == 4
    assert people["longest"] <= 8000
    assert (await one(neo4j_driver, "MATCH (p:Person {name: $name}) RETURN count(p) AS n", name=fits))["n"] == 1


@pytest.mark.asyncio
async def test_maintained_at_stamp_on_real_neo4j(neo4j_driver: AsyncDriver, monkeypatch: pytest.MonkeyPatch) -> None:
    """Stamp, failure record, idempotence and per-version independence in real Cypher."""
    monkeypatch.setattr(graphinator, "graph", neo4j_driver)
    for name, fn in _REAL_MAINTENANCE_HELPERS.items():
        monkeypatch.setattr(graphinator, name, fn)
    monkeypatch.setattr(graphinator, "_load_extraction_signals", _LOAD_EXTRACTION_SIGNALS)
    monkeypatch.setattr(graphinator, "_persist_extraction_signals", _PERSIST_EXTRACTION_SIGNALS)
    signals = set(graphinator.DATA_TYPES)
    await _PERSIST_EXTRACTION_SIGNALS("dump-1", signals)
    await asyncio.sleep(0.01)
    await _PERSIST_EXTRACTION_SIGNALS("dump-2", signals)

    # Newest complete, unstamped version is the restart-resume candidate.
    assert await graphinator._latest_extraction_version() == "dump-2"
    assert await graphinator._is_extraction_maintained("dump-2") is False

    # A failed pass records an error and no stamp; dump-1 is untouched.
    await graphinator._record_maintenance_failure("dump-2", "boom")
    row = await one(neo4j_driver, "MATCH (m:ExtractionCompletion {version: 'dump-2'}) RETURN m.maintained_at AS at, m.maintenance_error AS err")
    assert row == {"at": None, "err": "boom"}
    assert await graphinator._is_extraction_maintained("dump-1") is False

    # Success stamps, clears the error, and is idempotent (first stamp wins).
    await graphinator._stamp_maintenance_success("dump-2")
    first = await one(
        neo4j_driver,
        "MATCH (m:ExtractionCompletion {version: 'dump-2'}) RETURN toString(m.maintained_at) AS at, m.maintenance_error AS err, m.signals AS signals",
    )
    assert first["at"] is not None and first["err"] is None and set(first["signals"]) == signals
    await asyncio.sleep(0.01)
    await graphinator._stamp_maintenance_success("dump-2")
    second = await one(neo4j_driver, "MATCH (m:ExtractionCompletion {version: 'dump-2'}) RETURN toString(m.maintained_at) AS at")
    assert second["at"] == first["at"]
    assert await graphinator._is_extraction_maintained("dump-2") is True

    # A later failure never unstamps; the other dump stays independent.
    await graphinator._record_maintenance_failure("dump-2", "late")
    assert await graphinator._is_extraction_maintained("dump-2") is True
    assert await graphinator._is_extraction_maintained("dump-1") is False
    stamped = await one(neo4j_driver, "MATCH (m:ExtractionCompletion) WHERE m.maintained_at IS NOT NULL RETURN count(m) AS n")
    assert stamped["n"] == 1

    # Stamping a version with no node fails loudly instead of pretending.
    with pytest.raises(RuntimeError):
        await graphinator._stamp_maintenance_success("never-seen")


@pytest.mark.asyncio
async def test_paged_person_recount_resumes_from_its_committed_cursor(neo4j_driver: AsyncDriver, monkeypatch: pytest.MonkeyPatch) -> None:
    """Real Cypher: pages of one person, a crash after two pages, a resume that finishes."""
    await import_releases(
        Neo4jBatchProcessor(neo4j_driver),
        credited_release("R1", "r1-a", ("Alpha", "Producer"), ("Bravo", "Mixed By"), ("Charlie", "Bass"), ("Delta", "Mastered By")),
    )
    monkeypatch.setattr(graphinator, "graph", neo4j_driver)
    monkeypatch.setattr(graphinator, "MAINTENANCE_SETTLE_SECONDS", 0.0)
    monkeypatch.setattr(graphinator, "PERSON_CREDIT_COUNT_BATCH_SIZE", 1)
    token = graphinator._maintenance_run_key.set("integration-run")
    try:
        real_run = graphinator.maintenance_paging.run_statement
        pages = {"n": 0}

        async def crash_on_third_page(graph: Any, cypher: str, **params: Any) -> Any:
            if "MATCH (p:Person)" in cypher:
                pages["n"] += 1
                if pages["n"] == 3:
                    raise RuntimeError("connection lost")
            return await real_run(graph, cypher, **params)

        monkeypatch.setattr(graphinator.maintenance_paging, "run_statement", crash_on_third_page)
        assert await graphinator.compute_person_credit_counts() is False
        checkpoint = await one(
            neo4j_driver, "MATCH (c:MaintenanceCheckpoint {step: 'person_credit_counts'}) RETURN c.cursor AS cursor, c.done AS done"
        )
        assert checkpoint == {"cursor": "Bravo", "done": False}
        assert (await credit_counts(neo4j_driver, "Charlie"))["credit_count"] is None, "not yet reached"

        monkeypatch.setattr(graphinator.maintenance_paging, "run_statement", real_run)
        assert await graphinator.compute_person_credit_counts() is True
    finally:
        graphinator._maintenance_run_key.reset(token)

    for name, extra in (("Alpha", {"production": 1}), ("Bravo", {"engineering": 1}), ("Charlie", {"session": 1}), ("Delta", {"mastering": 1})):
        assert await credit_counts(neo4j_driver, name) == expected(1, **extra)
    done = await one(neo4j_driver, "MATCH (c:MaintenanceCheckpoint {step: 'person_credit_counts'}) RETURN c.done AS done")
    assert done == {"done": True}


@pytest.mark.asyncio
async def test_genre_style_label_stats_are_computed_by_paged_statements(neo4j_driver: AsyncDriver, monkeypatch: pytest.MonkeyPatch) -> None:
    """Real Cypher for the Genre/Style per-metric statements and the Label page."""
    async with neo4j_driver.session(database="neo4j") as session:
        await session.run(
            """
            CREATE (rock:Genre {name: 'Rock'}), (jazz:Genre {name: 'Jazz'}), (punk:Style {name: 'Punk'}),
                   (l1:Label {id: 'L1'}), (l2:Label {id: 'L2'}), (l3:Label {id: 'L3'}),
                   (a1:Artist {id: 'A1'}), (a2:Artist {id: 'A2'}),
                   (r1:Release {id: 'R1', year: 1977}), (r2:Release {id: 'R2', year: 1980}), (r3:Release {id: 'R3', year: 0}),
                   (r1)-[:IS]->(rock), (r1)-[:IS]->(punk), (r1)-[:BY]->(a1), (r1)-[:ON]->(l1),
                   (r2)-[:IS]->(rock), (r2)-[:BY]->(a2), (r2)-[:ON]->(l1), (r2)-[:ON]->(l2),
                   (r3)-[:IS]->(rock), (r3)-[:IS]->(jazz), (r3)-[:BY]->(a1), (r3)-[:ON]->(l2)
            """
        )
    monkeypatch.setattr(graphinator, "graph", neo4j_driver)
    monkeypatch.setattr(graphinator, "MAINTENANCE_SETTLE_SECONDS", 0.0)
    monkeypatch.setattr(graphinator, "LABEL_STATS_PAGE_SIZE", 2)
    token = graphinator._maintenance_run_key.set("integration-run")
    try:
        assert await graphinator.compute_genre_style_stats() is True
        # A second pass of the same run recomputes nothing and still succeeds.
        assert await graphinator.compute_genre_style_stats() is True
    finally:
        graphinator._maintenance_run_key.reset(token)

    rock = await one(
        neo4j_driver,
        "MATCH (g:Genre {name: 'Rock'}) RETURN g.release_count AS rc, g.artist_count AS ac, g.label_count AS lc, g.style_count AS sc, g.first_year AS fy",
    )
    assert rock == {"rc": 3, "ac": 2, "lc": 2, "sc": 1, "fy": 1977}
    punk = await one(neo4j_driver, "MATCH (s:Style {name: 'Punk'}) RETURN s.release_count AS rc, s.genre_count AS gc, s.first_year AS fy")
    assert punk == {"rc": 1, "gc": 1, "fy": 1977}
    l2 = await one(neo4j_driver, "MATCH (l:Label {id: 'L2'}) RETURN l.release_count AS rc, l.artist_count AS ac, l.genre_count AS gc")
    assert l2 == {"rc": 2, "ac": 2, "gc": 2}
    l3 = await one(neo4j_driver, "MATCH (l:Label {id: 'L3'}) RETURN l.release_count AS rc, l.artist_count AS ac, l.genre_count AS gc")
    assert l3 == {"rc": 0, "ac": 0, "gc": 0}


# catalog-api's statement for the two artist counts, verbatim from its
# tests/test_explore_artist_stats_integration.py (bead gm-catalog-api-87pz). The sweep has
# to store exactly what this computes.
CATALOG_API_ARTIST_COUNTS = """
MATCH (a:Artist)
CALL (a) {
    OPTIONAL MATCH (r:Release)-[:BY]->(a)
    WITH a, count(DISTINCT r) AS release_count
    OPTIONAL MATCH (r2:Release)-[:BY]->(a)
    OPTIONAL MATCH (r2)-[:ON]->(l:Label)
    WITH a, release_count, count(DISTINCT l) AS label_count
    RETURN a.id AS id, release_count, label_count
}
RETURN id, release_count, label_count ORDER BY id
"""

# The whole histogram, with no ORDER BY or LIMIT tricks, for comparing with what the sweep stored.
LIVE_GENRE_HISTOGRAM = """
MATCH (a:Artist)<-[:BY]-(r:Release)-[:IS]->(g:Genre)
RETURN a.id AS id, g.name AS genre, count(DISTINCT r) AS releases
"""

LIVE_LABEL_STYLES = """
MATCH (l:Label)<-[:ON]-(r:Release)-[:IS]->(s:Style)
RETURN l.id AS id, s.name AS style, count(DISTINCT r) AS releases ORDER BY id, style
"""

STORED_LABEL_STYLES = """
MATCH (l:Label)-[h:HAS_STYLE]->(s:Style)
RETURN l.id AS id, s.name AS style, h.release_count AS releases ORDER BY id, style
"""

ARTIST_STATS_SEED = """
CREATE (rock:Genre {name: 'Rock'}), (jazz:Genre {name: 'Jazz'}), (pop:Genre {name: 'Pop'}),
       (punk:Style {name: 'Punk'}), (bop:Style {name: 'Bebop'}),
       (l1:Label {id: 'L1'}), (l2:Label {id: 'L2'}), (l3:Label {id: 'L3'}),
       (heavy:Artist {id: 'A1'}), (tied:Artist {id: 'A2'}), (none:Artist {id: 'A3'}), (masters:Artist {id: 'A4'}),
       (various:Artist {id: '194'}),
       (m:Master {id: 'M1'}), (m)-[:BY]->(masters), (m)-[:IS]->(rock),
       (r1:Release {id: 'R1'}), (r2:Release {id: 'R2'}), (r3:Release {id: 'R3'}), (r4:Release {id: 'R4'}),
       (r1)-[:BY]->(heavy), (r1)-[:IS]->(rock), (r1)-[:IS]->(jazz), (r1)-[:IS]->(punk), (r1)-[:ON]->(l1), (r1)-[:ON]->(l2),
       (r2)-[:BY]->(heavy), (r2)-[:IS]->(rock), (r2)-[:IS]->(punk), (r2)-[:ON]->(l1),
       (r3)-[:BY]->(heavy), (r3)-[:BY]->(tied), (r3)-[:IS]->(jazz), (r3)-[:IS]->(bop), (r3)-[:ON]->(l2),
       (r4)-[:BY]->(tied), (r4)-[:IS]->(rock)
WITH various, rock, jazz, pop, punk, bop, l1, l2
UNWIND range(1, 500) AS i
WITH various, 'V' + i AS id, CASE WHEN i % 5 = 0 THEN rock ELSE pop END AS genre,
     CASE WHEN i % 2 = 0 THEN punk ELSE bop END AS style, CASE WHEN i % 3 = 0 THEN l1 ELSE l2 END AS label
CREATE (r:Release {id: id})-[:BY]->(various), (r)-[:IS]->(genre), (r)-[:IS]->(style), (r)-[:ON]->(label)
"""


async def run_artist_and_label_stats(driver: AsyncDriver, monkeypatch: pytest.MonkeyPatch, run_key: str) -> None:
    monkeypatch.setattr(graphinator, "graph", driver)
    monkeypatch.setattr(graphinator, "MAINTENANCE_SETTLE_SECONDS", 0.0)
    monkeypatch.setattr(graphinator, "LABEL_STATS_PAGE_SIZE", 2)
    monkeypatch.setattr(graphinator, "ARTIST_STATS_PAGE_SIZE", 2)
    token = graphinator._maintenance_run_key.set(run_key)
    try:
        assert await graphinator.compute_genre_style_stats() is True
        assert await graphinator.compute_artist_stats() is True
    finally:
        graphinator._maintenance_run_key.reset(token)


async def rows(driver: AsyncDriver, cypher: str) -> list[dict[str, Any]]:
    async with driver.session(database="neo4j") as session:
        result = await session.run(cypher)
        return [dict(record) async for record in result]


def exact_primary_genres(histogram: list[dict[str, Any]]) -> dict[str, str]:
    """The most releases, then the alphabetically first name, computed outside Cypher."""
    best: dict[str, tuple[int, str]] = {}
    for row in histogram:
        candidate = (-row["releases"], row["genre"])
        if row["id"] not in best or candidate < best[row["id"]]:
            best[row["id"]] = candidate
    return {artist: genre for artist, (_, genre) in best.items()}


@pytest.mark.asyncio
async def test_artist_stats_equal_the_live_counts_including_a_hub(neo4j_driver: AsyncDriver, monkeypatch: pytest.MonkeyPatch) -> None:
    """The stored values against catalog-api's statement and an exhaustive genre histogram."""
    await rows(neo4j_driver, ARTIST_STATS_SEED)
    await run_artist_and_label_stats(neo4j_driver, monkeypatch, "integration-run")

    stored = await rows(
        neo4j_driver,
        "MATCH (a:Artist) RETURN a.id AS id, a.release_count AS release_count, a.label_count AS label_count, a.primary_genre AS genre ORDER BY id",
    )
    assert [{k: row[k] for k in ("id", "release_count", "label_count")} for row in stored] == await rows(neo4j_driver, CATALOG_API_ARTIST_COUNTS)
    assert {row["id"]: row["genre"] for row in stored if row["genre"]} == exact_primary_genres(await rows(neo4j_driver, LIVE_GENRE_HISTOGRAM))

    by_id = {row["id"]: row for row in stored}
    # Rock on two of three releases, Jazz on two: the same tie rule, with a real count behind it.
    assert by_id["A1"] == {"id": "A1", "release_count": 3, "label_count": 2, "genre": "Jazz"}
    # A hub is computed like anyone else: every release counted, none sampled.
    assert by_id["194"] == {"id": "194", "release_count": 500, "label_count": 2, "genre": "Pop"}
    # One Jazz and one Rock release: the tie goes to the alphabetically first name, every time.
    assert by_id["A2"]["genre"] == "Jazz"
    # No releases, and a Master credit that is not a Release: zero counts, no genre.
    assert by_id["A3"] == {"id": "A3", "release_count": 0, "label_count": 0, "genre": None}
    assert by_id["A4"] == {"id": "A4", "release_count": 0, "label_count": 0, "genre": None}


@pytest.mark.asyncio
async def test_label_style_edges_equal_the_live_counts(neo4j_driver: AsyncDriver, monkeypatch: pytest.MonkeyPatch) -> None:
    await rows(neo4j_driver, ARTIST_STATS_SEED)
    await run_artist_and_label_stats(neo4j_driver, monkeypatch, "integration-run")

    stored = await rows(neo4j_driver, STORED_LABEL_STYLES)
    assert stored == await rows(neo4j_driver, LIVE_LABEL_STYLES)
    assert {"id": "L1", "style": "Punk", "releases": 2 + 83} in stored
    labels = await rows(neo4j_driver, "MATCH (l:Label {id: 'L3'}) RETURN l.release_count AS rc, l.artist_count AS ac, l.genre_count AS gc")
    assert labels == [{"rc": 0, "ac": 0, "gc": 0}]
    assert await rows(neo4j_driver, "MATCH (:Label {id: 'L3'})-[h:HAS_STYLE]->() RETURN h") == []


@pytest.mark.asyncio
async def test_a_later_pass_follows_the_graph_and_an_unchanged_one_writes_nothing(neo4j_driver: AsyncDriver, monkeypatch: pytest.MonkeyPatch) -> None:
    """A removed style loses its edge, a lost genre loses its property, and a rerun is free."""
    await rows(neo4j_driver, ARTIST_STATS_SEED)
    await run_artist_and_label_stats(neo4j_driver, monkeypatch, "run-1")

    async with neo4j_driver.session(database="neo4j") as session:
        before = await (await session.run(graphinator.build_artist_stats_page_cypher(), cursor="", size=100)).consume()
        label_before = await (await session.run(graphinator.build_label_stats_page_cypher(), cursor="", size=100)).consume()
    assert before.counters.properties_set == 0
    assert label_before.counters.properties_set == 0
    assert label_before.counters.relationships_created == 0
    assert label_before.counters.relationships_deleted == 0

    # Bebop leaves L2 entirely (R3 and every odd V release), and A2 loses its genres.
    await rows(neo4j_driver, "MATCH (:Style {name: 'Bebop'})<-[e:IS]-(:Release) DELETE e")
    await rows(neo4j_driver, "MATCH (:Artist {id: 'A2'})<-[:BY]-(:Release)-[e:IS]->(:Genre) DELETE e")
    await run_artist_and_label_stats(neo4j_driver, monkeypatch, "run-2")

    assert await rows(neo4j_driver, STORED_LABEL_STYLES) == await rows(neo4j_driver, LIVE_LABEL_STYLES)
    assert await rows(neo4j_driver, "MATCH (:Label)-[h:HAS_STYLE]->(:Style {name: 'Bebop'}) RETURN h") == []
    assert await rows(neo4j_driver, "MATCH (a:Artist {id: 'A2'}) RETURN a.primary_genre AS genre, a.release_count AS rc") == [
        {"genre": None, "rc": 2}
    ]


@pytest.mark.asyncio
async def test_derived_style_edges_do_not_keep_an_orphaned_stub_label(neo4j_driver: AsyncDriver, monkeypatch: pytest.MonkeyPatch) -> None:
    """A stub Label with only HAS_STYLE left is collected; a stub with a real edge is kept."""
    await rows(
        neo4j_driver,
        """
        CREATE (orphan:Label {id: 'L-orphan'})-[:HAS_STYLE {release_count: 3}]->(s:Style {name: 'Punk'}),
               (kept:Label {id: 'L-kept'})-[:HAS_STYLE {release_count: 1}]->(s),
               (:Release {id: 'R1'})-[:ON]->(kept)
        """,
    )
    monkeypatch.setattr(graphinator, "graph", neo4j_driver)
    assert await graphinator.cleanup_stub_nodes("labels") is True
    assert await rows(neo4j_driver, "MATCH (l:Label) RETURN l.id AS id") == [{"id": "L-kept"}]


_ORACLE_METRICS = {
    "release_count": "MATCH (n)<-[:IS]-(r:Release) RETURN count(DISTINCT r) AS v",
    "artist_count": "MATCH (n)<-[:IS]-(r:Release)-[:BY]->(a:Artist) RETURN count(DISTINCT a) AS v",
    "label_count": "MATCH (n)<-[:IS]-(r:Release)-[:ON]->(l:Label) RETURN count(DISTINCT l) AS v",
    "style_count": "MATCH (n)<-[:IS]-(r:Release)-[:IS]->(o:Style) RETURN count(DISTINCT o) AS v",
    "genre_count": "MATCH (n)<-[:IS]-(r:Release)-[:IS]->(o:Genre) RETURN count(DISTINCT o) AS v",
    "first_year": "MATCH (n)<-[:IS]-(r:Release) WHERE r.year > 0 RETURN min(r.year) AS v",
}


@pytest.mark.asyncio
async def test_shared_traversal_genre_style_metrics_equal_the_separate_distinct_queries(neo4j_driver: AsyncDriver) -> None:
    """The grouped statements give exactly what the one-aggregate-per-statement originals gave.

    A varied graph: releases with no artist, no label, several of each, years of 0, a Master
    that also has IS edges (it is not a Release, so it must not count), and nodes nothing is
    tagged with.
    """
    async with neo4j_driver.session(database="neo4j") as session:
        for statement in (
            "UNWIND range(1, 12) AS g CREATE (:Genre {name: 'G' + g})",
            "UNWIND range(1, 25) AS s CREATE (:Style {name: 'S' + s})",
            "UNWIND range(1, 9) AS a CREATE (:Artist {id: 'A' + a})",
            "UNWIND range(1, 7) AS l CREATE (:Label {id: 'L' + l})",
        ):
            await session.run(statement)
        await session.run(
            """
            UNWIND range(1, 120) AS i
            CREATE (r:Release {id: 'R' + i, year: CASE WHEN i % 7 = 0 THEN 0 ELSE 1950 + i % 60 END})
            WITH r, i
            CALL (r, i) { UNWIND range(0, i % 3) AS k MATCH (g:Genre {name: 'G' + (1 + (i + k * 5) % 12)}) MERGE (r)-[:IS]->(g) }
            CALL (r, i) { UNWIND range(0, i % 4) AS k MATCH (s:Style {name: 'S' + (1 + (i * 3 + k * 7) % 25)}) MERGE (r)-[:IS]->(s) }
            CALL (r, i) { UNWIND (CASE WHEN i % 5 = 0 THEN [] ELSE range(0, i % 3) END) AS k MATCH (a:Artist {id: 'A' + (1 + (i + k) % 9)}) MERGE (r)-[:BY]->(a) }
            CALL (r, i) { UNWIND (CASE WHEN i % 6 = 0 THEN [] ELSE range(0, i % 2) END) AS k MATCH (l:Label {id: 'L' + (1 + (i + k) % 7)}) MERGE (r)-[:ON]->(l) }
            """
        )
        await session.run(
            "CREATE (m:Master {id: 'M1'}) WITH m MATCH (g:Genre {name: 'G1'}), (s:Style {name: 'S1'}) CREATE (m)-[:IS]->(g), (m)-[:IS]->(s)"
        )

    async def run_all(node: str) -> dict[str, dict[str, Any]]:
        got: dict[str, dict[str, Any]] = {}
        async with neo4j_driver.session(database="neo4j") as session:
            rows = await (await session.run(f"MATCH (n:{node}) RETURN n.name AS name ORDER BY name")).data()
            for name in (row["name"] for row in rows):
                for cypher, _ in graphinator.build_node_stat_statements(node):
                    await (await session.run(cypher, key=name)).consume()
                expected_props: dict[str, Any] = {}
                for prop, query in _ORACLE_METRICS.items():
                    if prop == ("genre_count" if node == "Genre" else "style_count"):
                        continue
                    record = await (await session.run(f"MATCH (n:{node} {{name: $key}}) CALL {{ WITH n {query} }} RETURN v", key=name)).single()
                    expected_props[prop] = record["v"]
                actual = await (await session.run(f"MATCH (n:{node} {{name: $key}}) RETURN properties(n) AS p", key=name)).single()
                got[name] = {prop: actual["p"].get(prop) for prop in expected_props} | {"__expected__": expected_props}
        return got

    for node in ("Genre", "Style"):
        results = await run_all(node)
        assert results, node
        for name, row in results.items():
            expected_props = row.pop("__expected__")
            assert row == expected_props, f"{node} {name}"
        # The graph exercises real values, not only zeros.
        assert any(row["artist_count"] and row["label_count"] for row in results.values())


# --- names carried by release references (stub nodes) ------------------------------------


def named_release(sha256: str = "nr-a") -> dict[str, Any]:
    return {
        "id": "NR1",
        "title": "NR1",
        "sha256": sha256,
        "formats": [],
        "artists": [{"id": "A1", "name": " Stub Artist "}, {"id": "A2"}],
        "labels": [{"id": "L1", "name": "Stub Label", "catno": "X 1"}, {"id": "L2", "name": "  "}],
        "master_id": "M1",
    }


async def names(driver: AsyncDriver) -> dict[str, Any]:
    row = await one(
        driver,
        """
        MATCH (n) WHERE (n:Artist OR n:Label OR n:Master) AND n.id IN ['A1', 'A2', 'L1', 'L2', 'M1']
        RETURN collect([n.id, n.name]) AS pairs
        """,
    )
    return {pair[0]: pair[1] for pair in row["pairs"]}


async def single_path(driver: AsyncDriver, fn: Any, record: dict[str, Any]) -> None:
    async with driver.session(database="neo4j") as session:
        await session.execute_write(fn, record)


@pytest.mark.asyncio
async def test_batch_release_names_the_artist_and_label_stubs_it_creates(neo4j_driver: AsyncDriver) -> None:
    processor = Neo4jBatchProcessor(neo4j_driver)
    await import_releases(processor, pending("releases", named_release()))
    # A reference with no usable name stays unnamed; the master reference carries no title.
    assert await names(neo4j_driver) == {"A1": "Stub Artist", "A2": None, "L1": "Stub Label", "L2": None, "M1": None}
    # Stubs stay recognisable: no sha256.
    assert (await one(neo4j_driver, "MATCH (n:Label {id: 'L1'}) RETURN n.sha256 AS hash"))["hash"] is None


@pytest.mark.asyncio
async def test_single_release_names_the_artist_and_label_stubs_it_creates(neo4j_driver: AsyncDriver) -> None:
    await single_path(neo4j_driver, process_release, named_release())
    assert await names(neo4j_driver) == {"A1": "Stub Artist", "A2": None, "L1": "Stub Label", "L2": None, "M1": None}


@pytest.mark.asyncio
async def test_batch_entity_record_overwrites_a_stub_name_and_release_never_overwrites_it(neo4j_driver: AsyncDriver) -> None:
    processor = Neo4jBatchProcessor(neo4j_driver)
    # Stub first, entity second: the entity's own name wins.
    await import_releases(processor, pending("releases", named_release()))
    await enqueue(processor, pending("labels", {"id": "L1", "name": "Real Label", "sha256": "l1"}))
    await enqueue(processor, pending("artists", {"id": "A1", "name": "Real Artist", "sha256": "a1"}))
    assert await processor.flush_queue("labels") is True
    assert await processor.flush_queue("artists") is True
    assert (await names(neo4j_driver))["L1"] == "Real Label"
    assert (await names(neo4j_driver))["A1"] == "Real Artist"
    # Entity first, a later (changed) release second: the entity's name is untouched.
    changed = named_release("nr-b")
    changed["labels"][0]["name"] = "Different Spelling"
    changed["artists"][0]["name"] = "Different Artist"
    await import_releases(processor, pending("releases", changed))
    assert (await names(neo4j_driver))["L1"] == "Real Label"
    assert (await names(neo4j_driver))["A1"] == "Real Artist"


@pytest.mark.asyncio
async def test_single_path_both_arrival_orders_keep_the_entity_name(neo4j_driver: AsyncDriver) -> None:
    await single_path(neo4j_driver, process_release, named_release())
    await single_path(neo4j_driver, process_label, {"id": "L1", "name": "Real Label", "sha256": "l1"})
    await single_path(neo4j_driver, process_artist, {"id": "A1", "name": "Real Artist", "sha256": "a1"})
    assert (await names(neo4j_driver))["L1"] == "Real Label"
    assert (await names(neo4j_driver))["A1"] == "Real Artist"

    changed = named_release("nr-b")
    changed["labels"][0]["name"] = "Different Spelling"
    changed["artists"][0]["name"] = "Different Artist"
    await single_path(neo4j_driver, process_release, changed)
    assert (await names(neo4j_driver))["L1"] == "Real Label"
    assert (await names(neo4j_driver))["A1"] == "Real Artist"


@pytest.mark.asyncio
async def test_a_later_release_fills_a_missing_name_but_not_an_existing_one(neo4j_driver: AsyncDriver) -> None:
    """A stub first seen with no name takes the name from the next release that has one."""
    unnamed = named_release()
    unnamed["labels"][0].pop("name")
    await single_path(neo4j_driver, process_release, unnamed)
    assert (await names(neo4j_driver))["L1"] is None
    await single_path(neo4j_driver, process_release, named_release("nr-b"))
    assert (await names(neo4j_driver))["L1"] == "Stub Label"


@pytest.mark.asyncio
async def test_stub_name_backfill_is_batched_resumable_and_fill_only(neo4j_driver: AsyncDriver, tmp_path: Any) -> None:
    from graphinator.stub_name_backfill import backfill

    async with neo4j_driver.session(database="neo4j") as session:
        await session.run("CREATE (:Label {id: 'L1'}), (:Label {id: 'L2'}), (:Label {id: 'L3', name: 'Own Name'}), (:Artist {id: 'A1'})")
    lines = [
        '{"kind": "Label", "id": "L1", "name": "One"}',
        '{"kind": "Label", "id": "L2", "name": "Two"}',
        '{"kind": "Label", "id": "L3", "name": "Not Mine"}',
        "not json",
        '{"kind": "Artist", "id": "A1", "name": "Artist One"}',
    ]
    source = tmp_path / "names.ndjson"
    source.write_text("\n".join(lines) + "\n")
    checkpoint = tmp_path / "checkpoint"

    totals = await backfill(neo4j_driver, source, batch_size=2, checkpoint=checkpoint)

    assert totals["filled"] == 3
    assert totals["skipped"] == 1
    assert checkpoint.read_text().strip() == "5"
    got = await one(neo4j_driver, "MATCH (n) WHERE n.id IN ['L1','L2','L3','A1'] RETURN collect([n.id, n.name]) AS p")
    assert sorted(got["p"]) == [["A1", "Artist One"], ["L1", "One"], ["L2", "Two"], ["L3", "Own Name"]]
    # A re-run resumes after the checkpoint and has nothing left to do.
    assert (await backfill(neo4j_driver, source, batch_size=2, checkpoint=checkpoint))["lines"] == 0
