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
from graphinator.entity_projection import process_release
from graphinator.person_credits import CREDIT_COUNT_PROPERTIES


pytestmark = pytest.mark.integration


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
