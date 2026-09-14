"""Real-engine checks for the Cypher write paths mocks cannot validate."""

from __future__ import annotations

import asyncio
import os
from typing import Any
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from neo4j import AsyncDriver, AsyncGraphDatabase
from neo4j.exceptions import ServiceUnavailable

from graphinator.batch_processor import BatchConfig, Neo4jBatchProcessor, PendingMessage


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
    """Historical sy5k: co-occurrence must not be persisted as taxonomy."""
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
