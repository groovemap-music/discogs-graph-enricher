"""Real-engine checks for the Cypher write paths mocks cannot validate."""

from __future__ import annotations

import os
from typing import Any
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from neo4j import AsyncDriver, AsyncGraphDatabase

from graphinator.batch_processor import Neo4jBatchProcessor, PendingMessage


pytestmark = pytest.mark.integration


def pending(data_type: str, data: dict[str, Any]) -> PendingMessage:
    """Build one batch message; settlement belongs to the queue-level tests."""
    return PendingMessage(data_type, data, AsyncMock(), AsyncMock())


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
    failures = await processor._process_artists_batch(
        [
            pending(
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
        ]
    )

    assert failures == set()
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
    failures = await processor._process_releases_batch(
        [
            pending(
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
        ]
    )

    assert failures == set()
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
    failures = await processor._process_releases_batch(
        [
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
    )

    assert failures == set()
    assert await one(
        neo4j_driver,
        """
        MATCH (style:Style)-[:PART_OF]->(genre:Genre)
        RETURN collect(DISTINCT [style.name, genre.name]) AS pairs
        """,
    ) == {"pairs": [["Techno", "Electronic"]]}
