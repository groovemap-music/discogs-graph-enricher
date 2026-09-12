"""Per-entity Neo4j batch projections.

Queueing, flush scheduling, retry classification, and acknowledgements remain owned by
:mod:`graphinator.batch_processor`; this module owns only record selection, Cypher,
and parameter construction inside a supplied transaction boundary.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import structlog
from common.credit_roles import categorize_role

from graphinator.media_projection import (
    MEDIA_SOURCE,
    MERGE_MEDIA_CYPHER,
    PRUNE_ISSUED_ON_CYPHER,
    issued_on_rows,
    media_families,
    prune_record,
    resolve_media_block,
)


if TYPE_CHECKING:
    from collections.abc import Callable

    from graphinator.batch_processor import PendingMessage


logger = structlog.get_logger(__name__)


class Neo4jBatchProjector:
    """Build and execute entity-specific writes within processor-owned batches."""

    def __init__(self, driver: Any, projection_logger: Any = logger) -> None:
        self.driver = driver
        self.logger = projection_logger

    async def _process_artists_batch(self, messages: list[PendingMessage]) -> set[int]:
        """Process a batch of artist records.

        Uses a single session for hash check + write to ensure atomicity.
        Returns indices of messages that should be nacked (e.g. missing 'id').
        """
        nack_indices: set[int] = set()
        all_artists = []
        for i, msg in enumerate(messages):
            artist_id = msg.data.get("id")
            if not artist_id:
                self.logger.warning(
                    "⚠️ Message missing required 'id' field, will nack",
                    data_keys=list(msg.data.keys()),
                )
                nack_indices.add(i)
                continue
            all_artists.append(msg.data)

        if not all_artists:
            return nack_indices

        # Single session for both hash check and write to avoid TOCTOU race
        async with self.driver.session(database="neo4j") as session:
            # Check which artists need updates (by hash)
            ids = [a.get("id") for a in all_artists]
            existing_hashes: dict[str, str] = {}
            if ids:
                result = await session.run(
                    "UNWIND $ids AS id OPTIONAL MATCH (a:Artist {id: id}) RETURN id, a.sha256 AS hash",
                    ids=ids,
                )
                async for record in result:
                    if record["hash"]:
                        existing_hashes[str(record["id"])] = record["hash"]

            artists_to_process = [a for a in all_artists if existing_hashes.get(str(a["id"])) != a.get("sha256")]

            if not artists_to_process:
                self.logger.debug("🔄 All artists in batch already up to date")
                return nack_indices

            async def batch_write(tx: Any) -> None:
                # Create/update all artist nodes
                await tx.run(
                    """
                    UNWIND $artists AS artist
                    MERGE (a:Artist {id: artist.id})
                    SET a.name = artist.name,
                        a.sha256 = artist.sha256,
                        a.resource_url = 'https://api.discogs.com/artists/' + artist.id,
                        a.releases_url = 'https://api.discogs.com/artists/' + artist.id + '/releases'
                    """,
                    artists=artists_to_process,
                )

                # Process all member relationships
                members_data = []
                for artist in artists_to_process:
                    if artist.get("members"):
                        for member in artist["members"]:
                            if member.get("id"):
                                members_data.append(
                                    {
                                        "artist_id": artist["id"],
                                        "member_id": member["id"],
                                    }
                                )
                if members_data:
                    await tx.run(
                        """
                        UNWIND $members AS rel
                        MATCH (a:Artist {id: rel.artist_id})
                        MERGE (m:Artist {id: rel.member_id})
                        MERGE (m)-[:MEMBER_OF]->(a)
                        """,
                        members=members_data,
                    )

                # Process all group relationships
                groups_data = []
                for artist in artists_to_process:
                    if artist.get("groups"):
                        for group in artist["groups"]:
                            if group.get("id"):
                                groups_data.append(
                                    {
                                        "artist_id": artist["id"],
                                        "group_id": group["id"],
                                    }
                                )
                if groups_data:
                    await tx.run(
                        """
                        UNWIND $groups AS rel
                        MATCH (a:Artist {id: rel.artist_id})
                        MERGE (g:Artist {id: rel.group_id})
                        MERGE (a)-[:MEMBER_OF]->(g)
                        """,
                        groups=groups_data,
                    )

                # Process all alias relationships
                aliases_data = []
                for artist in artists_to_process:
                    if artist.get("aliases"):
                        for alias in artist["aliases"]:
                            if alias.get("id"):
                                aliases_data.append(
                                    {
                                        "artist_id": artist["id"],
                                        "alias_id": alias["id"],
                                    }
                                )
                if aliases_data:
                    await tx.run(
                        """
                        UNWIND $aliases AS rel
                        MATCH (a:Artist {id: rel.artist_id})
                        MERGE (al:Artist {id: rel.alias_id})
                        MERGE (al)-[:ALIAS_OF]->(a)
                        """,
                        aliases=aliases_data,
                    )

            await session.execute_write(batch_write)
        return nack_indices

    async def _process_labels_batch(self, messages: list[PendingMessage]) -> set[int]:
        """Process a batch of label records.

        Uses a single session for hash check + write to ensure atomicity.
        Returns indices of messages that should be nacked (e.g. missing 'id').
        """
        nack_indices: set[int] = set()
        all_labels = []
        for i, msg in enumerate(messages):
            label_id = msg.data.get("id")
            if not label_id:
                self.logger.warning(
                    "⚠️ Message missing required 'id' field, will nack",
                    data_keys=list(msg.data.keys()),
                )
                nack_indices.add(i)
                continue
            all_labels.append(msg.data)

        if not all_labels:
            return nack_indices

        # Single session for both hash check and write to avoid TOCTOU race
        async with self.driver.session(database="neo4j") as session:
            ids = [label.get("id") for label in all_labels]
            existing_hashes: dict[str, str] = {}
            if ids:
                result = await session.run(
                    "UNWIND $ids AS id OPTIONAL MATCH (l:Label {id: id}) RETURN id, l.sha256 AS hash",
                    ids=ids,
                )
                async for record in result:
                    if record["hash"]:
                        existing_hashes[str(record["id"])] = record["hash"]

            labels_to_process = [label for label in all_labels if existing_hashes.get(str(label["id"])) != label.get("sha256")]

            if not labels_to_process:
                self.logger.debug("🔄 All labels in batch already up to date")
                return nack_indices

            async def batch_write(tx: Any) -> None:
                # Create/update all label nodes
                await tx.run(
                    """
                    UNWIND $labels AS label
                    MERGE (l:Label {id: label.id})
                    SET l.name = label.name,
                        l.sha256 = label.sha256
                    """,
                    labels=labels_to_process,
                )

                # Process parent label relationships
                parent_data = []
                for label in labels_to_process:
                    parent = label.get("parentLabel")
                    if parent and parent.get("id"):
                        parent_data.append(
                            {
                                "label_id": label["id"],
                                "parent_id": parent["id"],
                            }
                        )
                if parent_data:
                    await tx.run(
                        """
                        UNWIND $parents AS rel
                        MATCH (l:Label {id: rel.label_id})
                        MERGE (p:Label {id: rel.parent_id})
                        MERGE (l)-[:SUBLABEL_OF]->(p)
                        """,
                        parents=parent_data,
                    )

                # Process sublabel relationships
                sublabel_data = []
                for label in labels_to_process:
                    if label.get("sublabels"):
                        for sublabel in label["sublabels"]:
                            if sublabel.get("id"):
                                sublabel_data.append(
                                    {
                                        "label_id": label["id"],
                                        "sublabel_id": sublabel["id"],
                                    }
                                )
                if sublabel_data:
                    await tx.run(
                        """
                        UNWIND $sublabels AS rel
                        MATCH (l:Label {id: rel.label_id})
                        MERGE (s:Label {id: rel.sublabel_id})
                        MERGE (s)-[:SUBLABEL_OF]->(l)
                        """,
                        sublabels=sublabel_data,
                    )

            await session.execute_write(batch_write)
        return nack_indices

    @staticmethod
    async def _prune_stale_edges(
        tx: Any,
        label: str,
        records: list[dict[str, Any]],
        *,
        rel_type: str,
        target_label: str,
        target_key: str,
        desired: Callable[[dict[str, Any]], list[Any]],
        outgoing: bool = True,
    ) -> None:
        """Delete this record's managed edges that its NEW version no longer asserts.

        Relationship writes are MERGE-only and therefore purely additive, but the
        underlying Discogs records are mutable: a release retagged from Rock to Jazz
        gets a new sha256, passes the hash gate, MERGEs the Jazz edge — and keeps the
        Rock one forever. compute_genre_style_stats then counts those stale edges
        (``MATCH (g)<-[:IS]-(r:Release) RETURN count(DISTINCT r)``), so Genre/Style/Label
        release_count/artist_count are permanently over-stated and explore endpoints
        list the release under a genre it no longer has (discogsography-bd0u).

        Only edges of ``rel_type`` to ``target_label``, and only for the records in this
        batch, are considered — an entity that is not being reprocessed is untouched.
        Records whose new version asserts NO edges of the type are included too: that is
        precisely the "all associations removed" case.
        """
        prune_data = [{"key": record["id"], "keep": desired(record)} for record in records]
        if not prune_data:
            return

        arrow = f"-[rel:{rel_type}]->" if outgoing else f"<-[rel:{rel_type}]-"
        await tx.run(
            f"""
            UNWIND $records AS record
            MATCH (n:{label} {{id: record.key}}){arrow}(t:{target_label})
            WHERE NOT t.{target_key} IN record.keep
            DELETE rel
            """,
            records=prune_data,
        )

    async def _process_masters_batch(self, messages: list[PendingMessage]) -> set[int]:
        """Process a batch of master records.

        Uses a single session for hash check + write to ensure atomicity.
        Returns indices of messages that should be nacked (e.g. missing 'id').
        """
        nack_indices: set[int] = set()
        all_masters = []
        for i, msg in enumerate(messages):
            master_id = msg.data.get("id")
            if not master_id:
                self.logger.warning(
                    "⚠️ Message missing required 'id' field, will nack",
                    data_keys=list(msg.data.keys()),
                )
                nack_indices.add(i)
                continue
            all_masters.append(msg.data)

        if not all_masters:
            return nack_indices

        # Single session for both hash check and write to avoid TOCTOU race
        async with self.driver.session(database="neo4j") as session:
            ids = [m.get("id") for m in all_masters]
            existing_hashes: dict[str, str] = {}
            if ids:
                result = await session.run(
                    "UNWIND $ids AS id OPTIONAL MATCH (m:Master {id: id}) RETURN id, m.sha256 AS hash",
                    ids=ids,
                )
                async for record in result:
                    if record["hash"]:
                        existing_hashes[str(record["id"])] = record["hash"]

            masters_to_process = [m for m in all_masters if existing_hashes.get(str(m["id"])) != m.get("sha256")]

            if not masters_to_process:
                self.logger.debug("🔄 All masters in batch already up to date")
                return nack_indices

            async def batch_write(tx: Any) -> None:
                # Create/update all master nodes
                await tx.run(
                    """
                    UNWIND $masters AS master
                    MERGE (m:Master {id: master.id})
                    SET m.title = master.title,
                        m.year = master.year,
                        m.sha256 = master.sha256
                    """,
                    masters=masters_to_process,
                )

                # Prune managed edges the NEW version of each record no longer asserts.
                # MERGE-only writes are additive, so without this a master retagged from
                # Rock to Jazz keeps both edges forever and inflates every aggregate
                # count computed off them (discogsography-bd0u).
                await self._prune_stale_edges(
                    tx,
                    "Master",
                    masters_to_process,
                    rel_type="BY",
                    target_label="Artist",
                    target_key="id",
                    desired=lambda m: [a["id"] for a in (m.get("artists") or []) if a.get("id")],
                )
                await self._prune_stale_edges(
                    tx,
                    "Master",
                    masters_to_process,
                    rel_type="IS",
                    target_label="Genre",
                    target_key="name",
                    desired=lambda m: [g for g in (m.get("genres") or []) if g],
                )
                await self._prune_stale_edges(
                    tx,
                    "Master",
                    masters_to_process,
                    rel_type="IS",
                    target_label="Style",
                    target_key="name",
                    desired=lambda m: [st for st in (m.get("styles") or []) if st],
                )

                # Process artist relationships
                artist_data = []
                for master in masters_to_process:
                    if master.get("artists"):
                        for artist in master["artists"]:
                            if artist.get("id"):
                                artist_data.append(
                                    {
                                        "master_id": master["id"],
                                        "artist_id": artist["id"],
                                    }
                                )
                if artist_data:
                    await tx.run(
                        """
                        UNWIND $artists AS rel
                        MATCH (m:Master {id: rel.master_id})
                        MERGE (a:Artist {id: rel.artist_id})
                        MERGE (m)-[:BY]->(a)
                        """,
                        artists=artist_data,
                    )

                # Process genre relationships
                genre_data = []
                for master in masters_to_process:
                    if master.get("genres"):
                        for genre in master["genres"]:
                            if genre:
                                genre_data.append(
                                    {
                                        "master_id": master["id"],
                                        "genre": genre,
                                    }
                                )
                if genre_data:
                    await tx.run(
                        """
                        UNWIND $genres AS rel
                        MATCH (m:Master {id: rel.master_id})
                        MERGE (g:Genre {name: rel.genre})
                        MERGE (m)-[:IS]->(g)
                        """,
                        genres=genre_data,
                    )

                # Process style relationships
                style_data = []
                for master in masters_to_process:
                    if master.get("styles"):
                        for style in master["styles"]:
                            if style:
                                style_data.append(
                                    {
                                        "master_id": master["id"],
                                        "style": style,
                                    }
                                )
                if style_data:
                    await tx.run(
                        """
                        UNWIND $styles AS rel
                        MATCH (m:Master {id: rel.master_id})
                        MERGE (s:Style {name: rel.style})
                        MERGE (m)-[:IS]->(s)
                        """,
                        styles=style_data,
                    )

                # Connect styles to genres. PART_OF asserts a style belongs to a genre,
                # which is only unambiguous when the record carries a single genre — a
                # multi-genre record cartesian-linking every style to every genre would
                # create false Style-[:PART_OF]->Genre edges (discogsography-sy5k).
                genre_style_data = []
                for master in masters_to_process:
                    genres = master.get("genres", [])
                    styles = master.get("styles", [])
                    if len(genres) == 1 and genres[0]:
                        genre = genres[0]
                        for style in styles:
                            if style:
                                genre_style_data.append(
                                    {
                                        "genre": genre,
                                        "style": style,
                                    }
                                )
                if genre_style_data:
                    await tx.run(
                        """
                        UNWIND $pairs AS pair
                        MERGE (g:Genre {name: pair.genre})
                        MERGE (s:Style {name: pair.style})
                        MERGE (s)-[:PART_OF]->(g)
                        """,
                        pairs=genre_style_data,
                    )

            await session.execute_write(batch_write)
        return nack_indices

    async def _process_releases_batch(self, messages: list[PendingMessage]) -> set[int]:
        """Process a batch of release records.

        Uses a single session for hash check + write to ensure atomicity.
        Returns indices of messages that should be nacked (e.g. missing 'id').
        """
        nack_indices: set[int] = set()
        all_releases = []
        for i, msg in enumerate(messages):
            release_id = msg.data.get("id")
            if not release_id:
                self.logger.warning(
                    "⚠️ Message missing required 'id' field, will nack",
                    data_keys=list(msg.data.keys()),
                )
                nack_indices.add(i)
                continue
            all_releases.append(msg)

        if not all_releases:
            return nack_indices

        # Single session for both hash check and write to avoid TOCTOU race
        async with self.driver.session(database="neo4j") as session:
            ids = [m.data.get("id") for m in all_releases]
            existing_hashes: dict[str, str] = {}
            if ids:
                result = await session.run(
                    "UNWIND $ids AS id OPTIONAL MATCH (r:Release {id: id}) RETURN id, r.sha256 AS hash",
                    ids=ids,
                )
                async for record in result:
                    if record["hash"]:
                        existing_hashes[str(record["id"])] = record["hash"]

            releases_to_process = []
            for msg in all_releases:
                rid = str(msg.data["id"])
                release_hash = msg.data.get("sha256")
                if existing_hashes.get(rid) != release_hash:
                    # Build a copy to avoid mutating the PendingMessage in case
                    # of re-enqueue on Neo4j failure
                    release_data = dict(msg.data)
                    release_data["format_names"] = [f["name"] for f in msg.data.get("formats", []) if isinstance(f, dict) and "name" in f]
                    # Canonical media projection (ADR 0007). `format_names` above stays
                    # the deprecated raw-name alias for one minor version; the rows here
                    # drive the Medium/MediaFamily nodes and ISSUED_ON edges, and are
                    # computed once per release so the write below is a flat UNWIND.
                    media_block = resolve_media_block(msg.data)
                    release_data["media_families"] = media_families(media_block)
                    release_data["media_rows"] = issued_on_rows(release_data["id"], media_block)
                    # Per-release metadata bag — populated only with non-null
                    # keys. The cypher uses `SET r += release.metadata`, which
                    # merges only the keys present, so absent fields don't wipe
                    # an existing value (in Neo4j, SET k = null deletes the
                    # property). Future per-release fields just add a key here.
                    raw_labels = msg.data.get("labels") or []
                    first_label = raw_labels[0] if isinstance(raw_labels, list) and raw_labels else {}
                    catno = first_label.get("catno") if isinstance(first_label, dict) else None
                    release_metadata: dict[str, Any] = {}
                    if catno:
                        release_metadata["catalog_number"] = catno
                    release_data["metadata"] = release_metadata
                    releases_to_process.append(release_data)

            if not releases_to_process:
                self.logger.debug("🔄 All releases in batch already up to date")
                return nack_indices

            async def batch_write(tx: Any) -> None:
                # Create/update all release nodes. `r += release.metadata`
                # merges the per-release metadata bag (catalog_number today,
                # potentially more fields tomorrow). Only keys present in the
                # bag are written — absent keys don't wipe existing values
                # that the per-user sync pipeline may have set.
                await tx.run(
                    """
                    UNWIND $releases AS release
                    MERGE (r:Release {id: release.id})
                    SET r.title = release.title,
                        r.year = release.year,
                        r.formats = release.format_names,
                        r.media_families = release.media_families,
                        r.sha256 = release.sha256,
                        r += release.metadata
                    """,
                    releases=releases_to_process,
                )

                # Prune managed edges the NEW version of each record no longer asserts
                # (discogsography-bd0u). MERGE-only writes never remove a dropped
                # association, so a release corrected from genres=["Rock"] to
                # genres=["Jazz"] kept (r)-[:IS]->(Rock) forever — permanently inflating
                # Rock.release_count and listing the release under a genre it no longer
                # has.
                await self._prune_stale_edges(
                    tx,
                    "Release",
                    releases_to_process,
                    rel_type="BY",
                    target_label="Artist",
                    target_key="id",
                    desired=lambda r: [a["id"] for a in (r.get("artists") or []) if a.get("id")],
                )
                await self._prune_stale_edges(
                    tx,
                    "Release",
                    releases_to_process,
                    rel_type="ON",
                    target_label="Label",
                    target_key="id",
                    desired=lambda r: [x["id"] for x in (r.get("labels") or []) if x.get("id")],
                )
                await self._prune_stale_edges(
                    tx,
                    "Release",
                    releases_to_process,
                    rel_type="DERIVED_FROM",
                    target_label="Master",
                    target_key="id",
                    desired=lambda r: [str(r["master_id"])] if r.get("master_id") else [],
                )
                await self._prune_stale_edges(
                    tx,
                    "Release",
                    releases_to_process,
                    rel_type="IS",
                    target_label="Genre",
                    target_key="name",
                    desired=lambda r: [g for g in (r.get("genres") or []) if g],
                )
                await self._prune_stale_edges(
                    tx,
                    "Release",
                    releases_to_process,
                    rel_type="IS",
                    target_label="Style",
                    target_key="name",
                    desired=lambda r: [st for st in (r.get("styles") or []) if st],
                )

                # Process artist relationships (Release)-[:BY]->(Artist)
                artist_data = []
                for release in releases_to_process:
                    if release.get("artists"):
                        for artist in release["artists"]:
                            if artist.get("id"):
                                artist_data.append(
                                    {
                                        "release_id": release["id"],
                                        "artist_id": artist["id"],
                                    }
                                )
                if artist_data:
                    await tx.run(
                        """
                        UNWIND $artists AS rel
                        MATCH (r:Release {id: rel.release_id})
                        MERGE (a:Artist {id: rel.artist_id})
                        MERGE (r)-[:BY]->(a)
                        """,
                        artists=artist_data,
                    )

                # Process label relationships (Release)-[:ON]->(Label)
                label_data = []
                for release in releases_to_process:
                    if release.get("labels"):
                        for label in release["labels"]:
                            if label.get("id"):
                                label_data.append(
                                    {
                                        "release_id": release["id"],
                                        "label_id": label["id"],
                                    }
                                )
                if label_data:
                    await tx.run(
                        """
                        UNWIND $labels AS rel
                        MATCH (r:Release {id: rel.release_id})
                        MERGE (l:Label {id: rel.label_id})
                        MERGE (r)-[:ON]->(l)
                        """,
                        labels=label_data,
                    )

                # Process master relationships (Release)-[:DERIVED_FROM]->(Master)
                master_data = []
                for release in releases_to_process:
                    master_id = release.get("master_id")
                    if master_id:
                        master_data.append(
                            {
                                "release_id": release["id"],
                                "master_id": str(master_id),
                            }
                        )
                if master_data:
                    await tx.run(
                        """
                        UNWIND $masters AS rel
                        MATCH (r:Release {id: rel.release_id})
                        MERGE (m:Master {id: rel.master_id})
                        MERGE (r)-[:DERIVED_FROM]->(m)
                        """,
                        masters=master_data,
                    )

                # Process genre relationships
                genre_data = []
                for release in releases_to_process:
                    if release.get("genres"):
                        for genre in release["genres"]:
                            if genre:
                                genre_data.append(
                                    {
                                        "release_id": release["id"],
                                        "genre": genre,
                                    }
                                )
                if genre_data:
                    await tx.run(
                        """
                        UNWIND $genres AS rel
                        MATCH (r:Release {id: rel.release_id})
                        MERGE (g:Genre {name: rel.genre})
                        MERGE (r)-[:IS]->(g)
                        """,
                        genres=genre_data,
                    )

                # Process style relationships
                style_data = []
                for release in releases_to_process:
                    if release.get("styles"):
                        for style in release["styles"]:
                            if style:
                                style_data.append(
                                    {
                                        "release_id": release["id"],
                                        "style": style,
                                    }
                                )
                if style_data:
                    await tx.run(
                        """
                        UNWIND $styles AS rel
                        MATCH (r:Release {id: rel.release_id})
                        MERGE (s:Style {name: rel.style})
                        MERGE (r)-[:IS]->(s)
                        """,
                        styles=style_data,
                    )

                # Connect styles to genres. PART_OF asserts a style belongs to a genre,
                # which is only unambiguous when the record carries a single genre — a
                # multi-genre record cartesian-linking every style to every genre would
                # create false Style-[:PART_OF]->Genre edges (discogsography-sy5k).
                genre_style_data = []
                for release in releases_to_process:
                    genres = release.get("genres", [])
                    styles = release.get("styles", [])
                    if len(genres) == 1 and genres[0]:
                        genre = genres[0]
                        for style in styles:
                            if style:
                                genre_style_data.append(
                                    {
                                        "genre": genre,
                                        "style": style,
                                    }
                                )
                if genre_style_data:
                    await tx.run(
                        """
                        UNWIND $pairs AS pair
                        MERGE (g:Genre {name: pair.genre})
                        MERGE (s:Style {name: pair.style})
                        MERGE (s)-[:PART_OF]->(g)
                        """,
                        pairs=genre_style_data,
                    )

                # Project the canonical media block onto Medium/MediaFamily nodes and
                # ISSUED_ON edges (ADR 0007), with the same statements the single-record
                # path runs. ISSUED_ON carries a `source`, so its prune is scoped to this
                # provider's edges and cannot use _prune_stale_edges; it runs for every
                # release in the batch, including those asserting no media — the empty
                # keep-list is the "all media removed" case.
                await tx.run(
                    PRUNE_ISSUED_ON_CYPHER,
                    records=[prune_record(release["id"], release["media_rows"]) for release in releases_to_process],
                    source=MEDIA_SOURCE,
                )
                media_rows = [row for release in releases_to_process for row in release["media_rows"]]
                if media_rows:
                    await tx.run(MERGE_MEDIA_CYPHER, rows=media_rows, source=MEDIA_SOURCE)

                # Process credits (extraartists) — Person nodes and CREDITED_ON relationships
                credit_data = []
                artist_credit_data = []
                for release in releases_to_process:
                    if release.get("extraartists"):
                        for credit in release["extraartists"]:
                            name = credit.get("name")
                            role = credit.get("role", "")
                            if name and role:
                                category = categorize_role(role)
                                entry: dict[str, Any] = {
                                    "name": name,
                                    "role": role,
                                    "category": category,
                                    "release_id": release["id"],
                                }
                                credit_data.append(entry)
                                artist_id = credit.get("id")
                                if artist_id:
                                    artist_credit_data.append(
                                        {
                                            "name": name,
                                            "artist_id": artist_id,
                                        }
                                    )
                if credit_data:
                    await tx.run(
                        """
                        UNWIND $credits AS credit
                        MATCH (r:Release {id: credit.release_id})
                        MERGE (p:Person {name: credit.name})
                        MERGE (p)-[c:CREDITED_ON {role: credit.role}]->(r)
                        SET c.category = credit.category
                        """,
                        credits=credit_data,
                    )
                if artist_credit_data:
                    await tx.run(
                        """
                        UNWIND $credits AS credit
                        MATCH (p:Person {name: credit.name})
                        MATCH (a:Artist {id: credit.artist_id})
                        MERGE (p)-[:SAME_AS]->(a)
                        """,
                        credits=artist_credit_data,
                    )

            await session.execute_write(batch_write)
        return nack_indices
