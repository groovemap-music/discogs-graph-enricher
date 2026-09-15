"""Single-record Neo4j projections for normalized Discogs entities."""

from typing import Any

from common.credit_roles import categorize_role

from graphinator.company_projection import (
    COMPANY_SOURCE,
    MERGE_COMPANY_CYPHER,
    PRUNE_CREDITED_TO_CYPHER,
    credit_prune_record,
    credited_to_rows,
    release_country,
    resolve_companies_block,
)
from graphinator.media_projection import (
    MEDIA_SOURCE,
    MERGE_MEDIA_CYPHER,
    PRUNE_ISSUED_ON_CYPHER,
    issued_on_rows,
    media_families,
    prune_record,
    resolve_media_block,
)


async def process_artist(tx: Any, record: dict[str, Any]) -> bool:
    """Project an artist within one transaction, returning whether it changed."""
    existing_result = await tx.run(
        "MATCH (a:Artist {id: $id}) RETURN a.sha256 AS hash",
        id=record["id"],
    )
    existing_record = await existing_result.single()
    if existing_record and existing_record["hash"] == record["sha256"]:
        return False

    resources = f"https://api.discogs.com/artists/{record['id']}"
    releases = f"{resources}/releases"
    await tx.run(
        "MERGE (a:Artist {id: $id}) "
        "ON CREATE SET a.name = $name, a.resource_url = $resource_url, a.releases_url = $releases_url, a.sha256 = $sha256 "
        "ON MATCH SET a.name = $name, a.resource_url = $resource_url, a.releases_url = $releases_url, a.sha256 = $sha256",
        id=record["id"],
        name=record.get("name", "Unknown Artist"),
        resource_url=resources,
        releases_url=releases,
        sha256=record["sha256"],
    )

    members: list[dict[str, Any]] | None = record.get("members")
    if members:
        valid_members = [member for member in members if member.get("id")]
        if valid_members:
            await tx.run(
                "UNWIND $members AS member MATCH (a:Artist {id: $artist_id}) MERGE (m_a:Artist {id: member.id}) MERGE (m_a)-[:MEMBER_OF]->(a)",
                members=valid_members,
                artist_id=record["id"],
            )

    groups: list[dict[str, Any]] | None = record.get("groups")
    if groups:
        valid_groups = [group for group in groups if group.get("id")]
        if valid_groups:
            await tx.run(
                "UNWIND $groups AS group MATCH (a:Artist {id: $artist_id}) MERGE (g_a:Artist {id: group.id}) MERGE (a)-[:MEMBER_OF]->(g_a)",
                groups=valid_groups,
                artist_id=record["id"],
            )

    aliases: list[dict[str, Any]] | None = record.get("aliases")
    if aliases:
        valid_aliases = [alias for alias in aliases if alias.get("id")]
        if valid_aliases:
            await tx.run(
                "UNWIND $aliases AS alias MATCH (a:Artist {id: $artist_id}) MERGE (a_a:Artist {id: alias.id}) MERGE (a_a)-[:ALIAS_OF]->(a)",
                aliases=valid_aliases,
                artist_id=record["id"],
            )

    return True


async def process_label(tx: Any, record: dict[str, Any]) -> bool:
    """Project a label within one transaction, returning whether it changed."""
    existing_result = await tx.run("MATCH (l:Label {id: $id}) RETURN l.sha256 AS hash", id=record["id"])
    existing_record = await existing_result.single()
    if existing_record and existing_record["hash"] == record["sha256"]:
        return False

    await tx.run(
        "MERGE (l:Label {id: $id}) ON CREATE SET l.name = $name, l.sha256 = $sha256 ON MATCH SET l.name = $name, l.sha256 = $sha256",
        id=record["id"],
        name=record.get("name", "Unknown Label"),
        sha256=record["sha256"],
    )

    parent: dict[str, Any] | None = record.get("parentLabel")
    if parent and parent.get("id"):
        await tx.run(
            "MATCH (l:Label {id: $id}) MERGE (p_l:Label {id: $p_id}) MERGE (l)-[:SUBLABEL_OF]->(p_l)",
            id=record["id"],
            p_id=parent["id"],
        )

    sublabels: list[dict[str, Any]] | None = record.get("sublabels")
    if sublabels:
        valid_sublabels = [sublabel for sublabel in sublabels if sublabel.get("id")]
        if valid_sublabels:
            await tx.run(
                "UNWIND $sublabels AS sublabel MATCH (l:Label {id: $label_id}) MERGE (s_l:Label {id: sublabel.id}) MERGE (s_l)-[:SUBLABEL_OF]->(l)",
                sublabels=valid_sublabels,
                label_id=record["id"],
            )

    return True


async def _prune_stale_edges(
    tx: Any,
    label: str,
    node_id: Any,
    *,
    rel_type: str,
    target_label: str,
    target_key: str,
    keep: list[Any],
) -> None:
    """Delete managed edges that the normalized record no longer asserts."""
    await tx.run(
        f"MATCH (n:{label} {{id: $node_id}})-[rel:{rel_type}]->(t:{target_label}) WHERE NOT t.{target_key} IN $keep DELETE rel",
        node_id=node_id,
        keep=keep,
    )


async def process_master(tx: Any, record: dict[str, Any]) -> bool:
    """Project a master within one transaction, returning whether it changed."""
    existing_result = await tx.run(
        "MATCH (m:Master {id: $id}) RETURN m.sha256 AS hash",
        id=record["id"],
    )
    existing_record = await existing_result.single()
    if existing_record and existing_record["hash"] == record["sha256"]:
        return False

    await tx.run(
        "MERGE (m:Master {id: $id}) "
        "ON CREATE SET m.title = $title, m.year = $year, m.sha256 = $sha256 "
        "ON MATCH SET m.title = $title, m.year = $year, m.sha256 = $sha256",
        id=record["id"],
        title=record.get("title", "Unknown Master"),
        year=record.get("year"),
        sha256=record["sha256"],
    )

    await _prune_stale_edges(
        tx,
        "Master",
        record["id"],
        rel_type="BY",
        target_label="Artist",
        target_key="id",
        keep=[artist["id"] for artist in (record.get("artists") or []) if artist.get("id")],
    )
    await _prune_stale_edges(
        tx,
        "Master",
        record["id"],
        rel_type="IS",
        target_label="Genre",
        target_key="name",
        keep=[genre for genre in (record.get("genres") or []) if genre],
    )
    await _prune_stale_edges(
        tx,
        "Master",
        record["id"],
        rel_type="IS",
        target_label="Style",
        target_key="name",
        keep=[style for style in (record.get("styles") or []) if style],
    )

    artists: list[dict[str, Any]] | None = record.get("artists")
    if artists:
        valid_artists = [artist for artist in artists if artist.get("id")]
        if valid_artists:
            await tx.run(
                "UNWIND $artists AS artist MATCH (m:Master {id: $master_id}) MERGE (a_m:Artist {id: artist.id}) MERGE (m)-[:BY]->(a_m)",
                artists=valid_artists,
                master_id=record["id"],
            )

    genres: list[str] = record.get("genres", [])
    if genres:
        await tx.run(
            "UNWIND $genres AS genre MATCH (m:Master {id: $master_id}) MERGE (g:Genre {name: genre.name}) MERGE (m)-[:IS]->(g)",
            genres=[{"name": genre} for genre in genres],
            master_id=record["id"],
        )

    styles: list[str] = record.get("styles", [])
    if styles:
        await tx.run(
            "UNWIND $styles AS style MATCH (m:Master {id: $master_id}) MERGE (s:Style {name: style.name}) MERGE (m)-[:IS]->(s)",
            styles=[{"name": style} for style in styles],
            master_id=record["id"],
        )

    # PART_OF is unambiguous only when the source record carries one genre.
    if len(genres) == 1 and styles:
        await tx.run(
            "UNWIND $genre_style_pairs AS pair MERGE (g:Genre {name: pair.genre}) MERGE (s:Style {name: pair.style}) MERGE (s)-[:PART_OF]->(g)",
            genre_style_pairs=[{"genre": genres[0], "style": style} for style in styles],
        )

    return True


async def process_release(tx: Any, record: dict[str, Any]) -> bool:
    """Project a release within one transaction, returning whether it changed."""
    existing_result = await tx.run(
        "MATCH (r:Release {id: $id}) RETURN r.sha256 AS hash",
        id=record["id"],
    )
    existing_record = await existing_result.single()
    if existing_record and existing_record["hash"] == record["sha256"]:
        return False

    formats = [item["name"] for item in record.get("formats", []) if isinstance(item, dict) and "name" in item]
    media_block = resolve_media_block(record)
    await tx.run(
        "MERGE (r:Release {id: $id}) "
        "ON CREATE SET r.title = $title, r.year = $year, r.country = $country, r.formats = $formats, r.media_families = $media_families, r.sha256 = $sha256 "
        "ON MATCH SET r.title = $title, r.year = $year, r.country = $country, r.formats = $formats, r.media_families = $media_families, r.sha256 = $sha256",
        id=record["id"],
        title=record.get("title", "Unknown Release"),
        year=record.get("year"),
        country=release_country(record),
        formats=formats,
        media_families=media_families(media_block),
        sha256=record["sha256"],
    )

    await _prune_stale_edges(
        tx,
        "Release",
        record["id"],
        rel_type="BY",
        target_label="Artist",
        target_key="id",
        keep=[artist["id"] for artist in (record.get("artists") or []) if artist.get("id")],
    )
    await _prune_stale_edges(
        tx,
        "Release",
        record["id"],
        rel_type="ON",
        target_label="Label",
        target_key="id",
        keep=[label["id"] for label in (record.get("labels") or []) if label.get("id")],
    )
    await _prune_stale_edges(
        tx,
        "Release",
        record["id"],
        rel_type="DERIVED_FROM",
        target_label="Master",
        target_key="id",
        keep=[record["master_id"]] if record.get("master_id") else [],
    )
    await _prune_stale_edges(
        tx,
        "Release",
        record["id"],
        rel_type="IS",
        target_label="Genre",
        target_key="name",
        keep=[genre for genre in (record.get("genres") or []) if genre],
    )
    await _prune_stale_edges(
        tx,
        "Release",
        record["id"],
        rel_type="IS",
        target_label="Style",
        target_key="name",
        keep=[style for style in (record.get("styles") or []) if style],
    )

    media_rows = issued_on_rows(record["id"], media_block)
    await tx.run(
        PRUNE_ISSUED_ON_CYPHER,
        records=[prune_record(record["id"], media_rows)],
        source=MEDIA_SOURCE,
    )
    if media_rows:
        await tx.run(MERGE_MEDIA_CYPHER, rows=media_rows, source=MEDIA_SOURCE)

    # Project the canonical companies block onto Company nodes and CREDITED_TO edges
    # (ADR 0011), with the same statements the batched path runs. Both are skipped for a
    # record carrying no canonical block: that record is silent about company credits
    # rather than asserting it has none, and an empty-keep prune would delete the credits
    # a post-cutover event already wrote.
    companies_block = resolve_companies_block(record)
    if companies_block is not None:
        company_rows = credited_to_rows(record["id"], companies_block)
        await tx.run(
            PRUNE_CREDITED_TO_CYPHER,
            records=[credit_prune_record(record["id"], company_rows)],
            source=COMPANY_SOURCE,
        )
        if company_rows:
            await tx.run(MERGE_COMPANY_CYPHER, companies=company_rows, source=COMPANY_SOURCE)

    artists: list[dict[str, Any]] | None = record.get("artists")
    if artists:
        valid_artists = [artist for artist in artists if artist.get("id")]
        if valid_artists:
            await tx.run(
                "UNWIND $artists AS artist MATCH (r:Release {id: $release_id}) MERGE (a_r:Artist {id: artist.id}) MERGE (r)-[:BY]->(a_r)",
                artists=valid_artists,
                release_id=record["id"],
            )

    labels: list[dict[str, Any]] | None = record.get("labels")
    if labels:
        valid_labels = [label for label in labels if label.get("id")]
        if valid_labels:
            await tx.run(
                "UNWIND $labels AS label MATCH (r:Release {id: $release_id}) MERGE (l_r:Label {id: label.id}) MERGE (r)-[:ON]->(l_r)",
                labels=valid_labels,
                release_id=record["id"],
            )

    master_id = record.get("master_id")
    if master_id:
        await tx.run(
            "MATCH (r:Release {id: $id}) MERGE (m_r:Master {id: $m_id}) MERGE (r)-[:DERIVED_FROM]->(m_r)",
            id=record["id"],
            m_id=master_id,
        )

    genres: list[str] = record.get("genres", [])
    if genres:
        await tx.run(
            "UNWIND $genres AS genre MATCH (r:Release {id: $release_id}) MERGE (g:Genre {name: genre.name}) MERGE (r)-[:IS]->(g)",
            genres=[{"name": genre} for genre in genres],
            release_id=record["id"],
        )

    styles: list[str] = record.get("styles", [])
    if styles:
        await tx.run(
            "UNWIND $styles AS style MATCH (r:Release {id: $release_id}) MERGE (s:Style {name: style.name}) MERGE (r)-[:IS]->(s)",
            styles=[{"name": style} for style in styles],
            release_id=record["id"],
        )

    # PART_OF is unambiguous only when the source record carries one genre.
    if len(genres) == 1 and styles:
        await tx.run(
            "UNWIND $genre_style_pairs AS pair MERGE (g:Genre {name: pair.genre}) MERGE (s:Style {name: pair.style}) MERGE (s)-[:PART_OF]->(g)",
            genre_style_pairs=[{"genre": genres[0], "style": style} for style in styles],
        )

    extraartists: list[dict[str, Any]] | None = record.get("extraartists")
    if extraartists:
        credit_rows: list[dict[str, Any]] = []
        for credit in extraartists:
            name = credit.get("name")
            role = credit.get("role", "")
            if name and role:
                entry: dict[str, Any] = {
                    "name": name,
                    "role": role,
                    "category": categorize_role(role),
                    "release_id": record["id"],
                }
                if artist_id := credit.get("id"):
                    entry["artist_id"] = artist_id
                credit_rows.append(entry)
        if credit_rows:
            await tx.run(
                "UNWIND $credits AS credit "
                "MATCH (r:Release {id: credit.release_id}) "
                "MERGE (p:Person {name: credit.name}) "
                "MERGE (p)-[c:CREDITED_ON {role: credit.role}]->(r) "
                "SET c.category = credit.category",
                credits=credit_rows,
            )
            artist_credits = [credit for credit in credit_rows if credit.get("artist_id")]
            if artist_credits:
                await tx.run(
                    "UNWIND $credits AS credit "
                    "MATCH (p:Person {name: credit.name}) "
                    "MATCH (a:Artist {id: credit.artist_id}) "
                    "MERGE (p)-[:SAME_AS]->(a)",
                    credits=artist_credits,
                )

    return True
