"""Projection of the canonical media block onto the Neo4j graph (ADR 0007).

Every releases event carries an additive ``media`` block whose ``items`` name canonical
medium ids from the vendored taxonomy. This module turns that block into the parameter
rows for the two Cypher statements below, which both the single-record path
(:mod:`graphinator.graphinator`) and the batched path
(:mod:`graphinator.batch_processor`) execute verbatim. Sharing the statements — not just
the shapes — is what keeps the two write paths from drifting apart.

The graph model is::

    (:Medium {id, family, label})            unique on id
    (:MediaFamily {name})                    unique on name
    (:Medium)-[:IN_FAMILY]->(:MediaFamily)
    (:Release)-[:ISSUED_ON {qty, source}]->(:Medium)   one edge per (release, medium, source)

``Release.media_families`` mirrors ``media.families`` as a list property so a
family filter needs no traversal. ``Release.formats`` keeps being written unchanged:
it is a deprecated alias retained for one minor version while consumers migrate.
"""

from __future__ import annotations

from typing import Any

import structlog
from common.media import flatten_descriptions, legacy_format_names_to_media, map_discogs_formats, medium_label


logger = structlog.get_logger(__name__)

# Provenance stamped on every ISSUED_ON edge this service writes. The MusicBrainz
# enricher stamps "musicbrainz" on its own edges over the same Medium nodes, so every
# read and every prune here is scoped to this provider's edges.
MEDIA_SOURCE = "discogs"

# Remove the Discogs ISSUED_ON edges this release's NEW version no longer asserts, before
# the additive MERGE below re-creates the current set. Discogs records are mutable: a
# release corrected from Vinyl to CD gets a new sha256, passes the hash gate, MERGEs the
# CD edge — and would keep the vinyl one forever, so a medium-centric query would list the
# release under a medium it was never issued on. Mirrors the Genre/Style prune
# (discogsography-bd0u). `rel.source` scopes the delete to this provider's edges: a
# MusicBrainz-sourced edge on the same release is another producer's assertion.
# A release whose new version asserts NO media is included too, with an empty keep-list —
# that is precisely the "every medium was removed" case.
PRUNE_ISSUED_ON_CYPHER = """
UNWIND $records AS record
MATCH (r:Release {id: record.release_id})-[rel:ISSUED_ON]->(m:Medium)
WHERE rel.source = $source AND NOT m.id IN record.keep
DELETE rel
"""

# MERGE is keyed on canonical ids alone, so re-processing the same release rewrites the
# same nodes and the same edges rather than duplicating them.
#
# `source` is part of the ISSUED_ON merge pattern, not a property SET afterwards. Medium
# nodes are shared across catalogs, so a release both this service and the MusicBrainz
# enricher know about has one edge per provider to the same node. Merging on the medium
# alone would match whichever edge already existed and overwrite the other catalog's
# assertion; merging on (medium, source) keeps each provider writing only its own edge.
# It also makes the prune above exact — the edges it deletes are the ones this statement
# would otherwise have to re-create.
MERGE_MEDIA_CYPHER = """
UNWIND $rows AS row
MATCH (r:Release {id: row.release_id})
MERGE (mf:MediaFamily {name: row.family})
MERGE (m:Medium {id: row.medium})
ON CREATE SET m.family = row.family, m.label = row.label
ON MATCH SET m.family = row.family, m.label = row.label
MERGE (m)-[:IN_FAMILY]->(mf)
MERGE (r)-[e:ISSUED_ON {source: $source}]->(m)
SET e.qty = row.qty
"""


def resolve_media_block(record: dict[str, Any]) -> dict[str, Any]:
    """Return the record's canonical media block, deriving one when it is absent.

    Post-cutover producers put the block on the event and it is covered by the record
    hash, so it is used as-is. A pre-cutover producer sends only the raw ``formats``
    list; deriving a best-effort block from it keeps the graph from being half-populated
    during the rollout — the alternative is releases with a ``formats`` property and no
    Medium edges at all.

    When every ``formats`` entry is an object, the raw list is handed to
    ``common.media.map_discogs_formats`` — the same shared mapper a post-cutover producer
    runs — rather than re-derived from flattened names. That is what keeps a quantity
    like a 2xLP's ``qty`` (and other per-entry structure: ``text``, nested
    ``descriptions``) alive through the fallback instead of being discarded. The
    enricher stays a consumer of the taxonomy, not a second implementation of the
    producer's mapping rules: this is the same function, called on data closer to what
    the producer received.

    Some pre-cutover records have already lost that structure — ``formats`` flattened to
    a bare list of names, or the odd non-object entry — and there the only recoverable
    signal is the names themselves, so the name-only fallback is kept for that malformed
    shape.
    """
    media = record.get("media")
    if isinstance(media, dict):
        return media

    formats = record.get("formats")
    if isinstance(formats, list) and all(isinstance(entry, dict) for entry in formats):
        return map_discogs_formats(formats)

    names: list[str] = []
    for entry in formats or []:
        if not isinstance(entry, dict):
            continue
        name = entry.get("name")
        if isinstance(name, str) and name:
            names.append(name)
        names.extend(flatten_descriptions(entry.get("descriptions")))
    return legacy_format_names_to_media(names)


def media_families(block: dict[str, Any]) -> list[str]:
    """Return the family ids for ``Release.media_families``."""
    families = block.get("families")
    return [family for family in families if isinstance(family, str)] if isinstance(families, list) else []


def issued_on_rows(release_id: Any, block: dict[str, Any]) -> list[dict[str, Any]]:
    """Return one MERGE_MEDIA_CYPHER row per distinct medium on the release.

    ISSUED_ON is keyed on (release, medium, source), so two entries that resolve to the
    same canonical medium — a 2xLP split across two Discogs format entries, say — are one
    edge whose ``qty`` is their sum. Source order is preserved so the rows read the way
    the release does. A release whose media are all unmapped yields no rows and therefore
    no edges.
    """
    rows: dict[str, dict[str, Any]] = {}
    for item in block.get("items") or []:
        if not isinstance(item, dict):
            continue
        medium = item.get("medium")
        family = item.get("family")
        if not isinstance(medium, str) or not medium or not isinstance(family, str) or not family:
            continue
        existing = rows.get(medium)
        if existing is not None:
            existing["qty"] += _quantity(item)
            continue
        rows[medium] = {
            "release_id": release_id,
            "medium": medium,
            "family": family,
            "label": _label(medium),
            "qty": _quantity(item),
        }
    return list(rows.values())


def prune_record(release_id: Any, rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Return the PRUNE_ISSUED_ON_CYPHER record for one release."""
    return {"release_id": release_id, "keep": [row["medium"] for row in rows]}


def _label(medium: str) -> str:
    """Return the taxonomy's label for a medium id, falling back to the id itself.

    A producer running a newer taxonomy version can name a medium this runtime's
    vendored vocabulary does not carry yet. Labelling that node with its own id keeps
    the release's media in the graph, where a later re-process corrects the label, rather
    than failing the whole record over a cosmetic property.
    """
    try:
        return medium_label(medium)
    except KeyError:
        logger.warning("⚠️ Unknown medium id in canonical media block; labelling it with its id", medium=medium)
        return medium


def _quantity(item: dict[str, Any]) -> int:
    """Return an item's unit count, defaulting to one when it is absent or unusable."""
    qty = item.get("qty")
    if isinstance(qty, bool) or not isinstance(qty, int) or qty < 1:
        return 1
    return qty
