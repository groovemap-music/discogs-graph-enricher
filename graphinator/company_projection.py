"""Projection of the canonical companies block and the release country (ADR 0011).

Every releases event from a post-cutover producer carries a ``companies`` block naming
who made the physical article — the pressing plant, the room that cut the lacquer, the
mastering house, the distributor, the rights holders — and keeps the release's raw
``country`` string. This module turns that block into the parameter rows for the two
Cypher statements below, which both the single-record path
(:mod:`graphinator.entity_projection`) and the batched path
(:mod:`graphinator.batch_projection`) execute verbatim. Sharing the statements — not just
the shapes — is what keeps the two write paths from drifting apart, exactly as
:mod:`graphinator.media_projection` does for the media block.

The graph model is::

    (:Company {id, name})                                        unique on id
    (:Release)-[:CREDITED_TO {role, role_category, source}]->(:Company)

The edge follows the two precedents ADR 0011 names: the typed ``role`` property from
``(:Person)-[:CREDITED_ON {role}]->(:Release)``, and the ``source`` property from
``(:Release)-[:ISSUED_ON {qty, source}]->(:Medium)``. One edge per
(release, company, role, source), so a company credited twice on the same release under
two roles is two edges to one node, and a credit a second catalog asserts is
distinguishable from a Discogs one without a second relationship type.

``Release.country`` is the release's country as received, written as an additive property
in both write paths and backed by the ``release_country`` range index.
"""

from __future__ import annotations

from typing import Any


# Provenance stamped on every CREDITED_TO edge this service writes. The MusicBrainz
# enricher may credit the same Company nodes from its own catalog, so every prune here is
# scoped to this provider's edges.
COMPANY_SOURCE = "discogs"

# The role category the vendored vocabulary reserves for a role it does not recognise.
# The producer already writes it for an unmapped role; it is repeated here only so an
# entry that somehow arrives without a category still gets a non-null edge property —
# `SET e.role_category = null` would delete the property rather than record the gap.
UNMAPPED_ROLE_CATEGORY = "other"

# Prefix for the derived id of a company the source did not give a Discogs id.
DERIVED_ID_PREFIX = "name:"

# Remove the Discogs CREDITED_TO edges this release's NEW version no longer asserts,
# before the additive MERGE below re-creates the current set. Discogs records are mutable:
# a release whose pressing plant is corrected gets a new sha256, passes the hash gate,
# MERGEs the corrected edge — and would keep the superseded one forever, so a
# plant-centric query would list the release under a plant that never pressed it. Mirrors
# the ISSUED_ON prune.
#
# The keep-list holds [company id, role] PAIRS, not bare company ids, because the edge is
# keyed on the role: a company credited under two roles has two edges, and dropping one of
# those roles upstream has to delete exactly that one edge and leave the other standing.
#
# `e.source` scopes the delete to this provider's edges: a MusicBrainz-sourced credit on
# the same release is another producer's assertion. A release whose new version asserts an
# EMPTY companies block is included too, with an empty keep-list — that is precisely the
# "every company credit was removed" case. A release carrying no canonical block at all is
# not included; see `resolve_companies_block`.
PRUNE_CREDITED_TO_CYPHER = """
UNWIND $records AS record
MATCH (r:Release {id: record.release_id})-[e:CREDITED_TO]->(co:Company)
WHERE e.source = $source AND NOT [co.id, e.role] IN record.keep
DELETE e
"""

# MERGE is keyed on the company id alone, so re-processing the same release rewrites the
# same nodes and the same edges rather than duplicating them.
#
# `source` is part of the CREDITED_TO merge pattern, not a property SET afterwards, for
# the same reason it is on ISSUED_ON: Company nodes are shared across catalogs, so merging
# on (role) alone would match whichever provider's edge already existed and overwrite the
# other catalog's assertion. Merging on (role, source) keeps each provider writing only
# its own edge, and makes the prune above exact — the edges it deletes are the ones this
# statement would otherwise have to re-create.
MERGE_COMPANY_CYPHER = """
UNWIND $companies AS c
MATCH (r:Release {id: c.release_id})
MERGE (co:Company {id: c.id})
SET co.name = c.name
MERGE (r)-[e:CREDITED_TO {role: c.role, source: $source}]->(co)
SET e.role_category = c.role_category
"""


def resolve_companies_block(record: dict[str, Any]) -> dict[str, Any] | None:
    """Return the record's canonical companies block, or ``None`` when it carries none.

    Unlike the media block, there is no best-effort derivation from the raw Discogs data.
    The producer's mapping of raw ``entity_type_name`` strings onto the closed role
    vocabulary is fixed by conformance fixtures that a reference implementation also has
    to satisfy; re-deriving it here would make this service a second, unverified
    implementation of those rules. So a pre-cutover record contributes no company rows.

    That is also why ``None`` is distinct from an empty block. A pre-cutover record whose
    ``companies`` key still holds the RAW Discogs list — a list, not an object — is silent
    about company credits, not asserting that there are none, and silence must not delete
    the credits a post-cutover event already wrote. An empty canonical block is the
    opposite: an explicit assertion that this release has no company credits, which the
    prune acts on.
    """
    companies = record.get("companies")
    return companies if isinstance(companies, dict) else None


def credited_to_rows(release_id: Any, block: dict[str, Any]) -> list[dict[str, Any]]:
    """Return one MERGE_COMPANY_CYPHER row per distinct (company, role) on the release.

    CREDITED_TO is keyed on (release, company, role, source), so the same entry repeated
    in the source is one edge. Source order is preserved so the rows read the way the
    release does. An entry with no usable identity — no Discogs id and no name — or no
    role yields no row and therefore no edge, rather than a node keyed on nothing.
    """
    rows: dict[tuple[str, str], dict[str, Any]] = {}
    for item in block.get("items") or []:
        if not isinstance(item, dict):
            continue
        role = _text(item.get("role"))
        company_id = company_identity(item)
        if role is None or company_id is None:
            continue
        key = (company_id, role)
        if key in rows:
            continue
        rows[key] = {
            "release_id": release_id,
            "id": company_id,
            "name": _text(item.get("name")) or company_id,
            "role": role,
            "role_category": _text(item.get("role_category")) or UNMAPPED_ROLE_CATEGORY,
        }
    return list(rows.values())


def company_identity(item: dict[str, Any]) -> str | None:
    """Return the ``Company.id`` for one companies entry, or ``None`` when it has none.

    A Discogs id is the identity whenever the source supplies one, stringified because the
    dump states it as element text while the API states it as a number and the uniqueness
    constraint must see one value for both. Discogs uses ``0`` for "no label", which is not
    an id, so only a whole number of at least one counts.

    An entry without one — common for pressing plants and cutting rooms that have no
    Discogs label page — is keyed on ``name:`` followed by its normalized name, so
    re-processing the same release reaches the same node instead of the constraint seeing
    a new one every time. The normalization is deliberately minimal: case folded and inner
    whitespace collapsed, with punctuation left alone. Two spellings that differ by a comma
    stay two nodes, which a later reconciliation can merge; two different companies folded
    onto one id could not be taken apart again.
    """
    discogs_id = _discogs_id(item.get("discogs_id"))
    if discogs_id is not None:
        return discogs_id
    name = _text(item.get("name"))
    return None if name is None else DERIVED_ID_PREFIX + " ".join(name.split()).casefold()


def credit_prune_record(release_id: Any, rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Return the PRUNE_CREDITED_TO_CYPHER record for one release."""
    return {"release_id": release_id, "keep": [[row["id"], row["role"]] for row in rows]}


def release_country(record: dict[str, Any]) -> str | None:
    """Return the release's country as received, or ``None`` when it has none.

    A blank or non-string value is ``None`` rather than an empty string, so the property
    is absent instead of holding a value the country facet would have to filter out.
    Writing ``None`` removes any country a previous version of the record set, which is
    the correct reading of a release whose country was cleared upstream.
    """
    return _text(record.get("country"))


def _discogs_id(value: Any) -> str | None:
    """Return a positive whole Discogs id as a string, or ``None`` when it is unusable."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return str(value) if value >= 1 else None
    if isinstance(value, str):
        text = value.strip()
        return text if text.isdigit() and int(text) >= 1 else None
    return None


def _text(value: Any) -> str | None:
    """Return a trimmed non-empty string, or ``None`` for anything else."""
    if not isinstance(value, str):
        return None
    text = value.strip()
    return text or None
