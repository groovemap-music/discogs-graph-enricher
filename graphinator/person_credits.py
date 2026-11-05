"""Person credit projection and the per-category Person credit-count recompute.

A release's ``extraartists`` project onto ``(:Person {name})-[:CREDITED_ON {role,
category}]->(:Release)``. ``role`` is part of the merge pattern, so one person credited
under two roles on a release is two edges to that release; ``category`` is the shared
:func:`common.credit_roles.categorize_role` bucket of the role.

Post-import maintenance turns those edges into precomputed counts on every Person, the
property contract shared with the database-schema indexes and the catalog-api role
leaderboard:

- ``credit_count_<category>`` — for each category in
  :data:`common.credit_roles.ALL_CATEGORIES`, the number of DISTINCT releases the person
  is ``CREDITED_ON`` with that category.
- ``credit_count`` — the number of DISTINCT releases the person is ``CREDITED_ON`` at all.

Every property holds a value on every Person after every pass (an unchanged one is not
rewritten, see :func:`build_credit_count_page_cypher`), and a category the person no
longer has reads ``0`` — never an absent property. Holding the zero is what makes a
removed credit visible: an absent property would need a separate ``REMOVE`` for exactly
the people who lost a category, and a reader could not tell "lost it" from "never
recomputed". ``credit_count`` is not the sum of the per-category counts: a person who
both produced and engineered one release counts it once in the total and once in each
of the two categories.
"""

from __future__ import annotations

import hashlib
import re
from typing import Any

import structlog
from common.credit_roles import ALL_CATEGORIES, categorize_role
from common.person_names import strip_person_name


logger = structlog.get_logger(__name__)


# Category names are interpolated into Cypher as property names and variables (Cypher
# has no portable dynamic property SET), so refuse anything that is not a plain
# identifier rather than trust the shared vocabulary to stay one.
_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")
for _category in ALL_CATEGORIES:
    if not _IDENTIFIER.fullmatch(_category):  # pragma: no cover - guards the shared vocabulary
        msg = f"credit category {_category!r} is not a safe Cypher identifier"
        raise ValueError(msg)

CREDIT_COUNT_PROPERTY = "credit_count"

# ``Person.name`` is the node key, so it is backed by a unique RANGE index, and Neo4j
# refuses to index a value over 8,164 bytes of UTF-8 (measured on Neo4j 2026: the same
# for 1-, 2-, 3- and 4-byte characters; 8,165 is rejected). One such name would fail the
# whole write batch, be retried, and end with the release nacked and lost. The longest real
# name on lux is 4,159 characters, so real data fits; this bounds the pathological one.
PERSON_NAME_MAX_BYTES = 8000


def bound_person_name(name: str) -> str:
    """Return ``name``, or a deterministic bounded form when it is too long to index.

    A name within :data:`PERSON_NAME_MAX_BYTES` UTF-8 bytes is returned unchanged, so every
    existing Person keeps its identity. A longer one is cut on a character boundary and
    ends in a digest of the full name: the credit is kept, the same input always maps to
    the same Person (re-imports stay idempotent), and two different long names that share a
    prefix do not collapse into one Person.
    """
    encoded = name.encode("utf-8")
    if len(encoded) <= PERSON_NAME_MAX_BYTES:
        return name
    suffix = f"... [{hashlib.sha256(encoded).hexdigest()[:16]}]"
    keep = PERSON_NAME_MAX_BYTES - len(suffix.encode("utf-8"))
    return encoded[:keep].decode("utf-8", errors="ignore") + suffix


def credit_count_property(category: str) -> str:
    """Return the Person property holding the distinct-release count for ``category``."""
    return f"{CREDIT_COUNT_PROPERTY}_{category}"


# Every property the recompute writes, total first.
CREDIT_COUNT_PROPERTIES: tuple[str, ...] = (
    CREDIT_COUNT_PROPERTY,
    *(credit_count_property(category) for category in ALL_CATEGORIES),
)


def person_credit_rows(release_id: Any, extraartists: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    """Return one CREDITED_ON row per usable extraartists credit of a release.

    A credit without a name or a role is not projected. The name is stripped of surrounding
    padding first (:func:`common.person_names.strip_person_name`, shared with discogs-sql-loader
    so both stores key a person alike); a name that is all padding is skipped and counted.
    Known gap: U+2800 BRAILLE PATTERN BLANK (category So) is padding on one measured name
    and the shared rule does not strip it yet. ``artist_id`` is carried only when Discogs
    links the credit to an artist, for the ``SAME_AS`` edge. A name too long
    for the Person index is replaced by :func:`bound_person_name`'s bounded form.
    """
    rows: list[dict[str, Any]] = []
    bounded = 0
    padding_only = 0
    for credit in extraartists or []:
        name = credit.get("name")
        role = credit.get("role", "")
        # Strip FIRST and test the result: an all-padding name must not become a blank Person.
        if isinstance(name, str) and name and role and not (name := strip_person_name(name)):
            padding_only += 1
            continue
        if not (name and role):
            continue
        if isinstance(name, str) and (limited := bound_person_name(name)) != name:
            bounded += 1
            name = limited
        row: dict[str, Any] = {
            "name": name,
            "role": role,
            "category": categorize_role(role),
            "release_id": release_id,
        }
        if artist_id := credit.get("id"):
            row["artist_id"] = artist_id
        rows.append(row)
    if padding_only:
        # Counts only: the name is padding, so there is nothing to log but how many.
        logger.warning("⚠️ Credit with an all-padding Person name was skipped", release_id=release_id, skipped=padding_only)
    if bounded:
        # Counts only: a name this long is not safe to log.
        logger.warning("⚠️ Person name over the index limit was bounded", release_id=release_id, bounded=bounded, limit_bytes=PERSON_NAME_MAX_BYTES)
    return rows


def credited_on_prune_record(release_id: Any, rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Return the prune row for one release: the ``[name, role]`` pairs it still asserts."""
    return {"key": release_id, "keep": [[row["name"], row["role"]] for row in rows]}


# Delete the CREDITED_ON edges a release's NEW version no longer asserts. The writes are
# MERGE-only, so without this a credit Discogs removed — or a role it corrected — would
# survive every re-import and keep counting towards the person's credit_count. The pair
# matches the merge key (Person by name, edge by role), so a person kept under one role
# and dropped under another loses exactly the dropped edge. A release asserting no
# credits has an empty keep-list: that is the "every credit was removed" case. Only
# Discogs writes CREDITED_ON, so the prune needs no provider scope.
PRUNE_CREDITED_ON_CYPHER = """
UNWIND $records AS record
MATCH (:Release {id: record.key})<-[rel:CREDITED_ON]-(p:Person)
WHERE NOT [p.name, rel.role] IN record.keep
DELETE rel
"""


def build_credit_count_page_cypher() -> str:
    """Build the keyed-page recompute of Person credit counts.

    One statement handles one page: the ``$size`` Persons whose ``name`` sorts after
    ``$cursor``, in name order. It returns ``last`` (the greatest name handled, the next
    cursor) and ``processed`` so :mod:`graphinator.maintenance_paging` can checkpoint and
    resume after it. The statement is bounded by ``$size``, not by the size of the graph.

    Inside, the edges are grouped by release first, so each release contributes once to
    the total and once to each category it carries (the distinct-release semantics) while
    the working set stays one row per release of a single person rather than a DISTINCT
    set per category. ``OPTIONAL MATCH`` keeps a person whose credits were all pruned, so
    it is rewritten to zero instead of keeping stale counts. The per-person subquery has
    no ``RETURN``, so every page row survives it to feed the final aggregate.
    """
    category_sums = ",\n            ".join(
        f"sum(CASE WHEN '{category}' IN categories THEN 1 ELSE 0 END) AS count_{category}" for category in ALL_CATEGORIES
    )
    # Compare before writing, per property: an unchanged value is not SET, so a rerun over
    # unchanged data takes no write lock and updates none of the eight credit_count indexes.
    # IS NULL covers a property never written; a stale value (including a lost category that
    # must now read 0) differs and is rewritten.
    counted = [(CREDIT_COUNT_PROPERTY, "total"), *((credit_count_property(category), f"count_{category}") for category in ALL_CATEGORIES)]
    guarded_sets = "\n        ".join(
        f"FOREACH (_ IN CASE WHEN p.{prop} IS NULL OR p.{prop} <> {value} THEN [1] ELSE [] END | SET p.{prop} = {value})" for prop, value in counted
    )
    return f"""
    MATCH (p:Person)
    WHERE p.name > $cursor
    WITH p ORDER BY p.name LIMIT $size
    CALL {{
        WITH p
        OPTIONAL MATCH (p)-[c:CREDITED_ON]->(r:Release)
        WITH p, r, collect(DISTINCT c.category) AS categories
        WITH p,
            count(r) AS total,
            {category_sums}
        {guarded_sets}
    }}
    RETURN max(p.name) AS last, count(p) AS processed
    """
