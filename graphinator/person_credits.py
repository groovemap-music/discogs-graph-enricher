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

Every property is written on every Person on every pass, and a category the person no
longer has reads ``0`` — never an absent property. Writing the zero is what makes a
removed credit visible: an absent property would need a separate ``REMOVE`` for exactly
the people who lost a category, and a reader could not tell "lost it" from "never
recomputed". ``credit_count`` is not the sum of the per-category counts: a person who
both produced and engineered one release counts it once in the total and once in each
of the two categories.
"""

from __future__ import annotations

import re
from typing import Any

from common.credit_roles import ALL_CATEGORIES, categorize_role


# Category names are interpolated into Cypher as property names and variables (Cypher
# has no portable dynamic property SET), so refuse anything that is not a plain
# identifier rather than trust the shared vocabulary to stay one.
_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")
for _category in ALL_CATEGORIES:
    if not _IDENTIFIER.fullmatch(_category):  # pragma: no cover - guards the shared vocabulary
        msg = f"credit category {_category!r} is not a safe Cypher identifier"
        raise ValueError(msg)

CREDIT_COUNT_PROPERTY = "credit_count"


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

    A credit without a name or a role is not projected. ``artist_id`` is carried only
    when Discogs links the credit to an artist, for the ``SAME_AS`` edge.
    """
    rows: list[dict[str, Any]] = []
    for credit in extraartists or []:
        name = credit.get("name")
        role = credit.get("role", "")
        if not (name and role):
            continue
        row: dict[str, Any] = {
            "name": name,
            "role": role,
            "category": categorize_role(role),
            "release_id": release_id,
        }
        if artist_id := credit.get("id"):
            row["artist_id"] = artist_id
        rows.append(row)
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


def build_credit_count_cypher(batch_size: int | None) -> str:
    """Build the batched recompute of every Person's credit counts.

    The driving ``MATCH (p:Person)`` sits OUTSIDE the transactional subquery and the
    person is re-imported with ``WITH``, so ``IN TRANSACTIONS OF n ROWS`` commits one
    inner transaction per ``n`` people (see ``compute_genre_style_stats``). Inside, the
    edges are grouped by release first, so each release contributes once to the total
    and once to each category it carries — the distinct-release semantics — while the
    working set stays one row per release of a single person rather than a DISTINCT set
    per category. ``OPTIONAL MATCH`` keeps a person whose credits were all pruned in
    the batch, so it is rewritten to zero instead of keeping stale counts.
    """
    category_sums = ",\n            ".join(
        f"sum(CASE WHEN '{category}' IN categories THEN 1 ELSE 0 END) AS count_{category}" for category in ALL_CATEGORIES
    )
    category_sets = ",\n            ".join(f"p.{credit_count_property(category)} = count_{category}" for category in ALL_CATEGORIES)
    return f"""
    MATCH (p:Person)
    CALL {{
        WITH p
        OPTIONAL MATCH (p)-[c:CREDITED_ON]->(r:Release)
        WITH p, r, collect(DISTINCT c.category) AS categories
        WITH p,
            count(r) AS total,
            {category_sums}
        SET p.{CREDIT_COUNT_PROPERTY} = total,
            {category_sets}
    }} IN TRANSACTIONS OF {batch_size} ROWS
    """
