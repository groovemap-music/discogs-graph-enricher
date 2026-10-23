"""The Discogs "no master" sentinel, shared by every projection path.

Discogs writes ``<master_id>0</master_id>`` for every release that has no master.
``discogs-ingestion`` passes it through verbatim, so it reaches the enricher as the
string ``"0"`` — which is truthy. Treating that as a real master id MERGEs one fake
``(:Master {id: "0"})`` that every masterless release then points at, a single node
holding millions of ``DERIVED_FROM`` edges.

Both write paths (:mod:`graphinator.entity_projection` and :mod:`graphinator.batch_projection`)
route every release-to-master decision through :func:`usable_master_id` so they cannot
disagree. The rule matches ``discogs-sql-loader``'s ``_entity_id``: a missing, blank or
``0`` reference — as the string ``"0"`` or the number ``0`` — is no master.
"""

from __future__ import annotations

from typing import Any


NO_MASTER_ID = "0"
"""The text of Discogs' "no entity" id."""


def usable_master_id(value: Any) -> str | None:
    """Return the master id as text, or None when the release has no master.

    None, booleans, blanks, ``0`` and ``"0"`` (surrounding whitespace ignored) all mean
    "no master". Anything else that is not a string or an int is rejected too.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        text = str(value)
    elif isinstance(value, str):
        text = value.strip()
    else:
        return None
    return None if not text or text == NO_MASTER_ID else text
