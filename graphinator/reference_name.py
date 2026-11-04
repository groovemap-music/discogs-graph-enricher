"""The display name a release carries for an Artist or Label it references.

A release lists its artists and labels as ``{"id": ..., "name": ...}``. When the referenced
entity has no record of its own in the dump, the release is the only place its name exists,
so the projection copies it onto the node it creates. Discogs does not carry a title for the
release's master reference (``master_id`` is a bare id), so a Master stub has no name to take.
"""

from __future__ import annotations

from typing import Any


def reference_name(reference: dict[str, Any]) -> str | None:
    """Return the reference's trimmed name, or None when it carries none."""
    name = reference.get("name")
    if not isinstance(name, str):
        return None
    name = name.strip()
    return name or None
