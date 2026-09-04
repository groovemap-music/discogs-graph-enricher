"""Legacy `formats`-only fallback preserves quantity (gm-discogs-graph-enricher-a89.1).

`resolve_media_block` derives a media block for a pre-cutover record that carries only
the raw Discogs `formats` list. When that list's entries are objects, the fallback must
hand them to `common.media.map_discogs_formats` — the same mapper a post-cutover
producer runs — rather than flattening them to bare names first, because the flattened
form has nowhere to carry a format entry's `qty`. A 2xLP is the representative case: its
`formats` entry carries `"qty": "2"`, and that quantity must survive onto the
`ISSUED_ON` edge. Malformed `formats` (non-object entries) has no such structure to
preserve, so the name-only derivation remains for that shape.

Kept as its own module (rather than appended to `tests/test_media_projection.py`) to
avoid colliding with concurrent edits to that file's replay harness.
"""

from __future__ import annotations

from typing import Any

from common.media import map_discogs_formats

from graphinator.media_projection import issued_on_rows, resolve_media_block


def test_object_formats_fallback_preserves_qty_for_a_2xlp() -> None:
    """A pre-cutover record with a 2xLP `formats` entry keeps qty=2 through the fallback."""
    record: dict[str, Any] = {
        "id": "R1",
        "title": "2xLP Legacy Event",
        "sha256": "h",
        "formats": [{"name": "Vinyl", "qty": "2", "descriptions": ["LP", "Album"]}],
    }

    block = resolve_media_block(record)
    rows = issued_on_rows(record["id"], block)

    assert [row["medium"] for row in rows] == ["vinyl_12"]
    (row,) = rows
    assert row["qty"] == 2


def test_object_formats_fallback_delegates_to_map_discogs_formats() -> None:
    """The fallback over object-shaped `formats` is the shared mapper, not a reimplementation.

    Asserted by equality with calling `map_discogs_formats` directly on the same list,
    so a future change to the mapper's behaviour cannot silently diverge from this path.
    """
    formats = [
        {"name": "Vinyl", "qty": "2", "text": "Blue Vinyl", "descriptions": ["LP", "Gatefold"]},
        {"name": "CD", "qty": "1", "descriptions": ["Mixed"]},
    ]
    record: dict[str, Any] = {"id": "R2", "title": "Box", "sha256": "h", "formats": formats}

    assert resolve_media_block(record) == map_discogs_formats(formats)


def test_malformed_formats_still_uses_the_name_only_fallback() -> None:
    """A `formats` list with a non-object entry cannot be handed to `map_discogs_formats`.

    Its qty is lost — there is no structure left to carry it — but the medium is still
    recovered from the names that are readable, matching the pre-existing malformed-input
    behaviour.
    """
    record: dict[str, Any] = {
        "id": "R3",
        "title": "Ragged Legacy Event",
        "sha256": "h",
        "formats": ["not-a-mapping", {"name": "Vinyl", "qty": "2", "descriptions": ["LP"]}],
    }

    block = resolve_media_block(record)
    rows = issued_on_rows(record["id"], block)

    assert [row["medium"] for row in rows] == ["vinyl_12"]
    (row,) = rows
    assert row["qty"] == 1


def test_non_list_formats_still_uses_the_name_only_fallback() -> None:
    """`formats` that is not even a list is malformed input, not an empty object list."""
    record: dict[str, Any] = {"id": "R4", "title": "Odd Shape", "sha256": "h", "formats": "Vinyl"}

    block = resolve_media_block(record)

    assert block == map_discogs_formats([])
    assert issued_on_rows(record["id"], block) == []
