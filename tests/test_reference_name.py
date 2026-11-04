"""The name a release reference carries onto the stub node it creates."""

from __future__ import annotations

import pytest

from graphinator.reference_name import reference_name
from graphinator.stub_name_backfill import parse_line, read_checkpoint


@pytest.mark.parametrize(
    ("reference", "expected"),
    [
        ({"id": "1", "name": " Warp "}, "Warp"),
        ({"id": "1", "name": "   "}, None),
        ({"id": "1", "name": 5}, None),
        ({"id": "1"}, None),
    ],
)
def test_reference_name(reference: dict[str, object], expected: str | None) -> None:
    assert reference_name(reference) == expected


def test_parse_line_accepts_only_named_artist_and_label_rows() -> None:
    assert parse_line('{"kind": "Label", "id": 7, "name": " X "}') == ("Label", {"id": "7", "name": "X"})
    assert parse_line('{"kind": "Master", "id": "1", "name": "X"}') is None
    assert parse_line('{"kind": "Label", "id": "1", "name": ""}') is None
    assert parse_line("garbage") is None
    assert parse_line("   ") is None


def test_read_checkpoint(tmp_path: object) -> None:
    from pathlib import Path

    base = Path(str(tmp_path))
    assert read_checkpoint(None) == 0
    assert read_checkpoint(base / "missing") == 0
    (base / "c").write_text("12\n")
    assert read_checkpoint(base / "c") == 12
    (base / "c").write_text("junk")
    assert read_checkpoint(base / "c") == 0


def test_parse_line_keeps_quotes_backslashes_and_non_ascii_from_the_psql_export() -> None:
    # Verbatim output of `psql -At -f scripts/stub-name-export.sql` for such a name.
    line = '{"id": "11358", "kind": "Label", "name": "RCA \\"Victor\\" \\\\ Ünï 日本"}'
    assert parse_line(line) == ("Label", {"id": "11358", "name": 'RCA "Victor" \\ Ünï 日本'})
