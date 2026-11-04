"""Person names are bounded to what the Person.name index can hold (see person_credits)."""

from __future__ import annotations

import pytest

from graphinator.person_credits import PERSON_NAME_MAX_BYTES, bound_person_name, person_credit_rows


def test_a_name_within_the_limit_keeps_its_identity() -> None:
    for name in ("Flood", "a" * PERSON_NAME_MAX_BYTES, "é" * (PERSON_NAME_MAX_BYTES // 2), "😀" * (PERSON_NAME_MAX_BYTES // 4)):
        assert bound_person_name(name) == name


@pytest.mark.parametrize("unit", ["a", "é", "€", "😀"])
def test_an_over_limit_name_is_bounded_on_a_character_boundary(unit: str) -> None:
    name = unit * (PERSON_NAME_MAX_BYTES // len(unit.encode()) + 1)
    assert len(name.encode()) > PERSON_NAME_MAX_BYTES

    bounded = bound_person_name(name)

    assert len(bounded.encode()) <= PERSON_NAME_MAX_BYTES
    assert bounded.encode().decode("utf-8") == bounded  # never a split character
    assert bounded.startswith(unit)


def test_bounding_is_deterministic_and_keeps_different_long_names_apart() -> None:
    first = "x" * (PERSON_NAME_MAX_BYTES + 10) + "one"
    second = "x" * (PERSON_NAME_MAX_BYTES + 10) + "two"

    assert bound_person_name(first) == bound_person_name(first)
    assert bound_person_name(first) != bound_person_name(second)


def test_credit_rows_carry_the_bounded_name_and_keep_the_credit() -> None:
    long_name = "L" * (PERSON_NAME_MAX_BYTES + 1)

    rows = person_credit_rows("R1", [{"name": long_name, "role": "Producer", "id": 7}, {"name": "Flood", "role": "Mixed By"}])

    assert [row["role"] for row in rows] == ["Producer", "Mixed By"]
    assert rows[0]["name"] == bound_person_name(long_name)
    assert len(rows[0]["name"].encode()) <= PERSON_NAME_MAX_BYTES
    assert rows[0]["artist_id"] == 7
    assert rows[1]["name"] == "Flood"
