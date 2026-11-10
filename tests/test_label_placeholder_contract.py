"""The placeholder-label constants the ranking uses are catalog-api's, pinned to a commit."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from graphinator import label_longevity_rank
from scripts import label_placeholders


ROOT = Path(__file__).resolve().parents[1]


def test_the_module_carries_the_pinned_constants() -> None:
    pinned = label_placeholders.pin()
    assert pinned["consumer_module"] == "graphinator/label_longevity_rank.py"
    # What the module holds at runtime, not only what its source says.
    assert {
        "PLACEHOLDER_LABEL_IDS": list(label_longevity_rank.PLACEHOLDER_LABEL_IDS),
        "PLACEHOLDER_LABEL_NAME": label_longevity_rank.PLACEHOLDER_LABEL_NAME,
        "REAL_LABELS_NAMED_LIKE_PLACEHOLDERS": list(label_longevity_rank.REAL_LABELS_NAMED_LIKE_PLACEHOLDERS),
    } == pinned["constants"]


def test_a_changed_constant_is_seen() -> None:
    source = (ROOT / "graphinator/label_longevity_rank.py").read_text()
    drifted = source.replace('PLACEHOLDER_LABEL_IDS = ("1818",)', 'PLACEHOLDER_LABEL_IDS = ("1818", "1")')
    assert drifted != source
    assert label_placeholders.constants_in(drifted) != label_placeholders.pin()["constants"]


def test_a_missing_or_repeated_constant_is_an_error() -> None:
    with pytest.raises(ValueError, match="missing constants: PLACEHOLDER_LABEL_NAME"):
        label_placeholders.constants_in('PLACEHOLDER_LABEL_IDS = ("1818",)\nREAL_LABELS_NAMED_LIKE_PLACEHOLDERS: tuple[str, ...] = ()\n')
    with pytest.raises(ValueError, match="more than once"):
        label_placeholders.constants_in("PLACEHOLDER_LABEL_IDS = ()\nPLACEHOLDER_LABEL_IDS = ()\n")


def test_the_contract_check_passes() -> None:
    subprocess.run([sys.executable, str(ROOT / "scripts/check-contracts.py")], check=True, cwd=ROOT)  # noqa: S603
