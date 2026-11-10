"""catalog-api's placeholder-label constants, read from Python source without importing it.

Shared by scripts/check-contracts.py (the enricher's module against the pin, offline) and
scripts/check-label-placeholders-upstream.py (the pin against catalog-api itself).
"""

import ast
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "contracts/label-placeholders/v1/source.json"
NAMES = ("PLACEHOLDER_LABEL_IDS", "PLACEHOLDER_LABEL_NAME", "REAL_LABELS_NAMED_LIKE_PLACEHOLDERS")


def pin() -> dict[str, Any]:
    """Return the pinned contract record."""
    return json.loads(SOURCE.read_text())


def constants_in(source: str) -> dict[str, Any]:
    """Return each constant's literal value in ``source``, tuples as lists, in JSON's shape.

    Raises:
        ValueError: if a constant is missing, assigned twice, or not a literal.
    """
    found: dict[str, Any] = {}
    for node in ast.parse(source).body:
        if isinstance(node, ast.Assign):
            targets, value = node.targets, node.value
        elif isinstance(node, ast.AnnAssign) and node.value is not None:
            targets, value = [node.target], node.value
        else:
            continue
        for target in targets:
            if isinstance(target, ast.Name) and target.id in NAMES:
                if target.id in found:
                    raise ValueError(f"{target.id} is assigned more than once")
                literal = ast.literal_eval(value)
                found[target.id] = list(literal) if isinstance(literal, tuple) else literal
    missing = [name for name in NAMES if name not in found]
    if missing:
        raise ValueError(f"missing constants: {', '.join(missing)}")
    return found
