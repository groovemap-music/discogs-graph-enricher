"""Import, publication, and promoted catalog-contract smoke tests."""

from pathlib import Path

import graphinator.graphinator as service
from graphinator.catalog_contract import AMQP_EXCHANGE_TYPE, DATA_TYPES, DISCOGS_EXCHANGE_PREFIX


ROOT = Path(__file__).parent.parent


def test_service_import_exposes_entry_point() -> None:
    assert callable(service.main)


def test_catalog_contract_matches_discogs_stream() -> None:
    assert DISCOGS_EXCHANGE_PREFIX == "groovemap-discogs"
    assert AMQP_EXCHANGE_TYPE == "fanout"
    assert DATA_TYPES == ["artists", "labels", "masters", "releases"]


def test_public_tree_excludes_private_planning_material() -> None:
    assert not (ROOT / "docs" / "extraction.md").exists()
    assert not any(item.is_file() for item in (ROOT / "docs" / "superpowers").rglob("*"))
    assert not any(item.is_file() for item in (ROOT / "docs" / "specs").rglob("*"))


def test_publication_docs_preserve_separate_operator_gates() -> None:
    release = (ROOT / "docs" / "release-compliance.md").read_text()
    history = (ROOT / "docs" / "history-rewrite-gate.md").read_text()
    assert "Dependabot-authored pull requests run the same required" in release
    assert "explicit operator" in release
    assert "Visibility, tags" in release
    assert "Explicit operator approval" in history
    assert "daf82a149aaa382b3cebbd4b43d3c82e53d4128e" in history
