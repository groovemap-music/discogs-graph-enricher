"""Import, publication, and promoted catalog-contract smoke tests."""

import ast
from pathlib import Path

import pytest

import graphinator.graphinator as service
from graphinator.queue_names import AMQP_EXCHANGE_TYPE, DATA_TYPES, DISCOGS_EXCHANGE_PREFIX


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


def test_neo4j_fixture_has_no_unspecced_database_boundary_mocks() -> None:
    """Keep the shared Neo4j fixture constrained to the installed driver API."""
    conftest = ROOT / "tests" / "conftest.py"
    tree = ast.parse(conftest.read_text())
    fixture = next(node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "mock_neo4j_driver")
    boundary_names = {"driver", "session", "transaction", "result"}
    unspecced: list[str] = []

    for node in ast.walk(fixture):
        if not isinstance(node, (ast.Assign, ast.AnnAssign)) or not isinstance(node.value, ast.Call):
            continue
        target = node.targets[0] if isinstance(node, ast.Assign) else node.target
        if not isinstance(target, ast.Name) or target.id not in boundary_names:
            continue
        function = node.value.func
        mock_name = function.id if isinstance(function, ast.Name) else ""
        if mock_name not in {"AsyncMock", "MagicMock", "Mock", "create_autospec"}:
            continue
        constrained = mock_name == "create_autospec" or any(keyword.arg in {"spec", "spec_set"} for keyword in node.value.keywords)
        if not constrained:
            unspecced.append(f"{target.id} at line {node.lineno}")

    assert unspecced == []


def test_neo4j_fixture_rejects_unknown_driver_methods(mock_neo4j_driver: object) -> None:
    """A typo at the driver boundary fails locally instead of becoming another mock."""
    with pytest.raises(AttributeError):
        _ = mock_neo4j_driver.sesion
