"""HEALTH_PORT parsing: role-port default, override, and loud rejection of bad values."""

import pytest

from graphinator.config import DEFAULT_HEALTH_PORT, GraphinatorConfig


@pytest.fixture(autouse=True)
def _required_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NEO4J_HOST", "neo4j")
    monkeypatch.setenv("NEO4J_USERNAME", "user")
    monkeypatch.setenv("NEO4J_PASSWORD", "pass")
    monkeypatch.delenv("HEALTH_PORT", raising=False)


def test_default_is_role_port() -> None:
    assert DEFAULT_HEALTH_PORT == 8001
    assert GraphinatorConfig.from_env().health_port == 8001


def test_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HEALTH_PORT", "9123")
    assert GraphinatorConfig.from_env().health_port == 9123


@pytest.mark.parametrize("value", ["abc", "", "0", "-1", "65536", "80.5"])
def test_invalid_value_is_rejected_not_defaulted(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    monkeypatch.setenv("HEALTH_PORT", value)
    with pytest.raises(ValueError, match="Invalid HEALTH_PORT"):
        GraphinatorConfig.from_env()
