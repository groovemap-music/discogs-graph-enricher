"""Publication-facing identity regressions for discogs-graph-enricher."""

from pathlib import Path

import graphinator.graphinator as service


ROOT = Path(__file__).parent.parent


def test_runtime_identity_uses_repository_name() -> None:
    assert service.SERVICE_NAME == "discogs-graph-enricher"
    assert service.get_health_data()["service"] == "discogs-graph-enricher"
    assert "GrooveMap / discogs-graph-enricher" in service.STARTUP_BANNER
    source = (ROOT / "graphinator" / "graphinator.py").read_text()
    assert 'consumer_tag=f"{SERVICE_NAME}-{data_type}"' in source


def test_image_and_entry_point_use_repository_name() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text()
    build_script = (ROOT / "scripts" / "build-image.sh").read_text()
    pyproject = (ROOT / "pyproject.toml").read_text()

    assert 'org.opencontainers.image.title="discogs-graph-enricher"' in dockerfile
    assert 'ENTRYPOINT ["/app/.venv/bin/discogs-graph-enricher"]' in dockerfile
    assert "--tag discogs-graph-enricher:local" in build_script
    assert 'discogs-graph-enricher = "graphinator.graphinator:cli"' in pyproject


def test_active_documentation_uses_current_product_identity() -> None:
    readme = (ROOT / "README.md").read_text()
    assert "ghcr.io/groovemap-music/discogs-graph-enricher" in readme
    assert "groovemap-discogs-graphinator-artists" in readme
    assert "emits no Discogs\n`User-Agent`" in readme
    assert "```mermaid" in readme
