"""Static contracts for immutable, fail-closed repository automation."""

import re
from pathlib import Path


ROOT = Path(__file__).parent.parent
AUTOMATION_REVISION = "833cb464507678c38ab78bd4718ce697399463e9"
PYTHON_LIBRARIES_REVISION = "e372b6a7598ae31ee6578fdff39bc920bedd7136"


def _maintained_markdown() -> list[Path]:
    return [ROOT / "README.md", ROOT / "graphinator" / "README.md", *sorted((ROOT / "docs").glob("*.md"))]


def test_reusable_workflows_are_immutably_pinned() -> None:
    expected = {
        "ci.yml": "reusable-ci.yml",
        "release.yml": "reusable-release.yml",
    }
    for name, reusable_name in expected.items():
        workflow = (ROOT / ".github" / "workflows" / name).read_text()
        refs = re.findall(
            rf"uses: groovemap-music/automation/\.github/workflows/{reusable_name}@([^\s]+)",
            workflow,
        )
        assert refs == [AUTOMATION_REVISION]
        assert "groovemap-music/.github/" not in workflow
        assert "secrets: inherit" not in workflow


def test_dependabot_pull_requests_run_the_ordinary_required_ci_graph() -> None:
    workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text()

    assert "pull_request:" in workflow
    assert "schedule:" in workflow
    assert "workflow_dispatch:" in workflow
    jobs = workflow.split("jobs:\n", 1)[1]
    assert len(re.findall(r"^  [a-zA-Z0-9_-]+:\s*$", jobs, re.MULTILINE)) == 1
    assert "jobs:\n  required:" in workflow
    assert "github.actor" not in workflow.lower()
    assert "dependabot" not in workflow.lower()
    assert "fallback-command" not in workflow
    assert "if:" not in workflow.lower()

    for fragment in (
        "language: python",
        "setup-command: just setup",
        "check-command: just check",
        "coverage-command: just coverage",
        "audit-command: just audit",
        "license-command: just license-check",
        "secret-scan-command: just secret-scan",
        "package-command: just build",
        "install-command: just install-check",
        "image-command: just image",
        "coverage-files: coverage.xml",
        "upload-codecov: true",
        "CODECOV_TOKEN: ${{ secrets.CODECOV_TOKEN }}",
    ):
        assert fragment in workflow

    for marker in (
        "requires-private-library",
        "private-library-client-id",
        "private-library-revision",
        "private_library_private_key",
        "groovemap_ci_app_client_id",
        "groovemap_ci_app_private_key",
    ):
        assert marker not in workflow.lower()

    pyproject = (ROOT / "pyproject.toml").read_text()
    assert "https://github.com/groovemap-music/python-libraries.git" in pyproject
    assert PYTHON_LIBRARIES_REVISION in pyproject


def test_release_is_tag_only_attested_and_repository_named() -> None:
    workflow = (ROOT / ".github" / "workflows" / "release.yml").read_text()

    assert re.search(r'on:\s*\n  push:\s*\n    tags: \["v\*"\]', workflow)
    assert "workflow_dispatch:" not in workflow
    assert "schedule:" not in workflow
    assert "branches:" not in workflow
    assert "attestations: write" in workflow
    assert "id-token: write" in workflow
    assert "packages: write" in workflow
    assert "repository-name: discogs-graph-enricher" in workflow
    assert "release-command: just release-dry-run" in workflow
    assert "publish-image: true" in workflow
    assert "prepare-image-command: just prepare-runtime-wheel" in workflow
    assert "latest" not in workflow.lower()
    for marker in (
        "requires-private-library",
        "private-library-client-id",
        "private-library-revision",
        "private_library_private_key",
        "groovemap_ci_app_client_id",
        "groovemap_ci_app_private_key",
    ):
        assert marker not in workflow.lower()


def test_required_regression_suites_remain_in_the_full_gate() -> None:
    expected_tests = {
        "tests/test_shutdown_delivery_churn.py": (
            "test_shutdown_guard_leaves_repeated_deliveries_unsettled",
            "test_shutdown_cancels_every_consumer_before_connection_close",
        ),
        "tests/test_file_completion.py": (
            "test_file_complete_flushes_marks_and_acknowledges",
            "test_failed_drain_requeues_marker_without_marking_complete",
        ),
        "tests/test_batch_processor.py": (
            "test_cancellation_restores_unsettled_delivery",
            "test_transient_failure_is_retained_without_poison_or_settlement",
        ),
    }
    for relative_path, test_names in expected_tests.items():
        source = (ROOT / relative_path).read_text()
        for test_name in test_names:
            assert f"def {test_name}(" in source


def test_validation_recipes_use_locked_narrow_capabilities() -> None:
    justfile = (ROOT / "Justfile").read_text()

    assert "uv run ruff format --check ." in justfile
    assert "uv run ruff check ." in justfile
    assert "uvx --from ruff" not in justfile
    assert "contract-check:" in justfile
    assert "coverage: test" in justfile
    assert "secret-scan:" in justfile
    assert "--version-files-only" in justfile
    assert "--files-only" not in justfile


def test_documentation_uses_current_owners_and_runtime_identifiers() -> None:
    documentation = "\n".join(path.read_text() for path in _maintained_markdown())

    assert "https://github.com/groovemap-music/discogs-ingestion" in documentation
    assert "catalog-ingestion" not in documentation
    for queue in (
        "groovemap-discogs-graphinator-artists",
        "groovemap-discogs-graphinator-labels",
        "groovemap-discogs-graphinator-masters",
        "groovemap-discogs-graphinator-releases",
    ):
        assert queue in documentation
    for metric in (
        "groovemap.pipeline.messages",
        "groovemap.pipeline.message.duration",
        "groovemap.pipeline.batch.size",
        "groovemap.pipeline.batch.flush.duration",
        "groovemap.pipeline.consumers.active",
        "messaging.client.consumed.messages",
        "db.client.operation.duration",
        "groovemap.pipeline.reconnects",
        "groovemap.runtime.event_loop.lag",
    ):
        assert metric in documentation


def test_documentation_local_links_and_recipe_paths_resolve() -> None:
    for markdown in _maintained_markdown():
        source = markdown.read_text()
        for target in re.findall(r"(?<!!)\[[^]]+\]\(([^)]+)\)", source):
            target = target.split("#", 1)[0]
            if not target or target.startswith(("http://", "https://", "mailto:")):
                continue
            assert (markdown.parent / target).resolve().exists(), f"broken link in {markdown.relative_to(ROOT)}: {target}"

        for test_path in re.findall(r"tests/test_[A-Za-z0-9_]+\.py", source):
            assert (ROOT / test_path).is_file(), f"missing recipe path in {markdown.relative_to(ROOT)}: {test_path}"


def test_completion_mermaid_diagrams_preserve_runtime_ordering() -> None:
    cancellation = (ROOT / "docs" / "consumer-cancellation.md").read_text()
    completion = (ROOT / "docs" / "file-completion-tracking.md").read_text()

    assert cancellation.count("```mermaid") == 1
    assert cancellation.count("```") == 4  # Mermaid plus the verification command block.
    assert cancellation.index("drain that entity batch queue") < cancellation.index("ack file_complete")
    assert cancellation.index("ack file_complete") < cancellation.index("cancel that entity consumer")

    assert completion.count("```mermaid") == 1
    assert completion.count("```") == 4  # Mermaid plus the verification command block.
    ordered_steps = (
        "drain signalling entity queue",
        "persist entity signal in Neo4j",
        "ack final signal",
        "start detached single-flight maintenance",
        "drain all four batch queues",
        "remove unresolved stubs",
        "refresh genre, style, and label aggregates",
    )
    positions = [completion.index(step) for step in ordered_steps]
    assert positions == sorted(positions)


def test_no_renovate_or_legacy_claude_workflow_exists() -> None:
    repository_paths = [path.relative_to(ROOT).as_posix().lower() for path in ROOT.rglob("*") if path.is_file()]
    assert not any("renovate" in path for path in repository_paths)
    assert not any(path.startswith(".github/workflows/claude") for path in repository_paths)
