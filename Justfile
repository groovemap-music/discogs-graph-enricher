set shell := ["bash", "-euo", "pipefail", "-c"]

default:
    @just --list

setup:
    uv sync --dev --frozen

source-check: format-check lint contract-check

format-check:
    uv run ruff format --check .

lint:
    uv run ruff check .

contract-check:
    uv run python scripts/check-contracts.py

secret-scan:
    gitleaks git --redact --no-banner
    gitleaks dir . --redact --no-banner

check: source-check typecheck coverage secret-scan build install-check license-check bump-preview

format:
    uv run ruff format .
    uv run ruff check --fix .

typecheck:
    uv run mypy

test:
    uv run pytest -m "not integration" --cov=graphinator --cov-report=term-missing --cov-report=xml

# Starts and removes a disposable, loopback-only Neo4j container.
test-integration:
    bash scripts/test-integration.sh

coverage: test

build:
    uv build --out-dir dist --clear

install-check: build
    bash scripts/install-check.sh

license-check:
    uv run python scripts/check-license.py
    uv run pip-licenses --fail-on "GPL-2.0-only;GPL-3.0-only;AGPL-3.0-only"

audit:
    uv run pip-audit

prepare-runtime-wheel:
    bash scripts/prepare-runtime-wheel.sh

image: prepare-runtime-wheel
    bash scripts/build-image.sh
    docker run --rm --entrypoint /app/.venv/bin/python discogs-graph-enricher:local -c 'import graphinator.graphinator'
    test "$(docker run --rm --entrypoint /usr/bin/id discogs-graph-enricher:local -u):$(docker run --rm --entrypoint /usr/bin/id discogs-graph-enricher:local -g)" = "1000:1000"

bump-preview:
    uv run python scripts/check_bump_preview.py

# Update local version metadata and changelog only; do not commit, tag, push, or publish.
bump:
    uv run cz bump --version-files-only --changelog --yes --check-consistency
    uv lock

release-dry-run: check
    bash scripts/release-dry-run.sh
