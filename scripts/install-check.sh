#!/usr/bin/env bash
set -euo pipefail

bash scripts/prepare-runtime-wheel.sh
enricher_tmp="$(mktemp -d)"
trap 'rm -rf "${enricher_tmp}"' EXIT

uv venv "${enricher_tmp}/venv"
uv pip install --python "${enricher_tmp}/venv/bin/python" ".build/runtime/$(basename "$(find .build/runtime -type f -name '*.whl' -print -quit)")[neo4j,rabbitmq]"
uv pip install --python "${enricher_tmp}/venv/bin/python" --no-deps dist/*.whl
"${enricher_tmp}/venv/bin/python" -c 'import graphinator.graphinator; import graphinator.config'
