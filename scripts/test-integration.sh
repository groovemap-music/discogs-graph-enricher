#!/usr/bin/env bash
set -euo pipefail

container="${NEO4J_INTEGRATION_CONTAINER:-groovemap-discogs-graph-integration-$$}"
image="${NEO4J_INTEGRATION_IMAGE:-neo4j:2026-community@sha256:dbc377fb9cd8fe8dabc19d3041b197d5ca0ef8bae514cea175b8df265e5b7a76}"
password="${NEO4J_INTEGRATION_PASSWORD:-integration-test-password}"

cleanup() {
    docker rm --force "${container}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker run --detach --rm \
    --name "${container}" \
    --publish 127.0.0.1::7687 \
    --env "NEO4J_AUTH=neo4j/${password}" \
    --env NEO4J_server_memory_heap_initial__size=256m \
    --env NEO4J_server_memory_heap_max__size=256m \
    "${image}" >/dev/null

ready=false
for _attempt in $(seq 1 60); do
    if docker exec "${container}" cypher-shell --username neo4j --password "${password}" "RETURN 1" >/dev/null 2>&1; then
        ready=true
        break
    fi
    sleep 2
done

if [[ "${ready}" != true ]]; then
    docker logs "${container}" >&2
    echo "Neo4j did not become ready within 120 seconds" >&2
    exit 1
fi

published="$(docker port "${container}" 7687/tcp)"
port="${published##*:}"
# Use direct Bolt for the random host port. The routing scheme would accept the
# container's advertised 7687 address and accidentally leave this disposable endpoint.
NEO4J_URI="bolt://127.0.0.1:${port}" \
NEO4J_INTEGRATION_USER=neo4j \
NEO4J_INTEGRATION_PASSWORD="${password}" \
    uv run pytest -m integration tests/integration
