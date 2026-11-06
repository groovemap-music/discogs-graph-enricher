#!/usr/bin/env bash
set -euo pipefail

container="${NEO4J_INTEGRATION_CONTAINER:-groovemap-discogs-graph-integration-$$}"
image="${NEO4J_INTEGRATION_IMAGE:-neo4j:2026-community@sha256:dbc377fb9cd8fe8dabc19d3041b197d5ca0ef8bae514cea175b8df265e5b7a76}"
password="${NEO4J_INTEGRATION_PASSWORD:-integration-test-password}"
rabbitmq="${container}-rabbitmq"
rabbitmq_image="${RABBITMQ_INTEGRATION_IMAGE:-rabbitmq:4-management@sha256:c200fd496482cda18c412be4e6f244830e1e75949b8cf5f0e887cea9ea456109}"

cleanup() {
    docker rm --force --volumes "${container}" "${rabbitmq}" >/dev/null 2>&1 || true
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

docker run --detach --rm \
    --name "${rabbitmq}" \
    --hostname "${rabbitmq}" \
    --user rabbitmq \
    --publish 127.0.0.1::5672 \
    --env RABBITMQ_DEFAULT_USER=groovemap \
    --env RABBITMQ_DEFAULT_PASS=groovemap \
    "${rabbitmq_image}" >/dev/null

ready=false
for _attempt in $(seq 1 60); do
    if docker exec --user rabbitmq "${rabbitmq}" rabbitmq-diagnostics -q check_port_connectivity >/dev/null 2>&1; then
        ready=true
        break
    fi
    sleep 2
done

if [[ "${ready}" != true ]]; then
    docker logs "${rabbitmq}" >&2
    echo "RabbitMQ did not become ready within 120 seconds" >&2
    exit 1
fi

published="$(docker port "${container}" 7687/tcp)"
port="${published##*:}"
amqp_published="$(docker port "${rabbitmq}" 5672/tcp)"
# Use direct Bolt for the random host port. The routing scheme would accept the
# container's advertised 7687 address and accidentally leave this disposable endpoint.
RABBITMQ_URL="amqp://groovemap:groovemap@127.0.0.1:${amqp_published##*:}/" \
NEO4J_URI="bolt://127.0.0.1:${port}" \
NEO4J_INTEGRATION_USER=neo4j \
NEO4J_INTEGRATION_PASSWORD="${password}" \
    uv run pytest -m integration tests/integration "$@"
