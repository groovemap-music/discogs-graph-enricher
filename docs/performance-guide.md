# Graph-writer performance guide

This repository owns the performance of RabbitMQ consumption, in-memory batching,
and Neo4j projection. It does not own export parsing, HTTP request latency, fleet
sizing, or database deployment settings.

For those concerns use the owning repositories:

- [Discogs extraction and publishing](https://github.com/groovemap-music/discogs-ingestion)
- [Catalog API performance](https://github.com/groovemap-music/catalog-api/blob/main/docs/performance-guide.md)
- [Deployment performance and capacity](https://github.com/groovemap-music/deployment/blob/main/docs/performance-guide.md)
- [Neo4j schema ownership](https://github.com/groovemap-music/database-schema)

## Local tuning controls

| Variable | Default | Effect |
| --- | ---: | --- |
| `NEO4J_BATCH_MODE` | `true` | Use per-entity batch queues and projection transactions. |
| `NEO4J_BATCH_SIZE` | `100` | Target records in each Neo4j flush. |
| `NEO4J_BATCH_FLUSH_INTERVAL` | `5.0` | Maximum seconds a low-volume batch waits for a periodic flush. |
| `CONSUMER_CANCEL_DELAY` | `300` | Grace period before cancelling a completed entity consumer; `0` disables it. |
| `QUEUE_CHECK_INTERVAL` | `3600` | Queue-depth polling interval after every consumer is idle. |
| `STUCK_CHECK_INTERVAL` | `30` | Interval for detecting unexpectedly absent consumers. |

Batch mode uses a per-consumer RabbitMQ prefetch of
`max(200, NEO4J_BATCH_SIZE * 2)`. Because the four consumers share one channel, the
possible channel-wide unsettled total is four times that value. Neo4j flushes are
limited to two concurrent entity types and serialized within each entity type.

The batch processor reduces its effective batch size after transient Neo4j failures,
backs off, and grows back toward the configured size after successful writes. It
keeps transient outage retries separate from deterministic poison retries so an
outage cannot spend the poison-message budget. Avoid changing these bounds from
documentation; they are code-level failure-policy constants covered by regression
tests.

## What to measure

Measure a complete import or a representative replay. Record the input count and
duration per entity type, queue depth, Neo4j resource saturation, and these emitted
signals:

- `groovemap.pipeline.batch.size`
- `groovemap.pipeline.batch.flush.duration`
- `groovemap.pipeline.messages`
- `groovemap.pipeline.message.duration`
- `groovemap.pipeline.consumers.active`
- `messaging.client.consumed.messages`
- `db.client.operation.duration`
- `groovemap.runtime.event_loop.lag`

The service pushes telemetry with OTLP/HTTP-protobuf and exposes no `/metrics` route.
Use the [deployment observability guide](https://github.com/groovemap-music/deployment/blob/main/docs/observability.md)
for collector-derived rates, dashboards, and alerting.

## Safe tuning loop

1. Run the focused batch and projection tests before changing a default:

   ```bash
   uv run pytest tests/test_batch_processor.py tests/test_media_projection.py
   ```

2. Change one control at a time and replay the same representative fixture.
3. Compare throughput with p95 flush duration, event-loop lag, retry counts, queue
   depth, and Neo4j saturation. A larger batch that increases failure recovery time
   or exhausts transaction memory is not an improvement.
4. Restore defaults or codify the new value in deployment only after the owning
   environment has demonstrated headroom.
5. Run `just check`; live load and deployment validation remain separate gates.

Do not copy catalog API query benchmarks into this repository. Writer query structure
and profiling guidance live in [Neo4j write-query design](query-performance-optimizations.md).
