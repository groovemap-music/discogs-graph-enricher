# Neo4j write-query design

This service constructs write queries for Discogs entity projection. Public API read
queries and their latency targets belong to the
[`catalog-api` performance guide](https://github.com/groovemap-music/catalog-api/blob/main/docs/performance-guide.md)
and its
[`query performance record`](https://github.com/groovemap-music/catalog-api/blob/main/docs/query-performance-optimizations.md).

## Projection boundaries

- `common.batch.AsyncBatchEngine` owns queueing, acknowledgement, bounded retries,
  adaptive batch size, cancellation restoration, drain, and flush concurrency.
- `Neo4jBatchProcessor` is the owner-hive adapter: it normalizes Discogs records and
  supplies Neo4j failure classification, telemetry, and projection policy to that engine.
- `Neo4jBatchProjector` owns record-to-parameter projection and the Cypher executed
  for artists, labels, masters, and releases.
- `entity_projection` owns the equivalent single-record path used when
  `NEO4J_BATCH_MODE=false`.
- The `database-schema` repository owns constraints and indexes. This repository's
  [schema guide](database-schema.md) documents only the media projection it writes.

Every batch query accepts records as parameters and begins with `UNWIND $records`.
Fix queue or delivery-settlement lifecycle defects once in `common.batch` or
`common.delivery`; keep Cypher, Neo4j exception mapping, and graph telemetry here.
No record value is interpolated into Cypher text. Entity nodes are matched or merged
on stable keys, then their `sha256` values gate idempotent updates. Relationship lists
are projected as bounded parameter arrays inside the same transaction that owns the
entity update.

## Correctness rules that also limit work

1. A matching `sha256` skips an unchanged entity and its relationship projection.
2. One driver transaction owns each projected batch; acknowledgement happens only
   after that transaction succeeds.
3. Failed-record indexes returned by the projector let the coordinator acknowledge
   healthy records while preserving poison-record isolation.
4. Release media reprocessing prunes only this service's stale `ISSUED_ON` edges:
   `rel.source = "discogs"`. Assertions from another catalog remain untouched.
5. Related Artist, Label, Master, Genre, Style, Person, Medium, and MediaFamily nodes
   use stable lookup keys so `MERGE` does not create duplicates.
6. Post-import cleanup starts only after the durable four-signal latch and a full
   drain of all per-entity queues.

These are wire and schema behaviors, not optional micro-optimizations. Preserve them
when reorganizing query templates.

## Profiling a write change

Use mocked unit tests for the default gate and a disposable Neo4j instance for live
profiling. Never point a profiling command at production.

```bash
uv run pytest tests/test_batch_processor.py tests/test_media_projection.py
just check
```

For a proposed query, substitute representative parameter shapes and inspect it with
Neo4j `EXPLAIN`; use `PROFILE` only in the disposable environment. Confirm that key
lookups use the constraints/indexes provisioned by
[`database-schema`](https://github.com/groovemap-music/database-schema), transaction
memory remains bounded at the configured batch size, and the source-scoped prune is
unchanged. Compare end-to-end batch flush duration rather than isolated Cypher timing,
because parameter construction, driver scheduling, and acknowledgements are part of
this service's cost.

See the [graph-writer performance guide](performance-guide.md) for runtime controls
and emitted metrics.
