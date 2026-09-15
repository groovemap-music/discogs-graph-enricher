# Neo4j schema and index ownership

The [`database-schema`](https://github.com/groovemap-music/database-schema)
repository owns Neo4j constraints, indexes, initialization, and migrations.
`discogs-graph-enricher` relies on that installed schema; it does not create or drop
schema objects at startup.

## Writer expectations

The graph projector matches or merges these stable keys:

| Label | Key |
| --- | --- |
| Artist, Label, Master, Release | `id` |
| Genre, Style, Person | `name` |
| Medium | `id` |
| MediaFamily | `name` |
| Company | `id` |
| ExtractionCompletion | `version` |

The owning schema should provide the constraints and indexes appropriate to those
lookups. The full deployed inventory is authoritative in `database-schema`; copying
its DDL here would create a second, drifting source of truth.

`discogs-graph-enricher` also refreshes denormalized Genre, Style, and Label aggregate
properties after a completed extraction. Those properties reduce work for readers,
but read-query indexes and performance targets belong to
[`catalog-api`](https://github.com/groovemap-music/catalog-api/blob/main/docs/query-performance-optimizations.md).

## Verification

When changing a projector query:

1. Run the repository's mocked projection and batch tests.
2. In a disposable environment initialized by the current `database-schema`, inspect
   representative writes with Neo4j `EXPLAIN` and, when safe, `PROFILE`.
3. Confirm key lookups use the expected constraint or index and that the configured
   batch size fits transaction-memory limits.
4. Run `just check` before review.

```bash
uv run pytest tests/test_batch_processor.py tests/test_media_projection.py tests/test_company_projection.py
just check
```

The media nodes and source-scoped relationship prune owned by this writer are
documented in [the media graph model](database-schema.md); the `Company` nodes and the
source-scoped `CREDITED_TO` prune, along with the `Release.country` property, are
documented in [company credits and release country](company-credits.md). Query construction and
batch invariants are documented in [Neo4j write-query design](query-performance-optimizations.md).
