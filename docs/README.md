# discogs-graph-enricher documentation

These documents describe the GrooveMap service that turns Discogs catalog events into
the Neo4j knowledge graph.

## Runtime contracts

- [Service configuration and graph model](../graphinator/README.md)
- [Database schema: the media graph model](database-schema.md)
- [Consumer cancellation and drain behavior](consumer-cancellation.md)
- [File and extraction completion](file-completion-tracking.md)
- [Neo4j and RabbitMQ resilience](database-resilience.md)
- [Neo4j schema and index ownership](neo4j-indexing.md)

## Performance and provenance

- [Performance guide](performance-guide.md)
- [Neo4j write-query design](query-performance-optimizations.md)
- [Release compliance](release-compliance.md)
- [History rewrite approval gate](history-rewrite-gate.md)

Private planning records are preserved exclusively in the private `planning-archive`
repository. They are not active service documentation and must not be copied here.
