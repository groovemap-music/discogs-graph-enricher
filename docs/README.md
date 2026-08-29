# discogs-graph-enricher documentation

These documents describe the GrooveMap service that turns Discogs catalog events into
the Neo4j knowledge graph.

## Runtime contracts

- [Service configuration and graph model](../graphinator/README.md)
- [Consumer cancellation and drain behavior](consumer-cancellation.md)
- [File and extraction completion](file-completion-tracking.md)
- [Neo4j and RabbitMQ resilience](database-resilience.md)
- [Neo4j indexing](neo4j-indexing.md)

## Performance and provenance

- [Performance guide](performance-guide.md)
- [Query performance optimization record](query-performance-optimizations.md)
- [Source-history provenance](extraction.md)
- [Historical plans](superpowers/plans/) and [design specifications](superpowers/specs/)

Historical design records preserve old component and issue identifiers when those names
are required to trace a regression. They do not define the current service identity.
