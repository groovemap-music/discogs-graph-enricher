# History-preserving extraction

The source was migration branch `wt/bead/issue/discogsography-2kpm.14` at `e4274316` in
the unchanged monorepo. A disposable clone retained `graphinator/`,
`tests/graphinator/`, applicable graph/resilience design documents, and `LICENSE`; owned
tests were promoted to `tests/`.

The exact `git filter-repo` arguments were:

```text
--path graphinator/
--path tests/graphinator/
--path LICENSE
--path docs/consumer-cancellation.md
--path docs/database-resilience.md
--path docs/file-completion-tracking.md
--path docs/neo4j-indexing.md
--path docs/performance-guide.md
--path docs/query-performance-optimizations.md
--path docs/superpowers/plans/2026-03-19-query-debug-profiling.md
--path docs/superpowers/specs/2026-03-19-query-debug-profiling-design.md
--path docs/superpowers/plans/2026-03-21-query-perf-opt-v5.md
--path docs/superpowers/plans/2026-05-21-neo4j-bolt-tls.md
--path docs/superpowers/specs/2026-05-21-neo4j-bolt-tls-design.md
--path-rename tests/graphinator/:tests/
```

The filter retained 220 relevant commits and no tags. The current tree is MIT licensed by
owner decision; earlier license revisions remain in history. The original monorepo and its
refs were not rewritten or deleted.
