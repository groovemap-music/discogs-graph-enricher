# Cypher performance audit — 2026-11-04

Bead: `gm-discogs-graph-enricher-hho`. Scope: the enricher's Cypher (batch projection,
entity projection, stub cleanup, paged maintenance). Nothing in production code was changed.

## Method and limits (read first)

**This is a static review. No PROFILE was taken and no timing in this report is measured.**

- Live read-only Cypher on lux was attempted and **denied by the Claude Code auto-mode
  classifier** ("Production Reads"). It was not worked around. The very first query (`SHOW INDEXES`)
  was the one blocked, so no index inventory from the live graph either.
- The slowest Genre/Style metric (for example Rock `artist_count`) and the first v0.5.0
  paged-maintenance run therefore remain **unmeasured**. The figures quoted below for the
  old statements come from the bead and from `graphinator/maintenance_paging.py`; every
  "estimated gain" is a reasoning-based estimate to be confirmed by the follow-up bead's measurement.
- Postgres: the enricher's hot path is Cypher only. The partial-Postgres load (3.97M of 19.6M
  releases, 7.9M of 10.3M artists) does not affect any statement here. `scripts/stub-name-export.sql`
  was not profiled; any SQL timing taken now would be a lower bound, and it should scale
  roughly linearly with release/artist rows (about 5x for releases, 1.3x for artists) unless it
  contains a sort or join over the full set, which was not checked.
- Neo4j on lux is complete (19.6M releases), so the live measurements, once permitted, are
  directly representative.

## Ranked hot spots

| # | Hot spot | Cause | Proposed fix | Est. gain | Risk |
|---|----------|-------|--------------|-----------|------|
| 1 | Genre/Style stats metrics (`graphinator.py` `_node_stat_metrics`) | Each metric traverses every `IS` edge of the node (the code comment cites ~200M DB hits for Rock) and builds a `count(DISTINCT ...)` set of up to millions of artists/labels in transaction memory. The smallest unit of paging is one node, so a single metric on Rock cannot be split by the halving backoff; at page size 1 it fails 3 times and the step gives up. Old Style total was 9m35s of a 600s limit. | Measure the single worst metric first. Then compute from the Release side in keyed Release pages into per-genre accumulators, or maintain counts incrementally on ingest. `first_year` can use a `MIN` over an indexed `Release.year` per genre rather than a full scan. | Large (order of magnitude on Rock-sized genres, estimate) | Medium: changes the counting method; must match current `DISTINCT` semantics |
| 2 | Unconditional rewrite in Label and Person sweeps | Label page `SET`s three properties on every Label (7.3M properties set in the old run); Person page `SET`s `credit_count` plus every category on every Person, including zeros that did not change. Each write costs WAL, index maintenance and page-cache churn. | `SET` only when the value differs (`WHERE l.release_count <> rc OR ...`), keeping the zero-writing semantics for removed credits | Large on re-runs (most values unchanged; estimate), small on a first run | Low |
| 3 | Label stats page runs three separate traversals | Three `CALL {}` subqueries each re-expand `(l)<-[:ON]-(r:Release)`; large labels pay 3x (a label with 55K releases is cited in the code) | One traversal collecting `r`, then derive the three counts | ~2-3x on big labels (estimate) | Low |
| 4 | Genre/Style supernode contention during load | Every release/master batch does `MERGE (g:Genre {name})` and `MERGE (r)-[:IS]->(g)` per row, repeating the same name up to batch-size times and taking write locks on a handful of very dense nodes. Plausible contributor to the 27 "Slow consumers detected" warnings. Hypothesis only; lock waits were not observed. | Deduplicate distinct genre/style names per batch (`UNWIND $names` → `MERGE` once), then `MATCH` the node for the edge `MERGE`. Verify with lock/wait metrics during a load. | Unknown until measured | Low-medium |
| 5 | `Style-[:PART_OF]->Genre` pairs repeated per release | `genre_style_data` carries one pair per release/style and runs three `MERGE`s for each; the same ~hundreds of distinct pairs repeat across 19.6M releases | De-duplicate pairs in Python before `UNWIND` (also in the master path) | Small-moderate, trivial | Very low |
| 6 | Eight prune statements per Release batch | `BY`, `ON`, `DERIVED_FROM`, `IS`→Genre, `IS`→Style, `ISSUED_ON`, `CREDITED_TO`, `CREDITED_ON` each seek every release again and expand its edges. On a first load the releases have no edges so these are cheap; on re-imports they dominate round-trips | Combine into one statement per release (single seek, expand once, delete by type predicate) | Moderate on re-import (estimate) | Medium: touches the edge-removal invariants |
| 7 | Person name index limit | RANGE index refused a 12,000-byte test value. The longest real name is 4,159 characters, which fits if mostly ASCII, but 4,159 characters of multibyte text can exceed the limit | Measure the longest name in UTF-8 bytes on the live graph; if close to the limit, reject or normalise over-long credit names in `person_credit_rows` with a counted log, instead of failing the whole batch | Prevents a batch-level failure | Low |

## Paged maintenance (v0.5.0)

Reviewed `maintenance_paging.py` and the Genre/Style/Label/Person page statements.

- Design is sound: keyed cursor, per-page checkpoint, halving on timeout or memory pressure.
- Genre/Style page size is 10 nodes with each metric its own statement; the floor is one node
  and one metric, so the halving cannot help the heaviest single metric (hot spot 1).
- Person pages are 1000 persons ordered by `name`; the cursor relies on the `Person.name`
  RANGE index for both the seek and the order, so hot spot 7 matters here too.
- Per-page checkpoint writes are a single `MERGE` on a tiny node; negligible.
- Not yet run on lux; the first real run timing is **unmeasured**.

## Indexes and constraints

The enricher does not own schema (`docs/neo4j-indexing.md`); the live inventory could not be
read (see Method). The statements assume constraints or indexes on `id` for Artist, Label,
Master, Release, Medium, Company, and on `name` for Genre, Style, Person, MediaFamily. The
follow-up should compare `SHOW INDEXES` / `SHOW CONSTRAINTS` on lux with that list.

## Do projected relationships earn their cost

`SAME_AS` (Person→Artist), `CREDITED_ON` (one edge per role, not per release) and `IS` to both
Genre and Style are the heaviest edge families. Whether readers use per-role `CREDITED_ON`
edges, rather than per-release, needs the catalog-api audit's query list; no conclusion here.

## Proposed follow-up beads (not filed)

1. Measure and fix the heaviest Genre/Style metric (hot spot 1) — target: no single metric above 120s on lux.
2. Skip unchanged writes in Label and Person sweeps (2) — target: a re-run writes near zero properties.
3. Single-traversal Label stats (3) — target: Label stats at least 2x faster on the largest labels.
4. Deduplicate Genre/Style/PART_OF merges per batch (4, 5) — target: fewer "Slow consumers" warnings in a comparable load.
5. Consolidate Release prune statements (6) — target: measured re-import throughput gain.
6. Measure Person name bytes and guard the index limit (7).
7. Permit and run the live read-only measurements (PROFILE of paged maintenance and the worst Genre/Style metric, `SHOW INDEXES`) once the operator allows it.
