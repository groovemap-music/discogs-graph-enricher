# Cypher performance audit — 2026-11-04

Bead: `gm-discogs-graph-enricher-hho`. Scope: the enricher's Cypher (batch projection,
entity projection, stub cleanup, paged maintenance). Nothing in production code was changed.

## Method and limits (read first)

The first pass was static (auto mode denied direct lux access). Later the operator approved
read-only access through the `gm-lux-cypher` wrapper only; the live results are in
"Live evidence" below. Everything there was taken on lux on 2026-11-04 around 20:00 UTC
against the complete graph (19.6M releases), one query at a time, read-only, each under
the 120s limit. Only the read half of each statement was profiled: the `SET` half is a
write and was not run, so **write cost, WAL and lock behaviour are not measured**.

- **Not measured:** the v0.5.0 paged maintenance has not run on lux; its end-to-end time and
  the "Slow consumers" cause remain unmeasured. Timings of the same query vary with page
  cache state (the first Rock query ran cold); treat single numbers as indicative.
- **Postgres:** nothing in the enricher's hot path uses it, and no SQL was profiled. The
  partial load (3.97M of 19.6M releases) matters only to any SQL timing, which would be a
  lower bound.

## Live evidence

Index inventory (`SHOW INDEXES`, all ONLINE) matches the writer's assumptions: RANGE on
`id` for Artist, Label, Master, Release, Company, Medium; `name` for Genre, Style, Person,
MediaFamily; uniqueness constraints on the same keys (`SHOW CONSTRAINTS`). Also present:
`release_year_index`, `genre_first_year_index`, `style_first_year_index`, seven
`person_credit_count_*` indexes. Not present: any constraint or index on
`MaintenanceCheckpoint.step` or `ExtractionCompletion.version` (both tiny, so this is
harmless today). The `person_credit_count_*` indexes mean every credit-count `SET` also
updates eight indexes, which raises the cost of unconditional rewrites (hot spot 2).

Graph scale: 6,261,762 Person, 2,434,989 Label, 757 Style, 15 Genre. Longest Person name:
4,159 characters. `IS` edges to Genre: 29.5M total (Rock 7,171,990 incl. Masters);
to Style: 33.9M.

PROFILE of the read part of each Genre/Style metric (Rock, then the heaviest Style):

| Statement | Result | Time | DbHits | Memory |
|-----------|--------|------|--------|--------|
| Rock `release_count` (`count(DISTINCT r)`) | 6,371,352 | 32.7s (cold) | 14.3M | 286 MB |
| Rock `artist_count` | 953,658 | 22.4s | 112.2M | 39.5 MB |
| Rock `label_count` | 623,120 | 8.8s | 112.8M | 23 MB |
| Rock `style_count` | 734 | 8.1s | 120.1M | 26 KB |
| Rock `first_year` | 1946 | 0.55s | 2.1M | 21 MB |
| Pop Rock (heaviest Style, 1.06M edges) `artist_count` | 93,797 | 1.9s | 19.4M | 3.3 MB |
| Pop Rock `label_count` | 74,053 | 1.8s | 19.6M | 2.8 MB |
| Pop Rock `genre_count` | 15 | 1.9s | 21.7M | 0.9 KB |
| Label 1818 (largest, 762,594 releases): all three Label subqueries | 762,594 / 490,698 / 15 | 10.3s | 21.4M | 26.6 MB |
| Person page: 1,000 Persons after "Bob" (18,532 credits) | n/a | 1.1s | 62K | 1.4 MB |

Reading: the slowest single Genre metric (Rock `release_count`, 33s) and Style metric
(Pop Rock, about 2s) are far below both the 120s base and 600s production transaction
limits, so the earlier risk that one metric cannot finish is **not borne out** for the
current graph. Rough scaling from these rates (estimate, not measured): the Genre sweep is
about 10s per million edges, about 5 minutes in total; the Style sweep is about 34M edges
at 5-10s per million, 3-6 minutes. The Person sweep is 6,262 pages at about 1s of reading
each, around 2 hours or more once the writes to eight indexed properties are added.
`first_year` is already cheap (the planner uses the year index).

## Ranked hot spots

| # | Hot spot | Cause | Proposed fix | Est. gain | Risk |
|---|----------|-------|--------------|-----------|------|
| 1 | Genre/Style stats metrics (`graphinator.py` `_node_stat_metrics`) | MEASURED: each Rock metric re-traverses all 7.2M `IS` edges (112-120M DbHits for artist/label/style). Worst single metric is `release_count` at 33s cold and 286 MB, because `count(DISTINCT r)` buffers 6.4M nodes though the `MERGE` already guarantees one edge per pair. Sum across all Genre/Style nodes is minutes, well under limits. | Use `count(r)` for `release_count` (no distinct set, edges are unique per pair, but verify no Master-labelled duplicates); share one traversal per node for the four counts. Keep paging as is. | Moderate: Genre/Style sweep maybe 2-3x less work, 286 MB less peak memory (estimate) | Low |
| 2 | Unconditional rewrite in Label and Person sweeps | Label page `SET`s three properties on every Label (7.3M properties set in the old run); Person page `SET`s `credit_count` plus every category on every Person, including zeros that did not change. Each write costs WAL, index maintenance and page-cache churn. | `SET` only when the value differs (`WHERE l.release_count <> rc OR ...`), keeping the zero-writing semantics for removed credits | Large on re-runs (most values unchanged; estimate), small on a first run | Low |
| 3 | Label stats page runs three separate traversals | Three `CALL {}` subqueries each re-expand `(l)<-[:ON]-(r:Release)`; MEASURED on the largest label (id 1818, 762,594 releases): 10.3s and 21.4M DbHits for the three subqueries, about 3x the single `ON` traversal | One traversal collecting `r`, then derive the three counts | ~2-3x on big labels (estimate from the measurement) | Low |
| 4 | Genre/Style supernode contention during load | Every release/master batch does `MERGE (g:Genre {name})` and `MERGE (r)-[:IS]->(g)` per row, repeating the same name up to batch-size times and taking write locks on a handful of very dense nodes. Plausible contributor to the 27 "Slow consumers detected" warnings. Hypothesis only; lock waits were not observed. | Deduplicate distinct genre/style names per batch (`UNWIND $names` → `MERGE` once), then `MATCH` the node for the edge `MERGE`. Verify with lock/wait metrics during a load. | Unknown until measured | Low-medium |
| 5 | `Style-[:PART_OF]->Genre` pairs repeated per release | `genre_style_data` carries one pair per release/style and runs three `MERGE`s for each; the same ~hundreds of distinct pairs repeat across 19.6M releases | De-duplicate pairs in Python before `UNWIND` (also in the master path) | Small-moderate, trivial | Very low |
| 6 | Eight prune statements per Release batch | `BY`, `ON`, `DERIVED_FROM`, `IS`→Genre, `IS`→Style, `ISSUED_ON`, `CREDITED_TO`, `CREDITED_ON` each seek every release again and expand its edges. On a first load the releases have no edges so these are cheap; on re-imports they dominate round-trips | Combine into one statement per release (single seek, expand once, delete by type predicate) | Moderate on re-import (estimate) | Medium: touches the edge-removal invariants |
| 7 | Person name index limit | RANGE index refused a 12,000-byte test value. MEASURED longest real name is 4,159 characters, which fits if mostly ASCII, but multibyte text can exceed the limit (byte length not measured; the wrapper returned characters via `size()`) | Measure the longest name in UTF-8 bytes on the live graph; if close to the limit, reject or normalise over-long credit names in `person_credit_rows` with a counted log, instead of failing the whole batch | Prevents a batch-level failure | Low |

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

See "Live evidence": every key the projector `MERGE`s on has a uniqueness constraint with a
backing RANGE index, so key lookups are indexed. Only `MaintenanceCheckpoint` and
`ExtractionCompletion` lack one (tiny). The eight indexed `credit_count*` Person
properties make unconditional rewrites expensive (hot spot 2).

## Do projected relationships earn their cost

`SAME_AS` (Person→Artist), `CREDITED_ON` (one edge per role, not per release) and `IS` to both
Genre and Style are the heaviest edge families. Whether readers use per-role `CREDITED_ON`
edges, rather than per-release, needs the catalog-api audit's query list; no conclusion here.

## Proposed follow-up beads (not filed)

1. Cheaper Genre/Style metrics (hot spot 1) — target: Rock `release_count` under 15s and under 50 MB; Genre+Style sweep under half its measured work.
2. Skip unchanged writes in Label and Person sweeps (2) — target: a re-run writes near zero properties.
3. Single-traversal Label stats (3) — target: Label stats at least 2x faster on the largest labels.
4. Deduplicate Genre/Style/PART_OF merges per batch (4, 5) — target: fewer "Slow consumers" warnings in a comparable load.
5. Consolidate Release prune statements (6) — target: measured re-import throughput gain.
6. Measure Person name bytes and guard the index limit (7).
7. Time the first real v0.5.0 paged-maintenance run on lux (writes included), which this audit could not do read-only.
