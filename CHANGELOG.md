# Changelog

All notable changes to this repository will be recorded here by Commitizen from
Conventional Commits.

## v0.6.2 (2026-11-06)

### Release notes

No schema change. A parked extraction_complete is now delivered within about a second, and `/health` lists `awaiting_extraction_complete`. Batch holds and writes are bounded through a long Neo4j outage.

### Fix

- **consumers**: deliver extraction_complete parked on a cancelled consumer
- **batch**: bound writes and size capacity from the engine's hold fixes
- **batch**: bound batch holds through a long Neo4j outage

## v0.6.1 (2026-11-05)

### Fix

- **person-names**: strip padding from a credit name before it keys a Person
- **redelivery**: stop malformed records hot-looping and bound every requeue
- **markers**: keep the consumer attached while a marker waits out a failure
- **markers**: pace end-of-type markers through a Neo4j failure and bound them

## v0.6.0 (2026-11-05)

### Operator notes

- **First start writes to Neo4j and needs operator go-ahead.** The first start after this deploy runs
  the `STATS_VERSION` 1 refresh on its own, with no manual step. Estimated on lux: about 12 minutes and
  about 0.8 GB of store growth. Deploy it after the Discogs load has finished, and only with operator
  go-ahead. See `docs/file-completion-tracking.md`.
- **Publish nothing to graphinator's queues during the refresh.** An arriving catalog message stops the
  refresh between pages, and nothing retries it until the next restart or the next dump's pass.
- **Batch engine fix.** `groovemap-runtime` is pinned to `ecde48b`, which stops one busy queue starving
  every other queue's periodic flush and the memory growth that followed from it. The pin also carries
  the other runtime commits since the previous pin.

### Feat

- **maintenance**: refresh stats on a dump stamped by older code
- **maintenance**: precompute artist stats and label style counts

### Fix

- **projection**: bound Person names to what the unique index can hold

### Perf

- **maintenance**: compute Genre and Style counts with fewer traversals
- **projection**: prune a release batch's BY, ON, DERIVED_FROM and IS edges in one statement
- **projection**: merge each distinct Genre, Style and PART_OF pair once per batch
- **maintenance**: skip unchanged writes in Label and Person sweeps

## v0.5.0 (2026-11-04)

### Feat

- **graphinator**: stamp ExtractionCompletion.maintained_at when post-import maintenance completes

### Fix

- **docs**: export stub names with psql -At so quotes and backslashes stay valid JSON
- **graph**: name Artist and Label stubs from the release that references them
- **maintenance**: page and checkpoint the aggregate sweeps so they finish on the full graph

## v0.4.0 (2026-11-01)

### Feat

- **maintenance**: precompute per-category Person credit counts
- **graphinator**: read health port from HEALTH_PORT, default 8001

### Fix

- **graphinator**: preserve concurrent extraction signals durably
- **deps**: upgrade msgpack past CVE-2026-114613
- **graphinator**: exit 1 when startup gives up
- **deps**: upgrade filelock to 4.2.1 past CVE-2026-112940
- **deps**: upgrade certifi and opentelemetry-api past PYSEC-2026-4320 and CVE-2026-114603
- **graphinator**: treat master_id "0" as no master in every projection path
- **deps**: update urllib3 to 2.8.0 (#8)
- **consumers**: bound confirmed cancels and rearm extraction state

## v0.3.0 (2026-09-15)

### Feat

- **graph**: project companies, credit edges, and release country
- **batch**: adopt the shared Discogs runtime
- **telemetry**: trace deliveries and batch flushes, and sample loop lag

### Fix

- **ci**: accept commitizen's no-eligible-commits bump-preview state
- **deps**: bump python to 3.14.7-slim and dockerfile frontend to 1.27 (#1)
- **media**: preserve format qty in the legacy formats fallback

### Refactor

- **ci**: normalize validation recipes
- **batch**: isolate entity projections
- **graph**: separate entity projection

## v0.2.0 (2026-09-04)

### Feat

- **graph**: project canonical media as Medium and MediaFamily nodes
- **telemetry**: adopt common.telemetry and record pipeline metrics

### Fix

- **ci**: use public python libraries

### Refactor

- **graphinator**: adapt queue naming to the promoted single-source contract

## v0.1.1 (2026-08-31)

### Fix

- **ci**: accept release-boundary bump states and use Commitizen's supported files-only option

## v0.1.0 (2026-08-31)

The v0.1.0 workflow failed before publishing artifacts or images. The tag is
retained as an immutable record of that release attempt.
