# Changelog

All notable changes to this repository will be recorded here by Commitizen from
Conventional Commits.

## v0.9.0 (2026-11-10)

### Release notes

After the last label_stats page and before the dump's stats_version is stamped, a new rank phase numbers every real label by catalog-api's label-longevity order: years_active desc, total_releases desc, then l.id as a string. Eligibility is the same as catalog-api's: a name, at least one dated release, and not a placeholder. Each label's rank is stored as `l.longevity_rank` (1..M). M is stored once, on the dump's ExtractionCompletion node, as `label_longevity_rank_of`. A label that is no longer eligible has its rank removed. Only changed ranks are written, in 10k-label transactions, so a rerun on unchanged stats writes nothing. STATS_VERSION is 4, so the first start after deploy runs one full stats sweep plus the rank phase (benchmarked locally at about 42 s for the first write and 5 s for a rerun, at lux's label count). The rank read holds about 420 MB of Neo4j transaction memory and must finish inside the transaction timeout. The placeholder constants are pinned to catalog-api by a contract check. catalog-api will serve the rank once its node_label change ships (gm-catalog-api-gkcu). No database-schema dependency.

### Feat

- **stats**: store each real label's longevity rank and the ranked count

## v0.8.0 (2026-11-09)

### Release notes

Label nodes gain stored longevity fields computed in the existing label_stats walk: first_year, last_year, years_active, total_releases (dated releases, year > 0) and peak_decade (earliest decade on a tie), matching catalog-api's /api/insights/label-longevity definitions; a label with no dated release stores total_releases = 0 and none of the others. Documented in graphinator/README.md under "Label longevity". STATS_VERSION is 3, so the first start after deploy runs one full stats sweep; catalog-api reads the stored fields once the newest ExtractionCompletion has stats_version >= 3 and falls back to a live traversal otherwise. The label_stats step reads about 4-5% more (an estimated 15-25 s on a 165 s step); the overall sweep stays shorter than before v0.7.0. No schema dependency.

### Feat

- **stats**: store label longevity on Label

## v0.7.0 (2026-11-09)

### Release notes

Genre and Style nodes gain `release_years` and `release_year_counts`, parallel lists of releases per year (documented in `graphinator/README.md`, "Genre/Style release series"). The separate `first_year` statement is folded into the `release_count` walk, and paged maintenance steps keep `started_at` in their checkpoints and log `duration_seconds`. `STATS_VERSION` is now 2, so the first start after deploy runs one full stats sweep (Genre/Style/Label, then Artist), about 4-5 minutes of Neo4j reads on the reference catalog (261 s read-side, down from 305 s). No schema dependency.

### Feat

- **stats**: store per-year release series on Genre and Style

### Fix

- **release**: keep breaking changes below 1.0 with major_version_zero

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
