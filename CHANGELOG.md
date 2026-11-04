# Changelog

All notable changes to this repository will be recorded here by Commitizen from
Conventional Commits.

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
