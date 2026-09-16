# Changelog

All notable changes to this repository will be recorded here by Commitizen from
Conventional Commits.

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
