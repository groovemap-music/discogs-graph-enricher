# Database schema: the media graph model

This document is the authoritative description of the canonical media graph model
that `discogs-graph-enricher` writes, and of the deprecation of `Release.formats`
that it superseded. For the full node and relationship catalog beyond media (Artist,
Label, Release, Master, Genre, Style, Person, User) see the
[Graph Data Model](../graphinator/README.md#graph-data-model) section of the service
reference; this document only goes deep on media.

The model is defined by [ADR 0007: canonical media taxonomy][adr-0007] in the
`groovemap-music/design` repository, which also defines the vendored taxonomy this
service reads (`common.media`) and the JSON Schema for the canonical `media` block
every `releases` event carries, at
[`taxonomy/media/v1/media-block.schema.json`][block-schema] in that repository. This
service is a consumer of that taxonomy, not a second implementation of it.

## Nodes and relationships

```
(:Medium {id, family, label})                          unique on id
(:MediaFamily {name})                                   unique on name
(:Medium)-[:IN_FAMILY]->(:MediaFamily)
(:Release)-[:ISSUED_ON {qty, source: "discogs"}]->(:Medium)
```

- **`Medium`** — one canonical medium (`vinyl_12`, `cd`, `cassette`, ...). `id` and
  `label` come from the vendored taxonomy, so `vinyl_12` names the same thing here,
  in the relational store, and in the API. `family` is copied onto the node (not just
  reachable via `IN_FAMILY`) so a query can filter by family without a traversal.
- **`MediaFamily`** — the family a medium belongs to (`vinyl`, `optical`, `digital`,
  `cassette`, ...). Keyed on `name` alone.
- **`IN_FAMILY`** — `Medium` → `MediaFamily`. Static per medium id; not scoped by
  provenance because it describes the taxonomy, not a specific catalog's assertion.
- **`ISSUED_ON`** — `Release` → `Medium`, one edge per `(release, medium, source)`.
  - `qty` — the number of physical/logical units of that medium the release
    comprises (`2` for a 2xLP). Format entries that resolve to the same canonical
    medium are summed into one edge rather than producing duplicates.
  - `source` — which catalog asserted the edge (`"discogs"` for this service). It is
    part of the `MERGE` key, not a property set afterwards, because `Medium` nodes
    are shared across catalogs: a release known to both this service and
    `musicbrainz-graph-enricher` carries one `ISSUED_ON` edge per provider to the
    same `Medium` node. Merging on the medium alone would match whichever edge
    already existed and silently overwrite the other provider's assertion. The
    MusicBrainz enricher stamps `source: "musicbrainz"` on its own edges over the
    same nodes, so the two providers' assertions coexist without collision.

## `Release.media_families`

`Release.media_families` is a list property mirroring `media.families` from the
event's canonical media block — the sorted set of family ids the release's media
resolve to (e.g. `["vinyl"]`, or `["vinyl", "cd"]` for a release issued on both). It
exists so a family filter (or a check like "does this release include vinyl") is a
property read, not a traversal through `ISSUED_ON` and `IN_FAMILY`.

## Re-processing and the source-scoped prune

Discogs records are mutable: a release corrected from Vinyl to CD gets a new
`sha256`, passes the hash gate, and is reprocessed. Before the additive `MERGE` runs,
this service deletes the `ISSUED_ON` edges the release's new version no longer
asserts:

```cypher
UNWIND $records AS record
MATCH (r:Release {id: record.release_id})-[rel:ISSUED_ON]->(m:Medium)
WHERE rel.source = $source AND NOT m.id IN record.keep
DELETE rel
```

The `WHERE rel.source = $source` clause scopes the delete to this service's own
edges — a `musicbrainz`-sourced `ISSUED_ON` edge on the same release is another
provider's assertion and is left untouched. The prune runs unconditionally, including
when a release's new version asserts no media at all: an empty `keep` list is exactly
how "every medium was removed from this release" is applied. Without the prune, a
release corrected from Vinyl to CD would keep the stale Vinyl edge forever, and a
medium-centric query (find releases issued on shellac, say) would list it under a
medium it was never actually issued on by that version.

## Legacy fallback for events without `media`

A pre-cutover producer sends only the raw `formats` list, without a `media` block.
Rather than leave those releases with a `formats` property and no `Medium` edges at
all, the service derives a best-effort media block from the format names and their
descriptors (`common.media.legacy_format_names_to_media`). The fallback is
deliberately the flat-name mapper, not the full Discogs mapper — this service reads
the taxonomy's own derivation rules rather than re-implementing the producer's
mapping logic a second time. A release whose formats are all unmapped by the fallback
yields no `ISSUED_ON` edges and an empty `media_families`.

An unmapped medium id encountered by a *post-cutover* producer (one running a newer
taxonomy version than this service's vendored copy) is not dropped: the `Medium` node
is still written, labelled with its own id in place of a taxonomy label, and a
warning is logged. This keeps the release's media in the graph — a later
redeployment with an updated taxonomy corrects the label on the next re-process,
rather than the whole record failing over a cosmetic property.

## `Release.formats` — deprecated

`Release.formats` is the deprecated raw Discogs format-name list. It is retained,
written unchanged, for one minor version after `media_families` and `ISSUED_ON`
shipped, to give consumers time to migrate off it. New queries should use
`media_families` for family-level filtering and `ISSUED_ON` → `Medium` for
medium-level detail; do not add new reads of `formats`.

## Example query: labels that issued on shellac

```cypher
MATCH (l:Label)<-[:ON]-(r:Release)-[e:ISSUED_ON {source: "discogs"}]->(m:Medium)-[:IN_FAMILY]->(mf:MediaFamily {name: "shellac"})
RETURN DISTINCT l.name
ORDER BY l.name
```

Scoping `ISSUED_ON` to `source: "discogs"` matches this service's own convention:
omit it to include MusicBrainz-asserted edges over the same `Medium` nodes as well.

[adr-0007]: https://github.com/groovemap-music/design/blob/main/docs/adr/0007-canonical-media-taxonomy.md
[block-schema]: https://github.com/groovemap-music/design/blob/main/taxonomy/media/v1/media-block.schema.json
