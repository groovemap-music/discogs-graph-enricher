# Stub node names and the backfill

A release lists the artists and labels it references with their names. When the referenced
entity has no record of its own in the dump, the release is the only place the name exists.
The projection (batch and single-record paths) now sets the name when it creates the stub:

- `ON CREATE SET name = <the release's name>`; `ON MATCH` only fills a missing name
  (`coalesce(existing, release name)`), so a release never overwrites a name.
- An entity's own record (artist/label) always sets its name, whichever arrived first.
- `sha256` stays unset on stubs, so they remain recognisable as stubs.
- A blank or missing name leaves the node unnamed. Masters get no name: the release's
  `master_id` is a bare id with no title, so `Master` stubs without their own record stay unnamed.

## Backfill for graphs loaded before this change

Stubs created earlier have `name = null`, and a re-import does not repair them (the release
hash gate skips unchanged releases). The names are in the release data, so the backfill reads
them from an NDJSON file, one `{"kind": "Label"|"Artist", "id": "...", "name": "..."}` per
distinct entity, e.g. from the `releases` table in Postgres (`data` is the release JSON):

```sql
\copy (
  SELECT json_build_object('kind', 'Label', 'id', l->>'id', 'name', l->>'name')
  FROM (SELECT DISTINCT ON (l->>'id') l
        FROM releases, jsonb_array_elements(data->'labels') l
        WHERE l->>'name' IS NOT NULL ORDER BY l->>'id') s(l)
  UNION ALL
  SELECT json_build_object('kind', 'Artist', 'id', a->>'id', 'name', a->>'name')
  FROM (SELECT DISTINCT ON (a->>'id') a
        FROM releases, jsonb_array_elements(data->'artists') a
        WHERE a->>'name' IS NOT NULL ORDER BY a->>'id') s(a)
) TO 'stub-names.ndjson'
```

Run it in the service image (same `NEO4J_*` environment as the service):

```sh
python -m graphinator.stub_name_backfill stub-names.ndjson --batch-size 5000 --checkpoint stub-names.ckpt
# add --dry-run to parse and count without writing
```

- **Batched**: each batch of `--batch-size` rows is its own transaction.
- **Fill-only**: it only sets `name` where it is still null, so it never overwrites an
  entity's own name and a repeat run changes nothing.
- **Resumable**: `--checkpoint` records the lines applied; rerun the same command to continue.
- It logs the count of nameless Artist/Label nodes before and after.
