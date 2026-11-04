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
distinct entity, exported from the `releases` table in Postgres (`data` is the release JSON,
e.g. `labels = [{"id": "11358", "name": "RCA Victor", "catno": "104037"}]`):

```sh
psql -X -At -f scripts/stub-name-export.sql -o stub-names.ndjson
```

Use `psql -At` (unaligned, tuples only), not `\copy`: COPY's text format doubles backslashes
and so breaks the JSON for any name containing a quote or backslash. The script
(`scripts/stub-name-export.sql`) skips rows whose `labels`/`artists` is not an array, so one
odd row cannot abort the export. It only reads `data->'labels'` and `data->'artists'`.

The export reads only what is already in Postgres, so run it after the SQL load has finished.
Releases not loaded yet contribute no names, and their stubs would stay nameless.

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
