-- One NDJSON line per distinct Label/Artist named by a release. Run with psql -At (unaligned,
-- tuples only) so output is not COPY-escaped:
--   psql -X -At -f scripts/stub-name-export.sql -o stub-names.ndjson
-- jsonb_typeof guards keep one non-array row from aborting the export.
SELECT jsonb_build_object('kind', 'Label', 'id', l.id, 'name', l.name)::text
FROM (
    SELECT DISTINCT ON (e->>'id') e->>'id' AS id, e->>'name' AS name
    FROM releases, jsonb_array_elements(data->'labels') e
    WHERE jsonb_typeof(data->'labels') = 'array' AND jsonb_typeof(e) = 'object'
      AND e->>'id' IS NOT NULL AND btrim(coalesce(e->>'name', '')) <> ''
    ORDER BY e->>'id'
) l
UNION ALL
SELECT jsonb_build_object('kind', 'Artist', 'id', a.id, 'name', a.name)::text
FROM (
    SELECT DISTINCT ON (e->>'id') e->>'id' AS id, e->>'name' AS name
    FROM releases, jsonb_array_elements(data->'artists') e
    WHERE jsonb_typeof(data->'artists') = 'array' AND jsonb_typeof(e) = 'object'
      AND e->>'id' IS NOT NULL AND btrim(coalesce(e->>'name', '')) <> ''
    ORDER BY e->>'id'
) a;
