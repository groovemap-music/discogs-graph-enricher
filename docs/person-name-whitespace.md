# Person names with surrounding whitespace

`Person.name` is the node key, and `graph.person.name` in Postgres holds the same raw
string: `discogs-sql-loader` reads the credit name verbatim on purpose, so both stores key
a person the same way. A name that differs only by surrounding whitespace is therefore a
second person in both stores. This records how often that happens and the options.
Nothing here has been migrated.

**Update 2026-11-05:** the operator chose a full reset and fresh ingest, so the one-off merge
(steps 4 to 6 below) is cancelled. What ships is step 1: `common.person_names.strip_person_name`
is used by `person_credit_rows` here and by `discogs-sql-loader`'s `_credits`, so a fresh ingest
creates no padded Person in either store. A credit whose name is only padding is skipped and
counted (a warning with counts only). One measured name on lux is padded with U+2800 BRAILLE
PATTERN BLANK (category So), which the rule does not strip yet; adding it is a separate change in
`python-libraries`.

**Decision: option B, after the Discogs load.** The operator chose this on 2026-11-04. It
is filed as its own bead, and nothing has been migrated yet.

## Measured on lux, 2026-11-04

Read-only, on dump `20261101`, while the Discogs SQL load was still running.

| Neo4j | Count |
| --- | --- |
| Person nodes | 6,261,762 |
| Padded, by Cypher `trim` or Python `str.strip()` (the same 89 names) | 89 |
| Padded, also counting invisible format and control characters | 181 |
| Padded names whose stripped form is already a Person: real duplicates | 87 |
| `CREDITED_ON` edges on the 181 | 312 |
| of those, on the 87 duplicates | 96 |
| `SAME_AS` edges on the 181 | 164 |
| Names over 255 characters with surrounding whitespace | 0 of 28 |

The 92 names that `trim` and `strip()` miss end in characters such as U+200F RIGHT-TO-LEFT
MARK, U+200C ZERO WIDTH NON-JOINER and U+FEFF. Plain spaces account for most of the rest.

| Postgres (`graph.person`, about 4.94M rows) | Count |
| --- | --- |
| The 181 padded names present | 121 |
| Their stripped forms present | 85 |
| Space or tab padded, in a 2% sample | 3 of 97,025 |

Postgres held fewer persons than Neo4j because its load had not finished, so these are a
lower bound and need repeating before any migration. Padding exists in both stores.

Padding is short. No name carries more than 3 padding characters, and the longest name
that needs stripping is 29 characters once stripped. The earlier note that the longest
names are mostly padding up to 4,159 characters was wrong; see below.

## Options

| | A. Leave it | B. Strip at the source and merge once | C. Normalize only in search |
| --- | --- | --- | --- |
| Duplicates | 87 stay, and new ones can appear | Removed, and no new ones | Stay, but hidden in search |
| Credit counts | 96 credits stay on the wrong person | Correct | Stay split |
| Stores in step | Yes | Only if both change together | Yes |
| Risk | None | Person identity changes in two stores | Low |

Option C hides the result without fixing it, so it is not recommended on its own.

## Migration path for option B

The content-hash skip decides the order. A byte-identical release is skipped before its
credits are projected, so changing the projector alone never reaches existing nodes. The
one-off merge is required, and new rows alone are not enough.

1. Add one strip rule to `python-libraries`, covering Unicode whitespace and the format and
   control characters measured above, at the two ends of the name only. Use it in both
   `person_credits.person_credit_rows` and the loader's `_credits`, released together.
   Stripping in one store alone would split the two stores' keys.
2. Confirm whether the loader's derivation also skips unchanged documents. If it rebuilds
   its rows, Postgres converges by itself and needs no one-off step.
3. After the load, recount the padded names in both stores.
4. Merge each padded Person into its stripped form in Neo4j, in one resumable run: `MERGE`
   each `CREDITED_ON` on `(release, role)`, keeping `category`, and move `SAME_AS`, then
   delete the padded node. Do the same for `graph.person`, `graph.credited_on`,
   `graph.track_credited_on` and `graph.same_as`, whose keys can collide on the same way.
5. Let the next maintenance pass recount `credit_count`.
6. Verify that no padded names remain in either store, that the two stores agree on every
   merged name, and that each merged person's credit total equals the sum of the two nodes
   before the merge, less any edge both held.

A padded name's URL or bookmark stops resolving after the merge. With at most 181 names,
this needs no redirect.

## Related, not part of this decision

Both are different problems, and stripping fixes neither.

- **Several people in one name.** All 28 names over 255 characters are free text, 26 of
  them comma-separated lists of people, including the longest at 4,159 characters. They
  carry 51 credits.
- **Whitespace inside a name.** 568 names contain a run of two or more spaces, carrying 842
  credits, and 4 contain a run of ten or more. `Mercedita López` and `Carmín Méndez` are
  joined by 1,238 spaces in one name of 1,264 characters.
