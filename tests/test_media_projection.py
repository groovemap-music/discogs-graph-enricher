"""Canonical media projection into Neo4j (gm-discogs-graph-enricher-sq6.2, ADR 0007).

Both write paths must project the same graph from the same event: `Medium` and
`MediaFamily` nodes, `IN_FAMILY` and `ISSUED_ON` edges, and `Release.media_families`.
The fixtures under `tests/fixtures/media/` are copied verbatim from the design
repository's media taxonomy fixtures, so a producer-side mapping change that this
consumer has not absorbed shows up here rather than in production.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from graphinator.batch_processor import Neo4jBatchProcessor, PendingMessage
from graphinator.graphinator import process_release
from graphinator.media_projection import MEDIA_SOURCE, MERGE_MEDIA_CYPHER, PRUNE_ISSUED_ON_CYPHER


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "media"


def load_fixture(name: str) -> dict[str, Any]:
    """Return one design-repository media fixture."""
    return json.loads((FIXTURE_DIR / f"{name}.json").read_text())


def fixture_names() -> list[str]:
    """Return every Discogs media fixture name, sorted."""
    return sorted(path.stem for path in FIXTURE_DIR.glob("discogs-*.json"))


def release_event(fixture: dict[str, Any], *, release_id: str = "R1", sha256: str = "hash-1") -> dict[str, Any]:
    """Build the releases event a post-cutover producer emits for a fixture.

    The raw `formats` list and the canonical `media` block both ride the event; the
    block is the fixture's expected mapping output.
    """
    return {
        "id": release_id,
        "title": fixture["name"],
        "year": 1997,
        "sha256": sha256,
        "formats": fixture["input"].get("formats", []),
        "media": fixture["expected"],
    }


def expected_rows(fixture: dict[str, Any], *, release_id: str = "R1") -> list[dict[str, Any]]:
    """Return the ISSUED_ON rows a fixture's items should produce, ignoring labels."""
    rows: dict[str, dict[str, Any]] = {}
    for item in fixture["expected"]["items"]:
        row = rows.setdefault(
            item["medium"],
            {"release_id": release_id, "medium": item["medium"], "family": item["family"], "qty": 0},
        )
        row["qty"] += item["qty"]
    return list(rows.values())


# --- capture harnesses -------------------------------------------------------------


class RecordedCall:
    """One recorded `tx.run` call."""

    def __init__(self, cypher: str, params: dict[str, Any]) -> None:
        self.cypher = cypher
        self.params = params


class RecordingTx:
    """A fake Neo4j transaction that records the Cypher and parameters it is handed."""

    def __init__(self, existing_hash: str | None = None) -> None:
        self.calls: list[RecordedCall] = []
        self._existing_hash = existing_hash

    async def run(self, cypher: str, **params: Any) -> Any:
        self.calls.append(RecordedCall(cypher, params))
        result = MagicMock()
        result.single = AsyncMock(return_value={"hash": self._existing_hash} if self._existing_hash else None)
        return result

    def matching(self, needle: str) -> list[RecordedCall]:
        """Return the recorded calls whose Cypher contains a fragment."""
        return [call for call in self.calls if needle in call.cypher]

    @property
    def media_merges(self) -> list[RecordedCall]:
        return self.matching("MERGE (m:Medium {id: row.medium})")

    @property
    def media_prunes(self) -> list[RecordedCall]:
        return self.matching("[rel:ISSUED_ON]")

    @property
    def release_node(self) -> RecordedCall:
        (call,) = self.matching("MERGE (r:Release {id: $id})") or self.matching("MERGE (r:Release {id: release.id})")
        return call


async def run_single(record: dict[str, Any], tx: RecordingTx | None = None) -> RecordingTx:
    """Drive the single-record path against a recording transaction."""
    tx = tx or RecordingTx()
    await process_release(tx, record)
    return tx


async def run_batch(records: list[dict[str, Any]]) -> RecordingTx:
    """Drive the batched path against a recording transaction."""
    tx = RecordingTx()

    async def execute_write(tx_func: Any) -> Any:
        return await tx_func(tx)

    session = AsyncMock()
    session.run = AsyncMock(return_value=_EmptyAsyncIterator())
    session.execute_write = AsyncMock(side_effect=execute_write)

    session_context = AsyncMock()
    session_context.__aenter__.return_value = session
    session_context.__aexit__.return_value = None

    driver = MagicMock()
    driver.session = MagicMock(return_value=session_context)

    processor = Neo4jBatchProcessor(driver)
    await processor._process_releases_batch([PendingMessage("releases", record, AsyncMock(), AsyncMock()) for record in records])
    return tx


class _EmptyAsyncIterator:
    """An async-iterable Neo4j result with no rows (no release has a stored hash)."""

    def __aiter__(self) -> _EmptyAsyncIterator:
        return self

    async def __anext__(self) -> Any:
        raise StopAsyncIteration


def merged_rows(tx: RecordingTx) -> list[dict[str, Any]]:
    """Return every ISSUED_ON row the recorded MERGE was handed, labels stripped."""
    return [{key: value for key, value in row.items() if key != "label"} for call in tx.media_merges for row in call.params["rows"]]


# --- both paths, every design fixture ----------------------------------------------


@pytest.mark.parametrize("name", fixture_names())
@pytest.mark.asyncio
async def test_single_path_projects_every_design_fixture(name: str) -> None:
    """Every Discogs design fixture maps onto the ISSUED_ON rows its items describe."""
    fixture = load_fixture(name)
    tx = await run_single(release_event(fixture))

    assert merged_rows(tx) == expected_rows(fixture)
    assert tx.release_node.params["media_families"] == fixture["expected"]["families"]


@pytest.mark.parametrize("name", fixture_names())
@pytest.mark.asyncio
async def test_batch_path_projects_every_design_fixture(name: str) -> None:
    """The batched path emits the same rows and families as the single-record path."""
    fixture = load_fixture(name)
    tx = await run_batch([release_event(fixture)])

    assert merged_rows(tx) == expected_rows(fixture)
    (release,) = tx.release_node.params["releases"]
    assert release["media_families"] == fixture["expected"]["families"]


@pytest.mark.parametrize("name", fixture_names())
@pytest.mark.asyncio
async def test_both_paths_agree(name: str) -> None:
    """The two write paths are the same projection, fixture for fixture."""
    fixture = load_fixture(name)
    record = release_event(fixture)

    single = await run_single(record)
    batch = await run_batch([release_event(fixture)])

    assert merged_rows(single) == merged_rows(batch)
    assert [call.params["records"] for call in single.media_prunes] == [call.params["records"] for call in batch.media_prunes]


# --- node, edge, and property shape ------------------------------------------------


@pytest.mark.asyncio
async def test_medium_and_family_nodes_carry_the_taxonomy_label() -> None:
    """A 2xLP MERGEs one Medium with its vocabulary label, its family, and qty 2."""
    tx = await run_single(release_event(load_fixture("discogs-2xlp-gatefold-reissue")))

    (call,) = tx.media_merges
    assert call.params["rows"] == [{"release_id": "R1", "medium": "vinyl_12", "family": "vinyl", "label": '12" vinyl', "qty": 2}]
    assert call.params["source"] == MEDIA_SOURCE
    assert "MERGE (mf:MediaFamily {name: row.family})" in call.cypher
    assert "MERGE (m)-[:IN_FAMILY]->(mf)" in call.cypher
    assert "MERGE (r)-[e:ISSUED_ON {source: $source}]->(m)" in call.cypher
    assert "SET e.qty = row.qty" in call.cypher


@pytest.mark.parametrize("cypher", [MERGE_MEDIA_CYPHER, PRUNE_ISSUED_ON_CYPHER])
def test_issued_on_is_keyed_on_source_in_both_statements(cypher: str) -> None:
    """`source` is merge-key material, never a property written after the fact.

    Medium nodes are shared across catalogs, so a release both this service and the
    MusicBrainz enricher know about carries one ISSUED_ON edge per provider to the same
    node. Merging on the medium alone would match whichever edge already existed and
    overwrite the other catalog's assertion — and the prune, which is scoped to
    `rel.source`, would then be deleting a different set of edges than the MERGE writes.
    """
    assert "MERGE (r)-[e:ISSUED_ON]->(m)" not in cypher
    assert "e.source" not in cypher
    assert "$source" in cypher


@pytest.mark.asyncio
async def test_box_set_projects_its_sibling_media_not_the_container() -> None:
    """Box Set is a container, so the CD and vinyl siblings are the media edges."""
    tx = await run_single(release_event(load_fixture("discogs-box-set-cd-and-vinyl")))

    (call,) = tx.media_merges
    assert [(row["medium"], row["family"], row["qty"]) for row in call.params["rows"]] == [
        ("optical_cd", "optical", 2),
        ("vinyl_12", "vinyl", 1),
    ]


@pytest.mark.asyncio
async def test_unspecified_medium_still_gets_an_edge() -> None:
    """A known family with an unknown medium links to `<family>_unspecified`."""
    tx = await run_single(release_event(load_fixture("discogs-vinyl-size-unknown")))

    (call,) = tx.media_merges
    (row,) = call.params["rows"]
    assert row["medium"] == "vinyl_unspecified"
    assert row["family"] == "vinyl"
    assert row["label"] == "Vinyl"


@pytest.mark.asyncio
async def test_deprecated_formats_alias_is_still_written() -> None:
    """`r.formats` keeps its raw Discogs names alongside the canonical families."""
    tx = await run_single(release_event(load_fixture("discogs-box-set-cd-and-vinyl")))

    assert tx.release_node.params["formats"] == ["Box Set", "CD", "Vinyl"]
    assert tx.release_node.params["media_families"] == ["optical", "vinyl"]
    assert "r.formats = $formats" in tx.release_node.cypher
    assert "r.media_families = $media_families" in tx.release_node.cypher


@pytest.mark.asyncio
async def test_repeated_media_entries_collapse_to_one_edge_with_summed_qty() -> None:
    """ISSUED_ON is keyed on (release, medium, source), so duplicates sum their qty."""
    fixture = load_fixture("discogs-2xlp-gatefold-reissue")
    record = release_event(fixture)
    record["media"] = dict(fixture["expected"], items=[fixture["expected"]["items"][0]] * 3)

    tx = await run_single(record)

    (call,) = tx.media_merges
    (row,) = call.params["rows"]
    assert row["medium"] == "vinyl_12"
    assert row["qty"] == 6


# --- unmapped-only release ----------------------------------------------------------


@pytest.mark.asyncio
async def test_unmapped_only_release_gets_no_edges_and_empty_families() -> None:
    """An unrecognised format name yields no item, so no Medium node and no edge."""
    fixture = load_fixture("discogs-unknown-format")
    tx = await run_single(release_event(fixture))

    assert tx.media_merges == []
    assert tx.release_node.params["media_families"] == []
    (prune,) = tx.media_prunes
    assert prune.params["records"] == [{"release_id": "R1", "keep": []}]


@pytest.mark.asyncio
async def test_unmapped_only_release_gets_no_edges_in_the_batch_path() -> None:
    """The batched path agrees: no rows, empty families, prune with an empty keep-list."""
    fixture = load_fixture("discogs-unknown-format")
    tx = await run_batch([release_event(fixture)])

    assert tx.media_merges == []
    (release,) = tx.release_node.params["releases"]
    assert release["media_families"] == []
    (prune,) = tx.media_prunes
    assert prune.params["records"] == [{"release_id": "R1", "keep": []}]


# --- idempotency and edge cleanup ---------------------------------------------------


@pytest.mark.asyncio
async def test_reprocessing_is_idempotent_in_both_paths() -> None:
    """Re-processing the same release emits identical MERGE-only writes."""
    fixture = load_fixture("discogs-box-set-cd-and-vinyl")

    first = await run_single(release_event(fixture))
    second = await run_single(release_event(fixture))
    assert merged_rows(first) == merged_rows(second)

    batch_first = await run_batch([release_event(fixture)])
    batch_second = await run_batch([release_event(fixture)])
    assert merged_rows(batch_first) == merged_rows(batch_second)

    for call in first.media_merges + batch_first.media_merges:
        assert "CREATE (" not in call.cypher


@pytest.mark.asyncio
async def test_changed_media_prunes_the_stale_discogs_edges() -> None:
    """A release corrected from vinyl to CD keeps only the CD edge.

    ISSUED_ON is written with MERGE and is therefore additive, so the vinyl edge would
    survive forever without this prune and a medium-centric query would list the release
    under a medium it was never issued on. The delete is scoped to `rel.source`: the
    MusicBrainz enricher's edges over the same Medium nodes are its own assertions.
    """
    corrected = release_event(load_fixture("discogs-box-set-cd-and-vinyl"), sha256="hash-2")
    tx = await run_single(corrected, RecordingTx(existing_hash="hash-1"))

    (prune,) = tx.media_prunes
    assert prune.params["records"] == [{"release_id": "R1", "keep": ["optical_cd", "vinyl_12"]}]
    assert prune.params["source"] == MEDIA_SOURCE
    assert "WHERE rel.source = $source AND NOT m.id IN record.keep" in prune.cypher
    assert "DELETE rel" in prune.cypher


@pytest.mark.asyncio
async def test_prune_covers_every_release_in_a_batch() -> None:
    """The batched prune carries one record per release, media or not."""
    tx = await run_batch(
        [
            release_event(load_fixture("discogs-box-set-cd-and-vinyl"), release_id="R1"),
            release_event(load_fixture("discogs-unknown-format"), release_id="R2"),
        ]
    )

    (prune,) = tx.media_prunes
    assert prune.params["records"] == [
        {"release_id": "R1", "keep": ["optical_cd", "vinyl_12"]},
        {"release_id": "R2", "keep": []},
    ]


@pytest.mark.asyncio
async def test_unchanged_release_writes_no_media() -> None:
    """The hash gate short-circuits before any media statement runs."""
    record = release_event(load_fixture("discogs-2xlp-gatefold-reissue"))
    tx = RecordingTx(existing_hash=record["sha256"])

    assert await process_release(tx, record) is False
    assert tx.media_merges == []
    assert tx.media_prunes == []


# --- legacy fallback ----------------------------------------------------------------


@pytest.mark.parametrize("name", fixture_names())
@pytest.mark.asyncio
async def test_legacy_events_without_media_still_populate_the_graph(name: str) -> None:
    """A pre-cutover event carrying only `formats` gets a derived block, not nothing.

    The derivation is best-effort — it reads flat names rather than re-implementing the
    producer's mapping — so it is asserted to agree with the canonical block on the set
    of families rather than on every item detail.
    """
    fixture = load_fixture(name)
    legacy = release_event(fixture)
    del legacy["media"]

    tx = await run_single(legacy)

    assert tx.release_node.params["media_families"] == fixture["expected"]["families"]
    assert [row["medium"] for row in merged_rows(tx)] == [row["medium"] for row in expected_rows(fixture)]


@pytest.mark.asyncio
async def test_legacy_fallback_agrees_across_both_paths() -> None:
    """The fallback lives in one shared helper, so both paths derive the same block."""
    legacy = release_event(load_fixture("discogs-7-inch-45-single"))
    del legacy["media"]

    single = await run_single(legacy)
    batch = await run_batch([{**legacy}])

    assert merged_rows(single) == merged_rows(batch)
    assert merged_rows(single) != []


@pytest.mark.asyncio
async def test_event_with_neither_media_nor_formats_is_not_half_populated() -> None:
    """No media at all is an empty family list and an empty keep-list, never a crash."""
    tx = await run_single({"id": "R9", "title": "Bare", "sha256": "h"})

    assert tx.release_node.params["media_families"] == []
    assert tx.media_merges == []
    (prune,) = tx.media_prunes
    assert prune.params["records"] == [{"release_id": "R9", "keep": []}]


# --- defensive behavior on malformed blocks -----------------------------------------


@pytest.mark.asyncio
async def test_unknown_medium_id_is_labelled_with_its_own_id() -> None:
    """A producer on a newer taxonomy version keeps its edge, with a fallback label.

    Vendored vocabularies lag producers by a deploy. Losing the release's media over a
    cosmetic property would be a worse trade than a label a later re-process corrects.
    """
    fixture = load_fixture("discogs-2xlp-gatefold-reissue")
    record = release_event(fixture)
    record["media"] = dict(
        fixture["expected"],
        items=[dict(fixture["expected"]["items"][0], medium="vinyl_from_the_future")],
        families=["vinyl"],
    )

    tx = await run_single(record)

    (row,) = tx.media_merges[0].params["rows"]
    assert row["medium"] == "vinyl_from_the_future"
    assert row["label"] == "vinyl_from_the_future"


@pytest.mark.asyncio
async def test_malformed_items_are_skipped_rather_than_crashing_the_record() -> None:
    """An item missing a medium or a family cannot be an edge, and must not be fatal."""
    fixture = load_fixture("discogs-2xlp-gatefold-reissue")
    good = fixture["expected"]["items"][0]
    record = release_event(fixture)
    record["media"] = dict(
        fixture["expected"],
        items=["not-a-mapping", dict(good, medium=None), dict(good, family=""), good],
    )

    tx = await run_single(record)

    assert [row["medium"] for row in tx.media_merges[0].params["rows"]] == ["vinyl_12"]


@pytest.mark.parametrize("qty", [None, 0, -3, True, "2"])
@pytest.mark.asyncio
async def test_unusable_quantities_fall_back_to_one_unit(qty: Any) -> None:
    """`qty` is a unit count, so an absent or nonsensical value still means one unit."""
    fixture = load_fixture("discogs-2xlp-gatefold-reissue")
    record = release_event(fixture)
    record["media"] = dict(fixture["expected"], items=[dict(fixture["expected"]["items"][0], qty=qty)])

    tx = await run_single(record)

    (row,) = tx.media_merges[0].params["rows"]
    assert row["qty"] == 1


@pytest.mark.asyncio
async def test_malformed_formats_do_not_break_the_legacy_fallback() -> None:
    """A pre-cutover event with junk in `formats` derives from the entries it can read."""
    tx = await run_single(
        {
            "id": "R7",
            "title": "Ragged Legacy Event",
            "sha256": "h",
            "formats": ["not-a-mapping", {"qty": "1"}, {"name": "Vinyl", "descriptions": ["LP"]}],
        }
    )

    assert [row["medium"] for row in merged_rows(tx)] == ["vinyl_12"]


# --- two catalogs over one Medium node ----------------------------------------------
#
# Medium nodes are shared, so a release both this service and the MusicBrainz enricher
# know about carries one ISSUED_ON edge per provider to the same node. Asserting that
# the sibling catalog's edge survives needs the write to be *interpreted*, not just read
# as a string: the helpers below replay the recorded prune and MERGE against a list of
# edges using Neo4j's own matching rule, with the merge key and the SET clauses parsed
# out of the real Cypher constants. A regression to
# `MERGE (r)-[e:ISSUED_ON]->(m) SET e.qty = ..., e.source = ...` therefore fails these
# tests by rewriting the MusicBrainz edge, which is exactly the production symptom.

MERGE_PATTERN = re.compile(r"MERGE \(r\)-\[e:ISSUED_ON(?: \{([^}]*)\})?\]->\(m\)")
SET_CLAUSE = re.compile(r"\be\.(\w+) = (row\.\w+|\$\w+)")


def _value(expression: str, row: dict[str, Any], params: dict[str, Any]) -> Any:
    """Resolve a Cypher expression against the statement's row and parameters."""
    if expression.startswith("$"):
        return params[expression[1:]]
    return row[expression.split(".", 1)[1]]


def merge_key(cypher: str, params: dict[str, Any]) -> dict[str, Any]:
    """Return the inline property map MERGE uses to match an existing ISSUED_ON edge."""
    match = MERGE_PATTERN.search(cypher)
    assert match is not None, "the media MERGE no longer writes an ISSUED_ON edge"
    body = match.group(1)
    if not body:
        return {}
    key = {}
    for part in body.split(","):
        name, _, expression = part.partition(":")
        key[name.strip()] = _value(expression.strip(), {}, params)
    return key


def replay(tx: RecordingTx, existing: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Apply the recorded media writes to a starting set of ISSUED_ON edges."""
    edges = [dict(edge) for edge in existing]

    for call in tx.media_prunes:
        keep = {record["release_id"]: record["keep"] for record in call.params["records"]}
        edges = [
            edge
            for edge in edges
            if not (edge["source"] == call.params["source"] and edge["release_id"] in keep and edge["medium"] not in keep[edge["release_id"]])
        ]

    for call in tx.media_merges:
        key = merge_key(call.cypher, call.params)
        sets = SET_CLAUSE.findall(call.cypher)
        for row in call.params["rows"]:
            matched = next(
                (
                    edge
                    for edge in edges
                    if edge["release_id"] == row["release_id"]
                    and edge["medium"] == row["medium"]
                    and all(edge.get(name) == value for name, value in key.items())
                ),
                None,
            )
            if matched is None:
                matched = {"release_id": row["release_id"], "medium": row["medium"], **key}
                edges.append(matched)
            for name, expression in sets:
                matched[name] = _value(expression, row, call.params)

    return edges


def musicbrainz_edge(medium: str) -> dict[str, Any]:
    """An ISSUED_ON edge the sibling enricher already wrote for the same release."""
    return {"release_id": "R1", "medium": medium, "source": "musicbrainz", "qty": 1}


@pytest.mark.asyncio
async def test_a_musicbrainz_edge_to_the_same_medium_survives_untouched() -> None:
    """The Discogs projection adds its own edge beside the MusicBrainz one.

    `optical_cd` is a medium both catalogs assert for this release. With `source` in the
    merge pattern the two edges coexist; without it the Discogs MERGE would capture the
    MusicBrainz edge and rewrite its `source` and `qty`, contradicting ADR 0007's
    two-catalog model.
    """
    tx = await run_single(release_event(load_fixture("discogs-box-set-cd-and-vinyl")))

    edges = replay(tx, [musicbrainz_edge("optical_cd")])

    assert musicbrainz_edge("optical_cd") in edges
    assert {"release_id": "R1", "medium": "optical_cd", "source": "discogs", "qty": 2} in edges
    assert {"release_id": "R1", "medium": "vinyl_12", "source": "discogs", "qty": 1} in edges
    assert len(edges) == 3


@pytest.mark.asyncio
async def test_the_batch_path_leaves_the_musicbrainz_edge_alone_too() -> None:
    """Both write paths run the same statements, so both agree on the other catalog."""
    tx = await run_batch([release_event(load_fixture("discogs-box-set-cd-and-vinyl"))])

    edges = replay(tx, [musicbrainz_edge("optical_cd")])

    assert musicbrainz_edge("optical_cd") in edges
    assert len(edges) == 3


@pytest.mark.asyncio
async def test_the_prune_never_deletes_another_catalogs_edge() -> None:
    """A medium only MusicBrainz asserts is not in the Discogs keep-list, and survives.

    This is the case the `rel.source` filter exists for: the prune is asked to remove
    every Discogs edge outside the new set, and `digital_file` is outside it.
    """
    tx = await run_single(release_event(load_fixture("discogs-box-set-cd-and-vinyl")))

    edges = replay(tx, [musicbrainz_edge("digital_file")])

    assert musicbrainz_edge("digital_file") in edges


@pytest.mark.asyncio
async def test_a_stale_discogs_edge_is_still_pruned_beside_a_surviving_one() -> None:
    """Scoping the prune by source must not cost it its job on this catalog's own edges."""
    stale = {"release_id": "R1", "medium": "digital_file", "source": "discogs", "qty": 1}
    tx = await run_single(release_event(load_fixture("discogs-box-set-cd-and-vinyl")))

    edges = replay(tx, [stale, musicbrainz_edge("digital_file")])

    assert stale not in edges
    assert musicbrainz_edge("digital_file") in edges


@pytest.mark.asyncio
async def test_replaying_twice_leaves_the_graph_identical() -> None:
    """Idempotency, asserted on the resulting edges rather than on the statements."""
    start = [musicbrainz_edge("optical_cd")]
    once = replay(await run_single(release_event(load_fixture("discogs-box-set-cd-and-vinyl"))), start)
    twice = replay(await run_single(release_event(load_fixture("discogs-box-set-cd-and-vinyl"))), once)

    assert once == twice
