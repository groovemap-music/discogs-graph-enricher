"""Company credit and release-country projection into Neo4j (gm-discogs-graph-enricher-bfi.2, ADR 0011).

Both write paths must project the same graph from the same event: `Company` nodes,
`CREDITED_TO` edges carrying `role`, `role_category`, and `source`, and `Release.country`.
The manufacturing-chain lenses read those edges, so the two paths disagreeing would make a
release's credits depend on whether it arrived in a batch — which is why every case below
is asserted against the exact shared Cypher and run through both paths.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from graphinator.batch_processor import Neo4jBatchProcessor, PendingMessage
from graphinator.company_projection import (
    COMPANY_SOURCE,
    MERGE_COMPANY_CYPHER,
    PRUNE_CREDITED_TO_CYPHER,
    company_identity,
)
from graphinator.graphinator import process_release


# --- events ------------------------------------------------------------------------


def company_item(
    name: str,
    role: str,
    *,
    discogs_id: int | None = None,
    role_category: str = "pressing",
    entity_type: str | None = "17",
) -> dict[str, Any]:
    """Build one canonical companies entry in the producer's shape."""
    return {
        "name": name,
        "discogs_id": discogs_id,
        "role": role,
        "role_category": role_category,
        "catno": None,
        "source": {"provider": "discogs", "entity_type": entity_type},
    }


def companies_block(*items: dict[str, Any]) -> dict[str, Any]:
    """Build the canonical companies block around a set of entries."""
    return {
        "companies_version": "1",
        "items": list(items),
        "role_categories": sorted({item["role_category"] for item in items}),
        "unmapped": {"roles": []},
    }


def release_event(
    *,
    release_id: str = "R1",
    sha256: str = "hash-1",
    companies: dict[str, Any] | list[Any] | None = None,
    country: Any = "UK",
) -> dict[str, Any]:
    """Build the releases event a post-cutover producer emits.

    `companies` is omitted entirely when None, which is the pre-cutover record; `country`
    is omitted when None, which is a release whose country Discogs never stated.
    """
    record: dict[str, Any] = {
        "id": release_id,
        "title": "Contract Release",
        "year": 1997,
        "sha256": sha256,
        "formats": [{"name": "Vinyl", "qty": "1"}],
    }
    if companies is not None:
        record["companies"] = companies
    if country is not None:
        record["country"] = country
    return record


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
    def company_merges(self) -> list[RecordedCall]:
        return self.matching("MERGE (co:Company {id: c.id})")

    @property
    def company_prunes(self) -> list[RecordedCall]:
        return self.matching("[e:CREDITED_TO]")

    @property
    def release_node(self) -> RecordedCall:
        (call,) = self.matching("MERGE (r:Release {id: $id})") or self.matching("MERGE (r:Release {id: release.id})")
        return call


class _EmptyAsyncIterator:
    """An async-iterable Neo4j result with no rows (no release has a stored hash)."""

    def __aiter__(self) -> _EmptyAsyncIterator:
        return self

    async def __anext__(self) -> Any:
        raise StopAsyncIteration


async def run_single(record: dict[str, Any]) -> RecordingTx:
    """Drive the single-record path against a recording transaction."""
    tx = RecordingTx()
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


def merged_companies(tx: RecordingTx) -> list[dict[str, Any]]:
    """Return every CREDITED_TO row the recorded MERGE was handed."""
    return [row for call in tx.company_merges for row in call.params["companies"]]


def prune_records(tx: RecordingTx) -> list[dict[str, Any]]:
    """Return every prune record the recorded PRUNE was handed."""
    return [record for call in tx.company_prunes for record in call.params["records"]]


def release_country_written(tx: RecordingTx) -> Any:
    """Return the country the recorded Release MERGE was handed, in either path."""
    call = tx.release_node
    if "releases" in call.params:
        (release,) = call.params["releases"]
        return release["country"]
    return call.params["country"]


PATHS = [pytest.param(run_single, id="single"), pytest.param(run_batch, id="batch")]


async def run_path(path: Any, record: dict[str, Any]) -> RecordingTx:
    """Drive one release through whichever write path is under test."""
    return await (path(record) if path is run_single else path([record]))


# --- company nodes and credit edges ------------------------------------------------


@pytest.mark.parametrize("path", PATHS)
@pytest.mark.asyncio
async def test_two_companies_with_different_roles_are_two_nodes_and_two_edges(path: Any) -> None:
    """A pressing plant and a cutting room are separate companies, each with its role."""
    tx = await run_path(
        path,
        release_event(
            companies=companies_block(
                company_item("Damont", "Pressed By", discogs_id=12345, role_category="pressing"),
                company_item("Utopia Studios", "Lacquer Cut At", discogs_id=266218, role_category="lacquer", entity_type="30"),
            )
        ),
    )

    assert merged_companies(tx) == [
        {"release_id": "R1", "id": "12345", "name": "Damont", "role": "Pressed By", "role_category": "pressing"},
        {"release_id": "R1", "id": "266218", "name": "Utopia Studios", "role": "Lacquer Cut At", "role_category": "lacquer"},
    ]
    (call,) = tx.company_merges
    assert call.params["source"] == COMPANY_SOURCE
    assert "MERGE (co:Company {id: c.id})" in call.cypher
    assert "SET co.name = c.name" in call.cypher
    assert "MERGE (r)-[e:CREDITED_TO {role: c.role, source: $source}]->(co)" in call.cypher
    assert "SET e.role_category = c.role_category" in call.cypher


@pytest.mark.parametrize("path", PATHS)
@pytest.mark.asyncio
async def test_one_company_with_two_roles_is_one_node_and_two_edges(path: Any) -> None:
    """A plant that also distributed the record is one Company with two CREDITED_TO edges."""
    tx = await run_path(
        path,
        release_event(
            companies=companies_block(
                company_item("Damont", "Pressed By", discogs_id=12345, role_category="pressing"),
                company_item("Damont", "Distributed By", discogs_id=12345, role_category="distribution"),
            )
        ),
    )

    rows = merged_companies(tx)
    assert {row["id"] for row in rows} == {"12345"}
    assert [(row["role"], row["role_category"]) for row in rows] == [("Pressed By", "pressing"), ("Distributed By", "distribution")]


@pytest.mark.parametrize("path", PATHS)
@pytest.mark.asyncio
async def test_the_same_company_and_role_twice_is_one_edge(path: Any) -> None:
    """A duplicated source entry is one edge, not a second write of the same edge."""
    tx = await run_path(
        path,
        release_event(
            companies=companies_block(
                company_item("Damont", "Pressed By", discogs_id=12345),
                company_item("Damont", "Pressed By", discogs_id=12345),
            )
        ),
    )

    assert [(row["id"], row["role"]) for row in merged_companies(tx)] == [("12345", "Pressed By")]


@pytest.mark.parametrize("path", PATHS)
@pytest.mark.asyncio
async def test_a_company_without_a_discogs_id_gets_a_stable_derived_id(path: Any) -> None:
    """A plant with no Discogs label page is keyed on its normalized name, not dropped."""
    tx = await run_path(
        path,
        release_event(companies=companies_block(company_item("Damont Audio", "Pressed By"))),
    )

    (row,) = merged_companies(tx)
    assert row["id"] == "name:damont audio"
    assert row["name"] == "Damont Audio"


def test_the_derived_id_folds_case_and_spacing_but_never_punctuation() -> None:
    """Two spellings of one name reach one node; two names never collapse into one.

    A wrong merge cannot be taken apart again, so the normalization only folds the
    differences that are certainly not meaningful. `Company.name` keeps the name as the
    producer sent it, which is why the id and the stored name can differ in spacing.
    """
    spellings = ["Damont Audio", "damont audio", "  DAMONT   AUDIO  "]
    assert {company_identity({"name": spelling}) for spelling in spellings} == {"name:damont audio"}
    assert company_identity({"name": "BMG Records (UK) Ltd."}) != company_identity({"name": "BMG Records UK Ltd"})


@pytest.mark.parametrize("path", PATHS)
@pytest.mark.asyncio
async def test_entries_without_a_usable_identity_or_role_are_skipped(path: Any) -> None:
    """An entry with no name and no id, or no role, would key a node on nothing."""
    block = companies_block(
        company_item("", "Pressed By"),
        company_item("Damont", ""),
        company_item("Utopia Studios", "Lacquer Cut At", discogs_id=266218),
    )
    block["items"].insert(0, "Damont")  # A malformed entry that is not an object at all.
    tx = await run_path(path, release_event(companies=block))

    assert [row["id"] for row in merged_companies(tx)] == ["266218"]


@pytest.mark.parametrize("path", PATHS)
@pytest.mark.asyncio
async def test_an_unmapped_role_category_never_writes_a_null_edge_property(path: Any) -> None:
    """A missing category becomes `other`; `SET e.role_category = null` would delete it."""
    item = company_item("Damont", "Pressed By", discogs_id=12345)
    del item["role_category"]
    tx = await run_path(
        path, release_event(companies={"companies_version": "1", "items": [item], "role_categories": [], "unmapped": {"roles": ["Pressed By"]}})
    )

    (row,) = merged_companies(tx)
    assert row["role_category"] == "other"


# --- prune-then-merge --------------------------------------------------------------


@pytest.mark.parametrize("path", PATHS)
@pytest.mark.asyncio
async def test_prune_keeps_only_the_company_and_role_pairs_the_new_version_asserts(path: Any) -> None:
    """Re-projecting a corrected release deletes exactly the credit it dropped.

    The keep-list is pairs, not bare company ids: the release below keeps Damont as its
    distributor and drops it as its plant, so the prune has to delete one of the two edges
    to one node and leave the other standing.
    """
    corrected = await run_path(
        path,
        release_event(
            sha256="hash-2",
            companies=companies_block(
                company_item("Damont", "Distributed By", discogs_id=12345, role_category="distribution"),
                company_item("Utopia Studios", "Lacquer Cut At", discogs_id=266218, role_category="lacquer"),
            ),
        ),
    )

    assert prune_records(corrected) == [{"release_id": "R1", "keep": [["12345", "Distributed By"], ["266218", "Lacquer Cut At"]]}]
    (call,) = corrected.company_prunes
    assert call.params["source"] == COMPANY_SOURCE
    assert "MATCH (r:Release {id: record.release_id})-[e:CREDITED_TO]->(co:Company)" in call.cypher
    assert "WHERE e.source = $source AND NOT [co.id, e.role] IN record.keep" in call.cypher
    assert "DELETE e" in call.cypher


@pytest.mark.parametrize("path", PATHS)
@pytest.mark.asyncio
async def test_an_empty_block_prunes_every_credit_and_merges_none(path: Any) -> None:
    """An explicitly empty block is the "every company credit was removed" case."""
    tx = await run_path(path, release_event(companies=companies_block()))

    assert prune_records(tx) == [{"release_id": "R1", "keep": []}]
    assert tx.company_merges == []


@pytest.mark.parametrize("path", PATHS)
@pytest.mark.asyncio
async def test_a_record_with_no_canonical_block_writes_nothing(path: Any) -> None:
    """A pre-cutover record is silent about credits, so it neither prunes nor merges."""
    tx = await run_path(path, release_event(companies=None))

    assert tx.company_prunes == []
    assert tx.company_merges == []


@pytest.mark.parametrize("path", PATHS)
@pytest.mark.asyncio
async def test_a_raw_discogs_companies_list_is_not_mistaken_for_the_block(path: Any) -> None:
    """Pre-cutover, `companies` still holds the raw list the producer has not mapped yet."""
    tx = await run_path(
        path,
        release_event(companies=[{"id": 12345, "name": "Damont", "entity_type": "17", "entity_type_name": "Pressed By"}]),
    )

    assert tx.company_prunes == []
    assert tx.company_merges == []


@pytest.mark.parametrize("cypher", [MERGE_COMPANY_CYPHER, PRUNE_CREDITED_TO_CYPHER])
def test_credited_to_is_keyed_on_source_in_both_statements(cypher: str) -> None:
    """`source` is merge-key material, never a property written after the fact.

    Company nodes are shared across catalogs, so a release both this service and the
    MusicBrainz enricher credit carries one CREDITED_TO edge per provider to the same node.
    Merging on the role alone would match whichever edge already existed and overwrite the
    other catalog's assertion — and the prune, which is scoped to `e.source`, would then be
    deleting a different set of edges than the MERGE writes.
    """
    assert "MERGE (r)-[e:CREDITED_TO {role: c.role}]->(co)" not in cypher
    assert "SET e.source" not in cypher
    assert "$source" in cypher


def test_a_discogs_id_of_zero_is_not_an_identity() -> None:
    """Discogs uses 0 for "no label", so it falls back to the derived name id."""
    assert company_identity(company_item("Damont", "Pressed By", discogs_id=0)) == "name:damont"
    assert company_identity(company_item("Damont", "Pressed By", discogs_id=12345)) == "12345"
    assert company_identity({"name": "Damont", "discogs_id": "12345"}) == "12345"
    assert company_identity({"discogs_id": None, "name": None}) is None
    assert company_identity({"discogs_id": [12345], "name": None}) is None
    assert company_identity({"discogs_id": True, "name": "Damont"}) == "name:damont"


# --- release country ---------------------------------------------------------------


@pytest.mark.parametrize("path", PATHS)
@pytest.mark.asyncio
async def test_country_is_written_when_the_release_has_one(path: Any) -> None:
    """`Release.country` carries the release's country as received."""
    tx = await run_path(path, release_event(country="UK"))

    assert release_country_written(tx) == "UK"


@pytest.mark.parametrize("path", PATHS)
@pytest.mark.parametrize("country", [None, "", "   ", 7])
@pytest.mark.asyncio
async def test_country_is_null_when_the_release_has_none(path: Any, country: Any) -> None:
    """An absent, blank, or non-string country removes the property rather than storing it."""
    tx = await run_path(path, release_event(country=country))

    assert release_country_written(tx) is None


@pytest.mark.asyncio
async def test_country_is_set_on_create_and_on_match_in_the_single_record_path() -> None:
    """A release that already exists has its country corrected, not only its title."""
    tx = await run_single(release_event(country="US"))

    cypher = tx.release_node.cypher
    assert "ON CREATE SET r.title = $title, r.year = $year, r.country = $country," in cypher
    assert "ON MATCH SET r.title = $title, r.year = $year, r.country = $country," in cypher


@pytest.mark.asyncio
async def test_country_is_set_unconditionally_in_the_batched_path() -> None:
    """The batched MERGE's single SET is both branches of the single-record path."""
    tx = await run_batch([release_event(country="US")])

    assert "r.country = release.country," in tx.release_node.cypher


# --- the two paths agree -----------------------------------------------------------


@pytest.mark.parametrize(
    "companies",
    [
        pytest.param(None, id="no-block"),
        pytest.param(companies_block(), id="empty-block"),
        pytest.param(companies_block(company_item("Damont", "Pressed By", discogs_id=12345)), id="one-credit"),
        pytest.param(
            companies_block(
                company_item("Damont", "Pressed By", discogs_id=12345),
                company_item("Damont", "Distributed By", discogs_id=12345, role_category="distribution"),
                company_item("Utopia Studios", "Lacquer Cut At", role_category="lacquer"),
            ),
            id="shared-node-and-derived-id",
        ),
    ],
)
@pytest.mark.parametrize("country", ["UK", None])
@pytest.mark.asyncio
async def test_both_paths_project_the_same_credits_and_country(companies: Any, country: Any) -> None:
    """The two write paths are the same projection, event for event."""
    single = await run_single(release_event(companies=companies, country=country))
    batch = await run_batch([release_event(companies=companies, country=country)])

    assert merged_companies(single) == merged_companies(batch)
    assert prune_records(single) == prune_records(batch)
    assert release_country_written(single) == release_country_written(batch)
    assert [call.cypher for call in single.company_merges] == [call.cypher for call in batch.company_merges]
    assert [call.cypher for call in single.company_prunes] == [call.cypher for call in batch.company_prunes]


@pytest.mark.asyncio
async def test_a_batch_prunes_and_merges_every_release_that_carries_a_block() -> None:
    """One flat UNWIND per statement covers the whole batch, skipping only silent records."""
    tx = await run_batch(
        [
            release_event(release_id="R1", companies=companies_block(company_item("Damont", "Pressed By", discogs_id=12345))),
            release_event(release_id="R2", companies=companies_block()),
            release_event(release_id="R3", companies=None),
        ]
    )

    assert prune_records(tx) == [{"release_id": "R1", "keep": [["12345", "Pressed By"]]}, {"release_id": "R2", "keep": []}]
    assert [row["release_id"] for row in merged_companies(tx)] == ["R1"]
    assert len(tx.company_prunes) == 1
    assert len(tx.company_merges) == 1


@pytest.mark.asyncio
async def test_a_batch_of_only_silent_records_runs_no_company_statements() -> None:
    """The prune must not run with an empty record list when nothing asserts credits."""
    tx = await run_batch([release_event(release_id="R1", companies=None), release_event(release_id="R2", companies=None)])

    assert tx.company_prunes == []
    assert tx.company_merges == []
