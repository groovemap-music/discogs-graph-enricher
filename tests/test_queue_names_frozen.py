"""Freeze the runtime identifiers ADR 0005 requires to stay stable.

The exchange, queue, and dead-letter names below are exactly what the retired
combined catalog-events binding (`catalog_contract.exchange_name("discogs", entity)`,
`queue_name("graphinator", entity)`, `dead_letter_exchange_name`/
`dead_letter_queue_name`, promoted from catalog-ingestion) produced for this
service's consumer key ("graphinator") across its four entities before the
discogs-ingestion promotion. ADR 0005, "Source-owned catalog ingestion
repositories" (design repo, docs/adr/0005-source-owned-catalog-ingestion.md),
requires these wire identifiers to remain stable across that producer split, since
renaming them would strand in-flight messages. This test pins `graphinator.queue_names`
-- the local adapter over the
newly promoted, discogs-only binding -- to those exact strings, and cross-checks
them against the promoted `contract.json`'s own precomputed `runtime_identifiers`,
so both sides of the promotion agree.
"""

import json
from pathlib import Path

from graphinator.queue_names import (
    dead_letter_exchange_name,
    dead_letter_queue_name,
    exchange_name,
    queue_name,
)


ROOT = Path(__file__).parent.parent
CONSUMER = "graphinator"

# Snapshot of what the retired combined binding produced for consumer="graphinator"
# (source="discogs") before the discogs-ingestion promotion. Do not regenerate this
# from current code -- it is the frozen baseline the adapter is checked against.
FROZEN_IDENTIFIERS = {
    "artists": {
        "exchange": "groovemap-discogs-artists",
        "queue": "groovemap-discogs-graphinator-artists",
        "dead_letter_exchange": "groovemap-discogs-graphinator-artists.dlx",
        "dead_letter_queue": "groovemap-discogs-graphinator-artists.dlq",
    },
    "labels": {
        "exchange": "groovemap-discogs-labels",
        "queue": "groovemap-discogs-graphinator-labels",
        "dead_letter_exchange": "groovemap-discogs-graphinator-labels.dlx",
        "dead_letter_queue": "groovemap-discogs-graphinator-labels.dlq",
    },
    "masters": {
        "exchange": "groovemap-discogs-masters",
        "queue": "groovemap-discogs-graphinator-masters",
        "dead_letter_exchange": "groovemap-discogs-graphinator-masters.dlx",
        "dead_letter_queue": "groovemap-discogs-graphinator-masters.dlq",
    },
    "releases": {
        "exchange": "groovemap-discogs-releases",
        "queue": "groovemap-discogs-graphinator-releases",
        "dead_letter_exchange": "groovemap-discogs-graphinator-releases.dlx",
        "dead_letter_queue": "groovemap-discogs-graphinator-releases.dlq",
    },
}


def test_adapter_matches_frozen_identifiers() -> None:
    for entity, expected in FROZEN_IDENTIFIERS.items():
        assert exchange_name(entity) == expected["exchange"]
        assert queue_name(CONSUMER, entity) == expected["queue"]
        assert dead_letter_exchange_name(CONSUMER, entity) == expected["dead_letter_exchange"]
        assert dead_letter_queue_name(CONSUMER, entity) == expected["dead_letter_queue"]


def test_frozen_identifiers_match_promoted_contract() -> None:
    contract = json.loads((ROOT / "contracts/catalog-events/v1/contract.json").read_text())
    runtime_identifiers = contract["runtime_identifiers"]
    for entity, expected in FROZEN_IDENTIFIERS.items():
        assert runtime_identifiers["exchanges"][entity] == expected["exchange"]
        queue_entry = runtime_identifiers["queues"][CONSUMER][entity]
        assert queue_entry["name"] == expected["queue"]
        assert queue_entry["dead_letter_exchange"] == expected["dead_letter_exchange"]
        assert queue_entry["dead_letter_queue"] == expected["dead_letter_queue"]
