"""Queue-naming adapter over the promoted catalog-events contract binding.

This module exists because the discogs-ingestion split producer's generated binding
has a different API from the retired combined catalog-ingestion one. The old,
multi-source binding exposed `DATA_TYPES`, `exchange_name(source, entity)`,
`queue_name(consumer, entity)`, and `dead_letter_exchange_name`/
`dead_letter_queue_name` helpers. `graphinator/catalog_contract.py` is now promoted
byte-for-byte from discogs-ingestion's generated binding and is Discogs-only: it
exposes `ENTITY_TYPES`, `exchange_name(entity)` (no source argument -- the binding is
already source-locked), and `queue_name(consumer, entity)`, with no dead-letter
helpers at all.

This module adapts that narrower API back to the local names the service and its
tests use (`DATA_TYPES`, `exchange_name`, `queue_name`), and derives dead-letter
names from `queue_name(...)` using `contracts/catalog-events/v1/contract.json`'s own
`queue.dead_letter_exchange_template` / `dead_letter_queue_template`
(``"{queue}.dlx"`` / ``"{queue}.dlq"``), which the generated binding does not expose
directly. [ADR 0005, "Source-owned catalog ingestion
repositories"](https://github.com/groovemap-music/design/blob/main/docs/adr/0005-source-owned-catalog-ingestion.md)
requires the resulting wire identifiers (exchange, queue, and dead-letter names) to
stay byte-identical across the producer split so in-flight messages are not
stranded; `tests/test_queue_names_frozen.py` pins them.
"""

from __future__ import annotations

from graphinator.catalog_contract import AMQP_EXCHANGE_TYPE, ENTITY_TYPES, EXCHANGE_PREFIX
from graphinator.catalog_contract import exchange_name as _exchange_name
from graphinator.catalog_contract import queue_name as _queue_name


__all__ = [
    "AMQP_EXCHANGE_TYPE",
    "DATA_TYPES",
    "DISCOGS_EXCHANGE_PREFIX",
    "dead_letter_exchange_name",
    "dead_letter_queue_name",
    "exchange_name",
    "queue_name",
]

DATA_TYPES = ENTITY_TYPES
DISCOGS_EXCHANGE_PREFIX = EXCHANGE_PREFIX


def exchange_name(entity: str) -> str:
    """Build a producer-owned Discogs exchange name."""
    return _exchange_name(entity)


def queue_name(consumer: str, entity: str) -> str:
    """Build a registered consumer queue name."""
    return _queue_name(consumer, entity)


def dead_letter_exchange_name(consumer: str, entity: str) -> str:
    """Build the dead-letter exchange name for a consumer queue."""
    return f"{queue_name(consumer, entity)}.dlx"


def dead_letter_queue_name(consumer: str, entity: str) -> str:
    """Build the dead-letter queue name for a consumer queue."""
    return f"{queue_name(consumer, entity)}.dlq"
