# Shared batch runtime migration attestation

This consumer is pinned in both `pyproject.toml` and `uv.lock` to the reviewed
`groovemap-runtime` source revision
`e372b6a7598ae31ee6578fdff39bc920bedd7136`, whose Git tree is
`5dfee55fd070deb3463e5eb54944561dc969ec40`.

`common.batch.AsyncBatchEngine` now owns the transport-neutral keyed queues,
backpressure, concurrency, retry accounting, poison isolation, cancellation
restoration, drain, and exactly-once settlement lifecycle. The local adapters retain
Discogs normalization, Neo4j projection and exception mapping, `gm_telemetry` flush
spans and outcomes, RabbitMQ QoS, control-message state, and maintenance.

The migration was verified from the consumer baseline
`622cc56df9beb1828459bd8615b29fc87775839f` with:

```console
uv run pytest -q tests/test_batch_processor.py tests/test_batch_processor_integration.py tests/test_tracing.py
just test-integration
just check
just image
```

The credential-free suite exercises the owner adapters and shared-engine lifecycle.
The disposable Neo4j tier exercises successful graph writes, transient recovery,
deterministic poison isolation, shutdown drain, and exactly-once member settlement
through the shared engine.
