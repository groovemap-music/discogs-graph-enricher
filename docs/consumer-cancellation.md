# Consumer cancellation and draining

<div align="center">

**Automatic consumer lifecycle management for completed file processing**

Last Updated: March 2026

</div>

## Overview

`discogs-graph-enricher` cancels each RabbitMQ consumer after its Discogs file has
completed processing. This frees broker resources while leaving other queues available
to finish and makes active versus completed work explicit in health data and logs.

## How It Works

### Consumer Cancellation Lifecycle

```mermaid
sequenceDiagram
    participant EXT as catalog-ingestion
    participant RMQ as RabbitMQ
    participant CONS as discogs-graph-enricher
    participant TIMER as Cancellation Timer

    EXT->>RMQ: Publish file_complete to fanout exchange
    RMQ->>CONS: Deliver file_complete via consumer queue
    CONS->>CONS: Mark file as complete (🎉)
    CONS->>TIMER: Schedule cancellation (300s grace period)

    Note over CONS,TIMER: Grace period (5 minutes)

    TIMER-->>CONS: Grace period expired
    CONS->>RMQ: Cancel consumer for queue
    CONS->>CONS: Update active consumers list
    CONS->>CONS: Log consumer status

    Note over CONS: Connection remains open<br/>for other queues

    style EXT fill:#fff9c4
    style RMQ fill:#fff3e0
    style CONS fill:#e0f2f1
    style TIMER fill:#ffebee
```

### Process Steps

1. When `catalog-ingestion` sends a `file_complete` message,
   `discogs-graph-enricher`:

   - Mark the file as complete (shows 🎉 in progress reports)
   - Schedule the consumer for that queue to be canceled after a grace period
   - The default grace period is 5 minutes (300 seconds)

1. After the grace period expires:

   - The consumer for that specific queue is canceled
   - The connection and channel remain open for other queues
   - Progress reports show which consumers are active vs. canceled

1. Benefits:

   - Frees up RabbitMQ resources (connections, channels, memory)
   - Clearer monitoring - easy to see which files are still being processed
   - Prevents unnecessary network traffic for completed queues

## Configuration

### Environment Variable

- `CONSUMER_CANCEL_DELAY`: Number of seconds to wait before canceling a consumer after file completion
  - Default: 300 (5 minutes)
  - Set to 0 to disable consumer cancellation
  - Can be set per service or globally

### Examples

```bash
# Start the service entry point with a short grace period
CONSUMER_CANCEL_DELAY=30 uv run discogs-graph-enricher

# Disable consumer cancellation
CONSUMER_CANCEL_DELAY=0 uv run discogs-graph-enricher
```

## Monitoring

### Progress Reports

The periodic progress reports now include consumer status:

```
📊 Progress: 1000 total messages processed (🎉 Artists: 500, Labels: 500, Masters: 0, Releases: 0)
🔧 Canceled consumers: ['artists']
✅ Active consumers: ['labels', 'masters', 'releases']
```

### Log Messages

Watch for these log messages:

- `🎉 File processing complete for {type}!` - File marked as complete
- `🔧 Canceling consumer for {type} after {delay}s grace period` - Consumer cancellation scheduled
- `✅ Consumer for {type} successfully canceled` - Consumer successfully canceled
- `❌ Failed to cancel consumer for {type}` - Cancellation failed (non-fatal)

## Testing

Run the focused regression tests:

1. **test_file_completion.py** - Tests the file completion message handling

```bash
uv run pytest tests/test_file_completion.py tests/test_shutdown_delivery_churn.py
```

## Edge Cases Handled

1. **Multiple Completion Messages**: If multiple completion messages are received, only one cancellation is scheduled
1. **Service Restart**: Consumer tags are lost on restart, but the feature continues to work for new messages
1. **Cancellation Failure**: Failures are logged but don't crash the service
1. **Grace Period**: Ensures all in-flight messages are processed before cancellation

## Technical Details

- Uses aio_pika's `queue.cancel(consumer_tag, nowait=True)` to cancel consumers
- Consumer tags are stored when consumers are created
- Cancellation tasks are tracked to allow proper cleanup on shutdown
- The `nowait=True` parameter prevents hanging if RabbitMQ is slow to respond

## catalog-ingestion integration

The upstream `catalog-ingestion` service integrates with consumer cancellation by:

1. **Sending File Completion Messages**: When a file finishes processing, the extractor sends a
   "file_complete" message
1. **Tracking Completed Files**: The extractor maintains a `completed_files` set to avoid false stalled warnings
1. **Progress Monitoring**: Completed files are excluded from stalled detection logic

This prevents the extractors from incorrectly reporting files as "stalled" when they have actually completed processing
and their consumers have been canceled.

### Extraction Completion Signal (March 2026)

After all files finish, `catalog-ingestion` sends an `extraction_complete` message to
all Discogs fanout exchanges. `discogs-graph-enricher` uses this signal to:

- **Flush remaining batches** before cleanup
- Delete stub Neo4j nodes (no `sha256` property) created by cross-type `MERGE`
  operations
- Recompute the aggregate properties consumed by graph-query services

This ensures database record counts match extractor counts after each run. See [File Completion Tracking](file-completion-tracking.md) and [Database Schema — Post-Extraction Cleanup](https://github.com/groovemap-music/database-schema) for details.
