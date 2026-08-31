# File and extraction completion

<div align="center">

**Intelligent file completion tracking and stalled detection management**

Last Updated: March 2026

[🏠 Back to Docs](README.md) | [🔄 Consumer Cancellation](consumer-cancellation.md)

</div>

## Overview

The service tracks Discogs file and extraction completion so that consumers drain in a
defined order, post-import maintenance runs once, and health data does not report
completed queues as stalled.

## How It Works

### 1. File Processing Lifecycle

```mermaid
graph LR
    A[File Processing Starts] --> B[Records Extracted]
    B --> C[Messages Sent to RabbitMQ]
    C --> D[file_complete Published to Fanout Exchange]
    D --> E[File Marked as Complete]
    E --> F[Consumer Cancellation Scheduled]
    F --> G[Stalled Detection Skips File]
    G --> H{All Files Done?}
    H -->|Yes| I[extraction_complete Sent to All Exchanges]
    I --> J[Post-Extraction Cleanup]

    style D fill:#f9f,stroke:#333,stroke-width:4px
    style E fill:#9f9,stroke:#333,stroke-width:4px
    style I fill:#ff9,stroke:#333,stroke-width:4px
    style J fill:#9ff,stroke:#333,stroke-width:4px
```

### 2. Completion Tracking

When a file finishes processing:

1. **`catalog-ingestion`** sends a `file_complete` message with:

   - `type`: "file_complete"
   - `data_type`: The type of data (artists, labels, masters, releases)
   - `timestamp`: Completion time
   - `total_processed`: Number of records processed
   - `file`: Original filename

1. **`catalog-ingestion`** marks the data type complete upstream.

1. **`discogs-graph-enricher`** receives the message and:

   - Mark the file as complete (🎉 in logs)
   - Schedule consumer cancellation after grace period

### 2a. Extraction Completion

After **all** Discogs files finish processing, `catalog-ingestion` sends an
`extraction_complete` message to all four Discogs fanout exchanges:

1. **`catalog-ingestion`** builds an `extraction_complete` message with:

   - `type`: "extraction_complete"
   - `version`: The Discogs data version (e.g., "20260301")
   - `timestamp`: Completion time
   - `started_at`: When the extraction began (used for stale row detection)
   - `record_counts`: Per-type record counts

1. **`discogs-graph-enricher`** receives the message on each queue, durably records
   the completion signal in Neo4j, and waits until all four signals name the same
   extraction version.

1. It then flushes remaining batches, deletes unresolved stub nodes without a `sha256`
   property, and recomputes aggregate graph statistics.

This ensures database counts match the extractor's record counts after each run.

### 3. Stalled Detection

The extractors' progress monitoring:

- Checks for files with no activity for >2 minutes
- **Excludes** files in the `completed_files` set
- Only reports actual stalls, not completed files

## Implementation Details

### Producer behavior

`catalog-ingestion` tracks completed files to prevent false stall warnings:

- Maintains a `completed_files` set
- Marks each data type as complete after sending the file completion message
- Excludes completed file types from stalled detection logic

### Progress Reporting

Enhanced progress reports show:

```
📊 Extraction Progress: 50000 total records extracted
(Artists: 20000, Labels: 15000, Masters: 10000, Releases: 5000)
✅ Completed file types: ['artists', 'labels']
✅ Active extractors: ['masters', 'releases']
```

## Benefits

1. **Accurate Monitoring**: No false warnings about completed files
1. **Clear Status**: Easy to see which files are done vs. active
1. **Resource Optimization**: Works with consumer cancellation for cleanup
1. **Better Debugging**: Clear indication of actual vs. false stalls

## Configuration

No additional configuration needed - the feature works automatically with existing settings.

### Related Environment Variables

- `CONSUMER_CANCEL_DELAY`: Grace period before canceling consumers (default: 300s)
- `FORCE_REPROCESS`: Set to "true" to reprocess all files

## Monitoring

### Log Messages to Watch

**`catalog-ingestion`**:

- `✅ Sent file completion message for {type}` - File marked complete
- `✅ Completed file types: [...]` - Shows all completed files
- `⚠️ Stalled extractors detected: [...]` - Only shows actual stalls

**Consumers**:

- `🎉 File processing complete for {type}!` - File completion received
- `🔧 Canceling consumer for {type}` - Cancellation scheduled
- `🏁 Received extraction_complete signal` - Extraction complete received
- `🧹 Cleaned up N stub {Label} nodes` - Neo4j stub-node cleanup

## Troubleshooting

### Issue: Still seeing stalled warnings for completed files

**Cause**: Service was restarted and lost completion state

**Solution**: The `completed_files` set is reset on restart. This is expected behavior - the warnings will stop once
files complete in the new session.

### Issue: Consumer not being canceled after completion

**Check**:

1. Verify `CONSUMER_CANCEL_DELAY` is not 0
1. Check logs for cancellation messages
1. Ensure RabbitMQ connection is stable

## Testing

Test the feature:

```bash
uv run pytest tests/test_file_completion.py tests/test_extraction_latch_durable.py
```

## Technical Architecture

### State Management

- `extraction_progress`: Tracks record counts per type
- `last_extraction_time`: Tracks last activity time per type
- `completed_files`: Set of completed data types
- State is reset when processing new files

### Integration Points

1. **`catalog-ingestion` → RabbitMQ**: Sends `file_complete` per data type
1. **`catalog-ingestion` → RabbitMQ**: Sends `extraction_complete` after all files finish
1. **`discogs-graph-enricher` → RabbitMQ**: Cancels completed queue consumers
1. **`discogs-graph-enricher` → Neo4j**: Persists the completion latch and performs
   post-extraction cleanup
1. **Health reporting**: Excludes completed files from stalled-consumer detection

## Future Enhancements

- [x] Persist extraction-completion signals in Neo4j across restarts
- [ ] Add completion timestamps to progress reports
- [ ] Create completion metrics for monitoring
- [ ] Add file-level (not just type-level) tracking
