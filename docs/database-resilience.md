# Neo4j and RabbitMQ resilience

This document describes how `discogs-graph-enricher` handles Neo4j maintenance,
RabbitMQ interruptions, and shutdown without losing or prematurely settling catalog
events.

## Overview

The service uses resilient connections that automatically handle:

- Nightly database maintenance windows
- Temporary network issues
- Database restarts
- Connection timeouts
- Service unavailability

## Key Features

### 1. Circuit Breaker Pattern

Each database connection uses a circuit breaker to prevent cascading failures:

- **Closed State**: Normal operation, all requests pass through
- **Open State**: After 5 consecutive failures, rejects requests immediately
- **Half-Open State**: After 30-60 seconds, allows one test request

```python
# Circuit breaker configuration
failure_threshold: 5  # Number of failures before opening
recovery_timeout: 30 - 60  # Seconds before trying half-open
```

### 2. Exponential Backoff

Failed connections retry with exponential backoff:

```yaml
# Backoff configuration
initial_delay: 0.5-1.0    # Initial retry delay (seconds)
max_delay: 30-60         # Maximum retry delay
exponential_base: 2.0    # Delay multiplier
jitter: 25%              # Random jitter to prevent thundering herd
```

### 3. Connection Health Monitoring

#### Neo4j

- Driver-level connection pooling (max 50 connections)
- Built-in keep-alive mechanism
- Session-level health checks
- Automatic reconnection on SessionExpired

#### RabbitMQ

- Robust connections with automatic recovery
- Heartbeat monitoring (600 seconds)
- Channel-level recovery
- Publisher confirmations for reliability

### 4. Message Durability

During database outages:

1. **Messages remain in RabbitMQ** (persistent storage)
1. **Failed messages are requeued** with `nack(requeue=True)`
1. **Idempotency prevents duplicates** using SHA256 hashes
1. **No data loss** - messages wait until databases recover

## Service-Specific Implementation

### discogs-graph-enricher

- Uses `ResilientNeo4jDriver` with automatic reconnection
- Handles `ServiceUnavailable` and `SessionExpired` exceptions
- Requeues messages on connection failures
- Removed reactive 2-minute reconnection timer (now proactive)

## Configuration

### Environment Variables

No changes required to existing environment variables. The resilient connections use the same configuration:

```bash
# Neo4j
NEO4J_HOST=neo4j
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=password

# RabbitMQ
RABBITMQ_HOST=rabbitmq
RABBITMQ_USERNAME=groovemap
RABBITMQ_PASSWORD=groovemap
```

### Tuning Parameters

The following parameters can be adjusted in the code if needed:

```python
# Circuit Breaker
failure_threshold = 5  # Failures before circuit opens
recovery_timeout = 30  # Seconds before recovery attempt

# Retry Settings
max_retries = 5  # Maximum connection attempts
initial_delay = 1.0  # Initial retry delay
max_delay = 60.0  # Maximum retry delay

# Neo4j Settings
neo4j_max_connection_lifetime = 1800  # 30 minutes
neo4j_max_connection_pool_size = 50
neo4j_connection_acquisition_timeout = 60.0
```

## Behavior During Maintenance

When databases undergo nightly maintenance:

1. **Connection Detection**: Services detect connection loss within seconds
1. **Circuit Breaker Opens**: After 5 failures, prevents cascade
1. **Message Queuing**: New messages remain in RabbitMQ
1. **Exponential Backoff**: Retry attempts with increasing delays
1. **Recovery**: When database returns, connections automatically restore
1. **Message Processing**: Queued messages process in order
1. **Idempotency**: Duplicate prevention via SHA256 hashes

## Monitoring

### Health Endpoints

`discogs-graph-enricher` exposes health data at `http://localhost:8001/health`.
The response identifies the service as `discogs-graph-enricher` and includes active
consumers, completed files, message counts, and current work.

### Logging

Enhanced logging for connection events:

```
🔄 Creating new connection (attempt 1/5)
⚠️ Connection attempt 1 failed: Connection refused. Retrying in 1.2 seconds...
🔄 Creating new connection (attempt 2/5)
✅ Connection established successfully
🚨 Circuit breaker OPEN after 5 failures
🔄 Circuit breaker entering HALF_OPEN state
✅ Circuit breaker reset to CLOSED
```

### Metrics

The dashboard service (`/metrics` endpoint) provides Prometheus metrics for monitoring.

## Testing Database Outages

To test the resilience features:

### 1. Stop a Database

```bash
# Use the deployment repository to stop Neo4j or RabbitMQ in a disposable stack.
# Then observe the service health endpoint and structured log.
```

### 2. Observe Service Behavior

Watch the logs to see connection failures and circuit breaker activation:

```bash
tail -f /logs/discogs-graph-enricher.log
```

### 3. Restart Database

```bash
# Restart the stopped dependency with the deployment repository's compose command.
```

### 4. Verify Recovery

- Services should automatically reconnect
- Queued messages should process
- No data should be lost

## Best Practices

1. **Don't Panic**: Services handle outages automatically
1. **Monitor Logs**: Watch for extended outage warnings
1. **Check Queues**: Monitor RabbitMQ queue depths during outages
1. **Verify Recovery**: Ensure message processing resumes after recovery
1. **Test Regularly**: Simulate outages in non-production environments

## Troubleshooting

### Services Not Recovering

If services don't recover after database restart:

1. Check circuit breaker state in logs
1. Verify database is fully started and accepting connections
1. Restart `discogs-graph-enricher` through the deployment repository if needed

### Messages Not Processing

If messages remain queued after recovery:

1. Check service health endpoints
1. Verify database connectivity manually
1. Look for poison messages causing repeated failures
1. Check dead letter queues for poison messages (each consumer has its own DLQ)

### Performance Issues

If services are slow after recovery:

1. Check for message backlog in RabbitMQ
1. Monitor database connection pool usage
1. Consider increasing prefetch counts temporarily
1. Watch for circuit breaker flapping (rapid open/close)
