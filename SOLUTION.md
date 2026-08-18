
# Diagnose the root cause
Your current flow is effectively:
```text
                    EventExists()
                        ↓
                    if !exists
                        ↓
                    InsertEvent()
                        ↓
                    UpsertCall()
                        ↓
                    IncrementAccountStats()
```
The problem is that EventExists() and InsertEvent() are separate database operations.

Two requests can arrive at the same time:

```text
Request A                 Request B
    │                         │
    ▼                         ▼
EventExists(false)       EventExists(false)
    │                         │
    ▼                         ▼
InsertEvent()            InsertEvent()
    │                         │
    ▼                         ▼
IncrementStats()         IncrementStats()
```
Both requests see that the event does not exist before either request inserts it.

This is a check-then-act race condition.

# Solution :
# Solution

## What was broken and why

The ingestion path used a check-then-insert flow with `EventExists()`
followed by `InsertEvent()`. Concurrent redeliveries could both observe
that an event did not exist and then both execute accounting, causing
call-count drift.

Recording processing also used the incoming HTTP request context inside
a background goroutine, so the context could be cancelled after the
webhook response. Recording errors were silently ignored.

The in-memory statistics cache performed concurrent read-modify-write
operations without locking.

## Deduplication strategy

I used the provider's stable `event_id` as the idempotency key and made
PostgreSQL the source of truth. `events.event_id` has a unique constraint
and the insert uses `INSERT ... ON CONFLICT (event_id) DO NOTHING`.

Only a newly inserted event proceeds to the call update and account
statistics update. These database operations are performed in one
transaction so duplicate deliveries cannot double-count and failures
cannot leave partial accounting state.

I considered Redis for deduplication, but PostgreSQL provides the durable
source of truth. Using Redis as the only deduplication mechanism could
create consistency problems if Redis succeeds while the database operation
fails.

## At 10,000 webhooks/second

I would horizontally scale the stateless ingestion service, use database
connection pooling and appropriate indexes, and move recording work to a
durable queue/outbox with persistent workers, retries, and backoff.

Redis could be used as a fast duplicate-filtering optimization while
PostgreSQL remains the durable idempotency source of truth. I would also
monitor queue depth, database contention, latency, failures, and duplicate
delivery rates.


## Final request flow
```text
POST /webhooks/calls
        |
        v
     Handler
        |
        v
     Ingest()
        |
        v
ProcessedTransactions()
        |
        v
      BEGIN
        |
        v
INSERT event_id
        |
   +----+----+
   |         |
duplicate    new
   |         |
   |         +----> Upsert Call
   |                  |
   |                  v
   |             Increment Stats
   |                  |
   |                  v
   |                COMMIT
   |                  |
   +------------------+
        |
        v
      200 OK
        |
        +----> Cache.Record()
        |
        +----> Recording Worker
```