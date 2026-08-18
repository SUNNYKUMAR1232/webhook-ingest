
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

# Solution 
## What was broken and why

There were three main problems.

1. **Duplicate events**
   
   The code first checked `EventExists()` and then called `InsertEvent()`.
   With concurrent requests, two requests could both see that the event did
   not exist and process it. This caused the account call count to increase
   more than once.

2. **Recording processing**
   
   Recording processing ran in the background using the HTTP request context.
   When the request finished, that context could be cancelled, so recording
   processing could fail. The error was also ignored.

3. **Cache race**
   
   Multiple requests could update the cache at the same time. The cache
   update was not properly protected, which could cause incorrect values.

## Deduplication strategy

I use `event_id` as the idempotency key because the provider keeps the same
`event_id` when it sends the same event again.

PostgreSQL is the source of truth. I made `event_id` unique and use:

`INSERT ... ON CONFLICT (event_id) DO NOTHING`

If the event is new, I update the call and account statistics.

If it is a duplicate, I do nothing.

The event insert, call update, and account statistics update are done in one
transaction. This makes sure the same event cannot be counted twice.

I considered Redis for deduplication, but chose PostgreSQL because the event
data is already stored there and PostgreSQL gives us durable uniqueness.

## At 10,000 webhooks/second

I would:

- Run multiple instances of the service (Horizontal Scaling).
- Optimize PostgreSQL connections and indexes.
- Move recording processing to a durable queue.
- Add workers with retries and backoff.
- Use Redis as a fast optimization if needed.
- Keep PostgreSQL as the final source of truth.
- Monitor database load, queue size, errors, and webhook latency.

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