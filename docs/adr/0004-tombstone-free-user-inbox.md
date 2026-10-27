# ADR-004: Tombstone-Free User Inbox Table

## Date

2026-10-27

## Context

Users need an inbox view displaying their active conversations sorted by most recent message activity (`last_activity_at DESC`).

In Apache Cassandra, primary keys (both partition and clustering columns) are physically immutable on disk. In an initial design where `PRIMARY KEY (user_id, last_activity_at, conversation_id)` was clustered by `last_activity_at DESC`:

- Updating `last_activity_at` when a new message arrives cannot be done in-place.
- The system must write a new row with the new timestamp and `DELETE` the old row.
- Deletions in Cassandra write **tombstones** to SSTables. High message throughput across multiple groups causes tombstones to accumulate rapidly, resulting in query timeouts (`ReadFailureException: Scanned over 100000 tombstones`).

## Decision

We model the user inbox table using a composite primary key without the timestamp in the key:

```cql
CREATE TABLE chat.user_conversations (
    user_id uuid,
    conversation_id uuid,
    title text,
    type text,
    last_activity_at timestamp,
    last_message_preview text,
    PRIMARY KEY (user_id, conversation_id)
);
```

- `last_activity_at` is a regular column. Updating it on an incoming message is a pure LSM upsert on the exact same row: **zero deletions and zero tombstones**.
- When rendering the inbox, the Gateway queries `SELECT * FROM chat.user_conversations WHERE user_id = ?`. Because users typically belong to tens or at most a few hundred conversations, reading this entire partition takes $< 2\text{ ms}$, and the Gateway sorts the conversations by `last_activity_at DESC` in memory before returning to the client.

## Alternatives Considered

### 1. Clustering by `last_activity_at DESC` in Cassandra

- **Pros:** Cassandra returns rows pre-sorted directly from the database.
- **Cons:** Requires a delete-and-insert cycle on every message received in any conversation, generating a continuous stream of tombstones that degrades read latency over time.
- **Rejected:** Causes severe SSTable degradation and tombstone read failures under high message volume.

## Consequences

- The database stores one row per conversation per user with zero tombstone accumulation.
- In-memory sorting of 20–100 conversations occurs on the Gateway or client, adding negligible CPU overhead ($< 0.1\text{ ms}$).
