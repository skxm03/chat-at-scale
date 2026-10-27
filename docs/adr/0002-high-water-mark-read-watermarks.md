# ADR-002: High-Water Mark Read Watermarks

## Date

2026-10-27

## Context

In a university chat system, group conversations can contain hundreds or thousands of members (e.g. lecture halls, department announcements).

Traditional messaging implementations record read receipts by creating an entry per member for each message, or updating an array of `read_by` user IDs on the message row itself. In a group with 1,000 members, publishing 1 message triggers 1,000 read updates as members open the channel, multiplying write traffic by a factor of $N$ (write amplification storm).

We need a read tracking strategy that:

1. Accurately tracks which messages each user has seen.
2. Scales without write amplification as conversation membership grows.

## Decision

We store a single **high-water mark pointer** (`last_read_seq`) per User per Conversation in:

```cql
CREATE TABLE chat.conversation_read_state (
    conversation_id uuid,
    user_id uuid,
    last_read_seq bigint,
    last_read_at timestamp,
    PRIMARY KEY (conversation_id, user_id)
);
```

- When a User views a conversation, the client sends their highest viewed sequence number (e.g. `seq = 450`).
- The server updates `last_read_seq = 450` in a single upsert.
- Unread count for a conversation is never stored as a mutable counter; it is calculated on demand:
  $$\text{unread\_count} = \text{conversation.current\_sequence\_number} - \text{user\_read\_state.last\_read\_seq}$$

## Alternatives Considered

### 1. Per-Message Read Receipts (`read_receipts (message_id, user_id, read_at)`)

- **Pros:** Precise per-message auditability (who read which exact message).
- **Cons:** A 1,000-student room reading 10 messages generates 10,000 database writes, overwhelming the write path.
- **Rejected:** Unacceptable write amplification on high-concurrency group chats.

### 2. Message Row Collection / Set (`read_by set<uuid>`)

- **Pros:** Keeps read state co-located with the message.
- **Cons:** Concurrent updates to Cassandra collection columns on the same row trigger read-before-write overhead and tombstone build-up in SSTables.
- **Rejected:** Mutating historical message rows creates severe SSTable compaction overhead.

## Consequences

- The write cost of read receipts is bounded to $1$ write per user session/read event, regardless of how many messages were delivered.
- Calculating unread counts requires reading the user's `last_read_seq` and comparing with `current_sequence_number`.
- Does not support granular "seen by Alice at 10:02" per individual message in the middle of a batch, which is standard for large group messaging platforms (e.g. Slack, Discord, Telegram).
