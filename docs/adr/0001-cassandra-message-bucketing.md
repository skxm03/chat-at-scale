# ADR-001: Cassandra Message Partitioning via Sequence Bucketing

## Date

2026-10-27

## Context

The platform is designed for a university with 50,000 students and high-velocity channels (e.g. course announcements, campus-wide groups) that can accumulate over 5 million messages per conversation.

Apache Cassandra uses an LSM-tree storage engine where rows in a partition are indexed in memory and on disk within SSTables. Cassandra’s performance degrades significantly when any single partition exceeds 100 MB or 100,000 rows (leading to large heap allocation, GC pauses, and lengthy compaction cycles).

We must satisfy two conflicting requirements:

1. Support up to 5 million messages in a single group conversation without unbounded partition growth.
2. Ensure recent message history and scroll-back pagination execute in a single partition read under 50 ms.

## Decision

We partition the `chat.messages` table using a compound partition key and clustering order:

```cql
PRIMARY KEY ((conversation_id, bucket), message_seq)
WITH CLUSTERING ORDER BY (message_seq DESC);
```

where `bucket = sequence_number / 50000`.

- **Bucket size:** Fixed at 50,000 messages. At an average size of 500 bytes to 1 KB per message, a partition occupies 25 MB to 50 MB on disk, well below Cassandra’s 100 MB threshold.
- **Clustering order:** `message_seq DESC` ensures the newest messages are located at the head of the SSTable file, allowing queries fetching recent history (`LIMIT 50`) to return in single-digit milliseconds without scanning.

## Alternatives Considered

### 1. Unbucketed Conversation Partition (`PRIMARY KEY (conversation_id, message_seq)`)

- **Pros:** Querying history across any range is simple; no bucket calculation needed.
- **Cons:** A group with 5 million messages creates a single 2.5 GB to 5 GB partition on disk.
- **Rejected:** Unbounded partitions cause JVM heap exhaustion, severe compaction pauses, and query timeouts.

### 2. Time-Based Bucketing (`PRIMARY KEY ((conversation_id, year_month), message_seq)`)

- **Pros:** Naturally aligns with calendar time; straightforward archiving of old months.
- **Cons:** High message velocity skew. A dead 1:1 conversation has 2 messages in a month partition; a viral campus group has 500,000 messages in the same month partition.
- **Rejected:** Causes hotspotting and unpredictable partition sizes on disk.

### 3. Date-Hour Bucketing (`(conversation_id, YYYY_MM_DD_HH)`)

- **Pros:** Small partitions during spikes.
- **Cons:** Reading the last 50 messages in a quiet chat requires querying multiple empty hourly partitions sequentially, destroying latency.
- **Rejected:** Fails the single-partition read SLA for low-to-medium velocity chats.

## Consequences

- The History Service and Gateway calculate the target bucket deterministically: `bucket = seq / 50000`.
- Fetching initial history always targets `bucket = current_seq / 50000` with `LIMIT 50`, guaranteeing a single-partition read under 50 ms.
- When paginating backward across a 50,000 boundary (e.g. from seq 50,000 to seq 49,999), the query client transitions to `bucket - 1`.
