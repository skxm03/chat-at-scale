# ADR-003: Per-Conversation Sequence Allocation via Redis

## Date

2026-10-27

## Context

The PRD mandates:

1. Strict per-conversation ordering.
2. Gap detection on clients: if a client receives sequence 101 and next receives 104, it must deterministically know that sequences 102 and 103 were dropped in transmission and trigger a sync catch-up.
3. No global bottleneck: sequence generation must scale horizontally across 50,000 users and sustain 300–500 messages per second.

This requirement necessitates **dense, strictly contiguous integers** ($1, 2, 3...$) per conversation.

## Decision

We assign sequence numbers using atomic Redis operations (`INCR conv:{conversation_id}:seq`) within the Messaging Service ingestion path, with Cassandra acting as the durable source of truth.

- **Atomic Sequencing:** Redis executes `INCR` in memory on a single-threaded event loop per key in sub-millisecond time ($\sim 0.2\text{ ms}$), providing dense integer assignment with zero lock contention across distributed Go nodes.
- **Horizontal Scaling:** When running Redis Cluster, keys hash by `CRC16(key)` across 16,384 slots. All increments for a given conversation route to the same slot, while independent conversations distribute evenly across cluster nodes without inter-node consensus latency.
- **Failover & Recovery:** When a Messaging Service instance encounters an uninitialized counter or after Redis restarts, it initializes the key from Cassandra:
    ```cql
    SELECT current_sequence_number FROM chat.conversations WHERE conversation_id = ?;
    ```
    and executes an atomic `SETNX` in Redis to ensure continuity.

## Alternatives Considered

### 1. Kafka Partition Offsets as Sequence Numbers

- **Pros:** Zero extra infrastructure; Kafka already provides monotonic 64-bit offsets.
- **Cons:** Kafka partitions multiplex multiple conversations. Offsets in partition 0 are shared across hundreds of rooms. Conversation A sees offsets `[100, 103, 108]`, while offsets `101`, `102` belong to Conversation B.
- **Rejected:** The client cannot distinguish between a lost message and an offset consumed by an unrelated conversation, making client gap detection impossible.

### 2. Cassandra Lightweight Transactions (Paxos LWT)

- **Pros:** Sequence generation lives entirely in the database; zero external dependency.
- **Cons:** Cassandra LWT executes Paxos consensus across replicas, requiring 4 network round trips per write. Under 300–500 messages/second, concurrent LWTs produce extreme lock contention, timeouts, and latency spikes $> 200\text{ ms}$.
- **Rejected:** Insufficient throughput and high latency under concurrency.

### 3. Distributed Snowflake 64-bit IDs

- **Pros:** Completely decentralized ID generation (timestamp + machine ID + local sequence) with zero network calls.
- **Cons:** Snowflake IDs are roughly time-ordered, but sparse and non-contiguous. Consecutive messages might have IDs `184729103` and `184729115`.
- **Rejected:** Lacks dense sequence guarantees, breaking deterministic client gap detection.

## Consequences

- The Messaging Service requires a connection to Redis during the message ingestion path.
- If Redis is unavailable, message ingestion halts until the Redis cluster is reached or the fallback sequencer engages.
- Redis AOF persistence (`appendonly yes`) is enabled to minimize counter regression on node restart.
