# Chat at Scale

A distributed, horizontally scalable real-time messaging platform designed for a 50,000-student university. Built with Go, Apache Cassandra, Apache Kafka (KRaft), and Redis, the platform provides strict per-conversation message ordering, client gap detection, single-partition history reads under 50 ms for channels with 5+ million messages, and stateless WebSocket gateways that support graceful connection draining.

---

## Architecture Overview

The system is split into independent microservices communicating via gRPC synchronously on the ingestion path and Apache Kafka asynchronously for persistence and fan-out:

```
                         ┌─────────────┐
                         │ Client (WS) │
                         └──────┬──────┘
                                │ WebSocket Handshake (JWT Auth)
                         ┌──────▼──────┐
                         │   Gateway   │ (Stateless, TLS/WS termination, Connection Drain)
                         └──────┬──────┘
                                │ gRPC (SendMessage, SyncGap)
                         ┌──────▼──────┐
                         │  Messaging  │ (Membership check, Redis atomic sequencer)
                         └──────┬──────┘
                                │ Produce (acks=all, key = conversation_id)
                         ┌──────▼──────┐
                         │ Kafka Topic │ chat-messages (partitioned by conversation_id)
                         └──┬────────┬─┘
                            │        │
        ┌───────────────────┘        └────────────────────┐
        │ Consumer Group: history-writer                  │ Consumer Group: push-dispatcher
     ┌──▼───────────────┐                          ┌──────▼──────────────┐
     │  History Service │                          │ Push Dispatcher     │
     └────────┬─────────┘                          └──────┬──────────────┘
              │ CQL Batch Write                           │ Redis Pub/Sub / gRPC
     ┌────────▼─────────┐                          ┌──────▼──────────────┐
     │ Cassandra (5.0)  │                          │ Gateway (WS push)   │
     └──────────────────┘                          └─────────────────────┘
```

### Microservice Boundaries

- **Gateway Service (`cmd/gateway`)**:
  Stateless WebSocket and HTTP termination. Handles JWT authentication, client session tracking, translates WebSocket frames into internal gRPC calls, and executes graceful connection draining on `SIGTERM`.
- **Messaging Service (`cmd/messaging`)**:
  Validates conversation membership, assigns dense strictly increasing sequence numbers per conversation using atomic Redis counters (`INCR`), and produces ordered events to Kafka partitioned by `conversation_id`.
- **History Service (`cmd/history`)**:
  Consumes messages asynchronously from Kafka in batches and writes them to Cassandra. Serves paginated message history and gap catch-up requests from a single partition read.
- **Presence Service (`cmd/presence`)**:
  Maintains user online status, heartbeat TTLs, and typing indicators using Redis key-value storage.

---

## Core Technical Decisions & ADRs

All significant architectural trade-offs are documented under [`docs/adr/`](docs/adr/):

| ADR                                                              | Title                                     | Key Trade-off / Decision                                                                                                                         |
| ---------------------------------------------------------------- | ----------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| [ADR-001](docs/adr/0001-cassandra-message-bucketing.md)          | **Cassandra Message Bucketing**           | Capping partitions to 50,000 messages (`seq / 50000`) so 5M+ message groups never exceed 50 MB, guaranteeing single-partition reads under 50 ms. |
| [ADR-002](docs/adr/0002-high-water-mark-read-watermarks.md)      | **High-Water Mark Read Receipts**         | Tracking a single `last_read_seq` pointer per member to eliminate $N$-factor write storms in large lecture channels.                             |
| [ADR-003](docs/adr/0003-per-conversation-sequence-allocation.md) | **Per-Conversation Sequencing via Redis** | Using Redis atomic `INCR` to provide dense integers ($1, 2, 3...$) without Paxos latency or Kafka partition offset gaps.                         |
| [ADR-004](docs/adr/0004-tombstone-free-user-inbox.md)            | **Tombstone-Free User Inbox**             | Keying `chat.user_conversations` by `(user_id, conversation_id)` with in-place updates to avoid Cassandra deletion tombstones.                   |

---

## Target Scale & Demonstrate Goals

The architecture is built to meet the following production benchmarks:

- **Target Capacity:** 50,000 concurrent students.
- **Concurrent Connections:** 3,000–5,000 active WebSocket connections.
- **Throughput:** 300–500 messages per second sustained for 10 minutes.
- **Latency:** Delivery p99 under 500 ms; initial history load p95 under 50 ms.
- **Data Volume:** 20 million seeded messages across individual and group conversations.
- **Fault Tolerance:** Killing one gateway instance, one broker node, or one database node results in zero lost or reordered acknowledged messages.

---

## Quick Start

### Prerequisites

- **Go**: 1.25+ (configured for Go 1.26 toolchain)
- **Docker & Docker Compose**: v2.20+
- **Protoc**: Protocol Buffers compiler with `protoc-gen-go` and `protoc-gen-go-grpc`

### 1. Clone & Set Up Infrastructure

```bash
git clone https://github.com/skxm03/chat-at-scale.git
cd chat-at-scale

# Start Cassandra 5.0, Kafka 3.8 (KRaft), and Redis 7.4
make infra-up
```

### 2. Initialize Database Schema

Wait for Cassandra to complete startup (`docker compose ps`), then apply the CQL schema:

```bash
make init-db
```

### 3. Build & Run Services

```bash
# Compile protobuf contracts
make proto

# Build all microservices
make build

# Run unit and integration tests
make test
```

---

## Commands

| Command           | Description                                              |
| ----------------- | -------------------------------------------------------- |
| `make infra-up`   | Starts Cassandra, Kafka (KRaft), and Redis in background |
| `make infra-down` | Stops and removes all infrastructure containers          |
| `make infra-logs` | Tails logs from Cassandra, Kafka, and Redis containers   |
| `make init-db`    | Applies `scripts/schema.cql` to the Cassandra cluster    |
| `make proto`      | Compiles `.proto` schemas in `api/proto/` into Go code   |
| `make build`      | Builds all Go packages and microservices                 |
| `make test`       | Executes unit and integration test suites                |
| `make clean`      | Removes local compilation binaries                       |

---

## Project Structure

```
chat-at-scale/
├── api/
│   └── proto/                    # Protobuf schemas & generated gRPC code
│       └── messaging/v1/
├── cmd/                          # Service entry points
│   ├── gateway/                  # WebSocket / HTTP Gateway
│   ├── messaging/                # Ingestion & sequencing engine
│   ├── history/                  # Cassandra batch writer & history queries
│   └── presence/                 # Heartbeats & session presence
├── docs/
│   └── adr/                      # Architecture Decision Records
├── internal/
│   ├── gateway/                  # WS hub, connection pool, session draining
│   ├── messaging/                # Sequencing logic, Kafka producer, gRPC server
│   ├── history/                  # Cassandra query engine, Kafka consumer
│   ├── presence/                 # Redis TTL heartbeat processing
│   └── platform/                 # Infrastructure drivers (Cassandra, Kafka, Redis)
├── scripts/
│   └── schema.cql                # Complete Cassandra keyspace and table DDL
├── docker-compose.yml            # Local multi-service infrastructure
├── CONCEPTS.md                   # Deep-dive systems concepts and notes
├── ERRORS.md                     # Error root-cause analysis and debugging log
└── Makefile                      # Automation targets
```

---
