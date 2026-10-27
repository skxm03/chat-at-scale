-- ==============================================================================
-- Project B1: Chat at Scale - Apache Cassandra Schema
--
-- Design Goals:
-- 1. Single-partition reads (< 50ms) for recent history and scroll-back pagination.
-- 2. Fixed partition size: Bucketed by sequence number (bucket = seq / 50,000)
--    to prevent unbounded partitions even with 5+ million messages per room.
-- 3. Clustering order message_seq DESC to serve latest messages first from disk.
-- 4. High-water mark read receipts to eliminate per-member write storms.
-- 5. Auto-expiring deduplication table (TTL 24h) for at-least-once retries.
-- 6. Direct Conversation lookup for deterministic 1:1 chat deduplication.
-- 7. Tombstone-free user inboxes by keying (user_id, conversation_id).
-- ==============================================================================

CREATE KEYSPACE IF NOT EXISTS chat
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};
-- Note: In multi-node production deployment, replace SimpleStrategy with:
-- 'class': 'NetworkTopologyStrategy', 'datacenter1': 3

-- ------------------------------------------------------------------------------
-- 1. Users & Authentication Metadata
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS chat.users (
    user_id uuid,
    username text,
    display_name text,
    avatar_url text,
    password_hash text,           -- bcrypt or argon2 hash
    created_at timestamp,
    PRIMARY KEY (user_id)
);

-- Denormalized table for single-query login authentication:
-- Allows verifying credentials in 1 partition read without an extra hop to chat.users.
CREATE TABLE IF NOT EXISTS chat.users_by_username (
    username text,
    user_id uuid,
    password_hash text,
    PRIMARY KEY (username)
);

-- ------------------------------------------------------------------------------
-- 2. Conversations & Channels
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS chat.conversations (
    conversation_id uuid,
    type text,                    -- 'direct' or 'group'
    title text,
    created_by uuid,
    created_at timestamp,
    current_sequence_number bigint,
    last_message_preview text,
    last_message_at timestamp,
    PRIMARY KEY (conversation_id)
);

-- Deterministic 1:1 Direct Conversation lookup:
-- Guarantees Alice and Bob share exactly one DM without scanning.
-- Convention: user_lower_id < user_higher_id (lexicographical UUID order).
CREATE TABLE IF NOT EXISTS chat.direct_conversation_lookup (
    user_lower_id uuid,
    user_higher_id uuid,
    conversation_id uuid,
    created_at timestamp,
    PRIMARY KEY ((user_lower_id, user_higher_id))
);

-- ------------------------------------------------------------------------------
-- 3. Membership & User Inboxes
-- ------------------------------------------------------------------------------

-- Members of a conversation: queried during fan-out routing and permission checks
CREATE TABLE IF NOT EXISTS chat.conversation_members (
    conversation_id uuid,
    user_id uuid,
    role text,                    -- 'member', 'admin'
    joined_at timestamp,
    PRIMARY KEY (conversation_id, user_id)
);

-- User's active conversation list (Inbox):
-- Keyed by (user_id, conversation_id) to allow in-place updates of last_activity_at.
-- Prevents tombstone generation caused by deleting rows when timestamps change.
CREATE TABLE IF NOT EXISTS chat.user_conversations (
    user_id uuid,
    conversation_id uuid,
    title text,
    type text,                    -- 'direct' or 'group'
    last_activity_at timestamp,
    last_message_preview text,
    PRIMARY KEY (user_id, conversation_id)
);

-- ------------------------------------------------------------------------------
-- 4. Messages (Core Scale Table)
--
-- Partition Key: ((conversation_id, bucket))
-- Bucketing: bucket = sequence_number / 50000
-- A partition holds at most 50,000 messages (~25MB-50MB), well below Cassandra's
-- 100MB limit. Recent history queries always hit the current active bucket.
--
-- Clustering Key: message_seq DESC
-- Returns the newest messages at the top of the partition file without scanning.
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS chat.messages (
    conversation_id uuid,
    bucket int,
    message_seq bigint,
    message_id uuid,
    sender_id uuid,
    content text,
    message_type text,            -- 'text', 'image', 'file', 'system'
    client_timestamp timestamp,
    server_timestamp timestamp,
    PRIMARY KEY ((conversation_id, bucket), message_seq)
) WITH CLUSTERING ORDER BY (message_seq DESC);

-- ------------------------------------------------------------------------------
-- 5. Read Receipts (High-Water Mark Pattern)
--
-- Instead of 1 write per message per member, each member simply updates their
-- latest read sequence pointer (watermark).
-- ------------------------------------------------------------------------------

-- Query: "Who has read up to which sequence in this conversation?"
CREATE TABLE IF NOT EXISTS chat.conversation_read_state (
    conversation_id uuid,
    user_id uuid,
    last_read_seq bigint,
    last_read_at timestamp,
    PRIMARY KEY (conversation_id, user_id)
);

-- Query: "What is this user's read pointer in this conversation?"
CREATE TABLE IF NOT EXISTS chat.user_read_state (
    user_id uuid,
    conversation_id uuid,
    last_read_seq bigint,
    last_read_at timestamp,
    PRIMARY KEY (user_id, conversation_id)
);

-- ------------------------------------------------------------------------------
-- 6. Message Deduplication / Idempotency
--
-- When a client retries sending message_id, we check if it was already assigned
-- a sequence number. Records automatically expire after 24 hours.
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS chat.message_deduplication (
    conversation_id uuid,
    message_id uuid,
    message_seq bigint,
    server_timestamp timestamp,
    PRIMARY KEY (conversation_id, message_id)
) WITH default_time_to_live = 86400;
