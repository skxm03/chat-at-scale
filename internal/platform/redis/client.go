package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

var (
	ErrClientClosed      = errors.New("redis client is closed")
	ErrPresenceNotFound  = errors.New("user presence not found")
)

// Client wraps a pooled go-redis client with operations for sequencing (INCR) and presence.
type Client struct {
	client goredis.UniversalClient
	cfg    Config
	closed atomic.Bool
}

// New creates a new pooled Redis client.
func New(cfg Config) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate redis config: %w", err)
	}

	rdb := goredis.NewClient(cfg.ToOptions())
	return NewFromUniversalClient(rdb, cfg), nil
}

// NewFromUniversalClient allows injecting a custom or mock UniversalClient (seam for testing).
func NewFromUniversalClient(client goredis.UniversalClient, cfg Config) *Client {
	return &Client{
		client: client,
		cfg:    cfg,
	}
}

// Redis returns the underlying go-redis UniversalClient.
func (c *Client) Redis() goredis.UniversalClient {
	return c.client
}

// IncrSequence atomically increments and returns the conversation sequence number (ADR-003).
// Key format: conv:{conversation_id}:seq
func (c *Client) IncrSequence(ctx context.Context, conversationID string) (int64, error) {
	if c.closed.Load() {
		return 0, ErrClientClosed
	}

	key := fmt.Sprintf("conv:%s:seq", conversationID)
	val, err := c.client.Incr(ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("incr sequence for conversation %s: %w", conversationID, err)
	}
	return val, nil
}

// SetSequenceIfNotExists atomically initializes the sequence counter if it does not already exist (ADR-003 failover recovery).
// Returns true if the key was set, false if it already existed.
func (c *Client) SetSequenceIfNotExists(ctx context.Context, conversationID string, seq int64) (bool, error) {
	if c.closed.Load() {
		return false, ErrClientClosed
	}

	key := fmt.Sprintf("conv:%s:seq", conversationID)
	val, err := c.client.SetNX(ctx, key, strconv.FormatInt(seq, 10), 0).Result()
	if err != nil {
		return false, fmt.Errorf("setnx sequence for conversation %s: %w", conversationID, err)
	}
	return val, nil
}

// SetPresence records online/active presence status for a user with a TTL.
// Key format: presence:{user_id}
func (c *Client) SetPresence(ctx context.Context, userID string, status string, ttl time.Duration) error {
	if c.closed.Load() {
		return ErrClientClosed
	}

	key := fmt.Sprintf("presence:%s", userID)
	if err := c.client.Set(ctx, key, status, ttl).Err(); err != nil {
		return fmt.Errorf("set presence for user %s: %w", userID, err)
	}
	return nil
}

// GetPresence retrieves the current presence status of a user.
// Returns ErrPresenceNotFound if the key does not exist or has expired.
func (c *Client) GetPresence(ctx context.Context, userID string) (string, error) {
	if c.closed.Load() {
		return "", ErrClientClosed
	}

	key := fmt.Sprintf("presence:%s", userID)
	val, err := c.client.Get(ctx, key).Result()
	if errors.Is(err, goredis.Nil) {
		return "", ErrPresenceNotFound
	}
	if err != nil {
		return "", fmt.Errorf("get presence for user %s: %w", userID, err)
	}
	return val, nil
}

// Ping verifies client connectivity respecting context cancellation.
func (c *Client) Ping(ctx context.Context) error {
	if c.closed.Load() {
		return ErrClientClosed
	}

	if err := c.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("ping redis: %w", err)
	}
	return nil
}

// Close gracefully closes the client and connection pool.
func (c *Client) Close() error {
	if c.closed.CompareAndSwap(false, true) && c.client != nil {
		return c.client.Close()
	}
	return nil
}

