package redis_test

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/skxm03/chat-at-scale/internal/platform/redis"
)

type fakeRedisUniversalClient struct {
	goredis.UniversalClient
	closed   bool
	incrMap  map[string]int64
	kvMap    map[string]string
	pingErr  error
}

func newFakeRedis() *fakeRedisUniversalClient {
	return &fakeRedisUniversalClient{
		incrMap: make(map[string]int64),
		kvMap:   make(map[string]string),
	}
}

func (f *fakeRedisUniversalClient) Incr(ctx context.Context, key string) *goredis.IntCmd {
	cmd := goredis.NewIntCmd(ctx, "incr", key)
	if err := ctx.Err(); err != nil {
		cmd.SetErr(err)
		return cmd
	}
	f.incrMap[key]++
	f.kvMap[key] = strconv.FormatInt(f.incrMap[key], 10)
	cmd.SetVal(f.incrMap[key])
	return cmd
}

func (f *fakeRedisUniversalClient) SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *goredis.BoolCmd {
	cmd := goredis.NewBoolCmd(ctx, "setnx", key, value, expiration)
	if err := ctx.Err(); err != nil {
		cmd.SetErr(err)
		return cmd
	}
	if _, exists := f.kvMap[key]; exists {
		cmd.SetVal(false)
		return cmd
	}
	f.kvMap[key] = value.(string)
	cmd.SetVal(true)
	return cmd
}

func (f *fakeRedisUniversalClient) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *goredis.StatusCmd {
	cmd := goredis.NewStatusCmd(ctx, "set", key, value, expiration)
	if err := ctx.Err(); err != nil {
		cmd.SetErr(err)
		return cmd
	}
	f.kvMap[key] = value.(string)
	cmd.SetVal("OK")
	return cmd
}

func (f *fakeRedisUniversalClient) Get(ctx context.Context, key string) *goredis.StringCmd {
	cmd := goredis.NewStringCmd(ctx, "get", key)
	if err := ctx.Err(); err != nil {
		cmd.SetErr(err)
		return cmd
	}
	val, ok := f.kvMap[key]
	if !ok {
		cmd.SetErr(goredis.Nil)
		return cmd
	}
	cmd.SetVal(val)
	return cmd
}

func (f *fakeRedisUniversalClient) Ping(ctx context.Context) *goredis.StatusCmd {
	cmd := goredis.NewStatusCmd(ctx, "ping")
	if f.pingErr != nil {
		cmd.SetErr(f.pingErr)
		return cmd
	}
	if err := ctx.Err(); err != nil {
		cmd.SetErr(err)
		return cmd
	}
	cmd.SetVal("PONG")
	return cmd
}

func (f *fakeRedisUniversalClient) Close() error {
	f.closed = true
	return nil
}

func TestConfig_DefaultsAndOptions(t *testing.T) {
	cfg := redis.DefaultConfig()

	if cfg.Addr != "localhost:6379" {
		t.Errorf("expected default Addr localhost:6379, got: %s", cfg.Addr)
	}
	if cfg.PoolSize != 50 {
		t.Errorf("expected default PoolSize 50, got: %d", cfg.PoolSize)
	}
	if cfg.MinIdleConns != 10 {
		t.Errorf("expected MinIdleConns 10, got: %d", cfg.MinIdleConns)
	}
	if cfg.MaxIdleConns != 25 {
		t.Errorf("expected MaxIdleConns 25, got: %d", cfg.MaxIdleConns)
	}
	if cfg.ConnMaxIdleTime != 5*time.Minute {
		t.Errorf("expected ConnMaxIdleTime 5m, got: %v", cfg.ConnMaxIdleTime)
	}
	if cfg.ConnMaxLifetime != 30*time.Minute {
		t.Errorf("expected ConnMaxLifetime 30m, got: %v", cfg.ConnMaxLifetime)
	}

	opts := cfg.ToOptions()
	if opts.PoolSize != 50 {
		t.Errorf("expected opts.PoolSize 50, got: %d", opts.PoolSize)
	}
	if opts.MinIdleConns != 10 {
		t.Errorf("expected opts.MinIdleConns 10, got: %d", opts.MinIdleConns)
	}
	if opts.MaxIdleConns != 25 {
		t.Errorf("expected opts.MaxIdleConns 25, got: %d", opts.MaxIdleConns)
	}
}

func TestConfig_Validation(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*redis.Config)
		wantErr bool
	}{
		{
			name: "empty addr",
			mutate: func(c *redis.Config) {
				c.Addr = ""
			},
			wantErr: true,
		},
		{
			name: "zero pool size",
			mutate: func(c *redis.Config) {
				c.PoolSize = 0
			},
			wantErr: true,
		},
		{
			name: "negative DB",
			mutate: func(c *redis.Config) {
				c.DB = -1
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := redis.DefaultConfig()
			tt.mutate(&cfg)
			if err := cfg.Validate(); (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestClient_SequenceGenerator_ADR003(t *testing.T) {
	fake := newFakeRedis()
	client := redis.NewFromUniversalClient(fake, redis.DefaultConfig())

	ctx := context.Background()
	convID := "conv-123"

	// Initial INCR should yield 1
	seq1, err := client.IncrSequence(ctx, convID)
	if err != nil {
		t.Fatalf("unexpected error on IncrSequence: %v", err)
	}
	if seq1 != 1 {
		t.Errorf("expected seq 1, got: %d", seq1)
	}

	// Next INCR should yield 2 (strictly contiguous dense integer)
	seq2, err := client.IncrSequence(ctx, convID)
	if err != nil {
		t.Fatalf("unexpected error on second IncrSequence: %v", err)
	}
	if seq2 != 2 {
		t.Errorf("expected seq 2, got: %d", seq2)
	}

	// Test failover / recovery: SETNX when key already exists should return false
	set, err := client.SetSequenceIfNotExists(ctx, convID, 100)
	if err != nil {
		t.Fatalf("unexpected error on SetSequenceIfNotExists: %v", err)
	}
	if set {
		t.Errorf("expected set=false for already existing conversation sequence")
	}

	// For a fresh conversation, SETNX should succeed
	freshConvID := "conv-456"
	setFresh, err := client.SetSequenceIfNotExists(ctx, freshConvID, 42)
	if err != nil {
		t.Fatalf("unexpected error on fresh SetSequenceIfNotExists: %v", err)
	}
	if !setFresh {
		t.Errorf("expected set=true for fresh conversation sequence")
	}
}

func TestClient_PresenceTracking(t *testing.T) {
	fake := newFakeRedis()
	client := redis.NewFromUniversalClient(fake, redis.DefaultConfig())

	ctx := context.Background()
	userID := "user-789"

	// Initially not found
	status, err := client.GetPresence(ctx, userID)
	if !errors.Is(err, redis.ErrPresenceNotFound) {
		t.Errorf("expected ErrPresenceNotFound, got: %v (val=%s)", err, status)
	}

	// Set presence to online
	if err := client.SetPresence(ctx, userID, "online", 30*time.Second); err != nil {
		t.Fatalf("unexpected error setting presence: %v", err)
	}

	// Fetch presence
	status, err = client.GetPresence(ctx, userID)
	if err != nil {
		t.Fatalf("unexpected error getting presence: %v", err)
	}
	if status != "online" {
		t.Errorf("expected status 'online', got: '%s'", status)
	}
}

func TestClient_LifecycleAndPing(t *testing.T) {
	fake := newFakeRedis()
	client := redis.NewFromUniversalClient(fake, redis.DefaultConfig())

	ctx := context.Background()
	if err := client.Ping(ctx); err != nil {
		t.Fatalf("expected Ping to succeed, got: %v", err)
	}

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := client.Ping(canceledCtx); !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled on canceled ping, got: %v", err)
	}

	if err := client.Close(); err != nil {
		t.Fatalf("unexpected error closing client: %v", err)
	}
	if !fake.closed {
		t.Errorf("expected underlying redis client to be closed")
	}

	if err := client.Ping(context.Background()); !errors.Is(err, redis.ErrClientClosed) {
		t.Errorf("expected ErrClientClosed on closed client ping, got: %v", err)
	}
}
