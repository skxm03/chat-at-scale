package cassandra_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/skxm03/chat-at-scale/internal/platform/cassandra"
)

type fakeSession struct {
	closed bool
	queries []string
}

func (f *fakeSession) Query(stmt string, values ...any) *gocql.Query {
	f.queries = append(f.queries, stmt)
	return nil
}

func (f *fakeSession) Close() {
	f.closed = true
}

func (f *fakeSession) Closed() bool {
	return f.closed
}

func TestClusterConfig_DefaultsAndPolicies(t *testing.T) {
	cfg := cassandra.DefaultConfig()

	cluster, err := cassandra.NewClusterConfig(cfg)
	if err != nil {
		t.Fatalf("expected no error creating cluster config, got: %v", err)
	}

	if cluster.Consistency != gocql.Quorum {
		t.Errorf("expected consistency Quorum, got: %v", cluster.Consistency)
	}
	if cluster.Timeout != 2*time.Second {
		t.Errorf("expected query timeout 2s, got: %v", cluster.Timeout)
	}
	if cluster.ConnectTimeout != 5*time.Second {
		t.Errorf("expected connect timeout 5s, got: %v", cluster.ConnectTimeout)
	}
	if cluster.NumConns != 4 {
		t.Errorf("expected NumConns 4, got: %d", cluster.NumConns)
	}

	reconnectPolicy, ok := cluster.ReconnectionPolicy.(*gocql.ExponentialReconnectionPolicy)
	if !ok || reconnectPolicy == nil {
		t.Fatalf("expected ExponentialReconnectionPolicy, got: %T", cluster.ReconnectionPolicy)
	}
	if reconnectPolicy.InitialInterval != 500*time.Millisecond {
		t.Errorf("expected reconnect initial interval 500ms, got: %v", reconnectPolicy.InitialInterval)
	}
	if reconnectPolicy.MaxInterval != 10*time.Second {
		t.Errorf("expected reconnect max interval 10s, got: %v", reconnectPolicy.MaxInterval)
	}

	retryPolicy, ok := cluster.RetryPolicy.(*gocql.ExponentialBackoffRetryPolicy)
	if !ok || retryPolicy == nil {
		t.Fatalf("expected ExponentialBackoffRetryPolicy, got: %T", cluster.RetryPolicy)
	}
	if retryPolicy.NumRetries != 3 {
		t.Errorf("expected NumRetries 3, got: %d", retryPolicy.NumRetries)
	}
}

func TestClusterConfig_Validation(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*cassandra.Config)
		wantErr bool
	}{
		{
			name: "empty hosts",
			mutate: func(c *cassandra.Config) {
				c.Hosts = nil
			},
			wantErr: true,
		},
		{
			name: "invalid port",
			mutate: func(c *cassandra.Config) {
				c.Port = 0
			},
			wantErr: true,
		},
		{
			name: "empty keyspace",
			mutate: func(c *cassandra.Config) {
				c.Keyspace = ""
			},
			wantErr: true,
		},
		{
			name: "zero timeout",
			mutate: func(c *cassandra.Config) {
				c.Timeout = 0
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := cassandra.DefaultConfig()
			tt.mutate(&cfg)
			_, err := cassandra.NewClusterConfig(cfg)
			if (err != nil) != tt.wantErr {
				t.Fatalf("NewClusterConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestClient_LifecycleAndPing(t *testing.T) {
	fake := &fakeSession{}
	cfg := cassandra.DefaultConfig()
	client := cassandra.NewWithSession(fake, cfg)

	ctx := context.Background()
	if err := client.Ping(ctx); err != nil {
		t.Fatalf("expected ping to succeed on active session, got: %v", err)
	}

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := client.Ping(canceledCtx); !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled on canceled context, got: %v", err)
	}

	client.Close()
	if !fake.Closed() {
		t.Errorf("expected session to be closed after client.Close()")
	}

	if err := client.Ping(context.Background()); !errors.Is(err, cassandra.ErrSessionClosed) {
		t.Errorf("expected ErrSessionClosed on closed client ping, got: %v", err)
	}
}

