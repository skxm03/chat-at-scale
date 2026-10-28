package cassandra

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/gocql/gocql"
)

var (
	ErrSessionClosed = errors.New("cassandra session is closed")
)

// Session represents the minimal interface required for executing Cassandra queries.
// *gocql.Session implements this interface.
type Session interface {
	Query(stmt string, values ...any) *gocql.Query
	Close()
	Closed() bool
}

// Client manages the lifecycle, pooling, and health checks of a Cassandra session.
type Client struct {
	session Session
	cfg     Config
	closed  atomic.Bool
}

// New creates a new Cassandra client session manager using the provided configuration.
func New(cfg Config) (*Client, error) {
	cluster, err := NewClusterConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("build cluster config: %w", err)
	}

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("create cassandra session: %w", err)
	}

	return NewWithSession(session, cfg), nil
}

// NewWithSession creates a Client with a pre-configured Session, primarily used for testing and seam injection.
func NewWithSession(session Session, cfg Config) *Client {
	return &Client{
		session: session,
		cfg:     cfg,
	}
}

// Session returns the underlying Cassandra session.
func (c *Client) Session() Session {
	return c.session
}

// Ping checks the health of the Cassandra session respecting context timeout/cancellation.
func (c *Client) Ping(ctx context.Context) error {
	if c.closed.Load() || c.session == nil || c.session.Closed() {
		return ErrSessionClosed
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	// For real sessions, issue a lightweight query against system.local.
	// System tables require LOCAL_ONE or ONE consistency; QUORUM is rejected by Cassandra.
	q := c.session.Query("SELECT release_version FROM system.local")
	if q != nil {
		if err := q.Consistency(gocql.One).WithContext(ctx).Exec(); err != nil {
			return fmt.Errorf("ping cassandra: %w", err)
		}
	}

	return nil
}

// Close gracefully closes the Cassandra session.
func (c *Client) Close() {
	if c.closed.CompareAndSwap(false, true) && c.session != nil {
		c.session.Close()
	}
}
