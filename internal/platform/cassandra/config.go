package cassandra

import (
	"errors"
	"fmt"
	"time"

	"github.com/gocql/gocql"
)

var (
	ErrEmptyHosts        = errors.New("cassandra hosts cannot be empty")
	ErrInvalidPort       = errors.New("cassandra port must be greater than 0")
	ErrEmptyKeyspace     = errors.New("cassandra keyspace cannot be empty")
	ErrInvalidTimeout    = errors.New("cassandra query timeout must be greater than 0")
	ErrInvalidConnCount  = errors.New("cassandra NumConns must be greater than 0")
)

// Config encapsulates Cassandra connection and policy parameters.
type Config struct {
	Hosts                    []string
	Port                     int
	Keyspace                 string
	Consistency              gocql.Consistency
	ConnectTimeout           time.Duration
	Timeout                  time.Duration
	NumConns                 int
	ReconnectInitialInterval time.Duration
	ReconnectMaxInterval     time.Duration
	ReconnectMaxRetries      int
	MaxRetries               int
	RetryMinBackoff          time.Duration
	RetryMaxBackoff          time.Duration
}

// DefaultConfig provides production-ready defaults tuned for resilience and low latency.
func DefaultConfig() Config {
	return Config{
		Hosts:                    []string{"localhost"},
		Port:                     9042,
		Keyspace:                 "chat",
		Consistency:              gocql.Quorum,
		ConnectTimeout:           5 * time.Second,
		Timeout:                  2 * time.Second,
		NumConns:                 4,
		ReconnectInitialInterval: 500 * time.Millisecond,
		ReconnectMaxInterval:     10 * time.Second,
		ReconnectMaxRetries:      10,
		MaxRetries:               3,
		RetryMinBackoff:          100 * time.Millisecond,
		RetryMaxBackoff:          1 * time.Second,
	}
}

// Validate checks whether the config parameters are structurally valid.
func (c Config) Validate() error {
	if len(c.Hosts) == 0 {
		return ErrEmptyHosts
	}
	if c.Port <= 0 {
		return ErrInvalidPort
	}
	if c.Keyspace == "" {
		return ErrEmptyKeyspace
	}
	if c.Timeout <= 0 {
		return ErrInvalidTimeout
	}
	if c.NumConns <= 0 {
		return ErrInvalidConnCount
	}
	return nil
}

// NewClusterConfig builds and configures a gocql.ClusterConfig with exponential backoff
// reconnection, exponential query retry policy, timeouts, and pool limits.
func NewClusterConfig(cfg Config) (*gocql.ClusterConfig, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate cassandra config: %w", err)
	}

	cluster := gocql.NewCluster(cfg.Hosts...)
	cluster.Port = cfg.Port
	cluster.Keyspace = cfg.Keyspace
	cluster.Consistency = cfg.Consistency
	cluster.Timeout = cfg.Timeout
	cluster.ConnectTimeout = cfg.ConnectTimeout
	cluster.NumConns = cfg.NumConns

	cluster.ReconnectionPolicy = &gocql.ExponentialReconnectionPolicy{
		InitialInterval: cfg.ReconnectInitialInterval,
		MaxInterval:     cfg.ReconnectMaxInterval,
		MaxRetries:      cfg.ReconnectMaxRetries,
	}

	cluster.RetryPolicy = &gocql.ExponentialBackoffRetryPolicy{
		NumRetries: cfg.MaxRetries,
		Min:        cfg.RetryMinBackoff,
		Max:        cfg.RetryMaxBackoff,
	}

	return cluster, nil
}

