package kafka

import (
	"errors"
	"fmt"
	"time"

	segmentio "github.com/segmentio/kafka-go"
)

var (
	ErrEmptyBrokers         = errors.New("kafka brokers cannot be empty")
	ErrEmptyTopic           = errors.New("kafka topic cannot be empty")
	ErrInvalidWriteTimeout  = errors.New("kafka write timeout must be greater than 0")
	ErrInvalidMaxAttempts   = errors.New("kafka max attempts must be non-negative")
)

// Config holds Kafka connection and producer settings.
type Config struct {
	Brokers      []string
	Topic        string
	MaxAttempts  int
	WriteTimeout time.Duration
	ReadTimeout  time.Duration
	BatchTimeout time.Duration
	BatchSize    int
}

// DefaultConfig provides settings compliant with the zero-lost-messages guarantee.
// For synchronous writes (Async = false), BatchTimeout is set to 0 and BatchSize to 1
// to avoid introducing an artificial latency delay on single-message produces.
func DefaultConfig() Config {
	return Config{
		Brokers:      []string{"localhost:9092"},
		Topic:        "chat-messages",
		MaxAttempts:  5,
		WriteTimeout: 5 * time.Second,
		ReadTimeout:  5 * time.Second,
		BatchTimeout: 0,
		BatchSize:    1,
	}
}

// Validate verifies config sanity.
func (c Config) Validate() error {
	if len(c.Brokers) == 0 {
		return ErrEmptyBrokers
	}
	if c.Topic == "" {
		return ErrEmptyTopic
	}
	if c.WriteTimeout <= 0 {
		return ErrInvalidWriteTimeout
	}
	if c.MaxAttempts < 0 {
		return ErrInvalidMaxAttempts
	}
	return nil
}

// NewWriter constructs a segmentio/kafka-go Writer configured with:
// - RequiredAcks = RequireAll (-1) for zero lost messages
// - Async = false for synchronous write acknowledgments
// - Balancer = &segmentio.Hash{} for deterministic partition hashing by key
func NewWriter(cfg Config) (*segmentio.Writer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate kafka config: %w", err)
	}

	return &segmentio.Writer{
		Addr:         segmentio.TCP(cfg.Brokers...),
		Topic:        cfg.Topic,
		Balancer:     &segmentio.Hash{},
		MaxAttempts:  cfg.MaxAttempts,
		WriteTimeout: cfg.WriteTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		BatchTimeout: cfg.BatchTimeout,
		BatchSize:    cfg.BatchSize,
		RequiredAcks: segmentio.RequireAll,
		Async:        false,
	}, nil
}

