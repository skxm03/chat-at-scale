package kafka

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	segmentio "github.com/segmentio/kafka-go"
)

var (
	ErrProducerClosed      = errors.New("kafka producer is closed")
	ErrEmptyConversationID = errors.New("conversation_id is required for partition routing")
)

// Dialer abstracts network dialing for testing and broker probing.
type Dialer func(ctx context.Context, network, address string) (net.Conn, error)

func defaultDialer(ctx context.Context, network, address string) (net.Conn, error) {
	return segmentio.DialContext(ctx, network, address)
}

// Message is the high-level domain message published to Kafka.
type Message struct {
	ConversationID string
	Payload        []byte
	Headers        map[string]string
}

// MessageWriter abstracts segmentio/kafka-go Writer for dependency injection and testing.
type MessageWriter interface {
	WriteMessages(ctx context.Context, msgs ...segmentio.Message) error
	Close() error
}

// Producer wraps a Kafka MessageWriter ensuring strict partition hashing by conversation_id
// and guaranteed acknowledgments.
type Producer struct {
	writer MessageWriter
	cfg    Config
	dialer Dialer
	closed atomic.Bool
}

// NewProducer builds a Kafka Producer using the specified configuration.
func NewProducer(cfg Config) (*Producer, error) {
	w, err := NewWriter(cfg)
	if err != nil {
		return nil, fmt.Errorf("create kafka writer: %w", err)
	}
	return NewProducerWithWriter(w, cfg), nil
}

// NewProducerWithWriter creates a Producer with an injected MessageWriter.
func NewProducerWithWriter(writer MessageWriter, cfg Config) *Producer {
	return &Producer{
		writer: writer,
		cfg:    cfg,
		dialer: defaultDialer,
	}
}

// WithDialer sets a custom dialer for health check network probes.
func (p *Producer) WithDialer(dialer Dialer) *Producer {
	p.dialer = dialer
	return p
}

// Publish writes a message to Kafka, hashing by ConversationID to preserve per-conversation ordering.
func (p *Producer) Publish(ctx context.Context, msg Message) error {
	if p.closed.Load() {
		return ErrProducerClosed
	}
	if msg.ConversationID == "" {
		return ErrEmptyConversationID
	}

	headers := make([]segmentio.Header, 0, len(msg.Headers))
	for k, v := range msg.Headers {
		headers = append(headers, segmentio.Header{
			Key:   k,
			Value: []byte(v),
		})
	}

	kafkaMsg := segmentio.Message{
		Key:     []byte(msg.ConversationID),
		Value:   msg.Payload,
		Headers: headers,
		Time:    time.Now(),
	}

	if err := p.writer.WriteMessages(ctx, kafkaMsg); err != nil {
		return fmt.Errorf("publish message to conversation %s: %w", msg.ConversationID, err)
	}

	return nil
}

// Ping verifies producer health and actively probes broker connectivity respecting context cancellation.
func (p *Producer) Ping(ctx context.Context) error {
	if p.closed.Load() {
		return ErrProducerClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(p.cfg.Brokers) == 0 {
		return ErrEmptyBrokers
	}

	dial := p.dialer
	if dial == nil {
		dial = defaultDialer
	}

	var lastErr error
	for _, broker := range p.cfg.Brokers {
		conn, err := dial(ctx, "tcp", broker)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		lastErr = err
	}

	return fmt.Errorf("ping kafka brokers: %w", lastErr)
}

// Close flushes and shuts down the Kafka writer.
func (p *Producer) Close() error {
	if p.closed.CompareAndSwap(false, true) && p.writer != nil {
		return p.writer.Close()
	}
	return nil
}

