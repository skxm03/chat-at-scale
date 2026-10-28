package kafka_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	segmentio "github.com/segmentio/kafka-go"
	"github.com/skxm03/chat-at-scale/internal/platform/kafka"
)

type fakeMessageWriter struct {
	closed       bool
	writtenMsgs  []segmentio.Message
	writeErr     error
	closeErr     error
}

func (f *fakeMessageWriter) WriteMessages(ctx context.Context, msgs ...segmentio.Message) error {
	if f.closed {
		return errors.New("writer closed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if f.writeErr != nil {
		return f.writeErr
	}
	f.writtenMsgs = append(f.writtenMsgs, msgs...)
	return nil
}

func (f *fakeMessageWriter) Close() error {
	f.closed = true
	return f.closeErr
}

func TestWriterConfig_GuaranteesAndDefaults(t *testing.T) {
	cfg := kafka.DefaultConfig()

	writer, err := kafka.NewWriter(cfg)
	if err != nil {
		t.Fatalf("expected no error creating writer, got: %v", err)
	}
	defer writer.Close()

	// Guarantee 1: RequiredAcks must be RequireAll (-1) for zero lost messages
	if writer.RequiredAcks != segmentio.RequireAll {
		t.Errorf("expected RequiredAcks to be RequireAll (-1), got: %v", writer.RequiredAcks)
	}

	// Guarantee 2: Async must be false for synchronous durability guarantee
	if writer.Async {
		t.Errorf("expected Async to be false for zero lost messages guarantee, got: true")
	}

	// Guarantee 3: Balancer must be Hash for partition routing by key (conversation_id)
	hashBalancer, ok := writer.Balancer.(*segmentio.Hash)
	if !ok || hashBalancer == nil {
		t.Errorf("expected Balancer to be *segmentio.Hash, got: %T", writer.Balancer)
	}

	if writer.Topic != "chat-messages" {
		t.Errorf("expected default topic chat-messages, got: %s", writer.Topic)
	}
	if writer.WriteTimeout != 5*time.Second {
		t.Errorf("expected WriteTimeout 5s, got: %v", writer.WriteTimeout)
	}
	if writer.ReadTimeout != 5*time.Second {
		t.Errorf("expected ReadTimeout 5s, got: %v", writer.ReadTimeout)
	}
	if writer.MaxAttempts != 5 {
		t.Errorf("expected MaxAttempts 5, got: %d", writer.MaxAttempts)
	}
}

func TestConfig_Validation(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*kafka.Config)
		wantErr bool
	}{
		{
			name: "empty brokers",
			mutate: func(c *kafka.Config) {
				c.Brokers = nil
			},
			wantErr: true,
		},
		{
			name: "empty topic",
			mutate: func(c *kafka.Config) {
				c.Topic = ""
			},
			wantErr: true,
		},
		{
			name: "zero write timeout",
			mutate: func(c *kafka.Config) {
				c.WriteTimeout = 0
			},
			wantErr: true,
		},
		{
			name: "negative max attempts",
			mutate: func(c *kafka.Config) {
				c.MaxAttempts = -1
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := kafka.DefaultConfig()
			tt.mutate(&cfg)
			if err := cfg.Validate(); (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestProducer_Publish_PartitionHashingByConversationID(t *testing.T) {
	fakeWriter := &fakeMessageWriter{}
	cfg := kafka.DefaultConfig()
	producer := kafka.NewProducerWithWriter(fakeWriter, cfg)

	ctx := context.Background()
	msg := kafka.Message{
		ConversationID: "conv-uuid-abc-123",
		Payload:        []byte(`{"seq": 1, "text": "hello university"}`),
		Headers: map[string]string{
			"sender_id": "user-456",
			"type":      "text",
		},
	}

	if err := producer.Publish(ctx, msg); err != nil {
		t.Fatalf("unexpected error publishing message: %v", err)
	}

	if len(fakeWriter.writtenMsgs) != 1 {
		t.Fatalf("expected 1 message written, got: %d", len(fakeWriter.writtenMsgs))
	}

	written := fakeWriter.writtenMsgs[0]

	// Key MUST match ConversationID so Kafka Balancer hashes by conversation_id
	if string(written.Key) != msg.ConversationID {
		t.Errorf("expected message key to equal ConversationID '%s', got: '%s'", msg.ConversationID, string(written.Key))
	}

	if string(written.Value) != string(msg.Payload) {
		t.Errorf("expected payload '%s', got: '%s'", string(msg.Payload), string(written.Value))
	}

	headerMap := make(map[string]string)
	for _, h := range written.Headers {
		headerMap[h.Key] = string(h.Value)
	}
	if headerMap["sender_id"] != "user-456" {
		t.Errorf("expected header sender_id=user-456, got: %s", headerMap["sender_id"])
	}
	if headerMap["type"] != "text" {
		t.Errorf("expected header type=text, got: %s", headerMap["type"])
	}
}

func TestProducer_EmptyConversationID_Rejected(t *testing.T) {
	fakeWriter := &fakeMessageWriter{}
	producer := kafka.NewProducerWithWriter(fakeWriter, kafka.DefaultConfig())

	msg := kafka.Message{
		ConversationID: "", // Missing partition key!
		Payload:        []byte("missing conversation ID"),
	}

	err := producer.Publish(context.Background(), msg)
	if !errors.Is(err, kafka.ErrEmptyConversationID) {
		t.Errorf("expected ErrEmptyConversationID, got: %v", err)
	}
}

type fakeConn struct {
	net.Conn
	closed bool
}

func (f *fakeConn) Close() error {
	f.closed = true
	return nil
}

func TestProducer_LifecycleAndPing(t *testing.T) {
	fakeWriter := &fakeMessageWriter{}
	conn := &fakeConn{}
	mockDialer := func(ctx context.Context, network, address string) (net.Conn, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return conn, nil
	}

	producer := kafka.NewProducerWithWriter(fakeWriter, kafka.DefaultConfig()).WithDialer(mockDialer)

	ctx := context.Background()
	if err := producer.Ping(ctx); err != nil {
		t.Fatalf("expected Ping to succeed on active writer and reachable broker, got: %v", err)
	}

	// Test broker unreachable
	unreachableDialer := func(ctx context.Context, network, address string) (net.Conn, error) {
		return nil, errors.New("connection refused")
	}
	producerWithUnreachableBroker := kafka.NewProducerWithWriter(fakeWriter, kafka.DefaultConfig()).WithDialer(unreachableDialer)
	if err := producerWithUnreachableBroker.Ping(ctx); err == nil {
		t.Errorf("expected Ping to fail when broker unreachable")
	}

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := producer.Ping(canceledCtx); !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled on canceled ping, got: %v", err)
	}

	if err := producer.Close(); err != nil {
		t.Fatalf("unexpected error closing producer: %v", err)
	}
	if !fakeWriter.closed {
		t.Errorf("expected fakeWriter to be closed")
	}

	if err := producer.Ping(context.Background()); !errors.Is(err, kafka.ErrProducerClosed) {
		t.Errorf("expected ErrProducerClosed on ping after close, got: %v", err)
	}

	msg := kafka.Message{
		ConversationID: "conv-1",
		Payload:        []byte("test"),
	}
	if err := producer.Publish(context.Background(), msg); !errors.Is(err, kafka.ErrProducerClosed) {
		t.Errorf("expected ErrProducerClosed on publish after close, got: %v", err)
	}
}

