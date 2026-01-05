package mq

import (
	"context"

	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/domain"
)

// MessageQueue defines the interface for a message queue implementation.
// This interface allows for different queue implementations (in-memory, Redis, etc.)
// to be plugged in via dependency injection.
type MessageQueue interface {
	// Publish publishes a message to the queue.
	// Returns an error if the queue is closed or if publishing fails.
	Publish(ctx context.Context, msg *domain.Message) error

	// Subscribe returns a channel that receives messages from the queue.
	// Messages are distributed using a work queue pattern (round-robin),
	// ensuring each message is delivered to only one subscriber.
	// The channel will be closed when the queue is shut down.
	// Returns an error if the queue is closed or if subscription fails.
	Subscribe(ctx context.Context, consumerID string) (<-chan *domain.Message, error)

	// Ack acknowledges that a message has been successfully processed.
	// This prevents message loss and allows the queue to track delivery status.
	// Returns an error if the message ID is invalid or if ACK fails.
	Ack(ctx context.Context, messageID string, consumerID string) error

	// Close gracefully shuts down the queue.
	// It stops accepting new messages and closes all subscriber channels.
	// Returns an error if shutdown fails.
	Close() error

	// IsClosed returns true if the queue has been closed
	IsClosed() bool
}
