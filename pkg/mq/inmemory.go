package mq

import (
	"context"
	"fmt"
	"sync"

	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/domain"
)

// InMemoryMessageQueue is an in-memory implementation of MessageQueue using Go channels.
// It supports multiple producers (Streamers) and multiple consumers (Collectors).
// Thread-safe for concurrent use.
type InMemoryMessageQueue struct {
	// messages is the main channel that buffers published messages
	messages chan *domain.Message

	// subscribers tracks all active subscriber channels
	subscribers map[chan *domain.Message]struct{}

	// mu protects subscribers map and closed flag
	mu sync.RWMutex

	// closed indicates if the queue has been shut down
	closed bool

	// wg tracks active goroutines
	wg sync.WaitGroup

	// bufferSize is the size of the message buffer channel
	bufferSize int
}

// NewInMemoryMessageQueue creates a new in-memory message queue.
// bufferSize determines the capacity of the internal message channel.
// A larger buffer size allows more messages to be buffered before blocking publishers.
func NewInMemoryMessageQueue(bufferSize int) *InMemoryMessageQueue {
	if bufferSize <= 0 {
		bufferSize = 100 // default buffer size
	}

	queue := &InMemoryMessageQueue{
		messages:    make(chan *domain.Message, bufferSize),
		subscribers: make(map[chan *domain.Message]struct{}),
		bufferSize:  bufferSize,
	}

	// Start the message distributor goroutine
	queue.wg.Add(1)
	go queue.distribute()

	return queue
}

// Publish publishes a message to the queue.
// It is safe to call from multiple goroutines concurrently.
func (q *InMemoryMessageQueue) Publish(ctx context.Context, msg *domain.Message) error {
	if msg == nil {
		return fmt.Errorf("cannot publish nil message")
	}

	q.mu.RLock()
	if q.closed {
		q.mu.RUnlock()
		return fmt.Errorf("queue is closed")
	}
	q.mu.RUnlock()

	select {
	case q.messages <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Subscribe creates a new subscription channel that receives messages from the queue.
// Each subscriber gets its own channel, enabling fan-out message distribution.
// The channel will be closed when the queue is shut down.
func (q *InMemoryMessageQueue) Subscribe(ctx context.Context) (<-chan *domain.Message, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return nil, fmt.Errorf("queue is closed")
	}

	// Create a new channel for this subscriber
	subChan := make(chan *domain.Message, q.bufferSize)
	q.subscribers[subChan] = struct{}{}

	// Start a goroutine to handle cleanup when context is cancelled
	go func() {
		<-ctx.Done()
		q.mu.Lock()
		delete(q.subscribers, subChan)
		close(subChan)
		q.mu.Unlock()
	}()

	return subChan, nil
}

// distribute is the internal goroutine that distributes messages to all subscribers.
// It implements a fan-out pattern where each message is sent to all active subscribers.
func (q *InMemoryMessageQueue) distribute() {
	defer q.wg.Done()

	for msg := range q.messages {
		q.mu.RLock()
		// Make a copy of subscribers to avoid holding lock while sending
		subs := make([]chan *domain.Message, 0, len(q.subscribers))
		for sub := range q.subscribers {
			subs = append(subs, sub)
		}
		q.mu.RUnlock()

		// Fan-out: send message to all subscribers
		// Use non-blocking sends to avoid blocking the distributor
		for _, sub := range subs {
			select {
			case sub <- msg:
				// Message sent successfully
			default:
				// Subscriber channel is full, skip to avoid blocking
				// In a production system, you might want to log this
			}
		}
	}

	// Close all subscriber channels when message channel is closed
	q.mu.Lock()
	for sub := range q.subscribers {
		close(sub)
	}
	q.subscribers = make(map[chan *domain.Message]struct{})
	q.mu.Unlock()
}

// Close gracefully shuts down the queue.
// It stops accepting new messages and closes all subscriber channels.
func (q *InMemoryMessageQueue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return nil // already closed
	}

	q.closed = true
	close(q.messages)

	// Wait for distributor goroutine to finish
	q.mu.Unlock()
	q.wg.Wait()
	q.mu.Lock()

	return nil
}

// IsClosed returns true if the queue has been closed
func (q *InMemoryMessageQueue) IsClosed() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.closed
}

// GetSubscriberCount returns the number of active subscribers (for testing/monitoring)
func (q *InMemoryMessageQueue) GetSubscriberCount() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.subscribers)
}
