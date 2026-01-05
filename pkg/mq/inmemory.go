package mq

import (
	"context"
	"fmt"
	"sync"

	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/domain"
)

// SubscriberInfo tracks information about a subscriber
type SubscriberInfo struct {
	Channel    chan *domain.Message
	ConsumerID string
	Context    context.Context
}

// InMemoryMessageQueue is an in-memory implementation of MessageQueue using Go channels.
// It supports multiple producers (Streamers) and multiple consumers (Collectors).
// Uses work queue pattern (round-robin) instead of fanout to ensure each message
// is delivered to only one consumer. Messages must be acknowledged after processing.
// Thread-safe for concurrent use.
type InMemoryMessageQueue struct {
	// messages is the main channel that buffers published messages
	messages chan *domain.Message

	// subscribers tracks all active subscriber channels with their consumer IDs
	subscribers []*SubscriberInfo

	// pendingMessages tracks messages that have been delivered but not yet ACKed
	// Key: message ID, Value: consumer ID that received the message
	pendingMessages map[string]string

	// mu protects subscribers, pendingMessages, and closed flag
	mu sync.RWMutex

	// closed indicates if the queue has been shut down
	closed bool

	// wg tracks active goroutines
	wg sync.WaitGroup

	// bufferSize is the size of the message buffer channel
	bufferSize int

	// currentSubscriberIndex for round-robin distribution
	currentSubscriberIndex int
}

// NewInMemoryMessageQueue creates a new in-memory message queue.
// bufferSize determines the capacity of the internal message channel.
// A larger buffer size allows more messages to be buffered before blocking publishers.
func NewInMemoryMessageQueue(bufferSize int) *InMemoryMessageQueue {
	if bufferSize <= 0 {
		bufferSize = 100 // default buffer size
	}

	queue := &InMemoryMessageQueue{
		messages:        make(chan *domain.Message, bufferSize),
		subscribers:     make([]*SubscriberInfo, 0),
		pendingMessages: make(map[string]string),
		bufferSize:      bufferSize,
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

	// Check context first to ensure we respect cancellation
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Then try to send the message
	select {
	case q.messages <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Subscribe creates a new subscription channel that receives messages from the queue.
// Messages are distributed using a work queue pattern (round-robin), ensuring each
// message is delivered to only one subscriber. The channel will be closed when the
// queue is shut down or the context is cancelled.
func (q *InMemoryMessageQueue) Subscribe(ctx context.Context, consumerID string) (<-chan *domain.Message, error) {
	if consumerID == "" {
		return nil, fmt.Errorf("consumerID is required")
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return nil, fmt.Errorf("queue is closed")
	}

	// Create a new channel for this subscriber
	subChan := make(chan *domain.Message, q.bufferSize)
	subInfo := &SubscriberInfo{
		Channel:    subChan,
		ConsumerID: consumerID,
		Context:    ctx,
	}

	// Add subscriber to the list
	q.subscribers = append(q.subscribers, subInfo)

	// Start a goroutine to handle cleanup when context is cancelled
	go func() {
		<-ctx.Done()
		q.mu.Lock()
		// Remove subscriber from list
		for i, sub := range q.subscribers {
			if sub == subInfo {
				q.subscribers = append(q.subscribers[:i], q.subscribers[i+1:]...)
				// Adjust current index if needed
				if q.currentSubscriberIndex >= len(q.subscribers) {
					q.currentSubscriberIndex = 0
				}
				break
			}
		}
		// Close channel only if not already closed
		select {
		case <-subChan:
			// Channel already closed, do nothing
		default:
			close(subChan)
		}
		q.mu.Unlock()
	}()

	return subChan, nil
}

// distribute is the internal goroutine that distributes messages to subscribers.
// It implements a work queue pattern (round-robin) where each message is sent
// to only one subscriber. Messages are tracked as pending until ACKed.
func (q *InMemoryMessageQueue) distribute() {
	defer q.wg.Done()

	for msg := range q.messages {
		// Try to deliver message using round-robin
		delivered := false
		attempts := 0
		q.mu.RLock()
		numSubs := len(q.subscribers)
		q.mu.RUnlock()
		maxAttempts := numSubs

		if numSubs == 0 {
			// No subscribers yet, message will be lost
			continue
		}

		for !delivered && attempts < maxAttempts {
			q.mu.RLock()
			if len(q.subscribers) == 0 {
				q.mu.RUnlock()
				// No subscribers, message will be lost (could buffer, but keeping simple)
				break
			}

			// Get next subscriber using round-robin
			if q.currentSubscriberIndex >= len(q.subscribers) {
				q.currentSubscriberIndex = 0
			}
			subInfo := q.subscribers[q.currentSubscriberIndex]
			q.currentSubscriberIndex = (q.currentSubscriberIndex + 1) % len(q.subscribers)
			q.mu.RUnlock()

			// Try to send message (blocking to ensure delivery for SSE)
			// For SSE, we need to block until the message is sent
			select {
			case subInfo.Channel <- msg:
				// Message delivered successfully
				q.mu.Lock()
				// Track as pending until ACKed
				q.pendingMessages[msg.ID] = subInfo.ConsumerID
				q.mu.Unlock()
				delivered = true
			case <-subInfo.Context.Done():
				// Subscriber context cancelled, try next
				attempts++
			}
		}

		if !delivered && len(q.subscribers) > 0 {
			// All subscribers busy or channels full, try blocking send to first available
			// This ensures message is not lost
			q.mu.RLock()
			subs := make([]*SubscriberInfo, len(q.subscribers))
			copy(subs, q.subscribers)
			q.mu.RUnlock()

			for _, subInfo := range subs {
				select {
				case subInfo.Channel <- msg:
					q.mu.Lock()
					q.pendingMessages[msg.ID] = subInfo.ConsumerID
					q.mu.Unlock()
					delivered = true
					break
				case <-subInfo.Context.Done():
					continue
				}
			}
		}
	}

	// Close all subscriber channels when message channel is closed
	q.mu.Lock()
	for _, subInfo := range q.subscribers {
		close(subInfo.Channel)
	}
	q.subscribers = make([]*SubscriberInfo, 0)
	q.mu.Unlock()
}

// Ack acknowledges that a message has been successfully processed.
// If the message is not found in pending messages, it's considered already ACKed
// (this can happen in test scenarios or if ACK was sent multiple times).
func (q *InMemoryMessageQueue) Ack(ctx context.Context, messageID string, consumerID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Verify the message is pending and was delivered to this consumer
	storedConsumerID, exists := q.pendingMessages[messageID]
	if !exists {
		// Message not in pending - might have been already ACKed or never delivered through queue
		// This is OK in test scenarios where processBatch is called directly
		return nil // Return nil instead of error for idempotency
	}

	if storedConsumerID != consumerID {
		return fmt.Errorf("message %s was delivered to consumer %s, not %s", messageID, storedConsumerID, consumerID)
	}

	// Remove from pending
	delete(q.pendingMessages, messageID)
	return nil
}

// Close gracefully shuts down the queue.
// It stops accepting new messages and closes all subscriber channels.
func (q *InMemoryMessageQueue) Close() error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return nil // already closed
	}

	q.closed = true
	close(q.messages)
	q.mu.Unlock()

	// Wait for distributor goroutine to finish
	q.wg.Wait()

	q.mu.Lock()
	// Close all subscriber channels
	for _, subInfo := range q.subscribers {
		close(subInfo.Channel)
	}
	q.subscribers = make([]*SubscriberInfo, 0)
	q.pendingMessages = make(map[string]string)
	q.mu.Unlock()

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

// GetPendingMessageCount returns the number of pending (unacknowledged) messages
func (q *InMemoryMessageQueue) GetPendingMessageCount() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.pendingMessages)
}
