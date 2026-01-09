package mq

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/domain"
)

// ErrQueueFull signals the queue cannot accept more messages without backpressure handling.
var ErrQueueFull = fmt.Errorf("message queue is full")

// PendingMessage tracks delivery metadata for redelivery.
type PendingMessage struct {
	Message     *domain.Message
	ConsumerID  string
	DeliveredAt time.Time
}

// SubscriberInfo tracks information about a subscriber
type SubscriberInfo struct {
	Channel    chan *domain.Message
	ConsumerID string
	Context    context.Context
	closed     atomic.Bool
	closeOnce  sync.Once
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
	// Key: message ID, Value: pending message metadata
	pendingMessages map[string]*PendingMessage

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

	// undeliveredQueue buffers messages when there are no subscribers
	undeliveredQueue []*domain.Message
	maxUndelivered   int

	// redelivery configuration
	ackTimeout  time.Duration
	pendingTTL  time.Duration
	redeliverWg sync.WaitGroup
	redeliverCh chan struct{}
	maxPending  int
}

// NewInMemoryMessageQueue creates a new in-memory message queue.
// bufferSize determines the capacity of the internal message channel.
// A larger buffer size allows more messages to be buffered before blocking publishers.
func NewInMemoryMessageQueue(bufferSize int) *InMemoryMessageQueue {
	if bufferSize <= 0 {
		bufferSize = 100 // default buffer size
	}

	queue := &InMemoryMessageQueue{
		messages:         make(chan *domain.Message, bufferSize),
		subscribers:      make([]*SubscriberInfo, 0),
		pendingMessages:  make(map[string]*PendingMessage),
		bufferSize:       bufferSize,
		undeliveredQueue: make([]*domain.Message, 0),
		maxUndelivered:   bufferSize, // default to bufferSize worth of backlog
		ackTimeout:       30 * time.Second,
		pendingTTL:       5 * time.Minute,
		redeliverCh:      make(chan struct{}),
		maxPending:       bufferSize * 10,
	}

	// Start the message distributor goroutine
	queue.wg.Add(1)
	go queue.distribute()

	// Start redelivery loop
	queue.redeliverWg.Add(1)
	go queue.redeliveryLoop()

	return queue
}

// Publish publishes a message to the queue.
// It is safe to call from multiple goroutines concurrently.
func (q *InMemoryMessageQueue) Publish(ctx context.Context, msg *domain.Message) error {
	if msg == nil {
		return fmt.Errorf("cannot publish nil message")
	}
	if msg.ID == "" {
		return fmt.Errorf("message ID is required")
	}
	if msg.Payload == nil {
		return fmt.Errorf("message payload cannot be nil")
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
	default:
		// Non-blocking publish to provide backpressure signal
		return ErrQueueFull
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
		// Close channel safely
		subInfo.closed.Store(true)
		subInfo.closeOnce.Do(func() {
			close(subChan)
		})
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
		// If no subscribers, buffer message instead of dropping
		q.mu.Lock()
		if len(q.subscribers) == 0 {
			if len(q.undeliveredQueue) < q.maxUndelivered {
				q.undeliveredQueue = append(q.undeliveredQueue, msg)
			}
			// If backlog is full, message will be dropped
			// This is expected behavior to prevent unbounded memory growth
			q.mu.Unlock()
			continue
		}
		// Before sending the new message, flush any buffered backlog
		// We need to unlock before calling deliverMessage since it acquires its own locks
		if len(q.undeliveredQueue) > 0 {
			backlog := q.undeliveredQueue
			q.undeliveredQueue = nil
			q.mu.Unlock()
			// Deliver buffered messages - they will be re-buffered if no subscribers
			for _, buffered := range backlog {
				q.deliverMessage(buffered)
			}
		} else {
			q.mu.Unlock()
		}

		// Try to deliver message using round-robin
		q.deliverMessage(msg)
	}

	// Close all subscriber channels when message channel is closed
	q.mu.Lock()
	for _, subInfo := range q.subscribers {
		subInfo.closeOnce.Do(func() {
			close(subInfo.Channel)
		})
	}
	q.subscribers = make([]*SubscriberInfo, 0)
	q.mu.Unlock()
}

// deliverMessage attempts to deliver a single message with round-robin selection.
// Returns true if delivered, false if message was buffered or dropped.
func (q *InMemoryMessageQueue) deliverMessage(msg *domain.Message) bool {
	const maxRetries = 100 // Prevent infinite loops - max retries before buffering

	attempts := 0
	totalAttempts := 0
	for {
		q.mu.RLock()
		numSubs := len(q.subscribers)
		if numSubs == 0 {
			q.mu.RUnlock()
			q.mu.Lock()
			if len(q.undeliveredQueue) < q.maxUndelivered {
				q.undeliveredQueue = append(q.undeliveredQueue, msg)
			}
			q.mu.Unlock()
			return false
		}

		if q.currentSubscriberIndex >= numSubs {
			q.currentSubscriberIndex = 0
		}
		subInfo := q.subscribers[q.currentSubscriberIndex]
		q.currentSubscriberIndex = (q.currentSubscriberIndex + 1) % numSubs
		q.mu.RUnlock()

		// Attempt send with protection against races with close
		if q.safeSend(subInfo, msg) {
			q.mu.Lock()
			if len(q.pendingMessages) < q.maxPending {
				q.pendingMessages[msg.ID] = &PendingMessage{
					Message:     msg,
					ConsumerID:  subInfo.ConsumerID,
					DeliveredAt: time.Now(),
				}
			}
			q.mu.Unlock()
			return true
		}

		attempts++
		totalAttempts++

		// If we've tried all subscribers once, wait briefly
		if attempts >= numSubs {
			time.Sleep(10 * time.Millisecond)
			attempts = 0
		}

		// Prevent infinite loop - buffer message if we can't deliver after max retries
		if totalAttempts >= maxRetries {
			q.mu.Lock()
			if len(q.undeliveredQueue) < q.maxUndelivered {
				q.undeliveredQueue = append(q.undeliveredQueue, msg)
			}
			q.mu.Unlock()
			return false
		}
	}
}

// safeSend guards against channel close races when delivering.
func (q *InMemoryMessageQueue) safeSend(subInfo *SubscriberInfo, msg *domain.Message) bool {
	defer func() {
		if r := recover(); r != nil {
			// channel closed between check and send
		}
	}()

	if subInfo.closed.Load() {
		return false
	}

	select {
	case subInfo.Channel <- msg:
		return true
	case <-subInfo.Context.Done():
		return false
	default:
		return false
	}
}

// redeliveryLoop periodically checks for ACK timeouts and requeues messages.
func (q *InMemoryMessageQueue) redeliveryLoop() {
	defer q.redeliverWg.Done()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// First collect candidates while holding the lock, then deliver outside
			q.mu.Lock()
			now := time.Now()
			var redeliver []*domain.Message
			for id, pending := range q.pendingMessages {
				elapsed := now.Sub(pending.DeliveredAt)
				switch {
				case elapsed > q.pendingTTL:
					// Message exceeded TTL, delete without redelivery
					delete(q.pendingMessages, id)
				case elapsed > q.ackTimeout:
					// Message exceeded ACK timeout but not TTL, queue for redelivery
					delete(q.pendingMessages, id)
					redeliver = append(redeliver, pending.Message)
				default:
					// Still within ackTimeout; keep pending
				}
			}
			hasSubs := len(q.subscribers) > 0
			q.mu.Unlock()

			// Deliver timed-out messages even if no new publishes arrive.
			if len(redeliver) > 0 {
				if hasSubs {
					for _, msg := range redeliver {
						q.deliverMessage(msg)
					}
				} else {
					q.mu.Lock()
					for _, msg := range redeliver {
						if len(q.undeliveredQueue) < q.maxUndelivered {
							q.undeliveredQueue = append(q.undeliveredQueue, msg)
						}
					}
					q.mu.Unlock()
				}
			}
		case <-q.redeliverCh:
			return
		}
	}
}

// Ack acknowledges that a message has been successfully processed.
// If the message is not found in pending messages, it's considered already ACKed
// (this can happen in test scenarios or if ACK was sent multiple times).
func (q *InMemoryMessageQueue) Ack(ctx context.Context, messageID string, consumerID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Verify the message is pending and was delivered to this consumer
	pending, exists := q.pendingMessages[messageID]
	if !exists {
		// Message not in pending - might have been already ACKed or never delivered through queue
		// This is OK in test scenarios where processBatch is called directly
		return nil // Return nil instead of error for idempotency
	}

	if pending.ConsumerID != consumerID {
		return fmt.Errorf("message %s was delivered to consumer %s, not %s", messageID, pending.ConsumerID, consumerID)
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
	close(q.redeliverCh)
	q.mu.Unlock()

	// Wait for distributor goroutine to finish
	q.wg.Wait()
	q.redeliverWg.Wait()

	q.mu.Lock()
	// Close all subscriber channels
	for _, subInfo := range q.subscribers {
		subInfo.closeOnce.Do(func() {
			close(subInfo.Channel)
		})
	}
	q.subscribers = make([]*SubscriberInfo, 0)
	q.pendingMessages = make(map[string]*PendingMessage)
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
