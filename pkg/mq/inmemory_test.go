package mq

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryMessageQueue_PublishSubscribe(t *testing.T) {
	queue := NewInMemoryMessageQueue(10)
	defer queue.Close()

	ctx := context.Background()

	// Create a subscriber
	subChan, err := queue.Subscribe(ctx)
	require.NoError(t, err)

	// Publish a message
	record := &domain.TelemetryRecord{
		GPUUUID:       "GPU-123",
		MetricName:    "DCGM_FI_DEV_GPU_UTIL",
		Value:         "100",
		IngestionTime: time.Now(),
	}
	msg := domain.NewMessage(record, "producer-1")

	err = queue.Publish(ctx, msg)
	require.NoError(t, err)

	// Receive the message
	select {
	case received := <-subChan:
		assert.Equal(t, msg.ID, received.ID)
		assert.Equal(t, msg.Payload.GPUUUID, received.Payload.GPUUUID)
		assert.Equal(t, msg.Payload.MetricName, received.Payload.MetricName)
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestInMemoryMessageQueue_MultipleProducers(t *testing.T) {
	queue := NewInMemoryMessageQueue(100)
	defer queue.Close()

	ctx := context.Background()
	subChan, err := queue.Subscribe(ctx)
	require.NoError(t, err)

	numProducers := 5
	messagesPerProducer := 10
	totalMessages := numProducers * messagesPerProducer

	var wg sync.WaitGroup
	wg.Add(numProducers)

	// Start multiple producers
	for i := 0; i < numProducers; i++ {
		go func(producerID int) {
			defer wg.Done()
			for j := 0; j < messagesPerProducer; j++ {
				record := &domain.TelemetryRecord{
					GPUUUID:       "GPU-123",
					MetricName:    "DCGM_FI_DEV_GPU_UTIL",
					Value:         "100",
					IngestionTime: time.Now(),
				}
				msg := domain.NewMessage(record, fmt.Sprintf("producer-%d", producerID))
				err := queue.Publish(ctx, msg)
				require.NoError(t, err)
			}
		}(i)
	}

	wg.Wait()

	// Collect all messages
	received := make([]*domain.Message, 0, totalMessages)
	done := make(chan struct{})

	go func() {
		for msg := range subChan {
			received = append(received, msg)
			if len(received) == totalMessages {
				close(done)
				return
			}
		}
	}()

	select {
	case <-done:
		assert.Equal(t, totalMessages, len(received))
	case <-time.After(1 * time.Second):
		t.Fatalf("timeout: expected %d messages, got %d", totalMessages, len(received))
	}
}

func TestInMemoryMessageQueue_MultipleConsumers(t *testing.T) {
	queue := NewInMemoryMessageQueue(100)
	defer queue.Close()

	ctx := context.Background()
	numConsumers := 5
	messagesToPublish := 50

	// Create multiple subscribers
	subChans := make([]<-chan *domain.Message, numConsumers)
	for i := 0; i < numConsumers; i++ {
		subChan, err := queue.Subscribe(ctx)
		require.NoError(t, err)
		subChans[i] = subChan
	}

	// Publish messages
	for i := 0; i < messagesToPublish; i++ {
		record := &domain.TelemetryRecord{
			GPUUUID:       "GPU-123",
			MetricName:    "DCGM_FI_DEV_GPU_UTIL",
			Value:         "100",
			IngestionTime: time.Now(),
		}
		msg := domain.NewMessage(record, "producer-1")
		err := queue.Publish(ctx, msg)
		require.NoError(t, err)
	}

	// Each consumer should receive all messages (fan-out pattern)
	var wg sync.WaitGroup
	wg.Add(numConsumers)

	for i, subChan := range subChans {
		go func(consumerID int, ch <-chan *domain.Message) {
			defer wg.Done()
			received := 0
			for msg := range ch {
				require.NotNil(t, msg)
				received++
				if received == messagesToPublish {
					return
				}
			}
			assert.Equal(t, messagesToPublish, received, "consumer %d should receive all messages", consumerID)
		}(i, subChan)
	}

	// Wait for all consumers to finish or timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All consumers received messages
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for consumers to receive messages")
	}
}

func TestInMemoryMessageQueue_ConcurrentPublishSubscribe(t *testing.T) {
	queue := NewInMemoryMessageQueue(200)
	defer queue.Close()

	ctx := context.Background()
	subChan, err := queue.Subscribe(ctx)
	require.NoError(t, err)

	numProducers := 10
	messagesPerProducer := 20
	totalMessages := numProducers * messagesPerProducer

	var wg sync.WaitGroup
	wg.Add(numProducers)

	// Start concurrent producers
	for i := 0; i < numProducers; i++ {
		go func(producerID int) {
			defer wg.Done()
			for j := 0; j < messagesPerProducer; j++ {
				record := &domain.TelemetryRecord{
					GPUUUID:       "GPU-123",
					MetricName:    "DCGM_FI_DEV_GPU_UTIL",
					Value:         "100",
					IngestionTime: time.Now(),
				}
				msg := domain.NewMessage(record, fmt.Sprintf("producer-%d", producerID))
				err := queue.Publish(ctx, msg)
				require.NoError(t, err)
			}
		}(i)
	}

	// Collect messages concurrently
	received := make([]*domain.Message, 0, totalMessages)
	var mu sync.Mutex

	go func() {
		for msg := range subChan {
			mu.Lock()
			received = append(received, msg)
			mu.Unlock()
		}
	}()

	wg.Wait()

	// Give some time for messages to be distributed
	time.Sleep(10 * time.Millisecond)

	mu.Lock()
	receivedCount := len(received)
	mu.Unlock()

	assert.GreaterOrEqual(t, receivedCount, totalMessages, "should receive at least all published messages")
}

func TestInMemoryMessageQueue_GracefulShutdown(t *testing.T) {
	queue := NewInMemoryMessageQueue(10)

	ctx := context.Background()

	// Create multiple subscribers
	numSubscribers := 3
	subChans := make([]<-chan *domain.Message, numSubscribers)
	for i := 0; i < numSubscribers; i++ {
		subChan, err := queue.Subscribe(ctx)
		require.NoError(t, err)
		subChans[i] = subChan
	}

	// Publish some messages
	for i := 0; i < 5; i++ {
		record := &domain.TelemetryRecord{
			GPUUUID:       "GPU-123",
			MetricName:    "DCGM_FI_DEV_GPU_UTIL",
			Value:         "100",
			IngestionTime: time.Now(),
		}
		msg := domain.NewMessage(record, "producer-1")
		err := queue.Publish(ctx, msg)
		require.NoError(t, err)
	}

	// Close the queue
	err := queue.Close()
	require.NoError(t, err)

	// Verify queue is closed
	assert.True(t, queue.IsClosed())

	// Verify subscribers are closed
	// Drain any remaining messages and verify channels are eventually closed
	for _, subChan := range subChans {
		// Drain messages until channel is closed
		closed := false
		for !closed {
			select {
			case _, ok := <-subChan:
				if !ok {
					// Channel is closed
					closed = true
				}
			case <-time.After(200 * time.Millisecond):
				// Timeout - try one more read to check if closed
				select {
				case _, ok := <-subChan:
					if !ok {
						closed = true
					}
				default:
					// Channel might still be open but empty, wait a bit more
					time.Sleep(50 * time.Millisecond)
					_, ok := <-subChan
					if !ok {
						closed = true
					} else {
						// Still has messages, continue draining
					}
				}
			}
		}
		assert.True(t, closed, "subscriber channel should be closed")
	}

	// Verify publishing after close fails
	record := &domain.TelemetryRecord{
		GPUUUID:       "GPU-123",
		MetricName:    "DCGM_FI_DEV_GPU_UTIL",
		Value:         "100",
		IngestionTime: time.Now(),
	}
	msg := domain.NewMessage(record, "producer-1")
	err = queue.Publish(ctx, msg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestInMemoryMessageQueue_SubscribeAfterClose(t *testing.T) {
	queue := NewInMemoryMessageQueue(10)
	queue.Close()

	ctx := context.Background()
	_, err := queue.Subscribe(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestInMemoryMessageQueue_PublishNilMessage(t *testing.T) {
	queue := NewInMemoryMessageQueue(10)
	defer queue.Close()

	ctx := context.Background()
	err := queue.Publish(ctx, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "nil message")
}

func TestInMemoryMessageQueue_ContextCancellation(t *testing.T) {
	queue := NewInMemoryMessageQueue(10)
	defer queue.Close()

	ctx, cancel := context.WithCancel(context.Background())

	// Create a subscriber
	subChan, err := queue.Subscribe(ctx)
	require.NoError(t, err)

	// Cancel the context
	cancel()

	// Wait a bit for cleanup
	time.Sleep(100 * time.Millisecond)

	// Verify subscriber channel is closed
	_, ok := <-subChan
	assert.False(t, ok, "subscriber channel should be closed after context cancellation")
}

func TestInMemoryMessageQueue_GetSubscriberCount(t *testing.T) {
	queue := NewInMemoryMessageQueue(10)
	defer queue.Close()

	ctx := context.Background()

	assert.Equal(t, 0, queue.GetSubscriberCount())

	// Add subscribers
	_, err := queue.Subscribe(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, queue.GetSubscriberCount())

	_, err = queue.Subscribe(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, queue.GetSubscriberCount())

	// Close queue
	queue.Close()
	assert.Equal(t, 0, queue.GetSubscriberCount())
}

func TestInMemoryMessageQueue_MessageOrdering(t *testing.T) {
	queue := NewInMemoryMessageQueue(100)
	defer queue.Close()

	ctx := context.Background()
	subChan, err := queue.Subscribe(ctx)
	require.NoError(t, err)

	// Publish messages in order
	numMessages := 50
	expectedOrder := make([]string, numMessages)
	for i := 0; i < numMessages; i++ {
		record := &domain.TelemetryRecord{
			GPUUUID:       "GPU-123",
			MetricName:    "DCGM_FI_DEV_GPU_UTIL",
			Value:         fmt.Sprintf("%d", i),
			IngestionTime: time.Now(),
		}
		msg := domain.NewMessage(record, "producer-1")
		expectedOrder[i] = msg.ID
		err := queue.Publish(ctx, msg)
		require.NoError(t, err)
	}

	// Receive messages and verify order
	receivedOrder := make([]string, 0, numMessages)
	for i := 0; i < numMessages; i++ {
		select {
		case msg := <-subChan:
			receivedOrder = append(receivedOrder, msg.ID)
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("timeout waiting for message %d", i)
		}
	}

	// Verify order (messages should be received in the same order they were published)
	assert.Equal(t, expectedOrder, receivedOrder)
}
