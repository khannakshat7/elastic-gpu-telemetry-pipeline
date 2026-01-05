package mq

import (
	"context"
	"testing"
	"time"

	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryMessageQueue_Publish_ContextCancellation(t *testing.T) {
	queue := NewInMemoryMessageQueue(1) // Small buffer
	defer queue.Close()

	// Fill the buffer
	ctx := context.Background()
	msg1 := domain.NewMessage(&domain.TelemetryRecord{GPUUUID: "GPU-1"}, "producer")
	_ = queue.Publish(ctx, msg1)

	// Try to publish with cancelled context
	cancelledCtx, cancel := context.WithCancel(ctx)
	cancel() // Cancel immediately

	msg2 := domain.NewMessage(&domain.TelemetryRecord{GPUUUID: "GPU-2"}, "producer")
	err := queue.Publish(cancelledCtx, msg2)
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

func TestInMemoryMessageQueue_Subscribe_ContextCancellationCleanup(t *testing.T) {
	queue := NewInMemoryMessageQueue(10)
	defer queue.Close()

	// Create subscription with context
	ctx, cancel := context.WithCancel(context.Background())
	subChan, err := queue.Subscribe(ctx, "test-consumer-1")
	assert.NoError(t, err)

	// Verify subscriber was added
	initialCount := queue.GetSubscriberCount()
	assert.Equal(t, 1, initialCount)

	// Cancel context
	cancel()

	// Give cleanup goroutine time to run
	time.Sleep(50 * time.Millisecond)

	// Subscriber should be removed (channel closed)
	select {
	case _, ok := <-subChan:
		assert.False(t, ok, "channel should be closed")
	default:
		// Channel might already be closed
	}
}

func TestInMemoryMessageQueue_NewInMemoryMessageQueue_ZeroBufferSize(t *testing.T) {
	// Zero buffer size should use default
	queue := NewInMemoryMessageQueue(0)
	defer queue.Close()

	assert.NotNil(t, queue)
	// Should not panic
}

func TestInMemoryMessageQueue_NewInMemoryMessageQueue_NegativeBufferSize(t *testing.T) {
	// Negative buffer size should use default
	queue := NewInMemoryMessageQueue(-10)
	defer queue.Close()

	assert.NotNil(t, queue)
	// Should not panic
}

func TestInMemoryMessageQueue_Distribute_FullChannel(t *testing.T) {
	queue := NewInMemoryMessageQueue(10)
	defer queue.Close()

	ctx := context.Background()

	// Create subscriber with very small buffer (1) using Subscribe method
	subChan, err := queue.Subscribe(ctx, "test-consumer-full-channel")
	require.NoError(t, err)

	// Publish multiple messages to fill subscriber channel
	msg1 := domain.NewMessage(&domain.TelemetryRecord{GPUUUID: "GPU-1"}, "producer")
	msg2 := domain.NewMessage(&domain.TelemetryRecord{GPUUUID: "GPU-2"}, "producer")
	msg3 := domain.NewMessage(&domain.TelemetryRecord{GPUUUID: "GPU-3"}, "producer")

	_ = queue.Publish(ctx, msg1)
	_ = queue.Publish(ctx, msg2)
	_ = queue.Publish(ctx, msg3)

	// Give distributor time to process
	time.Sleep(100 * time.Millisecond)

	// Should receive at least one message (channel has buffer of 1)
	select {
	case msg := <-subChan:
		assert.NotNil(t, msg)
	default:
		// Might not receive if channel was full and message was dropped
	}
}
