package domain

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMessage_Ack(t *testing.T) {
	record := &TelemetryRecord{
		GPUUUID:    "GPU-123",
		MetricName: "DCGM_FI_DEV_GPU_UTIL",
		Value:      "100",
	}
	msg := NewMessage(record, "producer-1")

	// Initially not acknowledged
	assert.False(t, msg.Acked)
	assert.Nil(t, msg.AckedAt)
	assert.Empty(t, msg.ConsumerID)

	// Acknowledge the message
	consumerID := "consumer-123"
	msg.Ack(consumerID)

	// Verify acknowledgment
	assert.True(t, msg.Acked)
	assert.NotNil(t, msg.AckedAt)
	assert.WithinDuration(t, time.Now(), *msg.AckedAt, 1*time.Second)
	assert.Equal(t, consumerID, msg.ConsumerID)
}

func TestMessage_Ack_MultipleTimes(t *testing.T) {
	record := &TelemetryRecord{
		GPUUUID:    "GPU-123",
		MetricName: "DCGM_FI_DEV_GPU_UTIL",
		Value:      "100",
	}
	msg := NewMessage(record, "producer-1")

	// First acknowledgment
	msg.Ack("consumer-1")
	firstAckTime := msg.AckedAt

	// Second acknowledgment (should update)
	time.Sleep(10 * time.Millisecond)
	msg.Ack("consumer-2")

	// Should still be acknowledged, but with new consumer ID
	assert.True(t, msg.Acked)
	assert.NotNil(t, msg.AckedAt)
	assert.Equal(t, "consumer-2", msg.ConsumerID)
	// AckedAt should be updated
	assert.True(t, msg.AckedAt.After(*firstAckTime))
}

func TestNewMessage(t *testing.T) {
	record := &TelemetryRecord{
		GPUUUID:    "GPU-123",
		MetricName: "DCGM_FI_DEV_GPU_UTIL",
		Value:      "100",
	}
	producerID := "producer-1"

	msg := NewMessage(record, producerID)

	// Verify message fields
	assert.NotEmpty(t, msg.ID)
	assert.Equal(t, record, msg.Payload)
	assert.Equal(t, producerID, msg.ProducerID)
	assert.WithinDuration(t, time.Now(), msg.Timestamp, 1*time.Second)
	assert.False(t, msg.Acked)
	assert.Nil(t, msg.AckedAt)
	assert.Empty(t, msg.ConsumerID)
}

func TestNewMessage_EmptyProducerID(t *testing.T) {
	record := &TelemetryRecord{
		GPUUUID: "GPU-123",
	}
	msg := NewMessage(record, "")

	assert.NotEmpty(t, msg.ID)
	assert.Equal(t, record, msg.Payload)
	assert.Empty(t, msg.ProducerID)
}
