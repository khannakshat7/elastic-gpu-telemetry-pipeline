package domain

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewMessage(t *testing.T) {
	record := &TelemetryRecord{
		GPUUUID:       "GPU-123",
		MetricName:    "DCGM_FI_DEV_GPU_UTIL",
		Value:         "85.5",
		IngestionTime: time.Now(),
	}
	producerID := "streamer-1"

	msg := NewMessage(record, producerID)

	assert.NotEmpty(t, msg.ID)
	assert.Equal(t, record, msg.Payload)
	assert.Equal(t, producerID, msg.ProducerID)
	assert.WithinDuration(t, time.Now(), msg.Timestamp, 1*time.Second)
}

func TestNewMessage_EmptyProducerID(t *testing.T) {
	record := &TelemetryRecord{
		GPUUUID:       "GPU-123",
		MetricName:    "DCGM_FI_DEV_GPU_UTIL",
		Value:         "85.5",
		IngestionTime: time.Now(),
	}

	msg := NewMessage(record, "")

	assert.NotEmpty(t, msg.ID)
	assert.Equal(t, record, msg.Payload)
	assert.Empty(t, msg.ProducerID)
}

func TestNewMessage_NilRecord(t *testing.T) {
	msg := NewMessage(nil, "producer-1")

	assert.NotEmpty(t, msg.ID)
	assert.Nil(t, msg.Payload)
	assert.Equal(t, "producer-1", msg.ProducerID)
}

func TestNewMessage_UniqueIDs(t *testing.T) {
	record := &TelemetryRecord{
		GPUUUID:       "GPU-123",
		MetricName:    "DCGM_FI_DEV_GPU_UTIL",
		Value:         "85.5",
		IngestionTime: time.Now(),
	}

	msg1 := NewMessage(record, "producer-1")
	msg2 := NewMessage(record, "producer-1")

	assert.NotEqual(t, msg1.ID, msg2.ID, "Each message should have a unique ID")
}
