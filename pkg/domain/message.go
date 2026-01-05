package domain

import (
	"time"

	"github.com/google/uuid"
)

// Message represents a message in the queue wrapping telemetry events
type Message struct {
	// ID is a unique identifier for the message
	ID string `json:"id"`

	// Payload contains the telemetry record data
	Payload *TelemetryRecord `json:"payload"`

	// Timestamp is when the message was created/published
	Timestamp time.Time `json:"timestamp"`

	// ProducerID identifies which streamer produced this message
	ProducerID string `json:"producer_id,omitempty"`

	// Acked indicates whether the message has been acknowledged by a consumer
	// Messages must be acknowledged after successful processing to prevent loss
	Acked bool `json:"acked,omitempty"`

	// AckedAt is the timestamp when the message was acknowledged
	AckedAt *time.Time `json:"acked_at,omitempty"`

	// ConsumerID identifies which collector consumed this message (for work queue pattern)
	ConsumerID string `json:"consumer_id,omitempty"`
}

// Ack marks the message as acknowledged
func (m *Message) Ack(consumerID string) {
	m.Acked = true
	now := time.Now()
	m.AckedAt = &now
	m.ConsumerID = consumerID
}

// NewMessage creates a new message with a generated ID and current timestamp
func NewMessage(record *TelemetryRecord, producerID string) *Message {
	return &Message{
		ID:         uuid.New().String(),
		Payload:    record,
		Timestamp:  time.Now(),
		ProducerID: producerID,
	}
}
