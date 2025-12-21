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
