package config

import (
	"strconv"
)

// QueueConfig holds configuration specific to the queue service
type QueueConfig struct {
	// Port is the HTTP port for the queue service
	Port string

	// BufferSize is the size of the message buffer channel
	BufferSize int
}

// LoadQueueConfig loads queue service configuration from environment variables
func LoadQueueConfig() *QueueConfig {
	// Parse buffer size
	bufferSize := 1000 // Default buffer size
	if bufferSizeStr := getEnv("QUEUE_BUFFER_SIZE", ""); bufferSizeStr != "" {
		if bs, err := strconv.Atoi(bufferSizeStr); err == nil && bs > 0 {
			bufferSize = bs
		}
	}

	return &QueueConfig{
		Port:       getEnv("QUEUE_PORT", "8080"),
		BufferSize: bufferSize,
	}
}
