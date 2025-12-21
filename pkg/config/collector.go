package config

import (
	"os"
	"strconv"
)

// CollectorConfig holds configuration specific to the collector service
type CollectorConfig struct {
	// InstanceID is a unique identifier for this collector instance
	// Used for logging and tracking which instance processed which messages
	InstanceID string

	// BatchSize is the number of messages to process in a batch before committing
	// Set to 1 for immediate processing, or higher for batch processing
	BatchSize int

	// QueueServiceURL is the URL of the message queue service
	QueueServiceURL string

	// StorageBackend is the storage backend to use (memory, mongo, etc.)
	StorageBackend string

	// StorageURI is the connection string for the storage backend
	StorageURI string
}

// LoadCollectorConfig loads collector configuration from environment variables
func LoadCollectorConfig() *CollectorConfig {
	// Generate instance ID if not provided
	instanceID := getEnv("COLLECTOR_INSTANCE_ID", "")
	if instanceID == "" {
		// Use hostname + PID as default instance ID
		hostname, _ := os.Hostname()
		instanceID = hostname + "-collector-" + strconv.Itoa(os.Getpid())
	}

	// Parse batch size
	batchSize := 1 // Default: process immediately
	if batchSizeStr := getEnv("COLLECTOR_BATCH_SIZE", ""); batchSizeStr != "" {
		if bs, err := strconv.Atoi(batchSizeStr); err == nil && bs > 0 {
			batchSize = bs
		}
	}

	return &CollectorConfig{
		InstanceID:      instanceID,
		BatchSize:       batchSize,
		QueueServiceURL: getEnv("QUEUE_SERVICE_URL", "http://localhost:8080"),
		StorageBackend:  getEnv("STORAGE_BACKEND", "memory"),
		StorageURI:      getEnv("STORAGE_URI", ""),
	}
}
