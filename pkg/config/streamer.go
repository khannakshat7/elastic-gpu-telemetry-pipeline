package config

import (
	"os"
	"strconv"
	"time"
)

// StreamerConfig holds configuration specific to the streamer service
type StreamerConfig struct {
	// CSVFilePath is the path to the CSV file to read
	CSVFilePath string

	// StreamInterval is the time to wait between sending each telemetry record
	StreamInterval time.Duration

	// InstanceID is a unique identifier for this streamer instance
	// Used for logging and tracking which instance sent which messages
	InstanceID string

	// QueueServiceURL is the URL of the message queue service
	QueueServiceURL string
}

// LoadStreamerConfig loads streamer configuration from environment variables
func LoadStreamerConfig() *StreamerConfig {
	// Default stream interval: 100ms between records
	defaultInterval := 100 * time.Millisecond
	intervalStr := getEnv("STREAM_INTERVAL_MS", "100")
	if intervalMs, err := strconv.Atoi(intervalStr); err == nil && intervalMs > 0 {
		defaultInterval = time.Duration(intervalMs) * time.Millisecond
	}

	// Generate instance ID if not provided
	instanceID := getEnv("STREAMER_INSTANCE_ID", "")
	if instanceID == "" {
		// Use hostname + PID as default instance ID
		hostname, _ := os.Hostname()
		instanceID = hostname + "-" + strconv.Itoa(os.Getpid())
	}

	return &StreamerConfig{
		CSVFilePath:     getEnv("CSV_FILE_PATH", "./csv/dcgm_metrics_20250718_134233.csv"),
		StreamInterval:  defaultInterval,
		InstanceID:      instanceID,
		QueueServiceURL: getEnv("QUEUE_SERVICE_URL", "http://localhost:8080"),
	}
}
