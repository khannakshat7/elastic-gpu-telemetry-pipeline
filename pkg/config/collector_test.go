package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoadCollectorConfig_Defaults(t *testing.T) {
	// Clear env vars
	os.Unsetenv("COLLECTOR_INSTANCE_ID")
	os.Unsetenv("COLLECTOR_BATCH_SIZE")
	os.Unsetenv("QUEUE_SERVICE_URL")
	os.Unsetenv("STORAGE_BACKEND")
	os.Unsetenv("STORAGE_URI")

	cfg := LoadCollectorConfig()

	assert.NotEmpty(t, cfg.InstanceID) // Should be auto-generated
	assert.Equal(t, 1, cfg.BatchSize)  // Default batch size
	assert.Equal(t, "http://localhost:8080", cfg.QueueServiceURL)
	assert.Equal(t, "memory", cfg.StorageBackend)
	assert.Empty(t, cfg.StorageURI)
}

func TestLoadCollectorConfig_WithEnvVars(t *testing.T) {
	os.Setenv("COLLECTOR_INSTANCE_ID", "custom-collector")
	os.Setenv("COLLECTOR_BATCH_SIZE", "10")
	os.Setenv("QUEUE_SERVICE_URL", "http://custom-queue:9090")
	os.Setenv("STORAGE_BACKEND", "mongo")
	os.Setenv("STORAGE_URI", "mongodb://localhost:27017")

	defer func() {
		os.Unsetenv("COLLECTOR_INSTANCE_ID")
		os.Unsetenv("COLLECTOR_BATCH_SIZE")
		os.Unsetenv("QUEUE_SERVICE_URL")
		os.Unsetenv("STORAGE_BACKEND")
		os.Unsetenv("STORAGE_URI")
	}()

	cfg := LoadCollectorConfig()

	assert.Equal(t, "custom-collector", cfg.InstanceID)
	assert.Equal(t, 10, cfg.BatchSize)
	assert.Equal(t, "http://custom-queue:9090", cfg.QueueServiceURL)
	assert.Equal(t, "mongo", cfg.StorageBackend)
	assert.Equal(t, "mongodb://localhost:27017", cfg.StorageURI)
}

func TestLoadCollectorConfig_InvalidBatchSize(t *testing.T) {
	os.Setenv("COLLECTOR_BATCH_SIZE", "invalid")
	defer os.Unsetenv("COLLECTOR_BATCH_SIZE")

	cfg := LoadCollectorConfig()

	// Should use default when invalid
	assert.Equal(t, 1, cfg.BatchSize)
}

func TestLoadCollectorConfig_ZeroBatchSize(t *testing.T) {
	os.Setenv("COLLECTOR_BATCH_SIZE", "0")
	defer os.Unsetenv("COLLECTOR_BATCH_SIZE")

	cfg := LoadCollectorConfig()

	// Should use default when zero or negative
	assert.Equal(t, 1, cfg.BatchSize)
}

func TestLoadCollectorConfig_NegativeBatchSize(t *testing.T) {
	os.Setenv("COLLECTOR_BATCH_SIZE", "-5")
	defer os.Unsetenv("COLLECTOR_BATCH_SIZE")

	cfg := LoadCollectorConfig()

	// Should use default when negative
	assert.Equal(t, 1, cfg.BatchSize)
}

