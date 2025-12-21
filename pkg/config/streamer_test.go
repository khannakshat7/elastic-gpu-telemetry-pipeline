package config

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLoadStreamerConfig_Defaults(t *testing.T) {
	// Clear env vars
	os.Unsetenv("CSV_FILE_PATH")
	os.Unsetenv("STREAM_INTERVAL_MS")
	os.Unsetenv("STREAMER_INSTANCE_ID")
	os.Unsetenv("QUEUE_SERVICE_URL")

	cfg := LoadStreamerConfig()

	assert.Equal(t, "./csv/dcgm_metrics_20250718_134233.csv", cfg.CSVFilePath)
	assert.Equal(t, 100*time.Millisecond, cfg.StreamInterval)
	assert.NotEmpty(t, cfg.InstanceID) // Should be auto-generated
	assert.Equal(t, "http://localhost:8080", cfg.QueueServiceURL)
}

func TestLoadStreamerConfig_WithEnvVars(t *testing.T) {
	os.Setenv("CSV_FILE_PATH", "/custom/path.csv")
	os.Setenv("STREAM_INTERVAL_MS", "500")
	os.Setenv("STREAMER_INSTANCE_ID", "custom-instance")
	os.Setenv("QUEUE_SERVICE_URL", "http://custom-queue:9090")

	defer func() {
		os.Unsetenv("CSV_FILE_PATH")
		os.Unsetenv("STREAM_INTERVAL_MS")
		os.Unsetenv("STREAMER_INSTANCE_ID")
		os.Unsetenv("QUEUE_SERVICE_URL")
	}()

	cfg := LoadStreamerConfig()

	assert.Equal(t, "/custom/path.csv", cfg.CSVFilePath)
	assert.Equal(t, 500*time.Millisecond, cfg.StreamInterval)
	assert.Equal(t, "custom-instance", cfg.InstanceID)
	assert.Equal(t, "http://custom-queue:9090", cfg.QueueServiceURL)
}

func TestLoadStreamerConfig_InvalidInterval(t *testing.T) {
	os.Setenv("STREAM_INTERVAL_MS", "invalid")
	defer os.Unsetenv("STREAM_INTERVAL_MS")

	cfg := LoadStreamerConfig()

	// Should use default when invalid
	assert.Equal(t, 100*time.Millisecond, cfg.StreamInterval)
}

func TestLoadStreamerConfig_ZeroInterval(t *testing.T) {
	os.Setenv("STREAM_INTERVAL_MS", "0")
	defer os.Unsetenv("STREAM_INTERVAL_MS")

	cfg := LoadStreamerConfig()

	// Should use default when zero or negative
	assert.Equal(t, 100*time.Millisecond, cfg.StreamInterval)
}

func TestLoadStreamerConfig_NegativeInterval(t *testing.T) {
	os.Setenv("STREAM_INTERVAL_MS", "-100")
	defer os.Unsetenv("STREAM_INTERVAL_MS")

	cfg := LoadStreamerConfig()

	// Should use default when negative
	assert.Equal(t, 100*time.Millisecond, cfg.StreamInterval)
}
