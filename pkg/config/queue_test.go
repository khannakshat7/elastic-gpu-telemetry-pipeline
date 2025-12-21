package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoadQueueConfig_Defaults(t *testing.T) {
	// Clear env vars
	os.Unsetenv("QUEUE_PORT")
	os.Unsetenv("QUEUE_BUFFER_SIZE")

	cfg := LoadQueueConfig()

	assert.Equal(t, "8080", cfg.Port)
	assert.Equal(t, 1000, cfg.BufferSize)
}

func TestLoadQueueConfig_WithEnvVars(t *testing.T) {
	os.Setenv("QUEUE_PORT", "9090")
	os.Setenv("QUEUE_BUFFER_SIZE", "5000")

	defer func() {
		os.Unsetenv("QUEUE_PORT")
		os.Unsetenv("QUEUE_BUFFER_SIZE")
	}()

	cfg := LoadQueueConfig()

	assert.Equal(t, "9090", cfg.Port)
	assert.Equal(t, 5000, cfg.BufferSize)
}

func TestLoadQueueConfig_InvalidBufferSize(t *testing.T) {
	os.Setenv("QUEUE_BUFFER_SIZE", "invalid")
	defer os.Unsetenv("QUEUE_BUFFER_SIZE")

	cfg := LoadQueueConfig()

	// Should use default when invalid
	assert.Equal(t, 1000, cfg.BufferSize)
}

func TestLoadQueueConfig_ZeroBufferSize(t *testing.T) {
	os.Setenv("QUEUE_BUFFER_SIZE", "0")
	defer os.Unsetenv("QUEUE_BUFFER_SIZE")

	cfg := LoadQueueConfig()

	// Should use default when zero or negative
	assert.Equal(t, 1000, cfg.BufferSize)
}

func TestLoadQueueConfig_NegativeBufferSize(t *testing.T) {
	os.Setenv("QUEUE_BUFFER_SIZE", "-100")
	defer os.Unsetenv("QUEUE_BUFFER_SIZE")

	cfg := LoadQueueConfig()

	// Should use default when negative
	assert.Equal(t, 1000, cfg.BufferSize)
}
