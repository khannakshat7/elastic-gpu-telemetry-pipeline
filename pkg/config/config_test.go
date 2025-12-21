package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetEnv_WithValue(t *testing.T) {
	key := "TEST_ENV_VAR"
	expectedValue := "test_value"
	os.Setenv(key, expectedValue)
	defer os.Unsetenv(key)

	value := getEnv(key, "default")
	assert.Equal(t, expectedValue, value)
}

func TestGetEnv_WithoutValue(t *testing.T) {
	key := "NON_EXISTENT_VAR"
	defaultValue := "default_value"

	// Ensure the env var is not set
	os.Unsetenv(key)

	value := getEnv(key, defaultValue)
	assert.Equal(t, defaultValue, value)
}

func TestGetEnv_EmptyValue(t *testing.T) {
	key := "EMPTY_ENV_VAR"
	defaultValue := "default_value"
	os.Setenv(key, "")
	defer os.Unsetenv(key)

	value := getEnv(key, defaultValue)
	assert.Equal(t, defaultValue, value)
}

func TestLoadConfig_Defaults(t *testing.T) {
	// Clear all relevant env vars
	envVars := []string{
		"QUEUE_SERVICE_URL",
		"STORAGE_BACKEND",
		"STORAGE_URI",
		"CSV_FILE_PATH",
		"LOG_LEVEL",
		"API_PORT",
	}

	for _, envVar := range envVars {
		os.Unsetenv(envVar)
	}

	cfg := LoadConfig()

	assert.Equal(t, "http://localhost:8080", cfg.QueueServiceURL)
	assert.Equal(t, "memory", cfg.StorageBackend)
	assert.Equal(t, "", cfg.StorageURI)
	assert.Equal(t, "./csv/dcgm_metrics_20250718_134233.csv", cfg.CSVFilePath)
	assert.Equal(t, "info", cfg.LogLevel)
	assert.Equal(t, "8081", cfg.APIPort)
}

func TestLoadConfig_WithEnvVars(t *testing.T) {
	// Set all env vars
	os.Setenv("QUEUE_SERVICE_URL", "http://custom-queue:9090")
	os.Setenv("STORAGE_BACKEND", "mongo")
	os.Setenv("STORAGE_URI", "mongodb://localhost:27017")
	os.Setenv("CSV_FILE_PATH", "/custom/path.csv")
	os.Setenv("LOG_LEVEL", "debug")
	os.Setenv("API_PORT", "9090")

	defer func() {
		os.Unsetenv("QUEUE_SERVICE_URL")
		os.Unsetenv("STORAGE_BACKEND")
		os.Unsetenv("STORAGE_URI")
		os.Unsetenv("CSV_FILE_PATH")
		os.Unsetenv("LOG_LEVEL")
		os.Unsetenv("API_PORT")
	}()

	cfg := LoadConfig()

	assert.Equal(t, "http://custom-queue:9090", cfg.QueueServiceURL)
	assert.Equal(t, "mongo", cfg.StorageBackend)
	assert.Equal(t, "mongodb://localhost:27017", cfg.StorageURI)
	assert.Equal(t, "/custom/path.csv", cfg.CSVFilePath)
	assert.Equal(t, "debug", cfg.LogLevel)
	assert.Equal(t, "9090", cfg.APIPort)
}

func TestLoadConfig_PartialEnvVars(t *testing.T) {
	// Set only some env vars
	os.Setenv("QUEUE_SERVICE_URL", "http://custom-queue:9090")
	os.Setenv("LOG_LEVEL", "warn")

	// Clear others
	os.Unsetenv("STORAGE_BACKEND")
	os.Unsetenv("STORAGE_URI")
	os.Unsetenv("CSV_FILE_PATH")
	os.Unsetenv("API_PORT")

	defer func() {
		os.Unsetenv("QUEUE_SERVICE_URL")
		os.Unsetenv("LOG_LEVEL")
	}()

	cfg := LoadConfig()

	assert.Equal(t, "http://custom-queue:9090", cfg.QueueServiceURL)
	assert.Equal(t, "warn", cfg.LogLevel)
	// Should use defaults for others
	assert.Equal(t, "memory", cfg.StorageBackend)
	assert.Equal(t, "./csv/dcgm_metrics_20250718_134233.csv", cfg.CSVFilePath)
	assert.Equal(t, "8081", cfg.APIPort)
}
