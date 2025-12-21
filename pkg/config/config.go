package config

import (
	"os"
)

// Config holds application configuration
type Config struct {
	QueueServiceURL string
	StorageBackend  string
	StorageURI      string
	CSVFilePath     string
	LogLevel        string
	APIPort         string
}

// LoadConfig loads configuration from environment variables
func LoadConfig() *Config {
	return &Config{
		QueueServiceURL: getEnv("QUEUE_SERVICE_URL", "http://localhost:8080"),
		StorageBackend:  getEnv("STORAGE_BACKEND", "memory"),
		StorageURI:       getEnv("STORAGE_URI", ""),
		CSVFilePath:      getEnv("CSV_FILE_PATH", "./csv/dcgm_metrics_20250718_134233.csv"),
		LogLevel:         getEnv("LOG_LEVEL", "info"),
		APIPort:          getEnv("API_PORT", "8081"),
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

