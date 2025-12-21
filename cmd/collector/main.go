package main

import (
	"os"

	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/internal/collector"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/config"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/mq"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/storage"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/utils"
)

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func main() {
	// Setup logger
	utils.SetupLogger()
	utils.Logger.Info("Starting Telemetry Collector service")

	// Load configuration
	cfg := config.LoadCollectorConfig()
	utils.Logger.Info("Configuration loaded",
		"instance_id", cfg.InstanceID,
		"batch_size", cfg.BatchSize,
		"storage_backend", cfg.StorageBackend,
		"queue_url", cfg.QueueServiceURL)

	// Initialize components
	// Create queue client - use HTTP client if queue service URL is provided, otherwise use in-memory
	var queue mq.MessageQueue
	if cfg.QueueServiceURL != "" && cfg.QueueServiceURL != "in-memory" {
		utils.Logger.Info("Connecting to queue service", "url", cfg.QueueServiceURL)
		queue = mq.NewHTTPMessageQueue(cfg.QueueServiceURL)
	} else {
		utils.Logger.Info("Using in-memory queue")
		queue = mq.NewInMemoryMessageQueue(1000)
	}
	defer queue.Close()

	// Create storage repository
	storageConfig := map[string]string{}
	if cfg.StorageURI != "" {
		storageConfig["uri"] = cfg.StorageURI
	}
	// Check if storage service URL is provided (for shared storage)
	if storageServiceURL := getEnv("STORAGE_SERVICE_URL", ""); storageServiceURL != "" {
		storageConfig["storage_service_url"] = storageServiceURL
		utils.Logger.Info("Connecting to storage service", "url", storageServiceURL)
	}

	repository, err := storage.NewRepository(storage.BackendType(cfg.StorageBackend), storageConfig)
	if err != nil {
		utils.Logger.Error("Failed to create storage repository", "error", err)
		os.Exit(1)
	}

	// Create collector
	col, err := collector.NewCollector(cfg, queue, repository)
	if err != nil {
		utils.Logger.Error("Failed to create collector", "error", err)
		os.Exit(1)
	}

	// Start collecting (blocks until shutdown)
	if err := col.Start(); err != nil {
		utils.Logger.Error("Collector error", "error", err)
		os.Exit(1)
	}

	utils.Logger.Info("Telemetry Collector service stopped")
}
