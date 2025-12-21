package main

import (
	"os"

	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/internal/streamer"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/config"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/mq"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/telemetry"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/utils"
)

func main() {
	// Setup logger
	utils.SetupLogger()
	utils.Logger.Info("Starting Telemetry Streamer service")

	// Load configuration
	cfg := config.LoadStreamerConfig()
	utils.Logger.Info("Configuration loaded",
		"csv_file", cfg.CSVFilePath,
		"stream_interval", cfg.StreamInterval,
		"instance_id", cfg.InstanceID,
		"queue_url", cfg.QueueServiceURL)

	// Initialize components
	parser := telemetry.NewCSVParser()

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

	// Create streamer
	str, err := streamer.NewStreamer(cfg, parser, queue)
	if err != nil {
		utils.Logger.Error("Failed to create streamer", "error", err)
		os.Exit(1)
	}

	// Load CSV file
	if err := str.LoadCSV(); err != nil {
		utils.Logger.Error("Failed to load CSV", "error", err)
		os.Exit(1)
	}

	// Start streaming (blocks until shutdown)
	if err := str.Start(); err != nil {
		utils.Logger.Error("Streamer error", "error", err)
		os.Exit(1)
	}

	utils.Logger.Info("Telemetry Streamer service stopped")
}
