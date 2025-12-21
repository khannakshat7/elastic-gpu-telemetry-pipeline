package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/config"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/mq"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/utils"
)

func main() {
	// Setup logger
	utils.SetupLogger()
	utils.Logger.Info("Starting Message Queue service")

	// Load configuration
	cfg := config.LoadQueueConfig()
	utils.Logger.Info("Configuration loaded",
		"port", cfg.Port,
		"buffer_size", cfg.BufferSize)

	// Create in-memory message queue
	queue := mq.NewInMemoryMessageQueue(cfg.BufferSize)
	defer func() {
		if err := queue.Close(); err != nil {
			utils.Logger.Error("Error closing queue", "error", err)
		}
	}()

	// Create and start HTTP server
	server := mq.NewServer(queue)

	// Start server in background
	errCh := make(chan error, 1)
	go func() {
		if err := server.Start(cfg.Port); err != nil {
			errCh <- err
		}
	}()

	// Setup signal handling for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	// Wait for shutdown signal or error
	select {
	case err := <-errCh:
		utils.Logger.Error("Server error", "error", err)
		os.Exit(1)
	case sig := <-sigCh:
		utils.Logger.Info("Received shutdown signal", "signal", sig)
	}

	// Graceful shutdown
	if err := server.Stop(); err != nil {
		utils.Logger.Error("Error stopping server", "error", err)
		os.Exit(1)
	}

	utils.Logger.Info("Message Queue service stopped")
}
