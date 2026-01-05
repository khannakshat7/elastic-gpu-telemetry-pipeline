package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/internal/health"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/config"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/domain"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/storage"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/storage/memory"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/utils"
)

func main() {
	// Setup logger
	utils.SetupLogger()
	utils.Logger.Info("Starting Storage Service")

	// Load configuration
	cfg := config.LoadConfig()
	port := getEnv("STORAGE_PORT", "8082")
	utils.Logger.Info("Configuration loaded",
		"port", port,
		"storage_backend", cfg.StorageBackend)

	// Create storage repository
	var repository storage.Repository
	if cfg.StorageBackend == "memory" {
		repository = memory.NewStore()
		utils.Logger.Info("Using in-memory storage")
	} else {
		// For other backends, use factory
		storageConfig := map[string]string{}

		// Build PostgreSQL connection string from environment variables if backend is postgres
		if cfg.StorageBackend == "postgres" {
			postgresHost := getEnv("POSTGRES_HOST", "localhost")
			postgresPort := getEnv("POSTGRES_PORT", "5432")
			postgresUser := getEnv("POSTGRES_USER", "postgres")
			postgresPassword := getEnv("POSTGRES_PASSWORD", "postgres")
			postgresDB := getEnv("POSTGRES_DB", "gpu_telemetry")
			postgresSSLMode := getEnv("POSTGRES_SSLMODE", "disable")

			// Build connection string
			connectionString := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
				postgresHost, postgresPort, postgresUser, postgresPassword, postgresDB, postgresSSLMode)
			storageConfig["connection_string"] = connectionString
			utils.Logger.Info("Using PostgreSQL storage", "host", postgresHost, "db", postgresDB)
		} else if cfg.StorageURI != "" {
			storageConfig["uri"] = cfg.StorageURI
		}

		var err error
		repository, err = storage.NewRepository(storage.BackendType(cfg.StorageBackend), storageConfig)
		if err != nil {
			utils.Logger.Error("Failed to create storage repository", "error", err)
			os.Exit(1)
		}
	}

	// Setup Gin router
	router := gin.Default()

	// Health check endpoints
	healthHandler := health.NewHandler()
	router.GET("/health", healthHandler.Liveness)
	router.GET("/ready", healthHandler.Readiness)

	// Storage API endpoints
	api := router.Group("/api/v1/storage")
	{
		// GET /api/v1/storage/gpus - List all GPUs
		api.GET("/gpus", func(c *gin.Context) {
			gpus, err := repository.ListGPUs(c.Request.Context())
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, gpus)
		})

		// GET /api/v1/storage/gpus/:uuid - Get specific GPU
		api.GET("/gpus/:uuid", func(c *gin.Context) {
			uuid := c.Param("uuid")
			gpu, err := repository.GetGPU(c.Request.Context(), uuid)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			if gpu == nil {
				c.JSON(http.StatusNotFound, gin.H{"error": "GPU not found"})
				return
			}
			c.JSON(http.StatusOK, gpu)
		})

		// POST /api/v1/storage/gpus - Save GPU
		api.POST("/gpus", func(c *gin.Context) {
			var gpu domain.GPU
			if err := c.ShouldBindJSON(&gpu); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				return
			}
			if err := repository.SaveGPU(c.Request.Context(), &gpu); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusCreated, gin.H{"status": "saved", "uuid": gpu.UUID})
		})

		// POST /api/v1/storage/telemetry - Save telemetry
		api.POST("/telemetry", func(c *gin.Context) {
			var record domain.TelemetryRecord
			if err := c.ShouldBindJSON(&record); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				return
			}
			if err := repository.SaveTelemetry(c.Request.Context(), &record); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusCreated, gin.H{"status": "saved"})
		})

		// GET /api/v1/storage/gpus/:uuid/telemetry - Get telemetry for GPU
		api.GET("/gpus/:uuid/telemetry", func(c *gin.Context) {
			uuid := c.Param("uuid")
			var startTime, endTime *time.Time

			if startStr := c.Query("start_time"); startStr != "" {
				t, err := time.Parse(time.RFC3339, startStr)
				if err == nil {
					startTime = &t
				}
			}
			if endStr := c.Query("end_time"); endStr != "" {
				t, err := time.Parse(time.RFC3339, endStr)
				if err == nil {
					endTime = &t
				}
			}

			records, err := repository.GetTelemetryByGPU(c.Request.Context(), uuid, startTime, endTime)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, records)
		})
	}

	// Create HTTP server
	server := &http.Server{
		Addr:         ":" + port,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start server in goroutine
	errCh := make(chan error, 1)
	go func() {
		utils.Logger.Info("Storage service starting", "port", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
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
	utils.Logger.Info("Shutting down Storage Service")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		utils.Logger.Error("Error shutting down server", "error", err)
		os.Exit(1)
	}

	utils.Logger.Info("Storage Service stopped")

	// Close storage repository if it has a Close method (e.g., PostgreSQL)
	if closer, ok := repository.(interface{ Close() error }); ok {
		if err := closer.Close(); err != nil {
			utils.Logger.Error("Error closing storage repository", "error", err)
		}
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
