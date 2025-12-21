package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"

	// Import swagger docs to trigger init() function that registers the docs
	_ "github.com/khannakshat7/elastic-gpu-telemetry-pipeline/docs/swagger"
	swagger "github.com/khannakshat7/elastic-gpu-telemetry-pipeline/docs/swagger"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/internal/health"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/api"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/config"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/storage"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/utils"
)

// @title GPU Telemetry Pipeline API
// @version 1.0
// @description REST API for querying GPU telemetry data
// @termsOfService http://swagger.io/terms/

// @contact.name API Support
// @contact.email support@example.com

// @license.name Apache 2.0
// @license.url http://www.apache.org/licenses/LICENSE-2.0.html

// @host localhost:8081
// @BasePath /api/v1

func main() {
	// Setup logger
	utils.SetupLogger()
	utils.Logger.Info("Starting API Gateway service")

	// Load configuration
	cfg := config.LoadConfig()
	utils.Logger.Info("Configuration loaded",
		"port", cfg.APIPort,
		"storage_backend", cfg.StorageBackend)

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

	// Create handlers
	handlers := api.NewHandlers(repository)

	// Setup Gin router
	router := gin.Default()

	// Apply middleware
	router.Use(api.LoggerMiddleware())
	router.Use(api.RecoveryMiddleware())
	router.Use(api.RequestIDMiddleware())
	router.Use(api.ErrorHandlerMiddleware())
	router.Use(api.TimeoutMiddleware(30 * time.Second))

	// Health check endpoints (before API routes)
	healthHandler := health.NewHandler()
	router.GET("/health", healthHandler.Liveness)
	router.GET("/ready", healthHandler.Readiness)

	// Setup API routes
	api.SetupRoutes(router, handlers)

	// Swagger UI endpoint
	// Import and configure the generated swagger docs
	// The import of "docs/swagger" above triggers the init() function which registers the swagger docs
	// We reference swagger.SwaggerInfo to ensure the package is imported and init() runs
	_ = swagger.SwaggerInfo // Ensure package is imported (triggers init())
	swagger.SwaggerInfo.Host = "localhost:" + cfg.APIPort
	swagger.SwaggerInfo.BasePath = "/api/v1"

	// Swagger UI - serves the UI and doc.json
	// ginSwagger.WrapHandler automatically finds the registered swagger docs via swag.GetSwagger()
	router.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))

	// Create HTTP server
	server := &http.Server{
		Addr:         ":" + cfg.APIPort,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start server in goroutine
	errCh := make(chan error, 1)
	go func() {
		utils.Logger.Info("API Gateway starting", "port", cfg.APIPort)
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
	utils.Logger.Info("Shutting down API Gateway")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		utils.Logger.Error("Error shutting down server", "error", err)
		os.Exit(1)
	}

	utils.Logger.Info("API Gateway service stopped")
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
