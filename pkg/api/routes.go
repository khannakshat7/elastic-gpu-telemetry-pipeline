package api

import (
	"github.com/gin-gonic/gin"
)

// SetupRoutes configures API routes for the API Gateway.
// Uses Gin framework for HTTP routing and middleware support.
//
// Framework Choice: Gin
// Justification:
// - Already in project dependencies (used in queue service)
// - Fast and lightweight HTTP web framework
// - Excellent middleware support
// - Built-in JSON binding and validation
// - Good OpenAPI/Swagger integration via swaggo
// - Widely used in Go community
// - Simple, intuitive API
func SetupRoutes(router *gin.Engine, handlers *Handlers) {
	// API v1 routes
	v1 := router.Group("/api/v1")
	{
		// GPU routes
		gpus := v1.Group("/gpus")
		{
			// GET /api/v1/gpus
			// Returns all GPUs for which telemetry exists
			gpus.GET("", handlers.ListGPUs)

			// GET /api/v1/gpus/:id/telemetry
			// Returns telemetry entries for a specific GPU
			// Path parameter: :id (GPU UUID)
			// Query parameters: start_time, end_time (optional, RFC3339 format)
			// Example: GET /api/v1/gpus/GPU-123/telemetry?start_time=2025-01-01T00:00:00Z&end_time=2025-01-01T23:59:59Z
			gpus.GET("/:id/telemetry", handlers.GetTelemetryByGPU)
		}
	}
}
