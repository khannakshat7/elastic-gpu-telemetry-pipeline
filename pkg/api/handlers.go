package api

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/storage"
)

// Handlers contains HTTP handlers for the API Gateway.
// Handlers use DTOs (Data Transfer Objects) to avoid exposing internal domain structs.
type Handlers struct {
	repository storage.Repository
}

// NewHandlers creates a new handlers instance with the required dependencies
func NewHandlers(repository storage.Repository) *Handlers {
	return &Handlers{
		repository: repository,
	}
}

// ListGPUs handles GET /api/v1/gpus
//
//	@Summary		List all GPUs with telemetry
//	@Description	Returns a list of all GPUs for which telemetry data exists. The response includes GPU UUID, device ID, model, and hostname.
//	@Tags			GPUs
//	@Accept			json
//	@Produce		json
//	@Success		200	{object}	ListGPUsResponse	"List of GPUs with telemetry data"
//	@Failure		500	{object}	ErrorResponse		"Internal server error"
//	@Router			/gpus [get]
func (h *Handlers) ListGPUs(c *gin.Context) {
	ctx := c.Request.Context()

	// Call repository to get all GPUs with telemetry
	gpus, err := h.repository.ListGPUs(ctx)
	if err != nil {
		respondInternalError(c, err)
		return
	}

	// Convert domain models to DTOs
	gpuResponses := make([]GPUResponse, len(gpus))
	for i, gpu := range gpus {
		gpuResponses[i] = GPUResponse{
			UUID:      gpu.UUID,
			DeviceID:  gpu.Device,
			GPUIndex:  gpu.GPUID,
			ModelName: gpu.Model,
			Hostname:  gpu.Hostname,
			Container: gpu.Container,
			Pod:       gpu.Pod,
			Namespace: gpu.Namespace,
		}
	}

	// Return response
	c.JSON(http.StatusOK, ListGPUsResponse{
		GPUs:  gpuResponses,
		Count: len(gpuResponses),
	})
}

// GetTelemetryByGPU handles GET /api/v1/gpus/{id}/telemetry
//
//	@Summary		Get telemetry for a specific GPU
//	@Description	Returns all telemetry entries for a specific GPU, ordered by ingestion time (oldest first). Supports optional time window filters via query parameters. Both start_time and end_time are inclusive.
//	@Tags			Telemetry
//	@Accept			json
//	@Produce		json
//	@Param			id			path		string	true	"GPU UUID"													example(GPU-5fd4f087-86f3-7a43-b711-4771313afc50)
//	@Param			start_time	query		string	false	"Start time (inclusive, RFC3339 format)"					example(2025-01-01T00:00:00Z)
//	@Param			end_time	query		string	false	"End time (inclusive, RFC3339 format)"					example(2025-01-01T23:59:59Z)
//	@Success		200			{object}	GetTelemetryResponse	"Telemetry records for the GPU"
//	@Failure		400			{object}	ErrorResponse		"Invalid GPU UUID or time range"
//	@Failure		404			{object}	ErrorResponse		"GPU not found"
//	@Failure		500			{object}	ErrorResponse		"Internal server error"
//	@Router			/gpus/{id}/telemetry [get]
func (h *Handlers) GetTelemetryByGPU(c *gin.Context) {
	ctx := c.Request.Context()

	// Extract GPU UUID from path parameter
	gpuUUID := c.Param("id")

	// Validate GPU UUID
	if err := validateGPUUUID(gpuUUID); err != nil {
		respondBadRequest(c, ErrCodeInvalidGPUUUID, "Invalid GPU UUID")
		return
	}

	// Parse optional time range query parameters
	startTime, endTime, err := parseTimeRange(c)
	if err != nil {
		respondBadRequest(c, ErrCodeInvalidTimeRange, err.Error())
		return
	}

	// Verify GPU exists (optional check - can be skipped if repository handles it)
	gpu, err := h.repository.GetGPU(ctx, gpuUUID)
	if err != nil {
		respondInternalError(c, err)
		return
	}
	if gpu == nil {
		respondNotFound(c, ErrCodeGPUNotFound, fmt.Sprintf("GPU with UUID '%s' not found", gpuUUID))
		return
	}

	// Call repository to get telemetry records
	records, err := h.repository.GetTelemetryByGPU(ctx, gpuUUID, startTime, endTime)
	if err != nil {
		respondInternalError(c, err)
		return
	}

	// Convert domain models to DTOs
	telemetryResponses := make([]TelemetryResponse, len(records))
	for i, record := range records {
		telemetryResponses[i] = TelemetryResponse{
			GPUUUID:       record.GPUUUID,
			MetricName:    record.MetricName,
			Value:         record.Value,
			IngestionTime: record.IngestionTime,
			Container:     record.Container,
			Pod:           record.Pod,
			Namespace:     record.Namespace,
			Hostname:      record.Hostname,
			ModelName:     record.ModelName,
		}
	}

	// Return response
	response := GetTelemetryResponse{
		GPUUUID:   gpuUUID,
		Records:   telemetryResponses,
		Count:     len(telemetryResponses),
		StartTime: startTime,
		EndTime:   endTime,
	}

	c.JSON(http.StatusOK, response)
}
