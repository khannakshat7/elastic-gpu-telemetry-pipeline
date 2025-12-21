package api

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/domain"
)

// Error codes for API errors
const (
	ErrCodeInvalidGPUUUID   = "INVALID_GPU_UUID"
	ErrCodeGPUNotFound      = "GPU_NOT_FOUND"
	ErrCodeInvalidTimeRange = "INVALID_TIME_RANGE"
	ErrCodeInvalidRequest   = "INVALID_REQUEST"
	ErrCodeInternalError    = "INTERNAL_ERROR"
)

// respondError sends an error response in a consistent format
func respondError(c *gin.Context, statusCode int, code, message string) {
	c.JSON(statusCode, ErrorResponse{
		Error:     message,
		Message:   message,
		Code:      code,
		Timestamp: time.Now(),
	})
}

// respondBadRequest sends a 400 Bad Request error
func respondBadRequest(c *gin.Context, code, message string) {
	respondError(c, http.StatusBadRequest, code, message)
}

// respondNotFound sends a 404 Not Found error
func respondNotFound(c *gin.Context, code, message string) {
	respondError(c, http.StatusNotFound, code, message)
}

// respondInternalError sends a 500 Internal Server Error
func respondInternalError(c *gin.Context, err error) {
	respondError(c, http.StatusInternalServerError, ErrCodeInternalError,
		fmt.Sprintf("Internal server error: %v", err))
}

// validateGPUUUID is now in validation.go

// parseTimeRange parses start_time and end_time query parameters
// Returns nil for times that are not provided
func parseTimeRange(c *gin.Context) (*time.Time, *time.Time, error) {
	var startTime, endTime *time.Time

	if startStr := c.Query("start_time"); startStr != "" {
		t, err := time.Parse(time.RFC3339, startStr)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid start_time format: %w", err)
		}
		startTime = &t
	}

	if endStr := c.Query("end_time"); endStr != "" {
		t, err := time.Parse(time.RFC3339, endStr)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid end_time format: %w", err)
		}
		endTime = &t
	}

	// Validate time range
	if startTime != nil && endTime != nil && startTime.After(*endTime) {
		return nil, nil, domain.ErrInvalidTimeRange
	}

	return startTime, endTime, nil
}
