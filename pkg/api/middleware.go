package api

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/utils"
)

// LoggerMiddleware logs HTTP requests with structured logging
func LoggerMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		method := c.Request.Method

		// Process request
		c.Next()

		// Log request details
		latency := time.Since(start)
		status := c.Writer.Status()

		utils.Logger.Info("HTTP request",
			"method", method,
			"path", path,
			"status", status,
			"latency", latency,
			"client_ip", c.ClientIP(),
			"user_agent", c.Request.UserAgent(),
		)
	}
}

// RecoveryMiddleware recovers from panics and returns a proper error response
func RecoveryMiddleware() gin.HandlerFunc {
	return gin.CustomRecovery(func(c *gin.Context, recovered interface{}) {
		err, ok := recovered.(string)
		if !ok {
			err = fmt.Sprintf("%v", recovered)
		}

		utils.Logger.Error("Panic recovered",
			"error", err,
			"path", c.Request.URL.Path,
			"method", c.Request.Method,
		)

		respondError(c, http.StatusInternalServerError, ErrCodeInternalError,
			"Internal server error: panic recovered")
		c.Abort()
	})
}

// RequestIDMiddleware adds a unique request ID to each request
func RequestIDMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Check if request ID already exists in header
		requestID := c.GetHeader("X-Request-ID")
		if requestID == "" {
			// Generate new request ID
			requestID = uuid.New().String()
		}

		// Set request ID in context and response header
		c.Set("request_id", requestID)
		c.Header("X-Request-ID", requestID)
		c.Next()
	}
}

// TimeoutMiddleware adds a timeout to requests
func TimeoutMiddleware(timeout time.Duration) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), timeout)
		defer cancel()

		c.Request = c.Request.WithContext(ctx)

		done := make(chan struct{})
		go func() {
			c.Next()
			close(done)
		}()

		select {
		case <-done:
			// Request completed
		case <-ctx.Done():
			// Timeout occurred
			c.Abort()
			respondError(c, http.StatusRequestTimeout, ErrCodeInternalError,
				fmt.Sprintf("Request timeout after %v", timeout))
		}
	}
}

// ErrorHandlerMiddleware handles errors consistently
func ErrorHandlerMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()

		// Check if there are any errors
		if len(c.Errors) > 0 {
			err := c.Errors.Last()
			utils.Logger.Error("Request error",
				"error", err.Error(),
				"path", c.Request.URL.Path,
				"method", c.Request.Method,
			)

			// If no response has been written yet, send error response
			if !c.Writer.Written() {
				respondInternalError(c, err)
			}
		}
	}
}
