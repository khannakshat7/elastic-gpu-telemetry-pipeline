package mq

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func init() {
	gin.SetMode(gin.TestMode)
	utils.SetupLogger()
}

func TestServer_StartStop(t *testing.T) {
	queue := NewInMemoryMessageQueue(1000)
	server := NewServer(queue)

	// Start server
	err := server.Start("0") // Use port 0 for random port
	assert.NoError(t, err)

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	// Stop server
	err = server.Stop()
	assert.NoError(t, err)
}

func TestServer_Stop_WithoutStart(t *testing.T) {
	queue := NewInMemoryMessageQueue(1000)
	server := NewServer(queue)

	// Stop without starting should not panic
	err := server.Stop()
	// Should either succeed or return an error, but not panic
	_ = err
}

func TestServer_publishMessage_QueueError(t *testing.T) {
	queue := NewInMemoryMessageQueue(1000)
	queue.Close() // Close queue to cause error
	server := NewServer(queue)
	router := gin.New()
	server.setupRoutes(router)

	// Try to publish when queue is closed
	msg := `{"id":"test","payload":{"gpu_uuid":"GPU-123"},"timestamp":"2025-01-01T00:00:00Z"}`
	req := httptest.NewRequest("POST", "/api/v1/messages", bytes.NewBufferString(msg))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Should return error
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestServer_subscribeMessages_QueueError(t *testing.T) {
	queue := NewInMemoryMessageQueue(1000)
	queue.Close() // Close queue to cause error
	server := NewServer(queue)
	router := gin.New()
	server.setupRoutes(router)

	// Try to subscribe when queue is closed
	req := httptest.NewRequest("GET", "/api/v1/messages?timeout=100ms", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Should return error
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}
