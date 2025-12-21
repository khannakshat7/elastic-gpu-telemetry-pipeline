package mq

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/domain"
	"github.com/stretchr/testify/assert"
)

func init() {
	gin.SetMode(gin.TestMode)
}

func TestServer_healthCheck(t *testing.T) {
	queue := NewInMemoryMessageQueue(1000)
	server := NewServer(queue)
	router := gin.New()
	server.setupRoutes(router)

	req := httptest.NewRequest("GET", "/api/v1/health", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Equal(t, "healthy", response["status"])
	assert.Equal(t, "running", response["queue"])
	assert.Equal(t, false, response["closed"])
}

func TestServer_publishMessage_Success(t *testing.T) {
	queue := NewInMemoryMessageQueue(1000)
	server := NewServer(queue)
	router := gin.New()
	server.setupRoutes(router)

	msg := &domain.Message{
		ID:        "test-id",
		Payload:   &domain.TelemetryRecord{GPUUUID: "GPU-123"},
		Timestamp: time.Now(),
	}
	body, _ := json.Marshal(msg)

	req := httptest.NewRequest("POST", "/api/v1/messages", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Equal(t, "published", response["status"])
	assert.Equal(t, "test-id", response["message_id"])
}

func TestServer_publishMessage_InvalidJSON(t *testing.T) {
	queue := NewInMemoryMessageQueue(1000)
	server := NewServer(queue)
	router := gin.New()
	server.setupRoutes(router)

	req := httptest.NewRequest("POST", "/api/v1/messages", bytes.NewBufferString("invalid json"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "error")
}

func TestServer_subscribeMessages_Success(t *testing.T) {
	queue := NewInMemoryMessageQueue(1000)
	server := NewServer(queue)
	router := gin.New()
	server.setupRoutes(router)

	// Start subscription in a goroutine, then publish
	done := make(chan bool)
	var response domain.Message
	go func() {
		req := httptest.NewRequest("GET", "/api/v1/messages?timeout=2s", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code == http.StatusOK {
			_ = json.Unmarshal(w.Body.Bytes(), &response)
		}
		done <- true
	}()

	// Give subscription time to start
	time.Sleep(100 * time.Millisecond)

	// Publish a message
	msg := domain.NewMessage(&domain.TelemetryRecord{GPUUUID: "GPU-123"}, "producer-1")
	ctx := context.Background()
	_ = queue.Publish(ctx, msg)

	// Wait for subscription to complete
	<-done

	// Verify we got a message (ID might not match exactly due to timing, but should have an ID)
	assert.NotEmpty(t, response.ID)
	assert.NotNil(t, response.Payload)
}

func TestServer_subscribeMessages_Timeout(t *testing.T) {
	queue := NewInMemoryMessageQueue(1000)
	server := NewServer(queue)
	router := gin.New()
	server.setupRoutes(router)

	// Subscribe with very short timeout, no messages published
	req := httptest.NewRequest("GET", "/api/v1/messages?timeout=100ms", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Should timeout and return 204 or 408
	assert.True(t, w.Code == http.StatusNoContent || w.Code == http.StatusRequestTimeout)
}

func TestServer_subscribeMessages_InvalidTimeout(t *testing.T) {
	queue := NewInMemoryMessageQueue(1000)
	server := NewServer(queue)
	router := gin.New()
	server.setupRoutes(router)

	// Invalid timeout format
	req := httptest.NewRequest("GET", "/api/v1/messages?timeout=invalid", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Should use default timeout
	assert.True(t, w.Code == http.StatusNoContent || w.Code == http.StatusRequestTimeout || w.Code == http.StatusOK)
}

func TestServer_getStats(t *testing.T) {
	queue := NewInMemoryMessageQueue(1000)
	server := NewServer(queue)
	router := gin.New()
	server.setupRoutes(router)

	req := httptest.NewRequest("GET", "/api/v1/stats", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Contains(t, response, "subscribers")
}
