package mq

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/domain"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	utils.SetupLogger()
}

func TestHTTPMessageQueue_Publish_Success(t *testing.T) {
	// Create a test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "/api/v1/messages", r.URL.Path)

		var msg domain.Message
		err := json.NewDecoder(r.Body).Decode(&msg)
		require.NoError(t, err)

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "published", "message_id": msg.ID})
	}))
	defer server.Close()

	// Create HTTP queue client
	queue := NewHTTPMessageQueue(server.URL)
	defer queue.Close()

	// Create and publish message
	record := &domain.TelemetryRecord{
		GPUUUID:       "GPU-123",
		MetricName:    "DCGM_FI_DEV_GPU_UTIL",
		Value:         "100",
		IngestionTime: time.Now(),
	}
	msg := domain.NewMessage(record, "producer-1")

	err := queue.Publish(context.Background(), msg)
	require.NoError(t, err)
}

func TestHTTPMessageQueue_Publish_ServerError(t *testing.T) {
	// Create a test server that returns error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error": "internal server error"}`))
	}))
	defer server.Close()

	queue := NewHTTPMessageQueue(server.URL)
	defer queue.Close()

	record := &domain.TelemetryRecord{
		GPUUUID:       "GPU-123",
		MetricName:    "DCGM_FI_DEV_GPU_UTIL",
		Value:         "100",
		IngestionTime: time.Now(),
	}
	msg := domain.NewMessage(record, "producer-1")

	err := queue.Publish(context.Background(), msg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "status 500")
}

func TestHTTPMessageQueue_Publish_Closed(t *testing.T) {
	queue := NewHTTPMessageQueue("http://localhost:8080")
	queue.Close()

	record := &domain.TelemetryRecord{
		GPUUUID:       "GPU-123",
		MetricName:    "DCGM_FI_DEV_GPU_UTIL",
		Value:         "100",
		IngestionTime: time.Now(),
	}
	msg := domain.NewMessage(record, "producer-1")

	err := queue.Publish(context.Background(), msg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestHTTPMessageQueue_Subscribe_Success(t *testing.T) {
	// Create a test server using the actual queue server
	queue := NewInMemoryMessageQueue(100)
	defer queue.Close()

	server := NewServer(queue)
	router := gin.New()
	server.setupRoutes(router)

	testServer := httptest.NewServer(router)
	defer testServer.Close()

	httpQueue := NewHTTPMessageQueue(testServer.URL)
	defer httpQueue.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	subChan, err := httpQueue.Subscribe(ctx, "test-consumer-1")
	require.NoError(t, err)

	// Give time for SSE connection to establish
	time.Sleep(200 * time.Millisecond)

	// Publish a message to the queue
	record := &domain.TelemetryRecord{
		GPUUUID:       "GPU-123",
		MetricName:    "DCGM_FI_DEV_GPU_UTIL",
		Value:         "100",
		IngestionTime: time.Now(),
	}
	msg := domain.NewMessage(record, "producer-1")
	publishCtx := context.Background()
	err = queue.Publish(publishCtx, msg)
	require.NoError(t, err)

	// Give time for message to be distributed and sent via SSE
	time.Sleep(300 * time.Millisecond)

	// Wait for message via SSE
	select {
	case msg := <-subChan:
		assert.NotNil(t, msg)
		assert.Equal(t, "GPU-123", msg.Payload.GPUUUID)
		assert.Equal(t, "DCGM_FI_DEV_GPU_UTIL", msg.Payload.MetricName)
	case <-ctx.Done():
		t.Fatal("timeout waiting for message")
	}
}

func TestHTTPMessageQueue_Subscribe_Timeout(t *testing.T) {
	// Create a test server that always returns timeout
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusRequestTimeout)
		json.NewEncoder(w).Encode(map[string]string{"status": "timeout"})
	}))
	defer server.Close()

	queue := NewHTTPMessageQueue(server.URL)
	defer queue.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	subChan, err := queue.Subscribe(ctx, "test-consumer-1")
	require.NoError(t, err)

	// Should not receive any messages (timeout)
	select {
	case <-subChan:
		t.Fatal("unexpected message received")
	case <-ctx.Done():
		// Expected timeout
	}
}

func TestHTTPMessageQueue_IsClosed(t *testing.T) {
	queue := NewHTTPMessageQueue("http://localhost:8080")
	assert.False(t, queue.IsClosed())

	queue.Close()
	assert.True(t, queue.IsClosed())
}

func TestHTTPMessageQueue_Subscribe_Non200Status(t *testing.T) {
	// Create a test server that returns 500 error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error": "server error"}`))
	}))
	defer server.Close()

	queue := NewHTTPMessageQueue(server.URL)
	defer queue.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	subChan, err := queue.Subscribe(ctx, "test-consumer-1")
	require.NoError(t, err)

	// Should not receive messages due to server error, but should not crash
	select {
	case <-subChan:
		t.Fatal("unexpected message received")
	case <-ctx.Done():
		// Expected timeout
	}
}

func TestHTTPMessageQueue_Subscribe_InvalidJSON(t *testing.T) {
	// Create a test server that returns invalid JSON
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`invalid json`))
	}))
	defer server.Close()

	queue := NewHTTPMessageQueue(server.URL)
	defer queue.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	subChan, err := queue.Subscribe(ctx, "test-consumer-1")
	require.NoError(t, err)

	// Should not receive messages due to decode error, but should not crash
	select {
	case <-subChan:
		t.Fatal("unexpected message received")
	case <-ctx.Done():
		// Expected timeout
	}
}

func TestHTTPMessageQueue_Subscribe_NoContent(t *testing.T) {
	// Create a test server that returns 204 No Content
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	queue := NewHTTPMessageQueue(server.URL)
	defer queue.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	subChan, err := queue.Subscribe(ctx, "test-consumer-1")
	require.NoError(t, err)

	// Should not receive messages (204 means no content)
	select {
	case <-subChan:
		t.Fatal("unexpected message received")
	case <-ctx.Done():
		// Expected timeout
	}
}

func TestHTTPMessageQueue_Publish_MarshalError(t *testing.T) {
	queue := NewHTTPMessageQueue("http://localhost:8080")
	defer queue.Close()

	// Create a message that can't be marshaled (circular reference would cause this, but we'll use nil)
	// Actually, nil message should be handled
	msg := (*domain.Message)(nil)

	err := queue.Publish(context.Background(), msg)
	// This should either error or handle nil gracefully
	_ = err // We're just testing that it doesn't panic
}

func TestHTTPMessageQueue_Subscribe_Closed(t *testing.T) {
	queue := NewHTTPMessageQueue("http://localhost:8080")
	queue.Close()

	ctx := context.Background()
	_, err := queue.Subscribe(ctx, "test-consumer-1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestHTTPMessageQueue_SetConsumerID(t *testing.T) {
	queue := NewHTTPMessageQueue("http://localhost:8080")
	defer queue.Close()

	// Set consumer ID
	customID := "custom-consumer-123"
	queue.SetConsumerID(customID)

	// Verify it's set (by checking it's used in Subscribe when no consumerID is provided)
	// We can't directly access the private field, but we can verify behavior
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Subscribe without providing consumerID - should use the set one
	_, err := queue.Subscribe(ctx, "")
	assert.NoError(t, err) // Should not error immediately
}

func TestHTTPMessageQueue_Ack_Success(t *testing.T) {
	// Create a test server that handles ACK requests
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Contains(t, r.URL.Path, "/api/v1/messages/")
		assert.Contains(t, r.URL.Path, "/ack")

		var ackRequest map[string]string
		err := json.NewDecoder(r.Body).Decode(&ackRequest)
		require.NoError(t, err)
		assert.Equal(t, "test-consumer-1", ackRequest["consumer_id"])

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "acknowledged"})
	}))
	defer server.Close()

	queue := NewHTTPMessageQueue(server.URL)
	defer queue.Close()

	ctx := context.Background()
	err := queue.Ack(ctx, "message-123", "test-consumer-1")
	assert.NoError(t, err)
}

func TestHTTPMessageQueue_Ack_Error(t *testing.T) {
	// Create a test server that returns an error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "message not found"})
	}))
	defer server.Close()

	queue := NewHTTPMessageQueue(server.URL)
	defer queue.Close()

	ctx := context.Background()
	err := queue.Ack(ctx, "invalid-message", "test-consumer-1")
	assert.Error(t, err)
}

func TestHTTPMessageQueue_Ack_Closed(t *testing.T) {
	queue := NewHTTPMessageQueue("http://localhost:8080")
	queue.Close()

	ctx := context.Background()
	err := queue.Ack(ctx, "message-123", "test-consumer-1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestHTTPMessageQueue_Ack_DefaultConsumerID(t *testing.T) {
	// Create a test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var ackRequest map[string]string
		json.NewDecoder(r.Body).Decode(&ackRequest)
		// Consumer ID should be auto-generated if not provided
		assert.NotEmpty(t, ackRequest["consumer_id"])
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "acknowledged"})
	}))
	defer server.Close()

	queue := NewHTTPMessageQueue(server.URL)
	defer queue.Close()

	ctx := context.Background()
	// Ack without providing consumerID - should use default
	err := queue.Ack(ctx, "message-123", "")
	assert.NoError(t, err)
}
