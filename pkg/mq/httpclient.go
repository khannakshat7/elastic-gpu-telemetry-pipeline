package mq

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/domain"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/utils"
)

// HTTPMessageQueue is an HTTP-based implementation of MessageQueue
// that connects to a remote queue service via HTTP REST API
type HTTPMessageQueue struct {
	baseURL    string
	httpClient *http.Client
	closed     bool
	mu         sync.RWMutex
}

// NewHTTPMessageQueue creates a new HTTP-based message queue client
func NewHTTPMessageQueue(baseURL string) *HTTPMessageQueue {
	return &HTTPMessageQueue{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		closed: false,
	}
}

// Publish publishes a message to the queue service via HTTP POST
func (q *HTTPMessageQueue) Publish(ctx context.Context, msg *domain.Message) error {
	q.mu.RLock()
	if q.closed {
		q.mu.RUnlock()
		return fmt.Errorf("queue client is closed")
	}
	baseURL := q.baseURL
	httpClient := q.httpClient
	q.mu.RUnlock()

	// Serialize message to JSON
	jsonData, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	// Create HTTP request
	url := baseURL + "/api/v1/messages"
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Send request
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("queue service returned error: status %d, body: %s", resp.StatusCode, string(body))
	}

	return nil
}

// Subscribe creates a subscription channel that polls the queue service via HTTP GET
// This is a polling-based subscription (not push-based)
func (q *HTTPMessageQueue) Subscribe(ctx context.Context) (<-chan *domain.Message, error) {
	q.mu.RLock()
	if q.closed {
		q.mu.RUnlock()
		return nil, fmt.Errorf("queue client is closed")
	}
	baseURL := q.baseURL
	httpClient := q.httpClient
	q.mu.RUnlock()

	// Create channel for messages
	msgChan := make(chan *domain.Message, 10)

	// Start polling goroutine
	go func() {
		defer close(msgChan)

		pollInterval := 100 * time.Millisecond // Poll every 100ms
		timeout := 5 * time.Second             // Request timeout

		for {
			// Check if context is cancelled
			select {
			case <-ctx.Done():
				return
			default:
			}

			// Create polling request
			url := fmt.Sprintf("%s/api/v1/messages?timeout=%s", baseURL, timeout)
			req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
			if err != nil {
				utils.Logger.Error("Failed to create subscribe request", "error", err)
				time.Sleep(pollInterval)
				continue
			}

			// Send request
			resp, err := httpClient.Do(req)
			if err != nil {
				// Log error but continue polling
				utils.Logger.Debug("Subscribe request failed, retrying", "error", err)
				time.Sleep(pollInterval)
				continue
			}

			// Check response status
			if resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode == http.StatusNoContent {
				// No messages available, continue polling
				resp.Body.Close()
				time.Sleep(pollInterval)
				continue
			}

			if resp.StatusCode != http.StatusOK {
				body, _ := io.ReadAll(resp.Body)
				utils.Logger.Error("Subscribe request returned error",
					"status", resp.StatusCode,
					"body", string(body))
				resp.Body.Close()
				time.Sleep(pollInterval)
				continue
			}

			// Parse message from response
			var msg domain.Message
			if err := json.NewDecoder(resp.Body).Decode(&msg); err != nil {
				utils.Logger.Error("Failed to decode message", "error", err)
				resp.Body.Close()
				time.Sleep(pollInterval)
				continue
			}
			resp.Body.Close()

			// Send message to channel (non-blocking)
			select {
			case msgChan <- &msg:
				// Message sent successfully
			case <-ctx.Done():
				return
			default:
				// Channel full, skip this message (or could block)
				utils.Logger.Warn("Message channel full, dropping message")
			}

			// Small delay before next poll
			time.Sleep(pollInterval)
		}
	}()

	return msgChan, nil
}

// Close closes the HTTP queue client
func (q *HTTPMessageQueue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	return nil
}

// IsClosed returns true if the queue client is closed
func (q *HTTPMessageQueue) IsClosed() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.closed
}
