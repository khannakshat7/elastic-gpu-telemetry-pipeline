package mq

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/domain"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/utils"
)

// HTTPMessageQueue is an HTTP-based implementation of MessageQueue
// that connects to a remote queue service via HTTP REST API.
// Uses Server-Sent Events (SSE) for push-based message delivery.
type HTTPMessageQueue struct {
	baseURL    string
	httpClient *http.Client
	closed     bool
	mu         sync.RWMutex
	consumerID string // Consumer ID for this client instance
}

// NewHTTPMessageQueue creates a new HTTP-based message queue client
func NewHTTPMessageQueue(baseURL string) *HTTPMessageQueue {
	// Generate a unique consumer ID for this client instance
	consumerID := fmt.Sprintf("consumer-%d", time.Now().UnixNano())
	return &HTTPMessageQueue{
		baseURL:    baseURL,
		consumerID: consumerID,
		httpClient: &http.Client{
			Timeout: 0, // No timeout for SSE connections
		},
		closed: false,
	}
}

// SetConsumerID sets the consumer ID for this client
func (q *HTTPMessageQueue) SetConsumerID(consumerID string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.consumerID = consumerID
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

// Subscribe creates a subscription channel that receives messages via Server-Sent Events (SSE).
// This is a push-based subscription where the queue service pushes messages to the client.
func (q *HTTPMessageQueue) Subscribe(ctx context.Context, consumerID string) (<-chan *domain.Message, error) {
	q.mu.RLock()
	if q.closed {
		q.mu.RUnlock()
		return nil, fmt.Errorf("queue client is closed")
	}
	baseURL := q.baseURL
	httpClient := q.httpClient
	if consumerID == "" {
		consumerID = q.consumerID
	}
	q.mu.RUnlock()

	// Create channel for messages
	msgChan := make(chan *domain.Message, 10)

	// Start SSE connection goroutine
	go func() {
		defer close(msgChan)

		for {
			// Check if context is cancelled
			select {
			case <-ctx.Done():
				return
			default:
			}

			// Create SSE request
			u, err := url.Parse(baseURL + "/api/v1/messages")
			if err != nil {
				utils.Logger.Error("Failed to parse URL", "error", err)
				time.Sleep(1 * time.Second)
				continue
			}

			// Add consumer_id query parameter
			q := u.Query()
			q.Set("consumer_id", consumerID)
			u.RawQuery = q.Encode()

			req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
			if err != nil {
				utils.Logger.Error("Failed to create SSE request", "error", err)
				time.Sleep(1 * time.Second)
				continue
			}
			req.Header.Set("Accept", "text/event-stream")
			req.Header.Set("Cache-Control", "no-cache")

			// Send request
			resp, err := httpClient.Do(req)
			if err != nil {
				utils.Logger.Debug("SSE connection failed, retrying", "error", err)
				time.Sleep(1 * time.Second)
				continue
			}

			if resp.StatusCode != http.StatusOK {
				body, _ := io.ReadAll(resp.Body)
				utils.Logger.Error("SSE request returned error",
					"status", resp.StatusCode,
					"body", string(body))
				resp.Body.Close()
				time.Sleep(1 * time.Second)
				continue
			}

			// Parse SSE stream - Gin's SSEvent sends: "event: <name>\ndata: <json>\n\n"
			// Read response body line by line
			reader := bufio.NewReader(resp.Body)
			var currentEvent string
			var currentData strings.Builder

			for {
				// Check context cancellation
				select {
				case <-ctx.Done():
					resp.Body.Close()
					return
				default:
				}

				// Read line (this will block until data arrives, which is what we want)
				line, err := reader.ReadString('\n')
				if err != nil {
					if err == io.EOF {
						// Handle any remaining data
						if currentData.Len() > 0 && currentEvent == "message" {
							jsonData := currentData.String()
							var msg domain.Message
							if err := json.Unmarshal([]byte(jsonData), &msg); err == nil {
								select {
								case msgChan <- &msg:
									utils.Logger.Debug("Message received via SSE (EOF)", "message_id", msg.ID)
								case <-ctx.Done():
									resp.Body.Close()
									return
								}
							}
						}
						// EOF means connection closed, reconnect
						resp.Body.Close()
						utils.Logger.Debug("SSE stream ended (EOF), reconnecting")
						goto reconnect
					}
					utils.Logger.Debug("SSE read error", "error", err)
					resp.Body.Close()
					goto reconnect
				}

				line = strings.TrimRight(line, "\r\n")

				// Handle empty lines (SSE events are separated by empty lines)
				// Process the event when we see an empty line (SSE event complete)
				if line == "" {
					// Process accumulated event if we have data and it's a message event
					if currentData.Len() > 0 && currentEvent == "message" {
						jsonData := currentData.String()
						var msg domain.Message
						if err := json.Unmarshal([]byte(jsonData), &msg); err != nil {
						} else {
							// Send message to channel
							select {
							case msgChan <- &msg:
							case <-ctx.Done():
								resp.Body.Close()
								return
							}
						}
					}
					// Reset for next event
					currentEvent = ""
					currentData.Reset()
					continue
				}

				// Parse event type (Gin sends "event:message" without space)
				if strings.HasPrefix(line, "event:") {
					currentEvent = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
					currentData.Reset() // Reset data when new event starts
					continue
				}

				// Parse data (Gin sends "data:{json}" without space)
				if strings.HasPrefix(line, "data:") {
					data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
					if currentData.Len() > 0 {
						currentData.WriteString("\n")
					}
					currentData.WriteString(data)
					continue
				}
			}

		reconnect:
			// Connection closed, reconnect
			utils.Logger.Debug("SSE connection closed, reconnecting")
			time.Sleep(1 * time.Second)
		}
	}()

	return msgChan, nil
}

// Ack acknowledges that a message has been successfully processed
func (q *HTTPMessageQueue) Ack(ctx context.Context, messageID string, consumerID string) error {
	q.mu.RLock()
	if q.closed {
		q.mu.RUnlock()
		return fmt.Errorf("queue client is closed")
	}
	baseURL := q.baseURL
	httpClient := q.httpClient
	if consumerID == "" {
		consumerID = q.consumerID
	}
	q.mu.RUnlock()

	// Create ACK request body
	ackRequest := map[string]string{
		"consumer_id": consumerID,
	}
	jsonData, err := json.Marshal(ackRequest)
	if err != nil {
		return fmt.Errorf("failed to marshal ACK request: %w", err)
	}

	// Create HTTP request
	url := fmt.Sprintf("%s/api/v1/messages/%s/ack", baseURL, messageID)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create ACK request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Send request
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send ACK request: %w", err)
	}
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("ACK request returned error: status %d, body: %s", resp.StatusCode, string(body))
	}

	return nil
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
