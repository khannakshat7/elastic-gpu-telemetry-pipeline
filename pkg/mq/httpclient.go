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
			Transport: &http.Transport{
				IdleConnTimeout:     90 * time.Second,
				TLSHandshakeTimeout: 10 * time.Second,
			},
			// Per-request timeouts are enforced via context
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

			if err := q.connectAndStream(ctx, baseURL, consumerID, msgChan, httpClient); err != nil {
				utils.Logger.Debug("SSE connection error, reconnecting", "error", err)
				time.Sleep(1 * time.Second)
			}
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
	if messageID == "" {
		q.mu.RUnlock()
		return fmt.Errorf("messageID is required")
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
	escapedID := url.PathEscape(messageID)
	url := fmt.Sprintf("%s/api/v1/messages/%s/ack", baseURL, escapedID)
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

// connectAndStream establishes SSE connection and streams messages until it ends.
func (q *HTTPMessageQueue) connectAndStream(ctx context.Context, baseURL, consumerID string, msgChan chan<- *domain.Message, httpClient *http.Client) error {
	u, err := url.Parse(baseURL + "/api/v1/messages")
	if err != nil {
		return fmt.Errorf("failed to parse URL: %w", err)
	}

	params := u.Query()
	params.Set("consumer_id", consumerID)
	u.RawQuery = params.Encode()

	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return fmt.Errorf("failed to create SSE request: %w", err)
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("SSE connection failed: %w", err)
	}
	defer resp.Body.Close() // Ensure body is always closed

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("SSE request error: status %d, body: %s", resp.StatusCode, string(body))
	}

	reader := bufio.NewReader(resp.Body)
	var currentEvent string
	var currentData strings.Builder

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				if currentData.Len() > 0 && currentEvent == "message" {
					if msg := q.decodeMessage(currentData.String()); msg != nil {
						select {
						case msgChan <- msg:
						case <-ctx.Done():
							return ctx.Err()
						}
					}
				}
				return fmt.Errorf("SSE stream ended (EOF)")
			}
			return fmt.Errorf("SSE read error: %w", err)
		}

		line = strings.TrimRight(line, "\r\n")

		if line == "" {
			if currentData.Len() > 0 && currentEvent == "message" {
				if msg := q.decodeMessage(currentData.String()); msg != nil {
					select {
					case msgChan <- msg:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			}
			currentEvent = ""
			currentData.Reset()
			continue
		}

		if strings.HasPrefix(line, "event:") {
			currentEvent = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			currentData.Reset()
			continue
		}

		if strings.HasPrefix(line, "data:") {
			data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			if currentData.Len() > 0 {
				currentData.WriteString("\n")
			}
			currentData.WriteString(data)
			continue
		}
	}
}

func (q *HTTPMessageQueue) decodeMessage(data string) *domain.Message {
	var msg domain.Message
	if err := json.Unmarshal([]byte(data), &msg); err != nil {
		return nil
	}
	return &msg
}

// IsClosed returns true if the queue client is closed
func (q *HTTPMessageQueue) IsClosed() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.closed
}
