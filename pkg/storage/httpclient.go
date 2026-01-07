package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/domain"
)

// HTTPRepository is an HTTP-based implementation of Repository
// that connects to a remote storage service via HTTP REST API
type HTTPRepository struct {
	baseURL    string
	httpClient *http.Client
}

// NewHTTPRepository creates a new HTTP-based storage repository client
func NewHTTPRepository(baseURL string) *HTTPRepository {
	return &HTTPRepository{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// ListGPUs returns all GPUs that have telemetry data
func (r *HTTPRepository) ListGPUs(ctx context.Context) ([]*domain.GPU, error) {
	url := r.baseURL + "/api/v1/storage/gpus"
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("storage service returned error: status %d, body: %s", resp.StatusCode, string(body))
	}

	var gpus []*domain.GPU
	if err := json.NewDecoder(resp.Body).Decode(&gpus); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return gpus, nil
}

// GetGPU retrieves a GPU by its UUID
func (r *HTTPRepository) GetGPU(ctx context.Context, uuid string) (*domain.GPU, error) {
	escapedUUID := url.PathEscape(uuid)
	endpoint := fmt.Sprintf("%s/api/v1/storage/gpus/%s", r.baseURL, escapedUUID)
	req, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("storage service returned error: status %d, body: %s", resp.StatusCode, string(body))
	}

	var gpu domain.GPU
	if err := json.NewDecoder(resp.Body).Decode(&gpu); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &gpu, nil
}

// SaveGPU saves or updates a GPU entity
func (r *HTTPRepository) SaveGPU(ctx context.Context, gpu *domain.GPU) error {
	jsonData, err := json.Marshal(gpu)
	if err != nil {
		return fmt.Errorf("failed to marshal GPU: %w", err)
	}

	url := r.baseURL + "/api/v1/storage/gpus"
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("storage service returned error: status %d, body: %s", resp.StatusCode, string(body))
	}

	return nil
}

// SaveTelemetry saves a telemetry record
func (r *HTTPRepository) SaveTelemetry(ctx context.Context, record *domain.TelemetryRecord) error {
	jsonData, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal telemetry record: %w", err)
	}

	url := r.baseURL + "/api/v1/storage/telemetry"
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("storage service returned error: status %d, body: %s", resp.StatusCode, string(body))
	}

	return nil
}

// GetTelemetryByGPU retrieves telemetry records for a specific GPU
func (r *HTTPRepository) GetTelemetryByGPU(ctx context.Context, gpuUUID string, startTime, endTime *time.Time) ([]*domain.TelemetryRecord, error) {
	escapedUUID := url.PathEscape(gpuUUID)
	endpoint := fmt.Sprintf("%s/api/v1/storage/gpus/%s/telemetry", r.baseURL, escapedUUID)
	req, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Add query parameters
	q := req.URL.Query()
	if startTime != nil {
		q.Set("start_time", startTime.Format(time.RFC3339))
	}
	if endTime != nil {
		q.Set("end_time", endTime.Format(time.RFC3339))
	}
	req.URL.RawQuery = q.Encode()

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("storage service returned error: status %d, body: %s", resp.StatusCode, string(body))
	}

	var records []*domain.TelemetryRecord
	if err := json.NewDecoder(resp.Body).Decode(&records); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return records, nil
}
