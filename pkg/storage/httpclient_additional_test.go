package storage

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTPRepository_ListGPUs_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error": "server error"}`))
	}))
	defer server.Close()

	repo := NewHTTPRepository(server.URL)
	ctx := context.Background()

	gpus, err := repo.ListGPUs(ctx)
	assert.Error(t, err)
	assert.Nil(t, gpus)
}

func TestHTTPRepository_ListGPUs_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`invalid json`))
	}))
	defer server.Close()

	repo := NewHTTPRepository(server.URL)
	ctx := context.Background()

	gpus, err := repo.ListGPUs(ctx)
	assert.Error(t, err)
	assert.Nil(t, gpus)
}

func TestHTTPRepository_GetGPU_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	repo := NewHTTPRepository(server.URL)
	ctx := context.Background()

	gpu, err := repo.GetGPU(ctx, "GPU-123")
	assert.Error(t, err)
	assert.Nil(t, gpu)
}

func TestHTTPRepository_SaveGPU_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	repo := NewHTTPRepository(server.URL)
	ctx := context.Background()

	gpu := &domain.GPU{UUID: "GPU-123"}
	err := repo.SaveGPU(ctx, gpu)
	assert.Error(t, err)
}

func TestHTTPRepository_SaveTelemetry_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	repo := NewHTTPRepository(server.URL)
	ctx := context.Background()

	record := &domain.TelemetryRecord{
		GPUUUID:       "GPU-123",
		MetricName:    "DCGM_FI_DEV_GPU_UTIL",
		Value:         "100",
		IngestionTime: time.Now(),
	}
	err := repo.SaveTelemetry(ctx, record)
	assert.Error(t, err)
}

func TestHTTPRepository_GetTelemetryByGPU_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	repo := NewHTTPRepository(server.URL)
	ctx := context.Background()

	records, err := repo.GetTelemetryByGPU(ctx, "GPU-123", nil, nil)
	assert.Error(t, err)
	assert.Nil(t, records)
}

func TestHTTPRepository_GetTelemetryByGPU_WithTimeRange_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	repo := NewHTTPRepository(server.URL)
	ctx := context.Background()

	startTime := time.Now().Add(-1 * time.Hour)
	endTime := time.Now()
	records, err := repo.GetTelemetryByGPU(ctx, "GPU-123", &startTime, &endTime)
	assert.Error(t, err)
	assert.Nil(t, records)
}

func TestHTTPRepository_GetTelemetryByGPU_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`invalid json`))
	}))
	defer server.Close()

	repo := NewHTTPRepository(server.URL)
	ctx := context.Background()

	records, err := repo.GetTelemetryByGPU(ctx, "GPU-123", nil, nil)
	assert.Error(t, err)
	assert.Nil(t, records)
}

func TestHTTPRepository_SaveGPU_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Read request body to verify it was sent
		var gpu domain.GPU
		err := json.NewDecoder(r.Body).Decode(&gpu)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	repo := NewHTTPRepository(server.URL)
	ctx := context.Background()

	gpu := &domain.GPU{UUID: "GPU-123"}
	err := repo.SaveGPU(ctx, gpu)
	require.NoError(t, err)
}

func TestHTTPRepository_SaveTelemetry_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Read request body to verify it was sent
		var record domain.TelemetryRecord
		err := json.NewDecoder(r.Body).Decode(&record)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	repo := NewHTTPRepository(server.URL)
	ctx := context.Background()

	record := &domain.TelemetryRecord{
		GPUUUID:       "GPU-123",
		MetricName:    "DCGM_FI_DEV_GPU_UTIL",
		Value:         "100",
		IngestionTime: time.Now(),
	}
	err := repo.SaveTelemetry(ctx, record)
	require.NoError(t, err)
}
