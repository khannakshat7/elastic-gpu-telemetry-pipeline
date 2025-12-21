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

func TestHTTPRepository_ListGPUs_Success(t *testing.T) {
	expectedGPUs := []*domain.GPU{
		{
			UUID:     "GPU-123",
			GPUID:    "0",
			Device:   "nvidia0",
			Model:    "NVIDIA H100 80GB HBM3",
			Hostname: "host-1",
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/api/v1/storage/gpus", r.URL.Path)
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(expectedGPUs)
	}))
	defer server.Close()

	repo := NewHTTPRepository(server.URL)
	gpus, err := repo.ListGPUs(context.Background())
	require.NoError(t, err)
	assert.Len(t, gpus, 1)
	assert.Equal(t, "GPU-123", gpus[0].UUID)
}

func TestHTTPRepository_GetGPU_Success(t *testing.T) {
	expectedGPU := &domain.GPU{
		UUID:     "GPU-123",
		GPUID:    "0",
		Device:   "nvidia0",
		Model:    "NVIDIA H100 80GB HBM3",
		Hostname: "host-1",
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/api/v1/storage/gpus/GPU-123", r.URL.Path)
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(expectedGPU)
	}))
	defer server.Close()

	repo := NewHTTPRepository(server.URL)
	gpu, err := repo.GetGPU(context.Background(), "GPU-123")
	require.NoError(t, err)
	assert.NotNil(t, gpu)
	assert.Equal(t, "GPU-123", gpu.UUID)
}

func TestHTTPRepository_GetGPU_NotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	repo := NewHTTPRepository(server.URL)
	gpu, err := repo.GetGPU(context.Background(), "GPU-999")
	require.NoError(t, err)
	assert.Nil(t, gpu)
}

func TestHTTPRepository_SaveGPU_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "/api/v1/storage/gpus", r.URL.Path)

		var gpu domain.GPU
		err := json.NewDecoder(r.Body).Decode(&gpu)
		require.NoError(t, err)
		assert.Equal(t, "GPU-123", gpu.UUID)

		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"status": "saved"})
	}))
	defer server.Close()

	repo := NewHTTPRepository(server.URL)
	gpu := &domain.GPU{
		UUID:     "GPU-123",
		GPUID:    "0",
		Device:   "nvidia0",
		Model:    "NVIDIA H100 80GB HBM3",
		Hostname: "host-1",
	}

	err := repo.SaveGPU(context.Background(), gpu)
	require.NoError(t, err)
}

func TestHTTPRepository_SaveTelemetry_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "/api/v1/storage/telemetry", r.URL.Path)

		var record domain.TelemetryRecord
		err := json.NewDecoder(r.Body).Decode(&record)
		require.NoError(t, err)
		assert.Equal(t, "GPU-123", record.GPUUUID)

		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"status": "saved"})
	}))
	defer server.Close()

	repo := NewHTTPRepository(server.URL)
	record := &domain.TelemetryRecord{
		GPUUUID:       "GPU-123",
		MetricName:    "DCGM_FI_DEV_GPU_UTIL",
		Value:         "100",
		IngestionTime: time.Now(),
	}

	err := repo.SaveTelemetry(context.Background(), record)
	require.NoError(t, err)
}

func TestHTTPRepository_GetTelemetryByGPU_Success(t *testing.T) {
	expectedRecords := []*domain.TelemetryRecord{
		{
			GPUUUID:       "GPU-123",
			MetricName:    "DCGM_FI_DEV_GPU_UTIL",
			Value:         "100",
			IngestionTime: time.Now(),
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/api/v1/storage/gpus/GPU-123/telemetry", r.URL.Path)
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(expectedRecords)
	}))
	defer server.Close()

	repo := NewHTTPRepository(server.URL)
	records, err := repo.GetTelemetryByGPU(context.Background(), "GPU-123", nil, nil)
	require.NoError(t, err)
	assert.Len(t, records, 1)
	assert.Equal(t, "GPU-123", records[0].GPUUUID)
}

func TestHTTPRepository_GetTelemetryByGPU_WithTimeRange(t *testing.T) {
	startTime := time.Now().Add(-1 * time.Hour)
	endTime := time.Now()

	expectedRecords := []*domain.TelemetryRecord{
		{
			GPUUUID:       "GPU-123",
			MetricName:    "DCGM_FI_DEV_GPU_UTIL",
			Value:         "100",
			IngestionTime: startTime.Add(30 * time.Minute),
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/api/v1/storage/gpus/GPU-123/telemetry", r.URL.Path)
		assert.NotEmpty(t, r.URL.Query().Get("start_time"))
		assert.NotEmpty(t, r.URL.Query().Get("end_time"))
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(expectedRecords)
	}))
	defer server.Close()

	repo := NewHTTPRepository(server.URL)
	records, err := repo.GetTelemetryByGPU(context.Background(), "GPU-123", &startTime, &endTime)
	require.NoError(t, err)
	assert.Len(t, records, 1)
}
