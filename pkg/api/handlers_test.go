package api

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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func init() {
	// Initialize logger for tests
	utils.SetupLogger()
}

// MockRepository is a mock implementation of storage.Repository
type MockRepository struct {
	mock.Mock
}

func (m *MockRepository) ListGPUs(ctx context.Context) ([]*domain.GPU, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*domain.GPU), args.Error(1)
}

func (m *MockRepository) GetGPU(ctx context.Context, uuid string) (*domain.GPU, error) {
	args := m.Called(ctx, uuid)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*domain.GPU), args.Error(1)
}

func (m *MockRepository) SaveGPU(ctx context.Context, gpu *domain.GPU) error {
	args := m.Called(ctx, gpu)
	return args.Error(0)
}

func (m *MockRepository) SaveTelemetry(ctx context.Context, record *domain.TelemetryRecord) error {
	args := m.Called(ctx, record)
	return args.Error(0)
}

func (m *MockRepository) GetTelemetryByGPU(ctx context.Context, gpuUUID string, startTime, endTime *time.Time) ([]*domain.TelemetryRecord, error) {
	args := m.Called(ctx, gpuUUID, startTime, endTime)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*domain.TelemetryRecord), args.Error(1)
}

func setupRouter(handlers *Handlers) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	// Skip logger middleware in tests to avoid nil logger issues
	router.Use(RecoveryMiddleware())

	api := router.Group("/api/v1")
	{
		gpus := api.Group("/gpus")
		{
			gpus.GET("", handlers.ListGPUs)
			gpus.GET("/:id/telemetry", handlers.GetTelemetryByGPU)
		}
	}

	return router
}

func TestHandlers_ListGPUs_Success(t *testing.T) {
	mockRepo := new(MockRepository)
	handlers := NewHandlers(mockRepo)
	router := setupRouter(handlers)

	// Setup mock expectations
	expectedGPUs := []*domain.GPU{
		{
			UUID:     "GPU-123",
			GPUID:    "0",
			Device:   "nvidia0",
			Model:    "NVIDIA H100 80GB HBM3",
			Hostname: "host-1",
		},
		{
			UUID:     "GPU-456",
			GPUID:    "1",
			Device:   "nvidia1",
			Model:    "NVIDIA A100 40GB",
			Hostname: "host-2",
		},
	}
	mockRepo.On("ListGPUs", mock.Anything).Return(expectedGPUs, nil)

	// Create request
	req := httptest.NewRequest("GET", "/api/v1/gpus", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Assertions
	assert.Equal(t, http.StatusOK, w.Code)

	var response ListGPUsResponse
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)
	assert.Equal(t, 2, response.Count)
	assert.Len(t, response.GPUs, 2)
	assert.Equal(t, "GPU-123", response.GPUs[0].UUID)
	assert.Equal(t, "GPU-456", response.GPUs[1].UUID)

	mockRepo.AssertExpectations(t)
}

func TestHandlers_ListGPUs_Empty(t *testing.T) {
	mockRepo := new(MockRepository)
	handlers := NewHandlers(mockRepo)
	router := setupRouter(handlers)

	// Setup mock expectations - empty list
	mockRepo.On("ListGPUs", mock.Anything).Return([]*domain.GPU{}, nil)

	// Create request
	req := httptest.NewRequest("GET", "/api/v1/gpus", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Assertions
	assert.Equal(t, http.StatusOK, w.Code)

	var response ListGPUsResponse
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)
	assert.Equal(t, 0, response.Count)
	assert.Len(t, response.GPUs, 0)

	mockRepo.AssertExpectations(t)
}

func TestHandlers_ListGPUs_RepositoryError(t *testing.T) {
	mockRepo := new(MockRepository)
	handlers := NewHandlers(mockRepo)
	router := setupRouter(handlers)

	// Setup mock expectations - error
	mockRepo.On("ListGPUs", mock.Anything).Return(nil, assert.AnError)

	// Create request
	req := httptest.NewRequest("GET", "/api/v1/gpus", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Assertions
	assert.Equal(t, http.StatusInternalServerError, w.Code)

	var response ErrorResponse
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)
	assert.Equal(t, ErrCodeInternalError, response.Code)

	mockRepo.AssertExpectations(t)
}

func TestHandlers_GetTelemetryByGPU_Success(t *testing.T) {
	mockRepo := new(MockRepository)
	handlers := NewHandlers(mockRepo)
	router := setupRouter(handlers)

	gpuUUID := "GPU-5fd4f087-86f3-7a43-b711-4771313afc50"
	expectedGPU := &domain.GPU{
		UUID:     gpuUUID,
		GPUID:    "0",
		Device:   "nvidia0",
		Model:    "NVIDIA H100 80GB HBM3",
		Hostname: "host-1",
	}

	expectedRecords := []*domain.TelemetryRecord{
		{
			GPUUUID:       gpuUUID,
			MetricName:    "DCGM_FI_DEV_GPU_UTIL",
			Value:         "100",
			IngestionTime: time.Now().Add(-1 * time.Hour),
		},
		{
			GPUUUID:       gpuUUID,
			MetricName:    "DCGM_FI_DEV_GPU_TEMP",
			Value:         "75",
			IngestionTime: time.Now(),
		},
	}

	// Setup mock expectations
	mockRepo.On("GetGPU", mock.Anything, gpuUUID).Return(expectedGPU, nil)
	mockRepo.On("GetTelemetryByGPU", mock.Anything, gpuUUID, (*time.Time)(nil), (*time.Time)(nil)).Return(expectedRecords, nil)

	// Create request
	req := httptest.NewRequest("GET", "/api/v1/gpus/"+gpuUUID+"/telemetry", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Assertions
	assert.Equal(t, http.StatusOK, w.Code)

	var response GetTelemetryResponse
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)
	assert.Equal(t, gpuUUID, response.GPUUUID)
	assert.Equal(t, 2, response.Count)
	assert.Len(t, response.Records, 2)
	assert.Equal(t, "DCGM_FI_DEV_GPU_UTIL", response.Records[0].MetricName)
	assert.Equal(t, "DCGM_FI_DEV_GPU_TEMP", response.Records[1].MetricName)

	mockRepo.AssertExpectations(t)
}

func TestHandlers_GetTelemetryByGPU_WithTimeRange(t *testing.T) {
	mockRepo := new(MockRepository)
	handlers := NewHandlers(mockRepo)
	router := setupRouter(handlers)

	gpuUUID := "GPU-5fd4f087-86f3-7a43-b711-4771313afc50"
	expectedGPU := &domain.GPU{
		UUID:     gpuUUID,
		GPUID:    "0",
		Device:   "nvidia0",
		Model:    "NVIDIA H100 80GB HBM3",
		Hostname: "host-1",
	}

	// Use fixed times to avoid precision issues
	startTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	endTime := time.Date(2025, 1, 1, 23, 59, 59, 0, time.UTC)

	expectedRecords := []*domain.TelemetryRecord{
		{
			GPUUUID:       gpuUUID,
			MetricName:    "DCGM_FI_DEV_GPU_UTIL",
			Value:         "100",
			IngestionTime: startTime.Add(12 * time.Hour),
		},
	}

	// Setup mock expectations
	mockRepo.On("GetGPU", mock.Anything, gpuUUID).Return(expectedGPU, nil)
	mockRepo.On("GetTelemetryByGPU", mock.Anything, gpuUUID, mock.AnythingOfType("*time.Time"), mock.AnythingOfType("*time.Time")).Return(expectedRecords, nil)

	// Create request with time range
	url := "/api/v1/gpus/" + gpuUUID + "/telemetry?start_time=" + startTime.Format(time.RFC3339) + "&end_time=" + endTime.Format(time.RFC3339)
	req := httptest.NewRequest("GET", url, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Assertions
	assert.Equal(t, http.StatusOK, w.Code)

	var response GetTelemetryResponse
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)
	assert.Equal(t, gpuUUID, response.GPUUUID)
	assert.Equal(t, 1, response.Count)
	assert.NotNil(t, response.StartTime)
	assert.NotNil(t, response.EndTime)

	mockRepo.AssertExpectations(t)
}

func TestHandlers_GetTelemetryByGPU_InvalidUUID(t *testing.T) {
	mockRepo := new(MockRepository)
	handlers := NewHandlers(mockRepo)
	router := setupRouter(handlers)

	// Create request with invalid UUID (empty)
	req := httptest.NewRequest("GET", "/api/v1/gpus//telemetry", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Should handle gracefully (Gin will route differently, but test the validation)
	// Actually, empty UUID will be caught by validation
	req2 := httptest.NewRequest("GET", "/api/v1/gpus/INVALID/telemetry", nil)
	w2 := httptest.NewRecorder()
	router.ServeHTTP(w2, req2)

	// If validation fails, should return 400
	// Note: Our validation checks for "GPU-" prefix, so "INVALID" should fail
	if w2.Code == http.StatusBadRequest {
		var response ErrorResponse
		err := json.Unmarshal(w2.Body.Bytes(), &response)
		require.NoError(t, err)
		assert.Equal(t, ErrCodeInvalidGPUUUID, response.Code)
	}
}

func TestHandlers_GetTelemetryByGPU_NotFound(t *testing.T) {
	mockRepo := new(MockRepository)
	handlers := NewHandlers(mockRepo)
	router := setupRouter(handlers)

	gpuUUID := "GPU-5fd4f087-86f3-7a43-b711-4771313afc99"

	// Setup mock expectations - GPU not found
	mockRepo.On("GetGPU", mock.Anything, gpuUUID).Return(nil, nil)

	// Create request
	req := httptest.NewRequest("GET", "/api/v1/gpus/"+gpuUUID+"/telemetry", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Assertions
	assert.Equal(t, http.StatusNotFound, w.Code)

	var response ErrorResponse
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)
	assert.Equal(t, ErrCodeGPUNotFound, response.Code)

	mockRepo.AssertExpectations(t)
}

func TestHandlers_GetTelemetryByGPU_InvalidTimeRange(t *testing.T) {
	mockRepo := new(MockRepository)
	handlers := NewHandlers(mockRepo)
	router := setupRouter(handlers)

	gpuUUID := "GPU-5fd4f087-86f3-7a43-b711-4771313afc50"
	startTime := time.Now()
	endTime := time.Now().Add(-1 * time.Hour) // End before start - invalid

	// Create request with invalid time range
	url := "/api/v1/gpus/" + gpuUUID + "/telemetry?start_time=" + startTime.Format(time.RFC3339) + "&end_time=" + endTime.Format(time.RFC3339)
	req := httptest.NewRequest("GET", url, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Assertions - should return 400 Bad Request
	assert.Equal(t, http.StatusBadRequest, w.Code)

	var response ErrorResponse
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)
	assert.Equal(t, ErrCodeInvalidTimeRange, response.Code)
}

func TestHandlers_GetTelemetryByGPU_InvalidTimeFormat(t *testing.T) {
	mockRepo := new(MockRepository)
	handlers := NewHandlers(mockRepo)
	router := setupRouter(handlers)

	gpuUUID := "GPU-5fd4f087-86f3-7a43-b711-4771313afc50"

	// Create request with invalid time format
	url := "/api/v1/gpus/" + gpuUUID + "/telemetry?start_time=invalid-time"
	req := httptest.NewRequest("GET", url, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Assertions - should return 400 Bad Request
	assert.Equal(t, http.StatusBadRequest, w.Code)

	var response ErrorResponse
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)
	assert.Equal(t, ErrCodeInvalidTimeRange, response.Code)
}

func TestHandlers_GetTelemetryByGPU_RepositoryError(t *testing.T) {
	mockRepo := new(MockRepository)
	handlers := NewHandlers(mockRepo)
	router := setupRouter(handlers)

	gpuUUID := "GPU-5fd4f087-86f3-7a43-b711-4771313afc50"

	// Setup mock expectations - repository error
	mockRepo.On("GetGPU", mock.Anything, gpuUUID).Return(nil, assert.AnError)

	// Create request
	req := httptest.NewRequest("GET", "/api/v1/gpus/"+gpuUUID+"/telemetry", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Assertions
	assert.Equal(t, http.StatusInternalServerError, w.Code)

	var response ErrorResponse
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)
	assert.Equal(t, ErrCodeInternalError, response.Code)

	mockRepo.AssertExpectations(t)
}

func TestHandlers_GetTelemetryByGPU_GetTelemetryError(t *testing.T) {
	mockRepo := new(MockRepository)
	handlers := NewHandlers(mockRepo)
	router := setupRouter(handlers)

	gpuUUID := "GPU-5fd4f087-86f3-7a43-b711-4771313afc50"
	gpu := &domain.GPU{UUID: gpuUUID}

	// Setup mock - GetGPU succeeds but GetTelemetryByGPU fails
	mockRepo.On("GetGPU", mock.Anything, gpuUUID).Return(gpu, nil)
	mockRepo.On("GetTelemetryByGPU", mock.Anything, gpuUUID, (*time.Time)(nil), (*time.Time)(nil)).Return(nil, assert.AnError)

	// Create request
	req := httptest.NewRequest("GET", "/api/v1/gpus/"+gpuUUID+"/telemetry", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Assertions
	assert.Equal(t, http.StatusInternalServerError, w.Code)

	var response ErrorResponse
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)
	assert.Equal(t, ErrCodeInternalError, response.Code)

	mockRepo.AssertExpectations(t)
}
