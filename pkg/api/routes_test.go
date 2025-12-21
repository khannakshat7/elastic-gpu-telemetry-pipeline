package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/domain"
)

func init() {
	gin.SetMode(gin.TestMode)
}

// MockRepositoryForRoutes is a mock implementation of storage.Repository for route testing
type MockRepositoryForRoutes struct {
	mock.Mock
}

func (m *MockRepositoryForRoutes) SaveGPU(ctx context.Context, gpu *domain.GPU) error {
	args := m.Called(ctx, gpu)
	return args.Error(0)
}

func (m *MockRepositoryForRoutes) GetGPU(ctx context.Context, gpuID string) (*domain.GPU, error) {
	args := m.Called(ctx, gpuID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*domain.GPU), args.Error(1)
}

func (m *MockRepositoryForRoutes) ListGPUs(ctx context.Context) ([]*domain.GPU, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*domain.GPU), args.Error(1)
}

func (m *MockRepositoryForRoutes) SaveTelemetry(ctx context.Context, record *domain.TelemetryRecord) error {
	args := m.Called(ctx, record)
	return args.Error(0)
}

func (m *MockRepositoryForRoutes) GetTelemetryByGPU(ctx context.Context, gpuID string, startTime, endTime *time.Time) ([]*domain.TelemetryRecord, error) {
	args := m.Called(ctx, gpuID, startTime, endTime)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*domain.TelemetryRecord), args.Error(1)
}

func TestSetupRoutes_ListGPUs(t *testing.T) {
	router := gin.New()
	mockRepo := new(MockRepositoryForRoutes)
	handlers := NewHandlers(mockRepo)

	SetupRoutes(router, handlers)

	mockRepo.On("ListGPUs", mock.Anything).Return([]*domain.GPU{}, nil)

	req := httptest.NewRequest("GET", "/api/v1/gpus", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	mockRepo.AssertExpectations(t)
}

func TestSetupRoutes_GetTelemetryByGPU(t *testing.T) {
	router := gin.New()
	mockRepo := new(MockRepositoryForRoutes)
	handlers := NewHandlers(mockRepo)

	SetupRoutes(router, handlers)

	gpuID := "GPU-5fd4f087-86f3-7a43-b711-4771313afc50"
	gpu := &domain.GPU{UUID: gpuID}
	mockRepo.On("GetGPU", mock.Anything, gpuID).Return(gpu, nil)
	mockRepo.On("GetTelemetryByGPU", mock.Anything, gpuID, (*time.Time)(nil), (*time.Time)(nil)).Return([]*domain.TelemetryRecord{}, nil)

	req := httptest.NewRequest("GET", "/api/v1/gpus/"+gpuID+"/telemetry", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	mockRepo.AssertExpectations(t)
}

func TestSetupRoutes_GetTelemetryByGPU_WithTimeRange(t *testing.T) {
	router := gin.New()
	mockRepo := new(MockRepositoryForRoutes)
	handlers := NewHandlers(mockRepo)

	SetupRoutes(router, handlers)

	gpuID := "GPU-5fd4f087-86f3-7a43-b711-4771313afc50"
	gpu := &domain.GPU{UUID: gpuID}
	mockRepo.On("GetGPU", mock.Anything, gpuID).Return(gpu, nil)
	mockRepo.On("GetTelemetryByGPU", mock.Anything, gpuID, mock.AnythingOfType("*time.Time"), mock.AnythingOfType("*time.Time")).Return([]*domain.TelemetryRecord{}, nil)

	req := httptest.NewRequest("GET", "/api/v1/gpus/"+gpuID+"/telemetry?start_time=2025-01-01T00:00:00Z&end_time=2025-01-01T23:59:59Z", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	mockRepo.AssertExpectations(t)
}

func TestSetupRoutes_InvalidRoute(t *testing.T) {
	router := gin.New()
	mockRepo := new(MockRepositoryForRoutes)
	handlers := NewHandlers(mockRepo)

	SetupRoutes(router, handlers)

	req := httptest.NewRequest("GET", "/api/v1/invalid", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestSetupRoutes_InvalidMethod(t *testing.T) {
	router := gin.New()
	mockRepo := new(MockRepositoryForRoutes)
	handlers := NewHandlers(mockRepo)

	SetupRoutes(router, handlers)

	// Try POST instead of GET - Gin returns 404 for unregistered routes
	req := httptest.NewRequest("POST", "/api/v1/gpus", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Gin returns 404 for routes that don't exist, not 405
	assert.Equal(t, http.StatusNotFound, w.Code)
}
