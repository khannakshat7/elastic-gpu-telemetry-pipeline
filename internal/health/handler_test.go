package health

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

func init() {
	gin.SetMode(gin.TestMode)
}

func TestNewHandler(t *testing.T) {
	handler := NewHandler()
	assert.NotNil(t, handler)
	assert.IsType(t, &Handler{}, handler)
}

func TestHandler_Liveness(t *testing.T) {
	router := gin.New()
	handler := NewHandler()
	router.GET("/health", handler.Liveness)

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "alive")
	assert.Contains(t, w.Body.String(), "status")
}

func TestHandler_Readiness(t *testing.T) {
	router := gin.New()
	handler := NewHandler()
	router.GET("/ready", handler.Readiness)

	req := httptest.NewRequest("GET", "/ready", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "ready")
	assert.Contains(t, w.Body.String(), "status")
}

func TestHandler_Liveness_MultipleCalls(t *testing.T) {
	router := gin.New()
	handler := NewHandler()
	router.GET("/health", handler.Liveness)

	for i := 0; i < 5; i++ {
		req := httptest.NewRequest("GET", "/health", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "alive")
	}
}

func TestHandler_Readiness_MultipleCalls(t *testing.T) {
	router := gin.New()
	handler := NewHandler()
	router.GET("/ready", handler.Readiness)

	for i := 0; i < 5; i++ {
		req := httptest.NewRequest("GET", "/ready", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "ready")
	}
}
