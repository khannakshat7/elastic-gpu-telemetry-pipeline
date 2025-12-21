package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/domain"
	"github.com/stretchr/testify/assert"
)

func init() {
	gin.SetMode(gin.TestMode)
}

func TestParseTimeRange_NoParams(t *testing.T) {
	router := gin.New()
	router.GET("/test", func(c *gin.Context) {
		startTime, endTime, err := parseTimeRange(c)
		assert.NoError(t, err)
		assert.Nil(t, startTime)
		assert.Nil(t, endTime)
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestParseTimeRange_StartTimeOnly(t *testing.T) {
	router := gin.New()
	router.GET("/test", func(c *gin.Context) {
		startTime, endTime, err := parseTimeRange(c)
		assert.NoError(t, err)
		assert.NotNil(t, startTime)
		assert.Nil(t, endTime)
		assert.Equal(t, "2025-01-01T00:00:00Z", startTime.Format(time.RFC3339))
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	req := httptest.NewRequest("GET", "/test?start_time=2025-01-01T00:00:00Z", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestParseTimeRange_EndTimeOnly(t *testing.T) {
	router := gin.New()
	router.GET("/test", func(c *gin.Context) {
		startTime, endTime, err := parseTimeRange(c)
		assert.NoError(t, err)
		assert.Nil(t, startTime)
		assert.NotNil(t, endTime)
		assert.Equal(t, "2025-01-01T23:59:59Z", endTime.Format(time.RFC3339))
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	req := httptest.NewRequest("GET", "/test?end_time=2025-01-01T23:59:59Z", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestParseTimeRange_BothTimes(t *testing.T) {
	router := gin.New()
	router.GET("/test", func(c *gin.Context) {
		startTime, endTime, err := parseTimeRange(c)
		assert.NoError(t, err)
		assert.NotNil(t, startTime)
		assert.NotNil(t, endTime)
		assert.True(t, startTime.Before(*endTime))
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	req := httptest.NewRequest("GET", "/test?start_time=2025-01-01T00:00:00Z&end_time=2025-01-01T23:59:59Z", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestParseTimeRange_InvalidStartTimeFormat(t *testing.T) {
	router := gin.New()
	router.GET("/test", func(c *gin.Context) {
		startTime, endTime, err := parseTimeRange(c)
		assert.Error(t, err)
		assert.Nil(t, startTime)
		assert.Nil(t, endTime)
		assert.Contains(t, err.Error(), "start_time")
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	req := httptest.NewRequest("GET", "/test?start_time=invalid-format", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestParseTimeRange_InvalidEndTimeFormat(t *testing.T) {
	router := gin.New()
	router.GET("/test", func(c *gin.Context) {
		startTime, endTime, err := parseTimeRange(c)
		assert.Error(t, err)
		assert.Nil(t, startTime)
		assert.Nil(t, endTime)
		assert.Contains(t, err.Error(), "end_time")
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	req := httptest.NewRequest("GET", "/test?end_time=invalid-format", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestParseTimeRange_InvalidTimeRange(t *testing.T) {
	router := gin.New()
	router.GET("/test", func(c *gin.Context) {
		startTime, endTime, err := parseTimeRange(c)
		assert.Error(t, err)
		assert.ErrorIs(t, err, domain.ErrInvalidTimeRange)
		// When there's an error, both times should be nil
		assert.Nil(t, startTime)
		assert.Nil(t, endTime)
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	// Start time is after end time
	req := httptest.NewRequest("GET", "/test?start_time=2025-01-01T23:59:59Z&end_time=2025-01-01T00:00:00Z", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestParseTimeRange_EqualTimes(t *testing.T) {
	router := gin.New()
	router.GET("/test", func(c *gin.Context) {
		startTime, endTime, err := parseTimeRange(c)
		// Equal times should be valid (start <= end)
		assert.NoError(t, err)
		assert.NotNil(t, startTime)
		assert.NotNil(t, endTime)
		assert.True(t, startTime.Equal(*endTime))
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	req := httptest.NewRequest("GET", "/test?start_time=2025-01-01T12:00:00Z&end_time=2025-01-01T12:00:00Z", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestParseTimeRange_WithTimezone(t *testing.T) {
	router := gin.New()
	router.GET("/test", func(c *gin.Context) {
		startTime, endTime, err := parseTimeRange(c)
		assert.NoError(t, err)
		assert.NotNil(t, startTime)
		assert.NotNil(t, endTime)
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	// Use URL-encoded timezone format (+ becomes %2B)
	req := httptest.NewRequest("GET", "/test?start_time=2025-01-01T00:00:00%2B05:30&end_time=2025-01-01T23:59:59%2B05:30", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestRespondError(t *testing.T) {
	router := gin.New()
	router.GET("/test", func(c *gin.Context) {
		respondError(c, http.StatusBadRequest, "TEST_ERROR", "Test error message")
	})

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "Test error message")
	assert.Contains(t, w.Body.String(), "TEST_ERROR")
}

func TestRespondBadRequest(t *testing.T) {
	router := gin.New()
	router.GET("/test", func(c *gin.Context) {
		respondBadRequest(c, "BAD_REQUEST", "Invalid request")
	})

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "Invalid request")
}

func TestRespondNotFound(t *testing.T) {
	router := gin.New()
	router.GET("/test", func(c *gin.Context) {
		respondNotFound(c, "NOT_FOUND", "Resource not found")
	})

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.Contains(t, w.Body.String(), "Resource not found")
}

func TestRespondInternalError(t *testing.T) {
	router := gin.New()
	router.GET("/test", func(c *gin.Context) {
		respondInternalError(c, assert.AnError)
	})

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Contains(t, w.Body.String(), "Internal server error")
}
