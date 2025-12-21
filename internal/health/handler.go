package health

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// Handler handles health check endpoints
type Handler struct{}

// NewHandler creates a new health check handler
func NewHandler() *Handler {
	return &Handler{}
}

// Liveness handles /health endpoint for liveness probe
func (h *Handler) Liveness(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "alive"})
}

// Readiness handles /ready endpoint for readiness probe
func (h *Handler) Readiness(c *gin.Context) {
	// TODO: Check dependencies (queue, storage, etc.)
	c.JSON(http.StatusOK, gin.H{"status": "ready"})
}

