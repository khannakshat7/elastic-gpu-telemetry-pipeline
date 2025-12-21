package mq

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/domain"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/utils"
)

// Server represents the message queue HTTP server
type Server struct {
	queue  *InMemoryMessageQueue
	server *http.Server
	mu     sync.RWMutex
}

// NewServer creates a new queue server instance
func NewServer(queue *InMemoryMessageQueue) *Server {
	return &Server{
		queue: queue,
	}
}

// Start starts the HTTP server for the queue service
// It starts the server in a goroutine and returns immediately.
// Use Stop() to gracefully shutdown the server.
func (s *Server) Start(port string) error {
	// Setup Gin router
	router := gin.Default()

	// Setup routes
	s.setupRoutes(router)

	// Create HTTP server
	s.mu.Lock()
	s.server = &http.Server{
		Addr:         ":" + port,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	s.mu.Unlock()

	// Start server in goroutine
	go func() {
		utils.Logger.Info("Queue service starting", "port", port)
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			utils.Logger.Error("Server error", "error", err)
		}
	}()

	// Give server a moment to start
	time.Sleep(100 * time.Millisecond)
	utils.Logger.Info("Queue service started successfully", "port", port)

	return nil
}

// Stop gracefully stops the HTTP server
func (s *Server) Stop() error {
	s.mu.RLock()
	server := s.server
	s.mu.RUnlock()

	if server == nil {
		return nil
	}

	utils.Logger.Info("Stopping queue service")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		return fmt.Errorf("error shutting down server: %w", err)
	}

	utils.Logger.Info("Queue service stopped")
	return nil
}

// setupRoutes configures HTTP routes for the queue service
func (s *Server) setupRoutes(router *gin.Engine) {
	api := router.Group("/api/v1")
	{
		// Health check
		api.GET("/health", s.healthCheck)

		// Queue operations
		api.POST("/messages", s.publishMessage)
		api.GET("/messages", s.subscribeMessages) // Polling-based subscription
		api.GET("/stats", s.getStats)
	}
}

// healthCheck handles GET /api/v1/health
func (s *Server) healthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":    "healthy",
		"queue":     "running",
		"closed":    s.queue.IsClosed(),
		"timestamp": time.Now(),
	})
}

// publishMessage handles POST /api/v1/messages
func (s *Server) publishMessage(c *gin.Context) {
	var msg domain.Message
	if err := c.ShouldBindJSON(&msg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("invalid request: %v", err)})
		return
	}

	ctx := c.Request.Context()
	if err := s.queue.Publish(ctx, &msg); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":     "published",
		"message_id": msg.ID,
	})
}

// subscribeMessages handles GET /api/v1/messages (polling-based)
func (s *Server) subscribeMessages(c *gin.Context) {
	ctx := c.Request.Context()

	// Subscribe to queue
	subChan, err := s.queue.Subscribe(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Get timeout from query parameter (default 5 seconds)
	timeout := 5 * time.Second
	if timeoutStr := c.Query("timeout"); timeoutStr != "" {
		if t, err := time.ParseDuration(timeoutStr); err == nil {
			timeout = t
		}
	}

	// Wait for a message with timeout
	select {
	case msg, ok := <-subChan:
		if !ok {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "queue is closed"})
			return
		}
		c.JSON(http.StatusOK, msg)
	case <-time.After(timeout):
		c.JSON(http.StatusRequestTimeout, gin.H{
			"status":  "timeout",
			"message": "no messages available",
		})
	case <-ctx.Done():
		c.JSON(http.StatusRequestTimeout, gin.H{"error": "request cancelled"})
	}
}

// getStats handles GET /api/v1/stats
func (s *Server) getStats(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"subscribers": s.queue.GetSubscriberCount(),
		"closed":      s.queue.IsClosed(),
		"timestamp":   time.Now(),
	})
}

// isTestEnvironment checks if we're running in a test environment
func isTestEnvironment() bool {
	return len(os.Args) > 0 && (strings.HasSuffix(os.Args[0], ".test") || strings.Contains(os.Args[0], "/_test/"))
}
