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
		api.GET("/messages", s.subscribeMessages)   // Push-based subscription (SSE)
		api.POST("/messages/:id/ack", s.ackMessage) // ACK endpoint
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

// subscribeMessages handles GET /api/v1/messages (push-based using Server-Sent Events)
func (s *Server) subscribeMessages(c *gin.Context) {
	ctx := c.Request.Context()

	// Get consumer ID from query parameter (required)
	consumerID := c.Query("consumer_id")
	if consumerID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "consumer_id query parameter is required"})
		return
	}

	// Subscribe to queue
	subChan, err := s.queue.Subscribe(ctx, consumerID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Set up Server-Sent Events (SSE)
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no") // Disable nginx buffering

	// Send initial connection message
	c.SSEvent("connected", gin.H{
		"consumer_id": consumerID,
		"timestamp":   time.Now(),
	})
	c.Writer.Flush()

	// Stream messages as they arrive
	for {
		select {
		case msg, ok := <-subChan:
			if !ok {
				// Channel closed, send close event
				c.SSEvent("closed", gin.H{"message": "queue is closed"})
				c.Writer.Flush()
				return
			}
			// Send message as SSE event
			c.SSEvent("message", msg)
			c.Writer.Flush()

		case <-ctx.Done():
			// Client disconnected
			return
		}
	}
}

// ackMessage handles POST /api/v1/messages/:id/ack
func (s *Server) ackMessage(c *gin.Context) {
	messageID := c.Param("id")
	if messageID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "message ID is required"})
		return
	}

	// Get consumer ID from request body or query parameter
	var ackRequest struct {
		ConsumerID string `json:"consumer_id" form:"consumer_id"`
	}
	if err := c.ShouldBindJSON(&ackRequest); err != nil {
		// Try query parameter as fallback
		ackRequest.ConsumerID = c.Query("consumer_id")
	}

	if ackRequest.ConsumerID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "consumer_id is required"})
		return
	}

	ctx := c.Request.Context()
	if err := s.queue.Ack(ctx, messageID, ackRequest.ConsumerID); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":     "acknowledged",
		"message_id": messageID,
	})
}

// getStats handles GET /api/v1/stats
func (s *Server) getStats(c *gin.Context) {
	stats := gin.H{
		"subscribers":      s.queue.GetSubscriberCount(),
		"pending_messages": s.queue.GetPendingMessageCount(),
		"closed":           s.queue.IsClosed(),
		"timestamp":        time.Now(),
	}
	c.JSON(http.StatusOK, stats)
}

// isTestEnvironment checks if we're running in a test environment
func isTestEnvironment() bool {
	return len(os.Args) > 0 && (strings.HasSuffix(os.Args[0], ".test") || strings.Contains(os.Args[0], "/_test/"))
}
