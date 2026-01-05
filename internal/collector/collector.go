package collector

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/config"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/domain"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/mq"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/storage"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/utils"
)

// Collector consumes telemetry messages from the queue and persists them to storage
type Collector struct {
	config     *config.CollectorConfig
	queue      mq.MessageQueue
	repository storage.Repository
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	shutdownCh chan struct{}
	startedCh  chan struct{} // Signals when Start() has finished initializing

	// Statistics
	mu                sync.RWMutex
	processedCount    int64
	errorCount        int64
	lastProcessedTime time.Time
}

// NewCollector creates a new collector instance
func NewCollector(cfg *config.CollectorConfig, queue mq.MessageQueue, repository storage.Repository) (*Collector, error) {
	if queue == nil {
		return nil, fmt.Errorf("queue cannot be nil")
	}
	if repository == nil {
		return nil, fmt.Errorf("repository cannot be nil")
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Collector{
		config:     cfg,
		queue:      queue,
		repository: repository,
		ctx:        ctx,
		cancel:     cancel,
		shutdownCh: make(chan struct{}),
		startedCh:  make(chan struct{}),
	}, nil
}

// Start begins consuming messages from the queue and persisting them
func (c *Collector) Start() error {
	// Setup signal handling for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	if !isTestEnvironment() {
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	}

	// Subscribe to queue with consumer ID
	consumerID := c.config.InstanceID
	subChan, err := c.queue.Subscribe(c.ctx, consumerID)
	if err != nil {
		return fmt.Errorf("failed to subscribe to queue: %w", err)
	}

	utils.Logger.Info("Collector started",
		"instance_id", c.config.InstanceID,
		"batch_size", c.config.BatchSize)

	// Start message processing goroutine
	c.wg.Add(1)
	go c.processMessages(subChan)

	// Signal that Start() has finished initializing
	close(c.startedCh)

	// Wait for shutdown signal
	select {
	case sig := <-sigCh:
		utils.Logger.Info("Received shutdown signal", "signal", sig, "instance_id", c.config.InstanceID)
	case <-c.shutdownCh:
		utils.Logger.Info("Shutdown requested", "instance_id", c.config.InstanceID)
	case <-c.ctx.Done():
		utils.Logger.Info("Context cancelled", "instance_id", c.config.InstanceID)
	}

	// Graceful shutdown
	c.Stop()
	return nil
}

// processMessages processes messages from the subscription channel
func (c *Collector) processMessages(subChan <-chan *domain.Message) {
	defer c.wg.Done()

	var batch []*domain.Message
	ticker := time.NewTicker(100 * time.Millisecond) // Flush batch every 100ms if not full
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			// Process any remaining batch before shutdown
			if len(batch) > 0 {
				c.processBatch(batch)
			}
			return

		case msg, ok := <-subChan:
			if !ok {
				// Channel closed, process remaining batch and exit
				if len(batch) > 0 {
					c.processBatch(batch)
				}
				utils.Logger.Info("Message channel closed", "instance_id", c.config.InstanceID)
				return
			}

			// Add message to batch
			batch = append(batch, msg)

			// Process batch if it reaches the configured size
			if len(batch) >= c.config.BatchSize {
				c.processBatch(batch)
				batch = batch[:0] // Clear batch
			}

		case <-ticker.C:
			// Flush batch periodically if it has messages
			if len(batch) > 0 {
				c.processBatch(batch)
				batch = batch[:0] // Clear batch
			}
		}
	}
}

// processBatch processes a batch of messages and sends ACKs
func (c *Collector) processBatch(batch []*domain.Message) {
	if len(batch) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Track GPUs to save (deduplicate by UUID)
	gpuMap := make(map[string]*domain.GPU)
	consumerID := c.config.InstanceID

	// Process messages and track which ones succeeded
	processedMessages := make([]*domain.Message, 0, len(batch))
	for _, msg := range batch {
		if err := c.processMessage(ctx, msg, gpuMap); err != nil {
			c.mu.Lock()
			c.errorCount++
			c.mu.Unlock()

			utils.Logger.Error("Failed to process message",
				"error", err,
				"instance_id", c.config.InstanceID,
				"message_id", msg.ID)
			// Don't ACK failed messages - they should be retried
		} else {
			c.mu.Lock()
			c.processedCount++
			c.lastProcessedTime = time.Now()
			c.mu.Unlock()
			processedMessages = append(processedMessages, msg)
		}
	}

	// Save all unique GPUs
	for _, gpu := range gpuMap {
		if err := c.repository.SaveGPU(ctx, gpu); err != nil {
			utils.Logger.Error("Failed to save GPU",
				"error", err,
				"instance_id", c.config.InstanceID,
				"gpu_uuid", gpu.UUID)
		}
	}

	// Send ACKs for successfully processed messages
	for _, msg := range processedMessages {
		if err := c.queue.Ack(ctx, msg.ID, consumerID); err != nil {
			utils.Logger.Error("Failed to ACK message",
				"error", err,
				"instance_id", c.config.InstanceID,
				"message_id", msg.ID)
			// ACK failure doesn't affect processing count, but should be logged
		}
	}

	utils.Logger.Debug("Processed batch",
		"instance_id", c.config.InstanceID,
		"batch_size", len(batch),
		"processed", len(processedMessages),
		"gpus_saved", len(gpuMap))
}

// processMessage processes a single message
func (c *Collector) processMessage(ctx context.Context, msg *domain.Message, gpuMap map[string]*domain.GPU) error {
	// Validate message
	if msg == nil {
		return fmt.Errorf("message is nil")
	}

	if msg.Payload == nil {
		return fmt.Errorf("message payload is nil")
	}

	record := msg.Payload

	// Validate telemetry record
	if err := c.validateTelemetryRecord(record); err != nil {
		return fmt.Errorf("invalid telemetry record: %w", err)
	}

	// Extract GPU information from telemetry record
	if record.GPUUUID != "" {
		gpu := &domain.GPU{
			UUID:      record.GPUUUID,
			GPUID:     record.GPUID,
			Device:    record.Device,
			Model:     record.ModelName,
			Hostname:  record.Hostname,
			Container: record.Container,
			Pod:       record.Pod,
			Namespace: record.Namespace,
		}
		// Only add if we don't already have this GPU (or update if we have more info)
		if existing, exists := gpuMap[gpu.UUID]; !exists || (gpu.Model != "" && existing.Model == "") {
			gpuMap[gpu.UUID] = gpu
		}
	}

	// Save telemetry record
	if err := c.repository.SaveTelemetry(ctx, record); err != nil {
		return fmt.Errorf("failed to save telemetry: %w", err)
	}

	return nil
}

// validateTelemetryRecord validates a telemetry record
func (c *Collector) validateTelemetryRecord(record *domain.TelemetryRecord) error {
	if record.GPUUUID == "" {
		return fmt.Errorf("GPU UUID is empty")
	}

	if record.MetricName == "" {
		return fmt.Errorf("metric name is empty")
	}

	if record.IngestionTime.IsZero() {
		return fmt.Errorf("ingestion time is zero")
	}

	return nil
}

// Stop gracefully stops the collector
// Safe to call multiple times (idempotent)
func (c *Collector) Stop() {
	// Wait for Start() to finish initializing before stopping
	select {
	case <-c.startedCh:
		// Start() has finished, safe to stop
	case <-time.After(1 * time.Second):
		// Timeout - Start() might not have been called, proceed anyway
	}

	utils.Logger.Info("Stopping collector", "instance_id", c.config.InstanceID)
	c.cancel()
	c.wg.Wait()

	// Read stats with proper locking
	c.mu.RLock()
	processed := c.processedCount
	errors := c.errorCount
	c.mu.RUnlock()

	utils.Logger.Info("Collector stopped",
		"instance_id", c.config.InstanceID,
		"processed", processed,
		"errors", errors)
}

// GetStats returns collector statistics
func (c *Collector) GetStats() (processed int64, errors int64, lastProcessed time.Time) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.processedCount, c.errorCount, c.lastProcessedTime
}

// isTestEnvironment checks if we're running in a test environment
func isTestEnvironment() bool {
	return len(os.Args) > 0 && (strings.HasSuffix(os.Args[0], ".test") || strings.Contains(os.Args[0], "/_test/"))
}
