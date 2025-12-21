package streamer

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
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/telemetry"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/utils"
)

// Streamer reads telemetry from CSV and publishes to message queue
type Streamer struct {
	config     *config.StreamerConfig
	parser     telemetry.Parser
	queue      mq.MessageQueue
	records    []*domain.TelemetryRecord
	gpus       []*domain.GPU
	mu         sync.RWMutex
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	shutdownCh chan struct{}
}

// NewStreamer creates a new streamer instance
func NewStreamer(cfg *config.StreamerConfig, parser telemetry.Parser, queue mq.MessageQueue) (*Streamer, error) {
	ctx, cancel := context.WithCancel(context.Background())

	return &Streamer{
		config:     cfg,
		parser:     parser,
		queue:      queue,
		ctx:        ctx,
		cancel:     cancel,
		shutdownCh: make(chan struct{}),
	}, nil
}

// LoadCSV loads and parses the CSV file
func (s *Streamer) LoadCSV() error {
	file, err := os.Open(s.config.CSVFilePath)
	if err != nil {
		return fmt.Errorf("failed to open CSV file %s: %w", s.config.CSVFilePath, err)
	}
	defer file.Close()

	records, gpus, err := s.parser.Parse(file)
	if err != nil {
		return fmt.Errorf("failed to parse CSV: %w", err)
	}

	s.mu.Lock()
	s.records = records
	s.gpus = gpus
	s.mu.Unlock()

	utils.Logger.Info("CSV loaded successfully",
		"file", s.config.CSVFilePath,
		"records", len(records),
		"gpus", len(gpus),
		"instance_id", s.config.InstanceID)

	return nil
}

// Start begins streaming telemetry records to the queue in a loop
func (s *Streamer) Start() error {
	if len(s.records) == 0 {
		return fmt.Errorf("no records loaded, call LoadCSV first")
	}

	// Setup signal handling for graceful shutdown (only in non-test environments)
	// Check if we're in a test by looking for the test flag
	sigCh := make(chan os.Signal, 1)
	if !isTestEnvironment() {
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	}

	// Start streaming goroutine
	s.wg.Add(1)
	go s.streamLoop()

	// Wait for shutdown signal or context cancellation
	select {
	case sig := <-sigCh:
		utils.Logger.Info("Received shutdown signal", "signal", sig, "instance_id", s.config.InstanceID)
	case <-s.shutdownCh:
		utils.Logger.Info("Shutdown requested", "instance_id", s.config.InstanceID)
	case <-s.ctx.Done():
		utils.Logger.Info("Context cancelled", "instance_id", s.config.InstanceID)
	}

	// Graceful shutdown
	s.Stop()
	return nil
}

// isTestEnvironment checks if we're running in a test environment
func isTestEnvironment() bool {
	return len(os.Args) > 0 && (strings.HasSuffix(os.Args[0], ".test") || strings.Contains(os.Args[0], "/_test/"))
}

// streamLoop continuously streams records from CSV in a loop
func (s *Streamer) streamLoop() {
	defer s.wg.Done()

	utils.Logger.Info("Starting stream loop",
		"instance_id", s.config.InstanceID,
		"interval", s.config.StreamInterval,
		"records", len(s.records))

	for {
		// Check if context is cancelled
		select {
		case <-s.ctx.Done():
			utils.Logger.Info("Stream loop stopped", "instance_id", s.config.InstanceID)
			return
		default:
		}

		// Stream all records
		s.mu.RLock()
		records := s.records
		s.mu.RUnlock()

		for _, record := range records {
			// Check for cancellation before each send
			select {
			case <-s.ctx.Done():
				return
			default:
			}

			// Create a copy of the record to avoid race conditions when modifying
			recordCopy := *record
			// Set ingestion time to now (ignore CSV timestamp)
			recordCopy.IngestionTime = time.Now()

			// Create message
			msg := domain.NewMessage(&recordCopy, s.config.InstanceID)

			// Publish to queue
			if err := s.queue.Publish(s.ctx, msg); err != nil {
				if s.ctx.Err() != nil {
					// Context cancelled, exit gracefully
					return
				}
				utils.Logger.Error("Failed to publish message",
					"error", err,
					"instance_id", s.config.InstanceID,
					"gpu_uuid", record.GPUUUID,
					"metric", record.MetricName)
				// Continue to next record even if publish fails
				continue
			}

			// Wait for interval before next record
			select {
			case <-s.ctx.Done():
				return
			case <-time.After(s.config.StreamInterval):
				// Continue to next record
			}
		}

		utils.Logger.Debug("Completed one CSV loop, restarting",
			"instance_id", s.config.InstanceID,
			"records", len(records))
	}
}

// Stop gracefully stops the streamer
func (s *Streamer) Stop() {
	utils.Logger.Info("Stopping streamer", "instance_id", s.config.InstanceID)
	s.cancel()
	s.wg.Wait()
	utils.Logger.Info("Streamer stopped", "instance_id", s.config.InstanceID)
}

// GetRecordCount returns the number of loaded records (for testing)
func (s *Streamer) GetRecordCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.records)
}

// GetGPUCount returns the number of unique GPUs found (for testing)
func (s *Streamer) GetGPUCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.gpus)
}
