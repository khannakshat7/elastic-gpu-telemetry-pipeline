package collector

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/config"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/domain"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/mq"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/storage"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/storage/memory"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	// Initialize logger for tests
	utils.SetupLogger()
}

func TestCollector_ProcessMessage(t *testing.T) {
	cfg := &config.CollectorConfig{
		InstanceID: "test-collector",
		BatchSize:  1,
	}

	queue := mq.NewInMemoryMessageQueue(100)
	defer queue.Close()

	repository := memory.NewStore()

	col, err := NewCollector(cfg, queue, repository)
	require.NoError(t, err)

	// Create a test message
	record := &domain.TelemetryRecord{
		GPUUUID:       "GPU-123",
		MetricName:    "DCGM_FI_DEV_GPU_UTIL",
		Value:         "100",
		IngestionTime: time.Now(),
		Hostname:      "host-1",
		ModelName:     "NVIDIA H100",
	}
	msg := domain.NewMessage(record, "producer-1")

	// Process message manually
	ctx := context.Background()
	gpuMap := make(map[string]*domain.GPU)
	err = col.processMessage(ctx, msg, gpuMap)
	require.NoError(t, err)

	// Verify telemetry was saved
	telemetry, err := repository.GetTelemetryByGPU(ctx, "GPU-123", nil, nil)
	require.NoError(t, err)
	require.Len(t, telemetry, 1)
	assert.Equal(t, "GPU-123", telemetry[0].GPUUUID)
	assert.Equal(t, "DCGM_FI_DEV_GPU_UTIL", telemetry[0].MetricName)
	assert.Equal(t, "100", telemetry[0].Value)

	// Verify GPU was tracked
	require.Len(t, gpuMap, 1)
	gpu, exists := gpuMap["GPU-123"]
	require.True(t, exists)
	assert.Equal(t, "GPU-123", gpu.UUID)
	assert.Equal(t, "host-1", gpu.Hostname)
	assert.Equal(t, "NVIDIA H100", gpu.Model)
}

func TestCollector_ProcessMessage_Invalid(t *testing.T) {
	cfg := &config.CollectorConfig{
		InstanceID: "test-collector",
		BatchSize:  1,
	}

	queue := mq.NewInMemoryMessageQueue(100)
	defer queue.Close()

	repository := memory.NewStore()

	col, err := NewCollector(cfg, queue, repository)
	require.NoError(t, err)

	ctx := context.Background()
	gpuMap := make(map[string]*domain.GPU)

	// Test nil message
	err = col.processMessage(ctx, nil, gpuMap)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "message is nil")

	// Test nil payload
	msg := &domain.Message{ID: "test", Payload: nil}
	err = col.processMessage(ctx, msg, gpuMap)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "payload is nil")

	// Test empty GPU UUID
	record := &domain.TelemetryRecord{
		GPUUUID:       "",
		MetricName:    "DCGM_FI_DEV_GPU_UTIL",
		Value:         "100",
		IngestionTime: time.Now(),
	}
	msg = domain.NewMessage(record, "producer-1")
	err = col.processMessage(ctx, msg, gpuMap)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "GPU UUID is empty")

	// Test empty metric name
	record = &domain.TelemetryRecord{
		GPUUUID:       "GPU-123",
		MetricName:    "",
		Value:         "100",
		IngestionTime: time.Now(),
	}
	msg = domain.NewMessage(record, "producer-1")
	err = col.processMessage(ctx, msg, gpuMap)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "metric name is empty")

	// Test zero ingestion time
	record = &domain.TelemetryRecord{
		GPUUUID:       "GPU-123",
		MetricName:    "DCGM_FI_DEV_GPU_UTIL",
		Value:         "100",
		IngestionTime: time.Time{},
	}
	msg = domain.NewMessage(record, "producer-1")
	err = col.processMessage(ctx, msg, gpuMap)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ingestion time is zero")
}

func TestCollector_BatchProcessing(t *testing.T) {
	cfg := &config.CollectorConfig{
		InstanceID: "test-collector",
		BatchSize:  3, // Process in batches of 3
	}

	queue := mq.NewInMemoryMessageQueue(100)
	defer queue.Close()

	repository := memory.NewStore()

	col, err := NewCollector(cfg, queue, repository)
	require.NoError(t, err)

	ctx := context.Background()

	// Create multiple messages
	messages := make([]*domain.Message, 5)
	for i := 0; i < 5; i++ {
		record := &domain.TelemetryRecord{
			GPUUUID:       "GPU-123",
			MetricName:    "DCGM_FI_DEV_GPU_UTIL",
			Value:         "100",
			IngestionTime: time.Now(),
			Hostname:      "host-1",
			ModelName:     "NVIDIA H100",
		}
		messages[i] = domain.NewMessage(record, "producer-1")
	}

	// Process first batch (3 messages)
	batch1 := messages[0:3]
	col.processBatch(batch1)

	// Verify first 3 were saved
	telemetry, err := repository.GetTelemetryByGPU(ctx, "GPU-123", nil, nil)
	require.NoError(t, err)
	assert.Len(t, telemetry, 3)

	// Process second batch (2 messages)
	batch2 := messages[3:5]
	col.processBatch(batch2)

	// Verify all 5 were saved
	telemetry, err = repository.GetTelemetryByGPU(ctx, "GPU-123", nil, nil)
	require.NoError(t, err)
	assert.Len(t, telemetry, 5)
}

func TestCollector_StartStop(t *testing.T) {
	cfg := &config.CollectorConfig{
		InstanceID: "test-collector",
		BatchSize:  1,
	}

	queue := mq.NewInMemoryMessageQueue(100)
	defer queue.Close()

	repository := memory.NewStore()

	col, err := NewCollector(cfg, queue, repository)
	require.NoError(t, err)

	// Start collector in background
	done := make(chan struct{})
	go func() {
		defer close(done)
		go func() {
			time.Sleep(50 * time.Millisecond)
			col.Stop()
		}()
		col.Start()
	}()

	// Wait for shutdown
	select {
	case <-done:
		// Success
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timeout waiting for collector shutdown")
	}
}

func TestCollector_ProcessMessages(t *testing.T) {
	cfg := &config.CollectorConfig{
		InstanceID: "test-collector",
		BatchSize:  2, // Process in batches of 2
	}

	queue := mq.NewInMemoryMessageQueue(100)
	defer queue.Close()

	repository := memory.NewStore()

	col, err := NewCollector(cfg, queue, repository)
	require.NoError(t, err)

	ctx := context.Background()

	// Subscribe to queue (collector will do this, but we need it for testing)
	// Use the collector's instance ID as consumer ID to match ACK requirements
	subChan, err := queue.Subscribe(ctx, cfg.InstanceID)
	require.NoError(t, err)

	// Publish messages
	for i := 0; i < 4; i++ {
		record := &domain.TelemetryRecord{
			GPUUUID:       "GPU-123",
			MetricName:    "DCGM_FI_DEV_GPU_UTIL",
			Value:         "100",
			IngestionTime: time.Now(),
			Hostname:      "host-1",
			ModelName:     "NVIDIA H100",
		}
		msg := domain.NewMessage(record, "producer-1")
		err := queue.Publish(ctx, msg)
		require.NoError(t, err)
	}

	// Start processing in background (need to add to waitgroup first)
	col.wg.Add(1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		col.processMessages(subChan)
	}()

	// Wait a bit for processing
	time.Sleep(100 * time.Millisecond)

	// Stop collector
	col.Stop()

	// Wait for processing to complete
	select {
	case <-done:
		// Success
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timeout waiting for message processing")
	}

	// Verify messages were processed
	telemetry, err := repository.GetTelemetryByGPU(ctx, "GPU-123", nil, nil)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(telemetry), 4, "should have processed at least 4 messages")
}

func TestCollector_MultipleInstances(t *testing.T) {
	queue := mq.NewInMemoryMessageQueue(1000)
	defer queue.Close()

	repository := memory.NewStore()

	// Create multiple collector instances
	numCollectors := 3
	collectors := make([]*Collector, numCollectors)

	for i := 0; i < numCollectors; i++ {
		cfg := &config.CollectorConfig{
			InstanceID: "collector-" + string(rune(i+'0')),
			BatchSize:  1,
		}

		col, err := NewCollector(cfg, queue, repository)
		require.NoError(t, err)
		collectors[i] = col
	}

	ctx := context.Background()

	// Subscribe each collector with their own consumer ID
	subChans := make([]<-chan *domain.Message, numCollectors)
	for i, col := range collectors {
		// Use the collector's instance ID as consumer ID
		consumerID := col.config.InstanceID
		subChan, err := queue.Subscribe(ctx, consumerID)
		require.NoError(t, err)
		subChans[i] = subChan

		// Start processing (need to add to waitgroup first)
		col.wg.Add(1)
		go col.processMessages(subChan)
	}

	// Publish messages
	numMessages := 10
	for i := 0; i < numMessages; i++ {
		record := &domain.TelemetryRecord{
			GPUUUID:       "GPU-123",
			MetricName:    "DCGM_FI_DEV_GPU_UTIL",
			Value:         "100",
			IngestionTime: time.Now(),
			Hostname:      "host-1",
			ModelName:     "NVIDIA H100",
		}
		msg := domain.NewMessage(record, "producer-1")
		err := queue.Publish(ctx, msg)
		require.NoError(t, err)
	}

	// Wait for processing
	time.Sleep(500 * time.Millisecond)

	// Stop all collectors
	for _, col := range collectors {
		col.Stop()
	}

	// Verify messages were processed (work queue pattern means each message goes to one collector)
	// So we should have exactly numMessages total records (distributed across collectors)
	telemetry, err := repository.GetTelemetryByGPU(ctx, "GPU-123", nil, nil)
	require.NoError(t, err)
	assert.Equal(t, numMessages, len(telemetry),
		"work queue pattern: each message should be processed exactly once")
}

func TestCollector_Stats(t *testing.T) {
	cfg := &config.CollectorConfig{
		InstanceID: "test-collector",
		BatchSize:  1,
	}

	queue := mq.NewInMemoryMessageQueue(100)
	defer queue.Close()

	repository := memory.NewStore()

	col, err := NewCollector(cfg, queue, repository)
	require.NoError(t, err)

	// Initially stats should be zero
	processed, errors, lastProcessed := col.GetStats()
	assert.Equal(t, int64(0), processed)
	assert.Equal(t, int64(0), errors)
	assert.True(t, lastProcessed.IsZero())

	// Process a valid message
	ctx := context.Background()
	record := &domain.TelemetryRecord{
		GPUUUID:       "GPU-123",
		MetricName:    "DCGM_FI_DEV_GPU_UTIL",
		Value:         "100",
		IngestionTime: time.Now(),
		Hostname:      "host-1",
		ModelName:     "NVIDIA H100",
	}
	msg := domain.NewMessage(record, "producer-1")

	gpuMap := make(map[string]*domain.GPU)
	err = col.processMessage(ctx, msg, gpuMap)
	require.NoError(t, err)

	// Save GPU
	for _, gpu := range gpuMap {
		repository.SaveGPU(ctx, gpu)
	}

	// Process batch to update stats
	col.processBatch([]*domain.Message{msg})

	// Check stats
	processed, errors, lastProcessed = col.GetStats()
	assert.Greater(t, processed, int64(0))
	assert.Equal(t, int64(0), errors)
	assert.False(t, lastProcessed.IsZero())
}

func TestCollector_ProcessMessages_ChannelClosed(t *testing.T) {
	queue := mq.NewInMemoryMessageQueue(100)
	defer queue.Close()
	repository, _ := storage.NewRepository(storage.BackendMemory, nil)

	cfg := &config.CollectorConfig{
		InstanceID: "test-collector",
		BatchSize:  10,
	}

	collector, err := NewCollector(cfg, queue, repository)
	require.NoError(t, err)

	// Set up WaitGroup properly (processMessages expects this)
	collector.wg.Add(1)

	// Create a channel and close it to simulate channel closure
	msgChan := make(chan *domain.Message, 1)
	close(msgChan)

	// Start processing in a goroutine
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		collector.processMessages(msgChan)
	}()

	// Wait for processing to complete
	wg.Wait()
	// Should complete without error when channel is closed
}

func TestCollector_ProcessMessages_ContextCancelled(t *testing.T) {
	queue := mq.NewInMemoryMessageQueue(100)
	defer queue.Close()
	repository, _ := storage.NewRepository(storage.BackendMemory, nil)

	cfg := &config.CollectorConfig{
		InstanceID: "test-collector",
		BatchSize:  10,
	}

	collector, err := NewCollector(cfg, queue, repository)
	require.NoError(t, err)

	// Set up WaitGroup properly (processMessages expects this)
	collector.wg.Add(1)

	// Create a channel that will block
	msgChan := make(chan *domain.Message)

	// Start processing in a goroutine
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		collector.processMessages(msgChan)
	}()

	// Cancel context
	collector.cancel()

	// Wait for processing to complete
	wg.Wait()
	// Should complete when context is cancelled
}

func TestCollector_ProcessMessages_RemainingBatch(t *testing.T) {
	queue := mq.NewInMemoryMessageQueue(100)
	defer queue.Close()
	repository, _ := storage.NewRepository(storage.BackendMemory, nil)

	cfg := &config.CollectorConfig{
		InstanceID: "test-collector",
		BatchSize:  5, // Small batch size
	}

	collector, err := NewCollector(cfg, queue, repository)
	require.NoError(t, err)

	// Set up WaitGroup properly (processMessages expects this)
	collector.wg.Add(1)

	// Create a channel and send some messages
	msgChan := make(chan *domain.Message, 3)
	for i := 0; i < 3; i++ {
		record := &domain.TelemetryRecord{
			GPUUUID:       fmt.Sprintf("GPU-%d", i),
			MetricName:    "DCGM_FI_DEV_GPU_UTIL",
			Value:         "100",
			IngestionTime: time.Now(),
		}
		msg := domain.NewMessage(record, "producer-1")
		msgChan <- msg
	}
	close(msgChan)

	// Start processing
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		collector.processMessages(msgChan)
	}()

	// Wait for processing to complete
	wg.Wait()
	// Should process remaining batch when channel closes
}

func TestCollector_GPUDeduplication(t *testing.T) {
	cfg := &config.CollectorConfig{
		InstanceID: "test-collector",
		BatchSize:  1,
	}

	queue := mq.NewInMemoryMessageQueue(100)
	defer queue.Close()

	repository := memory.NewStore()

	col, err := NewCollector(cfg, queue, repository)
	require.NoError(t, err)

	ctx := context.Background()

	// Create messages for the same GPU
	messages := []*domain.Message{
		domain.NewMessage(&domain.TelemetryRecord{
			GPUUUID:       "GPU-123",
			MetricName:    "DCGM_FI_DEV_GPU_UTIL",
			Value:         "100",
			IngestionTime: time.Now(),
			Hostname:      "host-1",
			ModelName:     "NVIDIA H100",
		}, "producer-1"),
		domain.NewMessage(&domain.TelemetryRecord{
			GPUUUID:       "GPU-123",
			MetricName:    "DCGM_FI_DEV_GPU_TEMP",
			Value:         "75",
			IngestionTime: time.Now(),
			Hostname:      "host-1",
			ModelName:     "NVIDIA H100",
		}, "producer-1"),
	}

	// Process batch
	gpuMap := make(map[string]*domain.GPU)
	for _, msg := range messages {
		err := col.processMessage(ctx, msg, gpuMap)
		require.NoError(t, err)
	}

	// Should only have one GPU in the map (deduplicated)
	assert.Len(t, gpuMap, 1)
	gpu, exists := gpuMap["GPU-123"]
	require.True(t, exists)
	assert.Equal(t, "GPU-123", gpu.UUID)
	assert.Equal(t, "host-1", gpu.Hostname)
	assert.Equal(t, "NVIDIA H100", gpu.Model)
}

func TestNewCollector_NilQueue(t *testing.T) {
	cfg := &config.CollectorConfig{
		InstanceID: "test-collector",
		BatchSize:  1,
	}

	repository := memory.NewStore()

	col, err := NewCollector(cfg, nil, repository)
	assert.Error(t, err)
	assert.Nil(t, col)
	assert.Contains(t, err.Error(), "queue cannot be nil")
}

func TestNewCollector_NilRepository(t *testing.T) {
	cfg := &config.CollectorConfig{
		InstanceID: "test-collector",
		BatchSize:  1,
	}

	queue := mq.NewInMemoryMessageQueue(100)
	defer queue.Close()

	col, err := NewCollector(cfg, queue, nil)
	assert.Error(t, err)
	assert.Nil(t, col)
	assert.Contains(t, err.Error(), "repository cannot be nil")
}

func TestCollector_ProcessBatch_EmptyBatch(t *testing.T) {
	cfg := &config.CollectorConfig{
		InstanceID: "test-collector",
		BatchSize:  5,
	}

	queue := mq.NewInMemoryMessageQueue(100)
	defer queue.Close()

	repository := memory.NewStore()

	col, err := NewCollector(cfg, queue, repository)
	require.NoError(t, err)

	// Process empty batch
	batch := []*domain.Message{}
	col.processBatch(batch) // Empty batch should not error
}

func TestCollector_ProcessBatch_PartialBatch(t *testing.T) {
	cfg := &config.CollectorConfig{
		InstanceID: "test-collector",
		BatchSize:  5,
	}

	queue := mq.NewInMemoryMessageQueue(100)
	defer queue.Close()

	repository := memory.NewStore()

	col, err := NewCollector(cfg, queue, repository)
	require.NoError(t, err)

	// Process partial batch (less than batch size)
	batch := []*domain.Message{
		domain.NewMessage(&domain.TelemetryRecord{
			GPUUUID:       "GPU-123",
			MetricName:    "DCGM_FI_DEV_GPU_UTIL",
			Value:         "100",
			IngestionTime: time.Now(),
		}, "producer-1"),
		domain.NewMessage(&domain.TelemetryRecord{
			GPUUUID:       "GPU-456",
			MetricName:    "DCGM_FI_DEV_GPU_UTIL",
			Value:         "50",
			IngestionTime: time.Now(),
		}, "producer-1"),
	}

	col.processBatch(batch)

	// Verify data was saved
	gpus, err := repository.ListGPUs(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 2, len(gpus))
}
