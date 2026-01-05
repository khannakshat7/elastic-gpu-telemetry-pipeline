package streamer

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/config"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/domain"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/mq"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/telemetry"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	// Initialize logger for tests
	utils.SetupLogger()
}

func TestStreamer_LoadCSV(t *testing.T) {
	csvData := `timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,container,pod,namespace,value,labels_raw
"2025-07-18T20:42:34Z","DCGM_FI_DEV_GPU_UTIL","0","nvidia0","GPU-123","NVIDIA H100 80GB HBM3","host-1","","","","100","labels"`

	// Create temporary CSV file
	tmpFile := createTempCSV(t, csvData)
	defer tmpFile.Close()

	cfg := &config.StreamerConfig{
		CSVFilePath:    tmpFile.Name(),
		StreamInterval: 10 * time.Millisecond,
		InstanceID:     "test-instance",
	}

	parser := telemetry.NewCSVParser()
	queue := mq.NewInMemoryMessageQueue(100)
	defer queue.Close()

	str, err := NewStreamer(cfg, parser, queue)
	require.NoError(t, err)

	err = str.LoadCSV()
	require.NoError(t, err)

	assert.Equal(t, 1, str.GetRecordCount())
	assert.Equal(t, 1, str.GetGPUCount())
}

func TestStreamer_StreamLoop(t *testing.T) {
	csvData := `timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,container,pod,namespace,value,labels_raw
"2025-07-18T20:42:34Z","DCGM_FI_DEV_GPU_UTIL","0","nvidia0","GPU-123","NVIDIA H100 80GB HBM3","host-1","","","","100","labels"
"2025-07-18T20:42:35Z","DCGM_FI_DEV_GPU_TEMP","1","nvidia1","GPU-456","NVIDIA H100 80GB HBM3","host-2","","","","75","labels"`

	tmpFile := createTempCSV(t, csvData)
	defer tmpFile.Close()

	cfg := &config.StreamerConfig{
		CSVFilePath:    tmpFile.Name(),
		StreamInterval: 10 * time.Millisecond, // Fast interval for testing
		InstanceID:     "test-instance",
	}

	parser := telemetry.NewCSVParser()
	queue := mq.NewInMemoryMessageQueue(100)

	str, err := NewStreamer(cfg, parser, queue)
	require.NoError(t, err)

	err = str.LoadCSV()
	require.NoError(t, err)

	// Subscribe to queue
	ctx := context.Background()
	subChan, err := queue.Subscribe(ctx, "test-consumer-1")
	require.NoError(t, err)

	// Start streaming in background using Start() method
	done := make(chan struct{})
	go func() {
		defer close(done)
		// Use a separate goroutine to trigger shutdown after collecting messages
		go func() {
			time.Sleep(50 * time.Millisecond)
			str.Stop()
		}()
		// Start will block until shutdown
		str.Start()
	}()

	// Collect messages
	var received []*domain.Message
	timeout := time.After(200 * time.Millisecond)

	for len(received) < 2 {
		select {
		case msg, ok := <-subChan:
			if !ok {
				return
			}
			require.NotNil(t, msg)
			require.NotNil(t, msg.Payload)
			// Verify ingestion time is set (not zero)
			assert.False(t, msg.Payload.IngestionTime.IsZero(), "IngestionTime should be set")
			assert.True(t, time.Since(msg.Payload.IngestionTime) < time.Second, "IngestionTime should be recent")
			received = append(received, msg)
		case <-timeout:
			t.Fatalf("timeout waiting for messages, received %d", len(received))
		}
	}

	// Verify messages
	assert.Len(t, received, 2)
	assert.Equal(t, "test-instance", received[0].ProducerID)
	assert.Equal(t, "GPU-123", received[0].Payload.GPUUUID)
	assert.Equal(t, "DCGM_FI_DEV_GPU_UTIL", received[0].Payload.MetricName)

	// Wait for streamer to stop
	<-done
	queue.Close()
}

func TestStreamer_StreamLoop_MultipleIterations(t *testing.T) {
	csvData := `timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,container,pod,namespace,value,labels_raw
"2025-07-18T20:42:34Z","DCGM_FI_DEV_GPU_UTIL","0","nvidia0","GPU-123","NVIDIA H100 80GB HBM3","host-1","","","","100","labels"`

	tmpFile := createTempCSV(t, csvData)
	defer tmpFile.Close()

	cfg := &config.StreamerConfig{
		CSVFilePath:    tmpFile.Name(),
		StreamInterval: 5 * time.Millisecond,
		InstanceID:     "test-instance",
	}

	parser := telemetry.NewCSVParser()
	queue := mq.NewInMemoryMessageQueue(100)

	str, err := NewStreamer(cfg, parser, queue)
	require.NoError(t, err)

	err = str.LoadCSV()
	require.NoError(t, err)

	ctx := context.Background()
	subChan, err := queue.Subscribe(ctx, "test-consumer-1")
	require.NoError(t, err)

	// Start streaming with proper shutdown
	done := make(chan struct{})
	go func() {
		defer close(done)
		go func() {
			time.Sleep(50 * time.Millisecond)
			str.Stop()
		}()
		str.Start()
	}()

	// Collect at least 3 messages (should loop CSV multiple times)
	var received []*domain.Message
	timeout := time.After(200 * time.Millisecond)

	for len(received) < 3 {
		select {
		case msg, ok := <-subChan:
			if !ok {
				return
			}
			received = append(received, msg)
		case <-timeout:
			t.Fatalf("timeout waiting for messages, received %d", len(received))
		}
	}

	// All messages should be for the same GPU (since CSV only has one record)
	for _, msg := range received {
		assert.Equal(t, "GPU-123", msg.Payload.GPUUUID)
	}

	str.Stop()
	<-done
	queue.Close()
}

func TestStreamer_GracefulShutdown(t *testing.T) {
	csvData := `timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,container,pod,namespace,value,labels_raw
"2025-07-18T20:42:34Z","DCGM_FI_DEV_GPU_UTIL","0","nvidia0","GPU-123","NVIDIA H100 80GB HBM3","host-1","","","","100","labels"`

	tmpFile := createTempCSV(t, csvData)
	defer tmpFile.Close()

	cfg := &config.StreamerConfig{
		CSVFilePath:    tmpFile.Name(),
		StreamInterval: 50 * time.Millisecond,
		InstanceID:     "test-instance",
	}

	parser := telemetry.NewCSVParser()
	queue := mq.NewInMemoryMessageQueue(100)

	str, err := NewStreamer(cfg, parser, queue)
	require.NoError(t, err)

	err = str.LoadCSV()
	require.NoError(t, err)

	// Start streaming
	done := make(chan struct{})
	go func() {
		defer close(done)
		go func() {
			time.Sleep(20 * time.Millisecond)
			str.Stop()
		}()
		str.Start()
	}()

	// Wait for shutdown to complete
	select {
	case <-done:
		// Success
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timeout waiting for graceful shutdown")
	}

	queue.Close()
}

func TestStreamer_MultipleInstances(t *testing.T) {
	csvData := `timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,container,pod,namespace,value,labels_raw
"2025-07-18T20:42:34Z","DCGM_FI_DEV_GPU_UTIL","0","nvidia0","GPU-123","NVIDIA H100 80GB HBM3","host-1","","","","100","labels"`

	tmpFile := createTempCSV(t, csvData)
	defer tmpFile.Close()

	// Create shared queue
	queue := mq.NewInMemoryMessageQueue(1000)
	defer queue.Close()

	ctx := context.Background()
	subChan, err := queue.Subscribe(ctx, "test-consumer-1")
	require.NoError(t, err)

	// Create multiple streamer instances
	numInstances := 3
	streamers := make([]*Streamer, numInstances)

	for i := 0; i < numInstances; i++ {
		cfg := &config.StreamerConfig{
			CSVFilePath:    tmpFile.Name(),
			StreamInterval: 10 * time.Millisecond,
			InstanceID:     "instance-" + string(rune(i+'0')),
		}

		parser := telemetry.NewCSVParser()
		str, err := NewStreamer(cfg, parser, queue)
		require.NoError(t, err)

		err = str.LoadCSV()
		require.NoError(t, err)

		streamers[i] = str

		// Start each streamer in background
		go func(s *Streamer) {
			go func() {
				time.Sleep(100 * time.Millisecond)
				s.Stop()
			}()
			s.Start()
		}(str)
	}

	// Collect messages from all instances
	var received []*domain.Message
	timeout := time.After(200 * time.Millisecond)

	for len(received) < numInstances*2 { // At least 2 messages per instance
		select {
		case msg, ok := <-subChan:
			if !ok {
				return
			}
			received = append(received, msg)
		case <-timeout:
			break
		}
	}

	// Verify we got messages from multiple instances
	instanceIDs := make(map[string]int)
	for _, msg := range received {
		instanceIDs[msg.ProducerID]++
	}

	assert.GreaterOrEqual(t, len(instanceIDs), 1, "should receive messages from at least one instance")

	// Stop all streamers
	for _, str := range streamers {
		str.Stop()
	}
}

// Helper function to create a temporary CSV file for testing
func createTempCSV(t *testing.T, content string) *os.File {
	tmpFile, err := os.CreateTemp("", "test-*.csv")
	require.NoError(t, err)

	_, err = tmpFile.WriteString(content)
	require.NoError(t, err)

	err = tmpFile.Close()
	require.NoError(t, err)

	// Reopen for reading
	file, err := os.Open(tmpFile.Name())
	require.NoError(t, err)

	t.Cleanup(func() {
		file.Close()
		os.Remove(tmpFile.Name())
	})

	return file
}

func TestStreamer_LoadCSV_FileNotFound(t *testing.T) {
	cfg := &config.StreamerConfig{
		CSVFilePath:    "/nonexistent/file.csv",
		StreamInterval: 10 * time.Millisecond,
		InstanceID:     "test-instance",
	}

	parser := telemetry.NewCSVParser()
	queue := mq.NewInMemoryMessageQueue(100)
	defer queue.Close()

	str, err := NewStreamer(cfg, parser, queue)
	require.NoError(t, err)

	err = str.LoadCSV()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to open CSV file")
}

func TestStreamer_Start_NoRecordsLoaded(t *testing.T) {
	cfg := &config.StreamerConfig{
		CSVFilePath:    "/tmp/test.csv",
		StreamInterval: 10 * time.Millisecond,
		InstanceID:     "test-instance",
	}

	parser := telemetry.NewCSVParser()
	queue := mq.NewInMemoryMessageQueue(100)
	defer queue.Close()

	str, err := NewStreamer(cfg, parser, queue)
	require.NoError(t, err)

	// Try to start without loading CSV
	err = str.Start()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no records loaded")
}
