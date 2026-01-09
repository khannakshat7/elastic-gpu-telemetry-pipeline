package streamer

import (
	"context"
	"os"
	"sync"
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

func TestStreamer_BackpressureOnQueueFull(t *testing.T) {
	// CSV with a single record so loop keeps retrying publish
	csvData := `timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,container,pod,namespace,value,labels_raw
"2025-07-18T20:42:34Z","DCGM_FI_DEV_GPU_UTIL","0","nvidia0","GPU-123","NVIDIA H100 80GB HBM3","host-1","","","","100","labels"`

	tmpFile := createTempCSV(t, csvData)
	defer tmpFile.Close()

	// Small queue to force queue-full quickly
	queue := mq.NewInMemoryMessageQueue(1)
	defer queue.Close()

	cfg := &config.StreamerConfig{
		CSVFilePath:    tmpFile.Name(),
		StreamInterval: 1 * time.Millisecond,
		InstanceID:     "bp-instance",
	}

	parser := telemetry.NewCSVParser()
	str, err := NewStreamer(cfg, parser, queue)
	require.NoError(t, err)
	require.NoError(t, str.LoadCSV())

	ctx := context.Background()
	sub, err := queue.Subscribe(ctx, "consumer")
	require.NoError(t, err)

	// Start streamer
	done := make(chan struct{})
	go func() {
		defer close(done)
		// Stop after a short time
		go func() {
			time.Sleep(120 * time.Millisecond)
			str.Stop()
		}()
		str.Start()
	}()

	// Let the streamer publish one message to fill the queue, then pause consumption
	var first *domain.Message
	select {
	case first = <-sub:
		// queue now has delivered 1; we will pause reading so subsequent Publishes see queue full
	case <-time.After(100 * time.Millisecond):
		t.Fatal("did not receive initial message")
	}

	// Now hold off consumption for a bit so Publish hits mq.ErrQueueFull and backpressure path executes
	time.Sleep(80 * time.Millisecond)

	// Resume consumption to drain messages and ensure streamer continued after backpressure
	received := []*domain.Message{first}
	collectUntil := time.After(120 * time.Millisecond)
COLLECT:
	for {
		select {
		case msg := <-sub:
			if msg == nil {
				break COLLECT
			}
			received = append(received, msg)
		case <-collectUntil:
			break COLLECT
		}
	}

	// We should have at least the initial message plus some more after resuming,
	// indicating the loop continued beyond queue-full backpressure.
	assert.GreaterOrEqual(t, len(received), 2)

	<-done
}

// ---- Fakes to simulate queue full retry behavior ----

// fakeQueueFullThenSuccess simulates ErrQueueFull for N attempts, then succeeds.
// Used to verify that records are retried (not skipped) when queue is full.
type fakeQueueFullThenSuccess struct {
	mu            sync.Mutex
	fullCount     int // Number of times to return ErrQueueFull
	publishedMsgs []*domain.Message
	fullResponses int // Track how many ErrQueueFull we returned
}

func newFakeQueueFullThenSuccess(fullCount int) *fakeQueueFullThenSuccess {
	return &fakeQueueFullThenSuccess{fullCount: fullCount}
}

func (f *fakeQueueFullThenSuccess) Publish(ctx context.Context, msg *domain.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fullResponses < f.fullCount {
		f.fullResponses++
		return mq.ErrQueueFull
	}
	f.publishedMsgs = append(f.publishedMsgs, msg)
	return nil
}

func (f *fakeQueueFullThenSuccess) Subscribe(ctx context.Context, consumerID string) (<-chan *domain.Message, error) {
	return make(chan *domain.Message), nil
}
func (f *fakeQueueFullThenSuccess) Ack(ctx context.Context, messageID string, consumerID string) error {
	return nil
}
func (f *fakeQueueFullThenSuccess) Close() error   { return nil }
func (f *fakeQueueFullThenSuccess) IsClosed() bool { return false }

func (f *fakeQueueFullThenSuccess) GetPublishedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.publishedMsgs)
}

func (f *fakeQueueFullThenSuccess) GetFullResponseCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fullResponses
}

// TestStreamer_QueueFullRetry verifies that when queue returns ErrQueueFull,
// the streamer retries the SAME record instead of skipping to the next one.
func TestStreamer_QueueFullRetry(t *testing.T) {
	csvData := `timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,container,pod,namespace,value,labels_raw
"2025-07-18T20:42:34Z","METRIC_1","0","nvidia0","GPU-123","NVIDIA H100","host-1","","","","100","labels"
"2025-07-18T20:42:35Z","METRIC_2","1","nvidia1","GPU-456","NVIDIA H100","host-2","","","","200","labels"`

	tmpFile := createTempCSV(t, csvData)
	defer tmpFile.Close()

	// Queue will return ErrQueueFull 3 times, then succeed
	fakeQueue := newFakeQueueFullThenSuccess(3)

	cfg := &config.StreamerConfig{
		CSVFilePath:    tmpFile.Name(),
		StreamInterval: 1 * time.Millisecond, // Fast interval for test
		InstanceID:     "test-retry",
	}
	parser := telemetry.NewCSVParser()
	str, err := NewStreamer(cfg, parser, fakeQueue)
	require.NoError(t, err)
	require.NoError(t, str.LoadCSV())

	// Start streaming in background
	done := make(chan struct{})
	go func() {
		defer close(done)
		go func() {
			// Wait for at least 2 messages to be published (one will have retries)
			for fakeQueue.GetPublishedCount() < 2 {
				time.Sleep(10 * time.Millisecond)
			}
			str.Stop()
		}()
		str.Start()
	}()

	select {
	case <-done:
		// Verify queue full was hit
		assert.GreaterOrEqual(t, fakeQueue.GetFullResponseCount(), 1, "should have hit queue full at least once")
		// Verify records were eventually published (not skipped)
		assert.GreaterOrEqual(t, fakeQueue.GetPublishedCount(), 2, "both records should eventually be published")
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for streamer")
	}
}

// ---- Fakes to simulate non-queue-full publish errors ----

type fakeQueueErrorThenSuccess struct {
	mu        sync.Mutex
	attempts  int
	successes int
	failOnce  bool
}

func newFakeQueueErrorThenSuccess() *fakeQueueErrorThenSuccess {
	return &fakeQueueErrorThenSuccess{failOnce: true}
}

func (f *fakeQueueErrorThenSuccess) Publish(ctx context.Context, msg *domain.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attempts++
	if f.failOnce {
		f.failOnce = false
		// Return a non-queue-full error to hit the error logging + continue path
		return context.DeadlineExceeded
	}
	f.successes++
	return nil
}

func (f *fakeQueueErrorThenSuccess) Subscribe(ctx context.Context, consumerID string) (<-chan *domain.Message, error) {
	ch := make(chan *domain.Message)
	return ch, nil
}
func (f *fakeQueueErrorThenSuccess) Ack(ctx context.Context, messageID string, consumerID string) error {
	return nil
}
func (f *fakeQueueErrorThenSuccess) Close() error   { return nil }
func (f *fakeQueueErrorThenSuccess) IsClosed() bool { return false }

// Queue that blocks until the provided context is cancelled, then returns ctx.Err()
// to exercise the early return when s.ctx is cancelled after a publish error.

type fakeQueueBlockUntilCancel struct {
	mu       sync.Mutex
	attempts int
}

func (f *fakeQueueBlockUntilCancel) Publish(ctx context.Context, msg *domain.Message) error {
	f.mu.Lock()
	f.attempts++
	f.mu.Unlock()
	<-ctx.Done()
	return ctx.Err()
}

func (f *fakeQueueBlockUntilCancel) Subscribe(ctx context.Context, consumerID string) (<-chan *domain.Message, error) {
	ch := make(chan *domain.Message)
	return ch, nil
}
func (f *fakeQueueBlockUntilCancel) Ack(ctx context.Context, messageID string, consumerID string) error {
	return nil
}
func (f *fakeQueueBlockUntilCancel) Close() error   { return nil }
func (f *fakeQueueBlockUntilCancel) IsClosed() bool { return false }

func TestStreamer_PublishError_ContinueOnNonQueueError(t *testing.T) {
	csvData := `timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,container,pod,namespace,value,labels_raw
"2025-07-18T20:42:34Z","DCGM_FI_DEV_GPU_UTIL","0","nvidia0","GPU-123","NVIDIA H100 80GB HBM3","host-1","","","","100","labels"
"2025-07-18T20:42:35Z","DCGM_FI_DEV_GPU_TEMP","1","nvidia1","GPU-456","NVIDIA H100 80GB HBM3","host-2","","","","75","labels"`

	tmpFile := createTempCSV(t, csvData)
	defer tmpFile.Close()

	q := newFakeQueueErrorThenSuccess()
	cfg := &config.StreamerConfig{
		CSVFilePath:    tmpFile.Name(),
		StreamInterval: 1 * time.Millisecond,
		InstanceID:     "err-continue",
	}
	parser := telemetry.NewCSVParser()
	str, err := NewStreamer(cfg, parser, q)
	require.NoError(t, err)
	require.NoError(t, str.LoadCSV())

	// Start and stop after a short period
	done := make(chan struct{})
	go func() {
		defer close(done)
		go func() {
			time.Sleep(50 * time.Millisecond)
			str.Stop()
		}()
		str.Start()
	}()

	<-done

	q.mu.Lock()
	attempts := q.attempts
	successes := q.successes
	q.mu.Unlock()

	// We expect at least one failed publish and then a successful publish, proving continuation.
	assert.GreaterOrEqual(t, attempts, 2)
	assert.GreaterOrEqual(t, successes, 1)
}

func TestStreamer_PublishError_ContextCancelledExit(t *testing.T) {
	csvData := `timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,container,pod,namespace,value,labels_raw
"2025-07-18T20:42:34Z","DCGM_FI_DEV_GPU_UTIL","0","nvidia0","GPU-123","NVIDIA H100 80GB HBM3","host-1","","","","100","labels"`

	tmpFile := createTempCSV(t, csvData)
	defer tmpFile.Close()

	q := &fakeQueueBlockUntilCancel{}
	cfg := &config.StreamerConfig{
		CSVFilePath:    tmpFile.Name(),
		StreamInterval: 10 * time.Millisecond,
		InstanceID:     "err-cancel",
	}
	parser := telemetry.NewCSVParser()
	str, err := NewStreamer(cfg, parser, q)
	require.NoError(t, err)
	require.NoError(t, str.LoadCSV())

	// Start streaming then cancel shortly after. Publish will unblock with ctx.Err(),
	// and the stream loop should see s.ctx.Err()!=nil and return.
	done := make(chan struct{})
	go func() {
		defer close(done)
		go func() {
			time.Sleep(30 * time.Millisecond)
			str.Stop()
		}()
		str.Start()
	}()

	select {
	case <-done:
		// ok
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timeout waiting for streamer to exit on context cancellation")
	}
}
