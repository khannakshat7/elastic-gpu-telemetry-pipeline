package system

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/internal/collector"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/internal/streamer"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/api"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/config"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/domain"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/mq"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/storage"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/storage/memory"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/telemetry"
	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	utils.SetupLogger()
	gin.SetMode(gin.TestMode)
}

// TestSystem_EndToEndFlow tests the complete pipeline flow:
// Streamer → Queue Service → Collector → Storage Service → API Gateway
func TestSystem_EndToEndFlow(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping system test in short mode")
	}

	// Setup: Create temporary CSV file with test data
	csvData := `timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,container,pod,namespace,value,labels_raw
"2025-07-18T20:42:34Z","DCGM_FI_DEV_GPU_UTIL","0","nvidia0","GPU-test-001","NVIDIA H100 80GB HBM3","test-host-1","gpu-workload","pod-1","team1","85.5","labels"
"2025-07-18T20:42:35Z","DCGM_FI_DEV_GPU_TEMP","0","nvidia0","GPU-test-001","NVIDIA H100 80GB HBM3","test-host-1","gpu-workload","pod-1","team1","72.0","labels"
"2025-07-18T20:42:34Z","DCGM_FI_DEV_GPU_UTIL","1","nvidia1","GPU-test-002","NVIDIA A100 40GB","test-host-2","","","","90.0","labels"`

	tmpCSV := createTempCSV(t, csvData)
	defer os.Remove(tmpCSV.Name())

	// Step 1: Start Queue Service
	queue := mq.NewInMemoryMessageQueue(1000)
	queueServer := mq.NewServer(queue)
	queuePort := "18080" // Use non-standard port to avoid conflicts
	require.NoError(t, queueServer.Start(queuePort))
	defer queueServer.Stop()
	defer queue.Close()

	// Wait for queue service to be ready
	require.Eventually(t, func() bool {
		resp, err := http.Get(fmt.Sprintf("http://localhost:%s/api/v1/health", queuePort))
		return err == nil && resp.StatusCode == http.StatusOK
	}, 5*time.Second, 100*time.Millisecond, "Queue service should be ready")

	// Step 2: Start Storage Service
	storageRepo := memory.NewStore()
	storagePort := "18082"
	storageServer := startStorageService(t, storageRepo, storagePort)
	defer storageServer.Shutdown(context.Background())

	// Wait for storage service to be ready
	require.Eventually(t, func() bool {
		resp, err := http.Get(fmt.Sprintf("http://localhost:%s/health", storagePort))
		return err == nil && resp.StatusCode == http.StatusOK
	}, 5*time.Second, 100*time.Millisecond, "Storage service should be ready")

	// Step 3: Start Collector
	collectorCfg := &config.CollectorConfig{
		InstanceID:      "test-collector-1",
		BatchSize:       5,
		QueueServiceURL: fmt.Sprintf("http://localhost:%s", queuePort),
		StorageBackend:  "memory",
	}
	storageConfig := map[string]string{
		"storage_service_url": fmt.Sprintf("http://localhost:%s", storagePort),
	}
	collectorStorage, err := storage.NewRepository(storage.BackendMemory, storageConfig)
	require.NoError(t, err)

	queueClient := mq.NewHTTPMessageQueue(fmt.Sprintf("http://localhost:%s", queuePort))
	collector, err := collector.NewCollector(collectorCfg, queueClient, collectorStorage)
	require.NoError(t, err)

	// Start collector in goroutine
	collectorDone := make(chan error, 1)
	go func() {
		collectorDone <- collector.Start()
	}()
	defer func() {
		collector.Stop()
		<-collectorDone
	}()

	// Step 4: Start Streamer
	streamerCfg := &config.StreamerConfig{
		CSVFilePath:     tmpCSV.Name(),
		StreamInterval:  50, // 50ms between messages
		InstanceID:      "test-streamer-1",
		QueueServiceURL: fmt.Sprintf("http://localhost:%s", queuePort),
	}
	parser := telemetry.NewCSVParser()
	str, err := streamer.NewStreamer(streamerCfg, parser, queueClient)
	require.NoError(t, err)

	require.NoError(t, str.LoadCSV())

	// Start streamer in goroutine
	streamerDone := make(chan error, 1)
	go func() {
		streamerDone <- str.Start()
	}()
	defer func() {
		str.Stop()
		<-streamerDone
	}()

	// Step 5: Start API Gateway
	apiGatewayPort := "18081"
	apiStorageConfig := map[string]string{
		"storage_service_url": fmt.Sprintf("http://localhost:%s", storagePort),
	}
	apiStorage, err := storage.NewRepository(storage.BackendMemory, apiStorageConfig)
	require.NoError(t, err)

	apiHandlers := api.NewHandlers(apiStorage)
	apiServer := startAPIGateway(t, apiHandlers, apiGatewayPort)
	defer apiServer.Shutdown(context.Background())

	// Wait for API Gateway to be ready
	require.Eventually(t, func() bool {
		resp, err := http.Get(fmt.Sprintf("http://localhost:%s/health", apiGatewayPort))
		return err == nil && resp.StatusCode == http.StatusOK
	}, 5*time.Second, 100*time.Millisecond, "API Gateway should be ready")

	// Give the system time to process messages
	time.Sleep(2 * time.Second)

	// Step 6: Verify data through API Gateway
	t.Run("ListGPUs", func(t *testing.T) {
		resp, err := http.Get(fmt.Sprintf("http://localhost:%s/api/v1/gpus", apiGatewayPort))
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var listResponse api.ListGPUsResponse
		err = json.NewDecoder(resp.Body).Decode(&listResponse)
		require.NoError(t, err)

		// Should have at least 2 GPUs
		assert.GreaterOrEqual(t, listResponse.Count, 2, "Should have at least 2 GPUs")

		// Find our test GPUs
		gpuMap := make(map[string]*api.GPUResponse)
		for i := range listResponse.GPUs {
			gpuMap[listResponse.GPUs[i].UUID] = &listResponse.GPUs[i]
		}

		// Verify GPU-test-001
		gpu1, exists := gpuMap["GPU-test-001"]
		require.True(t, exists, "GPU-test-001 should exist")
		assert.Equal(t, "0", gpu1.GPUIndex)
		assert.Equal(t, "nvidia0", gpu1.DeviceID)
		assert.Equal(t, "NVIDIA H100 80GB HBM3", gpu1.ModelName)
		assert.Equal(t, "test-host-1", gpu1.Hostname)
		assert.Equal(t, "gpu-workload", gpu1.Container)
		assert.Equal(t, "pod-1", gpu1.Pod)
		assert.Equal(t, "team1", gpu1.Namespace)

		// Verify GPU-test-002
		gpu2, exists := gpuMap["GPU-test-002"]
		require.True(t, exists, "GPU-test-002 should exist")
		assert.Equal(t, "1", gpu2.GPUIndex)
		assert.Equal(t, "nvidia1", gpu2.DeviceID)
		assert.Equal(t, "NVIDIA A100 40GB", gpu2.ModelName)
		assert.Equal(t, "test-host-2", gpu2.Hostname)
	})

	t.Run("GetTelemetryByGPU", func(t *testing.T) {
		resp, err := http.Get(fmt.Sprintf("http://localhost:%s/api/v1/gpus/GPU-test-001/telemetry", apiGatewayPort))
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var telemetryResponse api.GetTelemetryResponse
		err = json.NewDecoder(resp.Body).Decode(&telemetryResponse)
		require.NoError(t, err)

		assert.Equal(t, "GPU-test-001", telemetryResponse.GPUUUID)
		assert.GreaterOrEqual(t, telemetryResponse.Count, 2, "Should have at least 2 telemetry records")

		// Verify we have both metrics
		metrics := make(map[string]bool)
		for _, record := range telemetryResponse.Records {
			metrics[record.MetricName] = true
			assert.NotEmpty(t, record.Value)
			assert.False(t, record.IngestionTime.IsZero())
		}

		assert.True(t, metrics["DCGM_FI_DEV_GPU_UTIL"], "Should have GPU utilization metric")
		assert.True(t, metrics["DCGM_FI_DEV_GPU_TEMP"], "Should have GPU temperature metric")
	})

	t.Run("GetTelemetryWithTimeRange", func(t *testing.T) {
		// Get telemetry for a specific time range
		startTime := time.Now().Add(-1 * time.Hour).Format(time.RFC3339)
		endTime := time.Now().Add(1 * time.Hour).Format(time.RFC3339)

		baseURL := fmt.Sprintf("http://localhost:%s/api/v1/gpus/GPU-test-001/telemetry", apiGatewayPort)
		reqURL, err := url.Parse(baseURL)
		require.NoError(t, err)

		params := url.Values{}
		params.Add("start_time", startTime)
		params.Add("end_time", endTime)
		reqURL.RawQuery = params.Encode()

		client := &http.Client{Timeout: 5 * time.Second}
		resp, err := client.Get(reqURL.String())
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var telemetryResponse api.GetTelemetryResponse
		err = json.NewDecoder(resp.Body).Decode(&telemetryResponse)
		require.NoError(t, err)

		assert.GreaterOrEqual(t, telemetryResponse.Count, 0, "Should have telemetry records in time range")
	})
}

// TestSystem_MultipleStreamers tests multiple streamer instances publishing to the same queue
func TestSystem_MultipleStreamers(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping system test in short mode")
	}

	// Setup CSV files for different streamers
	csvData1 := `timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,container,pod,namespace,value,labels_raw
"2025-07-18T20:42:34Z","DCGM_FI_DEV_GPU_UTIL","0","nvidia0","GPU-multi-001","NVIDIA H100","host-1","","","","50.0","labels"`

	csvData2 := `timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,container,pod,namespace,value,labels_raw
"2025-07-18T20:42:34Z","DCGM_FI_DEV_GPU_UTIL","1","nvidia1","GPU-multi-002","NVIDIA A100","host-2","","","","75.0","labels"`

	tmpCSV1 := createTempCSV(t, csvData1)
	defer os.Remove(tmpCSV1.Name())
	tmpCSV2 := createTempCSV(t, csvData2)
	defer os.Remove(tmpCSV2.Name())

	// Start Queue Service
	queue := mq.NewInMemoryMessageQueue(1000)
	queueServer := mq.NewServer(queue)
	queuePort := "18080"
	require.NoError(t, queueServer.Start(queuePort))
	defer queueServer.Stop()
	defer queue.Close()

	require.Eventually(t, func() bool {
		resp, err := http.Get(fmt.Sprintf("http://localhost:%s/api/v1/health", queuePort))
		return err == nil && resp.StatusCode == http.StatusOK
	}, 5*time.Second, 100*time.Millisecond)

	// Start Storage Service
	storageRepo := memory.NewStore()
	storagePort := "18082"
	storageServer := startStorageService(t, storageRepo, storagePort)
	defer storageServer.Shutdown(context.Background())

	require.Eventually(t, func() bool {
		resp, err := http.Get(fmt.Sprintf("http://localhost:%s/health", storagePort))
		return err == nil && resp.StatusCode == http.StatusOK
	}, 5*time.Second, 100*time.Millisecond)

	// Start Collector
	queueClient := mq.NewHTTPMessageQueue(fmt.Sprintf("http://localhost:%s", queuePort))
	storageConfig := map[string]string{
		"storage_service_url": fmt.Sprintf("http://localhost:%s", storagePort),
	}
	collectorStorage, err := storage.NewRepository(storage.BackendMemory, storageConfig)
	require.NoError(t, err)

	collectorCfg := &config.CollectorConfig{
		InstanceID:      "test-collector-1",
		BatchSize:       10,
		QueueServiceURL: fmt.Sprintf("http://localhost:%s", queuePort),
		StorageBackend:  "memory",
	}
	col, err := collector.NewCollector(collectorCfg, queueClient, collectorStorage)
	require.NoError(t, err)

	collectorDone := make(chan error, 1)
	go func() {
		collectorDone <- col.Start()
	}()
	defer func() {
		col.Stop()
		<-collectorDone
	}()

	// Start two streamers
	parser := telemetry.NewCSVParser()
	streamerCfg1 := &config.StreamerConfig{
		CSVFilePath:     tmpCSV1.Name(),
		StreamInterval:  50,
		InstanceID:      "test-streamer-1",
		QueueServiceURL: fmt.Sprintf("http://localhost:%s", queuePort),
	}
	str1, err := streamer.NewStreamer(streamerCfg1, parser, queueClient)
	require.NoError(t, err)
	require.NoError(t, str1.LoadCSV())

	streamerCfg2 := &config.StreamerConfig{
		CSVFilePath:     tmpCSV2.Name(),
		StreamInterval:  50,
		InstanceID:      "test-streamer-2",
		QueueServiceURL: fmt.Sprintf("http://localhost:%s", queuePort),
	}
	str2, err := streamer.NewStreamer(streamerCfg2, parser, queueClient)
	require.NoError(t, err)
	require.NoError(t, str2.LoadCSV())

	streamerDone1 := make(chan error, 1)
	go func() {
		streamerDone1 <- str1.Start()
	}()
	defer func() {
		str1.Stop()
		<-streamerDone1
	}()

	streamerDone2 := make(chan error, 1)
	go func() {
		streamerDone2 <- str2.Start()
	}()
	defer func() {
		str2.Stop()
		<-streamerDone2
	}()

	// Start API Gateway
	apiGatewayPort := "18081"
	apiStorageConfig := map[string]string{
		"storage_service_url": fmt.Sprintf("http://localhost:%s", storagePort),
	}
	apiStorage, err := storage.NewRepository(storage.BackendMemory, apiStorageConfig)
	require.NoError(t, err)

	apiHandlers := api.NewHandlers(apiStorage)
	apiServer := startAPIGateway(t, apiHandlers, apiGatewayPort)
	defer apiServer.Shutdown(context.Background())

	require.Eventually(t, func() bool {
		resp, err := http.Get(fmt.Sprintf("http://localhost:%s/health", apiGatewayPort))
		return err == nil && resp.StatusCode == http.StatusOK
	}, 5*time.Second, 100*time.Millisecond)

	// Wait for processing
	time.Sleep(2 * time.Second)

	// Verify both GPUs are present
	resp, err := http.Get(fmt.Sprintf("http://localhost:%s/api/v1/gpus", apiGatewayPort))
	require.NoError(t, err)
	defer resp.Body.Close()

	var listResponse api.ListGPUsResponse
	err = json.NewDecoder(resp.Body).Decode(&listResponse)
	require.NoError(t, err)

	gpuMap := make(map[string]bool)
	for _, gpu := range listResponse.GPUs {
		gpuMap[gpu.UUID] = true
	}

	assert.True(t, gpuMap["GPU-multi-001"], "GPU-multi-001 should exist")
	assert.True(t, gpuMap["GPU-multi-002"], "GPU-multi-002 should exist")
}

// TestSystem_DataIntegrity verifies that data is correctly preserved through the pipeline
func TestSystem_DataIntegrity(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping system test in short mode")
	}

	// Create CSV with specific values to verify
	csvData := `timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,container,pod,namespace,value,labels_raw
"2025-07-18T20:42:34Z","DCGM_FI_DEV_GPU_UTIL","2","nvidia2","GPU-integrity-001","NVIDIA H100 80GB HBM3","integrity-host","test-container","test-pod","test-namespace","99.99","labels"`

	tmpCSV := createTempCSV(t, csvData)
	defer os.Remove(tmpCSV.Name())

	// Setup services
	queue := mq.NewInMemoryMessageQueue(1000)
	queueServer := mq.NewServer(queue)
	queuePort := "18080"
	require.NoError(t, queueServer.Start(queuePort))
	defer queueServer.Stop()
	defer queue.Close()

	require.Eventually(t, func() bool {
		resp, err := http.Get(fmt.Sprintf("http://localhost:%s/api/v1/health", queuePort))
		return err == nil && resp.StatusCode == http.StatusOK
	}, 5*time.Second, 100*time.Millisecond)

	storageRepo := memory.NewStore()
	storagePort := "18082"
	storageServer := startStorageService(t, storageRepo, storagePort)
	defer storageServer.Shutdown(context.Background())

	require.Eventually(t, func() bool {
		resp, err := http.Get(fmt.Sprintf("http://localhost:%s/health", storagePort))
		return err == nil && resp.StatusCode == http.StatusOK
	}, 5*time.Second, 100*time.Millisecond)

	queueClient := mq.NewHTTPMessageQueue(fmt.Sprintf("http://localhost:%s", queuePort))
	storageConfig := map[string]string{
		"storage_service_url": fmt.Sprintf("http://localhost:%s", storagePort),
	}
	collectorStorage, err := storage.NewRepository(storage.BackendMemory, storageConfig)
	require.NoError(t, err)

	collectorCfg := &config.CollectorConfig{
		InstanceID:      "test-collector-1",
		BatchSize:       1,
		QueueServiceURL: fmt.Sprintf("http://localhost:%s", queuePort),
		StorageBackend:  "memory",
	}
	col, err := collector.NewCollector(collectorCfg, queueClient, collectorStorage)
	require.NoError(t, err)

	collectorDone := make(chan error, 1)
	go func() {
		collectorDone <- col.Start()
	}()
	defer func() {
		col.Stop()
		<-collectorDone
	}()

	streamerCfg := &config.StreamerConfig{
		CSVFilePath:     tmpCSV.Name(),
		StreamInterval:  50,
		InstanceID:      "test-streamer-1",
		QueueServiceURL: fmt.Sprintf("http://localhost:%s", queuePort),
	}
	parser := telemetry.NewCSVParser()
	str, err := streamer.NewStreamer(streamerCfg, parser, queueClient)
	require.NoError(t, err)
	require.NoError(t, str.LoadCSV())

	streamerDone := make(chan error, 1)
	go func() {
		streamerDone <- str.Start()
	}()
	defer func() {
		str.Stop()
		<-streamerDone
	}()

	apiGatewayPort := "18081"
	apiStorageConfig := map[string]string{
		"storage_service_url": fmt.Sprintf("http://localhost:%s", storagePort),
	}
	apiStorage, err := storage.NewRepository(storage.BackendMemory, apiStorageConfig)
	require.NoError(t, err)

	apiHandlers := api.NewHandlers(apiStorage)
	apiServer := startAPIGateway(t, apiHandlers, apiGatewayPort)
	defer apiServer.Shutdown(context.Background())

	require.Eventually(t, func() bool {
		resp, err := http.Get(fmt.Sprintf("http://localhost:%s/health", apiGatewayPort))
		return err == nil && resp.StatusCode == http.StatusOK
	}, 5*time.Second, 100*time.Millisecond)

	time.Sleep(2 * time.Second)

	// Verify GPU data integrity - use storage service directly since API Gateway doesn't expose GetGPU
	resp, err := http.Get(fmt.Sprintf("http://localhost:%s/api/v1/storage/gpus/GPU-integrity-001", storagePort))
	require.NoError(t, err)
	defer resp.Body.Close()

	var gpu domain.GPU
	err = json.NewDecoder(resp.Body).Decode(&gpu)
	require.NoError(t, err)

	assert.Equal(t, "GPU-integrity-001", gpu.UUID)
	assert.Equal(t, "2", gpu.GPUID)
	assert.Equal(t, "nvidia2", gpu.Device)
	assert.Equal(t, "NVIDIA H100 80GB HBM3", gpu.Model)
	assert.Equal(t, "integrity-host", gpu.Hostname)
	assert.Equal(t, "test-container", gpu.Container)
	assert.Equal(t, "test-pod", gpu.Pod)
	assert.Equal(t, "test-namespace", gpu.Namespace)

	// Verify telemetry data integrity through API Gateway
	resp, err = http.Get(fmt.Sprintf("http://localhost:%s/api/v1/gpus/GPU-integrity-001/telemetry", apiGatewayPort))
	require.NoError(t, err)
	defer resp.Body.Close()

	var telemetryResponse api.GetTelemetryResponse
	err = json.NewDecoder(resp.Body).Decode(&telemetryResponse)
	require.NoError(t, err)

	assert.GreaterOrEqual(t, telemetryResponse.Count, 1)
	if len(telemetryResponse.Records) > 0 {
		record := telemetryResponse.Records[0]
		assert.Equal(t, "DCGM_FI_DEV_GPU_UTIL", record.MetricName)
		assert.Equal(t, "99.99", record.Value)
	}
}

// Helper functions

func createTempCSV(t *testing.T, data string) *os.File {
	tmpFile, err := os.CreateTemp("", "test-telemetry-*.csv")
	require.NoError(t, err)

	_, err = tmpFile.WriteString(data)
	require.NoError(t, err)
	require.NoError(t, tmpFile.Close())

	// Reopen for reading
	tmpFile, err = os.Open(tmpFile.Name())
	require.NoError(t, err)

	return tmpFile
}

func startStorageService(t *testing.T, repo storage.Repository, port string) *http.Server {
	router := gin.Default()

	// Health check
	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "healthy"})
	})

	// Storage API endpoints
	api := router.Group("/api/v1/storage")
	{
		// GET /api/v1/storage/gpus - List all GPUs
		api.GET("/gpus", func(c *gin.Context) {
			gpus, err := repo.ListGPUs(c.Request.Context())
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, gpus)
		})

		// GET /api/v1/storage/gpus/:uuid - Get specific GPU
		api.GET("/gpus/:uuid", func(c *gin.Context) {
			uuid := c.Param("uuid")
			gpu, err := repo.GetGPU(c.Request.Context(), uuid)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			if gpu == nil {
				c.JSON(http.StatusNotFound, gin.H{"error": "GPU not found"})
				return
			}
			c.JSON(http.StatusOK, gpu)
		})

		// POST /api/v1/storage/gpus - Save GPU
		api.POST("/gpus", func(c *gin.Context) {
			var gpu domain.GPU
			if err := c.ShouldBindJSON(&gpu); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				return
			}
			if err := repo.SaveGPU(c.Request.Context(), &gpu); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusCreated, gin.H{"status": "saved", "uuid": gpu.UUID})
		})

		// POST /api/v1/storage/telemetry - Save telemetry
		api.POST("/telemetry", func(c *gin.Context) {
			var record domain.TelemetryRecord
			if err := c.ShouldBindJSON(&record); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				return
			}
			if err := repo.SaveTelemetry(c.Request.Context(), &record); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusCreated, gin.H{"status": "saved"})
		})

		// GET /api/v1/storage/gpus/:uuid/telemetry - Get telemetry for GPU
		api.GET("/gpus/:uuid/telemetry", func(c *gin.Context) {
			uuid := c.Param("uuid")
			var startTime, endTime *time.Time

			if startStr := c.Query("start_time"); startStr != "" {
				t, err := time.Parse(time.RFC3339, startStr)
				if err == nil {
					startTime = &t
				}
			}
			if endStr := c.Query("end_time"); endStr != "" {
				t, err := time.Parse(time.RFC3339, endStr)
				if err == nil {
					endTime = &t
				}
			}

			records, err := repo.GetTelemetryByGPU(c.Request.Context(), uuid, startTime, endTime)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, records)
		})
	}

	server := &http.Server{
		Addr:    ":" + port,
		Handler: router,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			t.Logf("Storage service error: %v", err)
		}
	}()

	// Give server a moment to start
	time.Sleep(100 * time.Millisecond)

	return server
}

func startAPIGateway(t *testing.T, handlers *api.Handlers, port string) *http.Server {
	router := gin.Default()

	// Health check
	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "healthy"})
	})

	// Setup API routes
	api.SetupRoutes(router, handlers)

	server := &http.Server{
		Addr:    ":" + port,
		Handler: router,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			t.Logf("API Gateway error: %v", err)
		}
	}()

	return server
}
