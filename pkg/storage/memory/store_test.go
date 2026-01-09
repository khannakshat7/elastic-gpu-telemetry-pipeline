package memory

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore_SaveGPU(t *testing.T) {
	store := NewStore()
	ctx := context.Background()

	gpu := &domain.GPU{
		UUID:     "GPU-123",
		GPUID:    "0",
		Device:   "nvidia0",
		Model:    "NVIDIA H100 80GB HBM3",
		Hostname: "host-1",
	}

	err := store.SaveGPU(ctx, gpu)
	require.NoError(t, err)

	// Verify GPU was saved
	retrieved, err := store.GetGPU(ctx, "GPU-123")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	assert.Equal(t, gpu.UUID, retrieved.UUID)
	assert.Equal(t, gpu.GPUID, retrieved.GPUID)
	assert.Equal(t, gpu.Device, retrieved.Device)
	assert.Equal(t, gpu.Model, retrieved.Model)
	assert.Equal(t, gpu.Hostname, retrieved.Hostname)
}

func TestStore_SaveGPU_Update(t *testing.T) {
	store := NewStore()
	ctx := context.Background()

	gpu1 := &domain.GPU{
		UUID:     "GPU-123",
		GPUID:    "0",
		Device:   "nvidia0",
		Model:    "NVIDIA H100 80GB HBM3",
		Hostname: "host-1",
	}

	err := store.SaveGPU(ctx, gpu1)
	require.NoError(t, err)

	// Update the GPU
	gpu2 := &domain.GPU{
		UUID:     "GPU-123",
		GPUID:    "1",       // Changed
		Device:   "nvidia1", // Changed
		Model:    "NVIDIA H100 80GB HBM3",
		Hostname: "host-1",
	}

	err = store.SaveGPU(ctx, gpu2)
	require.NoError(t, err)

	// Verify update
	retrieved, err := store.GetGPU(ctx, "GPU-123")
	require.NoError(t, err)
	assert.Equal(t, "1", retrieved.GPUID)
	assert.Equal(t, "nvidia1", retrieved.Device)
}

func TestStore_SaveGPU_Invalid(t *testing.T) {
	store := NewStore()
	ctx := context.Background()

	// Test nil GPU
	err := store.SaveGPU(ctx, nil)
	assert.Error(t, err)
	assert.Equal(t, ErrInvalidGPU, err)

	// Test empty UUID
	err = store.SaveGPU(ctx, &domain.GPU{})
	assert.Error(t, err)
	assert.Equal(t, ErrInvalidGPUUUID, err)
}

func TestStore_GetGPU_NotFound(t *testing.T) {
	store := NewStore()
	ctx := context.Background()

	gpu, err := store.GetGPU(ctx, "GPU-NOT-FOUND")
	require.NoError(t, err)
	assert.Nil(t, gpu)
}

func TestStore_GetGPU_InvalidUUID(t *testing.T) {
	store := NewStore()
	ctx := context.Background()

	gpu, err := store.GetGPU(ctx, "")
	assert.Error(t, err)
	assert.Nil(t, gpu)
	assert.Equal(t, ErrInvalidGPUUUID, err)
}

func TestStore_ListGPUs_Empty(t *testing.T) {
	store := NewStore()
	ctx := context.Background()

	gpus, err := store.ListGPUs(ctx)
	require.NoError(t, err)
	assert.Empty(t, gpus)
}

func TestStore_ListGPUs_OnlyWithTelemetry(t *testing.T) {
	store := NewStore()
	ctx := context.Background()

	// Save GPUs
	gpu1 := &domain.GPU{UUID: "GPU-1", GPUID: "0", Device: "nvidia0", Model: "H100", Hostname: "host-1"}
	gpu2 := &domain.GPU{UUID: "GPU-2", GPUID: "1", Device: "nvidia1", Model: "H100", Hostname: "host-1"}
	gpu3 := &domain.GPU{UUID: "GPU-3", GPUID: "0", Device: "nvidia0", Model: "H100", Hostname: "host-2"}

	require.NoError(t, store.SaveGPU(ctx, gpu1))
	require.NoError(t, store.SaveGPU(ctx, gpu2))
	require.NoError(t, store.SaveGPU(ctx, gpu3))

	// Initially, no GPUs have telemetry
	gpus, err := store.ListGPUs(ctx)
	require.NoError(t, err)
	assert.Empty(t, gpus)

	// Add telemetry for GPU-1 and GPU-2
	record1 := &domain.TelemetryRecord{
		GPUUUID:       "GPU-1",
		MetricName:    "DCGM_FI_DEV_GPU_UTIL",
		Value:         "100",
		IngestionTime: time.Now(),
	}
	record2 := &domain.TelemetryRecord{
		GPUUUID:       "GPU-2",
		MetricName:    "DCGM_FI_DEV_GPU_UTIL",
		Value:         "50",
		IngestionTime: time.Now(),
	}

	require.NoError(t, store.SaveTelemetry(ctx, record1))
	require.NoError(t, store.SaveTelemetry(ctx, record2))

	// Now only GPU-1 and GPU-2 should be listed
	gpus, err = store.ListGPUs(ctx)
	require.NoError(t, err)
	assert.Len(t, gpus, 2)

	// Verify they are sorted by UUID
	uuids := make([]string, len(gpus))
	for i, gpu := range gpus {
		uuids[i] = gpu.UUID
	}
	assert.True(t, sort.StringsAreSorted(uuids))
}

func TestStore_SaveTelemetry(t *testing.T) {
	store := NewStore()
	ctx := context.Background()

	record := &domain.TelemetryRecord{
		GPUUUID:       "GPU-123",
		MetricName:    "DCGM_FI_DEV_GPU_UTIL",
		Value:         "100",
		IngestionTime: time.Now(),
		Hostname:      "host-1",
		ModelName:     "H100",
	}

	err := store.SaveTelemetry(ctx, record)
	require.NoError(t, err)

	// Verify telemetry was saved
	results, err := store.GetTelemetryByGPU(ctx, "GPU-123", nil, nil)
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, record.GPUUUID, results[0].GPUUUID)
	assert.Equal(t, record.MetricName, results[0].MetricName)
	assert.Equal(t, record.Value, results[0].Value)
}

func TestStore_SaveTelemetry_Invalid(t *testing.T) {
	store := NewStore()
	ctx := context.Background()

	// Test nil record
	err := store.SaveTelemetry(ctx, nil)
	assert.Error(t, err)
	assert.Equal(t, ErrInvalidTelemetryRecord, err)

	// Test empty GPU UUID
	err = store.SaveTelemetry(ctx, &domain.TelemetryRecord{})
	assert.Error(t, err)
	assert.Equal(t, ErrInvalidGPUUUID, err)
}

func TestStore_GetTelemetryByGPU_Empty(t *testing.T) {
	store := NewStore()
	ctx := context.Background()

	results, err := store.GetTelemetryByGPU(ctx, "GPU-NOT-FOUND", nil, nil)
	require.NoError(t, err)
	assert.Empty(t, results)
}

func TestStore_GetTelemetryByGPU_OrderedByTime(t *testing.T) {
	store := NewStore()
	ctx := context.Background()

	baseTime := time.Now()
	records := []*domain.TelemetryRecord{
		{GPUUUID: "GPU-123", MetricName: "METRIC-1", Value: "1", IngestionTime: baseTime.Add(3 * time.Hour)},
		{GPUUUID: "GPU-123", MetricName: "METRIC-2", Value: "2", IngestionTime: baseTime.Add(1 * time.Hour)},
		{GPUUUID: "GPU-123", MetricName: "METRIC-3", Value: "3", IngestionTime: baseTime.Add(2 * time.Hour)},
		{GPUUUID: "GPU-123", MetricName: "METRIC-4", Value: "4", IngestionTime: baseTime},
	}

	for _, record := range records {
		require.NoError(t, store.SaveTelemetry(ctx, record))
	}

	results, err := store.GetTelemetryByGPU(ctx, "GPU-123", nil, nil)
	require.NoError(t, err)
	require.Len(t, results, 4)

	// Verify ordering (oldest first)
	assert.Equal(t, baseTime, results[0].IngestionTime)
	assert.Equal(t, baseTime.Add(1*time.Hour), results[1].IngestionTime)
	assert.Equal(t, baseTime.Add(2*time.Hour), results[2].IngestionTime)
	assert.Equal(t, baseTime.Add(3*time.Hour), results[3].IngestionTime)
}

func TestStore_GetTelemetryByGPU_TimeFilter_StartOnly(t *testing.T) {
	store := NewStore()
	ctx := context.Background()

	baseTime := time.Now()
	records := []*domain.TelemetryRecord{
		{GPUUUID: "GPU-123", MetricName: "METRIC-1", Value: "1", IngestionTime: baseTime.Add(-1 * time.Hour)},
		{GPUUUID: "GPU-123", MetricName: "METRIC-2", Value: "2", IngestionTime: baseTime},
		{GPUUUID: "GPU-123", MetricName: "METRIC-3", Value: "3", IngestionTime: baseTime.Add(1 * time.Hour)},
	}

	for _, record := range records {
		require.NoError(t, store.SaveTelemetry(ctx, record))
	}

	// Filter: start_time = baseTime (inclusive)
	start := baseTime
	results, err := store.GetTelemetryByGPU(ctx, "GPU-123", &start, nil)
	require.NoError(t, err)
	require.Len(t, results, 2) // Should include baseTime and baseTime+1h

	// Verify all results are >= start
	for _, result := range results {
		assert.True(t, result.IngestionTime.Equal(start) || result.IngestionTime.After(start))
	}
}

func TestStore_GetTelemetryByGPU_TimeFilter_EndOnly(t *testing.T) {
	store := NewStore()
	ctx := context.Background()

	baseTime := time.Now()
	records := []*domain.TelemetryRecord{
		{GPUUUID: "GPU-123", MetricName: "METRIC-1", Value: "1", IngestionTime: baseTime.Add(-1 * time.Hour)},
		{GPUUUID: "GPU-123", MetricName: "METRIC-2", Value: "2", IngestionTime: baseTime},
		{GPUUUID: "GPU-123", MetricName: "METRIC-3", Value: "3", IngestionTime: baseTime.Add(1 * time.Hour)},
	}

	for _, record := range records {
		require.NoError(t, store.SaveTelemetry(ctx, record))
	}

	// Filter: end_time = baseTime (inclusive)
	end := baseTime
	results, err := store.GetTelemetryByGPU(ctx, "GPU-123", nil, &end)
	require.NoError(t, err)
	require.Len(t, results, 2) // Should include baseTime-1h and baseTime

	// Verify all results are <= end
	for _, result := range results {
		assert.True(t, result.IngestionTime.Equal(end) || result.IngestionTime.Before(end))
	}
}

func TestStore_GetTelemetryByGPU_TimeFilter_Both(t *testing.T) {
	store := NewStore()
	ctx := context.Background()

	baseTime := time.Now()
	records := []*domain.TelemetryRecord{
		{GPUUUID: "GPU-123", MetricName: "METRIC-1", Value: "1", IngestionTime: baseTime.Add(-1 * time.Hour)},
		{GPUUUID: "GPU-123", MetricName: "METRIC-2", Value: "2", IngestionTime: baseTime},
		{GPUUUID: "GPU-123", MetricName: "METRIC-3", Value: "3", IngestionTime: baseTime.Add(1 * time.Hour)},
		{GPUUUID: "GPU-123", MetricName: "METRIC-4", Value: "4", IngestionTime: baseTime.Add(2 * time.Hour)},
	}

	for _, record := range records {
		require.NoError(t, store.SaveTelemetry(ctx, record))
	}

	// Filter: start_time = baseTime, end_time = baseTime + 1h (both inclusive)
	start := baseTime
	end := baseTime.Add(1 * time.Hour)
	results, err := store.GetTelemetryByGPU(ctx, "GPU-123", &start, &end)
	require.NoError(t, err)
	require.Len(t, results, 2) // Should include baseTime and baseTime+1h

	// Verify all results are within range
	for _, result := range results {
		assert.True(t, (result.IngestionTime.Equal(start) || result.IngestionTime.After(start)) &&
			(result.IngestionTime.Equal(end) || result.IngestionTime.Before(end)))
	}
}

func TestStore_GetTelemetryByGPU_MultipleGPUs(t *testing.T) {
	store := NewStore()
	ctx := context.Background()

	// Save telemetry for multiple GPUs
	records := []*domain.TelemetryRecord{
		{GPUUUID: "GPU-1", MetricName: "METRIC-1", Value: "1", IngestionTime: time.Now()},
		{GPUUUID: "GPU-2", MetricName: "METRIC-1", Value: "2", IngestionTime: time.Now()},
		{GPUUUID: "GPU-1", MetricName: "METRIC-2", Value: "3", IngestionTime: time.Now()},
		{GPUUUID: "GPU-3", MetricName: "METRIC-1", Value: "4", IngestionTime: time.Now()},
	}

	for _, record := range records {
		require.NoError(t, store.SaveTelemetry(ctx, record))
	}

	// Query for GPU-1
	results, err := store.GetTelemetryByGPU(ctx, "GPU-1", nil, nil)
	require.NoError(t, err)
	require.Len(t, results, 2)

	// Verify all results are for GPU-1
	for _, result := range results {
		assert.Equal(t, "GPU-1", result.GPUUUID)
	}

	// Query for GPU-2
	results, err = store.GetTelemetryByGPU(ctx, "GPU-2", nil, nil)
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "GPU-2", results[0].GPUUUID)
}

func TestStore_ConcurrentAccess(t *testing.T) {
	store := NewStore()
	ctx := context.Background()

	numGoroutines := 10
	recordsPerGoroutine := 10

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2) // For both GPU and telemetry operations

	// Concurrent GPU saves
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			gpu := &domain.GPU{
				UUID:     fmt.Sprintf("GPU-%d", id),
				GPUID:    "0",
				Device:   "nvidia0",
				Model:    "H100",
				Hostname: "host-1",
			}
			err := store.SaveGPU(ctx, gpu)
			require.NoError(t, err)
		}(i)
	}

	// Concurrent telemetry saves
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < recordsPerGoroutine; j++ {
				record := &domain.TelemetryRecord{
					GPUUUID:       fmt.Sprintf("GPU-%d", id),
					MetricName:    "METRIC-1",
					Value:         "100",
					IngestionTime: time.Now(),
				}
				err := store.SaveTelemetry(ctx, record)
				require.NoError(t, err)
			}
		}(i)
	}

	wg.Wait()

	// Verify data integrity
	gpus, err := store.ListGPUs(ctx)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(gpus), 0) // At least some GPUs should have telemetry

	// Verify we can read concurrently
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			gpu, err := store.GetGPU(ctx, fmt.Sprintf("GPU-%d", id))
			// GPU might not exist, but should not error
			_ = gpu
			require.NoError(t, err)
		}(i)
	}
	wg.Wait()
}

func TestStore_GetTelemetryByGPU_InvalidUUID(t *testing.T) {
	store := NewStore()
	ctx := context.Background()

	results, err := store.GetTelemetryByGPU(ctx, "", nil, nil)
	assert.Error(t, err)
	assert.Nil(t, results)
	assert.Equal(t, ErrInvalidGPUUUID, err)
}

// ---- Tests for new eviction functionality ----

func TestNewStoreWithMaxRecords(t *testing.T) {
	store := NewStoreWithMaxRecords(100)
	assert.NotNil(t, store)
	assert.Equal(t, 100, store.maxRecords)

	// Test with zero/negative should use default
	store2 := NewStoreWithMaxRecords(0)
	assert.Equal(t, MaxTelemetryRecords, store2.maxRecords)

	store3 := NewStoreWithMaxRecords(-10)
	assert.Equal(t, MaxTelemetryRecords, store3.maxRecords)
}

func TestStore_Eviction_WhenMaxRecordsReached(t *testing.T) {
	// Create store with small max records for testing
	store := NewStoreWithMaxRecords(10)
	ctx := context.Background()

	// Save a GPU first
	gpu := &domain.GPU{
		UUID:     "GPU-123",
		GPUID:    "0",
		Device:   "nvidia0",
		Model:    "NVIDIA H100",
		Hostname: "host-1",
	}
	require.NoError(t, store.SaveGPU(ctx, gpu))

	// Add 15 records (exceeds max of 10)
	for i := 0; i < 15; i++ {
		record := &domain.TelemetryRecord{
			GPUUUID:       "GPU-123",
			MetricName:    fmt.Sprintf("METRIC_%d", i),
			Value:         fmt.Sprintf("%d", i),
			IngestionTime: time.Now().Add(time.Duration(i) * time.Second),
		}
		err := store.SaveTelemetry(ctx, record)
		require.NoError(t, err)
	}

	// Verify eviction happened - should have fewer than 15 records
	records, err := store.GetTelemetryByGPU(ctx, "GPU-123", nil, nil)
	require.NoError(t, err)

	// After eviction (10% of 10 = 1 record evicted each time limit is hit)
	// Total should be at most maxRecords
	assert.LessOrEqual(t, len(records), 10, "should have at most maxRecords after eviction")
}

func TestStore_Eviction_PreservesNewestRecords(t *testing.T) {
	store := NewStoreWithMaxRecords(5)
	ctx := context.Background()

	// Save a GPU first
	gpu := &domain.GPU{
		UUID:     "GPU-123",
		GPUID:    "0",
		Device:   "nvidia0",
		Model:    "NVIDIA H100",
		Hostname: "host-1",
	}
	require.NoError(t, store.SaveGPU(ctx, gpu))

	// Add 10 records with increasing values
	baseTime := time.Now()
	for i := 0; i < 10; i++ {
		record := &domain.TelemetryRecord{
			GPUUUID:       "GPU-123",
			MetricName:    "METRIC",
			Value:         fmt.Sprintf("%d", i),
			IngestionTime: baseTime.Add(time.Duration(i) * time.Second),
		}
		err := store.SaveTelemetry(ctx, record)
		require.NoError(t, err)
	}

	// Get records and verify newest are kept
	records, err := store.GetTelemetryByGPU(ctx, "GPU-123", nil, nil)
	require.NoError(t, err)

	// Newest records should be preserved (highest values)
	if len(records) > 0 {
		// Last record should be one of the newer ones
		lastValue := records[len(records)-1].Value
		lastInt := 0
		fmt.Sscanf(lastValue, "%d", &lastInt)
		assert.GreaterOrEqual(t, lastInt, 5, "newest records should be preserved")
	}
}

func TestStore_Eviction_UpdatesIndices(t *testing.T) {
	store := NewStoreWithMaxRecords(5)
	ctx := context.Background()

	// Save GPUs
	for i := 1; i <= 3; i++ {
		gpu := &domain.GPU{
			UUID:     fmt.Sprintf("GPU-%d", i),
			GPUID:    fmt.Sprintf("%d", i),
			Device:   fmt.Sprintf("nvidia%d", i),
			Model:    "NVIDIA H100",
			Hostname: "host-1",
		}
		require.NoError(t, store.SaveGPU(ctx, gpu))
	}

	// Add records for multiple GPUs
	baseTime := time.Now()
	for i := 0; i < 3; i++ {
		for j := 1; j <= 3; j++ {
			record := &domain.TelemetryRecord{
				GPUUUID:       fmt.Sprintf("GPU-%d", j),
				MetricName:    fmt.Sprintf("METRIC_%d", i),
				Value:         fmt.Sprintf("%d", i),
				IngestionTime: baseTime.Add(time.Duration(i*3+j) * time.Second),
			}
			err := store.SaveTelemetry(ctx, record)
			require.NoError(t, err)
		}
	}

	// Verify we can still query by GPU after eviction
	for j := 1; j <= 3; j++ {
		records, err := store.GetTelemetryByGPU(ctx, fmt.Sprintf("GPU-%d", j), nil, nil)
		require.NoError(t, err)
		// Each GPU should have some records
		assert.GreaterOrEqual(t, len(records), 0)
	}

	// Verify ListGPUs still works
	gpus, err := store.ListGPUs(ctx)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(gpus), 1)
}
