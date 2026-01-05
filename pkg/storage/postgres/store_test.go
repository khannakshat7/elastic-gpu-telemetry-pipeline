package postgres

import (
	"context"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// getTestConnectionString returns a PostgreSQL connection string for testing.
// It reads from environment variables or uses defaults suitable for local testing.
func getTestConnectionString() string {
	host := os.Getenv("POSTGRES_TEST_HOST")
	if host == "" {
		host = "localhost"
	}

	port := os.Getenv("POSTGRES_TEST_PORT")
	if port == "" {
		port = "5432"
	}

	user := os.Getenv("POSTGRES_TEST_USER")
	if user == "" {
		user = "postgres"
	}

	password := os.Getenv("POSTGRES_TEST_PASSWORD")
	if password == "" {
		password = "postgres"
	}

	dbname := os.Getenv("POSTGRES_TEST_DB")
	if dbname == "" {
		dbname = "gpu_telemetry_test"
	}

	sslmode := os.Getenv("POSTGRES_TEST_SSLMODE")
	if sslmode == "" {
		sslmode = "disable"
	}

	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		host, port, user, password, dbname, sslmode)
}

// setupTestStore creates a test store and ensures the database is clean.
func setupTestStore(t *testing.T) *Store {
	connectionString := getTestConnectionString()
	store, err := NewStore(connectionString)
	if err != nil {
		t.Skipf("Skipping PostgreSQL tests: failed to connect to database: %v", err)
		return nil
	}

	// Clean up test data
	ctx := context.Background()
	store.db.ExecContext(ctx, "TRUNCATE TABLE telemetry CASCADE")
	store.db.ExecContext(ctx, "TRUNCATE TABLE gpus CASCADE")

	return store
}

// teardownTestStore cleans up test data.
func teardownTestStore(t *testing.T, store *Store) {
	if store != nil {
		ctx := context.Background()
		store.db.ExecContext(ctx, "TRUNCATE TABLE telemetry CASCADE")
		store.db.ExecContext(ctx, "TRUNCATE TABLE gpus CASCADE")
		store.Close()
	}
}

func TestStore_SaveGPU(t *testing.T) {
	store := setupTestStore(t)
	if store == nil {
		return
	}
	defer teardownTestStore(t, store)

	ctx := context.Background()

	gpu := &domain.GPU{
		UUID:      "GPU-123",
		GPUID:     "0",
		Device:    "nvidia0",
		Model:     "NVIDIA H100 80GB HBM3",
		Hostname:  "host-1",
		Container: "container-1",
		Pod:       "pod-1",
		Namespace: "namespace-1",
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
	assert.Equal(t, gpu.Container, retrieved.Container)
	assert.Equal(t, gpu.Pod, retrieved.Pod)
	assert.Equal(t, gpu.Namespace, retrieved.Namespace)
}

func TestStore_SaveGPU_Update(t *testing.T) {
	store := setupTestStore(t)
	if store == nil {
		return
	}
	defer teardownTestStore(t, store)

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
		GPUID:    "1",           // Changed
		Device:   "nvidia1",     // Changed
		Model:    "NVIDIA A100", // Changed
		Hostname: "host-2",      // Changed
	}

	err = store.SaveGPU(ctx, gpu2)
	require.NoError(t, err)

	// Verify update
	retrieved, err := store.GetGPU(ctx, "GPU-123")
	require.NoError(t, err)
	assert.Equal(t, "1", retrieved.GPUID)
	assert.Equal(t, "nvidia1", retrieved.Device)
	assert.Equal(t, "NVIDIA A100", retrieved.Model)
	assert.Equal(t, "host-2", retrieved.Hostname)
}

func TestStore_SaveGPU_Invalid(t *testing.T) {
	store := setupTestStore(t)
	if store == nil {
		return
	}
	defer teardownTestStore(t, store)

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
	store := setupTestStore(t)
	if store == nil {
		return
	}
	defer teardownTestStore(t, store)

	ctx := context.Background()

	gpu, err := store.GetGPU(ctx, "GPU-NOT-FOUND")
	require.NoError(t, err)
	assert.Nil(t, gpu)
}

func TestStore_GetGPU_InvalidUUID(t *testing.T) {
	store := setupTestStore(t)
	if store == nil {
		return
	}
	defer teardownTestStore(t, store)

	ctx := context.Background()

	gpu, err := store.GetGPU(ctx, "")
	assert.Error(t, err)
	assert.Nil(t, gpu)
	assert.Equal(t, ErrInvalidGPUUUID, err)
}

func TestStore_ListGPUs_Empty(t *testing.T) {
	store := setupTestStore(t)
	if store == nil {
		return
	}
	defer teardownTestStore(t, store)

	ctx := context.Background()

	gpus, err := store.ListGPUs(ctx)
	require.NoError(t, err)
	assert.Empty(t, gpus)
}

func TestStore_ListGPUs_OnlyWithTelemetry(t *testing.T) {
	store := setupTestStore(t)
	if store == nil {
		return
	}
	defer teardownTestStore(t, store)

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
	store := setupTestStore(t)
	if store == nil {
		return
	}
	defer teardownTestStore(t, store)

	ctx := context.Background()

	// First, save the GPU
	gpu := &domain.GPU{UUID: "GPU-123", GPUID: "0", Device: "nvidia0", Model: "H100", Hostname: "host-1"}
	require.NoError(t, store.SaveGPU(ctx, gpu))

	record := &domain.TelemetryRecord{
		GPUUUID:       "GPU-123",
		MetricName:    "DCGM_FI_DEV_GPU_UTIL",
		Value:         "100",
		IngestionTime: time.Now(),
		Hostname:      "host-1",
		ModelName:     "H100",
		Container:     "container-1",
		Pod:           "pod-1",
		Namespace:     "namespace-1",
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
	assert.Equal(t, record.Hostname, results[0].Hostname)
	assert.Equal(t, record.ModelName, results[0].ModelName)
}

func TestStore_SaveTelemetry_Invalid(t *testing.T) {
	store := setupTestStore(t)
	if store == nil {
		return
	}
	defer teardownTestStore(t, store)

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
	store := setupTestStore(t)
	if store == nil {
		return
	}
	defer teardownTestStore(t, store)

	ctx := context.Background()

	results, err := store.GetTelemetryByGPU(ctx, "GPU-NOT-FOUND", nil, nil)
	require.NoError(t, err)
	assert.Empty(t, results)
}

func TestStore_GetTelemetryByGPU_OrderedByTime(t *testing.T) {
	store := setupTestStore(t)
	if store == nil {
		return
	}
	defer teardownTestStore(t, store)

	ctx := context.Background()

	// First, save the GPU
	gpu := &domain.GPU{UUID: "GPU-123", GPUID: "0", Device: "nvidia0", Model: "H100", Hostname: "host-1"}
	require.NoError(t, store.SaveGPU(ctx, gpu))

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
	assert.Equal(t, baseTime.Truncate(time.Second), results[0].IngestionTime.Truncate(time.Second))
	assert.Equal(t, baseTime.Add(1*time.Hour).Truncate(time.Second), results[1].IngestionTime.Truncate(time.Second))
	assert.Equal(t, baseTime.Add(2*time.Hour).Truncate(time.Second), results[2].IngestionTime.Truncate(time.Second))
	assert.Equal(t, baseTime.Add(3*time.Hour).Truncate(time.Second), results[3].IngestionTime.Truncate(time.Second))
}

func TestStore_GetTelemetryByGPU_TimeFilter_StartOnly(t *testing.T) {
	store := setupTestStore(t)
	if store == nil {
		return
	}
	defer teardownTestStore(t, store)

	ctx := context.Background()

	// First, save the GPU
	gpu := &domain.GPU{UUID: "GPU-123", GPUID: "0", Device: "nvidia0", Model: "H100", Hostname: "host-1"}
	require.NoError(t, store.SaveGPU(ctx, gpu))

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
	store := setupTestStore(t)
	if store == nil {
		return
	}
	defer teardownTestStore(t, store)

	ctx := context.Background()

	// First, save the GPU
	gpu := &domain.GPU{UUID: "GPU-123", GPUID: "0", Device: "nvidia0", Model: "H100", Hostname: "host-1"}
	require.NoError(t, store.SaveGPU(ctx, gpu))

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
	store := setupTestStore(t)
	if store == nil {
		return
	}
	defer teardownTestStore(t, store)

	ctx := context.Background()

	// First, save the GPU
	gpu := &domain.GPU{UUID: "GPU-123", GPUID: "0", Device: "nvidia0", Model: "H100", Hostname: "host-1"}
	require.NoError(t, store.SaveGPU(ctx, gpu))

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
	store := setupTestStore(t)
	if store == nil {
		return
	}
	defer teardownTestStore(t, store)

	ctx := context.Background()

	// Save GPUs first
	gpu1 := &domain.GPU{UUID: "GPU-1", GPUID: "0", Device: "nvidia0", Model: "H100", Hostname: "host-1"}
	gpu2 := &domain.GPU{UUID: "GPU-2", GPUID: "1", Device: "nvidia1", Model: "H100", Hostname: "host-1"}
	gpu3 := &domain.GPU{UUID: "GPU-3", GPUID: "0", Device: "nvidia0", Model: "H100", Hostname: "host-2"}
	require.NoError(t, store.SaveGPU(ctx, gpu1))
	require.NoError(t, store.SaveGPU(ctx, gpu2))
	require.NoError(t, store.SaveGPU(ctx, gpu3))

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

func TestStore_GetTelemetryByGPU_InvalidUUID(t *testing.T) {
	store := setupTestStore(t)
	if store == nil {
		return
	}
	defer teardownTestStore(t, store)

	ctx := context.Background()

	results, err := store.GetTelemetryByGPU(ctx, "", nil, nil)
	assert.Error(t, err)
	assert.Nil(t, results)
	assert.Equal(t, ErrInvalidGPUUUID, err)
}

func TestStore_SaveTelemetry_Duplicate(t *testing.T) {
	store := setupTestStore(t)
	if store == nil {
		return
	}
	defer teardownTestStore(t, store)

	ctx := context.Background()

	// First, save the GPU
	gpu := &domain.GPU{UUID: "GPU-123", GPUID: "0", Device: "nvidia0", Model: "H100", Hostname: "host-1"}
	require.NoError(t, store.SaveGPU(ctx, gpu))

	// Save the same telemetry record twice (should update, not error)
	record := &domain.TelemetryRecord{
		GPUUUID:       "GPU-123",
		MetricName:    "DCGM_FI_DEV_GPU_UTIL",
		Value:         "100",
		IngestionTime: time.Now().Truncate(time.Second), // Truncate to avoid microsecond differences
	}

	err := store.SaveTelemetry(ctx, record)
	require.NoError(t, err)

	// Save again with different value (should update)
	record.Value = "200"
	err = store.SaveTelemetry(ctx, record)
	require.NoError(t, err)

	// Verify only one record exists with updated value
	results, err := store.GetTelemetryByGPU(ctx, "GPU-123", nil, nil)
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "200", results[0].Value)
}

func TestStore_Close(t *testing.T) {
	store := setupTestStore(t)
	if store == nil {
		return
	}

	err := store.Close()
	require.NoError(t, err)

	// Closing again should not error
	err = store.Close()
	require.NoError(t, err)
}

func TestNewStore_InvalidConnection(t *testing.T) {
	_, err := NewStore("host=invalid port=5432 user=test password=test dbname=test sslmode=disable")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to")
}

func TestNewStore_InvalidConnectionString(t *testing.T) {
	// Test with malformed connection string
	_, err := NewStore("invalid connection string format")
	assert.Error(t, err)
}

func TestNewStore_EmptyConnectionString(t *testing.T) {
	_, err := NewStore("")
	assert.Error(t, err)
}

func TestStore_Close_NilDB(t *testing.T) {
	// Test Close with nil db (should not panic)
	store := &Store{db: nil}
	err := store.Close()
	assert.NoError(t, err)
}

func TestStore_Close_MultipleCalls(t *testing.T) {
	// Test closing multiple times (should not panic)
	store := &Store{db: nil}
	err1 := store.Close()
	err2 := store.Close()
	assert.NoError(t, err1)
	assert.NoError(t, err2)
}
