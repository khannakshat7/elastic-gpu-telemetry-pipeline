package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/khannakshat7/elastic-gpu-telemetry-pipeline/pkg/domain"
	_ "github.com/lib/pq"
)

// Store implements both GPURepository and TelemetryRepository interfaces
// using PostgreSQL database.
type Store struct {
	db *sql.DB
}

// NewStore creates a new PostgreSQL store.
// It expects a connection string in the format:
// "host=localhost port=5432 user=postgres password=postgres dbname=gpu_telemetry sslmode=disable"
func NewStore(connectionString string) (*Store, error) {
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	// Configure connection pool for production use
	db.SetMaxOpenConns(25)                 // Maximum number of open connections
	db.SetMaxIdleConns(10)                 // Maximum number of idle connections
	db.SetConnMaxLifetime(5 * time.Minute) // Maximum connection lifetime
	db.SetConnMaxIdleTime(1 * time.Minute) // Maximum idle time before closing

	// Test the connection
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	store := &Store{db: db}

	// Initialize database schema
	if err := store.initSchema(context.Background()); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	return store, nil
}

// Close closes the database connection
func (s *Store) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// initSchema creates the necessary tables if they don't exist
func (s *Store) initSchema(ctx context.Context) error {
	// Create GPUs table
	gpuTableSQL := `
	CREATE TABLE IF NOT EXISTS gpus (
		uuid VARCHAR(255) PRIMARY KEY,
		gpu_id VARCHAR(50) NOT NULL,
		device VARCHAR(100) NOT NULL,
		model VARCHAR(255) NOT NULL,
		hostname VARCHAR(255) NOT NULL,
		container VARCHAR(255),
		pod VARCHAR(255),
		namespace VARCHAR(255),
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);`

	if _, err := s.db.ExecContext(ctx, gpuTableSQL); err != nil {
		return fmt.Errorf("failed to create gpus table: %w", err)
	}

	// Create telemetry table
	telemetryTableSQL := `
	CREATE TABLE IF NOT EXISTS telemetry (
		id SERIAL PRIMARY KEY,
		gpu_uuid VARCHAR(255) NOT NULL,
		metric_name VARCHAR(255) NOT NULL,
		value VARCHAR(100) NOT NULL,
		ingestion_time TIMESTAMP NOT NULL,
		container VARCHAR(255),
		pod VARCHAR(255),
		namespace VARCHAR(255),
		hostname VARCHAR(255),
		model_name VARCHAR(255),
		gpu_id VARCHAR(50),
		device VARCHAR(100),
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		CONSTRAINT unique_telemetry UNIQUE (gpu_uuid, metric_name, ingestion_time),
		FOREIGN KEY (gpu_uuid) REFERENCES gpus(uuid) ON DELETE CASCADE
	);`

	if _, err := s.db.ExecContext(ctx, telemetryTableSQL); err != nil {
		return fmt.Errorf("failed to create telemetry table: %w", err)
	}

	// Create indexes for better query performance
	indexes := []string{
		"CREATE INDEX IF NOT EXISTS idx_telemetry_gpu_uuid ON telemetry(gpu_uuid);",
		"CREATE INDEX IF NOT EXISTS idx_telemetry_ingestion_time ON telemetry(ingestion_time);",
		"CREATE INDEX IF NOT EXISTS idx_telemetry_gpu_time ON telemetry(gpu_uuid, ingestion_time);",
		"CREATE INDEX IF NOT EXISTS idx_gpus_hostname ON gpus(hostname);",
	}

	for _, indexSQL := range indexes {
		if _, err := s.db.ExecContext(ctx, indexSQL); err != nil {
			return fmt.Errorf("failed to create index: %w", err)
		}
	}

	return nil
}

// SaveGPU saves or updates a GPU entity.
func (s *Store) SaveGPU(ctx context.Context, gpu *domain.GPU) error {
	if gpu == nil {
		return ErrInvalidGPU
	}
	if gpu.UUID == "" {
		return ErrInvalidGPUUUID
	}

	query := `
		INSERT INTO gpus (uuid, gpu_id, device, model, hostname, container, pod, namespace, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CURRENT_TIMESTAMP)
		ON CONFLICT (uuid) DO UPDATE SET
			gpu_id = EXCLUDED.gpu_id,
			device = EXCLUDED.device,
			model = EXCLUDED.model,
			hostname = EXCLUDED.hostname,
			container = EXCLUDED.container,
			pod = EXCLUDED.pod,
			namespace = EXCLUDED.namespace,
			updated_at = CURRENT_TIMESTAMP;`

	_, err := s.db.ExecContext(ctx, query,
		gpu.UUID, gpu.GPUID, gpu.Device, gpu.Model, gpu.Hostname,
		gpu.Container, gpu.Pod, gpu.Namespace)
	if err != nil {
		return fmt.Errorf("failed to save GPU: %w", err)
	}

	return nil
}

// GetGPU retrieves a GPU by its UUID.
func (s *Store) GetGPU(ctx context.Context, uuid string) (*domain.GPU, error) {
	if uuid == "" {
		return nil, ErrInvalidGPUUUID
	}

	query := `
		SELECT uuid, gpu_id, device, model, hostname, container, pod, namespace
		FROM gpus
		WHERE uuid = $1;`

	var gpu domain.GPU
	err := s.db.QueryRowContext(ctx, query, uuid).Scan(
		&gpu.UUID, &gpu.GPUID, &gpu.Device, &gpu.Model,
		&gpu.Hostname, &gpu.Container, &gpu.Pod, &gpu.Namespace)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get GPU: %w", err)
	}

	return &gpu, nil
}

// ListGPUs returns all GPUs that have telemetry data.
func (s *Store) ListGPUs(ctx context.Context) ([]*domain.GPU, error) {
	query := `
		SELECT DISTINCT g.uuid, g.gpu_id, g.device, g.model, g.hostname, g.container, g.pod, g.namespace
		FROM gpus g
		INNER JOIN telemetry t ON g.uuid = t.gpu_uuid
		ORDER BY g.uuid;`

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list GPUs: %w", err)
	}
	defer rows.Close()

	var gpus []*domain.GPU
	for rows.Next() {
		var gpu domain.GPU
		if err := rows.Scan(
			&gpu.UUID, &gpu.GPUID, &gpu.Device, &gpu.Model,
			&gpu.Hostname, &gpu.Container, &gpu.Pod, &gpu.Namespace); err != nil {
			return nil, fmt.Errorf("failed to scan GPU: %w", err)
		}
		gpus = append(gpus, &gpu)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating GPUs: %w", err)
	}

	return gpus, nil
}

// SaveTelemetry saves a telemetry record.
func (s *Store) SaveTelemetry(ctx context.Context, record *domain.TelemetryRecord) error {
	if record == nil {
		return ErrInvalidTelemetryRecord
	}
	if record.GPUUUID == "" {
		return ErrInvalidGPUUUID
	}

	query := `
		INSERT INTO telemetry (gpu_uuid, metric_name, value, ingestion_time, container, pod, namespace, hostname, model_name, gpu_id, device)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (gpu_uuid, metric_name, ingestion_time) DO UPDATE SET
			value = EXCLUDED.value,
			container = EXCLUDED.container,
			pod = EXCLUDED.pod,
			namespace = EXCLUDED.namespace,
			hostname = EXCLUDED.hostname,
			model_name = EXCLUDED.model_name,
			gpu_id = EXCLUDED.gpu_id,
			device = EXCLUDED.device;`

	_, err := s.db.ExecContext(ctx, query,
		record.GPUUUID, record.MetricName, record.Value, record.IngestionTime,
		record.Container, record.Pod, record.Namespace, record.Hostname,
		record.ModelName, record.GPUID, record.Device)
	if err != nil {
		return fmt.Errorf("failed to save telemetry: %w", err)
	}

	return nil
}

// GetTelemetryByGPU retrieves telemetry records for a specific GPU.
// Results are ordered by IngestionTime in ascending order (oldest first).
func (s *Store) GetTelemetryByGPU(ctx context.Context, gpuUUID string, start, end *time.Time) ([]*domain.TelemetryRecord, error) {
	if gpuUUID == "" {
		return nil, ErrInvalidGPUUUID
	}

	query := `
		SELECT gpu_uuid, metric_name, value, ingestion_time, container, pod, namespace, hostname, model_name, gpu_id, device
		FROM telemetry
		WHERE gpu_uuid = $1`

	args := []interface{}{gpuUUID}
	argIndex := 2

	if start != nil {
		query += fmt.Sprintf(" AND ingestion_time >= $%d", argIndex)
		args = append(args, *start)
		argIndex++
	}

	if end != nil {
		query += fmt.Sprintf(" AND ingestion_time <= $%d", argIndex)
		args = append(args, *end)
		argIndex++
	}

	query += " ORDER BY ingestion_time ASC;"

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to get telemetry: %w", err)
	}
	defer rows.Close()

	var records []*domain.TelemetryRecord
	for rows.Next() {
		var record domain.TelemetryRecord
		if err := rows.Scan(
			&record.GPUUUID, &record.MetricName, &record.Value, &record.IngestionTime,
			&record.Container, &record.Pod, &record.Namespace, &record.Hostname,
			&record.ModelName, &record.GPUID, &record.Device); err != nil {
			return nil, fmt.Errorf("failed to scan telemetry record: %w", err)
		}
		records = append(records, &record)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating telemetry records: %w", err)
	}

	return records, nil
}
