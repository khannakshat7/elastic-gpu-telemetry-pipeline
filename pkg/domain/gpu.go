package domain

// GPU represents a GPU entity in the system.
// Primary Key: UUID (globally unique across all hosts)
//
// CSV Column Mapping:
//   - UUID: maps to CSV column "uuid" (e.g., "GPU-5fd4f087-86f3-7a43-b711-4771313afc50")
//   - GPUID: maps to CSV column "gpu_id" (e.g., "0", "1", "2") - per-host GPU index
//   - Device: maps to CSV column "device" (e.g., "nvidia0", "nvidia1")
//   - Model: maps to CSV column "modelName" (e.g., "NVIDIA H100 80GB HBM3")
//   - Hostname: maps to CSV column "Hostname" (e.g., "mtv5-dgx1-hgpu-031")
type GPU struct {
	// UUID is the globally unique identifier for the GPU.
	// Used as the primary key and in API endpoints: /api/v1/gpus/{uuid}
	UUID string `json:"uuid"`

	// GPUID is the GPU index on the host (0, 1, 2, etc.)
	// Not globally unique - same GPUID can exist on different hosts
	GPUID string `json:"gpu_id"`

	// Device is the device name (e.g., "nvidia0", "nvidia1")
	Device string `json:"device"`

	// Model is the GPU model name (e.g., "NVIDIA H100 80GB HBM3")
	Model string `json:"model"`

	// Hostname is the host machine identifier where the GPU is located
	Hostname string `json:"hostname"`

	// Container is the container identifier (if applicable).
	// Maps to CSV column "container". Often empty in sample data.
	Container string `json:"container,omitempty"`

	// Pod is the Kubernetes pod identifier (if applicable).
	// Maps to CSV column "pod". Often empty in sample data.
	Pod string `json:"pod,omitempty"`

	// Namespace is the Kubernetes namespace (if applicable).
	// Maps to CSV column "namespace". Often empty in sample data.
	Namespace string `json:"namespace,omitempty"`
}
