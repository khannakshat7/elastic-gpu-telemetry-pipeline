// Package api provides HTTP handlers and routes for the API Gateway.
//
// This package implements the REST API for querying GPU telemetry data.
// It uses DTOs (Data Transfer Objects) to separate the API contract from
// internal domain models, and includes comprehensive error handling and validation.
//
// API Endpoints:
//   - GET /api/v1/gpus - List all GPUs with telemetry
//   - GET /api/v1/gpus/{id}/telemetry - Get telemetry for a specific GPU
//
// The API uses Gin framework for HTTP routing and middleware support.
package api
