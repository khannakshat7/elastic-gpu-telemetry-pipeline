# Multi-stage build for Streamer service
# Stage 1: Build
FROM golang:1.24-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git ca-certificates tzdata

# Set working directory
WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the binary
# CGO_ENABLED=0 creates a statically linked binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags='-w -s -extldflags "-static"' \
    -o streamer \
    ./cmd/streamer

# Stage 2: Runtime
FROM alpine:latest

# Install ca-certificates for HTTPS, tzdata for timezone support, and procps for pgrep
RUN apk --no-cache add ca-certificates tzdata procps

# Create non-root user
RUN addgroup -g 1000 appuser && \
    adduser -D -u 1000 -G appuser appuser

# Set working directory
WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/streamer .

# Copy CSV data directory
COPY --chown=appuser:appuser csv/ /app/csv/

# Switch to non-root user
USER appuser

# Expose port (if needed for health checks)
EXPOSE 8080

# Environment variables with defaults
ENV CSV_FILE_PATH=/app/csv/dcgm_metrics_20250718_134233.csv
ENV STREAM_INTERVAL_MS=100
ENV STREAMER_INSTANCE_ID=""
ENV QUEUE_SERVICE_URL=http://queue-service:8080
ENV LOG_LEVEL=info

# Health check (process-based, since streamer doesn't expose HTTP)
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD pgrep -f streamer > /dev/null || exit 1

# Run the service
CMD ["./streamer"]

