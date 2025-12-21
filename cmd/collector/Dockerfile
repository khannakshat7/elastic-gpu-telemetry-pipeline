# Multi-stage build for Collector service
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
    -o collector \
    ./cmd/collector

# Stage 2: Runtime
FROM alpine:latest

# Install ca-certificates for HTTPS and tzdata for timezone support
RUN apk --no-cache add ca-certificates tzdata

# Create non-root user
RUN addgroup -g 1000 appuser && \
    adduser -D -u 1000 -G appuser appuser

# Set working directory
WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/collector .

# Switch to non-root user
USER appuser

# Expose port (if needed for health checks)
EXPOSE 8080

# Environment variables with defaults
ENV COLLECTOR_INSTANCE_ID=""
ENV COLLECTOR_BATCH_SIZE=1
ENV QUEUE_SERVICE_URL=http://queue-service:8080
ENV STORAGE_BACKEND=memory
ENV STORAGE_URI=""
ENV STORAGE_SERVICE_URL=""
ENV LOG_LEVEL=info

# Health check (process-based, since collector doesn't expose HTTP)
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD pgrep -f collector > /dev/null || exit 1

# Run the service
CMD ["./collector"]

