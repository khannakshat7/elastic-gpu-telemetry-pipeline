# Multi-stage build for API Gateway service
# Stage 1: Build
FROM golang:1.25-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git ca-certificates tzdata

# Set working directory
WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Note: Swagger docs should be generated before building the image
# Run: make swagger (or swag init) before docker build
# The docs/swagger directory will be copied if it exists

# Build the binary
# CGO_ENABLED=0 creates a statically linked binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags='-w -s -extldflags "-static"' \
    -o api-gateway \
    ./cmd/api-gateway

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
COPY --from=builder /build/api-gateway .

# Copy Swagger docs if they exist
COPY --from=builder --chown=appuser:appuser /build/docs/swagger /app/docs/swagger

# Switch to non-root user
USER appuser

# Expose API port
EXPOSE 8081

# Environment variables with defaults
ENV API_PORT=8081
ENV STORAGE_BACKEND=memory
ENV STORAGE_URI=""
ENV STORAGE_SERVICE_URL=""
ENV LOG_LEVEL=info

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8081/health || exit 1

# Run the service
CMD ["./api-gateway"]

