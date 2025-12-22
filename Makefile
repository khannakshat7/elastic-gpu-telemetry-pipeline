.PHONY: build test test-system run-streamer run-collector run-api run-queue clean help \
	deploy-local check-deps-local kind-create-local kind-load-images-local \
	helm-deploy-local wait-for-pods-local port-forward-bg-local port-forward-stop-local

# Build variables
BINARY_STREAMER=bin/streamer
BINARY_COLLECTOR=bin/collector
BINARY_API=bin/api-gateway
BINARY_QUEUE=bin/queue-service
BINARY_STORAGE=bin/storage-service

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOMOD=$(GOCMD) mod

# Build all binaries
build:
	@echo "Building all services..."
	@mkdir -p bin
	$(GOBUILD) -o $(BINARY_STREAMER) ./cmd/streamer
	$(GOBUILD) -o $(BINARY_COLLECTOR) ./cmd/collector
	$(GOBUILD) -o $(BINARY_API) ./cmd/api-gateway
	$(GOBUILD) -o $(BINARY_QUEUE) ./cmd/queue-service
	$(GOBUILD) -o $(BINARY_STORAGE) ./cmd/storage-service
	@echo "Build complete!"

# Run tests with race detection and basic coverage
test:
	@echo "Running tests with race detection..."
	@if $(GOTEST) -v -race -timeout 30s -coverprofile=coverage.out ./pkg/... ./internal/...; then \
		echo ""; \
		echo "=== Test Coverage Summary ==="; \
		if [ -f coverage.out ]; then \
			$(GOCMD) tool cover -func=coverage.out 2>/dev/null | tail -1 || echo "Coverage summary unavailable"; \
			echo ""; \
		else \
			echo "Coverage file not generated"; \
		fi; \
		echo ""; \
		echo "✅ All tests passed!"; \
	else \
		echo ""; \
		echo "❌ Tests failed!"; \
		exit 1; \
	fi

# Run system/end-to-end tests
test-system:
	@echo "🧪 Running system/end-to-end tests..."
	@echo "This will start all services and test the complete pipeline flow..."
	@echo ""
	@if $(GOTEST) -v -timeout 60s ./tests/system/...; then \
		echo ""; \
		echo "✅ All system tests passed!"; \
	else \
		echo ""; \
		echo "❌ System tests failed!"; \
		exit 1; \
	fi

# Generate detailed coverage report (HTML)
cover: coverage.out
	@echo "Generating coverage HTML report..."
	@$(GOCMD) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"
	@echo "Open coverage.html in your browser to view detailed coverage"

# Generate coverage profile (run tests first)
coverage.out:
	@echo "Running tests to generate coverage profile..."
	@$(GOTEST) -v -race -timeout 30s -coverprofile=coverage.out ./...
	@echo "Coverage profile generated: coverage.out"

# View coverage in terminal
cover-func: coverage.out
	@echo "=== Coverage by Function ==="
	@$(GOCMD) tool cover -func=coverage.out

# Clean coverage files
clean-coverage:
	@echo "Cleaning coverage files..."
	@rm -f coverage.out coverage.html
	@echo "Coverage files cleaned"

# Run streamer service
run-streamer:
	@echo "Starting streamer service..."
	$(GOCMD) run ./cmd/streamer

# Run collector service
run-collector:
	@echo "Starting collector service..."
	$(GOCMD) run ./cmd/collector

# Run API Gateway service
run-api:
	@echo "Starting API Gateway service..."
	$(GOCMD) run ./cmd/api-gateway

# Run queue service
run-queue:
	@echo "Starting queue service..."
	$(GOCMD) run ./cmd/queue-service

# Run storage service
run-storage:
	@echo "Starting storage service..."
	$(GOCMD) run ./cmd/storage-service

# Clean build artifacts and coverage files
clean: clean-coverage
	@echo "Cleaning build artifacts..."
	@rm -rf bin/
	@echo "Clean complete!"

# Generate OpenAPI/Swagger spec using swaggo
# Requires: go install github.com/swaggo/swag/cmd/swag@latest
swagger: swag
	@echo "Swagger spec generated in docs/swagger/"

swag:
	@echo "Generating OpenAPI/Swagger specification..."
	@SWAG_CMD=$$(which swag 2>/dev/null || echo "$$HOME/go/bin/swag"); \
	if [ ! -f "$$SWAG_CMD" ]; then \
		echo "Error: swag CLI tool not found."; \
		echo "Install it with: go install github.com/swaggo/swag/cmd/swag@latest"; \
		echo "Make sure $$GOPATH/bin or $$HOME/go/bin is in your PATH, or run: export PATH=$$PATH:$$HOME/go/bin"; \
		exit 1; \
	fi
	@mkdir -p docs/swagger
	@SWAG_CMD=$$(which swag 2>/dev/null || echo "$$HOME/go/bin/swag"); \
	$$SWAG_CMD init -g cmd/api-gateway/main.go -o docs/swagger --parseDependency --parseInternal
	@echo "✓ OpenAPI/Swagger specification generated successfully!"
	@echo "  Files: docs/swagger/swagger.json, docs/swagger/swagger.yaml"
	@echo "  View at: http://localhost:8081/swagger/index.html (when API Gateway is running)"

# Alias for swagger
openapi: swagger

# Download dependencies
deps:
	@echo "Downloading dependencies..."
	$(GOMOD) download
	$(GOMOD) tidy

# Format code
fmt:
	@echo "Formatting code..."
	$(GOCMD) fmt ./...

# Lint code (requires golangci-lint)
lint:
	@echo "Linting code..."
	@echo "TODO: Add golangci-lint configuration"

# Docker variables
DOCKER_IMAGE_PREFIX=elastic-gpu-telemetry
DOCKER_TAG?=latest

# Docker build targets
docker-streamer:
	@echo "Building Docker image for streamer service..."
	@docker build -f cmd/streamer/Dockerfile -t $(DOCKER_IMAGE_PREFIX)-streamer:$(DOCKER_TAG) .
	@echo "✓ Streamer image built: $(DOCKER_IMAGE_PREFIX)-streamer:$(DOCKER_TAG)"

docker-collector:
	@echo "Building Docker image for collector service..."
	@docker build -f cmd/collector/Dockerfile -t $(DOCKER_IMAGE_PREFIX)-collector:$(DOCKER_TAG) .
	@echo "✓ Collector image built: $(DOCKER_IMAGE_PREFIX)-collector:$(DOCKER_TAG)"

docker-api-gateway:
	@echo "Building Docker image for API Gateway service..."
	@docker build -f cmd/api-gateway/Dockerfile -t $(DOCKER_IMAGE_PREFIX)-api-gateway:$(DOCKER_TAG) .
	@echo "✓ API Gateway image built: $(DOCKER_IMAGE_PREFIX)-api-gateway:$(DOCKER_TAG)"

docker-queue-service:
	@echo "Building Docker image for queue service..."
	@docker build -f cmd/queue-service/Dockerfile -t $(DOCKER_IMAGE_PREFIX)-queue-service:$(DOCKER_TAG) .
	@echo "✓ Queue service image built: $(DOCKER_IMAGE_PREFIX)-queue-service:$(DOCKER_TAG)"

docker-storage-service:
	@echo "Building Docker image for storage service..."
	@docker build -f cmd/storage-service/Dockerfile -t $(DOCKER_IMAGE_PREFIX)-storage-service:$(DOCKER_TAG) .
	@echo "✓ Storage service image built: $(DOCKER_IMAGE_PREFIX)-storage-service:$(DOCKER_TAG)"

# Build all Docker images
docker-all: docker-queue-service docker-storage-service docker-streamer docker-collector docker-api-gateway
	@echo ""
	@echo "✅ All Docker images built successfully!"
	@echo ""
	@echo "Available images:"
	@docker images | grep $(DOCKER_IMAGE_PREFIX) || echo "No images found"

# Clean Docker images
docker-clean:
	@echo "Cleaning Docker images..."
	@docker images | grep $(DOCKER_IMAGE_PREFIX) | awk '{print $$3}' | xargs -r docker rmi -f || true
	@echo "✓ Docker images cleaned"

# Kubernetes/Helm variables
KIND_CLUSTER_NAME?=elastic-gpu-telemetry-cluster
HELM_RELEASE_NAME?=elastic-gpu-telemetry

# Check if required tools are installed
check-deps-local:
	@echo "🔍 Checking dependencies..."
	@command -v docker >/dev/null 2>&1 || { echo "❌ docker is required but not installed."; echo "   Install: https://docs.docker.com/get-docker/"; exit 1; }
	@command -v kind >/dev/null 2>&1 || { echo "❌ kind is required but not installed."; echo "   Install: brew install kind (macOS) or https://kind.sigs.k8s.io/"; exit 1; }
	@command -v helm >/dev/null 2>&1 || { echo "❌ helm is required but not installed."; echo "   Install: brew install helm (macOS) or https://helm.sh/docs/intro/install/"; exit 1; }
	@command -v kubectl >/dev/null 2>&1 || { echo "❌ kubectl is required but not installed."; echo "   Install: brew install kubectl (macOS) or https://kubernetes.io/docs/tasks/tools/"; exit 1; }
	@echo "✅ All dependencies found"

# Create kind cluster if it doesn't exist
kind-create-local:
	@echo "📦 Checking kind cluster '$(KIND_CLUSTER_NAME)'..."
	@if kind get clusters 2>/dev/null | grep -q "^$(KIND_CLUSTER_NAME)$$"; then \
		echo "✓ Cluster already exists"; \
	else \
		echo "  Creating kind cluster..."; \
		kind create cluster --name $(KIND_CLUSTER_NAME); \
		echo "✓ Cluster created"; \
	fi

# Build and load Docker images into kind cluster
kind-load-images-local: docker-all
	@echo "📥 Loading Docker images into kind cluster..."
	@for service in streamer collector api-gateway queue-service storage-service; do \
		echo "  Loading elastic-gpu-telemetry-$${service}:$(DOCKER_TAG)..."; \
		kind load docker-image elastic-gpu-telemetry-$${service}:$(DOCKER_TAG) --name $(KIND_CLUSTER_NAME) || { \
			echo "  ⚠️  Failed to load elastic-gpu-telemetry-$${service}:$(DOCKER_TAG)"; \
		}; \
	done
	@echo "✓ Images loaded"

# Install or upgrade Helm chart
helm-deploy-local:
	@echo "📦 Deploying Helm chart..."
	@if helm list -q 2>/dev/null | grep -q "^$(HELM_RELEASE_NAME)$$"; then \
		echo "  Upgrading existing release..."; \
		helm upgrade $(HELM_RELEASE_NAME) ./charts/elastic-gpu-telemetry --wait --timeout=5m; \
		echo "✓ Chart upgraded"; \
	else \
		echo "  Installing new release..."; \
		helm install $(HELM_RELEASE_NAME) ./charts/elastic-gpu-telemetry --wait --timeout=5m; \
		echo "✓ Chart installed"; \
	fi

# Wait for all pods to be ready
wait-for-pods-local:
	@echo "⏳ Waiting for pods to be ready..."
	@kubectl wait --for=condition=ready pod \
		-l app.kubernetes.io/instance=$(HELM_RELEASE_NAME) \
		--timeout=300s 2>/dev/null || { \
		echo "⚠️  Some pods may not be ready yet. Check with: kubectl get pods"; \
	}
	@echo "✓ Pods are ready"

# Port forward API Gateway in background
port-forward-bg-local:
	@echo "🔌 Starting port forward in background..."
	@if [ -f /tmp/kubectl-port-forward.pid ]; then \
		PID=$$(cat /tmp/kubectl-port-forward.pid 2>/dev/null); \
		if ps -p $$PID > /dev/null 2>&1 || pgrep -f "kubectl port-forward.*api-gateway" > /dev/null 2>&1; then \
			echo "⚠️  Port forward already running (PID: $$PID)"; \
			exit 0; \
		fi; \
	fi
	@kubectl port-forward svc/api-gateway 8081:8081 > /tmp/kubectl-port-forward.log 2>&1 & \
	echo $$! > /tmp/kubectl-port-forward.pid; \
	sleep 3; \
	PID=$$(cat /tmp/kubectl-port-forward.pid 2>/dev/null); \
	if ps -p $$PID > /dev/null 2>&1 || pgrep -f "kubectl port-forward.*api-gateway" > /dev/null 2>&1; then \
		echo "✓ Port forward running (PID: $$PID)"; \
	else \
		echo "❌ Failed to start port forward. Check logs: cat /tmp/kubectl-port-forward.log"; \
		rm -f /tmp/kubectl-port-forward.pid; \
		exit 1; \
	fi

# Stop background port forward
port-forward-stop-local:
	@if [ -f /tmp/kubectl-port-forward.pid ]; then \
		PID=$$(cat /tmp/kubectl-port-forward.pid 2>/dev/null); \
		if ps -p $$PID > /dev/null 2>&1 || pgrep -f "kubectl port-forward.*api-gateway" > /dev/null 2>&1; then \
			pkill -f "kubectl port-forward.*api-gateway" 2>/dev/null || kill $$PID 2>/dev/null || true; \
			echo "✓ Port forward stopped"; \
		else \
			echo "Port forward not running"; \
		fi; \
		rm -f /tmp/kubectl-port-forward.pid; \
	else \
		if pgrep -f "kubectl port-forward.*api-gateway" > /dev/null 2>&1; then \
			pkill -f "kubectl port-forward.*api-gateway" 2>/dev/null; \
			echo "✓ Port forward stopped (found by process name)"; \
		else \
			echo "No port forward running"; \
		fi; \
	fi

# Complete local deployment: check deps, create cluster, build & load images, deploy chart, wait for pods, start port forward
deploy-local: check-deps-local kind-create-local kind-load-images-local helm-deploy-local wait-for-pods-local port-forward-bg-local
	@echo ""
	@echo "✅ Deployment complete!"
	@echo ""
	@echo "📊 Deployment status:"
	@kubectl get pods -l app.kubernetes.io/instance=$(HELM_RELEASE_NAME) 2>/dev/null || true
	@echo ""
	@echo "🌐 API Gateway is accessible at:"
	@echo "   http://localhost:8081"
	@echo "   Swagger UI: http://localhost:8081/swagger/index.html"
	@echo ""
	@echo "📝 Useful commands:"
	@echo "   View logs:        kubectl logs -l app.kubernetes.io/component=streamer"
	@echo "   Check status:     kubectl get pods,svc"
	@echo "   Stop port forward: make port-forward-stop-local"
	@echo "   Cleanup:          make cleanup-local"

# Cleanup local deployment
cleanup-local:
	@echo "🧹 Cleaning up local deployment..."
	@$(MAKE) port-forward-stop-local || true
	@echo "  Uninstalling Helm chart..."
	@helm uninstall $(HELM_RELEASE_NAME) 2>/dev/null || echo "  No Helm release to uninstall"
	@read -p "  Delete kind cluster '$(KIND_CLUSTER_NAME)'? (y/N) " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		echo "  Deleting kind cluster..."; \
		kind delete cluster --name $(KIND_CLUSTER_NAME) 2>/dev/null || echo "  Cluster not found"; \
	fi
	@echo "✓ Cleanup complete"

# Kubernetes/Helm targets (legacy - kept for compatibility)
kind-load-images:
	@echo "Loading Docker images into kind cluster..."
	@CLUSTER_NAME=$${KIND_CLUSTER_NAME:-$(KIND_CLUSTER_NAME)}; \
	for service in streamer collector api-gateway queue-service storage-service; do \
		echo "Loading elastic-gpu-telemetry-$${service}:latest..."; \
		kind load docker-image elastic-gpu-telemetry-$${service}:latest --name $$CLUSTER_NAME || true; \
	done
	@echo "✓ Images loaded"

helm-install:
	@echo "Installing Helm chart..."
	@helm install $(HELM_RELEASE_NAME) ./charts/elastic-gpu-telemetry
	@echo "✓ Chart installed"

helm-upgrade:
	@echo "Upgrading Helm chart..."
	@helm upgrade $(HELM_RELEASE_NAME) ./charts/elastic-gpu-telemetry
	@echo "✓ Chart upgraded"

helm-uninstall:
	@echo "Uninstalling Helm chart..."
	@helm uninstall $(HELM_RELEASE_NAME) || true
	@echo "✓ Chart uninstalled"

helm-status:
	@helm status $(HELM_RELEASE_NAME)

# Help target
help:
	@echo "Available targets:"
	@echo "  build              - Build all services"
	@echo "  test               - Run unit tests with race detection and basic coverage"
	@echo "  test-system        - Run system/end-to-end tests (starts all services)"
	@echo "  cover              - Generate detailed HTML coverage report"
	@echo "  cover-func         - Show coverage by function"
	@echo "  clean-coverage     - Clean coverage files"
	@echo "  run-streamer       - Run streamer service"
	@echo "  run-collector      - Run collector service"
	@echo "  run-api            - Run API Gateway service"
	@echo "  run-queue          - Run queue service"
	@echo "  run-storage        - Run storage service"
	@echo "  clean              - Clean build artifacts"
	@echo "  swagger            - Generate OpenAPI/Swagger specification"
	@echo "  openapi            - Alias for swagger"
	@echo "  deps               - Download and tidy dependencies"
	@echo "  fmt                - Format code"
	@echo "  lint               - Lint code"
	@echo ""
	@echo "Docker targets:"
	@echo "  docker-streamer        - Build streamer Docker image"
	@echo "  docker-collector       - Build collector Docker image"
	@echo "  docker-api-gateway     - Build API Gateway Docker image"
	@echo "  docker-queue-service   - Build queue service Docker image"
	@echo "  docker-storage-service - Build storage service Docker image"
	@echo "  docker-all             - Build all Docker images"
	@echo "  docker-clean           - Remove all Docker images"
	@echo ""
	@echo "  Use DOCKER_TAG=<tag> to specify image tag (default: latest)"
	@echo "  Example: make docker-streamer DOCKER_TAG=v1.0.0"
	@echo ""
	@echo "Kubernetes/Helm targets:"
	@echo "  deploy-local          - 🚀 Complete local deployment: check deps, create cluster, build & load images, deploy chart, port forward"
	@echo "  cleanup-local         - Clean up local deployment (stop port forward, uninstall chart, optionally delete cluster)"
	@echo "  port-forward-stop-local - Stop background port forward"
	@echo ""
	@echo "  Individual steps (used by deploy-local):"
	@echo "  check-deps-local      - Check if required tools (docker, kind, helm, kubectl) are installed"
	@echo "  kind-create-local     - Create kind cluster if it doesn't exist"
	@echo "  kind-load-images-local - Build Docker images and load into kind cluster"
	@echo "  helm-deploy-local     - Install or upgrade Helm chart"
	@echo "  wait-for-pods-local   - Wait for all pods to be ready"
	@echo "  port-forward-bg-local - Port forward API Gateway in background"
	@echo ""
	@echo "  Legacy targets:"
	@echo "  kind-load-images      - Load all Docker images into kind cluster (legacy)"
	@echo "  helm-install          - Install Helm chart (legacy)"
	@echo "  helm-upgrade          - Upgrade Helm chart (legacy)"
	@echo "  helm-uninstall        - Uninstall Helm chart"
	@echo "  helm-status          - Show Helm release status"
	@echo ""
	@echo "  Variables:"
	@echo "    KIND_CLUSTER_NAME=<name>  - kind cluster name (default: gpu-telemetry)"
	@echo "    HELM_RELEASE_NAME=<name>  - Helm release name (default: elastic-gpu-telemetry)"
	@echo "    DOCKER_TAG=<tag>         - Docker image tag (default: latest)"
	@echo ""
	@echo "  Examples:"
	@echo "    make deploy-local                              # Complete deployment with defaults"
	@echo "    make deploy-local KIND_CLUSTER_NAME=my-cluster # Use custom cluster name"
	@echo "    make cleanup-local                            # Clean up everything"
	@echo ""
	@echo "  help               - Show this help message"

