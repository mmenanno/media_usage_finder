.PHONY: build clean dev test docker-build docker-run install-deps tailwind

VERSION := $(shell cat VERSION)
BINARY_NAME := media-finder
DOCKER_IMAGE := ghcr.io/mmenanno/media-usage-finder

# Build the binary
build:
	@echo "Building $(BINARY_NAME)..."
	@go build -ldflags="-X main.Version=$(VERSION)" -o bin/$(BINARY_NAME) ./cmd/media-finder

# Install dependencies
install-deps:
	@echo "Installing Go dependencies..."
	@go mod download
	@go mod tidy

# Build Tailwind CSS
tailwind:
	@echo "Building Tailwind CSS..."
	@npx tailwindcss -i ./web/static/css/input.css -o ./web/static/css/styles.css --minify

# Development mode with hot reload
dev:
	@echo "Starting development server..."
	@go run ./cmd/media-finder serve

# Run tests
test:
	@echo "Running tests..."
	@go test -v ./...

# Clean build artifacts
clean:
	@echo "Cleaning..."
	@rm -rf bin/
	@rm -f web/static/css/styles.css

# Build Docker image
docker-build:
	@echo "Building Docker image..."
	@docker build -t $(DOCKER_IMAGE):$(VERSION) -t $(DOCKER_IMAGE):latest .

# Run Docker container
docker-run:
	@docker run -d \
		-p 8080:8080 \
		-v $(PWD)/data:/data \
		-v /mnt/user/data/media:/media:rw \
		-v /mnt/user/data/downloads/torrents:/downloads:rw \
		--name media-finder \
		$(DOCKER_IMAGE):latest

# Stop and remove container
docker-stop:
	@docker stop media-finder || true
	@docker rm media-finder || true

# Generate embedded assets
generate:
	@echo "Generating embedded assets..."
	@go generate ./...

