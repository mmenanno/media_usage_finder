# Build stage
FROM golang:1.25-alpine AS builder

# Install Node.js and npm for Tailwind CSS
RUN apk add --no-cache nodejs npm git

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Install Tailwind CSS
RUN npm install -D tailwindcss@latest

# Build Tailwind CSS
RUN ./node_modules/.bin/tailwindcss -i ./web/static/css/input.css -o ./web/static/css/styles.css --minify

# Build Go binary
RUN CGO_ENABLED=1 GOOS=linux go build -ldflags="-X main.Version=$(cat VERSION)" -o /app/bin/media-finder ./cmd/media-finder

# Runtime stage
FROM alpine:latest

# Install ca-certificates, sqlite libs, and wget for healthcheck
RUN apk --no-cache add ca-certificates sqlite-libs wget

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/bin/media-finder /app/media-finder

# Copy web assets
COPY --from=builder /app/web /app/web

# Create data directory
RUN mkdir -p /data /config

# Expose port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health || exit 1

# Set default command
ENTRYPOINT ["/app/media-finder"]
CMD ["serve", "--port", "8080"]

