# Build stage
FROM golang:1.25-alpine AS builder

# Install Node.js, npm, and build tools for Tailwind CSS and CGO
# sqlite-dev is required for FTS5 support in go-sqlite3
RUN apk add --no-cache nodejs npm git gcc musl-dev sqlite-dev

WORKDIR /app

# Copy package files for npm
COPY package*.json ./

# Install npm dependencies (including devDependencies for tailwindcss)
RUN npm ci

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build Tailwind CSS using npm script
RUN npm run build:css

# Build Go binary with FTS5 support
RUN CGO_ENABLED=1 GOOS=linux go build -tags "sqlite_fts5" -ldflags="-X main.Version=$(cat VERSION)" -o /app/bin/media-finder ./cmd/media-finder

# Runtime stage
FROM alpine:latest

# Install ca-certificates, sqlite (full package with FTS5), and wget for healthcheck
RUN apk --no-cache add ca-certificates sqlite wget

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/bin/media-finder /app/media-finder

# Copy web assets
COPY --from=builder /app/web /app/web

# Create appdata directories
RUN mkdir -p /appdata/data /appdata/config

# Expose port
EXPOSE 8787

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:8787/health || exit 1

# Set default command
ENTRYPOINT ["/app/media-finder"]
CMD ["serve"]

