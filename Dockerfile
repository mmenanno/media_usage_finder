# syntax=docker/dockerfile:1.7
# Multi-stage build. Templates and static assets are embedded into the
# binary via `//go:embed` (see web/web.go), so the runtime stage carries
# only the binary itself plus a couple of OS packages.

FROM golang:1.25-alpine AS builder

# Build deps:
#   nodejs/npm  -> Tailwind CSS pipeline
#   git         -> some `go install`/module operations
#   gcc/musl-dev -> CGO toolchain (required by mattn/go-sqlite3)
#   sqlite-dev  -> SQLite headers for the FTS5-enabled build
RUN apk add --no-cache nodejs npm git gcc musl-dev sqlite-dev

WORKDIR /app

# 1) Dependency layers — copy only what the install step needs so the
#    layer is cached across source changes.
COPY package*.json ./
RUN --mount=type=cache,target=/root/.npm,sharing=locked \
    npm ci

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod,sharing=locked \
    --mount=type=cache,target=/root/.cache/go-build,sharing=locked \
    go mod download

# 2) CSS step — depends only on the input stylesheet + Tailwind config
#    (if any). Reads no Go source, so an unrelated Go change won't bust
#    this layer.
COPY web/static/css ./web/static/css
RUN npm run build:css

# 3) Source for the Go build. Avoid `COPY . .` so changes to README,
#    Dockerfile, etc. don't invalidate the build layer.
COPY cmd ./cmd
COPY internal ./internal
COPY web/templates ./web/templates
COPY web/static ./web/static
COPY web/web.go ./web/web.go
COPY VERSION ./

# 4) Build with FTS5 + size/symbol stripping. -trimpath removes the
#    machine-local build prefix from binaries for reproducibility.
RUN --mount=type=cache,target=/go/pkg/mod,sharing=locked \
    --mount=type=cache,target=/root/.cache/go-build,sharing=locked \
    CGO_ENABLED=1 GOOS=linux \
    go build -tags "sqlite_fts5" \
             -ldflags="-s -w -X main.Version=$(cat VERSION)" \
             -trimpath \
             -o /app/bin/media-finder ./cmd/media-finder


# Runtime stage
FROM alpine:latest

# Only ca-certificates (HTTPS to upstream services) and wget (HEALTHCHECK).
# The sqlite CLI is no longer installed: FTS5 is compiled into the binary
# via -tags "sqlite_fts5", so the CLI tool isn't needed at runtime.
RUN apk --no-cache add ca-certificates wget

WORKDIR /app

# Single self-contained binary. Templates and static assets are embedded.
COPY --from=builder /app/bin/media-finder /app/media-finder

# appdata directories (overlaid by the volume in production)
RUN mkdir -p /appdata/data /appdata/config

EXPOSE 8787

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:8787/health || exit 1

ENTRYPOINT ["/app/media-finder"]
CMD ["serve"]
