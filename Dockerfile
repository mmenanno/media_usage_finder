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

# 2) Web assets needed by the Tailwind CSS build. input.css uses
#    `@source "../../../web/templates"` and `@source "../../../web/static/js"`,
#    so both directories must exist in the image before `build:css`
#    runs — otherwise Tailwind sees no class usage and only emits the
#    handful of utilities literally referenced in input.css.
COPY web/static ./web/static
COPY web/templates ./web/templates
RUN npm run build:css

# 3) Remaining Go source. Templates/static are already in place from
#    the previous step.
COPY cmd ./cmd
COPY internal ./internal
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

# Only ca-certificates (HTTPS to upstream services). The healthcheck uses
# `media-finder healthcheck`, so wget/curl aren't needed. The sqlite CLI
# isn't installed either — FTS5 is compiled into the binary.
RUN apk --no-cache add ca-certificates

WORKDIR /app

# Single self-contained binary. Templates and static assets are embedded.
COPY --from=builder /app/bin/media-finder /app/media-finder

# appdata directories (overlaid by the volume in production)
RUN mkdir -p /appdata/data /appdata/config

EXPOSE 8787

# Healthcheck via the binary itself (exits non-zero on failure). No
# external tooling required in the runtime image.
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD ["/app/media-finder", "healthcheck"]

ENTRYPOINT ["/app/media-finder"]
CMD ["serve"]
