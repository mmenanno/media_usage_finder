# Media Usage Finder - Implementation Summary

## Project Overview

A high-performance Go application designed to scan large media servers (90TB+) and track file usage across multiple services (Plex, Sonarr, Radarr, qBittorrent). The tool identifies orphaned files, detects hardlinks, and provides detailed statistics through a modern web interface.

## Key Features Implemented

### Core Functionality

✅ **Multi-threaded File Scanner**

- Worker pool architecture with configurable concurrency
- Buffered channels for efficient file processing
- Progress tracking with real-time updates
- Graceful shutdown and resumption support

✅ **Service Integration**

- Plex API client with XML parsing
- Sonarr/Radarr v3 API support
- qBittorrent Web API with qui proxy support
- Automatic path translation between container paths

✅ **Hardlink Detection**

- Inode-based tracking across filesystems
- Grouping and space savings calculations
- Efficient in-memory caching during scans

✅ **Database Layer**

- SQLite with WAL mode for concurrency
- Comprehensive indexing for performance
- FTS5 full-text search on file paths
- Audit logging for deletions

### Web Interface

✅ **Dashboard**

- Real-time scan progress with SSE
- Overall statistics display
- Service breakdown cards
- Quick action links

✅ **Files Page**

- Full-text search with HTMX
- Advanced filtering (service, orphaned status)
- Pagination (25/50/100/500 items per page)
- Export to JSON/CSV

✅ **Statistics Page**

- Detailed storage metrics
- Service usage breakdown with progress bars
- Hardlink efficiency calculations
- Contextual recommendations

✅ **Configuration Page**

- Live editing of all configuration options
- Service connection testing
- Dynamic path mapping management
- YAML import/export

### Real-time Features

✅ **Server-Sent Events (SSE)**

- Live streaming of scan logs
- Non-blocking updates

✅ **HTMX Integration**

- Dynamic content updates without page reloads
- Progressive enhancement
- Polling for progress bars (2-second intervals)

### CLI Commands

✅ **Serve Command**

- Starts web server on configurable port
- Template loading with custom functions

✅ **Scan Command**

- Full and incremental scan modes
- Command-line progress output

✅ **Stats Command**

- Text-based statistics display
- Service breakdowns

✅ **Export Command**

- JSON and CSV export formats
- Filtered exports (orphaned only)
- Output to file or stdout

✅ **Config Commands**

- Validation
- Display current configuration

### Docker Support

✅ **Containerization**

- Multi-stage Dockerfile
- Alpine-based runtime image
- Embedded web assets
- Volume mounts for data persistence

✅ **CI/CD**

- GitHub Actions workflow
- VERSION-based releases
- GHCR publishing
- Multi-platform support (amd64)

## Technical Implementation

### Architecture

- **Language**: Go 1.25
- **Database**: SQLite with WAL mode
- **Frontend**: HTMX + Tailwind CSS
- **Templating**: Go html/template with custom functions

### Key Dependencies

- `github.com/mattn/go-sqlite3` - SQLite driver
- `github.com/spf13/cobra` - CLI framework
- `github.com/spf13/viper` - Configuration management
- `gopkg.in/yaml.v3` - YAML parsing
- Tailwind CSS 3.4+ - Styling
- HTMX 1.9+ - Dynamic HTML

### Performance Optimizations

- **Database**: Proper indexes, prepared statements, transaction batching
- **Concurrency**: Worker pools with configurable size
- **Caching**: Inode maps for hardlink lookups
- **Incremental Scans**: Only process modified files

### Security & Reliability

- **Graceful Shutdown**: SIGTERM/SIGINT handling
- **Database Locking**: Busy timeout and retry logic
- **Atomic Operations**: Transaction-based updates
- **Audit Logging**: Track all deletions and modifications

## File Structure

```text
media_usage_finder/
├── cmd/media-finder/       # CLI entrypoint
├── internal/
│   ├── api/                # Service API clients
│   ├── config/             # Configuration management
│   ├── database/           # SQLite layer
│   ├── scanner/            # File scanning engine
│   ├── server/             # HTTP server
│   └── stats/              # Statistics calculator
├── web/
│   ├── templates/          # HTML templates
│   └── static/             # CSS and JS
├── .github/workflows/      # CI/CD
├── Dockerfile
├── docker-compose.yml
└── Documentation files
```

## Database Schema

### Tables

- **files**: Core file tracking with inode information
- **usage**: Service-to-file relationships
- **scans**: Scan history and status
- **config**: Key-value configuration storage
- **audit_log**: Change tracking

### Indexes

- Path uniqueness
- Composite (device_id, inode) for hardlinks
- Size, modified_time, orphaned status
- FTS5 virtual table for path search

## Configuration System

### Path Mappings

Two-tier system for accurate path translation:

1. **Local Mappings**: Media-finder's container paths
2. **Service Mappings**: Per-service container paths

All paths automatically translated to canonical host paths for comparison.

### Service Configuration

- Plex: Token-based authentication
- Sonarr/Radarr: API key authentication
- qBittorrent: Username/password or qui proxy

## Deployment

### Docker Deployment

1. Pull image: `docker pull ghcr.io/mmenanno/media-usage-finder:latest`
2. Create config file
3. Map volumes for data and media
4. Start container

### Development

1. Clone repository
2. Run `make install-deps`
3. Run `make tailwind`
4. Run `make build`
5. Execute `./bin/media-finder serve`

## Testing & Quality

- No linter errors
- Proper error handling throughout
- Graceful degradation for missing services
- Transaction-safe database operations

## Future Enhancements

Potential additions not yet implemented:

- Integration tests
- Metrics/Prometheus support
- Additional service integrations
- Advanced filtering options
- Bulk operations UI

## Performance Characteristics

Designed for:

- **Scale**: 90TB+ datasets
- **Files**: Millions of files
- **Services**: 4+ concurrent service queries
- **Workers**: Configurable (default: 10)
- **Memory**: Efficient with streaming operations

## Conclusion

The Media Usage Finder is a complete, production-ready application that fulfills all requirements from the original specification. It provides a robust, performant, and user-friendly solution for managing large media libraries across multiple services.
