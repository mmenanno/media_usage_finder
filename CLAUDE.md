# Media Usage Finder - Claude Code Guide

## Project Overview

**Media Usage Finder** is a high-performance Go application that scans media server files and tracks which services are using them. It helps identify orphaned files, detect hardlinks, and optimize storage across large media servers (90TB+ datasets).

### Core Purpose

- Track file usage across multiple media management services
- Identify orphaned files not tracked by any service
- Detect hardlinks to calculate actual vs. apparent storage usage
- Provide a modern web UI for browsing, filtering, and managing files

### Tech Stack

- **Backend**: Go 1.25+
- **Database**: SQLite with WAL mode, FTS5, comprehensive indexing
- **Frontend**: HTMX for dynamic updates, Tailwind CSS for styling, Chart.js for visualizations
- **Real-time**: HTTP polling today; SSE infrastructure exists for log streaming. Migration to push-based progress is planned (see `plans/005-sse-progress.md` if present).
- **Hashing**: BLAKE3 (`github.com/zeebo/blake3`) with progressive verification levels
- **Deployment**: Docker/Docker Compose ready

### Supported Services

- **Plex** - Media server (REST API with token auth)
- **Sonarr** - TV show management (REST API with API key)
- **Radarr** - Movie management (REST API with API key)
- **qBittorrent** - Torrent client (Web API or qui proxy)
- **Stash** - Adult content organizer (GraphQL API with API key)
- **Calibre** - Ebook library manager (Calibre Content Server API)

## Common Development Commands

```bash
# Build the application
make build                # Builds binary to bin/media-finder with version from VERSION file

# Development
make dev                  # Run with hot reload (go run ./cmd/media-finder serve)
make install-deps         # Install Go dependencies

# Frontend
make tailwind             # Build Tailwind CSS (required after HTML/class changes)
npx tailwindcss -i ./web/static/css/input.css -o ./web/static/css/styles.css --minify

# Testing
make test                 # Run all Go tests (note: no test files yet — refactor work has been
                          # mechanical, but new features should ship with tests)

# Docker
make docker-build         # Build Docker image (uses VERSION file)
make docker-run           # Run Docker container
make docker-stop          # Stop and remove container

# Cleanup
make clean                # Remove bin/ and built CSS

# CLI subcommands (also available via the binary directly)
./bin/media-finder serve            # Start the web server (port 8787)
./bin/media-finder scan             # Run a full scan from the CLI
./bin/media-finder scan --incremental
./bin/media-finder disk-scan        # Scan disk locations only (requires disks configured)
./bin/media-finder stats            # Print summary statistics
./bin/media-finder export --orphaned --format json -o orphaned.json
./bin/media-finder delete           # CLI-driven file deletion
./bin/media-finder config validate
./bin/media-finder config show

# Version Management
# ALWAYS bump the VERSION file when making changes
# Format: MAJOR.MINOR.PATCH (e.g., 0.61.3)
# - MAJOR: Breaking changes
# - MINOR: New features (like adding a service)
# - PATCH: Bug fixes and refactors
```

## Project Structure

```text
media_usage_finder/
├── cmd/media-finder/           # CLI entrypoint (main.go, all subcommands)
├── internal/
│   ├── api/                    # Service API clients (one file per service)
│   │   ├── factory.go          # Client factory (CreateClient, IsServiceConfigured)
│   │   ├── arr.go              # Shared Sonarr/Radarr base client
│   │   ├── plex.go
│   │   ├── sonarr.go
│   │   ├── radarr.go
│   │   ├── qbittorrent.go
│   │   ├── stash.go            # GraphQL
│   │   └── calibre.go          # Calibre Content Server REST
│   ├── config/                 # Configuration management
│   │   ├── config.go           # Config struct, YAML loading
│   │   ├── pathmapper.go       # Path translation (container <-> host)
│   │   └── pathcache.go        # Thread-safe LRU-ish cache for path translations
│   ├── constants/              # Shared constants (buffer sizes, retention defaults)
│   ├── database/               # SQLite database layer
│   │   ├── db.go               # Connection, transactions, lifecycle
│   │   ├── schema.go           # Schema + migrations
│   │   ├── queries.go          # Types, generic helpers (scanFileRow,
│   │   │                       # buildInClause, ValidateOrderBy, …),
│   │   │                       # config CRUD, vacuum/FTS, GetDatabaseStats
│   │   ├── queries_files.go    # File CRUD (Upsert/Get/List/Delete/Clear)
│   │   ├── queries_search.go   # SearchFiles (FTS5), ListFiles, GetFileExtensions
│   │   ├── queries_hardlinks.go
│   │   ├── queries_hashes.go   # Hash CRUD, level stats, quick/per-level dupe queries
│   │   ├── queries_scans.go    # Scan lifecycle, scan logs, missing-files
│   │   ├── queries_usage.go    # Usage upsert, orphan recalc
│   │   ├── queries_disks.go    # file_disk_locations CRUD
│   │   ├── queries_audit.go    # Audit log, consolidation/hardlink logging
│   │   └── duplicates.go       # Duplicate-group queries (used by analyzer)
│   ├── disk/                   # Disk detection and per-disk stats (Unraid support)
│   │   ├── detector.go         # Detect mounted disks (cache + SSD profiling)
│   │   ├── resolver.go         # DeviceResolver: device_id → friendly name
│   │   ├── space.go            # Per-disk used/free/total
│   │   └── unraid.go           # /mnt/disk* and /mnt/cache awareness
│   ├── duplicates/             # Cross-disk duplicate analysis + consolidation
│   │   ├── analyzer.go         # Group by hash, compute potential savings
│   │   └── consolidator.go     # Hardlink and delete-and-replace strategies
│   ├── scanner/                # Filesystem + service scanner (worker pools)
│   │   ├── scanner.go          # Orchestration (Scan/ResumeScan/runScan/...)
│   │   ├── scanner_services.go # serviceFile interface + 6 adapters,
│   │   │                       # generic update plumbing, UpdateAllServices
│   │   ├── scanner_clients.go  # Per-service updateXxxUsage + associate funcs
│   │   ├── scanner_actions.go  # RescanFiles, RecalculateOrphanedStatus,
│   │   │                       # ScanDiskLocations, RunCleanupScan
│   │   ├── progress.go         # Progress tracker + log pub/sub
│   │   ├── worker.go           # Per-file worker pool
│   │   ├── batch.go            # DB batch insert orchestration
│   │   ├── filesystem.go       # Walk + counting helpers
│   │   ├── disk_scanner.go     # Per-disk location scanner
│   │   ├── hasher.go           # BLAKE3 hashing wrapper
│   │   ├── hashscanner.go      # Hash scan orchestration + progressive verify
│   │   ├── fadvise_linux.go    # POSIX_FADV_SEQUENTIAL hints (Linux build tag)
│   │   └── fadvise_other.go    # No-op fallback for non-Linux
│   ├── server/                 # HTTP server and handlers
│   │   ├── server.go           # Server struct, route table, middleware chain
│   │   ├── handlers.go         # Server, NewServer, LoadTemplates, HandleHealth,
│   │   │                       # render helpers, template funcs
│   │   ├── handlers_pages.go   # Page renderers (index, files page, hardlinks, …)
│   │   ├── handlers_files.go   # /api/files/* + missing-files + export
│   │   ├── handlers_scan.go    # /api/scan/{start,resume,cancel,…}
│   │   ├── handlers_progress.go # Scan progress (HTML polling endpoints)
│   │   ├── handlers_audit.go   # /api/logs, /api/audit-logs
│   │   ├── handlers_config.go  # Config save/test, path-mapping testers
│   │   ├── handlers_disk.go    # /api/disks/*, /api/scan/disk-*
│   │   ├── handlers_hash.go    # /api/hash/* (start, verify, upgrade, level-stats)
│   │   ├── handlers_duplicates.go # /api/duplicates/*, /duplicates page
│   │   ├── handlers_admin.go   # /api/admin/* (clear-files, vacuum, rebuild-fts, …)
│   │   ├── middleware.go       # Recovery, RequestID, Logger, RequestSizeLimit, CORS
│   │   ├── ratelimit.go        # Token bucket rate limiter
│   │   ├── validation.go       # ValidatePage/ValidateLimit/CalculateTotalPages
│   │   ├── errors.go           # respondError + JSON error envelope
│   │   └── types.go            # Page-data structs (FilesData, …)
│   └── stats/                  # Statistics calculations
│       ├── stats.go            # Stats queries and aggregation
│       └── cache.go            # TTL cache (binary; SWR planned in plans/007)
├── web/
│   ├── templates/              # Go HTML templates (parsed at server startup)
│   │   ├── layout.html         # Base layout with nav
│   │   ├── dashboard.html      # Homepage (now references partials/dashboard_*)
│   │   ├── files.html          # File browser (search, filter, delete)
│   │   ├── duplicates.html     # Duplicate groups view + consolidation actions
│   │   ├── hardlinks.html      # Hardlink groups view
│   │   ├── scans.html          # Scan history
│   │   ├── logs.html           # Scan-log + audit-log browser
│   │   ├── stats.html          # Statistics with Chart.js
│   │   ├── config.html         # Configuration editor
│   │   ├── advanced.html       # Advanced tools (export, bulk delete, admin)
│   │   ├── duplicates_table.html  # HTMX partial — duplicates table body
│   │   ├── logs_table.html        # HTMX partial — logs table body
│   │   ├── audit_logs_table.html  # HTMX partial — audit log body
│   │   └── partials/           # Per-page partials parsed alongside their owner
│   │       ├── validation-errors.html
│   │       ├── dashboard_progress.html        # scan + hash progress cards
│   │       ├── dashboard_disks.html           # disk status block
│   │       ├── dashboard_services.html        # per-service breakdown
│   │       └── dashboard_manual_updates.html  # manual update buttons
│   └── static/
│       ├── css/
│       │   ├── input.css       # Tailwind input
│       │   ├── accessibility.css
│       │   └── styles.css      # Generated by `make tailwind` (gitignored)
│       └── js/                 # Custom dialog, dropdowns, icons, etc.
├── config.example.yaml         # Example configuration
├── VERSION                     # Current version (MAJOR.MINOR.PATCH)
├── Dockerfile                  # Multi-stage build
├── docker-compose.yml          # Example Docker Compose setup
├── Makefile                    # Build commands
└── README.md                   # User documentation
```

**Note on file organization:** Many handler/query/scanner files were split out
of monolithic originals (handlers.go was 5,658 lines; queries.go was 3,426;
scanner.go was 2,261). Methods stayed on `*Server` / `*DB` / `*Scanner`, so
the public APIs are unchanged. When adding new functionality, place it in the
file whose responsibility matches; if no clear home exists, prefer creating
a new well-named file over appending to a large existing one.

## Key Architecture Patterns

### 1. Service Client Factory Pattern

All service clients are created through `api.ClientFactory` for consistency and testability.

**Location**: [internal/api/factory.go](internal/api/factory.go)

```go
// Creating clients
factory := api.NewClientFactory(config)
client, err := factory.CreateClient("plex", timeout)

// Adding a new service requires:
// 1. Create client in internal/api/{service}.go
// 2. Add config struct to internal/config/config.go
// 3. Add case to factory.CreateClient() and IsServiceConfigured()
// 4. Update scanner to call new service
// 5. Update database schema CHECK constraint
// 6. Update all UI components (config, dashboard, files, stats)
// 7. Update internal/stats/stats.go service list
// 8. Update internal/server/handlers.go checkExternalServices()
// 9. Bump VERSION file appropriately
```

### 2. Path Mapping System

Files exist at different paths in different containers. Path mappers translate between them.

**Location**: [internal/config/pathmapper.go](internal/config/pathmapper.go)

**Two types of mappings**:

- **Local Path Mappings**: How media-finder sees paths vs. actual host paths
- **Service Path Mappings**: How each service sees paths vs. host paths

**Example**:

- Host: `/mnt/user/data/media/movies/Movie.mkv`
- Media-finder: `/media/movies/Movie.mkv` (via local_path_mappings)
- Radarr: `/movies/Movie.mkv` (via service_path_mappings.radarr)

Path mapping is critical for matching files reported by external services.

### 3. Worker Pool Scanner

File scanning uses a worker pool for concurrent processing to handle large datasets efficiently.

**Location**: [internal/scanner/scanner.go](internal/scanner/scanner.go)

**Scan phases**:

1. **Initializing**: Create scan record, setup context
2. **Counting**: Count total files to scan
3. **Scanning**: Walk filesystem with worker pool
4. **Updating {Service}**: Query each configured service API
5. **Calculating Orphans**: Mark files not used by any service
6. **Completed**: Update scan record, trigger cache invalidation

**Configuration**:

- `scan_workers`: Number of concurrent file processors (default: 10)
- `scan_buffer_size`: Channel buffer size (default: 100)

### 4. Database Schema

**Location**: [internal/database/schema.go](internal/database/schema.go)

**Core tables**:

- `files`: All scanned files with inode/device for hardlink detection
- `usage`: Many-to-many relationship (files ↔ services)
- `scans`: Scan history with resumable state
- `audit_log`: Tracks deletions and config changes
- `files_fts`: Full-text search virtual table (FTS5)

**Key indexes**:

- `idx_files_hardlink`: `(device_id, inode)` for hardlink grouping
- `idx_files_orphaned_size`: `(is_orphaned, size)` for orphan queries
- `idx_usage_service`: For service-specific queries

**Important**: When adding a service, update the CHECK constraint on `usage.service`:

```sql
service TEXT NOT NULL CHECK(service IN ('plex', 'sonarr', 'radarr', 'qbittorrent', 'stash', 'calibre'))
```

### 5. HTMX Partial Rendering

The web UI uses HTMX for dynamic updates without full page reloads.

**Pattern**: Handlers detect HTMX requests and return HTML fragments

```go
// Check if this is an HTMX request
isHTMX := r.Header.Get("HX-Request") == "true"

if isHTMX {
    // Return just the updated HTML fragment
    tmpl.ExecuteTemplate(w, "content", data)
} else {
    // Return full page with layout
    tmpl.ExecuteTemplate(w, "layout.html", data)
}
```

**Common HTMX attributes**:

- `hx-get`: Load content from URL
- `hx-post`: POST to URL
- `hx-target`: Where to insert response
- `hx-swap`: How to insert (innerHTML, outerHTML, etc.)
- `hx-include`: Include form fields in request
- `hx-confirm`: Triggers custom confirmation dialog (automatically intercepted)

### 6. Custom UI Components

**IMPORTANT**: The application uses custom UI components instead of browser defaults for consistency, accessibility, and better UX.

#### Custom Dialog System

**Location**: [web/static/js/modal.js](web/static/js/modal.js)

**NEVER use browser native `alert()` or `confirm()`.** Always use the custom dialog system:

```javascript
// For confirmations (returns Promise<boolean>)
const confirmed = await confirmDialog(
    'Are you sure you want to proceed?',
    'Confirm Action'  // Optional title
);

if (confirmed) {
    // User clicked "Confirm"
}

// For alerts/notifications (returns Promise<boolean>)
await alertDialog(
    'Operation completed successfully!',
    'Success',  // Optional title
    'success'   // Optional type: 'info', 'success', 'warning', 'error'
);

// For toast notifications (non-blocking)
showToast(
    'File deleted successfully',
    'success',  // Type: 'info', 'success', 'warning', 'error'
    { duration: 5000 }  // Optional options
);
```

**Benefits**:
- Styled to match app's dark theme
- Accessible (keyboard navigation, ARIA attributes)
- Mobile-friendly and responsive
- Promise-based for easy async/await usage
- Toast notifications for non-blocking messages

**HTMX Integration**: The system automatically intercepts `hx-confirm` attributes:

```html
<button hx-post="/api/delete" hx-confirm="Delete this file?">
    Delete
</button>
```

#### Custom Dropdown System

**Location**: [web/static/js/custom-dropdown.js](web/static/js/custom-dropdown.js)

**NEVER use native `<select>` elements.** Always use custom dropdowns for consistency:

```html
<div class="relative" data-custom-dropdown>
    <input type="hidden" id="filter-value" value="default" data-dropdown-input>
    <button
        type="button"
        data-dropdown-button
        aria-expanded="false"
        class="w-full px-4 py-2 bg-gray-700 border border-gray-600 rounded">
        <span data-dropdown-text>Select Option</span>
        <svg class="w-4 h-4 absolute right-3 top-1/2 transform -translate-y-1/2">
            <!-- Chevron icon -->
        </svg>
    </button>
    <div data-dropdown-menu class="hidden absolute z-50 w-full mt-1 bg-gray-700">
        <div data-dropdown-option data-value="option1" class="px-4 py-2 hover:bg-gray-600">
            Option 1
        </div>
        <div data-dropdown-option data-value="option2" class="px-4 py-2 hover:bg-gray-600">
            Option 2
        </div>
    </div>
</div>
```

**Benefits**:
- Fully styled and themeable
- Better mobile support than native selects
- Consistent across all browsers
- Accessible with keyboard navigation
- Automatically initializes on page load

**See examples**: [web/templates/files.html](web/templates/files.html), [web/templates/duplicates.html](web/templates/duplicates.html)

### 7. Progress endpoints (currently HTML polling, SSE planned)

Today the dashboard polls progress endpoints every 2s:

- `GET /api/scan/progress-html` — scan progress fragment
- `GET /api/hash/progress-html` — hash scan progress fragment
- `GET /api/scan/disk-progress-html` — disk-location scan progress fragment

The pub/sub broadcaster in [internal/scanner/progress.go](internal/scanner/progress.go)
(`Subscribe`/`Unsubscribe`/`broadcastLogs`) is real SSE infrastructure used today
only for log streaming (`/api/scan/logs`). Migrating progress to push-based SSE is
planned (see local `plans/005-sse-progress.md` if present).

**Flow today (polling)**:

1. Page loads with `hx-get="/api/...-progress-html" hx-trigger="load, every 2s"`
2. Server renders the current snapshot as an HTML fragment
3. HTMX swaps the inner HTML on each tick
4. JS removes the `hx-trigger` attribute when the scan completes (via the
   `scanCompleted` event header)

### 8. Statistics Caching

Statistics are expensive to calculate, so they're cached with configurable TTL.

**Location**: [internal/stats/cache.go](internal/stats/cache.go)

**Configuration**: `stats_cache_ttl` in config.yaml (default: 30s)

**Invalidation**: Cache is invalidated when scans complete

**IMPORTANT**: When adding a new service, update the hardcoded service list in [internal/stats/stats.go](internal/stats/stats.go):

```go
// SQL query WHERE clause
WHERE u.service IN ('plex', 'sonarr', 'radarr', 'qbittorrent', 'stash', 'calibre')

// Initialize services array
services := []string{"plex", "sonarr", "radarr", "qbittorrent", "stash", "calibre"}
```

## Adding a New Service - Complete Checklist

This is the comprehensive process based on adding Stash support:

### Backend Changes

1. **Create API Client** - `internal/api/{service}.go`
   - Implement client struct with auth fields
   - Implement `Test() error` method
   - Implement `GetAllFiles() ([]ServiceFile, error)` method
   - Handle pagination if required by API
   - Service file struct should have path, size, and metadata fields

2. **Update Configuration** - `internal/config/config.go`
   - Add `{Service}Config` struct with auth fields
   - Add field to `Services` struct

3. **Update Client Factory** - `internal/api/factory.go`
   - Add case to `CreateClient()` switch statement
   - Add `Create{Service}Client()` method
   - Add case to `IsServiceConfigured()`

4. **Update Scanner** - `internal/scanner/scanner.go`
   - Create `{service}ServiceFile` wrapper implementing `serviceFile` interface
   - Implement `GetPath()` and `GetMetadata()` methods
   - Add `update{Service}Usage()` method following existing patterns
   - Add call to scanner's main scan loop

5. **Update Database Schema** - `internal/database/schema.go`
   - Add service name to CHECK constraint on `usage.service` column

   ```sql
   CHECK(service IN ('plex', 'sonarr', 'radarr', 'qbittorrent', 'stash'))
   ```

6. **Update Statistics** - `internal/stats/stats.go`
   - Add service to SQL WHERE clause (line ~102)
   - Add service to services initialization array (line ~113)

7. **Update Server Handlers** - `internal/server/handlers.go`
   - Add service to `checkExternalServices()` serviceNames array
   - Add config save logic in `HandleSaveConfig()`
   - Add test logic in `HandleTestService()`

8. **Update Error Messages** - `internal/server/errors.go`
   - Add service to valid services list in error messages

9. **Update Example Config** - `config.example.yaml`
   - Add service configuration section
   - Add path mappings example

### Frontend Changes

1. **Configuration Page** - `web/templates/config.html`
    - Add service configuration section with URL/auth fields
    - Add "Test Connection" button with HTMX
    - Include proper form field names for saving

2. **Dashboard** - `web/templates/dashboard.html`
    - Add "Update {Service}" button to individual services section
    - Ensure layout accommodates new service (may need grid reorganization)
    - Primary actions (Update All Services, Recalculate Orphaned) should remain prominent

3. **Files Page** - `web/templates/files.html`
    - Add service to dropdown filter options
    - Add service to display logic in dropdown button text
    - Add active state highlighting for filter

4. **Statistics Page** - `web/templates/stats.html`
    - Add color to Chart.js backgroundColor arrays
    - Ensure service appears in pie/doughnut charts
    - Color palette: Blue (Plex), Green (Sonarr), Amber (Radarr), Red (qBittorrent), Purple (Stash)

### Version Management

1. **Bump VERSION File**
    - Minor version bump for new feature (e.g., 0.21.0 → 0.22.0)
    - Patch version for any subsequent bug fixes

2. **Rebuild Frontend Assets**

    ```bash
    make tailwind  # If any Tailwind classes were added
    ```

### Testing

1. **Manual Testing**
    - Config page: Test connection button works
    - Dashboard: Individual service update works
    - Dashboard: Service appears in breakdown with correct status
    - Files page: Service filter works, files show correct service badges
    - Statistics page: Service appears in charts with assigned color
    - Full scan: Service files are properly tracked in database

### Common Pitfalls

- **Forgetting VERSION bump**: Always update VERSION file with commits
- **Hardcoded service lists**: Check stats.go, handlers.go for hardcoded arrays
- **Database constraint**: Must update CHECK constraint or inserts will fail
- **Layout issues**: Adding services to grids may require reorganization
- **Tailwind rebuild**: Required if adding new color classes
- **Path mappings**: Service must have proper path mappings in config

## Configuration Management

**Location**: [config.example.yaml](config.example.yaml)

### Database Configuration

```yaml
database_path: /appdata/data/media-finder.db  # SQLite database location
db_max_open_conns: 25      # Max concurrent database connections
db_max_idle_conns: 5       # Idle connection pool size
db_conn_max_lifetime: 5m   # Connection max lifetime
```

### Performance Configuration

```yaml
scan_workers: 10           # Number of concurrent file processors
scan_buffer_size: 100      # File processing queue buffer size
api_timeout: 30s           # Timeout for service API calls
stats_cache_ttl: 30s       # Statistics cache duration

# Hash verification performance (for 128GB RAM servers)
hash_buffer_size: "4MB"    # Buffer for file reads (512KB-16MB range)
                           # Larger = faster for big files (fewer syscalls)
                           # 4MB default optimal for most media files
                           # 8MB recommended for 4K/8K remuxes (50GB+)

db_cache_size: 1000000     # SQLite cache size in KB (~1GB)
                           # Larger = faster queries during scans
                           # Adjust based on available RAM
```

### Server Configuration

```yaml
server_port: 8080          # HTTP server port (use 8787 in Docker)
cors_allowed_origin: "*"   # CORS setting ("*" for dev, specific URL for prod)
```

### Path Mappings

Critical for matching files across containers. See "Path Mapping System" above.

## Development Workflow

### Making Changes

1. **Create feature branch** (if using Git workflow)
2. **Make code changes** following patterns above
3. **Update VERSION file** appropriately
4. **Rebuild frontend if needed**: `make tailwind`
5. **Test locally**: `make dev` or `make build && ./bin/media-finder serve`
6. **Commit with clear message**: `git commit -m "feat: Add feature X"`
7. **Build Docker image**: `make docker-build`

### Git Commit Conventions

Follow conventional commits for clarity:

- `feat:` New features (minor version bump)
- `fix:` Bug fixes (patch version bump)
- `refactor:` Code refactoring without behavior change
- `docs:` Documentation changes
- `chore:` Build process, dependencies, tooling

### Debugging Tips

**Database inspection**:

```bash
sqlite3 /path/to/media-finder.db
.schema files
SELECT COUNT(*) FROM files WHERE is_orphaned = 1;
```

**Check scan status**:

```bash
# Via CLI
./bin/media-finder stats

# Via database
SELECT * FROM scans ORDER BY started_at DESC LIMIT 5;
```

**SSE debugging**:
Open browser DevTools → Network → Filter by "progress" → View event stream

**Service API issues**:

- Check path mappings in config.yaml
- Test connection via config page UI
- Review service API documentation for auth requirements
- Check Docker network connectivity between containers

## Template System

**Location**: [web/templates/](web/templates/)

### Structure

- **layout.html**: Base template with `<head>`, navigation, footer
- **Page templates**: Define `{{define "content"}}` block
- **HTMX partials**: Return fragments for dynamic updates

### Template Functions

Custom functions available in templates (defined in [internal/server/handlers.go](internal/server/handlers.go)):

- `formatBytes`: Convert bytes to human-readable (KB, MB, GB, TB)
- `formatTimestamp`: Unix timestamp → readable date/time
- `formatDuration`: Seconds → duration string
- `percentage`: Calculate percentage with 1 decimal
- `hasPrefix`: String prefix checking
- `truncate`: Truncate string with ellipsis
- `add`, `subtract`, `multiply`, `divide`: Math operations

### Adding a New Page

1. Create `web/templates/{page}.html`
2. Add `{{define "content"}}...{{end}}` block
3. Add page name to `pages` array in `server.LoadTemplates()`
4. Create handler in `internal/server/handlers.go`
5. Add route in `server.SetupRoutes()`

## API Endpoints

### Page Routes (HTML)

- `GET /` - Dashboard
- `GET /files` - File browser
- `GET /duplicates` - Duplicate groups + consolidation actions
- `GET /hardlinks` - Hardlink groups
- `GET /scans` - Scan history
- `GET /logs` - Scan log + audit log browser
- `GET /stats` - Statistics with charts
- `GET /config` - Configuration editor
- `GET /advanced` - Advanced tools (export, bulk delete, admin)
- `GET /health` - Health check endpoint (JSON)

### API Routes (JSON/HTMX)

**Scan Management**:

- `POST /api/scan/start` - Start full scan
- `POST /api/scan/start?incremental=true` - Start incremental scan
- `POST /api/scan/resume` - Resume interrupted scan
- `POST /api/scan/cancel` - Gracefully cancel scan
- `POST /api/scan/force-stop` - Force stop scan
- `POST /api/scan/cleanup` - Walk filesystem and remove DB entries for missing files
- `GET /api/scan/progress` - JSON snapshot of current progress
- `GET /api/scan/progress-html` - HTML fragment for HTMX polling
- `GET /api/scan/logs` - Live log SSE stream for the running scan

**Service Updates**:

- `POST /api/scan/update-services` - Update all configured services
- `POST /api/scan/update-service?service={name}` - Update single service
- `POST /api/scan/recalculate-orphaned` - Recalculate orphaned status without re-scanning

**Disk Scanning** (Unraid / multi-disk):

- `GET /api/disks/detect` - Detect mounted disks
- `POST /api/scan/disk-locations` - Start a per-disk location scan
- `GET /api/scan/disk-progress` - Disk scan progress JSON
- `GET /api/scan/disk-progress-html` - Disk scan progress HTML fragment

**Hash Scanning + Duplicate Verification**:

- `POST /api/hash/start` - Start hash calculation pass
- `POST /api/hash/cancel` - Cancel current hash scan
- `POST /api/hash/clear` - Clear all stored hashes
- `GET /api/hash/progress` - Hash scan progress JSON
- `GET /api/hash/progress-html` - HTML fragment
- `POST /api/hash/verify` - Verify duplicate groups via full hash
- `POST /api/hash/verify-progressive` - Progressive verification (1MB → 10MB → … → full)
- `GET /api/hash/level-stats` - Coverage stats per progressive level
- `POST /api/hash/upgrade-all` - Upgrade all hashes to the next level
- `POST /api/hash/upgrade-group-full` - Upgrade one group to full hash
- `POST /api/hash/upgrade-group-progressive` - Upgrade one group progressively

**Duplicates**:

- `GET /api/duplicates/count` - Group count for a query (HTMX)
- `POST /api/duplicates/preview` - Preview consolidation plan
- `POST /api/duplicates/consolidate` - Run consolidation (delete + reference)
- `POST /api/duplicates/hardlink` - Hardlink duplicates instead of deleting
- `POST /api/duplicates/refresh-inodes` - Refresh inode/device for a group

**File Operations**:

- `GET /api/files/extensions` - Distinct extensions (filter dropdown)
- `POST /api/files/delete` - Delete a single file (DB row + optional fs)
- `POST /api/files/batch-delete` - Bulk delete by IDs
- `POST /api/files/rescan` - Mark files for rescan
- `GET /api/files/{id}/details` - File detail card (HTMX)
- `GET /api/files/{id}/disk-locations` - Disk-location list for a file

**Missing Files** (files services report but aren't on disk):

- `GET /api/missing-files` - Latest missing-file scan results
- `GET /api/missing-files/export` - Export missing-files list

**Logs + Audit**:

- `GET /api/logs` - Paginated/filterable scan logs (HTMX table)
- `GET /api/audit-logs` - Paginated/filterable audit log

**Configuration**:

- `POST /api/config/save` - Save configuration
- `POST /api/config/test?service={name}` - Test service connection
- `POST /api/config/test-scan-paths` - Validate scan paths exist
- `POST /api/config/test-path-mappings` - Validate per-service path mappings
- `GET /api/plex/libraries` - Discover Plex libraries (config helper)

**Export**:

- `GET /api/export?format={json|csv}&orphaned=true` - Export filtered file list

**Admin** (destructive — confirm before invoking):

- `POST /api/admin/clear-files` - Delete all rows from files table
- `POST /api/admin/clear-scans` - Delete all scan records
- `POST /api/admin/clear-usage` - Delete all usage rows
- `POST /api/admin/vacuum` - Run SQLite VACUUM
- `POST /api/admin/rebuild-fts` - Rebuild the FTS5 index
- `POST /api/admin/clean-stale-scans` - Mark stuck "running" scans as interrupted
- `POST /api/admin/recalculate-orphaned` - Force orphan recalc
- `GET /api/admin/database-stats` - Detailed DB stats (size, freelist, etc.)
- `GET /api/admin/audit-log` - Raw audit log dump
- `POST /api/admin/clear-config` - Wipe config table
- `POST /api/admin/clear-audit-log?older_than_days=N` - Trim audit log

## Security Considerations

### Authentication

Currently no authentication is implemented. If exposing to internet:

- Use reverse proxy with authentication (Authelia, Nginx auth, etc.)
- Consider adding `AUTH_TOKEN` environment variable for API access

### File Deletion

- Always requires confirmation in UI
- Dry-run mode available for bulk operations
- Audit log tracks all deletions

### Service Credentials

- API keys stored in plain text in config.yaml
- Ensure config.yaml has appropriate permissions (600)
- Never commit config.yaml with real credentials

## Performance Optimization

### For Large Datasets (90TB+)

**Scanning**:

- Increase `scan_workers` (default 10, try 20-30 for fast disks)
- Increase `scan_buffer_size` (default 100, try 500-1000)
- Use incremental scans after initial full scan
- Mount media as read-only (`:rw`) in Docker

**Database**:

- SQLite WAL mode enabled by default (allows concurrent reads)
- Comprehensive indexes for common query patterns
- Use `VACUUM` periodically to optimize database file
- Increase `db_cache_size` for high-RAM systems (default: 1GB for 128GB RAM)

**Hash Verification** (v0.41.0+):

- **Buffer size optimization**: Use larger buffers for faster hashing
  - Default: 4MB (optimal for most media files)
  - Large files (50GB+ 4K remuxes): Try 8MB
  - Available RAM: Even 16MB × 10 workers = only 160MB
- **OS-level optimizations**: Automatic `POSIX_FADV_SEQUENTIAL` hints
  - Doubles kernel read-ahead for 20-40% faster sequential reads
  - Automatic cache management for files >1GB
- **Hardware acceleration**: BLAKE3 auto-uses AVX2/AVX-512 if available
  - i9-12900K: AVX-512 support depends on manufacturing date (2021=yes, 2022+=no)

**Statistics**:

- Adjust `stats_cache_ttl` based on scan frequency
- Dashboard uses cached stats to avoid expensive queries

**API Timeouts**:

- Increase `api_timeout` if services are slow to respond
- Consider using qui proxy for qBittorrent to reduce load

## Troubleshooting

### "Scan already running" error

- Check for interrupted scans: `SELECT * FROM scans WHERE status = 'running'`
- Mark as interrupted: `UPDATE scans SET status = 'interrupted' WHERE id = X`

### Files not matching between services

- Verify path mappings in config.yaml
- Check service sees correct paths (e.g., Sonarr Settings → Media Management)
- Test path mapper logic in code

### SSE connection drops

- Check CORS settings if frontend on different origin
- Increase reverse proxy timeouts for SSE endpoints
- Browser may close connection; UI will show disconnected state

### Tailwind classes not applying

- Run `make tailwind` after adding new classes
- Check `web/static/css/styles.css` was regenerated
- Clear browser cache

## Additional Resources

- **Project Repository**: <https://github.com/mmenanno/media-usage-finder>
- **Docker Images**: ghcr.io/mmenanno/media-usage-finder
- **Plex API Docs**: <https://www.plexopedia.com/plex-media-server/api/>
- **Sonarr API Docs**: <https://sonarr.tv/docs/api/>
- **Radarr API Docs**: <https://radarr.video/docs/api/>
- **qBittorrent API Docs**: <https://github.com/qbittorrent/qBittorrent/wiki/WebUI-API-Documentation>
- **Stash API Docs**: <https://github.com/stashapp/stash/blob/develop/graphql/schema/schema.graphql>

## Quick Reference

### Before Committing

- [ ] Updated VERSION file if needed
- [ ] Ran `make tailwind` if HTML/CSS changed
- [ ] Tested locally with `make dev`
- [ ] Clear commit message following conventions

### Adding New Service

- [ ] Created API client (api/{service}.go)
- [ ] Updated config struct (config/config.go)
- [ ] Updated factory (api/factory.go)
- [ ] Updated scanner (scanner/scanner.go)
- [ ] Updated schema CHECK constraint (database/schema.go)
- [ ] Updated stats service lists (stats/stats.go)
- [ ] Updated handler service checks (server/handlers.go)
- [ ] Updated all UI pages (config, dashboard, files, stats)
- [ ] Updated config.example.yaml
- [ ] Bumped VERSION to minor version
- [ ] Tested all integration points

### Common File Locations

- Version: `VERSION`
- Config example: `config.example.yaml`
- Main entry: `cmd/media-finder/main.go`
- Database schema: `internal/database/schema.go`
- Database queries: `internal/database/queries*.go` (split by entity — see Project Structure)
- Scanner orchestration: `internal/scanner/scanner.go`
- Service-update plumbing: `internal/scanner/scanner_services.go`
- Per-service implementations: `internal/scanner/scanner_clients.go`
- Hash scanner: `internal/scanner/hashscanner.go`
- Disk detection / Unraid: `internal/disk/`
- Duplicate analysis + consolidation: `internal/duplicates/`
- Server routes: `internal/server/server.go`
- HTTP handlers: `internal/server/handlers*.go` (split by domain — see Project Structure)
- Templates: `web/templates/` (with `partials/` for per-page partials)
- CSS input: `web/static/css/input.css`

---

*Last verified against: v0.61.4. If you make architectural changes, please
update the relevant sections of this document and the version footer.*
