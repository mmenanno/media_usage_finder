# Media Usage Finder - Claude Code Guide

## Project Overview

**Media Usage Finder** is a high-performance Go application that scans media server files and tracks which services are using them. It helps identify orphaned files, detect hardlinks, and optimize storage across large media servers (90TB+ datasets).

### Core Purpose

- Track file usage across multiple media management services
- Identify orphaned files not tracked by any service
- Detect hardlinks to calculate actual vs. apparent storage usage
- Provide a modern web UI for browsing, filtering, and managing files

### Tech Stack

- **Backend**: Go 1.21+
- **Database**: SQLite with WAL mode, FTS5, comprehensive indexing
- **Frontend**: HTMX for dynamic updates, Tailwind CSS for styling, Chart.js for visualizations
- **Real-time**: Server-Sent Events (SSE) for live scan progress
- **Deployment**: Docker/Docker Compose ready

### Supported Services

- **Plex** - Media server (REST API with token auth)
- **Sonarr** - TV show management (REST API with API key)
- **Radarr** - Movie management (REST API with API key)
- **qBittorrent** - Torrent client (Web API or qui proxy)
- **Stash** - Adult content organizer (GraphQL API with API key)

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
make test                 # Run all Go tests

# Docker
make docker-build         # Build Docker image (uses VERSION file)
make docker-run           # Run Docker container
make docker-stop          # Stop and remove container

# Cleanup
make clean                # Remove bin/ and built CSS

# Version Management
# ALWAYS bump the VERSION file when making changes
# Format: MAJOR.MINOR.PATCH (e.g., 0.22.2)
# - MAJOR: Breaking changes
# - MINOR: New features (like adding a service)
# - PATCH: Bug fixes
```

## Project Structure

```text
media_usage_finder/
├── cmd/media-finder/           # CLI entrypoint (main.go, commands)
├── internal/
│   ├── api/                    # Service API clients
│   │   ├── factory.go          # Client factory pattern (CreateClient)
│   │   ├── arr.go              # Shared Sonarr/Radarr client
│   │   ├── plex.go             # Plex REST API client
│   │   ├── sonarr.go           # Sonarr v3 API client
│   │   ├── radarr.go           # Radarr v3 API client
│   │   ├── qbittorrent.go      # qBittorrent Web API client
│   │   └── stash.go            # Stash GraphQL client
│   ├── config/                 # Configuration management
│   │   ├── config.go           # Config struct, YAML loading
│   │   └── pathmapper.go       # Path translation (container <-> host)
│   ├── database/               # SQLite database layer
│   │   ├── db.go               # Connection, queries, transactions
│   │   ├── schema.go           # Schema definition and migrations
│   │   └── migrations.go       # Schema versioning
│   ├── scanner/                # File scanner with worker pools
│   │   ├── scanner.go          # Main scanner orchestration
│   │   ├── progress.go         # Progress tracking for SSE
│   │   └── walker.go           # Concurrent file walking
│   ├── server/                 # HTTP server and handlers
│   │   ├── server.go           # Server struct, routing
│   │   ├── handlers.go         # Page handlers, API endpoints
│   │   ├── sse.go              # Server-Sent Events for real-time updates
│   │   └── errors.go           # Error response helpers
│   ├── stats/                  # Statistics calculations
│   │   ├── stats.go            # Stats queries and aggregation
│   │   └── cache.go            # TTL-based caching
│   └── constants/              # Shared constants
├── web/
│   ├── templates/              # Go HTML templates
│   │   ├── layout.html         # Base layout with nav
│   │   ├── dashboard.html      # Homepage (stats, scan controls)
│   │   ├── files.html          # File browser (search, filter, delete)
│   │   ├── hardlinks.html      # Hardlink groups view
│   │   ├── scans.html          # Scan history
│   │   ├── stats.html          # Statistics with charts
│   │   ├── config.html         # Configuration editor
│   │   └── advanced.html       # Advanced tools (export, bulk delete)
│   └── static/
│       └── css/
│           ├── input.css       # Tailwind input file
│           └── styles.css      # Generated CSS (gitignored)
├── config.example.yaml         # Example configuration
├── VERSION                     # Current version (MAJOR.MINOR.PATCH)
├── Dockerfile                  # Multi-stage build
├── docker-compose.yml          # Example Docker Compose setup
├── Makefile                    # Build commands
└── README.md                   # User documentation
```

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
service TEXT NOT NULL CHECK(service IN ('plex', 'sonarr', 'radarr', 'qbittorrent', 'stash'))
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
        <div data-dropdown-option data-value="option1" class="px-4 py-2 hover:bg-gray-600 cursor-pointer">
            Option 1
        </div>
        <div data-dropdown-option data-value="option2" class="px-4 py-2 hover:bg-gray-600 cursor-pointer">
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

### 7. Server-Sent Events (SSE)

Real-time scan progress uses SSE to stream updates to the browser.

**Location**: [internal/server/sse.go](internal/server/sse.go)

**Flow**:

1. Browser connects to `/api/scan/progress`
2. Server streams JSON events with progress updates
3. JavaScript updates UI in real-time
4. Connection closes when scan completes

### 8. Statistics Caching

Statistics are expensive to calculate, so they're cached with configurable TTL.

**Location**: [internal/stats/cache.go](internal/stats/cache.go)

**Configuration**: `stats_cache_ttl` in config.yaml (default: 30s)

**Invalidation**: Cache is invalidated when scans complete

**IMPORTANT**: When adding a new service, update the hardcoded service list in [internal/stats/stats.go](internal/stats/stats.go):

```go
// Line ~102: SQL query WHERE clause
WHERE u.service IN ('plex', 'sonarr', 'radarr', 'qbittorrent', 'stash')

// Line ~113: Initialize services array
services := []string{"plex", "sonarr", "radarr", "qbittorrent", "stash"}
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
- `GET /hardlinks` - Hardlink groups
- `GET /scans` - Scan history
- `GET /stats` - Statistics
- `GET /config` - Configuration editor
- `GET /advanced` - Advanced tools

### API Routes (JSON/HTMX)

**Scan Management**:

- `POST /api/scan/start` - Start full scan
- `POST /api/scan/start?incremental=true` - Start incremental scan
- `POST /api/scan/cancel` - Gracefully cancel scan
- `POST /api/scan/force-stop` - Force stop scan
- `POST /api/scan/resume` - Resume interrupted scan
- `GET /api/scan/progress` - SSE endpoint for real-time progress

**Service Updates**:

- `POST /api/scan/update-services` - Update all configured services
- `POST /api/scan/update-service?service={name}` - Update single service
- `POST /api/scan/recalculate-orphaned` - Recalculate orphaned status

**File Operations**:

- `GET /api/files` - List files (supports filtering, pagination, search)
- `DELETE /api/files?path={path}` - Delete single file
- `DELETE /api/files/orphaned` - Bulk delete orphaned files
- `POST /api/files/mark-rescan` - Mark files for rescan

**Configuration**:

- `POST /api/config/save` - Save configuration
- `POST /api/config/test?service={name}` - Test service connection
- `GET /api/config/reload` - Reload configuration from disk

**Statistics**:

- `GET /api/stats` - Get statistics (cached)

**Export**:

- `GET /api/export?format={json|csv}` - Export files

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
- Scanner: `internal/scanner/scanner.go`
- Server routes: `internal/server/server.go`
- Templates: `web/templates/`
- CSS input: `web/static/css/input.css`
