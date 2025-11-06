# Media Usage Finder

A high-performance Go application that scans your media server files and tracks which services (Plex, Sonarr, Radarr, qBittorrent) are using them. Helps identify orphaned files, detect hardlinks, and optimize storage.

## Features

- 🚀 **Fast Concurrent Scanning** - Multi-threaded file scanning with configurable worker pools
- 📊 **Service Integration** - Tracks file usage across Plex, Sonarr, Radarr, qBittorrent, and Stash
- 🔗 **Hardlink Detection** - Identifies hardlinked files to track space savings
- 🎯 **Orphaned File Detection** - Find files not tracked by any service
- 💿 **Cross-Disk Duplicate Detection** - Find duplicate files across multiple disks (Unraid support)
- 🔄 **Duplicate Consolidation** - Automatically consolidate duplicates to optimize storage
- 💾 **Incremental Scans** - Only rescan modified files for faster updates
- 🌐 **Modern Web UI** - Dark-themed interface with HTMX for real-time updates
- 📈 **Detailed Statistics** - Storage efficiency, service breakdown, disk usage
- 🔄 **Resumable Scans** - Graceful interruption and resumption
- 🐳 **Docker Ready** - Easy deployment with Docker/Docker Compose
- 🖥️ **Unraid Integration** - Native support for accurate disk statistics

## Quick Start

### Using Docker Compose

1. Create a `docker-compose.yml`:

```yaml
version: '3.8'

services:
  media-finder:
    image: ghcr.io/mmenanno/media-usage-finder:latest
    container_name: media-finder
    ports:
      - "8787:8787"
    volumes:
      - ./appdata:/appdata
      - /mnt/user/data/media:/media:ro
      - /mnt/user/data/downloads/torrents:/downloads:ro
    environment:
      - TZ=America/New_York
    restart: unless-stopped
```

1. Create a configuration file at `./appdata/config/config.yaml`:

```yaml
database_path: /appdata/data/media-finder.db
scan_workers: 10
api_timeout: 30s

local_path_mappings:
  - container: /media
    host: /mnt/user/data/media
  - container: /downloads
    host: /mnt/user/data/downloads/torrents

service_path_mappings:
  plex:
    - container: /media
      host: /mnt/user/data/media
  sonarr:
    - container: /tv
      host: /mnt/user/data/media/tv
  radarr:
    - container: /movies
      host: /mnt/user/data/media/movies
  qbittorrent:
    - container: /downloads
      host: /mnt/user/data/downloads/torrents

services:
  plex:
    url: http://plex:32400
    token: YOUR_PLEX_TOKEN
  sonarr:
    url: http://sonarr:8989
    api_key: YOUR_SONARR_API_KEY
  radarr:
    url: http://radarr:7878
    api_key: YOUR_RADARR_API_KEY
  qbittorrent:
    url: http://qbittorrent:8080
    username: admin
    password: adminpass

scan_paths:
  - /media
  - /downloads
```

1. Start the container:

```bash
docker-compose up -d
```

The application will automatically generate a default configuration file on first run if one doesn't exist.

1. Access the web UI at `http://localhost:8787`

### Using CLI

```bash
# Start web server (listens on port 8787)
media-finder serve

# Run a scan
media-finder scan

# Run incremental scan
media-finder scan --incremental

# View statistics
media-finder stats

# Export orphaned files
media-finder export --orphaned --format json -o orphaned.json

# Mark files for rescan
media-finder mark-rescan --orphaned
media-finder mark-rescan --filter "path LIKE '%season%'"

# Delete files
media-finder delete --path /path/to/file
media-finder delete --orphaned --dry-run
media-finder delete --orphaned  # requires confirmation

# Validate configuration
media-finder config validate
```

## Configuration

### Path Mappings

Path mappings are crucial for matching files across different Docker containers. There are two types:

1. **Local Path Mappings** - How media-finder sees paths vs actual host paths
1. **Service Path Mappings** - How each service (Plex, Sonarr, etc.) sees paths vs actual host paths

**Note:** The application stores its configuration and database in `/appdata` which should be mapped to a persistent volume on your host.

Example:

- Unraid actual path: `/mnt/user/data/media/movies/Movie.mkv`
- Media-finder sees: `/media/movies/Movie.mkv`
- Radarr sees: `/movies/Movie.mkv`

The tool automatically translates these paths to match files correctly.

### Service Configuration

#### Plex

- Requires Plex Token (get from: <https://support.plex.tv/articles/204059436-finding-an-authentication-token-x-plex-token/>)

#### Sonarr/Radarr

- Requires API key from Settings → General → Security

#### qBittorrent

- Can use direct connection or qui proxy
- For qui proxy, use the full proxy URL: `http://qui:7476/proxy/YOUR_KEY`

#### Stash

- Requires API key from Settings → Security → Authentication
- Uses GraphQL API for querying media files

### Unraid-Specific Setup

Media Usage Finder has native Unraid integration for accurate disk statistics and cross-disk duplicate detection.

#### Required Volume Mounts

For Unraid deployments, add these volume mounts:

```bash
# Individual disk mounts for duplicate detection
-v '/mnt/disk1/':'/disk1':'rw'
-v '/mnt/disk2/':'/disk2':'rw'
-v '/mnt/disk3/':'/disk3':'rw'
# ... add all disks

# Unraid stats for accurate disk usage
-v '/var/local/emhttp':'/unraid/stats':'ro'
```

#### Disk Configuration

Configure disks in the web UI (Configuration page):

```yaml
disks:
  - name: Disk 1
    mount_path: /disk1
  - name: Disk 2
    mount_path: /disk2
  # ... etc
```

#### Disk Location Scanning

After configuring disks, run a disk scan to populate file locations:

**Via Web UI:** Click "Scan Disk Locations" on the Dashboard

**Via CLI:**

```bash
docker exec MediaUsageFinder /app/media-finder disk-scan
```

This scan:

- Walks each disk mount path individually
- Records file locations with disk-specific device IDs
- Maps files to their physical disk locations
- Enables cross-disk duplicate detection

**Note:** Disk scanning is independent of the main file scan. Run the main scan first to populate the database, then run disk-scan to add location tracking.

#### Duplicate Detection & Consolidation

Once disks are configured, the application can:

1. **Detect cross-disk duplicates** - Find identical files on different physical disks
2. **Calculate actual storage usage** - Shows accurate disk capacities and usage percentages
3. **Consolidate duplicates** - Move duplicates to optimize storage:
   - **Least Full Disk** - Move files to disks with most free space
   - **Most Full Disk** - Consolidate to fuller disks to free up empty ones
   - **Custom Strategy** - Define your own consolidation rules

**How it works:**

- Without `/var/local/emhttp` mount: Falls back to statfs (may show incorrect disk sizes for ZFS datasets)
- With `/var/local/emhttp` mount: Reads Unraid's native disk statistics for accurate capacity and usage
- Device ID detection always works correctly for duplicate detection

**Benefits:**

- **Optimize array usage** - Consolidate duplicates to free up entire disks for parity checks
- **Prevent overfilling** - Consolidation strategy respects disk capacities
- **Safe operations** - Dry-run mode and manual approval options available

## Web UI Features

### Dashboard

- Real-time scan progress with SSE
- Overall statistics
- Quick actions
- Service breakdown

### Files Page

- Full-text search
- Filter by service, orphaned status
- Pagination (25/50/100/500 items) with optional infinite scroll
- Export to JSON/CSV
- Delete individual files with confirmation
- Bulk delete orphaned files
- Mark files for rescan

### Statistics Page

- Visual charts (doughnut and bar charts using Chart.js)
- Detailed breakdowns by service
- Storage efficiency metrics
- Hardlink statistics and savings
- Contextual recommendations

### Configuration Page

- Edit all settings via UI
- Test service connections
- Manage path mappings

## Performance

Designed for large media servers:

- Handles 90TB+ datasets
- Configurable worker pools (default: 10 workers)
- Incremental scanning for speed
- SQLite with WAL mode and proper indexing
- Thread-safe concurrent operations

## Development

### Prerequisites

- Go 1.21+
- Node.js (for Tailwind CSS)
- SQLite

### Build from source

```bash
# Clone repository
git clone https://github.com/mmenanno/media-usage-finder
cd media-usage-finder

# Install dependencies
make install-deps

# Build Tailwind CSS
make tailwind

# Build binary
make build

# Run locally
./bin/media-finder serve
```

### Project Structure

```text
media_usage_finder/
├── cmd/media-finder/       # CLI entrypoint
├── internal/
│   ├── api/                # API clients (Plex, Sonarr, etc.)
│   ├── config/             # Configuration management
│   ├── database/           # SQLite database layer
│   ├── scanner/            # File scanner with worker pools
│   ├── server/             # HTTP server and handlers
│   └── stats/              # Statistics calculations
├── web/
│   ├── templates/          # Go HTML templates
│   └── static/             # CSS and JS assets
├── Dockerfile
├── docker-compose.yml
└── README.md
```

## Architecture

### Database Schema

- **files** - All scanned files with inode/device tracking
- **usage** - Tracks which services use each file
- **scans** - Scan history and status
- **audit_log** - Tracks deletions and changes

### Scanning Process

1. Count total files
2. Walk filesystem with worker pool
3. Query each service API
4. Cross-reference and update usage
5. Calculate orphaned status

### Hardlink Detection

Files are grouped by `(device_id, inode)` tuple, allowing detection of hardlinks even with different paths.

## License

MIT License - see LICENSE file for details

## Contributing

Contributions welcome! Please open an issue or PR.

## Support

For issues, questions, or feature requests, please open a GitHub issue.
