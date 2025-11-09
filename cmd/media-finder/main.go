package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/mmenanno/media-usage-finder/internal/config"
	"github.com/mmenanno/media-usage-finder/internal/constants"
	"github.com/mmenanno/media-usage-finder/internal/database"
	"github.com/mmenanno/media-usage-finder/internal/disk"
	"github.com/mmenanno/media-usage-finder/internal/scanner"
	"github.com/mmenanno/media-usage-finder/internal/server"
	"github.com/mmenanno/media-usage-finder/internal/stats"
	"github.com/spf13/cobra"
)

var (
	// Version is set at build time
	Version = "dev"

	// Global flags
	configPath string
	cfg        *config.Config
	db         *database.DB
)

func main() {
	// Ensure database is closed even on panic
	defer func() {
		if r := recover(); r != nil {
			if db != nil {
				db.Close()
			}
			panic(r) // Re-panic after cleanup
		}
	}()

	rootCmd := &cobra.Command{
		Use:   "media-finder",
		Short: "Media Usage Finder - Track and manage media files across services",
		Long: `Media Usage Finder scans your media files and tracks which services
(Plex, Sonarr, Radarr, qBittorrent) are using them. It helps identify
orphaned files and optimizes storage through hardlink detection.`,
		Version: Version,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			// Load configuration
			var err error
			cfg, err = config.Load(configPath)
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}

			// Auto-generate config file if it doesn't exist
			if _, err := os.Stat(configPath); os.IsNotExist(err) {
				log.Printf("Config file not found, creating default at %s", configPath)
				if err := cfg.Save(configPath); err != nil {
					log.Printf("Warning: failed to save default config: %v", err)
				} else {
					log.Println("Default configuration file created successfully")
				}
			}

			// Open database with config
			db, err = database.NewWithConfig(cfg.DatabasePath, database.DBConfig{
				MaxOpenConns:    cfg.DBMaxOpenConns,
				MaxIdleConns:    cfg.DBMaxIdleConns,
				ConnMaxLifetime: cfg.DBConnMaxLifetime,
				CacheSize:       cfg.DBCacheSize,
			})
			if err != nil {
				return fmt.Errorf("failed to open database: %w", err)
			}

			return nil
		},
		PersistentPostRunE: func(cmd *cobra.Command, args []string) error {
			if db != nil {
				return db.Close()
			}
			return nil
		},
	}

	rootCmd.PersistentFlags().StringVarP(&configPath, "config", "c", "/appdata/config/config.yaml", "Path to configuration file")

	// Serve command
	serveCmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the web server (listens on port 8787)",
		RunE:  runServe,
	}

	// Scan command
	scanCmd := &cobra.Command{
		Use:   "scan",
		Short: "Run a filesystem scan",
		RunE:  runScan,
	}
	scanCmd.Flags().BoolP("incremental", "i", false, "Run incremental scan (only changed files)")

	// Disk-scan command
	diskScanCmd := &cobra.Command{
		Use:   "disk-scan",
		Short: "Scan individual disks to populate disk location tracking",
		RunE:  runDiskScan,
	}
	diskScanCmd.Flags().StringP("disk", "d", "", "Scan only a specific disk by name (optional)")

	// Stats command
	statsCmd := &cobra.Command{
		Use:   "stats",
		Short: "Display statistics",
		RunE:  runStats,
	}

	// Export command
	exportCmd := &cobra.Command{
		Use:   "export",
		Short: "Export file list",
		RunE:  runExport,
	}
	exportCmd.Flags().BoolP("orphaned", "o", false, "Export only orphaned files")
	exportCmd.Flags().StringP("format", "f", "json", "Output format (json, csv)")
	exportCmd.Flags().StringP("output", "O", "", "Output file (default: stdout)")

	// Note: mark-rescan command removed in v0.58.0 - use web UI for file rescans

	// Delete command
	deleteCmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete files",
		RunE:  runDelete,
	}
	deleteCmd.Flags().StringP("path", "p", "", "Path to file to delete")
	deleteCmd.Flags().BoolP("orphaned", "o", false, "Delete all orphaned files")
	deleteCmd.Flags().BoolP("dry-run", "n", false, "Show what would be deleted without actually deleting")

	// Config command
	configCmd := &cobra.Command{
		Use:   "config",
		Short: "Configuration management",
	}

	configValidateCmd := &cobra.Command{
		Use:   "validate",
		Short: "Validate configuration",
		RunE:  runConfigValidate,
	}

	configShowCmd := &cobra.Command{
		Use:   "show",
		Short: "Show current configuration",
		RunE:  runConfigShow,
	}

	configCmd.AddCommand(configValidateCmd, configShowCmd)

	rootCmd.AddCommand(serveCmd, scanCmd, diskScanCmd, statsCmd, exportCmd, deleteCmd, configCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func runServe(cmd *cobra.Command, args []string) error {
	srv := server.NewServer(db, cfg, Version)

	// Load templates from embedded FS
	log.Println("Loading templates...")
	if err := srv.LoadTemplates("web/templates/*.html"); err != nil {
		return fmt.Errorf("failed to load templates: %w", err)
	}

	log.Printf("Starting Media Usage Finder v%s on port 8787", Version)
	return srv.Run()
}

func runScan(cmd *cobra.Command, args []string) error {
	incremental, _ := cmd.Flags().GetBool("incremental")

	log.Println("Starting scan...")
	s := scanner.NewScanner(db, cfg)

	ctx := context.Background()
	if err := s.Scan(ctx, incremental); err != nil {
		return fmt.Errorf("scan failed: %w", err)
	}

	log.Println("Scan completed successfully")
	return nil
}

func runDiskScan(cmd *cobra.Command, args []string) error {
	diskName, _ := cmd.Flags().GetString("disk")

	// Check if disks are configured
	if len(cfg.Disks) == 0 {
		return fmt.Errorf("no disks configured in config.yaml - disk scanning not available")
	}

	// If specific disk requested, validate it exists
	if diskName != "" {
		found := false
		for _, diskCfg := range cfg.Disks {
			if diskCfg.Name == diskName {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("disk '%s' not found in configuration", diskName)
		}
		return fmt.Errorf("scanning specific disks not yet implemented - run without --disk flag to scan all")
	}

	log.Println("Starting disk location scan...")

	// Create disk detector
	detector := disk.NewDetector(cfg.Disks)
	if err := detector.DetectDisks(); err != nil {
		return fmt.Errorf("failed to detect disks: %w", err)
	}
	log.Printf("Detected %d disk(s)", detector.GetDiskCount())

	// Create scanner and run disk scan
	s := scanner.NewScanner(db, cfg)
	if err := s.ScanDiskLocations(detector); err != nil {
		return fmt.Errorf("disk scan failed: %w", err)
	}

	log.Println("Disk location scan completed successfully")
	return nil
}

func runStats(cmd *cobra.Command, args []string) error {
	calculator := stats.NewCalculator(db)
	statistics, err := calculator.Calculate()
	if err != nil {
		return fmt.Errorf("failed to calculate stats: %w", err)
	}

	fmt.Printf("\n=== Media Usage Statistics ===\n\n")
	fmt.Printf("Total Files:       %d\n", statistics.TotalFiles)
	fmt.Printf("Total Size:        %s\n", stats.FormatSize(statistics.TotalSize))
	fmt.Printf("Orphaned Files:    %d (%s)\n", statistics.OrphanedFiles, stats.FormatSize(statistics.OrphanedSize))
	fmt.Printf("Hardlink Groups:   %d\n", statistics.HardlinkGroups)
	fmt.Printf("Space Saved:       %s\n", stats.FormatSize(statistics.HardlinkSavings))
	fmt.Printf("\nService Breakdown:\n")

	for service, serviceStats := range statistics.ServiceBreakdown {
		fmt.Printf("  %-12s %d files (%s)\n", service+":", serviceStats.FileCount, stats.FormatSize(serviceStats.TotalSize))
	}

	fmt.Println()
	return nil
}

func runExport(cmd *cobra.Command, args []string) error {
	orphaned, _ := cmd.Flags().GetBool("orphaned")
	format, _ := cmd.Flags().GetString("format")
	output, _ := cmd.Flags().GetString("output")

	files, _, err := db.ListFiles(orphaned, nil, "any", false, nil, nil, constants.MaxExportFiles, 0, "path", "asc")
	if err != nil {
		return fmt.Errorf("failed to list files: %w", err)
	}

	var data []byte
	switch format {
	case "json":
		data, err = json.MarshalIndent(files, "", "  ")
	case "csv":
		data = []byte("path,size,is_orphaned\n")
		for _, file := range files {
			data = append(data, []byte(fmt.Sprintf("%s,%d,%v\n", file.Path, file.Size, file.IsOrphaned))...)
		}
	default:
		return fmt.Errorf("unsupported format: %s", format)
	}

	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	if output != "" {
		// Ensure directory exists
		dir := filepath.Dir(output)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create output directory: %w", err)
		}

		if err := os.WriteFile(output, data, 0644); err != nil {
			return fmt.Errorf("failed to write output file: %w", err)
		}
		log.Printf("Exported %d files to %s", len(files), output)
	} else {
		fmt.Println(string(data))
	}

	return nil
}

func runConfigValidate(cmd *cobra.Command, args []string) error {
	if err := cfg.Validate(); err != nil {
		fmt.Printf("Configuration is INVALID: %v\n", err)
		return err
	}

	fmt.Println("Configuration is valid ✓")
	return nil
}

func runConfigShow(cmd *cobra.Command, args []string) error {
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	fmt.Println(string(data))
	return nil
}

// Note: mark-rescan CLI command was removed in v0.58.0
// The feature was changed to an active "rescan now" feature accessible via the web UI
// Files are no longer marked for rescan but are instead rescanned immediately

func runDelete(cmd *cobra.Command, args []string) error {
	path, _ := cmd.Flags().GetString("path")
	orphaned, _ := cmd.Flags().GetBool("orphaned")
	dryRun, _ := cmd.Flags().GetBool("dry-run")

	if path == "" && !orphaned {
		return fmt.Errorf("must specify either --path or --orphaned")
	}

	if path != "" && orphaned {
		return fmt.Errorf("cannot specify both --path and --orphaned")
	}

	if path != "" {
		// Delete single file
		if dryRun {
			file, err := db.GetFileByPath(path)
			if err != nil {
				return fmt.Errorf("file not found: %w", err)
			}
			fmt.Printf("Would delete: %s (%d bytes)\n", file.Path, file.Size)
			return nil
		}

		if err := db.DeleteFileByPath(path, "CLI deletion", false); err != nil {
			return fmt.Errorf("failed to delete file: %w", err)
		}

		log.Printf("Deleted file: %s", path)
		return nil
	}

	// Delete orphaned files
	files, _, err := db.ListFiles(true, nil, "any", false, nil, nil, constants.MaxExportFiles, 0, "path", "asc")
	if err != nil {
		return fmt.Errorf("failed to list orphaned files: %w", err)
	}

	if dryRun {
		var totalSize int64
		fmt.Printf("Would delete %d orphaned files:\n", len(files))
		for _, file := range files {
			fmt.Printf("  %s (%d bytes)\n", file.Path, file.Size)
			totalSize += file.Size
		}
		fmt.Printf("\nTotal size: %d bytes\n", totalSize)
		return nil
	}

	// Ask for confirmation
	fmt.Printf("About to delete %d orphaned files. Continue? (yes/no): ", len(files))
	var response string
	fmt.Scanln(&response)
	if response != "yes" {
		fmt.Println("Aborted")
		return nil
	}

	deleted := 0
	for _, file := range files {
		if err := db.DeleteFile(file.ID, "Orphaned file cleanup", false); err != nil {
			log.Printf("Failed to delete %s: %v", file.Path, err)
			continue
		}
		deleted++
	}

	log.Printf("Deleted %d orphaned files", deleted)
	return nil
}
