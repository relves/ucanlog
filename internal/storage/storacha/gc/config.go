// internal/storage/storacha/gc/config.go
package gc

import (
	"log/slog"
	"time"
)

// Config holds configuration for garbage collection.
type Config struct {
	// MinInterval is the minimum time between GC runs.
	// Default: 30s
	MinInterval time.Duration

	// MaxBundles is the maximum number of bundles to process per GC run.
	// Default: 100 (same as GCP)
	MaxBundles uint

	// Logger for structured logging.
	// Default: slog.Default()
	Logger *slog.Logger
}

// ApplyDefaults sets default values for unset fields.
func (c *Config) ApplyDefaults() {
	if c.MinInterval == 0 {
		c.MinInterval = 30 * time.Second
	}
	if c.MaxBundles == 0 {
		c.MaxBundles = 100
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}
