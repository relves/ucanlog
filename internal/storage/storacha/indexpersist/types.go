// storage/storacha/indexpersist/types.go
package indexpersist

import (
	"log/slog"
	"time"
)

// IndexMeta stores metadata about the latest uploaded index CAR.
type IndexMeta struct {
	// RootCID is the CID of the uploaded UnixFS directory root.
	RootCID string `json:"root_cid"`

	// Version is a monotonically increasing counter for each upload.
	Version uint64 `json:"version"`

	// LastUploaded is when the CAR was last uploaded to Storacha.
	LastUploaded time.Time `json:"last_uploaded"`

	// EntryCount is the number of entries in the index at upload time.
	EntryCount int `json:"entry_count"`

	// TreeSize is the tessera tree size at upload time.
	TreeSize uint64 `json:"tree_size,omitempty"`
}

// Config configures the index persistence behavior.
type Config struct {
	// Interval is how often to check for changes and upload.
	// Default: 30 seconds
	Interval time.Duration

	// MinInterval is the minimum time between persists.
	// Default: 10 seconds
	// Rate limiting disabled if set to 0
	MinInterval time.Duration

	// PathPrefix for uploaded index CARs in Storacha.
	// Default: "index/"
	PathPrefix string

	// OnUpload is called after a successful upload with the new root CID.
	// Optional.
	OnUpload func(rootCID string, meta IndexMeta)

	// Logger for structured logging.
	// Default: slog.Default()
	Logger *slog.Logger
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		Interval:    30 * time.Second,
		MinInterval: 10 * time.Second,
		PathPrefix:  "index/",
	}
}
