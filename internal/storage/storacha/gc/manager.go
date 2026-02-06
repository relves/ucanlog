// internal/storage/storacha/gc/manager.go
package gc

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/ipfs/go-cid"
	"github.com/relves/ucanlog/internal/storage"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/transparency-dev/tessera/api/layout"
)

// BlobRemover handles removing blobs from storage.
type BlobRemover interface {
	// RemoveBlob removes a blob by its digest (multihash).
	RemoveBlob(ctx context.Context, spaceDID string, digest []byte, dlg delegation.Delegation) error
}

// PathStore provides read and delete access to path->CID mappings.
type PathStore interface {
	// GetCID returns the CID for a path, or empty string if not found.
	GetCID(path string) string
	// DeletePrefix removes all path mappings with the given prefix.
	// Returns the number of deleted entries.
	DeletePrefix(prefix string) int
}

// TreeSizeProvider provides the current tree size.
type TreeSizeProvider interface {
	// GetTreeSize returns the current tree size.
	GetTreeSize() uint64
}

// Manager handles garbage collection of obsolete partial bundles.
type Manager struct {
	cfg              Config
	removerGetter    func() BlobRemover
	pathStore        PathStore
	treeSizeProvider TreeSizeProvider
	stateStore       storage.StateStore
	spaceDID         string
	logDID           string
	logger           *slog.Logger

	mu           sync.Mutex
	gcInProgress bool
}

// NewManager creates a new GC manager.
func NewManager(cfg Config, removerGetter func() BlobRemover, pathStore PathStore, treeSizeProvider TreeSizeProvider, spaceDID string) *Manager {
	cfg.ApplyDefaults()
	return &Manager{
		cfg:              cfg,
		removerGetter:    removerGetter,
		pathStore:        pathStore,
		treeSizeProvider: treeSizeProvider,
		spaceDID:         spaceDID,
		logger:           cfg.Logger,
	}
}

// SetStateStore configures the manager to track GC progress.
func (m *Manager) SetStateStore(store storage.StateStore, logDID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stateStore = store
	m.logDID = logDID
}

// RunGCSync runs garbage collection synchronously and returns the new position.
// This is used by the explicit GC API endpoint.
func (m *Manager) RunGCSync(ctx context.Context, fromSize, treeSize uint64, dlg delegation.Delegation) (uint64, int, error) {
	m.mu.Lock()
	if m.gcInProgress {
		m.mu.Unlock()
		return fromSize, 0, fmt.Errorf("garbage collection already in progress")
	}
	m.gcInProgress = true
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		m.gcInProgress = false
		m.mu.Unlock()
	}()

	if fromSize >= treeSize {
		return fromSize, 0, nil // Nothing to GC
	}

	// Run GC
	newFromSize, blobsRemoved, err := m.garbageCollect(ctx, fromSize, treeSize, dlg)
	if err != nil {
		return fromSize, 0, fmt.Errorf("garbage collection failed: %w", err)
	}

	return newFromSize, blobsRemoved, nil
}

// garbageCollect removes obsolete partial bundles.
// Returns the new fromSize (progress).
func (m *Manager) garbageCollect(ctx context.Context, fromSize, treeSize uint64, dlg delegation.Delegation) (uint64, int, error) {
	bundlesProcessed := uint(0)
	blobsRemoved := 0

	// Iterate over bundles using tessera's layout.Range
	for ri := range layout.Range(fromSize, treeSize-fromSize, treeSize) {
		// Only process completed bundles (partial == 0)
		if ri.Partial > 0 {
			break
		}

		// Stop if we've reached our limit
		if bundlesProcessed >= m.cfg.MaxBundles {
			break
		}

		// Delete partial versions of the entry bundle
		entriesPrefix := layout.EntriesPath(ri.Index, 0) + ".p/"
		removed, err := m.deleteWithPrefix(ctx, entriesPrefix, dlg)
		if err != nil {
			return fromSize, blobsRemoved, fmt.Errorf("failed to delete entries partials: %w", err)
		}
		blobsRemoved += removed

		// Delete partial versions of the tile at level 0
		tilePrefix := layout.TilePath(0, ri.Index, 0) + ".p/"
		removed, err = m.deleteWithPrefix(ctx, tilePrefix, dlg)
		if err != nil {
			return fromSize, blobsRemoved, fmt.Errorf("failed to delete tile partials: %w", err)
		}
		blobsRemoved += removed

		fromSize += uint64(ri.N)
		bundlesProcessed++

		// Walk up parent tiles when at right edge of subtree
		pL, pIdx := uint64(0), ri.Index
		for isLastLeafInParent(pIdx) {
			pL, pIdx = pL+1, pIdx>>layout.TileHeight
			parentPrefix := layout.TilePath(pL, pIdx, 0) + ".p/"
			removed, err := m.deleteWithPrefix(ctx, parentPrefix, dlg)
			blobsRemoved += removed
			if err != nil {
				m.logger.Warn("failed to delete parent tile partials", "level", pL, "error", err)
				// Continue - parent tile cleanup is best-effort
			}
		}
	}

	return fromSize, blobsRemoved, nil
}

// deleteWithPrefix removes all blobs whose paths start with the given prefix.
// This is a logical delete - we look up CIDs by path and remove those blobs.
func (m *Manager) deleteWithPrefix(ctx context.Context, prefix string, dlg delegation.Delegation) (int, error) {
	removed := 0
	// For each partial size 1-255, check if the path exists and delete
	for p := 1; p < 256; p++ {
		path := fmt.Sprintf("%s%d", prefix, p)
		cidStr := m.pathStore.GetCID(path)
		if cidStr == "" {
			continue // Path doesn't exist
		}

		// Parse CID to get multihash
		digest, err := cidToMultihash(cidStr)
		if err != nil {
			m.logger.Warn("failed to parse CID", "cid", cidStr, "error", err)
			continue
		}

		remover := m.removerGetter
		if remover == nil {
			return removed, fmt.Errorf("no blob remover configured")
		}
		client := remover()
		if client == nil {
			return removed, fmt.Errorf("no blob remover configured")
		}

		// Remove the blob
		if err := client.RemoveBlob(ctx, m.spaceDID, digest, dlg); err != nil {
			// Log but continue - partial cleanup is best-effort
			m.logger.Warn("failed to remove blob", "cid", cidStr, "error", err)
			continue
		}
		removed++
	}

	// Clean up index mappings for this prefix
	deleted := m.pathStore.DeletePrefix(prefix)
	if deleted > 0 {
		m.logger.Debug("cleaned up index entries", "count", deleted, "prefix", prefix)
	}

	return removed, nil
}

// isLastLeafInParent returns true if a tile with the provided index is the final child
// node of a (hypothetical) full parent tile.
func isLastLeafInParent(i uint64) bool {
	return i%layout.TileWidth == layout.TileWidth-1
}

// cidToMultihash extracts the multihash from a CID string.
func cidToMultihash(cidStr string) ([]byte, error) {
	c, err := cid.Decode(cidStr)
	if err != nil {
		return nil, err
	}
	return c.Hash(), nil
}
