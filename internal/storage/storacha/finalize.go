package storacha

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/ipfs/go-cid"
	"github.com/relves/ucanlog/internal/storage"
)

// finalizeCompleteBundles scans stateMap for full entry bundles (partial=0, no .p/ suffix),
// uploads each individually via UploadFinalizedBlob, then records their CIDs so the head CAR
// references them by link rather than embedding the bytes.
//
// Deprecated: use precomputeFinalizedBundleCIDs for async mode.
func finalizeCompleteBundles(ctx context.Context, store *objStore, client StorachaClient, logger *slog.Logger) error {
	store.mu.RLock()
	var toFinalize []string
	for path := range store.stateMap {
		if isFullBundlePath(path) {
			toFinalize = append(toFinalize, path)
		}
	}
	store.mu.RUnlock()

	for _, path := range toFinalize {
		data, err := store.getObject(ctx, path)
		if err != nil {
			return fmt.Errorf("read bundle %s for finalization: %w", path, err)
		}

		// Compute the real content CID before uploading so we have it regardless
		// of what the client returns (mock clients return synthetic CIDs).
		cidStr, mh, err := ComputeCID(data)
		if err != nil {
			return fmt.Errorf("compute CID for bundle %s: %w", path, err)
		}
		c := cid.NewCidV1(cid.Raw, mh)

		if _, err := client.UploadFinalizedBlob(ctx, data); err != nil {
			return fmt.Errorf("upload finalized bundle %s: %w", path, err)
		}

		store.SetFinalizedCID(path, c)
		logger.Info("bundle finalized", "path", path, "cid", cidStr, "size", len(data))
	}

	return nil
}

// precomputeFinalizedBundleCIDs scans stateMap for full entry bundles, computes their
// CIDs, registers them with the objStore (so the hybrid CAR links correctly), and returns
// them as PendingBlobs for async upload.  No network I/O is performed.
func precomputeFinalizedBundleCIDs(ctx context.Context, store *objStore, logger *slog.Logger) ([]storage.PendingBlob, error) {
	store.mu.RLock()
	var toFinalize []string
	for path := range store.stateMap {
		if isFullBundlePath(path) {
			toFinalize = append(toFinalize, path)
		}
	}
	store.mu.RUnlock()

	blobs := make([]storage.PendingBlob, 0, len(toFinalize))
	for _, path := range toFinalize {
		data, err := store.getObject(ctx, path)
		if err != nil {
			return nil, fmt.Errorf("read bundle %s: %w", path, err)
		}

		cidStr, mh, err := ComputeCID(data)
		if err != nil {
			return nil, fmt.Errorf("compute CID for bundle %s: %w", path, err)
		}
		c := cid.NewCidV1(cid.Raw, mh)

		// Register the CID so the hybrid CAR contains a link rather than the full bytes.
		store.SetFinalizedCID(path, c)
		logger.Debug("bundle CID pre-computed", "path", path, "cid", cidStr, "size", len(data))

		blobs = append(blobs, storage.PendingBlob{
			Path:     path,
			BlobCID:  cidStr,
			BlobData: data,
		})
	}
	return blobs, nil
}

// isFullBundlePath returns true if path is a full (non-partial) entry bundle.
// Full bundles: "tile/entries/000" (no .p/ suffix)
// Partial bundles: "tile/entries/000.p/50" (has .p/ suffix)
func isFullBundlePath(path string) bool {
	return strings.HasPrefix(path, "tile/entries/") && !strings.Contains(path, ".p/")
}
