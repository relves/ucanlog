package storacha

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/transparency-dev/merkle/rfc6962"
	"golang.org/x/sync/errgroup"
)

func integrateEntries(ctx context.Context, fromSeq uint64, entries []SequencedEntry, lrs *logResourceStore, logger *slog.Logger) ([]byte, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if len(entries) == 0 {
		if fromSeq == 0 {
			return rfc6962.DefaultHasher.EmptyRoot(), nil
		}
		return nil, nil
	}

	var newRoot []byte
	errG := errgroup.Group{}

	errG.Go(func() error {
		if err := updateEntryBundles(ctx, fromSeq, entries, lrs); err != nil {
			return fmt.Errorf("updateEntryBundles: %w", err)
		}
		return nil
	})

	errG.Go(func() error {
		leafHashes := make([][]byte, len(entries))
		for i, e := range entries {
			leafHashes[i] = e.LeafHash
		}

		root, err := integrate(ctx, fromSeq, leafHashes, lrs, logger)
		if err != nil {
			return fmt.Errorf("integrate: %w", err)
		}
		newRoot = root
		return nil
	})

	if err := errG.Wait(); err != nil {
		return nil, err
	}

	return newRoot, nil
}
