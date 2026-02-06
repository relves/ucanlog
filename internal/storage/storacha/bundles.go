package storacha

import (
	"bytes"
	"context"
	"fmt"

	"github.com/transparency-dev/tessera/api/layout"
	"golang.org/x/sync/errgroup"
)

func updateEntryBundles(ctx context.Context, fromSeq uint64, entries []SequencedEntry, lrs *logResourceStore) error {
	if len(entries) == 0 {
		return nil
	}

	bundleIndex := fromSeq / layout.EntryBundleWidth
	entriesInBundle := fromSeq % layout.EntryBundleWidth

	bundleWriter := &bytes.Buffer{}

	if entriesInBundle > 0 {
		existingData, err := lrs.getEntryBundle(ctx, bundleIndex, uint8(entriesInBundle))
		if err != nil {
			return fmt.Errorf("failed to read existing partial bundle: %w", err)
		}
		if _, err := bundleWriter.Write(existingData); err != nil {
			return fmt.Errorf("failed to write existing data to buffer: %w", err)
		}
	}

	writeG := errgroup.Group{}

	goSetEntryBundle := func(ctx context.Context, idx uint64, partial uint8, data []byte) {
		writeG.Go(func() error {
			return lrs.setEntryBundle(ctx, idx, partial, data)
		})
	}

	for _, entry := range entries {
		if _, err := bundleWriter.Write(entry.BundleData); err != nil {
			return fmt.Errorf("failed to write entry to buffer: %w", err)
		}
		entriesInBundle++

		if entriesInBundle == layout.EntryBundleWidth {
			// Make a copy of the buffer bytes to avoid sharing the underlying array
			bundleData := make([]byte, bundleWriter.Len())
			copy(bundleData, bundleWriter.Bytes())
			goSetEntryBundle(ctx, bundleIndex, 0, bundleData)

			bundleIndex++
			entriesInBundle = 0
			bundleWriter = &bytes.Buffer{}
		}
	}

	if entriesInBundle > 0 {
		// Make a copy of the buffer bytes to avoid sharing the underlying array
		bundleData := make([]byte, bundleWriter.Len())
		copy(bundleData, bundleWriter.Bytes())
		goSetEntryBundle(ctx, bundleIndex, uint8(entriesInBundle), bundleData)
	}

	return writeG.Wait()
}

func marshalBundleEntry(data []byte) []byte {
	buf := make([]byte, 2+len(data))
	buf[0] = byte(len(data) >> 8)
	buf[1] = byte(len(data))
	copy(buf[2:], data)
	return buf
}
