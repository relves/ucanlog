package storacha

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/relves/ucanlog/internal/storage"
	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/api/layout"
)

func (s *Storage) Appender(ctx context.Context, opts *tessera.AppendOptions) (*tessera.Appender, tessera.LogReader, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Use the existing objStore so that onDirty callback is preserved
	// for index persistence notifications
	objStore := s.objStore

	coord, err := newCoordinator(s.cfg.StateStore, s.cfg.LogDID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create coordinator: %w", err)
	}

	entriesPath := opts.EntriesPath()
	if entriesPath == nil {
		entriesPath = layout.EntriesPath
	}

	lrs := newLogResourceStore(objStore, entriesPath)

	// Create reader first so we can use it for checkpoint publisher
	reader := &storachaLogReader{
		lrs:   lrs,
		coord: coord,
	}

	// Get checkpoint publisher function from options
	// This creates signed checkpoints, optionally with witnessing
	newCP := opts.CheckpointPublisher(reader, s.cfg.HTTPClient)

	maxSize := opts.BatchMaxSize()
	if maxSize == 0 {
		maxSize = 256
	}
	flushFn := func(ctx context.Context, items []queueItem) error {
		if len(items) == 0 {
			return nil
		}

		unlock, err := coord.lock()
		if err != nil {
			return fmt.Errorf("failed to acquire lock: %w", err)
		}
		defer unlock()

		currentSize, _, err := coord.readTreeState(ctx)
		if err != nil {
			return fmt.Errorf("failed to read tree state: %w", err)
		}

		entries := make([]SequencedEntry, len(items))
		for i, item := range items {
			entries[i] = SequencedEntry{
				BundleData: item.entry.MarshalBundleData(currentSize + uint64(i)),
				LeafHash:   item.entry.LeafHash(),
			}
		}

		newRoot, err := integrateEntries(ctx, currentSize, entries, lrs, s.logger)
		if err != nil {
			return fmt.Errorf("failed to integrate entries: %w", err)
		}

		newSize := currentSize + uint64(len(entries))

		if err := coord.writeTreeState(ctx, newSize, newRoot); err != nil {
			return fmt.Errorf("failed to write tree state: %w", err)
		}

		// Publish checkpoint after successful integration
		var cpRaw []byte
		if newCP != nil {
			var err error
			cpRaw, err = newCP(ctx, newSize, newRoot)
			if err != nil {
				return fmt.Errorf("failed to create checkpoint: %w", err)
			}
			if err := lrs.setCheckpoint(ctx, cpRaw); err != nil {
				return fmt.Errorf("failed to store checkpoint: %w", err)
			}
		}

		// Pre-compute CIDs for any newly-sealed bundles.  The CIDs are registered
		// with objStore so the hybrid CAR links correctly.  No Storacha I/O here.
		pendingBlobs, err := precomputeFinalizedBundleCIDs(ctx, objStore, s.logger)
		if err != nil {
			return fmt.Errorf("failed to pre-compute bundle CIDs: %w", err)
		}

		// Build hybrid CAR: embedded mutable state + CID links for finalized bundles.
		snapshot := objStore.Snapshot()
		linkedCIDs := objStore.GetFinalizedCIDs()
		carData, rootCID, err := BuildHybridCAR(ctx, snapshot, linkedCIDs)
		if err != nil {
			return fmt.Errorf("failed to build hybrid CAR: %w", err)
		}
		headCID := rootCID.String()

		// Persist to latest_head_car and enqueue for async upload — single SQLite tx.
		if qs, ok := s.cfg.StateStore.(storage.QueueStore); ok {
			if _, err := qs.EnqueueAndUpdateHead(ctx, s.cfg.LogDID, newSize, headCID, carData, pendingBlobs); err != nil {
				return fmt.Errorf("failed to enqueue CAR upload: %w", err)
			}
			// Stash receipt data — available to the handler immediately after Add() returns.
			s.receiptMu.Lock()
			s.lastReceipt = FlushReceipt{HeadCID: headCID, TreeSize: newSize, Checkpoint: cpRaw}
			s.receiptMu.Unlock()
			// Wake the background upload worker.
			if s.uploadQueue != nil {
				s.uploadQueue.Notify()
			}
			s.logger.Info("hybrid CAR enqueued", "headCID", headCID, "carSize", len(carData),
				"embeddedFiles", len(snapshot), "linkedBundles", len(linkedCIDs), "pendingBlobs", len(pendingBlobs))
		} else {
			// Fallback: synchronous upload (StateStore doesn't implement QueueStore).
			client := s.clientRef.Get()
			if client == nil {
				return fmt.Errorf("no Storacha client configured: provide Config.Client")
			}
			if err := finalizeCompleteBundles(ctx, objStore, client, s.logger); err != nil {
				return fmt.Errorf("failed to finalize complete bundles: %w", err)
			}
			positions, err := ExtractBlockPositions(carData)
			if err != nil {
				return fmt.Errorf("failed to extract CAR block positions: %w", err)
			}
			uploadedCID, err := client.UploadFullStateCAR(ctx, carData, rootCID, positions)
			if err != nil {
				return fmt.Errorf("failed to upload hybrid CAR: %w", err)
			}
			s.logger.Info("hybrid CAR uploaded (sync)", "headCID", uploadedCID, "carSize", len(carData))
		}

		for i, item := range items {
			item.result <- queueResult{index: tessera.Index{Index: currentSize + uint64(i)}, err: nil}
		}

		return nil
	}

	q := newEntryQueue(uint(maxSize), flushFn)

	appender := &storachaAppender{
		lrs:    lrs,
		coord:  coord,
		queue:  q,
		logger: s.logger,
	}

	return &tessera.Appender{
		Add: appender.Add,
	}, reader, nil
}

type storachaAppender struct {
	lrs    *logResourceStore
	coord  *coordinator
	queue  *entryQueue
	logger *slog.Logger
}

func (a *storachaAppender) Add(ctx context.Context, entry *tessera.Entry) tessera.IndexFuture {
	return a.queue.Add(ctx, entry)
}

type storachaLogReader struct {
	lrs   *logResourceStore
	coord *coordinator
}

func (r *storachaLogReader) ReadCheckpoint(ctx context.Context) ([]byte, error) {
	return r.lrs.getCheckpoint(ctx)
}

func (r *storachaLogReader) ReadTile(ctx context.Context, level, index uint64, p uint8) ([]byte, error) {
	tile, err := r.lrs.getTile(ctx, level, index, p)
	if err != nil {
		return nil, err
	}
	return tile.MarshalText()
}

func (r *storachaLogReader) ReadEntryBundle(ctx context.Context, index uint64, p uint8) ([]byte, error) {
	return r.lrs.getEntryBundle(ctx, index, p)
}

func (r *storachaLogReader) IntegratedSize(ctx context.Context) (uint64, error) {
	size, _, err := r.coord.readTreeState(ctx)
	return size, err
}

func (r *storachaLogReader) NextIndex(ctx context.Context) (uint64, error) {
	return r.coord.readNextIndex(ctx)
}
