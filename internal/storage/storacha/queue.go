package storacha

import (
	"context"
	"sync"
	"time"

	"github.com/transparency-dev/tessera"
)

type entryQueue struct {
	maxSize uint
	maxAge  time.Duration
	flushFn func(context.Context, []queueItem) error

	mu       sync.Mutex
	items    []queueItem
	timer    *time.Timer
	flushing bool
}

type queueItem struct {
	entry  *tessera.Entry
	result chan queueResult
}

type queueResult struct {
	index tessera.Index
	err   error
}

func newEntryQueue(ctx context.Context, maxAge time.Duration, maxSize uint, flushFn func(context.Context, []queueItem) error) *entryQueue {
	q := &entryQueue{
		maxSize: maxSize,
		maxAge:  maxAge,
		flushFn: flushFn,
		items:   make([]queueItem, 0, maxSize),
	}

	return q
}

func (q *entryQueue) Add(ctx context.Context, entry *tessera.Entry) tessera.IndexFuture {
	resultCh := make(chan queueResult, 1)

	q.mu.Lock()

	q.items = append(q.items, queueItem{
		entry:  entry,
		result: resultCh,
	})

	shouldFlush := len(q.items) >= int(q.maxSize)

	if len(q.items) == 1 && q.maxAge > 0 {
		q.timer = time.AfterFunc(q.maxAge, func() {
			q.flush(context.Background())
		})
	}

	if shouldFlush {
		if q.timer != nil {
			q.timer.Stop()
		}
		items := q.items
		q.items = make([]queueItem, 0, q.maxSize)
		q.mu.Unlock()

		go q.doFlush(ctx, items)
	} else {
		q.mu.Unlock()
	}

	return func() (tessera.Index, error) {
		result := <-resultCh
		return result.index, result.err
	}
}

func (q *entryQueue) flush(ctx context.Context) {
	q.mu.Lock()
	if len(q.items) == 0 {
		q.mu.Unlock()
		return
	}

	items := q.items
	q.items = make([]queueItem, 0, q.maxSize)
	q.mu.Unlock()

	q.doFlush(ctx, items)
}

func (q *entryQueue) doFlush(ctx context.Context, items []queueItem) {
	if len(items) == 0 {
		return
	}

	err := q.flushFn(ctx, items)

	// Only send error results here - on success, flushFn already sent results
	// with the correct indices via the result channels
	if err != nil {
		q.mu.Lock()
		defer q.mu.Unlock()

		for _, item := range items {
			item.result <- queueResult{err: err}
		}
	}
}

func (q *entryQueue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.timer != nil {
		q.timer.Stop()
	}

	return nil
}
