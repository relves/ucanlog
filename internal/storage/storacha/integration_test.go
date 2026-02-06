package storacha

import (
	"context"
	"log/slog"
	"testing"

	"github.com/relves/ucanlog/internal/storage/storacha/storachatest"
	"github.com/stretchr/testify/require"
	"github.com/transparency-dev/merkle/rfc6962"
	"github.com/transparency-dev/tessera/api/layout"
)

func TestIntegrateEntries_ParallelExecution(t *testing.T) {
	client := NewMockClient()
	index := NewCIDIndex()

	objStore := newObjStore(newClientRef(client), index, "did:key:test", "https://w3s.link", slog.Default())
	lrs := newLogResourceStore(objStore, layout.EntriesPath)

	ctx := WithDelegation(context.Background(), storachatest.MockDelegation())

	entries := make([]SequencedEntry, 5)
	for i := 0; i < 5; i++ {
		data := []byte{byte(i)}
		entries[i] = SequencedEntry{
			BundleData: marshalBundleEntry(data),
			LeafHash:   rfc6962.DefaultHasher.HashLeaf(data),
		}
	}

	root, err := integrateEntries(ctx, 0, entries, lrs, slog.Default())
	require.NoError(t, err)
	require.NotEmpty(t, root)
	require.Len(t, root, 32)

	_, ok := index.Get(layout.EntriesPath(0, 5))
	require.True(t, ok, "entry bundle should exist")

	_, ok = index.Get(layout.TilePath(0, 0, 5))
	require.True(t, ok, "hash tile should exist")
}

func TestIntegrateEntries_LargeIntegration(t *testing.T) {
	client := NewMockClient()
	index := NewCIDIndex()

	objStore := newObjStore(newClientRef(client), index, "did:key:test", "https://w3s.link", slog.Default())
	lrs := newLogResourceStore(objStore, layout.EntriesPath)

	ctx := WithDelegation(context.Background(), storachatest.MockDelegation())

	entries := make([]SequencedEntry, 300)
	for i := 0; i < 300; i++ {
		data := []byte{byte(i % 256), byte(i / 256)}
		entries[i] = SequencedEntry{
			BundleData: marshalBundleEntry(data),
			LeafHash:   rfc6962.DefaultHasher.HashLeaf(data),
		}
	}

	root, err := integrateEntries(ctx, 0, entries, lrs, slog.Default())
	require.NoError(t, err)
	require.NotEmpty(t, root)

	_, ok := index.Get(layout.EntriesPath(0, 0))
	require.True(t, ok, "first entry bundle should be full")

	_, ok = index.Get(layout.EntriesPath(1, 44))
	require.True(t, ok, "second entry bundle should be partial")

	_, ok = index.Get(layout.TilePath(0, 0, 0))
	require.True(t, ok, "first hash tile should be full")
}

func TestIntegrateEntries_EmptyEntries(t *testing.T) {
	client := NewMockClient()
	index := NewCIDIndex()

	objStore := newObjStore(newClientRef(client), index, "did:key:test", "https://w3s.link", slog.Default())
	lrs := newLogResourceStore(objStore, layout.EntriesPath)

	ctx := WithDelegation(context.Background(), storachatest.MockDelegation())

	root, err := integrateEntries(ctx, 0, []SequencedEntry{}, lrs, slog.Default())
	require.NoError(t, err)

	require.Equal(t, rfc6962.DefaultHasher.EmptyRoot(), root)
}

func TestIntegrateEntries_ExtendExisting(t *testing.T) {
	client := NewMockClient()
	index := NewCIDIndex()

	objStore := newObjStore(newClientRef(client), index, "did:key:test", "https://w3s.link", slog.Default())
	lrs := newLogResourceStore(objStore, layout.EntriesPath)

	ctx := WithDelegation(context.Background(), storachatest.MockDelegation())

	entries1 := make([]SequencedEntry, 10)
	for i := 0; i < 10; i++ {
		data := []byte{byte(i)}
		entries1[i] = SequencedEntry{
			BundleData: marshalBundleEntry(data),
			LeafHash:   rfc6962.DefaultHasher.HashLeaf(data),
		}
	}

	root1, err := integrateEntries(ctx, 0, entries1, lrs, slog.Default())
	require.NoError(t, err)
	require.NotEmpty(t, root1)

	entries2 := make([]SequencedEntry, 5)
	for i := 0; i < 5; i++ {
		data := []byte{byte(10 + i)}
		entries2[i] = SequencedEntry{
			BundleData: marshalBundleEntry(data),
			LeafHash:   rfc6962.DefaultHasher.HashLeaf(data),
		}
	}

	root2, err := integrateEntries(ctx, 10, entries2, lrs, slog.Default())
	require.NoError(t, err)
	require.NotEmpty(t, root2)

	require.NotEqual(t, root1, root2, "roots should be different after extending")

	_, ok := index.Get(layout.EntriesPath(0, 10))
	require.True(t, ok, "first bundle should exist")

	_, ok = index.Get(layout.EntriesPath(0, 15))
	require.True(t, ok, "extended first bundle should exist")
}
