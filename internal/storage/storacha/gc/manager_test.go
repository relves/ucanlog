// internal/storage/storacha/gc/manager_test.go
package gc

import (
	"context"
	"strings"
	"testing"

	"github.com/storacha/go-ucanto/core/delegation"
)

type mockRemover struct {
	removed [][]byte
}

func (m *mockRemover) RemoveBlob(ctx context.Context, spaceDID string, digest []byte, dlg delegation.Delegation) error {
	m.removed = append(m.removed, digest)
	return nil
}

type mockPathStore struct {
	paths map[string]string
}

func (m *mockPathStore) GetCID(path string) string {
	return m.paths[path]
}

func (m *mockPathStore) DeletePrefix(prefix string) int {
	count := 0
	for path := range m.paths {
		if strings.HasPrefix(path, prefix) {
			delete(m.paths, path)
			count++
		}
	}
	return count
}

type mockTreeSizeProvider struct {
	size uint64
}

func (m *mockTreeSizeProvider) GetTreeSize() uint64 {
	return m.size
}

func TestConfig_ApplyDefaults(t *testing.T) {
	cfg := Config{}
	cfg.ApplyDefaults()

	if cfg.MinInterval == 0 {
		t.Error("MinInterval should have default")
	}
	if cfg.MaxBundles == 0 {
		t.Error("MaxBundles should have default")
	}
}
