package tlog

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFileCIDStore(t *testing.T) {
	// Create temp directory
	tmpDir := t.TempDir()

	// Create CID store
	store := NewFileCIDStore(tmpDir)

	logID := "test-log"
	expectedCID := "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"

	// Setup: create the directory structure and write a CID file
	stateDir := filepath.Join(tmpDir, "logs", logID, ".state")
	err := os.MkdirAll(stateDir, 0755)
	if err != nil {
		t.Fatalf("failed to create state dir: %v", err)
	}

	cidPath := filepath.Join(stateDir, "latest-index-cid")
	err = os.WriteFile(cidPath, []byte(expectedCID), 0644)
	if err != nil {
		t.Fatalf("failed to write CID file: %v", err)
	}

	// Test GetLatestCID
	cid, err := store.GetLatestCID(logID)
	if err != nil {
		t.Fatalf("GetLatestCID failed: %v", err)
	}

	if cid != expectedCID {
		t.Errorf("expected CID %q, got %q", expectedCID, cid)
	}

	// Test cache: read again should hit cache
	cid2, err := store.GetLatestCID(logID)
	if err != nil {
		t.Fatalf("GetLatestCID (cached) failed: %v", err)
	}

	if cid2 != expectedCID {
		t.Errorf("cached CID mismatch: expected %q, got %q", expectedCID, cid2)
	}

	// Test SetLatestCID updates cache
	newCID := "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdj"
	err = store.SetLatestCID(logID, newCID)
	if err != nil {
		t.Fatalf("SetLatestCID failed: %v", err)
	}

	// Verify cache was updated
	cid3, err := store.GetLatestCID(logID)
	if err != nil {
		t.Fatalf("GetLatestCID after Set failed: %v", err)
	}

	if cid3 != newCID {
		t.Errorf("after SetLatestCID: expected %q, got %q", newCID, cid3)
	}
}

func TestFileCIDStore_MissingLog(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewFileCIDStore(tmpDir)

	_, err := store.GetLatestCID("nonexistent-log")
	if err == nil {
		t.Fatal("expected error for nonexistent log, got nil")
	}
}
