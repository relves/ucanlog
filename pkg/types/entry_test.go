// pkg/types/entry_test.go
package types

import (
	"testing"
	"time"
)

func TestRevocationEntry_Serialize(t *testing.T) {
	entry := RevocationEntry{
		Type:      RevokeUCAN,
		Target:    []byte("bafy2bzacedfkjkj..."),
		Timestamp: time.Unix(1700000000, 0),
	}

	data, err := entry.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize: %v", err)
	}

	if len(data) == 0 {
		t.Fatal("expected non-empty serialized data")
	}
}

func TestRevocationEntry_Deserialize(t *testing.T) {
	original := RevocationEntry{
		Type:      RevokeUCAN,
		Target:    []byte("bafy2bzacedfkjkj..."),
		Timestamp: time.Unix(1700000000, 0),
	}

	data, err := original.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize: %v", err)
	}

	var restored RevocationEntry
	err = restored.Deserialize(data)
	if err != nil {
		t.Fatalf("failed to deserialize: %v", err)
	}

	if restored.Type != original.Type {
		t.Errorf("type mismatch: got %s, want %s", restored.Type, original.Type)
	}
	if string(restored.Target) != string(original.Target) {
		t.Errorf("target mismatch: got %s, want %s", restored.Target, original.Target)
	}
	if !restored.Timestamp.Equal(original.Timestamp) {
		t.Errorf("timestamp mismatch: got %v, want %v", restored.Timestamp, original.Timestamp)
	}
}
