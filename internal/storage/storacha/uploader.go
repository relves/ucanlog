// storage/storacha/uploader.go
package storacha

import (
	"bytes"
	"context"
	"fmt"

	"github.com/relves/ucanlog/internal/storage/storacha/indexpersist"
)

// StorachaUploader uploads CAR files to Storacha.
type StorachaUploader struct {
	clientRef *clientRef
	spaceDID  string
}

// NewStorachaUploader creates an uploader using the provided Storacha client.
func NewStorachaUploader(clientRef *clientRef, spaceDID string) *StorachaUploader {
	return &StorachaUploader{
		clientRef: clientRef,
		spaceDID:  spaceDID,
	}
}

// UploadCAR uploads CAR data to Storacha using the dag/add capability.
// This properly indexes all blocks in the CAR, making the DAG browsable.
// The delegation is retrieved from context (set via WithDelegation).
func (u *StorachaUploader) UploadCAR(ctx context.Context, data []byte) (string, error) {
	if u.clientRef == nil {
		return "", fmt.Errorf("no client configured")
	}
	client := u.clientRef.Get()
	if client == nil {
		return "", fmt.Errorf("no client configured")
	}

	// Get delegation from context
	dlg := GetDelegation(ctx)
	if dlg == nil {
		return "", fmt.Errorf("delegation required in context for write operations")
	}

	// Delegate to the client's UploadCAR method which uses dag/add
	return client.UploadCAR(ctx, u.spaceDID, data, dlg)
}

// UploadCARReader uploads CAR data from a reader.
func (u *StorachaUploader) UploadCARReader(ctx context.Context, r *bytes.Reader) (string, error) {
	data := make([]byte, r.Len())
	if _, err := r.Read(data); err != nil {
		return "", fmt.Errorf("failed to read CAR data: %w", err)
	}
	return u.UploadCAR(ctx, data)
}

// Ensure StorachaUploader implements indexpersist.Uploader
var _ indexpersist.Uploader = (*StorachaUploader)(nil)
