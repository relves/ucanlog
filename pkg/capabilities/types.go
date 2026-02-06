// Package capabilities defines the public types for tlog UCAN capabilities.
package capabilities

// Capability ability constants
const (
	AbilityCreate  = "tlog/create"
	AbilityAppend  = "tlog/append"
	AbilityRead    = "tlog/read"
	AbilityRevoke  = "tlog/revoke"  // Changed from tlog/admin/revoke
	AbilityGarbage = "tlog/gc" // For GC with remove delegation
)

// CreateCaveats represents the caveats for tlog/create capability
type CreateCaveats struct {
	// Delegation is the base64-encoded UCAN delegation granting write access to the space
	Delegation string `json:"delegation"`
}

// CreateSuccess is the success result for tlog/create
type CreateSuccess struct {
	LogID    string `json:"logId"`
	IndexCID string `json:"index_cid"` // Initial head CID
	TreeSize uint64 `json:"tree_size"` // Initial tree size (0)
}

// CreateFailure is the failure result for tlog/create
type CreateFailure struct {
	name    string
	message string
}

func (f CreateFailure) Name() string {
	return f.name
}

func (f CreateFailure) Error() string {
	return f.message
}

// NewCreateFailure creates a new CreateFailure
func NewCreateFailure(name, message string) CreateFailure {
	return CreateFailure{name: name, message: message}
}

// AppendCaveats represents the caveats for tlog/append capability
type AppendCaveats struct {
	// Data is the base64-encoded data to append
	Data string `json:"data"`

	// IndexCID is the expected current head CID for optimistic concurrency (optional)
	IndexCID *string `json:"index_cid,omitempty"`

	// Delegation is the base64-encoded UCAN delegation (required)
	Delegation string `json:"delegation"`
}

// AppendSuccess is the success result for tlog/append
type AppendSuccess struct {
	Index       int64  `json:"index"`
	NewIndexCID string `json:"new_index_cid"` // New head CID after append
	TreeSize    uint64 `json:"tree_size"`     // New tree size
}

// AppendFailure is the failure result for tlog/append
type AppendFailure struct {
	name    string
	message string
}

func (f AppendFailure) Name() string {
	return f.name
}

func (f AppendFailure) Error() string {
	return f.message
}

// NewAppendFailure creates a new AppendFailure
func NewAppendFailure(name, message string) AppendFailure {
	return AppendFailure{name: name, message: message}
}

// ReadCaveats represents the caveats for tlog/read capability
type ReadCaveats struct {
	// Offset is the starting index (optional)
	Offset *int64
	// Limit is the maximum number of entries to return (optional)
	Limit *int64
}

// ReadSuccess is the success result for tlog/read
type ReadSuccess struct {
	Entries []string `json:"entries"`
	Total   int64    `json:"total"`
}

// ReadFailure is the failure result for tlog/read
type ReadFailure struct {
	name    string
	message string
}

func (f ReadFailure) Name() string {
	return f.name
}

func (f ReadFailure) Error() string {
	return f.message
}

// NewReadFailure creates a new ReadFailure
func NewReadFailure(name, message string) ReadFailure {
	return ReadFailure{name: name, message: message}
}

// RevokeCaveats represents the caveats for tlog/revoke capability
type RevokeCaveats struct {
	// Cid is the CID of the delegation to revoke.
	// The delegation must be stored in the space (uploaded by the client).
	Cid string `json:"cid"`

	// Delegation grants access to the space for fetching the delegation
	// and writing to the revocation log (base64-encoded)
	Delegation string `json:"delegation"`
}

// RevokeSuccess is the success result for tlog/revoke
type RevokeSuccess struct {
	Revoked bool `json:"revoked"`
}

// RevokeFailure is the failure result for tlog/revoke
type RevokeFailure struct {
	name    string
	message string
}

func (f RevokeFailure) Name() string {
	return f.name
}

func (f RevokeFailure) Error() string {
	return f.message
}

// NewRevokeFailure creates a new RevokeFailure
func NewRevokeFailure(name, message string) RevokeFailure {
	return RevokeFailure{name: name, message: message}
}

// GarbageCaveats represents the caveats for tlog/gc capability
type GarbageCaveats struct {
	// LogID is the space DID (log identifier)
	LogID string `json:"logId"`

	// Delegation grants space/blob/remove capability (base64-encoded)
	// Must be a direct delegation from space owner to service
	Delegation string `json:"delegation"`
}

// GarbageSuccess is the success result for tlog/gc
type GarbageSuccess struct {
	BundlesProcessed int    `json:"bundlesProcessed"` // Number of bundles processed
	BlobsRemoved     int    `json:"blobsRemoved"`     // Number of blobs removed
	BytesFreed       uint64 `json:"bytesFreed"`       // Bytes freed (estimated)
	NewGCPosition    uint64 `json:"newGCPosition"`    // New GC checkpoint position
}

// GarbageFailure is the failure result for tlog/gc
type GarbageFailure struct {
	name    string
	message string
}

func (f GarbageFailure) Name() string {
	return f.name
}

func (f GarbageFailure) Error() string {
	return f.message
}

// NewGarbageFailure creates a new GarbageFailure
func NewGarbageFailure(name, message string) GarbageFailure {
	return GarbageFailure{name: name, message: message}
}
