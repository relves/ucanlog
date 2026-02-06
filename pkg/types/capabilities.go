// pkg/types/capabilities.go
package types

// Capability constants for UCAN authorization.
const (
	CapabilityAll           = "tlog/*"
	CapabilityAdmin         = "tlog/admin"
	CapabilityAdminRevoke   = "tlog/admin/revoke"
	CapabilityAdminDelegate = "tlog/admin/delegate"
	CapabilityAppend        = "tlog/append"
	CapabilityRead          = "tlog/read"
	CapabilityGarbage       = "tlog/gc" // For GC with remove delegation
)

// ResourceURI creates a resource URI for a log.
func ResourceURI(logID string) string {
	return "tlog://log/" + logID
}
