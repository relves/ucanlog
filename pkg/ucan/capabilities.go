// pkg/ucan/capabilities.go
package ucan

import (
	"fmt"
	"strings"

	"github.com/relves/ucanlog/pkg/types"
)

// CapabilityInfo represents a validated capability
type CapabilityInfo struct {
	With string
	Can  string
}

// CapabilityAllows checks if a held capability grants the required capability.
func CapabilityAllows(held, required, resource string) bool {
	// Wildcard grants everything
	if held == types.CapabilityAll {
		return true
	}

	// Exact match
	if held == required {
		return true
	}

	// Hierarchical: tlog/admin allows tlog/admin/*
	if strings.HasPrefix(required, held+"/") {
		return true
	}

	return false
}

// RequiredCapability returns the required capability for an operation.
func RequiredCapability(operation string) string {
	switch operation {
	case "append":
		return types.CapabilityAppend
	case "read":
		return types.CapabilityRead
	case "revoke":
		return types.CapabilityAdminRevoke
	case "delegate":
		return types.CapabilityAdminDelegate
	default:
		return types.CapabilityAll
	}
}

// ParseResourceGroup extracts the group ID from a resource URI.
func ParseResourceGroup(resource string) (types.GroupID, error) {
	// Expected format: tlog://group/<group-id>
	prefix := "tlog://group/"
	if !strings.HasPrefix(resource, prefix) {
		return "", fmt.Errorf("invalid resource URI: %s", resource)
	}
	return types.GroupID(strings.TrimPrefix(resource, prefix)), nil
}
