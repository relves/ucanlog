package server

import (
	"context"

	"github.com/storacha/go-ucanto/core/invocation"
)

// RequestValidator validates incoming UCAN invocations before processing.
// Implementations can check account status, rate limits, permissions, etc.
type RequestValidator interface {
	// ValidateRequest is called before each capability invocation.
	// Return nil to allow the request, or an error to reject it.
	// The error message will be returned to the client.
	ValidateRequest(ctx context.Context, inv invocation.Invocation) error
}

// ValidationError represents a validation failure with structured info.
type ValidationError struct {
	Code    string // Machine-readable error code (e.g., "ACCOUNT_SUSPENDED")
	Message string // Human-readable message
}

func (e *ValidationError) Error() string {
	return e.Message
}

// NewValidationError creates a new validation error.
func NewValidationError(code, message string) *ValidationError {
	return &ValidationError{Code: code, Message: message}
}
