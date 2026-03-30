package server_test

import (
	"testing"

	"github.com/relves/ucanlog/pkg/server"
	"github.com/stretchr/testify/require"
)

func TestNewServerRequiresParameters(t *testing.T) {
	_, err := server.NewServer()
	require.Error(t, err)
	require.Contains(t, err.Error(), "signer is required")
}
