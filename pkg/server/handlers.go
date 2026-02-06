package server

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/storacha/go-ucanto/core/dag/blockstore"
	"github.com/storacha/go-ucanto/core/delegation"

	"github.com/relves/ucanlog/internal/storage/sqlite"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/receipt/fx"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/server"
	"github.com/storacha/go-ucanto/ucan"

	"github.com/relves/ucanlog/pkg/capabilities"
	logSvc "github.com/relves/ucanlog/pkg/log"
	ucanPkg "github.com/relves/ucanlog/pkg/ucan"
)

// createHandler returns a handler function for tlog/create capability
func createHandler(serviceDID string, logService *logSvc.LogService, storeManager interface{}, validator RequestValidator) server.HandlerFunc[capabilities.CreateCaveats, capabilities.CreateSuccess, capabilities.CreateFailure] {
	return func(
		ctx context.Context,
		cap ucan.Capability[capabilities.CreateCaveats],
		inv invocation.Invocation,
		ictx server.InvocationContext,
	) (result.Result[capabilities.CreateSuccess, capabilities.CreateFailure], fx.Effects, error) {
		// Validate request if validator is configured
		if validator != nil {
			if err := validator.ValidateRequest(ctx, inv); err != nil {
				var vErr *ValidationError
				if errors.As(err, &vErr) {
					return result.Error[capabilities.CreateSuccess](capabilities.NewCreateFailure(
						vErr.Code,
						vErr.Message,
					)), nil, nil
				}
				return result.Error[capabilities.CreateSuccess](capabilities.NewCreateFailure(
					"VALIDATION_ERROR",
					err.Error(),
				)), nil, nil
			}
		}

		// Parse delegation
		dlg, err := ucanPkg.ParseDelegation(cap.Nb().Delegation)
		if err != nil {
			return result.Error[capabilities.CreateSuccess](capabilities.NewCreateFailure(
				"InvalidDelegation",
				fmt.Sprintf("failed to parse delegation: %v", err),
			)), nil, nil
		}

		// Extract space DID from delegation
		spaceDID, err := ucanPkg.ExtractSpaceDID(dlg)
		if err != nil {
			return result.Error[capabilities.CreateSuccess](capabilities.NewCreateFailure(
				"InvalidSpaceDID",
				fmt.Sprintf("failed to extract space DID: %v", err),
			)), nil, nil
		}

		// Validate delegation
		if err := ucanPkg.ValidateDelegation(dlg, serviceDID, spaceDID); err != nil {
			return result.Error[capabilities.CreateSuccess](capabilities.NewCreateFailure(
				"InvalidDelegation",
				err.Error(),
			)), nil, nil
		}

		// Validate invocation authority
		// The invocation issuer must be the delegation issuer
		invocationIssuerDID := inv.Issuer().DID().String()
		if err := ucanPkg.ValidateInvocationAuthority(invocationIssuerDID, dlg); err != nil {
			return result.Error[capabilities.CreateSuccess](capabilities.NewCreateFailure(
				ucanPkg.ErrCodeInvocationNotAuthorized,
				err.Error(),
			)), nil, nil
		}

		// Validate proof chain
		// The delegation must trace back to the space owner
		if err := ucanPkg.ValidateProofChain(dlg, spaceDID); err != nil {
			return result.Error[capabilities.CreateSuccess](capabilities.NewCreateFailure(
				ucanPkg.ErrCodeDelegationNoAuthority,
				err.Error(),
			)), nil, nil
		}

		// Create log using space DID as identity
		logResult, err := logService.CreateLogWithDelegation(ctx, logSvc.CreateLogParams{
			SpaceDID:   spaceDID,
			Delegation: dlg,
		})
		if err != nil {
			return result.Error[capabilities.CreateSuccess](capabilities.NewCreateFailure(
				"LogCreationFailed",
				fmt.Sprintf("failed to create log: %v", err),
			)), nil, nil
		}

		// Fetch initial state from store
		var indexCID string
		var treeSize uint64
		if storeManager != nil {
			if sm, ok := storeManager.(*sqlite.StoreManager); ok {
				if store, err := sm.GetStore(logResult.LogID); err == nil {
					indexCID, treeSize, _ = store.GetHead(ctx, logResult.LogID)
				}
			}
		}

		return result.Ok[capabilities.CreateSuccess, capabilities.CreateFailure](capabilities.CreateSuccess{
			LogID:    logResult.LogID,
			IndexCID: indexCID,
			TreeSize: treeSize,
		}), nil, nil
	}
}

// appendHandler returns a handler function for tlog/append capability
func appendHandler(serviceDID string, logService *logSvc.LogService, storeManager interface{}, validator RequestValidator) server.HandlerFunc[capabilities.AppendCaveats, capabilities.AppendSuccess, capabilities.AppendFailure] {
	return func(
		ctx context.Context,
		cap ucan.Capability[capabilities.AppendCaveats],
		inv invocation.Invocation,
		ictx server.InvocationContext,
	) (result.Result[capabilities.AppendSuccess, capabilities.AppendFailure], fx.Effects, error) {
		// Validate request if validator is configured
		if validator != nil {
			if err := validator.ValidateRequest(ctx, inv); err != nil {
				var vErr *ValidationError
				if errors.As(err, &vErr) {
					return result.Error[capabilities.AppendSuccess](capabilities.NewAppendFailure(
						vErr.Code,
						vErr.Message,
					)), nil, nil
				}
				return result.Error[capabilities.AppendSuccess](capabilities.NewAppendFailure(
					"VALIDATION_ERROR",
					err.Error(),
				)), nil, nil
			}
		}

		// Delegation is now required
		if cap.Nb().Delegation == "" {
			return result.Error[capabilities.AppendSuccess](capabilities.NewAppendFailure(
				"MissingDelegation",
				"delegation is required",
			)), nil, nil
		}

		// Parse delegation
		dlg, err := ucanPkg.ParseDelegation(cap.Nb().Delegation)
		if err != nil {
			return result.Error[capabilities.AppendSuccess](capabilities.NewAppendFailure(
				"InvalidDelegation",
				fmt.Sprintf("failed to parse delegation: %v", err),
			)), nil, nil
		}

		// Extract space DID (this is the log identity)
		spaceDID, err := ucanPkg.ExtractSpaceDID(dlg)
		if err != nil {
			return result.Error[capabilities.AppendSuccess](capabilities.NewAppendFailure(
				"InvalidSpaceDID",
				fmt.Sprintf("failed to extract space DID: %v", err),
			)), nil, nil
		}

		// Validate delegation
		if err := ucanPkg.ValidateDelegation(dlg, serviceDID, spaceDID); err != nil {
			return result.Error[capabilities.AppendSuccess](capabilities.NewAppendFailure(
				"InvalidDelegation",
				err.Error(),
			)), nil, nil
		}

		// Validate invocation authority
		invocationIssuerDID := inv.Issuer().DID().String()
		if err := ucanPkg.ValidateInvocationAuthority(invocationIssuerDID, dlg); err != nil {
			return result.Error[capabilities.AppendSuccess](capabilities.NewAppendFailure(
				ucanPkg.ErrCodeInvocationNotAuthorized,
				err.Error(),
			)), nil, nil
		}

		// Validate proof chain
		// The delegation must trace back to the space owner
		if err := ucanPkg.ValidateProofChain(dlg, spaceDID); err != nil {
			return result.Error[capabilities.AppendSuccess](capabilities.NewAppendFailure(
				ucanPkg.ErrCodeDelegationNoAuthority,
				err.Error(),
			)), nil, nil
		}

		// Check if the delegation (from caveat) or any in its proof chain is revoked
		revokedCID, err := checkDelegationChainRevoked(ctx, dlg, spaceDID, logService)
		if err != nil {
			return result.Error[capabilities.AppendSuccess](capabilities.NewAppendFailure(
				"RevocationCheckFailed",
				fmt.Sprintf("failed to check delegation revocations: %v", err),
			)), nil, nil
		}
		if revokedCID != "" {
			return result.Error[capabilities.AppendSuccess](capabilities.NewAppendFailure(
				"DelegationRevoked",
				fmt.Sprintf("delegation %s has been revoked", revokedCID),
			)), nil, nil
		}

		// Also check revocations for any proofs attached to the invocation itself
		revokedCID, err = checkRevocations(ctx, inv, spaceDID, logService)
		if err != nil {
			return result.Error[capabilities.AppendSuccess](capabilities.NewAppendFailure(
				"RevocationCheckFailed",
				fmt.Sprintf("failed to check revocations: %v", err),
			)), nil, nil
		}
		if revokedCID != "" {
			return result.Error[capabilities.AppendSuccess](capabilities.NewAppendFailure(
				"DelegationRevoked",
				fmt.Sprintf("delegation %s has been revoked", revokedCID),
			)), nil, nil
		}

		// Validate optimistic concurrency (IndexCID from caveats must match current head)
		// Only validate if IndexCID is provided (optional field)
		if storeManager != nil && cap.Nb().IndexCID != nil {
			sm, ok := storeManager.(*sqlite.StoreManager)
			if !ok {
				return result.Error[capabilities.AppendSuccess](capabilities.NewAppendFailure(
					"InternalError",
					"store manager type mismatch",
				)), nil, nil
			}

			store, err := sm.GetStore(spaceDID)
			if err != nil {
				return result.Error[capabilities.AppendSuccess](capabilities.NewAppendFailure(
					"StoreAccessFailed",
					fmt.Sprintf("failed to get store: %v", err),
				)), nil, nil
			}

			expectedIndexCID := *cap.Nb().IndexCID
			currentIndexCID, treeSize, err := store.GetHead(ctx, spaceDID)
			if err != nil {
				return result.Error[capabilities.AppendSuccess](capabilities.NewAppendFailure(
					"HeadAccessFailed",
					fmt.Sprintf("failed to get current head: %v", err),
				)), nil, nil
			}
			if currentIndexCID != expectedIndexCID {
				// Head mismatch - concurrent modification detected
				return result.Error[capabilities.AppendSuccess](capabilities.NewAppendFailure(
					"HeadMismatch",
					fmt.Sprintf("expected head %s but current head is %s (tree size: %d)",
						expectedIndexCID, currentIndexCID, treeSize),
				)), nil, nil
			}
		}

		// Decode base64 data from caveats
		data, err := base64.StdEncoding.DecodeString(cap.Nb().Data)
		if err != nil {
			return result.Error[capabilities.AppendSuccess](capabilities.NewAppendFailure(
				"InvalidData",
				fmt.Sprintf("failed to decode base64 data: %v", err),
			)), nil, nil
		}

		// Append to the log using spaceDID and the validated delegation
		index, err := logService.Append(ctx, spaceDID, data, dlg)
		if err != nil {
			return result.Error[capabilities.AppendSuccess](capabilities.NewAppendFailure(
				"AppendFailed",
				fmt.Sprintf("failed to append to log: %v", err),
			)), nil, nil
		}

		// Get updated head and tree size after append
		var newIndexCID string
		var treeSize uint64
		if storeManager != nil {
			if sm, ok := storeManager.(*sqlite.StoreManager); ok {
				if store, err := sm.GetStore(spaceDID); err == nil {
					newIndexCID, treeSize, _ = store.GetHead(ctx, spaceDID)
				}
			}
		}

		return result.Ok[capabilities.AppendSuccess, capabilities.AppendFailure](capabilities.AppendSuccess{
			Index:       int64(index),
			NewIndexCID: newIndexCID,
			TreeSize:    treeSize,
		}), nil, nil
	}
}

// readHandler returns a handler function for tlog/read capability
func readHandler(logService *logSvc.LogService, validator RequestValidator) server.HandlerFunc[capabilities.ReadCaveats, capabilities.ReadSuccess, capabilities.ReadFailure] {
	return func(
		ctx context.Context,
		cap ucan.Capability[capabilities.ReadCaveats],
		inv invocation.Invocation,
		ictx server.InvocationContext,
	) (result.Result[capabilities.ReadSuccess, capabilities.ReadFailure], fx.Effects, error) {
		// Validate request if validator is configured
		if validator != nil {
			if err := validator.ValidateRequest(ctx, inv); err != nil {
				var vErr *ValidationError
				if errors.As(err, &vErr) {
					return result.Error[capabilities.ReadSuccess](capabilities.NewReadFailure(
						vErr.Code,
						vErr.Message,
					)), nil, nil
				}
				return result.Error[capabilities.ReadSuccess](capabilities.NewReadFailure(
					"VALIDATION_ERROR",
					err.Error(),
				)), nil, nil
			}
		}

		// Extract logID from the "with" field - this is the space DID
		logID := cap.With()

		// Check for revoked delegations
		revokedCID, err := checkRevocations(ctx, inv, logID, logService)
		if err != nil {
			return result.Error[capabilities.ReadSuccess](capabilities.NewReadFailure(
				"RevocationCheckFailed",
				fmt.Sprintf("failed to check revocations: %v", err),
			)), nil, nil
		}
		if revokedCID != "" {
			return result.Error[capabilities.ReadSuccess](capabilities.NewReadFailure(
				"DelegationRevoked",
				fmt.Sprintf("delegation %s has been revoked", revokedCID),
			)), nil, nil
		}

		// Extract pagination parameters from caveats
		var offset, limit int64 = 0, 100
		if cap.Nb().Offset != nil {
			offset = *cap.Nb().Offset
		}
		if cap.Nb().Limit != nil {
			limit = *cap.Nb().Limit
		}

		// Read from the log
		readResult, err := logService.Read(ctx, logID, offset, limit)
		if err != nil {
			return result.Error[capabilities.ReadSuccess](capabilities.NewReadFailure(
				"ReadFailed",
				fmt.Sprintf("failed to read from log: %v", err),
			)), nil, nil
		}

		// Convert byte slices to strings
		entries := make([]string, len(readResult.Entries))
		for i, entry := range readResult.Entries {
			entries[i] = string(entry)
		}

		return result.Ok[capabilities.ReadSuccess, capabilities.ReadFailure](capabilities.ReadSuccess{
			Entries: entries,
			Total:   int64(readResult.Total),
		}), nil, nil
	}
}

// revokeHandler returns a handler function for tlog/admin/revoke capability
func revokeHandler(serviceDID string, logService *logSvc.LogService, validator RequestValidator) server.HandlerFunc[capabilities.RevokeCaveats, capabilities.RevokeSuccess, capabilities.RevokeFailure] {
	return func(
		ctx context.Context,
		cap ucan.Capability[capabilities.RevokeCaveats],
		inv invocation.Invocation,
		ictx server.InvocationContext,
	) (result.Result[capabilities.RevokeSuccess, capabilities.RevokeFailure], fx.Effects, error) {
		// Validate request if validator is configured
		if validator != nil {
			if err := validator.ValidateRequest(ctx, inv); err != nil {
				var vErr *ValidationError
				if errors.As(err, &vErr) {
					return result.Error[capabilities.RevokeSuccess](capabilities.NewRevokeFailure(
						vErr.Code,
						vErr.Message,
					)), nil, nil
				}
				return result.Error[capabilities.RevokeSuccess](capabilities.NewRevokeFailure(
					"VALIDATION_ERROR",
					err.Error(),
				)), nil, nil
			}
		}

		// 1. Validate CID is provided
		cidToRevoke := cap.Nb().Cid
		if cidToRevoke == "" {
			return result.Error[capabilities.RevokeSuccess](capabilities.NewRevokeFailure(
				"MissingCID",
				"cid is required",
			)), nil, nil
		}

		// 2. Parse the storage delegation (for fetching and writing)
		storageDlg, err := ucanPkg.ParseDelegation(cap.Nb().Delegation)
		if err != nil {
			return result.Error[capabilities.RevokeSuccess](capabilities.NewRevokeFailure(
				"InvalidDelegation",
				fmt.Sprintf("failed to parse storage delegation: %v", err),
			)), nil, nil
		}

		// 3. Extract space DID from storage delegation
		spaceDID, err := ucanPkg.ExtractSpaceDID(storageDlg)
		if err != nil {
			return result.Error[capabilities.RevokeSuccess](capabilities.NewRevokeFailure(
				"InvalidSpaceDID",
				fmt.Sprintf("failed to extract space DID: %v", err),
			)), nil, nil
		}

		// 4. Validate storage delegation grants access
		if err := ucanPkg.ValidateDelegation(storageDlg, serviceDID, spaceDID); err != nil {
			return result.Error[capabilities.RevokeSuccess](capabilities.NewRevokeFailure(
				"InvalidDelegation",
				fmt.Sprintf("storage delegation invalid: %v", err),
			)), nil, nil
		}

		// Validate invocation authority for storage delegation
		invocationIssuerDID := inv.Issuer().DID().String()
		if err := ucanPkg.ValidateInvocationAuthority(invocationIssuerDID, storageDlg); err != nil {
			return result.Error[capabilities.RevokeSuccess](capabilities.NewRevokeFailure(
				ucanPkg.ErrCodeInvocationNotAuthorized,
				fmt.Sprintf("not authorized to use storage delegation: %v", err),
			)), nil, nil
		}

		// Validate proof chain for storage delegation
		if err := ucanPkg.ValidateProofChain(storageDlg, spaceDID); err != nil {
			return result.Error[capabilities.RevokeSuccess](capabilities.NewRevokeFailure(
				ucanPkg.ErrCodeDelegationNoAuthority,
				fmt.Sprintf("storage delegation has no authority: %v", err),
			)), nil, nil
		}

		// 5. Get a blob fetcher for the space
		fetcher, err := logService.GetBlobFetcher(ctx, spaceDID, storageDlg)
		if err != nil {
			return result.Error[capabilities.RevokeSuccess](capabilities.NewRevokeFailure(
				"FetcherError",
				fmt.Sprintf("failed to get blob fetcher: %v", err),
			)), nil, nil
		}

		// 6. Fetch the delegation to revoke from storage
		dlgToRevoke, err := ucanPkg.FetchDelegation(ctx, fetcher, cidToRevoke)
		if err != nil {
			return result.Error[capabilities.RevokeSuccess](capabilities.NewRevokeFailure(
				"DelegationNotFound",
				fmt.Sprintf("failed to fetch delegation %s: %v (delegation must be stored in the space before revocation)", cidToRevoke, err),
			)), nil, nil
		}

		// 7. Validate revocation authority
		// The revoker is the issuer of the invocation
		revokerDID := inv.Issuer().DID().String()
		if err := ucanPkg.ValidateRevocationAuthority(revokerDID, dlgToRevoke); err != nil {
			return result.Error[capabilities.RevokeSuccess](capabilities.NewRevokeFailure(
				"NotAuthorized",
				err.Error(),
			)), nil, nil
		}

		// 8. Get the delegation's internal CID (this is what we check against during authorization)
		delegationCID := dlgToRevoke.Link().String()

		// 9. Add to revocation log using the delegation's internal CID and storage delegation
		if err := logService.Revoke(ctx, spaceDID, delegationCID, storageDlg); err != nil {
			return result.Error[capabilities.RevokeSuccess](capabilities.NewRevokeFailure(
				"RevokeFailed",
				fmt.Sprintf("failed to revoke delegation: %v", err),
			)), nil, nil
		}

		return result.Ok[capabilities.RevokeSuccess, capabilities.RevokeFailure](capabilities.RevokeSuccess{
			Revoked: true,
		}), nil, nil
	}
}

// func didToEd25519PublicKey(did string) (ed25519.PublicKey, error) {
// 	// Use go-ucanto's verifier to parse the DID
// 	v, err := verifier.Parse(did)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to parse DID %s: %w", did, err)
// 	}

// 	// The verifier implements principal.Verifier which has Raw()
// 	pv, ok := v.(principal.Verifier)
// 	if !ok {
// 		return nil, fmt.Errorf("verifier does not implement principal.Verifier")
// 	}

// 	// Raw() returns the 32-byte Ed25519 public key
// 	rawKey := pv.Raw()
// 	if len(rawKey) != ed25519.PublicKeySize {
// 		return nil, fmt.Errorf("unexpected key size: got %d, want %d", len(rawKey), ed25519.PublicKeySize)
// 	}

// 	return ed25519.PublicKey(rawKey), nil
// }

// checkDelegationRevokedSQLite recursively checks if a delegation or any in its proof chain is revoked using SQLite
func checkDelegationRevokedSQLite(
	ctx context.Context,
	logService *logSvc.LogService,
	logID string,
	dlg delegation.Delegation,
	bs blockstore.BlockReader,
) (string, error) {
	// Check if this delegation is revoked
	delegationCID := dlg.Link().String()
	isRevoked, err := logService.IsRevoked(ctx, logID, delegationCID)
	if err != nil {
		return "", fmt.Errorf("failed to check if delegation is revoked: %w", err)
	}
	if isRevoked {
		return delegationCID, nil
	}

	// Recursively check all proofs in the delegation's proof chain
	proofLinks := dlg.Proofs()
	proofs := delegation.NewProofsView(proofLinks, bs)
	for _, proof := range proofs {
		if proofDlg, ok := proof.Delegation(); ok {
			if revokedCID, err := checkDelegationRevokedSQLite(ctx, logService, logID, proofDlg, bs); err != nil {
				return "", err
			} else if revokedCID != "" {
				return revokedCID, nil
			}
		}
	}

	return "", nil
}

// checkDelegationChainRevoked checks if a delegation (passed in caveat) or any in its proof chain is revoked.
// This is used to check delegations that are passed as base64 strings in request caveats.
func checkDelegationChainRevoked(
	ctx context.Context,
	dlg delegation.Delegation,
	logID string,
	logService *logSvc.LogService,
) (string, error) {
	// Create a block reader from the delegation's embedded blocks
	bs, err := blockstore.NewBlockReader(blockstore.WithBlocksIterator(dlg.Blocks()))
	if err != nil {
		return "", fmt.Errorf("failed to create block reader: %w", err)
	}

	// Check this delegation and its proof chain
	return checkDelegationRevokedSQLite(ctx, logService, logID, dlg, bs)
}

// checkRevocations checks if any delegation in the proof chain is revoked.
// Returns the CID of the revoked delegation if found, empty string otherwise.
func checkRevocations(
	ctx context.Context,
	inv invocation.Invocation,
	logID string,
	logService *logSvc.LogService,
) (string, error) {
	// Create a block reader from the invocation's blocks
	bs, err := blockstore.NewBlockReader(blockstore.WithBlocksIterator(inv.Blocks()))
	if err != nil {
		return "", fmt.Errorf("failed to create block reader: %w", err)
	}

	// Resolve proof links to delegations and check each one recursively
	proofLinks := inv.Proofs()
	proofs := delegation.NewProofsView(proofLinks, bs)
	for _, proof := range proofs {
		if proofDlg, ok := proof.Delegation(); ok {
			if revokedCID, err := checkDelegationRevokedSQLite(ctx, logService, logID, proofDlg, bs); err != nil {
				return "", err
			} else if revokedCID != "" {
				return revokedCID, nil
			}
		}
	}

	return "", nil
}

// garbageHandler returns a handler function for tlog/gc capability
func garbageHandler(serviceDID string, logService *logSvc.LogService, validator RequestValidator) server.HandlerFunc[capabilities.GarbageCaveats, capabilities.GarbageSuccess, capabilities.GarbageFailure] {
	return func(
		ctx context.Context,
		cap ucan.Capability[capabilities.GarbageCaveats],
		inv invocation.Invocation,
		ictx server.InvocationContext,
	) (result.Result[capabilities.GarbageSuccess, capabilities.GarbageFailure], fx.Effects, error) {
		// Validate request if validator is configured
		if validator != nil {
			if err := validator.ValidateRequest(ctx, inv); err != nil {
				var vErr *ValidationError
				if errors.As(err, &vErr) {
					return result.Error[capabilities.GarbageSuccess](capabilities.NewGarbageFailure(
						vErr.Code,
						vErr.Message,
					)), nil, nil
				}
				return result.Error[capabilities.GarbageSuccess](capabilities.NewGarbageFailure(
					"VALIDATION_ERROR",
					err.Error(),
				)), nil, nil
			}
		}

		// Extract logID and delegation from caveats
		logID := cap.Nb().LogID
		delegationStr := cap.Nb().Delegation

		if logID == "" {
			return result.Error[capabilities.GarbageSuccess](capabilities.NewGarbageFailure(
				"MISSING_LOG_ID",
				"logId is required",
			)), nil, nil
		}

		if delegationStr == "" {
			return result.Error[capabilities.GarbageSuccess](capabilities.NewGarbageFailure(
				"MISSING_DELEGATION",
				"delegation is required",
			)), nil, nil
		}

		// Parse delegation
		dlg, err := ucanPkg.ParseDelegation(delegationStr)
		if err != nil {
			return result.Error[capabilities.GarbageSuccess](capabilities.NewGarbageFailure(
				ucanPkg.ErrCodeDelegationParseError,
				fmt.Sprintf("failed to parse delegation: %v", err),
			)), nil, nil
		}

		// CRITICAL: Validate delegation is DIRECT from space owner to service
		// Issuer must be the space DID (no intermediaries allowed)
		issuerDID := dlg.Issuer().DID().String()
		if issuerDID != logID {
			return result.Error[capabilities.GarbageSuccess](capabilities.NewGarbageFailure(
				ucanPkg.ErrCodeGCDelegationNotDirect,
				fmt.Sprintf("GC delegation must be issued by space owner %s, but was issued by %s", logID, issuerDID),
			)), nil, nil
		}

		// Validate GC delegation (checks for space/blob/remove capability and direct delegation)
		if err := ucanPkg.ValidateGCDelegation(dlg, serviceDID, logID); err != nil {
			var dlgErr *ucanPkg.DelegationError
			if errors.As(err, &dlgErr) {
				return result.Error[capabilities.GarbageSuccess](capabilities.NewGarbageFailure(
					dlgErr.Code,
					dlgErr.Message,
				)), nil, nil
			}
			return result.Error[capabilities.GarbageSuccess](capabilities.NewGarbageFailure(
				"INVALID_DELEGATION",
				err.Error(),
			)), nil, nil
		}

		// Validate invocation authorization (issuer must match delegation issuer)
		invocationIssuerDID := inv.Issuer().DID().String()
		if err := ucanPkg.ValidateInvocationAuthority(invocationIssuerDID, dlg); err != nil {
			return result.Error[capabilities.GarbageSuccess](capabilities.NewGarbageFailure(
				ucanPkg.ErrCodeInvocationNotAuthorized,
				err.Error(),
			)), nil, nil
		}

		// Run garbage collection with this delegation
		gcResult, err := logService.RunGC(ctx, logID, dlg)
		if err != nil {
			return result.Error[capabilities.GarbageSuccess](capabilities.NewGarbageFailure(
				ucanPkg.ErrCodeGCFailed,
				fmt.Sprintf("garbage collection failed: %v", err),
			)), nil, nil
		}

		return result.Ok[capabilities.GarbageSuccess, capabilities.GarbageFailure](capabilities.GarbageSuccess{
			BundlesProcessed: gcResult.BundlesProcessed,
			BlobsRemoved:     gcResult.BlobsRemoved,
			BytesFreed:       gcResult.BytesFreed,
			NewGCPosition:    gcResult.NewGCPosition,
		}), nil, nil
	}
}
