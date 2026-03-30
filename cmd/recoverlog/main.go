// recover-log restores a log's state in the SQLite database from a known IPFS
// head CID, enabling the ucanlog service to resume the log without forking.
//
// Use this after a partial or total DB wipeout when the head CID is known from
// monitoring, the Storacha console, or operator notes.
//
// Usage:
//
//	recover-log --data-path ./data --log-id did:key:z6Mk... --head-cid bafybeifoo
//	recover-log --data-path ./data --log-id did:key:z6Mk... --head-cid bafybeifoo \
//	            --revocations-head-cid bafybeebar
//
// The --revocations-head-cid flag is required if the revocations log ever had
// entries written to it. Omitting it when revocations existed leaves the log
// exposed to formerly revoked credentials.
//
// Override the IPFS gateway with the IPFS_GATEWAY_URL environment variable
// (default: https://w3s.link).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/relves/ucanlog/internal/storage/sqlite"
	"github.com/relves/ucanlog/internal/storage/storacha"
)

func main() {
	dataPath := flag.String("data-path", "", "Path to the ucanlog data directory (required)")
	logID := flag.String("log-id", "", "DID of the log to recover (required)")
	headCID := flag.String("head-cid", "", "Last known head CID for the main log (required)")
	revocationsHeadCID := flag.String("revocations-head-cid", "", "Last known head CID for the revocations log (omit if no revocations were ever recorded)")
	flag.Parse()

	if *dataPath == "" || *logID == "" || *headCID == "" {
		flag.Usage()
		os.Exit(1)
	}

	gatewayURL := os.Getenv("IPFS_GATEWAY_URL")
	if gatewayURL == "" {
		gatewayURL = "https://w3s.link"
	}

	ctx := context.Background()

	if err := run(ctx, *dataPath, *logID, *headCID, *revocationsHeadCID, gatewayURL); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, dataPath, logID, headCID, revocationsHeadCID, gatewayURL string) error {
	storeManager := sqlite.NewStoreManager(dataPath)
	defer storeManager.CloseAll()

	// Recover the main log.
	if err := recoverOne(ctx, storeManager, logID, headCID, gatewayURL); err != nil {
		return fmt.Errorf("main log: %w", err)
	}
	fmt.Printf("main log recovered: %s\n", logID)

	// Recover the revocations log if a head CID was supplied.
	if revocationsHeadCID != "" {
		revLogID := logID + "-revocations"
		if err := recoverOne(ctx, storeManager, revLogID, revocationsHeadCID, gatewayURL); err != nil {
			return fmt.Errorf("revocations log: %w", err)
		}
		fmt.Printf("revocations log recovered: %s\n", revLogID)
	} else {
		fmt.Println("revocations log: skipped (no --revocations-head-cid supplied)")
	}

	fmt.Println()
	fmt.Println("Recovery complete.")
	fmt.Println("If ucanlog is running, the log will be restored on next access (no restart needed,")
	fmt.Println("unless the log was already loaded in memory with incorrect state — restart in that case).")
	return nil
}

// recoverOne ensures the log record exists and seeds the state store from the
// gateway so that ModeResume startup finds valid state.
func recoverOne(ctx context.Context, storeManager *sqlite.StoreManager, logID, headCID, gatewayURL string) error {
	store, err := storeManager.GetStore(logID)
	if err != nil {
		return fmt.Errorf("open state store: %w", err)
	}

	// Ensure the log record row exists — required by restoreLog before New() is called.
	if err := store.CreateLogRecord(ctx, logID); err != nil {
		// Ignore duplicate-key errors; the record may already exist.
		_ = err
	}

	return storacha.Recover(ctx, storacha.Config{
		LogDID:     logID,
		SpaceDID:   logID,
		StateStore: store,
		GatewayURL: gatewayURL,
	}, headCID)
}
