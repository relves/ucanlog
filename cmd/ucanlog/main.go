package main

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"

	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	thttp "github.com/storacha/go-ucanto/transport/http"

	"github.com/relves/ucanlog/internal/storage/sqlite"
	logSvc "github.com/relves/ucanlog/pkg/log"
	"github.com/relves/ucanlog/pkg/server"
	"github.com/relves/ucanlog/pkg/tlog"
	"github.com/relves/ucanlog/pkg/ucan"
)

func main() {
	basePath := getEnv("DATA_PATH", "./data")

	levelStr := getEnv("LOG_LEVEL", "info")
	var level slog.Level
	if err := level.UnmarshalText([]byte(levelStr)); err != nil {
		level = slog.LevelInfo
	}
	handler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: level,
	})
	logger := slog.New(handler)
	slog.SetDefault(logger)

	// Initialize UCAN issuer (using keys from env var or ephemeral)
	// In production, load proper keys from secure storage
	pub, priv, err := loadKeys()
	if err != nil {
		logger.Error("failed to load keys", "error", err)
		os.Exit(1)
	}
	ucanIssuer := ucan.NewIssuer(priv, pub)

	// Create Ed25519 signer for Tessera checkpoints
	tlogSigner, err := tlog.NewEd25519Signer(ed25519.PrivateKey(priv), "ucanlog")
	if err != nil {
		logger.Error("failed to create tlog signer", "error", err)
		os.Exit(1)
	}

	// Create ucanto service signer (needed for delegated storage)
	serviceSigner, err := signer.FromRaw(priv)
	if err != nil {
		logger.Error("failed to create service signer", "error", err)
		os.Exit(1)
	}

	// Create SQLite store manager for state persistence
	storeManager := sqlite.NewStoreManager(basePath)
	defer storeManager.CloseAll()

	// Create CID store for tracking latest index CIDs (backed by SQLite)
	cidStore := tlog.NewStateStoreCIDStore(storeManager.GetStateStore)

	// Get origin prefix from environment or use default
	originPrefix := getEnv("TLOG_ORIGIN_PREFIX", "ucanlog")

	// Create tlog manager with delegated storage model
	// Each customer provides their own Storacha delegation - no service-owned space needed
	tlogMgr, err := tlog.NewDelegatedManager(tlog.DelegatedManagerConfig{
		BasePath:      basePath,
		Signer:        tlogSigner,
		PrivateKey:    priv,
		OriginPrefix:  originPrefix,
		ServiceSigner: serviceSigner,
		CIDStore:      cidStore,
		Logger:        logger,
	})
	if err != nil {
		logger.Error("failed to create delegated tlog manager", "error", err)
		os.Exit(1)
	}

	logger.Info("using customer-delegated Storacha storage for transparency logs")

	logService := logSvc.NewLogServiceWithConfig(logSvc.LogServiceConfig{
		TlogManager:  tlogMgr,
		UcanIssuer:   ucanIssuer,
		StoreManager: storeManager,
	})

	// Create ucanto server
	ucantoServer, err := server.NewServer(
		server.WithSigner(serviceSigner),
		server.WithLogService(logService),
		server.WithStoreManager(storeManager),
		server.WithValidator(nil),
	)
	if err != nil {
		logger.Error("failed to create ucanto server", "error", err)
		os.Exit(1)
	}

	// Create tlog-tiles API handler using IPFS gateway proxy
	// Tiles are stored in customer spaces and retrieved via IPFS gateway
	gatewayURL := getEnv("IPFS_GATEWAY_URL", "https://w3s.link")
	tlogHandler := server.NewTlogIPFSHandler(cidStore, gatewayURL, http.DefaultClient)

	// Create HTTP handler for head endpoint
	httpHandler := server.NewHTTPHandler(storeManager)

	// HTTP routes
	mux := http.NewServeMux()

	// UCAN RPC endpoint (POST)
	mux.HandleFunc("POST /", func(w http.ResponseWriter, r *http.Request) {
		req := thttp.NewRequest(r.Body, r.Header)

		res, err := ucantoServer.Request(r.Context(), req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		for name, values := range res.Headers() {
			for _, value := range values {
				w.Header().Add(name, value)
			}
		}

		if res.Status() != 0 {
			w.WriteHeader(res.Status())
		}

		body := res.Body()
		io.Copy(w, body)
		body.Close()
	})

	// tlog-tiles API endpoints (GET) - public for witness validation
	mux.HandleFunc("GET /logs/{logID}/head", httpHandler.HandleGetHead)
	mux.HandleFunc("GET /logs/{logID}/checkpoint", tlogHandler.HandleCheckpoint)
	mux.HandleFunc("GET /logs/{logID}/tile/{level}/{tilePath...}", tlogHandler.HandleTile)
	mux.HandleFunc("GET /logs/{logID}/tile/entries/{entryPath...}", tlogHandler.HandleEntries)

	port := getEnv("PORT", "8080")
	addr := ":" + port

	fmt.Println("UCANLOG Service Startup")
	fmt.Println("===================================")
	fmt.Printf("Service DID: %s\n", serviceSigner.DID().String())
	fmt.Printf("Public Key (hex): %s\n", hex.EncodeToString(pub))
	if os.Getenv("UCANLOG_PRIVATE_KEY") != "" {
		fmt.Println("Key Source: UCANLOG_PRIVATE_KEY environment variable")
	} else {
		fmt.Println("Key Source: Ephemeral (generated on startup)")
	}
	fmt.Println("Storage Backend: Customer-delegated Storacha spaces")
	fmt.Printf("IPFS Gateway: %s\n", gatewayURL)
	fmt.Println()
	fmt.Println("UCAN RPC Endpoint (authenticated):")
	fmt.Printf("  POST http://localhost:%s/\n", port)
	fmt.Println()
	fmt.Println("UCAN Capabilities:")
	fmt.Println("  tlog/create      - Create new transparent log")
	fmt.Println("  tlog/append      - Append entries")
	fmt.Println("  tlog/read        - Read entries")
	fmt.Println("  tlog/revoke      - Revoke delegations")
	fmt.Println()
	fmt.Println("Log State API:")
	fmt.Printf("  GET http://localhost:%s/logs/{logID}/head\n", port)
	fmt.Println()
	fmt.Println("Public tlog-tiles API (for witness validation):")
	fmt.Printf("  GET http://localhost:%s/logs/{logID}/checkpoint\n", port)
	fmt.Printf("  GET http://localhost:%s/logs/{logID}/tile/{level}/{path}\n", port)
	fmt.Printf("  GET http://localhost:%s/logs/{logID}/tile/entries/{path}\n", port)

	if err := http.ListenAndServe(addr, mux); err != nil {
		logger.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// loadKeys loads Ed25519 keys from UCANLOG_PRIVATE_KEY env var or generates new ones
func loadKeys() (publicKey, privateKey []byte, err error) {
	// Check for UCANLOG_PRIVATE_KEY environment variable
	if privKeyEnv := os.Getenv("UCANLOG_PRIVATE_KEY"); privKeyEnv != "" {
		// Decode base64-encoded private key
		priv, err := base64.StdEncoding.DecodeString(privKeyEnv)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to decode UCANLOG_PRIVATE_KEY: %w", err)
		}

		if len(priv) != ed25519.PrivateKeySize {
			return nil, nil, fmt.Errorf("UCANLOG_PRIVATE_KEY must be %d bytes, got %d", ed25519.PrivateKeySize, len(priv))
		}

		// Derive public key from private key
		privKey := ed25519.PrivateKey(priv)
		pubKey := privKey.Public().(ed25519.PublicKey)

		return pubKey, priv, nil
	}

	// Fall back to generating ephemeral keys
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		return nil, nil, err
	}
	return pub, priv, nil
}
