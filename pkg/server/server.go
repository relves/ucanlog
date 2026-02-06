package server

import (
	"context"
	"errors"
	"fmt"

	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/core/result/failure"
	"github.com/storacha/go-ucanto/core/schema"
	"github.com/storacha/go-ucanto/principal"
	ucantoServer "github.com/storacha/go-ucanto/server"
	"github.com/storacha/go-ucanto/server/transaction"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/go-ucanto/validator"

	"github.com/relves/ucanlog/pkg/capabilities"
	logSvc "github.com/relves/ucanlog/pkg/log"
)

// LogService defines the interface for log operations.
// This allows the pkg/server to depend on an interface rather than concrete type.
type LogService interface {
	// Defined in Task 3
}

// ProvideWithoutAuth is like ucantoServer.Provide but skips UCAN authorization.
// Authorization is expected to be handled in the handler via delegation in caveats.
// This allows anyone to invoke the capability - access control is done by validating
// the Storacha delegation passed in the caveats.
func ProvideWithoutAuth[C any, O ipld.Builder, X failure.IPLDBuilderFailure](
	capability validator.CapabilityParser[C],
	handler ucantoServer.HandlerFunc[C, O, X],
) ucantoServer.ServiceMethod[O, failure.IPLDBuilderFailure] {
	return func(ctx context.Context, inv invocation.Invocation, ictx ucantoServer.InvocationContext) (transaction.Transaction[O, failure.IPLDBuilderFailure], error) {
		// Confirm the audience of the invocation is this service
		acceptedAudiences := schema.Literal(ictx.ID().DID().String())
		if len(ictx.AlternativeAudiences()) > 0 {
			altAudiences := make([]schema.Reader[string, string], 0, len(ictx.AlternativeAudiences()))
			for _, a := range ictx.AlternativeAudiences() {
				altAudiences = append(altAudiences, schema.Literal(a.DID().String()))
			}
			acceptedAudiences = schema.Or(append(altAudiences, acceptedAudiences)...)
		}

		if _, err := acceptedAudiences.Read(inv.Audience().DID().String()); err != nil {
			expectedAudiences := append([]ucan.Principal{ictx.ID()}, ictx.AlternativeAudiences()...)
			audErr := ucantoServer.NewInvalidAudienceError(inv.Audience(), expectedAudiences...)
			return transaction.NewTransaction(result.Error[O, failure.IPLDBuilderFailure](audErr)), nil
		}

		// Parse the capability WITHOUT full UCAN authorization
		// We just need to extract and validate the capability schema
		caps := inv.Capabilities()
		if len(caps) == 0 {
			return transaction.NewTransaction(result.Error[O](failure.FromError(fmt.Errorf("no capabilities in invocation")))), nil
		}

		// Create a source from the first capability (invocation is self-issued, so delegation is itself)
		source := validator.NewSource(caps[0], inv)

		// Match the capability against the expected schema
		match, invalidCap := capability.Match(source)
		if invalidCap != nil {
			return transaction.NewTransaction(result.Error[O](failure.FromError(invalidCap))), nil
		}

		// Get the parsed capability from the match
		// The match contains the capability with properly typed caveats
		parsedCap := match.Value()

		res, effects, herr := handler(ctx, parsedCap, inv, ictx)
		if herr != nil {
			return nil, herr
		}

		return transaction.NewTransaction(
			result.MapResultR0(
				res,
				func(o O) O { return o },
				func(x X) failure.IPLDBuilderFailure { return x },
			),
			transaction.WithEffects(effects),
		), nil
	}
}

// NewServer creates a new UCAN log server with optional validation.
//
// Parameters:
//   - opts: Configuration options (WithSigner, WithLogService, WithStoreManager, WithValidator)
//
// Returns a UCanto server ready to handle HTTP requests.
func NewServer(opts ...Option) (ucantoServer.ServerView[ucantoServer.Service], error) {
	cfg := applyOptions(opts...)

	if cfg.Signer == nil {
		return nil, errors.New("signer is required")
	}
	if cfg.LogService == nil {
		return nil, errors.New("logService is required")
	}
	if cfg.StoreManager == nil {
		return nil, errors.New("storeManager is required")
	}

	// Import the sqlite package to get the concrete type
	// The storeManager is passed as interface{} to avoid circular imports
	return newServerDirect(cfg.Signer.(principal.Signer), cfg.LogService.(*logSvc.LogService), cfg.StoreManager, cfg.Validator)
}

// newServerDirect creates a UCanto server for handling tlog capabilities.
// The validator parameter is optional - pass nil to skip validation.
func newServerDirect(
	signer principal.Signer,
	logService *logSvc.LogService,
	storeManager interface{},
	validator RequestValidator,
) (ucantoServer.ServerView[ucantoServer.Service], error) {
	serviceDID := signer.DID().String()

	return ucantoServer.NewServer(
		signer,
		// Register tlog/create handler - uses ProvideWithoutAuth since authorization
		// is handled by validating the Storacha delegation in caveats
		ucantoServer.WithServiceMethod(
			capabilities.TlogCreate.Can(),
			ProvideWithoutAuth(
				capabilities.TlogCreate,
				createHandler(serviceDID, logService, storeManager, validator),
			),
		),
		// Register tlog/append handler
		ucantoServer.WithServiceMethod(
			capabilities.TlogAppend.Can(),
			ProvideWithoutAuth(
				capabilities.TlogAppend,
				appendHandler(serviceDID, logService, storeManager, validator),
			),
		),
		// Register tlog/read handler - public read, no authorization needed
		ucantoServer.WithServiceMethod(
			capabilities.TlogRead.Can(),
			ProvideWithoutAuth(
				capabilities.TlogRead,
				readHandler(logService, validator),
			),
		),
		// Register tlog/admin/revoke handler
		ucantoServer.WithServiceMethod(
			capabilities.TlogRevoke.Can(),
			ProvideWithoutAuth(
				capabilities.TlogRevoke,
				revokeHandler(serviceDID, logService, validator),
			),
		),
		// Register tlog/gc handler
		ucantoServer.WithServiceMethod(
			capabilities.TlogGarbage.Can(),
			ProvideWithoutAuth(
				capabilities.TlogGarbage,
				garbageHandler(serviceDID, logService, validator),
			),
		),
	)
}
