package server

// Config holds server configuration.
type Config struct {
	Signer       interface{} // go-ucanto signer.Signer
	LogService   interface{} // log.LogService
	StoreManager interface{} // sqlite.StoreManager
	Validator    RequestValidator
}

// Option configures the server.
type Option func(*Config)

// WithSigner sets the UCAN signer.
func WithSigner(s interface{}) Option {
	return func(c *Config) {
		c.Signer = s
	}
}

// WithLogService sets the log service.
func WithLogService(ls interface{}) Option {
	return func(c *Config) {
		c.LogService = ls
	}
}

// WithStoreManager sets the SQLite store manager.
func WithStoreManager(sm interface{}) Option {
	return func(c *Config) {
		c.StoreManager = sm
	}
}

// WithValidator sets a request validator for account/rate-limit checks.
// If nil (default), no validation is performed.
func WithValidator(v RequestValidator) Option {
	return func(c *Config) {
		c.Validator = v
	}
}

func applyOptions(opts ...Option) *Config {
	cfg := &Config{}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}
