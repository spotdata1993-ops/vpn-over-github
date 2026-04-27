package client

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Version is set at build time via -ldflags.
// Set this from cmd/client/main.go or via ldflags:
// -X github.com/sartoopjj/vpn-over-github/client.Version=x.y.z
var Version = "dev"

// TokenConfig holds per-token GitHub configuration.
// Transport defaults to "git" when empty.
// Repo is required when Transport is "git".
//
// BatchInterval and FetchInterval can override the global github.batch_interval
// and github.fetch_interval for this token only — useful when mixing a fast
// `git` transport with a rate-limited `gist` transport in the same config.
type TokenConfig struct {
	Token         string        `yaml:"token"`
	Transport     string        `yaml:"transport"`
	Repo          string        `yaml:"repo"`
	BatchInterval time.Duration `yaml:"batch_interval,omitempty"`
	FetchInterval time.Duration `yaml:"fetch_interval,omitempty"`
}

// EffectiveBatchInterval returns the per-token batch interval if set, else fallback.
func (tc TokenConfig) EffectiveBatchInterval(fallback time.Duration) time.Duration {
	if tc.BatchInterval > 0 {
		return tc.BatchInterval
	}
	return fallback
}

// EffectiveFetchInterval returns the per-token fetch interval if set, else fallback.
func (tc TokenConfig) EffectiveFetchInterval(fallback time.Duration) time.Duration {
	if tc.FetchInterval > 0 {
		return tc.FetchInterval
	}
	return fallback
}

// EffectiveTransport returns the configured transport, defaulting to "git".
func (tc TokenConfig) EffectiveTransport() string {
	if tc.Transport == "" {
		return "git"
	}
	return tc.Transport
}

// Config holds all client configuration, loaded from YAML file and/or CLI flags.
// CLI flags always override config file values.
type Config struct {
	SOCKS struct {
		Listen  string        `yaml:"listen_addr"`
		Timeout time.Duration `yaml:"timeout"`
	} `yaml:"socks"`

	GitHub struct {
		Tokens              []TokenConfig `yaml:"tokens"`
		UpstreamConnections int           `yaml:"upstream_connections"`
		BatchInterval       time.Duration `yaml:"batch_interval"`
		FetchInterval       time.Duration `yaml:"fetch_interval"`
		APITimeout          time.Duration `yaml:"api_timeout"`
	} `yaml:"github"`

	Encryption struct {
		Algorithm string `yaml:"algorithm"`
	} `yaml:"encryption"`

	Logging struct {
		Level  string `yaml:"level"`
		Format string `yaml:"format"`
		File   string `yaml:"file"`
	} `yaml:"logging"`

	RateLimit struct {
		MaxRequestsPerHour int     `yaml:"max_requests_per_hour"`
		BurstLimit         int     `yaml:"burst_limit"`
		BackoffMultiplier  float64 `yaml:"backoff_multiplier"`
		LowRemainingWarn   int     `yaml:"low_remaining_warn"`
	} `yaml:"rate_limit"`
}

// DefaultConfig returns a Config with sensible production defaults.
func DefaultConfig() *Config {
	cfg := &Config{}

	cfg.SOCKS.Listen = "127.0.0.1:1080"
	cfg.SOCKS.Timeout = 30 * time.Second

	cfg.GitHub.UpstreamConnections = 2
	cfg.GitHub.BatchInterval = 100 * time.Millisecond
	cfg.GitHub.FetchInterval = 200 * time.Millisecond
	cfg.GitHub.APITimeout = 10 * time.Second

	cfg.Encryption.Algorithm = "xor"

	cfg.Logging.Level = "info"
	cfg.Logging.Format = "text"

	cfg.RateLimit.MaxRequestsPerHour = 4500
	cfg.RateLimit.BurstLimit = 25
	cfg.RateLimit.BackoffMultiplier = 2.0
	cfg.RateLimit.LowRemainingWarn = 30

	return cfg
}

// ParseFlags parses command-line flags, optionally loads a YAML config file,
// and returns the resolved configuration. CLI flags take precedence over the file.
func ParseFlags() (*Config, error) {
	var (
		configFile  = flag.String("config", "client_config.yaml", "Path to YAML config file")
		socksListen = flag.String("socks-listen", "", `SOCKS5 listen address (default "127.0.0.1:1080")`)
		tokens      = flag.String("tokens", "", "Comma-separated GitHub tokens (overrides config file)")
		logLevel    = flag.String("log-level", "", "Log level: debug, info, warn, error")
		encAlgo     = flag.String("encryption-algo", "", "Encryption algorithm: xor, aes")
		showVersion = flag.Bool("version", false, "Print version and exit")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("gh-tunnel-client %s\n", Version)
		os.Exit(0)
	}

	cfg := DefaultConfig()

	if *configFile != "" {
		if err := loadConfigFile(cfg, *configFile); err != nil {
			return nil, fmt.Errorf("loading config file %q: %w", *configFile, err)
		}
	}

	// CLI flags override config file
	if *socksListen != "" {
		cfg.SOCKS.Listen = *socksListen
	}
	if *tokens != "" {
		cfg.GitHub.Tokens = parseTokensFlag(*tokens)
	}
	if *logLevel != "" {
		cfg.Logging.Level = *logLevel
	}
	if *encAlgo != "" {
		cfg.Encryption.Algorithm = *encAlgo
	}

	return cfg, validateClientConfig(cfg)
}

// loadConfigFile reads and unmarshals a YAML config file into cfg.
func loadConfigFile(cfg *Config, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("reading config file: %w", err)
	}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return fmt.Errorf("parsing YAML: %w", err)
	}
	return nil
}

// parseTokensFlag splits a comma-separated token string from --tokens flag.
// Each entry becomes a TokenConfig with default git transport.
func parseTokensFlag(s string) []TokenConfig {
	parts := strings.Split(s, ",")
	result := make([]TokenConfig, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			result = append(result, TokenConfig{Token: t})
		}
	}
	return result
}

// validateClientConfig checks that all required fields are present and valid.
// Returns a clear error message that helps the user understand what to fix.
func validateClientConfig(cfg *Config) error {
	if len(cfg.GitHub.Tokens) == 0 {
		return fmt.Errorf("at least one GitHub token is required\n" +
			"  Use --tokens ghp_xxx or set github.tokens in config file\n" +
			"  Token must have the 'gist' scope (gist transport) or 'repo' scope (git transport)")
	}
	for i, tc := range cfg.GitHub.Tokens {
		if tc.Token == "" {
			return fmt.Errorf("token at index %d is empty", i)
		}
		if !strings.HasPrefix(tc.Token, "ghp_") && !strings.HasPrefix(tc.Token, "github_pat_") {
			return fmt.Errorf("token %d does not look like a GitHub token "+
				"(expected 'ghp_' or 'github_pat_' prefix)", i+1)
		}
		transport := tc.Transport
		if transport == "" {
			transport = "git"
		}
		if transport != "gist" && transport != "git" {
			return fmt.Errorf("token %d has unsupported transport %q (use 'gist' or 'git')", i+1, transport)
		}
		if transport == "git" && tc.Repo == "" {
			return fmt.Errorf("token %d uses transport=git but no repo is set "+
				"(set token.repo to 'owner/repo')", i+1)
		}
	}
	if cfg.SOCKS.Listen == "" {
		return fmt.Errorf("socks.listen is required (e.g., '127.0.0.1:1080')")
	}
	switch cfg.Encryption.Algorithm {
	case "xor", "aes":
		// valid
	default:
		return fmt.Errorf("unsupported encryption algorithm %q (use 'xor' or 'aes')", cfg.Encryption.Algorithm)
	}
	if cfg.RateLimit.MaxRequestsPerHour < 1 {
		return fmt.Errorf("rate_limit.max_requests_per_hour must be >= 1, got %d", cfg.RateLimit.MaxRequestsPerHour)
	}
	if cfg.RateLimit.LowRemainingWarn < 1 {
		return fmt.Errorf("rate_limit.low_remaining_warn must be >= 1, got %d", cfg.RateLimit.LowRemainingWarn)
	}
	if cfg.GitHub.UpstreamConnections <= 0 {
		cfg.GitHub.UpstreamConnections = 2
	}
	if cfg.GitHub.BatchInterval <= 0 {
		cfg.GitHub.BatchInterval = 100 * time.Millisecond
	}
	if cfg.GitHub.FetchInterval <= 0 {
		cfg.GitHub.FetchInterval = 200 * time.Millisecond
	}
	if cfg.GitHub.APITimeout <= 0 {
		cfg.GitHub.APITimeout = 10 * time.Second
	}
	return nil
}
