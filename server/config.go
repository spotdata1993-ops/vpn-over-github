package server

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

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

// ServerConfig holds all server configuration.
type ServerConfig struct {
	Cleanup struct {
		Enabled           bool          `yaml:"enabled"`
		Interval          time.Duration `yaml:"interval"`
		DeadConnectionTTL time.Duration `yaml:"dead_connection_ttl"`
	} `yaml:"cleanup"`

	Proxy struct {
		TargetTimeout time.Duration `yaml:"target_timeout"`
		BufferSize    int           `yaml:"buffer_size"`
	} `yaml:"proxy"`

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
}

// DefaultServerConfig returns a ServerConfig with sensible defaults.
func DefaultServerConfig() *ServerConfig {
	cfg := &ServerConfig{}
	cfg.Cleanup.Enabled = false
	cfg.Cleanup.Interval = 10 * time.Minute
	cfg.Cleanup.DeadConnectionTTL = 15 * time.Minute
	cfg.Proxy.TargetTimeout = 30 * time.Second
	cfg.Proxy.BufferSize = 65536
	cfg.GitHub.UpstreamConnections = 2
	cfg.GitHub.BatchInterval = 100 * time.Millisecond
	cfg.GitHub.FetchInterval = 200 * time.Millisecond
	cfg.GitHub.APITimeout = 10 * time.Second
	cfg.Encryption.Algorithm = "xor"
	cfg.Logging.Level = "info"
	cfg.Logging.Format = "text"
	cfg.Logging.File = "/var/log/gh-tunnel/server.log"
	return cfg
}

// LoadServerConfig loads the server YAML config file.
func LoadServerConfig(path string) (*ServerConfig, error) {
	cfg := DefaultServerConfig()
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading server config %q: %w", path, err)
	}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parsing server config YAML: %w", err)
	}
	if err := validateServerConfig(cfg); err != nil {
		return nil, fmt.Errorf("invalid server config: %w", err)
	}
	return cfg, nil
}

// validateServerConfig checks required fields and value ranges.
func validateServerConfig(cfg *ServerConfig) error {
	if len(cfg.GitHub.Tokens) == 0 {
		return fmt.Errorf("github.tokens must have at least one token")
	}
	for i, tc := range cfg.GitHub.Tokens {
		tok := strings.TrimSpace(tc.Token)
		if tok == "" {
			return fmt.Errorf("github.tokens[%d].token is empty", i)
		}
		if !strings.HasPrefix(tok, "ghp_") && !strings.HasPrefix(tok, "github_pat_") {
			return fmt.Errorf("github.tokens[%d] must start with ghp_ or github_pat_", i)
		}
		cfg.GitHub.Tokens[i].Token = tok
		transport := tc.Transport
		if transport == "" {
			transport = "git"
		}
		if transport != "gist" && transport != "git" {
			return fmt.Errorf("github.tokens[%d] has unsupported transport %q (use gist or git)", i, transport)
		}
		if transport == "git" && tc.Repo == "" {
			return fmt.Errorf("github.tokens[%d] uses transport=git but no repo is set", i)
		}
	}
	algo := strings.ToLower(cfg.Encryption.Algorithm)
	if algo != "xor" && algo != "aes" {
		return fmt.Errorf("encryption.algorithm must be xor or aes, got %q", algo)
	}
	cfg.Encryption.Algorithm = algo
	if cfg.GitHub.UpstreamConnections <= 0 {
		cfg.GitHub.UpstreamConnections = 2
	}
	if cfg.GitHub.BatchInterval <= 0 {
		cfg.GitHub.BatchInterval = 100 * time.Millisecond
	}
	if cfg.GitHub.FetchInterval <= 0 {
		cfg.GitHub.FetchInterval = 200 * time.Millisecond
	}
	if cfg.Proxy.BufferSize <= 0 {
		cfg.Proxy.BufferSize = 65536
	}
	if cfg.Cleanup.Interval <= 0 {
		cfg.Cleanup.Interval = 10 * time.Minute
	}
	if cfg.Cleanup.DeadConnectionTTL <= 0 {
		cfg.Cleanup.DeadConnectionTTL = 15 * time.Minute
	}
	if cfg.Proxy.TargetTimeout <= 0 {
		cfg.Proxy.TargetTimeout = 30 * time.Second
	}
	if cfg.GitHub.APITimeout <= 0 {
		cfg.GitHub.APITimeout = 10 * time.Second
	}
	return nil
}
