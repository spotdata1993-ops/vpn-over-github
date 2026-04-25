package server

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/sartoopjj/vpn-over-github/shared"
)

// ChannelListener discovers channels and starts a handler for each one.
type ChannelListener struct {
	cfg        *ServerConfig
	transports map[int]shared.Transport
	tokens     []TokenConfig

	mu       sync.Mutex
	handlers map[string]context.CancelFunc // channelID -> cancel
}

func NewChannelListener(cfg *ServerConfig, transports map[int]shared.Transport, tokens []TokenConfig) *ChannelListener {
	return &ChannelListener{
		cfg:        cfg,
		transports: transports,
		tokens:     tokens,
		handlers:   make(map[string]context.CancelFunc),
	}
}

func (l *ChannelListener) Run(ctx context.Context) error {
	scanInterval := l.cfg.GitHub.FetchInterval
	if scanInterval < 250*time.Millisecond {
		scanInterval = 250 * time.Millisecond
	}
	ticker := time.NewTicker(scanInterval)
	defer ticker.Stop()

	slog.Info("channel listener started", "scan_interval", scanInterval.String(), "fetch_interval", l.cfg.GitHub.FetchInterval.String())
	l.scan(ctx)

	for {
		select {
		case <-ctx.Done():
			l.stopAll()
			return nil
		case <-ticker.C:
			l.scan(ctx)
		}
	}
}

func (l *ChannelListener) scan(ctx context.Context) {
	active := make(map[string]struct{})
	hadListError := false

	for tokenIdx, transport := range l.transports {
		channels, err := transport.ListChannels(ctx)
		if err != nil {
			slog.Warn("list channels failed", "token_index", tokenIdx, "error", err)
			hadListError = true
			continue
		}
		for _, ch := range channels {
			active[ch.ID] = struct{}{}
			l.startHandler(ctx, tokenIdx, ch.ID, transport)
		}
	}

	if hadListError {
		return
	}

	l.mu.Lock()
	for channelID, cancel := range l.handlers {
		if _, ok := active[channelID]; ok {
			continue
		}
		cancel()
		delete(l.handlers, channelID)
		slog.Info("channel deleted, stopping handler", "channel_id", channelID)
	}
	l.mu.Unlock()
}

func (l *ChannelListener) startHandler(parent context.Context, tokenIdx int, channelID string, transport shared.Transport) {
	l.mu.Lock()
	if _, exists := l.handlers[channelID]; exists {
		l.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(parent)
	l.handlers[channelID] = cancel
	l.mu.Unlock()

	token := ""
	if tokenIdx < len(l.tokens) {
		token = l.tokens[tokenIdx].Token
	}
	h := NewChannelHandler(l.cfg, channelID, tokenIdx, transport, token)
	go func() {
		slog.Info("channel handler started", "channel_id", channelID, "token_index", tokenIdx)
		h.Run(ctx)
		l.mu.Lock()
		delete(l.handlers, channelID)
		l.mu.Unlock()
		slog.Info("channel handler stopped", "channel_id", channelID)
	}()
}

func (l *ChannelListener) stopAll() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for channelID, cancel := range l.handlers {
		cancel()
		delete(l.handlers, channelID)
	}
}
