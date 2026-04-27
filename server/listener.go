package server

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/sartoopjj/vpn-over-github/shared"
)

// ChannelListener discovers channels and starts a handler for each one.
//
// Each token's transport is scanned at its own scan interval — the gist
// transport burns a REST quota slot per non-cached scan, so it must scan
// slowly; the git transport reads the local working tree, so it can scan
// fast for free.
type ChannelListener struct {
	cfg        *ServerConfig
	transports map[int]shared.Transport
	tokens     []TokenConfig

	mu       sync.Mutex
	handlers map[string]context.CancelFunc // channelID -> cancel
	owner    map[string]int                // channelID -> tokenIdx
}

func NewChannelListener(cfg *ServerConfig, transports map[int]shared.Transport, tokens []TokenConfig) *ChannelListener {
	return &ChannelListener{
		cfg:        cfg,
		transports: transports,
		tokens:     tokens,
		handlers:   make(map[string]context.CancelFunc),
		owner:      make(map[string]int),
	}
}

// scanInterval picks a sane per-token scan interval based on the transport.
//   - gist: clamp to ≥2s. Each ListChannels burns one REST quota slot when
//     the result changes; ETag caches the unchanged case as 304 (free).
//   - git:  local-only readdir, so it can run as fast as the fetch interval
//     (with a 200ms floor to avoid spinning).
func (l *ChannelListener) scanInterval(tokenIdx int) time.Duration {
	transport := "git"
	tokenFetch := l.cfg.GitHub.FetchInterval
	if tokenIdx < len(l.tokens) {
		tc := l.tokens[tokenIdx]
		transport = tc.EffectiveTransport()
		tokenFetch = tc.EffectiveFetchInterval(tokenFetch)
	}

	if transport == "gist" {
		if tokenFetch < 2*time.Second {
			return 2 * time.Second
		}
		return tokenFetch
	}
	if tokenFetch < 200*time.Millisecond {
		return 200 * time.Millisecond
	}
	return tokenFetch
}

func (l *ChannelListener) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	for tokenIdx, transport := range l.transports {
		wg.Add(1)
		go func(tokenIdx int, transport shared.Transport) {
			defer wg.Done()
			l.runForToken(ctx, tokenIdx, transport)
		}(tokenIdx, transport)
	}

	transportKinds := make([]string, 0, len(l.tokens))
	for i := range l.tokens {
		transportKinds = append(transportKinds, l.tokens[i].EffectiveTransport())
	}
	slog.Info("channel listener started",
		"tokens", len(l.tokens),
		"transports", transportKinds,
	)

	wg.Wait()
	l.stopAll()
	return nil
}

func (l *ChannelListener) runForToken(ctx context.Context, tokenIdx int, transport shared.Transport) {
	interval := l.scanInterval(tokenIdx)
	slog.Info("channel scanner started",
		"token_idx", tokenIdx,
		"transport", l.transportKind(tokenIdx),
		"scan_interval", interval.String(),
	)

	l.scanOne(ctx, tokenIdx, transport)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			l.scanOne(ctx, tokenIdx, transport)
		}
	}
}

func (l *ChannelListener) transportKind(tokenIdx int) string {
	if tokenIdx >= len(l.tokens) {
		return "git"
	}
	return l.tokens[tokenIdx].EffectiveTransport()
}

// scanOne reconciles handlers for a single token's transport.
func (l *ChannelListener) scanOne(ctx context.Context, tokenIdx int, transport shared.Transport) {
	channels, err := transport.ListChannels(ctx)
	if err != nil {
		slog.Warn("list channels failed", "token_index", tokenIdx, "error", err)
		return
	}

	active := make(map[string]struct{}, len(channels))
	for _, ch := range channels {
		active[ch.ID] = struct{}{}
		l.startHandler(ctx, tokenIdx, ch.ID, transport)
	}

	// Stop handlers for channels that disappeared from THIS token's listing —
	// only ones we previously associated with this token, so per-token scans
	// don't fight each other.
	l.mu.Lock()
	defer l.mu.Unlock()
	for channelID, owner := range l.owner {
		if owner != tokenIdx {
			continue
		}
		if _, ok := active[channelID]; ok {
			continue
		}
		if cancel, exists := l.handlers[channelID]; exists {
			cancel()
			delete(l.handlers, channelID)
		}
		delete(l.owner, channelID)
		slog.Info("channel deleted, stopping handler", "channel_id", channelID, "token_index", tokenIdx)
	}
}

func (l *ChannelListener) startHandler(parent context.Context, tokenIdx int, channelID string, transport shared.Transport) {
	l.mu.Lock()
	if _, exists := l.handlers[channelID]; exists {
		l.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(parent)
	l.handlers[channelID] = cancel
	l.owner[channelID] = tokenIdx
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
		delete(l.owner, channelID)
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
	l.owner = make(map[string]int)
}
