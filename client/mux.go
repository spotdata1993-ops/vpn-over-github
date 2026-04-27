package client

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sartoopjj/vpn-over-github/shared"
)

// upstreamChannel holds the state for one pre-allocated channel (gist or git dir).
//
// Each upstream channel carries its own writer goroutine (batchWriteLoop) and
// reader goroutine (batchReadLoop). The writer is woken either by a periodic
// ticker or by an on-demand flush signal — VirtualConns ping flushSig from
// Connect / Write / Close so that interactive traffic doesn't have to wait
// the full BatchInterval.
type upstreamChannel struct {
	channelID string
	tokenIdx  int
	transport shared.Transport
	encryptor *shared.Encryptor

	transportKind string // "gist" or "git"
	batchInterval time.Duration
	fetchInterval time.Duration

	// epoch is randomized at startup; reader side resets dedup on epoch change.
	epoch    int64
	batchSeq atomic.Int64

	// dedup state for the server-written batch we read.
	lastReadEpoch atomic.Int64
	lastReadSeq   atomic.Int64

	// flushSig is a 1-buffered chan; signalFlush() pings without blocking.
	flushSig chan struct{}
}

// signalFlush pings the writer loop to flush as soon as the coalescing window
// allows. It never blocks.
func (ch *upstreamChannel) signalFlush() {
	select {
	case ch.flushSig <- struct{}{}:
	default:
	}
}

// VirtualConn is a single SOCKS connection multiplexed over an upstream channel.
type VirtualConn struct {
	connID    string
	dst       string
	channel   *upstreamChannel
	recvBuf   chan []byte
	closed    chan struct{}
	closeOnce sync.Once

	// openSent flips to true once the OPEN frame has been emitted on the wire.
	// closeSent flips when the FrameClosing frame has been emitted.
	openSent  atomic.Bool
	closeSent atomic.Bool

	mu         sync.Mutex
	writeQueue []writeChunk
	readBuf    []byte

	seq       atomic.Int64
	bytesUp   atomic.Int64
	bytesDown atomic.Int64
	startTime time.Time
}

type writeChunk struct {
	data []byte
	seq  int64
}

// Write enqueues data to be sent to the server in the next batch.
func (vc *VirtualConn) Write(p []byte) (int, error) {
	select {
	case <-vc.closed:
		return 0, io.ErrClosedPipe
	default:
	}
	buf := make([]byte, len(p))
	copy(buf, p)
	seq := vc.seq.Add(1)
	vc.mu.Lock()
	vc.writeQueue = append(vc.writeQueue, writeChunk{data: buf, seq: seq})
	vc.mu.Unlock()
	vc.bytesUp.Add(int64(len(p)))
	if vc.channel != nil {
		vc.channel.signalFlush()
	}
	return len(p), nil
}

// Read blocks until data is received from the server.
func (vc *VirtualConn) Read(p []byte) (int, error) {
	vc.mu.Lock()
	if len(vc.readBuf) > 0 {
		n := copy(p, vc.readBuf)
		vc.readBuf = vc.readBuf[n:]
		vc.mu.Unlock()
		vc.bytesDown.Add(int64(n))
		return n, nil
	}
	vc.mu.Unlock()

	select {
	case data, ok := <-vc.recvBuf:
		if !ok {
			return 0, io.EOF
		}
		n := copy(p, data)
		if n < len(data) {
			vc.mu.Lock()
			vc.readBuf = append(vc.readBuf[:0], data[n:]...)
			vc.mu.Unlock()
		}
		vc.bytesDown.Add(int64(n))
		return n, nil
	case <-vc.closed:
		return 0, io.EOF
	}
}

// Close signals the connection closed. The next batch flush will emit a
// FrameClosing frame so the server can release the upstream destination.
func (vc *VirtualConn) Close() error {
	vc.closeOnce.Do(func() {
		close(vc.closed)
		if vc.channel != nil {
			vc.channel.signalFlush()
		}
	})
	return nil
}

// MuxClient manages N upstream channels per token, multiplexing all SOCKS
// connections through them using batched frames.
type MuxClient struct {
	cfg         *Config
	rateLimiter *RateLimiter
	channels    []*upstreamChannel

	mu          sync.RWMutex
	conns       map[string]*VirtualConn // connID → VirtualConn
	nextChannel uint64                  // round-robin index (atomic via mu)
}

// NewMuxClient creates and starts a MuxClient. It pre-allocates N channels
// per token using EnsureChannel.
func NewMuxClient(ctx context.Context, cfg *Config, rl *RateLimiter, transports map[int]shared.Transport) (*MuxClient, error) {
	n := cfg.GitHub.UpstreamConnections
	if n <= 0 {
		n = 2
	}

	m := &MuxClient{
		cfg:         cfg,
		rateLimiter: rl,
		conns:       make(map[string]*VirtualConn),
	}

	var wg sync.WaitGroup
	var initMu sync.Mutex
	var initErr error

	for tokenIdx, transport := range transports {
		wg.Add(1)
		go func(tokenIdx int, transport shared.Transport) {
			defer wg.Done()
			channels, err := m.initTokenChannels(ctx, tokenIdx, transport, n)
			initMu.Lock()
			defer initMu.Unlock()
			if err != nil {
				slog.Error("token channel initialization failed", "token_idx", tokenIdx, "error", err)
				if initErr == nil {
					initErr = err
				}
			}
			m.channels = append(m.channels, channels...)
		}(tokenIdx, transport)
	}

	wg.Wait()

	if len(m.channels) == 0 {
		if initErr != nil {
			return nil, fmt.Errorf("no upstream channels available: %w", initErr)
		}
		return nil, fmt.Errorf("no upstream channels available")
	}

	for _, ch := range m.channels {
		go m.batchWriteLoop(ctx, ch)
		go m.batchReadLoop(ctx, ch)
	}
	return m, nil
}

// Connect opens a new virtual connection to dst. Returns a VirtualConn that
// implements io.ReadWriteCloser. The OPEN frame is sent on the next flush
// (which is signalled immediately, not waited for).
func (m *MuxClient) Connect(_ context.Context, dst string) (*VirtualConn, error) {
	connID, err := shared.GenerateConnID()
	if err != nil {
		return nil, fmt.Errorf("generating conn ID: %w", err)
	}

	m.mu.Lock()
	idx := m.nextChannel % uint64(len(m.channels))
	m.nextChannel++
	ch := m.channels[idx]
	vc := &VirtualConn{
		connID:    connID,
		dst:       dst,
		channel:   ch,
		recvBuf:   make(chan []byte, 256),
		closed:    make(chan struct{}),
		startTime: time.Now(),
	}
	m.conns[connID] = vc
	m.mu.Unlock()

	// Wake the writer immediately so the OPEN frame goes out within
	// the coalescing window (≈ batchInterval / 4) instead of waiting a
	// full ticker period.
	ch.signalFlush()

	slog.Info("virtual connection opened", "conn_id", connID, "dst", dst, "channel", ch.channelID)
	return vc, nil
}

// CloseConn marks vc as closed. The actual FrameClosing frame is emitted on
// the next batch flush.
func (m *MuxClient) CloseConn(_ context.Context, vc *VirtualConn) {
	_ = vc.Close()
}

// CloseAll closes all connections and deletes upstream channels.
func (m *MuxClient) CloseAll(ctx context.Context) {
	m.mu.Lock()
	conns := make([]*VirtualConn, 0, len(m.conns))
	for _, vc := range m.conns {
		conns = append(conns, vc)
	}
	m.conns = make(map[string]*VirtualConn)
	m.mu.Unlock()

	for _, vc := range conns {
		vc.closeOnce.Do(func() { close(vc.closed) })
	}

	// Best-effort delete of channels; ignore individual failures.
	for _, ch := range m.channels {
		if err := ch.transport.DeleteChannel(ctx, ch.channelID); err != nil {
			slog.Warn("cleanup channel failed", "channel_id", ch.channelID, "error", err)
		}
	}
}

// Snapshot returns a sorted (by start time) view of all active connections.
func (m *MuxClient) Snapshot() []ConnSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]ConnSnapshot, 0, len(m.conns))
	for _, vc := range m.conns {
		ch := vc.channel
		transport := ""
		if ch != nil {
			transport = ch.transportKind
		}
		var tokenIdx int
		if ch != nil {
			tokenIdx = ch.tokenIdx
		}
		out = append(out, ConnSnapshot{
			ConnID:    vc.connID,
			Dst:       vc.dst,
			Transport: transport,
			TokenIdx:  tokenIdx,
			BytesUp:   vc.bytesUp.Load(),
			BytesDown: vc.bytesDown.Load(),
			StartTime: vc.startTime,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].StartTime.Before(out[j].StartTime)
	})
	return out
}

// ── batch loops ────────────────────────────────────────────────────────────

// batchWriteLoop drives the write side of one upstream channel.
//
// The timer is single-shot and reset to batchInterval after every flush.
// flushSig may shorten the timer (down to fastFlushGap) so that interactive
// connect/write events don't wait a full tick. flushSig is bounded so a
// busy connection cannot trigger more than ~1 flush per fastFlushGap.
func (m *MuxClient) batchWriteLoop(ctx context.Context, ch *upstreamChannel) {
	batchInterval := ch.batchInterval
	if batchInterval <= 0 {
		batchInterval = m.cfg.GitHub.BatchInterval
	}
	if batchInterval <= 0 {
		batchInterval = 100 * time.Millisecond
	}

	fastFlushGap := batchInterval / 4
	if fastFlushGap < 20*time.Millisecond {
		fastFlushGap = 20 * time.Millisecond
	}
	if fastFlushGap > batchInterval {
		fastFlushGap = batchInterval
	}

	timer := time.NewTimer(batchInterval)
	defer timer.Stop()
	nextDeadline := time.Now().Add(batchInterval)

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			m.flushOutbound(ctx, ch)
			nextDeadline = time.Now().Add(batchInterval)
			timer.Reset(batchInterval)
		case <-ch.flushSig:
			target := time.Now().Add(fastFlushGap)
			if target.Before(nextDeadline) {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				d := time.Until(target)
				if d < 0 {
					d = 0
				}
				timer.Reset(d)
				nextDeadline = target
			}
		}
	}
}

func (m *MuxClient) flushOutbound(ctx context.Context, ch *upstreamChannel) {
	m.mu.RLock()
	snapshot := make([]*VirtualConn, 0, len(m.conns))
	for _, vc := range m.conns {
		if vc.channel == ch {
			snapshot = append(snapshot, vc)
		}
	}
	m.mu.RUnlock()

	frames := make([]shared.Frame, 0, len(snapshot))
	toDelete := make([]string, 0)

	for _, vc := range snapshot {
		vc.mu.Lock()
		queue := vc.writeQueue
		vc.writeQueue = nil
		vc.mu.Unlock()

		isClosed := false
		select {
		case <-vc.closed:
			isClosed = true
		default:
		}

		needsOpen := !vc.openSent.Load()

		// Already-closed conn that never opened: just drop it. The server
		// never knew about this conn so no cleanup frame is needed.
		if needsOpen && len(queue) == 0 && isClosed {
			toDelete = append(toDelete, vc.connID)
			continue
		}

		// Already-closed and we already sent the close frame previously:
		// final GC pass.
		if isClosed && vc.closeSent.Load() {
			toDelete = append(toDelete, vc.connID)
			continue
		}

		// Nothing to send and not closed and OPEN already sent: skip.
		if !needsOpen && len(queue) == 0 && !isClosed {
			continue
		}

		// Bare-OPEN case: we need to send OPEN but have no data yet.
		if needsOpen && len(queue) == 0 {
			seq := vc.seq.Add(1)
			status := shared.FrameActive
			if isClosed {
				status = shared.FrameClosing
				vc.closeSent.Store(true)
				toDelete = append(toDelete, vc.connID)
			}
			frames = append(frames, shared.Frame{
				ConnID: vc.connID,
				Seq:    seq,
				Dst:    vc.dst,
				Status: status,
			})
			vc.openSent.Store(true)
			continue
		}

		// We have data (and possibly close). The OPEN is piggybacked onto
		// the first frame that ACTUALLY goes out — not blindly onto the
		// first iteration — so an encrypt failure on chunk[0] doesn't
		// leave the server with a data frame for an unknown conn.
		// Same logic for FrameClosing: the close marker only applies if
		// the close-bearing chunk was successfully appended.
		for i, chunk := range queue {
			isLast := i == len(queue)-1
			encoded := ""
			if len(chunk.data) > 0 {
				enc, err := ch.encryptor.Encrypt(chunk.data, vc.connID, chunk.seq)
				if err != nil {
					slog.Warn("encrypt failed", "conn_id", vc.connID, "error", err)
					continue
				}
				encoded = enc
			}
			dst := ""
			if needsOpen {
				dst = vc.dst
			}
			status := shared.FrameActive
			closingHere := isClosed && isLast
			if closingHere {
				status = shared.FrameClosing
			}
			frames = append(frames, shared.Frame{
				ConnID: vc.connID,
				Seq:    chunk.seq,
				Dst:    dst,
				Data:   encoded,
				Status: status,
			})
			if needsOpen {
				vc.openSent.Store(true)
				needsOpen = false
			}
			if closingHere {
				vc.closeSent.Store(true)
				toDelete = append(toDelete, vc.connID)
			}
		}

		// Defensive: if we processed all chunks but the conn is closed and we
		// somehow didn't tag any of them as Closing (e.g., all encrypts failed),
		// emit an explicit close frame.
		if isClosed && !vc.closeSent.Load() {
			frames = append(frames, shared.Frame{
				ConnID: vc.connID,
				Seq:    vc.seq.Add(1),
				Status: shared.FrameClosing,
			})
			vc.closeSent.Store(true)
			toDelete = append(toDelete, vc.connID)
		}
	}

	if len(frames) == 0 {
		m.gcConns(toDelete)
		return
	}

	if err := m.acquireForToken(ctx, ch.tokenIdx); err != nil {
		slog.Debug("write skipped due to rate limiter", "token_idx", ch.tokenIdx, "error", err)
		return
	}

	seq := ch.batchSeq.Add(1)
	batch := &shared.Batch{
		Epoch:  ch.epoch,
		Seq:    seq,
		Ts:     time.Now().Unix(),
		Frames: frames,
	}
	if err := ch.transport.Write(ctx, ch.channelID, shared.ClientBatchFile, batch); err != nil {
		slog.Warn("batch write failed", "channel", ch.channelID, "error", err)
		return
	}

	m.afterTransportCall(ch.tokenIdx, ch.transport)

	// RecordWrite is only meaningful for the gist transport, which has the
	// 80/min and 500/hr secondary write quotas. The git transport has no
	// equivalent quota, so applying these counters there is wrong (it makes
	// the writer go silent for an hour after 500 batches).
	if ch.transportKind == "gist" {
		if wait := m.rateLimiter.RecordWrite(ch.tokenIdx); wait > 0 {
			select {
			case <-ctx.Done():
			case <-time.After(wait):
			}
		}
	}

	m.gcConns(toDelete)
}

// gcConns removes the given conn IDs from the active map.
func (m *MuxClient) gcConns(ids []string) {
	if len(ids) == 0 {
		return
	}
	m.mu.Lock()
	for _, id := range ids {
		delete(m.conns, id)
	}
	m.mu.Unlock()
}

// batchReadLoop polls the server-written file for new batches.
//
// When no virtual connections are bound to this channel, the loop backs off
// to a longer interval so idle channels don't burn rate-limit quota.
func (m *MuxClient) batchReadLoop(ctx context.Context, ch *upstreamChannel) {
	fetchInterval := ch.fetchInterval
	if fetchInterval <= 0 {
		fetchInterval = m.cfg.GitHub.FetchInterval
	}
	if fetchInterval <= 0 {
		fetchInterval = 200 * time.Millisecond
	}

	timer := time.NewTimer(fetchInterval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			m.doBatchRead(ctx, ch)

			m.mu.RLock()
			active := 0
			for _, vc := range m.conns {
				if vc.channel == ch {
					active++
					if active > 0 {
						break
					}
				}
			}
			m.mu.RUnlock()

			interval := fetchInterval
			if active == 0 {
				interval *= 10
				if interval > 5*time.Second {
					interval = 5 * time.Second
				}
			}
			timer.Reset(interval)
		}
	}
}

func (m *MuxClient) doBatchRead(ctx context.Context, ch *upstreamChannel) {
	if err := m.acquireForToken(ctx, ch.tokenIdx); err != nil {
		slog.Debug("read skipped due to rate limiter", "token_idx", ch.tokenIdx, "error", err)
		return
	}
	batch, err := ch.transport.Read(ctx, ch.channelID, shared.ServerBatchFile)
	m.afterTransportCall(ch.tokenIdx, ch.transport)
	if err != nil {
		slog.Debug("batch read failed", "channel", ch.channelID, "error", err)
		return
	}
	if batch == nil {
		return
	}

	// Epoch-aware dedup. If the server restarted (new epoch), reset our
	// view; otherwise require strictly-increasing Seq.
	lastEpoch := ch.lastReadEpoch.Load()
	lastSeq := ch.lastReadSeq.Load()
	if batch.Epoch != lastEpoch {
		ch.lastReadEpoch.Store(batch.Epoch)
		ch.lastReadSeq.Store(batch.Seq)
		slog.Info("server batch epoch changed; resetting dedup",
			"channel", ch.channelID, "old_epoch", lastEpoch, "new_epoch", batch.Epoch)
	} else if batch.Seq <= lastSeq {
		return
	} else {
		ch.lastReadSeq.Store(batch.Seq)
	}

	m.dispatchFrames(ch, batch.Frames)
}

// dispatchFrames hands each server frame to its VirtualConn.
//
// Locking discipline:
//   - m.mu.RLock is held only for the conn-map lookup. The blocking deliver
//     to vc.recvBuf happens with NO mux-wide lock held, so a slow consumer
//     can't freeze Connect / CloseAll / dispatch on other conns.
//   - We block on vc.recvBuf (rather than spawning a per-frame goroutine
//     under back-pressure) because Go does not guarantee FIFO scheduling
//     between multiple goroutines simultaneously waiting to send on the
//     same channel — using the runtime's blocking-send queue is the only
//     simple way to preserve per-conn frame ordering. With a generous
//     vc.recvBuf capacity and TCP's natural back-pressure on the SOCKS
//     side, blocking should be rare in practice.
func (m *MuxClient) dispatchFrames(ch *upstreamChannel, frames []shared.Frame) {
	for _, f := range frames {
		m.mu.RLock()
		vc, ok := m.conns[f.ConnID]
		m.mu.RUnlock()
		if !ok {
			continue
		}

		switch f.Status {
		case shared.FrameClosed, shared.FrameError:
			if f.Status == shared.FrameError && f.Error != "" {
				slog.Info("server reported error for conn", "conn_id", f.ConnID, "error", f.Error)
			}
			vc.closeOnce.Do(func() { close(vc.closed) })
			// Suppress the redundant client-side FrameClosing that would
			// otherwise be emitted on the next flush — the server has
			// already torn down its destConn.
			vc.closeSent.Store(true)
			continue
		}

		if f.Data == "" {
			continue
		}
		plaintext, err := ch.encryptor.Decrypt(f.Data, f.ConnID, f.Seq)
		if err != nil {
			slog.Warn("decrypt failed", "conn_id", f.ConnID, "seq", f.Seq, "error", err)
			continue
		}
		if len(plaintext) == 0 {
			continue
		}

		select {
		case vc.recvBuf <- plaintext:
		case <-vc.closed:
		}
	}
}

// ── helpers ────────────────────────────────────────────────────────────────

func (m *MuxClient) initTokenChannels(ctx context.Context, tokenIdx int, transport shared.Transport, count int) ([]*upstreamChannel, error) {
	token := m.rateLimiter.GetToken(tokenIdx)
	encryptor := shared.NewEncryptor(shared.EncryptionAlgorithm(m.cfg.Encryption.Algorithm), token)

	var tc TokenConfig
	if tokenIdx < len(m.cfg.GitHub.Tokens) {
		tc = m.cfg.GitHub.Tokens[tokenIdx]
	}
	transportKind := tc.EffectiveTransport()
	batchInterval := tc.EffectiveBatchInterval(m.cfg.GitHub.BatchInterval)
	fetchInterval := tc.EffectiveFetchInterval(m.cfg.GitHub.FetchInterval)
	epoch := randomEpoch()

	channels := make([]*upstreamChannel, 0, count)
	for len(channels) < count {
		if err := m.acquireForToken(ctx, tokenIdx); err != nil {
			return channels, err
		}
		chID, err := transport.EnsureChannel(ctx, "")
		m.afterTransportCall(tokenIdx, transport)
		if err != nil {
			return channels, fmt.Errorf("token %d channel %d: %w", tokenIdx, len(channels), err)
		}
		channels = append(channels, &upstreamChannel{
			channelID:     chID,
			tokenIdx:      tokenIdx,
			transport:     transport,
			encryptor:     encryptor,
			transportKind: transportKind,
			batchInterval: batchInterval,
			fetchInterval: fetchInterval,
			epoch:         epoch,
			flushSig:      make(chan struct{}, 1),
		})
		slog.Info("upstream channel ready",
			"channel_id", chID,
			"token_idx", tokenIdx,
			"transport", transportKind,
			"batch_interval", batchInterval.String(),
			"fetch_interval", fetchInterval.String(),
		)
	}

	return channels, nil
}

func (m *MuxClient) acquireForToken(ctx context.Context, tokenIdx int) error {
	if tokenIdx >= len(m.cfg.GitHub.Tokens) {
		return nil
	}
	if m.cfg.GitHub.Tokens[tokenIdx].EffectiveTransport() != "gist" {
		return nil
	}
	return m.rateLimiter.Acquire(ctx, tokenIdx)
}

func (m *MuxClient) afterTransportCall(tokenIdx int, transport shared.Transport) {
	m.rateLimiter.RecordTransportCall(tokenIdx)

	if tokenIdx >= len(m.cfg.GitHub.Tokens) {
		return
	}
	if m.cfg.GitHub.Tokens[tokenIdx].EffectiveTransport() != "gist" {
		return
	}
	m.rateLimiter.UpdateFromHeaders(tokenIdx, transport.GetRateLimitInfo())
}

// randomEpoch returns a non-zero random int64 for use as a Batch.Epoch tag.
func randomEpoch() int64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// Fall back to nanosecond clock — collision possibility but acceptable.
		return time.Now().UnixNano()
	}
	v := int64(binary.BigEndian.Uint64(b[:]))
	if v == 0 {
		return 1
	}
	return v
}

// Ensure VirtualConn implements io.ReadWriteCloser.
var _ io.ReadWriteCloser = (*VirtualConn)(nil)
