package server

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sartoopjj/vpn-over-github/shared"
)

// destConn tracks one upstream destination tunnelled through this channel.
//
// A destConn is created the moment the server first sees the client's OPEN
// frame. The actual TCP dial then runs in a goroutine — meanwhile any further
// client frames for this conn are buffered in earlyData. Buffering is
// essential: a single batch may carry the OPEN of conn A and 8 data frames
// of conn B that's currently mid-dial; without buffering those B-frames
// would be silently dropped (the original "stuck on telegram" bug).
type destConn struct {
	connID    string
	dst       string
	conn      net.Conn
	seq       atomic.Int64
	closed    chan struct{}
	closeOnce sync.Once

	mu        sync.Mutex
	pending   []shared.Frame  // outgoing frames (server → client)
	earlyData []shared.Frame  // incoming frames buffered while dial in flight
	dialed    bool            // dial complete (success or failure)
	dialErr   error
}

func (d *destConn) enqueue(f shared.Frame) {
	d.mu.Lock()
	d.pending = append(d.pending, f)
	d.mu.Unlock()
}

func (d *destConn) drain() []shared.Frame {
	d.mu.Lock()
	out := d.pending
	d.pending = nil
	d.mu.Unlock()
	return out
}

func (d *destConn) close() {
	d.closeOnce.Do(func() {
		close(d.closed)
		if d.conn != nil {
			_ = d.conn.Close()
		}
	})
}

// ChannelHandler runs both directions of a single upstream channel.
//
// readClientLoop polls client.json for new batches and dispatches each
// frame to the matching destConn. writeServerLoop drains pending frames
// from each destConn and writes a server.json batch.
type ChannelHandler struct {
	cfg       *ServerConfig
	channelID string
	tokenIdx  int
	transport shared.Transport
	encryptor *shared.Encryptor

	transportKind string
	batchInterval time.Duration
	fetchInterval time.Duration

	// epoch is randomized per handler run; clients reset their dedup state
	// when they observe an epoch change (e.g., server restart).
	epoch int64

	serverBatchSeq atomic.Int64

	// dedup state for client → server batches.
	lastClientEpoch atomic.Int64
	lastClientSeq   atomic.Int64
	hadFirstRead    atomic.Bool

	mu    sync.RWMutex
	conns map[string]*destConn

	// flushSig wakes the writer; same pattern as the client side.
	flushSig chan struct{}
}

func NewChannelHandler(cfg *ServerConfig, channelID string, tokenIdx int, transport shared.Transport, token string) *ChannelHandler {
	transportKind := "git"
	batchInterval := cfg.GitHub.BatchInterval
	fetchInterval := cfg.GitHub.FetchInterval
	if tokenIdx < len(cfg.GitHub.Tokens) {
		tc := cfg.GitHub.Tokens[tokenIdx]
		transportKind = tc.EffectiveTransport()
		batchInterval = tc.EffectiveBatchInterval(batchInterval)
		fetchInterval = tc.EffectiveFetchInterval(fetchInterval)
	}
	return &ChannelHandler{
		cfg:           cfg,
		channelID:     channelID,
		tokenIdx:      tokenIdx,
		transport:     transport,
		encryptor:     shared.NewEncryptor(shared.EncryptionAlgorithm(cfg.Encryption.Algorithm), token),
		transportKind: transportKind,
		batchInterval: batchInterval,
		fetchInterval: fetchInterval,
		epoch:         randomEpoch(),
		conns:         make(map[string]*destConn),
		flushSig:      make(chan struct{}, 1),
	}
}

func (h *ChannelHandler) Run(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer cancel()
		h.readClientLoop(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		h.writeServerLoop(ctx)
	}()

	wg.Wait()
	h.closeAll()
}

// signalFlush wakes writeServerLoop without blocking. Called whenever a
// destConn enqueues a new outbound frame.
func (h *ChannelHandler) signalFlush() {
	select {
	case h.flushSig <- struct{}{}:
	default:
	}
}

// ── client → destination ────────────────────────────────────────────────

func (h *ChannelHandler) readClientLoop(ctx context.Context) {
	timer := time.NewTimer(h.fetchInterval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if !h.doReadClient(ctx) {
				return
			}

			h.mu.RLock()
			active := len(h.conns)
			h.mu.RUnlock()

			interval := h.fetchInterval
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

func (h *ChannelHandler) doReadClient(ctx context.Context) bool {
	batch, err := h.transport.Read(ctx, h.channelID, shared.ClientBatchFile)
	if err != nil {
		if errors.Is(err, shared.ErrNotFound) {
			// Channel may exist while the client batch file is not yet visible
			// (race during EnsureChannel on the client side). Keep handler alive.
			slog.Debug("client batch not found yet", "channel_id", h.channelID)
			return true
		}
		slog.Debug("read client batch failed", "channel_id", h.channelID, "error", err)
		return true
	}
	if batch == nil {
		return true
	}

	// Epoch-aware dedup. On epoch change, reset our view; on stale batch, drop.
	lastEpoch := h.lastClientEpoch.Load()
	lastSeq := h.lastClientSeq.Load()
	if batch.Epoch != lastEpoch {
		h.lastClientEpoch.Store(batch.Epoch)
		h.lastClientSeq.Store(batch.Seq)
		if h.hadFirstRead.Load() {
			slog.Info("client batch epoch changed; resetting dedup",
				"channel_id", h.channelID, "old_epoch", lastEpoch, "new_epoch", batch.Epoch)
		}
	} else if batch.Seq <= lastSeq {
		return true
	} else {
		h.lastClientSeq.Store(batch.Seq)
	}

	// On the very first read, drop batches that are clearly stale (e.g. left
	// behind from a previous session). With epoch this is mostly defensive.
	if !h.hadFirstRead.Load() {
		h.hadFirstRead.Store(true)
		if batch.Age() > 60*time.Second {
			slog.Debug("skipping stale client batch on startup",
				"channel_id", h.channelID, "age", batch.Age().Round(time.Second))
			return true
		}
	}

	for _, f := range batch.Frames {
		h.handleClientFrame(ctx, f)
	}
	return true
}

// handleClientFrame routes one client frame. New conns spawn an async dial
// and buffer subsequent frames until the dial completes.
func (h *ChannelHandler) handleClientFrame(ctx context.Context, f shared.Frame) {
	h.mu.RLock()
	dc, ok := h.conns[f.ConnID]
	h.mu.RUnlock()

	if !ok {
		// Unknown conn. We can only register it if this frame announces a Dst.
		// A bare data frame for an unknown conn is a protocol violation —
		// log at debug and drop.
		if f.Dst == "" {
			slog.Debug("data frame for unknown conn", "channel_id", h.channelID, "conn_id", f.ConnID)
			return
		}

		newDC := &destConn{
			connID: f.ConnID,
			dst:    f.Dst,
			closed: make(chan struct{}),
		}

		h.mu.Lock()
		if existing, exists := h.conns[f.ConnID]; exists {
			// Lost the race; another caller already registered this conn.
			dc = existing
			h.mu.Unlock()
		} else {
			h.conns[f.ConnID] = newDC
			dc = newDC
			h.mu.Unlock()
			go h.dialAsync(ctx, newDC)
		}
	}

	// Buffer or process. We hold dc.mu for the decision so dialAsync can't
	// race past us with a "dial complete" while we append to earlyData.
	// We also capture dc.dialErr while holding the lock — reading those
	// fields without synchronization is a data race even if the value is
	// only written once during dialAsync.
	dc.mu.Lock()
	dialed := dc.dialed
	dialErr := dc.dialErr
	if !dialed {
		dc.earlyData = append(dc.earlyData, f)
		dc.mu.Unlock()
		return
	}
	dc.mu.Unlock()

	if dialErr != nil {
		// Dial already failed and we already pushed an Error frame to the
		// client. Quietly drop subsequent frames for this conn.
		return
	}

	h.processClientFrame(dc, f)
}

// dialAsync dials dc.dst, then either replays buffered frames (success) or
// emits a single FrameError (failure) and closes the conn.
//
// The "dialed" flag is the gate that handleClientFrame uses to decide
// between buffering and direct processing. We must not set dialed=true
// until the earlyData replay has fully drained — otherwise a new frame
// arriving in the middle of replay would race the in-progress replay
// (concurrent dc.conn.Write from two goroutines, plus out-of-order
// delivery to the destination).
//
// The drain loop releases dc.mu around the actual I/O (processClientFrame
// can do TCP writes that may block) and re-acquires it to check whether
// new frames slipped into earlyData. Only when an iteration finds an
// empty earlyData under lock do we set dialed=true and exit.
func (h *ChannelHandler) dialAsync(ctx context.Context, dc *destConn) {
	conn, err := h.dialDestination(ctx, dc.dst)

	if err != nil {
		dc.mu.Lock()
		dc.dialed = true
		dc.dialErr = err
		dc.earlyData = nil // any buffered data dies with the conn
		dc.mu.Unlock()

		slog.Warn("dial failed",
			"channel_id", h.channelID,
			"conn_id", dc.connID,
			"dst", dc.dst,
			"error", err,
		)
		dc.enqueue(shared.Frame{
			ConnID: dc.connID,
			Seq:    dc.seq.Add(1),
			Status: shared.FrameError,
			Error:  err.Error(),
		})
		dc.close()
		h.signalFlush()
		return
	}

	dc.mu.Lock()
	dc.conn = conn
	dc.mu.Unlock()

	slog.Info("server opened destination",
		"channel_id", h.channelID,
		"conn_id", dc.connID,
		"dst", dc.dst,
	)
	go h.readDestLoop(ctx, dc)

	// Drain replay loop. handleClientFrame keeps appending to earlyData
	// while dialed==false, so new arrivals join the back of the queue
	// and are replayed in order alongside their predecessors.
	dc.mu.Lock()
	for {
		early := dc.earlyData
		dc.earlyData = nil
		if len(early) == 0 {
			dc.dialed = true
			dc.mu.Unlock()
			return
		}
		dc.mu.Unlock()
		for _, f := range early {
			h.processClientFrame(dc, f)
		}
		dc.mu.Lock()
	}
}

// processClientFrame applies one frame to an already-dialed destConn.
func (h *ChannelHandler) processClientFrame(dc *destConn, f shared.Frame) {
	switch f.Status {
	case shared.FrameClosing, shared.FrameClosed, shared.FrameError:
		dc.close()
		dc.enqueue(shared.Frame{
			ConnID: dc.connID,
			Seq:    dc.seq.Add(1),
			Status: shared.FrameClosed,
		})
		h.signalFlush()
		return
	}

	if f.Data == "" {
		return
	}

	plaintext, err := h.encryptor.Decrypt(f.Data, f.ConnID, f.Seq)
	if err != nil {
		dc.enqueue(shared.Frame{
			ConnID: dc.connID,
			Seq:    dc.seq.Add(1),
			Status: shared.FrameError,
			Error:  "decrypt failed",
		})
		dc.close()
		h.signalFlush()
		return
	}
	if len(plaintext) == 0 {
		return
	}

	_ = dc.conn.SetWriteDeadline(time.Now().Add(h.cfg.Proxy.TargetTimeout))
	if _, err := dc.conn.Write(plaintext); err != nil {
		dc.enqueue(shared.Frame{
			ConnID: dc.connID,
			Seq:    dc.seq.Add(1),
			Status: shared.FrameError,
			Error:  "destination write failed",
		})
		dc.close()
		h.signalFlush()
	}
}

// readDestLoop reads from the upstream destination and enqueues encrypted
// data frames to the writer side.
func (h *ChannelHandler) readDestLoop(ctx context.Context, dc *destConn) {
	buf := make([]byte, h.cfg.Proxy.BufferSize)
	for {
		select {
		case <-ctx.Done():
			return
		case <-dc.closed:
			return
		default:
		}
		_ = dc.conn.SetReadDeadline(time.Now().Add(h.cfg.Proxy.TargetTimeout))
		n, err := dc.conn.Read(buf)
		if n > 0 {
			payload := make([]byte, n)
			copy(payload, buf[:n])
			seq := dc.seq.Add(1)
			enc, encErr := h.encryptor.Encrypt(payload, dc.connID, seq)
			if encErr != nil {
				dc.enqueue(shared.Frame{
					ConnID: dc.connID,
					Seq:    dc.seq.Add(1),
					Status: shared.FrameError,
					Error:  "encrypt failed",
				})
			} else {
				dc.enqueue(shared.Frame{
					ConnID: dc.connID,
					Seq:    seq,
					Data:   enc,
					Status: shared.FrameActive,
				})
			}
			h.signalFlush()
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				dc.enqueue(shared.Frame{
					ConnID: dc.connID,
					Seq:    dc.seq.Add(1),
					Status: shared.FrameClosed,
				})
				dc.close()
				h.signalFlush()
				return
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				// Idle read timeout is expected; keep the destination connection open.
				continue
			}
			dc.enqueue(shared.Frame{
				ConnID: dc.connID,
				Seq:    dc.seq.Add(1),
				Status: shared.FrameError,
				Error:  "destination read failed",
			})
			dc.close()
			h.signalFlush()
			return
		}
	}
}

// ── destination → client ────────────────────────────────────────────────

func (h *ChannelHandler) writeServerLoop(ctx context.Context) {
	batchInterval := h.batchInterval
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
			h.flushServerFrames(ctx)
			nextDeadline = time.Now().Add(batchInterval)
			timer.Reset(batchInterval)
		case <-h.flushSig:
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

func (h *ChannelHandler) flushServerFrames(ctx context.Context) {
	h.mu.RLock()
	conns := make([]*destConn, 0, len(h.conns))
	for _, dc := range h.conns {
		conns = append(conns, dc)
	}
	h.mu.RUnlock()

	frames := make([]shared.Frame, 0)
	var toGC []string

	for _, dc := range conns {
		drained := dc.drain()
		frames = append(frames, drained...)

		select {
		case <-dc.closed:
			dc.mu.Lock()
			pendingEmpty := len(dc.pending) == 0
			dc.mu.Unlock()
			if pendingEmpty {
				toGC = append(toGC, dc.connID)
			}
		default:
		}
	}

	if len(frames) == 0 {
		h.gcConns(toGC)
		return
	}

	seq := h.serverBatchSeq.Add(1)
	batch := &shared.Batch{
		Epoch:  h.epoch,
		Seq:    seq,
		Ts:     time.Now().Unix(),
		Frames: frames,
	}
	if err := h.transport.Write(ctx, h.channelID, shared.ServerBatchFile, batch); err != nil {
		slog.Debug("write server batch failed", "channel_id", h.channelID, "error", err)
		return
	}

	h.gcConns(toGC)
}

// gcConns removes conn IDs whose final close frame has been flushed.
func (h *ChannelHandler) gcConns(ids []string) {
	if len(ids) == 0 {
		return
	}
	h.mu.Lock()
	for _, id := range ids {
		// Re-check that the conn still has no pending frames before removing —
		// a write-loss race could mean readDestLoop appended after our drain.
		if dc, ok := h.conns[id]; ok {
			dc.mu.Lock()
			pendingEmpty := len(dc.pending) == 0
			dc.mu.Unlock()
			if pendingEmpty {
				delete(h.conns, id)
			}
		}
	}
	h.mu.Unlock()
}

func (h *ChannelHandler) dialDestination(ctx context.Context, dst string) (net.Conn, error) {
	dialCtx, cancel := context.WithTimeout(ctx, h.cfg.Proxy.TargetTimeout)
	defer cancel()
	return (&net.Dialer{}).DialContext(dialCtx, "tcp", dst)
}

func (h *ChannelHandler) closeAll() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for connID, dc := range h.conns {
		dc.close()
		delete(h.conns, connID)
	}
}

// randomEpoch returns a non-zero random int64 used as Batch.Epoch.
func randomEpoch() int64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return time.Now().UnixNano()
	}
	v := int64(binary.BigEndian.Uint64(b[:]))
	if v == 0 {
		return 1
	}
	return v
}
