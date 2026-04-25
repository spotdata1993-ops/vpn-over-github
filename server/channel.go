package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sartoopjj/vpn-over-github/shared"
)

type destConn struct {
	connID    string
	dst       string
	conn      net.Conn
	seq       atomic.Int64
	closed    chan struct{}
	closeOnce sync.Once

	mu      sync.Mutex
	pending []shared.Frame
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

type ChannelHandler struct {
	cfg       *ServerConfig
	channelID string
	tokenIdx  int
	transport shared.Transport
	encryptor *shared.Encryptor

	serverBatchSeq  atomic.Int64
	lastClientBatch atomic.Int64
	lastClientTs    atomic.Int64

	mu    sync.RWMutex
	conns map[string]*destConn
}

func NewChannelHandler(cfg *ServerConfig, channelID string, tokenIdx int, transport shared.Transport, token string) *ChannelHandler {
	return &ChannelHandler{
		cfg:       cfg,
		channelID: channelID,
		tokenIdx:  tokenIdx,
		transport: transport,
		encryptor: shared.NewEncryptor(shared.EncryptionAlgorithm(cfg.Encryption.Algorithm), token),
		conns:     make(map[string]*destConn),
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

func (h *ChannelHandler) readClientLoop(ctx context.Context) {
	timer := time.NewTimer(h.cfg.GitHub.FetchInterval)
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

			interval := h.cfg.GitHub.FetchInterval
			if active == 0 {
				interval *= 10
				if interval > 10*time.Second {
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
			// The channel may exist while the client batch file is not yet visible.
			// Keep the handler alive; listener reconciliation stops truly deleted channels.
			slog.Debug("client batch not found yet", "channel_id", h.channelID)
			return true
		}
		slog.Debug("read client batch failed", "channel_id", h.channelID, "error", err)
		return true
	}
	if batch == nil {
		return true
	}
	lastSeq := h.lastClientBatch.Load()
	lastTs := h.lastClientTs.Load()
	if batch.Seq <= lastSeq && batch.Ts <= lastTs {
		return true
	}
	if batch.Seq <= lastSeq && batch.Ts > lastTs {
		slog.Info("detected client batch sequence reset", "channel_id", h.channelID, "prev_seq", lastSeq, "new_seq", batch.Seq)
	}
	isFirstRead := (lastSeq == 0 && lastTs == 0)
	h.lastClientBatch.Store(batch.Seq)
	h.lastClientTs.Store(batch.Ts)

	if isFirstRead && batch.Age() > 60*time.Second {
		slog.Debug("skipping stale client batch on startup", "channel_id", h.channelID, "age", batch.Age().Round(time.Second))
		return true
	}

	for _, f := range batch.Frames {
		h.handleClientFrame(ctx, f)
	}
	return true
}

func (h *ChannelHandler) handleClientFrame(ctx context.Context, f shared.Frame) {
	h.mu.RLock()
	dc, ok := h.conns[f.ConnID]
	h.mu.RUnlock()

	if !ok && f.Dst != "" {
		conn, err := h.dialDestination(ctx, f.Dst)
		if err != nil {
			h.writeImmediateFrame(ctx, shared.Frame{
				ConnID: f.ConnID,
				Seq:    1,
				Status: shared.FrameError,
				Error:  err.Error(),
			})
			return
		}
		dc = &destConn{
			connID: f.ConnID,
			dst:    f.Dst,
			conn:   conn,
			closed: make(chan struct{}),
		}
		h.mu.Lock()
		h.conns[f.ConnID] = dc
		h.mu.Unlock()
		go h.readDestLoop(ctx, dc)
		slog.Info("server opened destination", "channel_id", h.channelID, "conn_id", f.ConnID, "dst", f.Dst)
	}

	if dc == nil {
		return
	}

	switch f.Status {
	case shared.FrameClosing, shared.FrameClosed, shared.FrameError:
		dc.closeOnce.Do(func() {
			close(dc.closed)
			_ = dc.conn.Close()
		})
		dc.enqueue(shared.Frame{ConnID: dc.connID, Seq: dc.seq.Add(1), Status: shared.FrameClosed})
		return
	}

	if f.Data == "" {
		return
	}
	plaintext, err := h.encryptor.Decrypt(f.Data, f.ConnID, f.Seq)
	if err != nil {
		dc.enqueue(shared.Frame{ConnID: dc.connID, Seq: dc.seq.Add(1), Status: shared.FrameError, Error: "decrypt failed"})
		return
	}
	if len(plaintext) == 0 {
		return
	}
	_ = dc.conn.SetWriteDeadline(time.Now().Add(h.cfg.Proxy.TargetTimeout))
	if _, err := dc.conn.Write(plaintext); err != nil {
		dc.enqueue(shared.Frame{ConnID: dc.connID, Seq: dc.seq.Add(1), Status: shared.FrameError, Error: "destination write failed"})
		dc.closeOnce.Do(func() {
			close(dc.closed)
			_ = dc.conn.Close()
		})
	}
}

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
				dc.enqueue(shared.Frame{ConnID: dc.connID, Seq: dc.seq.Add(1), Status: shared.FrameError, Error: "encrypt failed"})
			} else {
				dc.enqueue(shared.Frame{ConnID: dc.connID, Seq: seq, Data: enc, Status: shared.FrameActive})
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				dc.enqueue(shared.Frame{ConnID: dc.connID, Seq: dc.seq.Add(1), Status: shared.FrameClosed})
				dc.closeOnce.Do(func() {
					close(dc.closed)
					_ = dc.conn.Close()
				})
				return
			}

			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				// Idle read timeout is expected; keep the destination connection open.
				continue
			}

			dc.enqueue(shared.Frame{ConnID: dc.connID, Seq: dc.seq.Add(1), Status: shared.FrameError, Error: "destination read failed"})
			dc.closeOnce.Do(func() {
				close(dc.closed)
				_ = dc.conn.Close()
			})
			return
		}
	}
}

func (h *ChannelHandler) writeServerLoop(ctx context.Context) {
	ticker := time.NewTicker(h.cfg.GitHub.BatchInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.flushServerFrames(ctx)
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
	for _, dc := range conns {
		frames = append(frames, dc.drain()...)
	}
	if len(frames) == 0 {
		return
	}

	seq := h.serverBatchSeq.Add(1)
	batch := &shared.Batch{Seq: seq, Ts: time.Now().Unix(), Frames: frames}
	if err := h.transport.Write(ctx, h.channelID, shared.ServerBatchFile, batch); err != nil {
		slog.Debug("write server batch failed", "channel_id", h.channelID, "error", err)
	}
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
		dc.closeOnce.Do(func() {
			close(dc.closed)
			_ = dc.conn.Close()
		})
		delete(h.conns, connID)
	}
}

func (h *ChannelHandler) queueFrame(f shared.Frame) {
	h.mu.RLock()
	dc, ok := h.conns[f.ConnID]
	h.mu.RUnlock()
	if ok {
		dc.enqueue(f)
	}
}

func (h *ChannelHandler) nextConnSeq(dc *destConn) int64 {
	if dc == nil {
		return 1
	}
	return dc.seq.Add(1)
}

func (h *ChannelHandler) writeImmediateFrame(ctx context.Context, f shared.Frame) {
	seq := h.serverBatchSeq.Add(1)
	batch := &shared.Batch{Seq: seq, Ts: time.Now().Unix(), Frames: []shared.Frame{f}}
	if err := h.transport.Write(ctx, h.channelID, shared.ServerBatchFile, batch); err != nil {
		slog.Debug("write immediate frame failed", "channel_id", h.channelID, "error", err)
	}
}
