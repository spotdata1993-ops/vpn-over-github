package tests

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sartoopjj/vpn-over-github/server"
	"github.com/sartoopjj/vpn-over-github/shared"
	"github.com/sartoopjj/vpn-over-github/tests/mocks"
)

// makeFakeTarget starts a tcp listener on 127.0.0.1:0 with optional accept
// delay. It returns the address and a chan that receives every byte slice the
// server sees. The listener is closed when ctx is cancelled.
func makeFakeTarget(t *testing.T, ctx context.Context, acceptDelay time.Duration) (string, <-chan []byte, *atomic.Int64) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	received := make(chan []byte, 64)
	var accepts atomic.Int64

	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			accepts.Add(1)
			go func(c net.Conn) {
				if acceptDelay > 0 {
					time.Sleep(acceptDelay)
				}
				defer c.Close()
				buf := make([]byte, 4096)
				for {
					n, err := c.Read(buf)
					if n > 0 {
						out := make([]byte, n)
						copy(out, buf[:n])
						select {
						case received <- out:
						case <-ctx.Done():
							return
						}
					}
					if err != nil {
						return
					}
				}
			}(c)
		}
	}()

	return ln.Addr().String(), received, &accepts
}

func newE2ECfg() *server.ServerConfig {
	cfg := server.DefaultServerConfig()
	cfg.Encryption.Algorithm = "xor"
	cfg.GitHub.BatchInterval = 30 * time.Millisecond
	cfg.GitHub.FetchInterval = 30 * time.Millisecond
	cfg.Proxy.TargetTimeout = 5 * time.Second
	cfg.Proxy.BufferSize = 4096
	cfg.GitHub.Tokens = []server.TokenConfig{{Token: "ghp_test_token", Transport: "git", Repo: "x/y"}}
	return cfg
}

// TestServer_BuffersDataBeforeDial reproduces the original "stuck on telegram"
// bug. A single batch carries the OPEN frame for connA plus two data frames
// for the same conn. The destination accept is delayed 200 ms; data frames
// arrive at the server while the dial is still in flight. Before the fix
// those frames were dropped because handleClientFrame returned with dc==nil.
func TestServer_BuffersDataBeforeDial(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	target, recv, _ := makeFakeTarget(t, ctx, 200*time.Millisecond)

	transport := mocks.NewMockTransport()
	chID, err := transport.EnsureChannel(ctx, "")
	if err != nil {
		t.Fatalf("EnsureChannel: %v", err)
	}

	cfg := newE2ECfg()
	handler := server.NewChannelHandler(cfg, chID, 0, transport, "ghp_test_token")
	handlerCtx, handlerCancel := context.WithCancel(ctx)
	defer handlerCancel()
	go handler.Run(handlerCtx)

	enc := shared.NewEncryptor(shared.AlgorithmXOR, "ghp_test_token")
	p1, _ := enc.Encrypt([]byte("hello "), "connA", 2)
	p2, _ := enc.Encrypt([]byte("world"), "connA", 3)

	batch := &shared.Batch{
		Epoch: 12345,
		Seq:   1,
		Ts:    time.Now().Unix(),
		Frames: []shared.Frame{
			{ConnID: "connA", Seq: 1, Dst: target, Status: shared.FrameActive},
			{ConnID: "connA", Seq: 2, Data: p1, Status: shared.FrameActive},
			{ConnID: "connA", Seq: 3, Data: p2, Status: shared.FrameActive},
		},
	}
	if err := transport.Write(ctx, chID, shared.ClientBatchFile, batch); err != nil {
		t.Fatalf("Write batch: %v", err)
	}

	got := []byte{}
	deadline := time.After(3 * time.Second)
	for len(got) < len("hello world") {
		select {
		case b := <-recv:
			got = append(got, b...)
		case <-deadline:
			t.Fatalf("timed out; received only %q", got)
		}
	}
	if string(got) != "hello world" {
		t.Fatalf("got %q want %q", got, "hello world")
	}
}

// TestServer_HandlesManyParallelConns simulates the telegram load: a single
// batch carries OPEN+data for many independent destinations. Before the fix
// the server dialled them serially AND any data frame sharing the batch
// with an in-flight dial was silently dropped. Verifying that each destination
// receives its specific payload proves both bugs are fixed.
func TestServer_HandlesManyParallelConns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()

	const numConns = 10

	type dest struct {
		addr string
		recv <-chan []byte
	}
	dests := make([]dest, numConns)
	for i := 0; i < numConns; i++ {
		addr, recv, _ := makeFakeTarget(t, ctx, 0)
		dests[i] = dest{addr: addr, recv: recv}
	}

	transport := mocks.NewMockTransport()
	chID, err := transport.EnsureChannel(ctx, "")
	if err != nil {
		t.Fatalf("EnsureChannel: %v", err)
	}

	cfg := newE2ECfg()
	handler := server.NewChannelHandler(cfg, chID, 0, transport, "ghp_test_token")
	handlerCtx, handlerCancel := context.WithCancel(ctx)
	defer handlerCancel()
	go handler.Run(handlerCtx)

	enc := shared.NewEncryptor(shared.AlgorithmXOR, "ghp_test_token")

	frames := make([]shared.Frame, 0, numConns*2)
	want := make(map[string]string, numConns)
	for i, d := range dests {
		connID := connIDFor(i)
		payload := []byte("payload-" + string(rune('0'+i%10)))
		want[connID] = string(payload)
		ct, err := enc.Encrypt(payload, connID, 2)
		if err != nil {
			t.Fatalf("Encrypt: %v", err)
		}
		// OPEN + data, both for the same conn. The server must:
		//   1. spawn a parallel dial for the OPEN
		//   2. NOT drop the data frame even though dial is mid-flight
		//   3. forward the data to the destination once dial completes
		frames = append(frames, shared.Frame{
			ConnID: connID, Seq: 1, Dst: d.addr, Status: shared.FrameActive,
		})
		frames = append(frames, shared.Frame{
			ConnID: connID, Seq: 2, Data: ct, Status: shared.FrameActive,
		})
	}
	batch := &shared.Batch{Epoch: 7, Seq: 1, Ts: time.Now().Unix(), Frames: frames}
	if err := transport.Write(ctx, chID, shared.ClientBatchFile, batch); err != nil {
		t.Fatalf("Write batch: %v", err)
	}

	// Each destination must receive its own payload exactly. Read with a
	// generous deadline; on the old code, ≥1 destination would never receive
	// anything because its data frame was dropped during the dial-serialization.
	deadline := 3 * time.Second
	for i, d := range dests {
		connID := connIDFor(i)
		got := waitFor(t, d.recv, len(want[connID]), deadline)
		if string(got) != want[connID] {
			t.Fatalf("conn %s: got %q want %q", connID, got, want[connID])
		}
	}
}

// TestServer_EpochResetsDedup — a Batch with the same (epoch, seq) as a prior
// one is dropped; a Batch with a new epoch (seq counter reset) is accepted.
func TestServer_EpochResetsDedup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	target, recv, _ := makeFakeTarget(t, ctx, 0)

	transport := mocks.NewMockTransport()
	chID, _ := transport.EnsureChannel(ctx, "")

	cfg := newE2ECfg()
	handler := server.NewChannelHandler(cfg, chID, 0, transport, "ghp_test_token")
	handlerCtx, handlerCancel := context.WithCancel(ctx)
	defer handlerCancel()
	go handler.Run(handlerCtx)

	enc := shared.NewEncryptor(shared.AlgorithmXOR, "ghp_test_token")
	p, _ := enc.Encrypt([]byte("xx"), "connE", 2)

	// Batch 1: epoch=10, seq=1 — opens connE and sends data "xx".
	b1 := &shared.Batch{
		Epoch: 10, Seq: 1, Ts: time.Now().Unix(),
		Frames: []shared.Frame{
			{ConnID: "connE", Seq: 1, Dst: target, Status: shared.FrameActive},
			{ConnID: "connE", Seq: 2, Data: p, Status: shared.FrameActive},
		},
	}
	if err := transport.Write(ctx, chID, shared.ClientBatchFile, b1); err != nil {
		t.Fatalf("write b1: %v", err)
	}

	// Wait for "xx".
	if data := waitFor(t, recv, 2, 1*time.Second); string(data) != "xx" {
		t.Fatalf("expected xx, got %q", data)
	}

	// Batch 2: same epoch+seq → must be dropped.
	if err := transport.Write(ctx, chID, shared.ClientBatchFile, b1); err != nil {
		t.Fatalf("write b1 dup: %v", err)
	}
	select {
	case extra := <-recv:
		t.Fatalf("dedup should have dropped batch; got extra bytes %q", extra)
	case <-time.After(300 * time.Millisecond):
	}

	// Batch 3: new epoch with seq=1 again — must be accepted (writer restart).
	enc2 := shared.NewEncryptor(shared.AlgorithmXOR, "ghp_test_token")
	p2, _ := enc2.Encrypt([]byte("yy"), "connF", 2)
	b3 := &shared.Batch{
		Epoch: 99, Seq: 1, Ts: time.Now().Unix(),
		Frames: []shared.Frame{
			{ConnID: "connF", Seq: 1, Dst: target, Status: shared.FrameActive},
			{ConnID: "connF", Seq: 2, Data: p2, Status: shared.FrameActive},
		},
	}
	if err := transport.Write(ctx, chID, shared.ClientBatchFile, b3); err != nil {
		t.Fatalf("write b3: %v", err)
	}
	if data := waitFor(t, recv, 2, 1*time.Second); string(data) != "yy" {
		t.Fatalf("expected yy after epoch change, got %q", data)
	}
}

// TestServer_DropsDataForUnknownConn — a data frame whose ConnID was never
// announced (and never will be) should be silently dropped. We assert by
// running for a window without a panic / hang.
func TestServer_DropsDataForUnknownConn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	transport := mocks.NewMockTransport()
	chID, _ := transport.EnsureChannel(ctx, "")

	cfg := newE2ECfg()
	handler := server.NewChannelHandler(cfg, chID, 0, transport, "ghp_test_token")
	handlerCtx, handlerCancel := context.WithCancel(ctx)
	defer handlerCancel()
	go handler.Run(handlerCtx)

	// Send a batch containing only orphan data (no Dst).
	b := &shared.Batch{
		Epoch: 1, Seq: 1, Ts: time.Now().Unix(),
		Frames: []shared.Frame{
			{ConnID: "ghost", Seq: 7, Data: "AAAA", Status: shared.FrameActive},
		},
	}
	if err := transport.Write(ctx, chID, shared.ClientBatchFile, b); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Just ensure the handler doesn't hang or crash within the deadline.
	time.Sleep(150 * time.Millisecond)
	// Ctx expires implicitly via defer cancel — test passes if we got here.
}

// connIDFor returns a deterministic conn id for the i-th conn in a test.
func connIDFor(i int) string {
	return "conn_" + string(rune('A'+i))
}

// waitFor accumulates incoming bytes until at least minLen are received.
func waitFor(t *testing.T, recv <-chan []byte, minLen int, timeout time.Duration) []byte {
	t.Helper()
	deadline := time.After(timeout)
	got := []byte{}
	for len(got) < minLen {
		select {
		case b := <-recv:
			got = append(got, b...)
		case <-deadline:
			t.Fatalf("timeout waiting for %d bytes; got %q", minLen, got)
		}
	}
	return got
}
