package tests

import (
	"context"
	"testing"
	"time"

	"github.com/sartoopjj/vpn-over-github/client"
	"github.com/sartoopjj/vpn-over-github/shared"
	"github.com/sartoopjj/vpn-over-github/tests/mocks"
)

func newClientCfg() *client.Config {
	cfg := client.DefaultConfig()
	cfg.Encryption.Algorithm = "xor"
	cfg.GitHub.BatchInterval = 50 * time.Millisecond
	cfg.GitHub.FetchInterval = 50 * time.Millisecond
	cfg.GitHub.UpstreamConnections = 1
	cfg.SOCKS.Listen = "127.0.0.1:0"
	cfg.GitHub.Tokens = []client.TokenConfig{{
		Token:     "ghp_test_token",
		Transport: "git",
		Repo:      "x/y",
	}}
	return cfg
}

func newMux(t *testing.T, ctx context.Context) (*client.MuxClient, *mocks.MockTransport) {
	t.Helper()
	cfg := newClientCfg()
	rl := client.NewRateLimiter([]string{"ghp_test_token"}, cfg)
	transport := mocks.NewMockTransport()
	mux, err := client.NewMuxClient(ctx, cfg, rl, map[int]shared.Transport{0: transport})
	if err != nil {
		t.Fatalf("NewMuxClient: %v", err)
	}
	return mux, transport
}

// readClientBatch polls the mock transport's client.json for any non-empty
// batch up to deadline.
func readClientBatch(t *testing.T, ctx context.Context, transport *mocks.MockTransport, chID string, deadline time.Duration) *shared.Batch {
	t.Helper()
	until := time.Now().Add(deadline)
	for time.Now().Before(until) {
		b, _ := transport.Read(ctx, chID, shared.ClientBatchFile)
		if b != nil && len(b.Frames) > 0 {
			return b
		}
		time.Sleep(5 * time.Millisecond)
	}
	return nil
}

// TestClient_CoalescesOpenWithFirstWrite — a Connect followed immediately by
// a Write should produce ONE frame carrying both Dst (OPEN) and Data,
// saving a round-trip versus emitting OPEN + data as two separate frames.
func TestClient_CoalescesOpenWithFirstWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	mux, transport := newMux(t, ctx)
	defer mux.CloseAll(context.Background())

	vc, err := mux.Connect(ctx, "10.10.10.10:9999")
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	// Write immediately so the OPEN and the first chunk are flushed together.
	if _, err := vc.Write([]byte("hello")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	b := readClientBatch(t, ctx, transport, "ch-1", 1*time.Second)
	if b == nil {
		t.Fatal("no client batch written")
	}
	if len(b.Frames) != 1 {
		t.Fatalf("expected 1 coalesced frame, got %d: %+v", len(b.Frames), b.Frames)
	}
	if b.Epoch == 0 {
		t.Fatal("expected non-zero epoch")
	}
	f := b.Frames[0]
	if f.Dst != "10.10.10.10:9999" {
		t.Fatalf("frame missing Dst: %+v", f)
	}
	if f.Data == "" {
		t.Fatalf("frame missing Data: %+v", f)
	}
	enc := shared.NewEncryptor(shared.AlgorithmXOR, "ghp_test_token")
	plain, err := enc.Decrypt(f.Data, f.ConnID, f.Seq)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if string(plain) != "hello" {
		t.Fatalf("decrypted %q want %q", plain, "hello")
	}
}

// TestClient_EmitsBareOpenWhenNoData — Connect with no Write should still
// emit a single OPEN frame so the server can dial and (e.g.) read a banner.
func TestClient_EmitsBareOpenWhenNoData(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	mux, transport := newMux(t, ctx)
	defer mux.CloseAll(context.Background())

	if _, err := mux.Connect(ctx, "10.10.10.10:9999"); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	b := readClientBatch(t, ctx, transport, "ch-1", 1*time.Second)
	if b == nil {
		t.Fatal("no client batch written")
	}
	if len(b.Frames) != 1 {
		t.Fatalf("expected 1 OPEN frame, got %d: %+v", len(b.Frames), b.Frames)
	}
	f := b.Frames[0]
	if f.Dst != "10.10.10.10:9999" {
		t.Fatalf("OPEN missing Dst: %+v", f)
	}
	if f.Data != "" {
		t.Fatalf("bare OPEN should have empty Data, got %q", f.Data)
	}
	if f.Status != shared.FrameActive {
		t.Fatalf("expected FrameActive, got %v", f.Status)
	}
}

// TestClient_FlushSignalBeatsTicker — Connect/Write are signalling the writer
// loop. The frame must appear well before the full BatchInterval. We use a
// 200ms BatchInterval and demand a frame within 80ms.
func TestClient_FlushSignalBeatsTicker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	cfg := newClientCfg()
	cfg.GitHub.BatchInterval = 200 * time.Millisecond
	cfg.GitHub.FetchInterval = 200 * time.Millisecond
	rl := client.NewRateLimiter([]string{"ghp_test_token"}, cfg)
	transport := mocks.NewMockTransport()
	mux, err := client.NewMuxClient(ctx, cfg, rl, map[int]shared.Transport{0: transport})
	if err != nil {
		t.Fatalf("NewMuxClient: %v", err)
	}
	defer mux.CloseAll(context.Background())

	vc, err := mux.Connect(ctx, "10.10.10.10:9999")
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	start := time.Now()
	if _, err := vc.Write([]byte("ping")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	b := readClientBatch(t, ctx, transport, "ch-1", 500*time.Millisecond)
	elapsed := time.Since(start)
	if b == nil {
		t.Fatal("no batch written")
	}
	// fastFlushGap = batchInterval / 4 = 50ms. Allow generous slack for CI.
	if elapsed > 150*time.Millisecond {
		t.Fatalf("flush took %v — flush signal isn't shortening the timer", elapsed)
	}
}

// TestClient_GarbageCollectsClosedConn — a closed conn should not linger in
// the conn map. Snapshot exposes this externally.
func TestClient_GarbageCollectsClosedConn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	mux, _ := newMux(t, ctx)
	defer mux.CloseAll(context.Background())

	vc, err := mux.Connect(ctx, "10.10.10.10:9999")
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if got := len(mux.Snapshot()); got != 1 {
		t.Fatalf("expected 1 active conn, got %d", got)
	}

	_ = vc.Close()

	until := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(until) {
		if len(mux.Snapshot()) == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("conn was not GC'd after Close; still %d active", len(mux.Snapshot()))
}

// TestClient_DoesNotEmitOpenForOpenedThenClosedConn — Connect followed by
// Close before the first flush should drop the conn entirely without sending
// any frame: server never knew about it, no cleanup needed.
func TestClient_DoesNotEmitOpenForOpenedThenClosedConn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	mux, transport := newMux(t, ctx)
	defer mux.CloseAll(context.Background())

	vc, err := mux.Connect(ctx, "10.10.10.10:9999")
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	_ = vc.Close()

	// Wait long enough for two flush cycles.
	time.Sleep(150 * time.Millisecond)

	b, _ := transport.Read(ctx, "ch-1", shared.ClientBatchFile)
	if b != nil && len(b.Frames) > 0 {
		t.Fatalf("unexpected frame for never-opened conn: %+v", b.Frames)
	}
}
