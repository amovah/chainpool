package gateway

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/amovah/chainpool"
)

func TestServeBindErrorReturned(t *testing.T) {
	m := newTestManager(t, []chainpool.NodeConfig{{Name: "a", BaseURL: "http://127.0.0.1:1", Priority: 100, RateRPS: 1000}}, nil)
	// Occupy a port, then ask Serve to bind the same one.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	err = Serve(context.Background(), m, ln.Addr().String())
	if err == nil {
		t.Fatal("expected bind error, got nil")
	}
}

func TestServeGracefulShutdownOnContextCancel(t *testing.T) {
	up := rpcServer(t, `{"jsonrpc":"2.0","id":1,"result":"0x1"}`)
	defer up.Close()
	m := newTestManager(t, []chainpool.NodeConfig{{Name: "a", BaseURL: up.URL, Priority: 100, RateRPS: 1000}}, nil)

	// Pick a free port deterministically.
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := probe.Addr().String()
	probe.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, m, addr) }()

	// Wait until the gateway accepts connections.
	var ready bool
	for i := 0; i < 100; i++ {
		resp, err := http.Post("http://"+addr+"/ethereum", "application/json", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"x"}`))
		if err == nil {
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if string(b) == `{"jsonrpc":"2.0","id":1,"result":"0x1"}` {
				ready = true
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !ready {
		t.Fatal("gateway never became ready")
	}

	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("Serve returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after context cancel")
	}
}
