package gateway

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/amovah/chainpool"
)

// rpcServer returns an httptest server replying with a fixed JSON-RPC body.
func rpcServer(t *testing.T, body string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
}

func newTestManager(t *testing.T, nodes []chainpool.NodeConfig, fallback []int) *chainpool.Manager {
	t.Helper()
	m, err := chainpool.New(chainpool.Config{
		Defaults: chainpool.Defaults{
			RateRPS:          1000,
			Timeout:          chainpool.Duration(200 * time.Millisecond),
			FallbackRPCCodes: fallback,
		},
		Chains: map[string]chainpool.ChainConfig{
			"ethereum": {
				Nodes:   nodes,
				Timeout: chainpool.Duration(200 * time.Millisecond),
				Backoff: chainpool.BackoffConfig{Initial: chainpool.Duration(time.Millisecond), Max: chainpool.Duration(time.Millisecond), Factor: 2},
			},
		},
	})
	if err != nil {
		t.Fatalf("chainpool.New: %v", err)
	}
	return m
}

func post(t *testing.T, srv *httptest.Server, path, body string) (int, string) {
	t.Helper()
	resp, err := http.Post(srv.URL+path, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestHandlerRoutesByChainAndReturnsBody(t *testing.T) {
	up := rpcServer(t, `{"jsonrpc":"2.0","id":1,"result":"0x1"}`)
	defer up.Close()
	m := newTestManager(t, []chainpool.NodeConfig{{Name: "a", BaseURL: up.URL, Priority: 100, RateRPS: 1000}}, nil)

	gw := httptest.NewServer(newHandler(m, defaultSettings()))
	defer gw.Close()

	code, body := post(t, gw, "/ethereum", `{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`)
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200", code)
	}
	if body != `{"jsonrpc":"2.0","id":1,"result":"0x1"}` {
		t.Fatalf("body = %s", body)
	}
}

func TestHandlerUnknownChain404(t *testing.T) {
	up := rpcServer(t, `{}`)
	defer up.Close()
	m := newTestManager(t, []chainpool.NodeConfig{{Name: "a", BaseURL: up.URL, Priority: 100, RateRPS: 1000}}, nil)

	gw := httptest.NewServer(newHandler(m, defaultSettings()))
	defer gw.Close()

	code, _ := post(t, gw, "/dogecoin", `{"jsonrpc":"2.0","id":1,"method":"x"}`)
	if code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", code)
	}
}

func TestHandlerNonPost405(t *testing.T) {
	up := rpcServer(t, `{}`)
	defer up.Close()
	m := newTestManager(t, []chainpool.NodeConfig{{Name: "a", BaseURL: up.URL, Priority: 100, RateRPS: 1000}}, nil)

	gw := httptest.NewServer(newHandler(m, defaultSettings()))
	defer gw.Close()

	resp, err := http.Get(gw.URL + "/ethereum")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", resp.StatusCode)
	}
}

func TestHandlerFailoverAcrossNodes(t *testing.T) {
	bad := rpcServer(t, `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"busy"}}`)
	defer bad.Close()
	good := rpcServer(t, `{"jsonrpc":"2.0","id":1,"result":"0x7"}`)
	defer good.Close()
	m := newTestManager(t, []chainpool.NodeConfig{
		{Name: "bad", BaseURL: bad.URL, Priority: 100, RateRPS: 1000},
		{Name: "good", BaseURL: good.URL, Priority: 50, RateRPS: 1000},
	}, []int{-32000})

	gw := httptest.NewServer(newHandler(m, defaultSettings()))
	defer gw.Close()

	code, body := post(t, gw, "/ethereum", `{"jsonrpc":"2.0","id":1,"method":"x"}`)
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200", code)
	}
	if body != `{"jsonrpc":"2.0","id":1,"result":"0x7"}` {
		t.Fatalf("body = %s, want good node result", body)
	}
}

func TestHandlerAllNodesFailed502(t *testing.T) {
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer down.Close()
	m := newTestManager(t, []chainpool.NodeConfig{{Name: "a", BaseURL: down.URL, Priority: 100, RateRPS: 1000}}, nil)

	gw := httptest.NewServer(newHandler(m, defaultSettings()))
	defer gw.Close()

	code, _ := post(t, gw, "/ethereum", `{"jsonrpc":"2.0","id":1,"method":"x"}`)
	if code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", code)
	}
}

func TestWithOptionsSetFields(t *testing.T) {
	s := defaultSettings()
	WithShutdownTimeout(3 * time.Second)(s)
	if s.shutdownTimeout != 3*time.Second {
		t.Fatalf("shutdownTimeout = %v, want 3s", s.shutdownTimeout)
	}
	WithLogger(chainpool.NopLogger{})(s)
	if s.logger == nil {
		t.Fatal("logger should be set")
	}
}
