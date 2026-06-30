// Package gateway exposes a chainpool Manager as a local HTTP JSON-RPC node.
// The URL path selects the chain (e.g. POST /ethereum); the request body is
// forwarded verbatim (single or batch) through the pool with full failover.
package gateway

import (
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/amovah/chainpool"
)

type settings struct {
	logger          chainpool.Logger
	shutdownTimeout time.Duration
}

func defaultSettings() *settings {
	return &settings{
		logger:          chainpool.NopLogger{},
		shutdownTimeout: 5 * time.Second,
	}
}

// Option configures the gateway.
type Option func(*settings)

// WithLogger sets the logger used for routing errors.
func WithLogger(l chainpool.Logger) Option { return func(s *settings) { s.logger = l } }

// WithShutdownTimeout bounds graceful shutdown when the Serve context is cancelled.
func WithShutdownTimeout(d time.Duration) Option { return func(s *settings) { s.shutdownTimeout = d } }

type handler struct {
	m   *chainpool.Manager
	log chainpool.Logger
}

func newHandler(m *chainpool.Manager, s *settings) http.Handler {
	return &handler{m: m, log: s.logger}
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	chain := strings.Trim(r.URL.Path, "/")
	p, err := h.m.Pool(chain)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "cannot read body", http.StatusBadRequest)
		return
	}
	out, err := p.DoRPC(r.Context(), body)
	if err != nil {
		h.log.Log("error", "gateway route failed", "chain", chain, "err", err.Error())
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(out)
}
