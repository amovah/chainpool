package gateway

import (
	"context"
	"net"
	"net/http"

	"github.com/amovah/chainpool"
)

// Serve runs the gateway on addr until ctx is cancelled or the server fails.
// On cancellation it shuts down gracefully within the configured timeout and
// returns ctx.Err(). Binding errors are returned synchronously.
func Serve(ctx context.Context, m *chainpool.Manager, addr string, opts ...Option) error {
	s := defaultSettings()
	for _, opt := range opts {
		opt(s)
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	srv := &http.Server{Handler: newHandler(m, s)}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()

	select {
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), s.shutdownTimeout)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}
