package chainpool

import (
	"testing"
	"time"
)

// recordingHook embeds NopHook and records selected events for assertions.
type recordingHook struct {
	NopHook
	fallbacks  []string
	opened     []string
	closed     []string
	authFails  []string
	rateLimits []string
	results    int
}

func (h *recordingHook) OnFallback(from, to, reason string) {
	h.fallbacks = append(h.fallbacks, from+"->"+to+":"+reason)
}
func (h *recordingHook) OnCircuitOpen(node string)  { h.opened = append(h.opened, node) }
func (h *recordingHook) OnCircuitClose(node string) { h.closed = append(h.closed, node) }
func (h *recordingHook) OnAuthFailure(node string)  { h.authFails = append(h.authFails, node) }
func (h *recordingHook) OnRateLimited(node string)  { h.rateLimits = append(h.rateLimits, node) }
func (h *recordingHook) OnResult(node string, status int, err error, latency time.Duration) {
	h.results++
}

func TestNopHookSatisfiesInterfaceAndIsEmbeddable(t *testing.T) {
	var h Hook = &recordingHook{}
	h.OnRequest("n", Request{})         // inherited no-op from NopHook
	h.OnFallback("a", "b", "open")      // overridden
	rh := h.(*recordingHook)
	if len(rh.fallbacks) != 1 {
		t.Fatalf("expected 1 fallback recorded, got %d", len(rh.fallbacks))
	}
}

func TestNopLoggerSatisfiesInterface(t *testing.T) {
	var l Logger = NopLogger{}
	l.Log("info", "msg", "k", "v") // must not panic
}
