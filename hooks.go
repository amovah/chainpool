package chainpool

import "time"

// Logger is a pluggable structured logger. kv are alternating key/value pairs.
type Logger interface {
	Log(level, msg string, kv ...any)
}

// NopLogger discards all log lines.
type NopLogger struct{}

func (NopLogger) Log(string, string, ...any) {}

// Hook receives routing/health events for metrics and tracing. Embed NopHook
// to override only the events you care about.
type Hook interface {
	OnRequest(node string, req Request)
	OnFallback(from, to, reason string)
	OnCircuitOpen(node string)
	OnCircuitClose(node string)
	OnAuthFailure(node string)
	OnRateLimited(node string)
	OnResult(node string, status int, err error, latency time.Duration)
}

// NopHook implements Hook with no-ops; embed it for partial implementations.
type NopHook struct{}

func (NopHook) OnRequest(string, Request)                    {}
func (NopHook) OnFallback(string, string, string)            {}
func (NopHook) OnCircuitOpen(string)                         {}
func (NopHook) OnCircuitClose(string)                        {}
func (NopHook) OnAuthFailure(string)                         {}
func (NopHook) OnRateLimited(string)                         {}
func (NopHook) OnResult(string, int, error, time.Duration)  {}
