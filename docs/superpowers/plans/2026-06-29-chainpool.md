# chainpool Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Go package that routes blockchain RPC requests across multiple public nodes per chain, respecting per-node rate limits and priority, with circuit-breaker fallback, supporting EVM/Solana/TON/Tron via generic HTTP + a JSON-RPC helper.

**Architecture:** `Manager` holds named `Pool`s (one per chain). A `Pool` holds priority-ordered `Node`s, each with a token-bucket limiter and a circuit breaker. `Pool.Do` walks nodes by priority, skipping OPEN breakers and saturated limiters, falling back on node errors, and backing off until a deadline. A generic HTTP transport carries `Request`/`Response`; a `RPCClient` layers JSON-RPC on top, classifying configurable RPC error codes as node errors.

**Tech Stack:** Go (stdlib `net/http`, `context`, `encoding/json`), `golang.org/x/time/rate`, `gopkg.in/yaml.v3`. Hand-rolled circuit breaker, backoff, and clock abstraction.

## Global Constraints

- Module path: `github.com/alimovahedi/chainpool` (single Go module, package `chainpool`).
- Go version floor: `go 1.22`.
- All time-dependent logic (breaker cooldown, backoff, limiter refill) MUST go through the injected `Clock` interface — never call `time.Now()` / `time.Sleep` directly in production code paths.
- Tests: no real network (use `httptest`), no real sleeps (use fake clock). Table-driven where practical.
- Run `go test ./... -race` clean. Coverage target ≥ 85%.
- Do NOT add concurrent-request caps, per-request override knobs, WebSocket transport, or typed chain wrappers (explicit non-goals).
- Public error sentinels and types per spec; callers rely on `errors.Is`/`errors.As`.

---

### Task 1: Module init + Clock abstraction

**Files:**
- Create: `go.mod`
- Create: `clock.go`
- Test: `clock_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `type Clock interface { Now() time.Time; Sleep(ctx context.Context, d time.Duration) error }`
  - `func newRealClock() Clock`
  - `type fakeClock struct{ ... }` with `func newFakeClock(t time.Time) *fakeClock`, `(*fakeClock).Now() time.Time`, `(*fakeClock).Advance(d time.Duration)`, `(*fakeClock).Sleep(ctx, d) error` (advances virtual time, returns `ctx.Err()` if already cancelled).

- [ ] **Step 1: Init module**

Run:
```bash
cd /home/killingcode/workstation/chainpool
go mod init github.com/alimovahedi/chainpool
go mod edit -go=1.22
```

- [ ] **Step 2: Write the failing test**

Create `clock_test.go`:
```go
package chainpool

import (
	"context"
	"testing"
	"time"
)

func TestFakeClockNowAndAdvance(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	c := newFakeClock(base)
	if !c.Now().Equal(base) {
		t.Fatalf("Now = %v, want %v", c.Now(), base)
	}
	c.Advance(5 * time.Second)
	if got := c.Now().Sub(base); got != 5*time.Second {
		t.Fatalf("after advance elapsed = %v, want 5s", got)
	}
}

func TestFakeClockSleepAdvances(t *testing.T) {
	c := newFakeClock(time.Unix(0, 0))
	if err := c.Sleep(context.Background(), 2*time.Second); err != nil {
		t.Fatalf("Sleep err = %v", err)
	}
	if c.Now().Sub(time.Unix(0, 0)) != 2*time.Second {
		t.Fatalf("Sleep did not advance virtual clock")
	}
}

func TestFakeClockSleepRespectsCancelledContext(t *testing.T) {
	c := newFakeClock(time.Unix(0, 0))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.Sleep(ctx, time.Second); err == nil {
		t.Fatalf("expected ctx error from cancelled context")
	}
}

func TestRealClockSleepReturnsOnContextCancel(t *testing.T) {
	c := newRealClock()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.Sleep(ctx, time.Hour); err == nil {
		t.Fatalf("expected ctx error, got nil")
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./... -run TestFakeClock -v`
Expected: FAIL — `undefined: newFakeClock`.

- [ ] **Step 4: Write minimal implementation**

Create `clock.go`:
```go
package chainpool

import (
	"context"
	"sync"
	"time"
)

// Clock abstracts time so breaker cooldowns, backoff, and limiter refill are
// deterministic in tests. Production uses newRealClock.
type Clock interface {
	Now() time.Time
	// Sleep blocks for d or until ctx is done, returning ctx.Err() on cancel.
	Sleep(ctx context.Context, d time.Duration) error
}

type realClock struct{}

func newRealClock() Clock { return realClock{} }

func (realClock) Now() time.Time { return time.Now() }

func (realClock) Sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(t time.Time) *fakeClock { return &fakeClock{now: t} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func (c *fakeClock) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.Advance(d)
	return nil
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./... -run "Clock" -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add go.mod clock.go clock_test.go
git commit -m "feat: module init and Clock abstraction"
```

---

### Task 2: Token-bucket limiter

**Files:**
- Create: `limiter.go`
- Test: `limiter_test.go`

**Interfaces:**
- Consumes: `Clock` (Task 1).
- Produces:
  - `type limiter struct { ... }`
  - `func newLimiter(rps float64, burst int, clock Clock) *limiter`
  - `func (l *limiter) Allow() bool` — non-blocking; true if a token is available at `clock.Now()`.

- [ ] **Step 1: Add dependency**

Run:
```bash
cd /home/killingcode/workstation/chainpool
go get golang.org/x/time/rate@latest
```

- [ ] **Step 2: Write the failing test**

Create `limiter_test.go`:
```go
package chainpool

import (
	"testing"
	"time"
)

func TestLimiterAllowsWithinBurst(t *testing.T) {
	c := newFakeClock(time.Unix(0, 0))
	l := newLimiter(1, 3, c) // 1 rps, burst 3
	for i := 0; i < 3; i++ {
		if !l.Allow() {
			t.Fatalf("token %d should be allowed within burst", i)
		}
	}
}

func TestLimiterBlocksWhenDrained(t *testing.T) {
	c := newFakeClock(time.Unix(0, 0))
	l := newLimiter(1, 1, c)
	if !l.Allow() {
		t.Fatal("first token should be allowed")
	}
	if l.Allow() {
		t.Fatal("second token should be denied (bucket drained, no time passed)")
	}
}

func TestLimiterRefillsAfterClockAdvance(t *testing.T) {
	c := newFakeClock(time.Unix(0, 0))
	l := newLimiter(1, 1, c) // refills 1 token/sec
	if !l.Allow() {
		t.Fatal("first allowed")
	}
	if l.Allow() {
		t.Fatal("drained")
	}
	c.Advance(time.Second)
	if !l.Allow() {
		t.Fatal("should refill after 1s")
	}
}

func TestLimiterDefaultBurstFromRPS(t *testing.T) {
	c := newFakeClock(time.Unix(0, 0))
	l := newLimiter(5, 0, c) // burst 0 -> default ceil(rps)=5
	for i := 0; i < 5; i++ {
		if !l.Allow() {
			t.Fatalf("token %d should be allowed with default burst", i)
		}
	}
	if l.Allow() {
		t.Fatal("6th should be denied")
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./... -run TestLimiter -v`
Expected: FAIL — `undefined: newLimiter`.

- [ ] **Step 4: Write minimal implementation**

Create `limiter.go`:
```go
package chainpool

import (
	"math"

	"golang.org/x/time/rate"
)

// limiter is a non-blocking token bucket whose refill is driven by an injected
// Clock so tests are deterministic.
type limiter struct {
	rl    *rate.Limiter
	clock Clock
}

func newLimiter(rps float64, burst int, clock Clock) *limiter {
	if burst <= 0 {
		burst = int(math.Ceil(rps))
		if burst < 1 {
			burst = 1
		}
	}
	return &limiter{
		rl:    rate.NewLimiter(rate.Limit(rps), burst),
		clock: clock,
	}
}

// Allow reports whether one request may proceed now, consuming a token if so.
func (l *limiter) Allow() bool {
	return l.rl.AllowN(l.clock.Now(), 1)
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./... -run TestLimiter -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add limiter.go limiter_test.go go.mod go.sum
git commit -m "feat: clock-driven token-bucket limiter"
```

---

### Task 3: Circuit breaker

**Files:**
- Create: `breaker.go`
- Test: `breaker_test.go`

**Interfaces:**
- Consumes: `Clock` (Task 1).
- Produces:
  - `type breaker struct { ... }`
  - `func newBreaker(failThreshold int, cooldown time.Duration, authFailThreshold int, authCooldown time.Duration, clock Clock) *breaker`
  - `func (b *breaker) Allowed() bool` — true if CLOSED, or OPEN with elapsed cooldown (transitions to HALF-OPEN and allows one probe); false while OPEN within cooldown or while a HALF-OPEN probe is in flight.
  - `func (b *breaker) RecordFail(isAuth bool) (opened bool)` — returns true when this call transitions the breaker into OPEN.
  - `func (b *breaker) RecordSuccess() (closed bool)` — returns true when this call transitions the breaker back to CLOSED from a non-closed state.
  - `func (b *breaker) State() string` — `"closed"|"open"|"half-open"` (for stats).

- [ ] **Step 1: Write the failing test**

Create `breaker_test.go`:
```go
package chainpool

import (
	"testing"
	"time"
)

func newTestBreaker(c Clock) *breaker {
	return newBreaker(3, 30*time.Second, 1, 5*time.Minute, c)
}

func TestBreakerOpensAfterThreshold(t *testing.T) {
	c := newFakeClock(time.Unix(0, 0))
	b := newTestBreaker(c)
	if !b.Allowed() {
		t.Fatal("starts closed")
	}
	if b.RecordFail(false) || b.RecordFail(false) {
		t.Fatal("should not open before threshold")
	}
	if !b.RecordFail(false) {
		t.Fatal("third fail should open")
	}
	if b.Allowed() {
		t.Fatal("OPEN should deny within cooldown")
	}
}

func TestBreakerHalfOpenProbeSuccessCloses(t *testing.T) {
	c := newFakeClock(time.Unix(0, 0))
	b := newTestBreaker(c)
	b.RecordFail(false)
	b.RecordFail(false)
	b.RecordFail(false) // OPEN
	c.Advance(30 * time.Second)
	if !b.Allowed() {
		t.Fatal("after cooldown, one probe allowed (half-open)")
	}
	if b.Allowed() {
		t.Fatal("second probe must be denied while one is in flight")
	}
	if !b.RecordSuccess() {
		t.Fatal("probe success should close breaker")
	}
	if !b.Allowed() {
		t.Fatal("closed breaker allows requests")
	}
}

func TestBreakerHalfOpenProbeFailReopens(t *testing.T) {
	c := newFakeClock(time.Unix(0, 0))
	b := newTestBreaker(c)
	b.RecordFail(false)
	b.RecordFail(false)
	b.RecordFail(false) // OPEN
	c.Advance(30 * time.Second)
	b.Allowed() // half-open probe
	if !b.RecordFail(false) {
		t.Fatal("probe failure should re-open")
	}
	if b.Allowed() {
		t.Fatal("should be OPEN again within fresh cooldown")
	}
}

func TestBreakerAuthFailTripsImmediatelyWithLongCooldown(t *testing.T) {
	c := newFakeClock(time.Unix(0, 0))
	b := newTestBreaker(c)
	if !b.RecordFail(true) {
		t.Fatal("single auth fail should open immediately (threshold 1)")
	}
	c.Advance(30 * time.Second) // normal cooldown elapsed...
	if b.Allowed() {
		t.Fatal("auth cooldown (5m) not yet elapsed; still OPEN")
	}
	c.Advance(5 * time.Minute)
	if !b.Allowed() {
		t.Fatal("after auth cooldown, probe allowed")
	}
}

func TestBreakerSuccessResetsConsecutiveFails(t *testing.T) {
	c := newFakeClock(time.Unix(0, 0))
	b := newTestBreaker(c)
	b.RecordFail(false)
	b.RecordFail(false)
	b.RecordSuccess() // reset
	if b.RecordFail(false) {
		t.Fatal("counter reset; one fail must not open")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./... -run TestBreaker -v`
Expected: FAIL — `undefined: newBreaker`.

- [ ] **Step 3: Write minimal implementation**

Create `breaker.go`:
```go
package chainpool

import (
	"sync"
	"time"
)

type bstate int

const (
	stateClosed bstate = iota
	stateOpen
	stateHalfOpen
)

type breaker struct {
	mu sync.Mutex

	clock             Clock
	failThreshold     int
	cooldown          time.Duration
	authFailThreshold int
	authCooldown      time.Duration

	state        bstate
	consecFails  int
	openedAt     time.Time
	openCooldown time.Duration // cooldown applied to the current OPEN period
}

func newBreaker(failThreshold int, cooldown time.Duration, authFailThreshold int, authCooldown time.Duration, clock Clock) *breaker {
	if failThreshold < 1 {
		failThreshold = 1
	}
	if authFailThreshold < 1 {
		authFailThreshold = 1
	}
	return &breaker{
		clock:             clock,
		failThreshold:     failThreshold,
		cooldown:          cooldown,
		authFailThreshold: authFailThreshold,
		authCooldown:      authCooldown,
		state:             stateClosed,
	}
}

func (b *breaker) Allowed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case stateClosed:
		return true
	case stateOpen:
		if b.clock.Now().Sub(b.openedAt) >= b.openCooldown {
			b.state = stateHalfOpen
			return true // single probe
		}
		return false
	default: // stateHalfOpen: probe already in flight
		return false
	}
}

func (b *breaker) RecordFail(isAuth bool) (opened bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.consecFails++
	threshold := b.failThreshold
	cooldown := b.cooldown
	if isAuth {
		threshold = b.authFailThreshold
		cooldown = b.authCooldown
	}
	if b.state == stateHalfOpen || b.consecFails >= threshold {
		b.state = stateOpen
		b.openedAt = b.clock.Now()
		b.openCooldown = cooldown
		return true
	}
	return false
}

func (b *breaker) RecordSuccess() (closed bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	was := b.state
	b.consecFails = 0
	b.state = stateClosed
	return was != stateClosed
}

func (b *breaker) State() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case stateOpen:
		return "open"
	case stateHalfOpen:
		return "half-open"
	default:
		return "closed"
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./... -run TestBreaker -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add breaker.go breaker_test.go
git commit -m "feat: per-node circuit breaker with auth fast-trip"
```

---

### Task 4: Transport types, errors, and classification

**Files:**
- Create: `transport.go`
- Create: `errors.go`
- Test: `errors_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `type Request struct { Method string; Path string; Body []byte; Header http.Header }`
  - `type Response struct { StatusCode int; Body []byte; Header http.Header; Node string }`
  - `type RPCError struct { Code int `json:"code"`; Message string `json:"message"`; Data json.RawMessage `json:"data,omitempty"` }`; `func (e *RPCError) Error() string`
  - `type NodeAttempt struct { Node string; LastStatus int; LastErr error }`
  - `type AllFailedError struct { Chain string; Attempts []NodeAttempt }`; `Error()`, `Unwrap() error`
  - `var ErrAllNodesUnavailable, ErrNoNodes, ErrUnknownChain error`
  - `type errKind int` with `kindReturn`, `kindNode`, `kindAuth`
  - `func classifyHTTP(resp *Response, err error) errKind`

- [ ] **Step 1: Write the failing test**

Create `errors_test.go`:
```go
package chainpool

import (
	"errors"
	"net/http"
	"testing"
)

func TestClassifyHTTP(t *testing.T) {
	cases := []struct {
		name string
		resp *Response
		err  error
		want errKind
	}{
		{"transport error", nil, errors.New("dial fail"), kindNode},
		{"429", &Response{StatusCode: 429}, nil, kindNode},
		{"500", &Response{StatusCode: 500}, nil, kindNode},
		{"503", &Response{StatusCode: 503}, nil, kindNode},
		{"401 auth", &Response{StatusCode: 401}, nil, kindAuth},
		{"403 auth", &Response{StatusCode: 403}, nil, kindAuth},
		{"400 caller", &Response{StatusCode: 400}, nil, kindReturn},
		{"404 caller", &Response{StatusCode: 404}, nil, kindReturn},
		{"200 ok", &Response{StatusCode: 200}, nil, kindReturn},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyHTTP(tc.resp, tc.err); got != tc.want {
				t.Fatalf("classifyHTTP = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestAllFailedErrorUnwraps(t *testing.T) {
	e := &AllFailedError{Chain: "ethereum", Attempts: []NodeAttempt{{Node: "a", LastStatus: 500}}}
	if !errors.Is(e, ErrAllNodesUnavailable) {
		t.Fatal("AllFailedError should unwrap to ErrAllNodesUnavailable")
	}
	if e.Error() == "" {
		t.Fatal("Error() should be non-empty")
	}
}

func TestRPCErrorImplementsError(t *testing.T) {
	var err error = &RPCError{Code: -32000, Message: "boom"}
	var re *RPCError
	if !errors.As(err, &re) || re.Code != -32000 {
		t.Fatal("RPCError should be unwrappable via errors.As")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./... -run "TestClassifyHTTP|TestAllFailed|TestRPCError" -v`
Expected: FAIL — undefined identifiers.

- [ ] **Step 3: Write minimal implementation**

Create `transport.go`:
```go
package chainpool

import "net/http"

// Request is a generic HTTP request routed to a node. Path is appended to the
// node's base URL; Header is merged on top of the node's static headers.
type Request struct {
	Method string
	Path   string
	Body   []byte
	Header http.Header
}

// Response is the result served by a node.
type Response struct {
	StatusCode int
	Body       []byte
	Header     http.Header
	Node       string
}
```

Create `errors.go`:
```go
package chainpool

import (
	"encoding/json"
	"errors"
	"fmt"
)

var (
	ErrAllNodesUnavailable = errors.New("chainpool: all nodes unavailable")
	ErrNoNodes             = errors.New("chainpool: pool has no nodes")
	ErrUnknownChain        = errors.New("chainpool: unknown chain")
)

// RPCError is a valid JSON-RPC error object returned to the caller (same answer
// on every node, so not subject to fallback unless its code is in the
// pool's fallback set).
type RPCError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *RPCError) Error() string {
	return fmt.Sprintf("chainpool: rpc error %d: %s", e.Code, e.Message)
}

// NodeAttempt records one node's last failure during an exhausted request.
type NodeAttempt struct {
	Node       string
	LastStatus int
	LastErr    error
}

// AllFailedError is returned when every node was unavailable before the
// deadline. It unwraps to ErrAllNodesUnavailable.
type AllFailedError struct {
	Chain    string
	Attempts []NodeAttempt
}

func (e *AllFailedError) Error() string {
	return fmt.Sprintf("chainpool: all %d nodes unavailable for chain %q", len(e.Attempts), e.Chain)
}

func (e *AllFailedError) Unwrap() error { return ErrAllNodesUnavailable }

// errKind drives the pool's per-response decision.
type errKind int

const (
	kindReturn errKind = iota // hand response/err to caller; record breaker success
	kindNode                  // node failure; record breaker fail, fall back
	kindAuth                  // auth failure; record breaker fail (auth), fall back
)

func classifyHTTP(resp *Response, err error) errKind {
	if err != nil {
		return kindNode
	}
	switch {
	case resp.StatusCode == 401 || resp.StatusCode == 403:
		return kindAuth
	case resp.StatusCode == 429:
		return kindNode
	case resp.StatusCode >= 500:
		return kindNode
	default:
		return kindReturn
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./... -run "TestClassifyHTTP|TestAllFailed|TestRPCError" -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add transport.go errors.go errors_test.go
git commit -m "feat: transport types, error model, HTTP classification"
```

---

### Task 5: Hooks and Logger

**Files:**
- Create: `hooks.go`
- Test: `hooks_test.go`

**Interfaces:**
- Consumes: `Request` (Task 4).
- Produces:
  - `type Logger interface { Log(level, msg string, kv ...any) }`
  - `type Hook interface { OnRequest(node string, req Request); OnFallback(from, to, reason string); OnCircuitOpen(node string); OnCircuitClose(node string); OnAuthFailure(node string); OnRateLimited(node string); OnResult(node string, status int, err error, latency time.Duration) }`
  - `type NopHook struct{}` implementing `Hook` (all no-ops) — embeddable so users override only what they need.
  - `type NopLogger struct{}` implementing `Logger`.

- [ ] **Step 1: Write the failing test**

Create `hooks_test.go`:
```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./... -run "TestNopHook|TestNopLogger" -v`
Expected: FAIL — `undefined: NopHook` / `NopLogger`.

- [ ] **Step 3: Write minimal implementation**

Create `hooks.go`:
```go
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./... -run "TestNopHook|TestNopLogger" -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add hooks.go hooks_test.go
git commit -m "feat: Logger and Hook interfaces with Nop defaults"
```

---

### Task 6: Node (HTTP executor + health)

**Files:**
- Create: `node.go`
- Test: `node_test.go`

**Interfaces:**
- Consumes: `Clock`, `limiter`, `breaker`, `Request`, `Response` (Tasks 1–4).
- Produces:
  - `type node struct { name string; baseURL string; priority int; headers map[string]string; timeout time.Duration; client *http.Client; lim *limiter; brk *breaker }`
  - `func (n *node) do(ctx context.Context, req Request) (*Response, error)` — builds the HTTP request, merges static headers then per-request headers, applies per-request timeout, returns `*Response` with `Node` set.
  - `type NodeStat struct { Name string; Priority int; State string }`

- [ ] **Step 1: Write the failing test**

Create `node_test.go`:
```go
package chainpool

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func newHTTPTestNode(name, url string, headers map[string]string, timeout time.Duration) *node {
	c := newFakeClock(time.Unix(0, 0))
	return &node{
		name:    name,
		baseURL: url,
		headers: headers,
		timeout: timeout,
		client:  &http.Client{},
		lim:     newLimiter(1000, 1000, c),
		brk:     newBreaker(5, time.Second, 1, time.Minute, c),
	}
}

func TestNodeDoMergesHeadersAndSetsNode(t *testing.T) {
	var gotAuth, gotExtra string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotExtra = r.Header.Get("X-Trace")
		body, _ := io.ReadAll(r.Body)
		w.WriteHeader(201)
		_, _ = w.Write(append([]byte("echo:"), body...))
	}))
	defer srv.Close()

	n := newHTTPTestNode("n1", srv.URL, map[string]string{"Authorization": "Bearer K"}, 0)
	req := Request{Method: "POST", Path: "/v1", Body: []byte("hi"), Header: http.Header{"X-Trace": []string{"abc"}}}
	resp, err := n.do(context.Background(), req)
	if err != nil {
		t.Fatalf("do err = %v", err)
	}
	if gotAuth != "Bearer K" {
		t.Fatalf("auth header = %q", gotAuth)
	}
	if gotExtra != "abc" {
		t.Fatalf("trace header = %q", gotExtra)
	}
	if resp.StatusCode != 201 || string(resp.Body) != "echo:hi" {
		t.Fatalf("resp = %d %q", resp.StatusCode, resp.Body)
	}
	if resp.Node != "n1" {
		t.Fatalf("resp.Node = %q, want n1", resp.Node)
	}
}

func TestNodeDoPerRequestTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	n := newHTTPTestNode("slow", srv.URL, nil, 10*time.Millisecond)
	_, err := n.do(context.Background(), Request{Method: "GET"})
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestNodeDoRespectsContextCancel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
	}))
	defer srv.Close()

	n := newHTTPTestNode("n", srv.URL, nil, 0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := n.do(ctx, Request{Method: "GET"}); err == nil {
		t.Fatal("expected ctx cancel error")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./... -run TestNodeDo -v`
Expected: FAIL — `undefined: node`.

- [ ] **Step 3: Write minimal implementation**

Create `node.go`:
```go
package chainpool

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"time"
)

type node struct {
	name     string
	baseURL  string
	priority int
	headers  map[string]string
	timeout  time.Duration
	client   *http.Client
	lim      *limiter
	brk      *breaker
}

// NodeStat is a snapshot of a node's identity and breaker state.
type NodeStat struct {
	Name     string
	Priority int
	State    string
}

func (n *node) do(ctx context.Context, req Request) (*Response, error) {
	rctx := ctx
	if n.timeout > 0 {
		var cancel context.CancelFunc
		rctx, cancel = context.WithTimeout(ctx, n.timeout)
		defer cancel()
	}

	method := req.Method
	if method == "" {
		method = http.MethodGet
	}
	hr, err := http.NewRequestWithContext(rctx, method, n.baseURL+req.Path, bytes.NewReader(req.Body))
	if err != nil {
		return nil, err
	}
	for k, v := range n.headers {
		hr.Header.Set(k, v)
	}
	for k, vs := range req.Header {
		for _, v := range vs {
			hr.Header.Add(k, v)
		}
	}

	resp, err := n.client.Do(hr)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return &Response{
		StatusCode: resp.StatusCode,
		Body:       body,
		Header:     resp.Header,
		Node:       n.name,
	}, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./... -run TestNodeDo -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add node.go node_test.go
git commit -m "feat: node HTTP executor with header merge and timeouts"
```

---

### Task 7: Config schema, Duration, loader

**Files:**
- Create: `config.go`
- Test: `config_test.go`
- Test fixtures: `testdata/valid.yaml`, `testdata/valid.json`

**Interfaces:**
- Consumes: nothing (pure data + parsing).
- Produces:
  - `type Duration time.Duration` with `UnmarshalYAML`, `UnmarshalJSON`, `func (d Duration) D() time.Duration`
  - `type Config`, `ChainConfig`, `NodeConfig`, `BreakerConfig`, `BackoffConfig`, `Defaults` exactly as in the spec (yaml+json tags).
  - `func LoadConfig(path string) (Config, error)` — reads file, `${ENV}` expands, unmarshals by extension, applies defaults, validates.
  - `func (c *Config) applyDefaults()` — fills zero node/chain fields from `Defaults`.
  - `func (c *Config) validate() error` — duplicate node names per chain, parseable URL, `rate_rps > 0`.

- [ ] **Step 1: Add dependency**

Run:
```bash
cd /home/killingcode/workstation/chainpool
go get gopkg.in/yaml.v3@latest
```

- [ ] **Step 2: Write fixtures**

Create `testdata/valid.yaml`:
```yaml
defaults:
  rate_rps: 10
  timeout: 8s
  breaker: { fail_threshold: 5, cooldown: 30s, auth_fail_threshold: 1, auth_cooldown: 5m }
  backoff: { initial: 50ms, max: 2s, factor: 2.0 }
  fallback_rpc_codes: [-32603, -32005]
chains:
  ethereum:
    timeout: 10s
    nodes:
      - name: alchemy
        url: https://eth.example/v2/x
        priority: 100
        rate_rps: 25
        headers: { Authorization: "Bearer ${TEST_ALCHEMY_KEY}" }
      - name: ankr
        url: https://rpc.example/eth
        priority: 50
```

Create `testdata/valid.json`:
```json
{
  "defaults": { "rate_rps": 10, "timeout": "8s" },
  "chains": {
    "solana": {
      "nodes": [
        { "name": "sol", "url": "https://sol.example", "priority": 10, "rate_rps": 8 }
      ]
    }
  }
}
```

- [ ] **Step 3: Write the failing test**

Create `config_test.go`:
```go
package chainpool

import (
	"strings"
	"testing"
	"time"
)

func TestDurationParsesYAMLAndJSON(t *testing.T) {
	cfg, err := LoadConfig("testdata/valid.yaml")
	if err != nil {
		t.Fatalf("load yaml: %v", err)
	}
	if cfg.Chains["ethereum"].Timeout.D() != 10*time.Second {
		t.Fatalf("timeout = %v", cfg.Chains["ethereum"].Timeout.D())
	}
}

func TestLoadConfigEnvInterpolation(t *testing.T) {
	t.Setenv("TEST_ALCHEMY_KEY", "secret123")
	cfg, err := LoadConfig("testdata/valid.yaml")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	eth := cfg.Chains["ethereum"]
	if got := eth.Nodes[0].Headers["Authorization"]; got != "Bearer secret123" {
		t.Fatalf("interpolation failed: %q", got)
	}
}

func TestApplyDefaultsFillsNodeFields(t *testing.T) {
	cfg, err := LoadConfig("testdata/valid.yaml")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	ankr := cfg.Chains["ethereum"].Nodes[1] // only name/url/priority set
	if ankr.RateRPS != 10 {
		t.Fatalf("default rate_rps not applied: %v", ankr.RateRPS)
	}
	if ankr.Breaker.FailThreshold != 5 {
		t.Fatalf("default breaker not applied: %+v", ankr.Breaker)
	}
}

func TestLoadConfigJSON(t *testing.T) {
	cfg, err := LoadConfig("testdata/valid.json")
	if err != nil {
		t.Fatalf("load json: %v", err)
	}
	if cfg.Chains["solana"].Nodes[0].Name != "sol" {
		t.Fatal("json node not parsed")
	}
}

func TestValidateRejectsDuplicateNodeNames(t *testing.T) {
	cfg := Config{Chains: map[string]ChainConfig{
		"x": {Nodes: []NodeConfig{
			{Name: "a", BaseURL: "https://a", RateRPS: 1},
			{Name: "a", BaseURL: "https://b", RateRPS: 1},
		}},
	}}
	if err := cfg.validate(); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("expected duplicate error, got %v", err)
	}
}

func TestValidateRejectsBadURLAndRPS(t *testing.T) {
	bad := Config{Chains: map[string]ChainConfig{
		"x": {Nodes: []NodeConfig{{Name: "a", BaseURL: "://nope", RateRPS: 1}}},
	}}
	if err := bad.validate(); err == nil {
		t.Fatal("expected bad url error")
	}
	zero := Config{Chains: map[string]ChainConfig{
		"x": {Nodes: []NodeConfig{{Name: "a", BaseURL: "https://a", RateRPS: 0}}},
	}}
	if err := zero.validate(); err == nil {
		t.Fatal("expected rps>0 error")
	}
}
```

- [ ] **Step 4: Run test to verify it fails**

Run: `go test ./... -run "TestDuration|TestLoadConfig|TestApplyDefaults|TestValidate" -v`
Expected: FAIL — `undefined: LoadConfig`.

- [ ] **Step 5: Write minimal implementation**

Create `config.go`:
```go
package chainpool

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration parses Go duration strings ("10s", "500ms") from YAML and JSON.
type Duration time.Duration

func (d Duration) D() time.Duration { return time.Duration(d) }

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	return d.parse(s)
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	return d.parse(s)
}

func (d *Duration) parse(s string) error {
	if s == "" {
		*d = 0
		return nil
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("chainpool: invalid duration %q: %w", s, err)
	}
	*d = Duration(parsed)
	return nil
}

type Config struct {
	Chains   map[string]ChainConfig `yaml:"chains" json:"chains"`
	Defaults Defaults               `yaml:"defaults" json:"defaults"`
}

type ChainConfig struct {
	Nodes   []NodeConfig  `yaml:"nodes" json:"nodes"`
	Timeout Duration      `yaml:"timeout" json:"timeout"`
	Backoff BackoffConfig `yaml:"backoff" json:"backoff"`
}

type NodeConfig struct {
	Name     string            `yaml:"name" json:"name"`
	BaseURL  string            `yaml:"url" json:"url"`
	Priority int               `yaml:"priority" json:"priority"`
	RateRPS  float64           `yaml:"rate_rps" json:"rate_rps"`
	Burst    int               `yaml:"burst" json:"burst"`
	Timeout  Duration          `yaml:"timeout" json:"timeout"`
	Headers  map[string]string `yaml:"headers" json:"headers"`
	Breaker  BreakerConfig     `yaml:"breaker" json:"breaker"`
}

type BreakerConfig struct {
	FailThreshold     int      `yaml:"fail_threshold" json:"fail_threshold"`
	Cooldown          Duration `yaml:"cooldown" json:"cooldown"`
	AuthFailThreshold int      `yaml:"auth_fail_threshold" json:"auth_fail_threshold"`
	AuthCooldown      Duration `yaml:"auth_cooldown" json:"auth_cooldown"`
}

type BackoffConfig struct {
	Initial Duration `yaml:"initial" json:"initial"`
	Max     Duration `yaml:"max" json:"max"`
	Factor  float64  `yaml:"factor" json:"factor"`
}

type Defaults struct {
	RateRPS          float64       `yaml:"rate_rps" json:"rate_rps"`
	Burst            int           `yaml:"burst" json:"burst"`
	Timeout          Duration      `yaml:"timeout" json:"timeout"`
	Breaker          BreakerConfig `yaml:"breaker" json:"breaker"`
	Backoff          BackoffConfig `yaml:"backoff" json:"backoff"`
	FallbackRPCCodes []int         `yaml:"fallback_rpc_codes" json:"fallback_rpc_codes"`
}

func LoadConfig(path string) (Config, error) {
	var cfg Config
	raw, err := os.ReadFile(path)
	if err != nil {
		return cfg, fmt.Errorf("chainpool: read config: %w", err)
	}
	expanded := os.Expand(string(raw), os.Getenv)

	switch strings.ToLower(filepath.Ext(path)) {
	case ".yaml", ".yml":
		if err := yaml.Unmarshal([]byte(expanded), &cfg); err != nil {
			return cfg, fmt.Errorf("chainpool: parse yaml: %w", err)
		}
	case ".json":
		if err := json.Unmarshal([]byte(expanded), &cfg); err != nil {
			return cfg, fmt.Errorf("chainpool: parse json: %w", err)
		}
	default:
		return cfg, fmt.Errorf("chainpool: unsupported config extension %q", filepath.Ext(path))
	}

	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func (c *Config) applyDefaults() {
	d := c.Defaults
	if len(d.FallbackRPCCodes) == 0 {
		d.FallbackRPCCodes = []int{-32603}
		c.Defaults = d
	}
	for cname, chain := range c.Chains {
		if chain.Backoff == (BackoffConfig{}) {
			chain.Backoff = d.Backoff
		}
		for i := range chain.Nodes {
			n := &chain.Nodes[i]
			if n.RateRPS == 0 {
				n.RateRPS = d.RateRPS
			}
			if n.Burst == 0 {
				n.Burst = d.Burst
			}
			if n.Timeout == 0 {
				n.Timeout = d.Timeout
			}
			if n.Breaker == (BreakerConfig{}) {
				n.Breaker = d.Breaker
			}
		}
		c.Chains[cname] = chain
	}
}

func (c *Config) validate() error {
	for cname, chain := range c.Chains {
		seen := map[string]bool{}
		for _, n := range chain.Nodes {
			if seen[n.Name] {
				return fmt.Errorf("chainpool: chain %q has duplicate node name %q", cname, n.Name)
			}
			seen[n.Name] = true
			u, err := url.Parse(n.BaseURL)
			if err != nil || u.Scheme == "" || u.Host == "" {
				return fmt.Errorf("chainpool: chain %q node %q has invalid url %q", cname, n.Name, n.BaseURL)
			}
			if n.RateRPS <= 0 {
				return fmt.Errorf("chainpool: chain %q node %q must have rate_rps > 0", cname, n.Name)
			}
		}
	}
	return nil
}
```

- [ ] **Step 6: Run test to verify it passes**

Run: `go test ./... -run "TestDuration|TestLoadConfig|TestApplyDefaults|TestValidate" -v`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add config.go config_test.go testdata/valid.yaml testdata/valid.json go.mod go.sum
git commit -m "feat: config schema, Duration, env-expanding loader with validation"
```

---

### Task 8: Functional options + runtime settings

**Files:**
- Create: `options.go`
- Test: `options_test.go`

**Interfaces:**
- Consumes: `Hook`, `Logger`, `Clock`, `NodeConfig` (earlier tasks).
- Produces:
  - `type settings struct { hook Hook; logger Logger; clock Clock; httpClient *http.Client; extraNodes map[string][]NodeConfig }`
  - `type Option func(*settings)`
  - `func WithHook(h Hook) Option`
  - `func WithLogger(l Logger) Option`
  - `func WithClock(c Clock) Option` (test seam)
  - `func WithHTTPClient(hc *http.Client) Option`
  - `func WithNode(chain string, n NodeConfig) Option`
  - `func defaultSettings() *settings` (Nop hook/logger, real clock, `&http.Client{}`).

- [ ] **Step 1: Write the failing test**

Create `options_test.go`:
```go
package chainpool

import (
	"net/http"
	"testing"
)

func TestOptionsApply(t *testing.T) {
	s := defaultSettings()
	h := &recordingHook{}
	hc := &http.Client{}
	WithHook(h)(s)
	WithLogger(NopLogger{})(s)
	WithHTTPClient(hc)(s)
	WithNode("ethereum", NodeConfig{Name: "extra", BaseURL: "https://x", RateRPS: 1})(s)

	if s.hook != h {
		t.Fatal("WithHook not applied")
	}
	if s.httpClient != hc {
		t.Fatal("WithHTTPClient not applied")
	}
	if len(s.extraNodes["ethereum"]) != 1 {
		t.Fatal("WithNode not applied")
	}
}

func TestDefaultSettingsAreNonNil(t *testing.T) {
	s := defaultSettings()
	if s.hook == nil || s.logger == nil || s.clock == nil || s.httpClient == nil {
		t.Fatal("defaults must be non-nil")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./... -run "TestOptions|TestDefaultSettings" -v`
Expected: FAIL — `undefined: defaultSettings`.

- [ ] **Step 3: Write minimal implementation**

Create `options.go`:
```go
package chainpool

import "net/http"

type settings struct {
	hook       Hook
	logger     Logger
	clock      Clock
	httpClient *http.Client
	extraNodes map[string][]NodeConfig
}

func defaultSettings() *settings {
	return &settings{
		hook:       NopHook{},
		logger:     NopLogger{},
		clock:      newRealClock(),
		httpClient: &http.Client{},
		extraNodes: map[string][]NodeConfig{},
	}
}

// Option configures a Manager at construction time.
type Option func(*settings)

func WithHook(h Hook) Option { return func(s *settings) { s.hook = h } }

func WithLogger(l Logger) Option { return func(s *settings) { s.logger = l } }

// WithClock injects a clock (test seam).
func WithClock(c Clock) Option { return func(s *settings) { s.clock = c } }

func WithHTTPClient(hc *http.Client) Option { return func(s *settings) { s.httpClient = hc } }

// WithNode appends a node to a chain programmatically.
func WithNode(chain string, n NodeConfig) Option {
	return func(s *settings) { s.extraNodes[chain] = append(s.extraNodes[chain], n) }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./... -run "TestOptions|TestDefaultSettings" -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add options.go options_test.go
git commit -m "feat: functional options for Manager construction"
```

---

### Task 9: Backoff helper

**Files:**
- Create: `backoff.go`
- Test: `backoff_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `type backoff struct { initial, max time.Duration; factor float64; cur time.Duration }`
  - `func newBackoff(cfg BackoffConfig) *backoff` (applies sane defaults: initial 50ms, max 2s, factor 2.0 when zero).
  - `func (b *backoff) next() time.Duration` — exponential, capped at max.
  - `func (b *backoff) reset()`.

- [ ] **Step 1: Write the failing test**

Create `backoff_test.go`:
```go
package chainpool

import (
	"testing"
	"time"
)

func TestBackoffExponentialCapped(t *testing.T) {
	b := newBackoff(BackoffConfig{
		Initial: Duration(100 * time.Millisecond),
		Max:     Duration(400 * time.Millisecond),
		Factor:  2.0,
	})
	got := []time.Duration{b.next(), b.next(), b.next(), b.next()}
	want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 400 * time.Millisecond}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("step %d = %v, want %v", i, got[i], want[i])
		}
	}
}

func TestBackoffDefaultsWhenZero(t *testing.T) {
	b := newBackoff(BackoffConfig{})
	if b.next() != 50*time.Millisecond {
		t.Fatalf("default initial = %v, want 50ms", b.next())
	}
}

func TestBackoffReset(t *testing.T) {
	b := newBackoff(BackoffConfig{Initial: Duration(time.Second), Max: Duration(time.Minute), Factor: 2})
	b.next()
	b.next()
	b.reset()
	if b.next() != time.Second {
		t.Fatal("reset should return to initial")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./... -run TestBackoff -v`
Expected: FAIL — `undefined: newBackoff`.

- [ ] **Step 3: Write minimal implementation**

Create `backoff.go`:
```go
package chainpool

import "time"

type backoff struct {
	initial time.Duration
	max     time.Duration
	factor  float64
	cur     time.Duration
}

func newBackoff(cfg BackoffConfig) *backoff {
	b := &backoff{
		initial: cfg.Initial.D(),
		max:     cfg.Max.D(),
		factor:  cfg.Factor,
	}
	if b.initial <= 0 {
		b.initial = 50 * time.Millisecond
	}
	if b.max <= 0 {
		b.max = 2 * time.Second
	}
	if b.factor < 1 {
		b.factor = 2.0
	}
	return b
}

func (b *backoff) next() time.Duration {
	if b.cur == 0 {
		b.cur = b.initial
	} else {
		b.cur = time.Duration(float64(b.cur) * b.factor)
		if b.cur > b.max {
			b.cur = b.max
		}
	}
	return b.cur
}

func (b *backoff) reset() { b.cur = 0 }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./... -run TestBackoff -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add backoff.go backoff_test.go
git commit -m "feat: exponential capped backoff"
```

---

### Task 10: Pool selection loop

**Files:**
- Create: `pool.go`
- Test: `pool_test.go`
- Test helper: `pooltest_test.go` (FakeNode server + builder)

**Interfaces:**
- Consumes: `node`, `limiter`, `breaker`, `backoff`, `Hook`, `Clock`, `classifyHTTP`, error types (Tasks 1–9).
- Produces:
  - `type Pool struct { chain string; nodes []*node; backoffCfg BackoffConfig; timeout time.Duration; hook Hook; clock Clock; fallbackCodes []int }`
  - `type respClassifier func(resp *Response) errKind`
  - `func (p *Pool) Do(ctx context.Context, req Request) (*Response, error)` — public, no RPC classifier.
  - `func (p *Pool) doClassified(ctx context.Context, req Request, rc respClassifier) (*Response, error)` — internal; used by RPCClient.
  - `func (p *Pool) Stats() []NodeStat`
  - `func (p *Pool) FallbackCodes() []int` (exposes set for RPCClient).

**Behavior:** nodes sorted by priority desc; OPEN breaker → skip (`OnFallback` reason "open"); `limiter.Allow()` false → skip (`OnRateLimited`); send (`OnRequest`/`OnResult`); classify (HTTP then optional `rc`); `kindAuth` → `RecordFail(true)`+`OnAuthFailure`+`OnCircuitOpen` if opened, fall back; `kindNode` → `RecordFail(false)`+`OnCircuitOpen` if opened, fall back; `kindReturn` → `RecordSuccess`+`OnCircuitClose` if closed, return. Full pass with no return → backoff sleep until deadline → `*AllFailedError`.

- [ ] **Step 1: Write the FakeNode helper**

Create `pooltest_test.go`:
```go
package chainpool

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"time"
)

// scriptedResponse is one canned reply.
type scriptedResponse struct {
	status int
	body   string
	delay  time.Duration
}

// fakeServer serves a queue of scripted responses; the last one repeats.
type fakeServer struct {
	mu     sync.Mutex
	srv    *httptest.Server
	queue  []scriptedResponse
	hits   int
}

func newFakeServer(responses ...scriptedResponse) *fakeServer {
	f := &fakeServer{queue: responses}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		idx := f.hits
		f.hits++
		var resp scriptedResponse
		if idx < len(f.queue) {
			resp = f.queue[idx]
		} else if len(f.queue) > 0 {
			resp = f.queue[len(f.queue)-1]
		} else {
			resp = scriptedResponse{status: 200, body: "{}"}
		}
		f.mu.Unlock()
		if resp.delay > 0 {
			time.Sleep(resp.delay)
		}
		w.WriteHeader(resp.status)
		_, _ = w.Write([]byte(resp.body))
	}))
	return f
}

func (f *fakeServer) url() string { return f.srv.URL }
func (f *fakeServer) close()      { f.srv.Close() }
func (f *fakeServer) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hits
}

// buildPool wires a Pool with the given nodes using a fake clock and recording hook.
func buildPool(clock Clock, hook Hook, fallbackCodes []int, nodes ...*node) *Pool {
	return &Pool{
		chain:         "test",
		nodes:         nodes,
		backoffCfg:    BackoffConfig{Initial: Duration(time.Millisecond), Max: Duration(time.Millisecond), Factor: 2},
		timeout:       5 * time.Second,
		hook:          hook,
		clock:         clock,
		fallbackCodes: fallbackCodes,
	}
}

func testNode(name, url string, priority int, clock Clock, rps float64) *node {
	return &node{
		name:     name,
		baseURL:  url,
		priority: priority,
		timeout:  2 * time.Second,
		client:   &http.Client{},
		lim:      newLimiter(rps, int(rps)+1, clock),
		brk:      newBreaker(2, 30*time.Second, 1, 5*time.Minute, clock),
	}
}
```

- [ ] **Step 2: Write the failing test**

Create `pool_test.go`:
```go
package chainpool

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPoolPrefersHighestPriority(t *testing.T) {
	c := newFakeClock(time.Unix(0, 0))
	good := newFakeServer(scriptedResponse{status: 200, body: "ok"})
	defer good.close()
	low := newFakeServer(scriptedResponse{status: 200, body: "low"})
	defer low.close()

	hook := &recordingHook{}
	p := buildPool(c, hook, nil,
		testNode("hi", good.url(), 100, c, 1000),
		testNode("lo", low.url(), 1, c, 1000),
	)
	resp, err := p.Do(context.Background(), Request{Method: "GET"})
	if err != nil {
		t.Fatalf("Do err = %v", err)
	}
	if resp.Node != "hi" {
		t.Fatalf("served by %q, want hi", resp.Node)
	}
	if low.count() != 0 {
		t.Fatal("low-priority node should not be touched")
	}
}

func TestPoolFallsBackOnNodeError(t *testing.T) {
	c := newFakeClock(time.Unix(0, 0))
	bad := newFakeServer(scriptedResponse{status: 500, body: "err"})
	defer bad.close()
	good := newFakeServer(scriptedResponse{status: 200, body: "ok"})
	defer good.close()

	hook := &recordingHook{}
	p := buildPool(c, hook, nil,
		testNode("bad", bad.url(), 100, c, 1000),
		testNode("good", good.url(), 50, c, 1000),
	)
	resp, err := p.Do(context.Background(), Request{Method: "GET"})
	if err != nil {
		t.Fatalf("Do err = %v", err)
	}
	if resp.Node != "good" {
		t.Fatalf("served by %q, want good", resp.Node)
	}
	if len(hook.fallbacks) == 0 {
		t.Fatal("expected OnFallback event")
	}
}

func TestPoolSkipsSaturatedNode(t *testing.T) {
	c := newFakeClock(time.Unix(0, 0))
	primary := newFakeServer(scriptedResponse{status: 200, body: "p"})
	defer primary.close()
	secondary := newFakeServer(scriptedResponse{status: 200, body: "s"})
	defer secondary.close()

	hook := &recordingHook{}
	// primary rps=1 burst=1; drain it first.
	pn := testNode("primary", primary.url(), 100, c, 1)
	pn.lim = newLimiter(1, 1, c)
	p := buildPool(c, hook, nil, pn, testNode("secondary", secondary.url(), 50, c, 1000))

	// first request drains primary
	if _, err := p.Do(context.Background(), Request{Method: "GET"}); err != nil {
		t.Fatalf("first Do: %v", err)
	}
	// second: primary saturated -> secondary
	resp, err := p.Do(context.Background(), Request{Method: "GET"})
	if err != nil {
		t.Fatalf("second Do: %v", err)
	}
	if resp.Node != "secondary" {
		t.Fatalf("served by %q, want secondary", resp.Node)
	}
	if len(hook.rateLimits) == 0 {
		t.Fatal("expected OnRateLimited event")
	}
}

func TestPoolAuthFailureTripsImmediatelyAndAlerts(t *testing.T) {
	c := newFakeClock(time.Unix(0, 0))
	authBad := newFakeServer(scriptedResponse{status: 401, body: "no"})
	defer authBad.close()
	good := newFakeServer(scriptedResponse{status: 200, body: "ok"})
	defer good.close()

	hook := &recordingHook{}
	p := buildPool(c, hook, nil,
		testNode("authbad", authBad.url(), 100, c, 1000),
		testNode("good", good.url(), 50, c, 1000),
	)
	if _, err := p.Do(context.Background(), Request{Method: "GET"}); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if len(hook.authFails) != 1 {
		t.Fatalf("expected 1 OnAuthFailure, got %d", len(hook.authFails))
	}
	if len(hook.opened) == 0 {
		t.Fatal("auth failure should open breaker immediately")
	}
	// second request: authbad breaker OPEN -> goes straight to good
	hitsBefore := authBad.count()
	resp, _ := p.Do(context.Background(), Request{Method: "GET"})
	if resp.Node != "good" {
		t.Fatalf("served by %q, want good", resp.Node)
	}
	if authBad.count() != hitsBefore {
		t.Fatal("OPEN breaker node must be skipped (no new hit)")
	}
}

func TestPoolAllFailedReturnsAllFailedError(t *testing.T) {
	c := newFakeClock(time.Unix(0, 0))
	a := newFakeServer(scriptedResponse{status: 500, body: "x"})
	defer a.close()
	b := newFakeServer(scriptedResponse{status: 503, body: "y"})
	defer b.close()

	hook := &recordingHook{}
	p := buildPool(c, hook, nil,
		testNode("a", a.url(), 100, c, 1000),
		testNode("b", b.url(), 50, c, 1000),
	)
	p.timeout = 10 * time.Millisecond // fake clock advances via backoff Sleep

	_, err := p.Do(context.Background(), Request{Method: "GET"})
	if err == nil {
		t.Fatal("expected error when all nodes fail")
	}
	if !errors.Is(err, ErrAllNodesUnavailable) {
		t.Fatalf("err = %v, want ErrAllNodesUnavailable", err)
	}
	var afe *AllFailedError
	if !errors.As(err, &afe) || len(afe.Attempts) == 0 {
		t.Fatalf("expected AllFailedError with attempts, got %v", err)
	}
}

func TestPoolReturnsCallerErrorWithoutFallback(t *testing.T) {
	c := newFakeClock(time.Unix(0, 0))
	primary := newFakeServer(scriptedResponse{status: 400, body: "bad request"})
	defer primary.close()
	secondary := newFakeServer(scriptedResponse{status: 200, body: "ok"})
	defer secondary.close()

	hook := &recordingHook{}
	p := buildPool(c, hook, nil,
		testNode("primary", primary.url(), 100, c, 1000),
		testNode("secondary", secondary.url(), 50, c, 1000),
	)
	resp, err := p.Do(context.Background(), Request{Method: "GET"})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if resp.StatusCode != 400 || resp.Node != "primary" {
		t.Fatalf("expected 400 from primary, got %d from %s", resp.StatusCode, resp.Node)
	}
	if secondary.count() != 0 {
		t.Fatal("4xx caller error must not fall back")
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./... -run TestPool -v`
Expected: FAIL — `undefined: Pool`.

- [ ] **Step 4: Write minimal implementation**

Create `pool.go`:
```go
package chainpool

import (
	"context"
	"sort"
	"time"
)

type respClassifier func(resp *Response) errKind

// Pool routes requests across priority-ordered nodes for a single chain.
type Pool struct {
	chain         string
	nodes         []*node
	backoffCfg    BackoffConfig
	timeout       time.Duration
	hook          Hook
	clock         Clock
	fallbackCodes []int
}

func (p *Pool) FallbackCodes() []int { return p.fallbackCodes }

// Do routes a generic HTTP request with fallback and backoff.
func (p *Pool) Do(ctx context.Context, req Request) (*Response, error) {
	return p.doClassified(ctx, req, nil)
}

func (p *Pool) doClassified(ctx context.Context, req Request, rc respClassifier) (*Response, error) {
	if len(p.nodes) == 0 {
		return nil, ErrNoNodes
	}

	ordered := make([]*node, len(p.nodes))
	copy(ordered, p.nodes)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].priority > ordered[j].priority })

	deadline := p.clock.Now().Add(p.timeout)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}

	bo := newBackoff(p.backoffCfg)
	attempts := map[string]*NodeAttempt{}

	for {
		progressed := false
		for _, n := range ordered {
			if !n.brk.Allowed() {
				p.hook.OnFallback(n.name, "", "open")
				continue
			}
			if !n.lim.Allow() {
				p.hook.OnRateLimited(n.name)
				continue
			}

			progressed = true
			p.hook.OnRequest(n.name, req)
			start := p.clock.Now()
			resp, err := n.do(ctx, req)
			p.hook.OnResult(n.name, statusOf(resp), err, p.clock.Now().Sub(start))

			kind := classifyHTTP(resp, err)
			if kind == kindReturn && rc != nil && resp != nil {
				kind = rc(resp)
			}

			switch kind {
			case kindAuth:
				p.hook.OnAuthFailure(n.name)
				if n.brk.RecordFail(true) {
					p.hook.OnCircuitOpen(n.name)
				}
				recordAttempt(attempts, n.name, resp, err)
				p.hook.OnFallback(n.name, "", "auth")
				continue
			case kindNode:
				if n.brk.RecordFail(false) {
					p.hook.OnCircuitOpen(n.name)
				}
				recordAttempt(attempts, n.name, resp, err)
				p.hook.OnFallback(n.name, "", "node-error")
				continue
			default: // kindReturn
				if n.brk.RecordSuccess() {
					p.hook.OnCircuitClose(n.name)
				}
				return resp, nil
			}
		}

		if p.clock.Now().After(deadline) || p.clock.Now().Equal(deadline) {
			return nil, p.allFailed(attempts)
		}
		_ = progressed
		if err := p.clock.Sleep(ctx, bo.next()); err != nil {
			return nil, err
		}
	}
}

func (p *Pool) allFailed(attempts map[string]*NodeAttempt) error {
	out := make([]NodeAttempt, 0, len(attempts))
	for _, a := range attempts {
		out = append(out, *a)
	}
	return &AllFailedError{Chain: p.chain, Attempts: out}
}

// Stats returns a snapshot of node identity and breaker state.
func (p *Pool) Stats() []NodeStat {
	out := make([]NodeStat, 0, len(p.nodes))
	for _, n := range p.nodes {
		out = append(out, NodeStat{Name: n.name, Priority: n.priority, State: n.brk.State()})
	}
	return out
}

func statusOf(resp *Response) int {
	if resp == nil {
		return 0
	}
	return resp.StatusCode
}

func recordAttempt(m map[string]*NodeAttempt, name string, resp *Response, err error) {
	a, ok := m[name]
	if !ok {
		a = &NodeAttempt{Node: name}
		m[name] = a
	}
	a.LastStatus = statusOf(resp)
	a.LastErr = err
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./... -run TestPool -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add pool.go pool_test.go pooltest_test.go
git commit -m "feat: pool selection loop with priority, fallback, backoff"
```

---

### Task 11: JSON-RPC helper

**Files:**
- Create: `jsonrpc.go`
- Test: `jsonrpc_test.go`

**Interfaces:**
- Consumes: `Pool.doClassified`, `Pool.FallbackCodes`, `RPCError`, `Request`, `Response`, `errKind` (Tasks 4,10).
- Produces:
  - `type RPCClient struct { pool *Pool; id atomic.Int64; fallback map[int]bool }`
  - `func NewRPC(p *Pool) *RPCClient`
  - `func (c *RPCClient) Call(ctx context.Context, method string, params any) (json.RawMessage, error)` — builds the JSON-RPC body, routes through `doClassified` with a classifier that maps fallback error codes to `kindNode`, parses the envelope, returns `*RPCError` for non-fallback RPC errors, else the `result`.

- [ ] **Step 1: Write the failing test**

Create `jsonrpc_test.go`:
```go
package chainpool

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRPCCallSuccessReturnsResult(t *testing.T) {
	c := newFakeClock(time.Unix(0, 0))
	srv := newFakeServer(scriptedResponse{status: 200, body: `{"jsonrpc":"2.0","id":1,"result":"0x10"}`})
	defer srv.close()

	p := buildPool(c, &recordingHook{}, []int{-32603}, testNode("n", srv.url(), 100, c, 1000))
	rpc := NewRPC(p)
	res, err := rpc.Call(context.Background(), "eth_blockNumber", nil)
	if err != nil {
		t.Fatalf("Call err = %v", err)
	}
	if string(res) != `"0x10"` {
		t.Fatalf("result = %s", res)
	}
}

func TestRPCCallApplicationErrorReturnsRPCError(t *testing.T) {
	c := newFakeClock(time.Unix(0, 0))
	srv := newFakeServer(scriptedResponse{status: 200, body: `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"reverted"}}`})
	defer srv.close()
	other := newFakeServer(scriptedResponse{status: 200, body: `{"jsonrpc":"2.0","id":1,"result":"x"}`})
	defer other.close()

	p := buildPool(c, &recordingHook{}, []int{-32603}, // -32000 NOT in fallback set
		testNode("n", srv.url(), 100, c, 1000),
		testNode("other", other.url(), 50, c, 1000),
	)
	rpc := NewRPC(p)
	_, err := rpc.Call(context.Background(), "eth_call", nil)
	var re *RPCError
	if !errors.As(err, &re) || re.Code != -32000 {
		t.Fatalf("expected RPCError -32000, got %v", err)
	}
	if other.count() != 0 {
		t.Fatal("non-fallback RPC error must not fall back")
	}
}

func TestRPCCallFallbackCodeTriggersFallback(t *testing.T) {
	c := newFakeClock(time.Unix(0, 0))
	overloaded := newFakeServer(scriptedResponse{status: 200, body: `{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"overloaded"}}`})
	defer overloaded.close()
	good := newFakeServer(scriptedResponse{status: 200, body: `{"jsonrpc":"2.0","id":1,"result":"ok"}`})
	defer good.close()

	hook := &recordingHook{}
	p := buildPool(c, hook, []int{-32603},
		testNode("overloaded", overloaded.url(), 100, c, 1000),
		testNode("good", good.url(), 50, c, 1000),
	)
	rpc := NewRPC(p)
	res, err := rpc.Call(context.Background(), "eth_call", nil)
	if err != nil {
		t.Fatalf("Call err = %v", err)
	}
	if string(res) != `"ok"` {
		t.Fatalf("result = %s, expected fallback to good", res)
	}
	if len(hook.fallbacks) == 0 {
		t.Fatal("expected fallback event for -32603")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./... -run TestRPCCall -v`
Expected: FAIL — `undefined: NewRPC`.

- [ ] **Step 3: Write minimal implementation**

Create `jsonrpc.go`:
```go
package chainpool

import (
	"context"
	"encoding/json"
	"net/http"
	"sync/atomic"
)

type rpcRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      int64  `json:"id"`
	Method  string `json:"method"`
	Params  any    `json:"params"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int64           `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *RPCError       `json:"error"`
}

// RPCClient is a JSON-RPC convenience layer over a Pool.
type RPCClient struct {
	pool     *Pool
	id       atomic.Int64
	fallback map[int]bool
}

func NewRPC(p *Pool) *RPCClient {
	fb := make(map[int]bool, len(p.FallbackCodes()))
	for _, code := range p.FallbackCodes() {
		fb[code] = true
	}
	return &RPCClient{pool: p, fallback: fb}
}

// Call performs a single JSON-RPC request. It returns the raw result, an
// *RPCError for application-level errors, or a routing error.
func (c *RPCClient) Call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	body, err := json.Marshal(rpcRequest{
		JSONRPC: "2.0",
		ID:      c.id.Add(1),
		Method:  method,
		Params:  params,
	})
	if err != nil {
		return nil, err
	}
	req := Request{
		Method: http.MethodPost,
		Body:   body,
		Header: http.Header{"Content-Type": []string{"application/json"}},
	}

	classifier := func(resp *Response) errKind {
		var rr rpcResponse
		if err := json.Unmarshal(resp.Body, &rr); err != nil {
			return kindReturn // not JSON-RPC shaped; let caller deal with body
		}
		if rr.Error != nil && c.fallback[rr.Error.Code] {
			return kindNode
		}
		return kindReturn
	}

	resp, err := c.pool.doClassified(ctx, req, classifier)
	if err != nil {
		return nil, err
	}

	var rr rpcResponse
	if err := json.Unmarshal(resp.Body, &rr); err != nil {
		return nil, err
	}
	if rr.Error != nil {
		return nil, rr.Error
	}
	return rr.Result, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./... -run TestRPCCall -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add jsonrpc.go jsonrpc_test.go
git commit -m "feat: JSON-RPC helper with configurable fallback codes"
```

---

### Task 12: Manager (construction + routing)

**Files:**
- Create: `manager.go`
- Test: `manager_test.go`

**Interfaces:**
- Consumes: `Config`, `settings`, `Option`, `Pool`, `node`, `limiter`, `breaker`, errors (all earlier tasks).
- Produces:
  - `type Manager struct { pools map[string]*Pool; httpClient *http.Client }`
  - `func New(cfg Config, opts ...Option) (*Manager, error)` — merges `WithNode` extras into config, applies defaults, validates, builds a `Pool` per chain (each node gets limiter+breaker+shared http client+clock+hook).
  - `func (m *Manager) Pool(chain string) (*Pool, error)` — returns pool or `ErrUnknownChain`.
  - `func (m *Manager) Do(ctx context.Context, chain string, req Request) (*Response, error)`
  - `func (m *Manager) Close() error` — closes idle HTTP connections.

- [ ] **Step 1: Write the failing test**

Create `manager_test.go`:
```go
package chainpool

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNewBuildsPoolsAndRoutes(t *testing.T) {
	srv := newFakeServer(scriptedResponse{status: 200, body: "hello"})
	defer srv.close()

	cfg := Config{
		Defaults: Defaults{RateRPS: 100, Timeout: Duration(2 * time.Second)},
		Chains: map[string]ChainConfig{
			"ethereum": {
				Timeout: Duration(5 * time.Second),
				Nodes:   []NodeConfig{{Name: "n1", BaseURL: srv.url(), Priority: 100, RateRPS: 100}},
			},
		},
	}
	m, err := New(cfg, WithClock(newFakeClock(time.Unix(0, 0))))
	if err != nil {
		t.Fatalf("New err = %v", err)
	}
	defer m.Close()

	resp, err := m.Do(context.Background(), "ethereum", Request{Method: "GET"})
	if err != nil {
		t.Fatalf("Do err = %v", err)
	}
	if string(resp.Body) != "hello" {
		t.Fatalf("body = %q", resp.Body)
	}
}

func TestManagerUnknownChain(t *testing.T) {
	m, err := New(Config{Chains: map[string]ChainConfig{}})
	if err != nil {
		t.Fatalf("New err = %v", err)
	}
	_, err = m.Pool("nope")
	if !errors.Is(err, ErrUnknownChain) {
		t.Fatalf("err = %v, want ErrUnknownChain", err)
	}
}

func TestNewMergesWithNodeOption(t *testing.T) {
	srv := newFakeServer(scriptedResponse{status: 200, body: "x"})
	defer srv.close()
	cfg := Config{
		Defaults: Defaults{RateRPS: 10, Timeout: Duration(time.Second)},
		Chains:   map[string]ChainConfig{"ethereum": {Nodes: []NodeConfig{}}},
	}
	m, err := New(cfg,
		WithClock(newFakeClock(time.Unix(0, 0))),
		WithNode("ethereum", NodeConfig{Name: "extra", BaseURL: srv.url(), Priority: 1, RateRPS: 10}),
	)
	if err != nil {
		t.Fatalf("New err = %v", err)
	}
	defer m.Close()
	p, _ := m.Pool("ethereum")
	if len(p.Stats()) != 1 || p.Stats()[0].Name != "extra" {
		t.Fatalf("WithNode not merged: %+v", p.Stats())
	}
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	cfg := Config{Chains: map[string]ChainConfig{
		"x": {Nodes: []NodeConfig{{Name: "a", BaseURL: "://bad", RateRPS: 1}}},
	}}
	if _, err := New(cfg); err == nil {
		t.Fatal("expected validation error from New")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./... -run "TestNew|TestManager" -v`
Expected: FAIL — `undefined: New`.

- [ ] **Step 3: Write minimal implementation**

Create `manager.go`:
```go
package chainpool

import (
	"context"
	"net/http"
)

// Manager holds one Pool per configured chain.
type Manager struct {
	pools      map[string]*Pool
	httpClient *http.Client
}

func New(cfg Config, opts ...Option) (*Manager, error) {
	s := defaultSettings()
	for _, opt := range opts {
		opt(s)
	}

	// Merge programmatic nodes into config before defaults/validation.
	if cfg.Chains == nil {
		cfg.Chains = map[string]ChainConfig{}
	}
	for chain, extra := range s.extraNodes {
		cc := cfg.Chains[chain]
		cc.Nodes = append(cc.Nodes, extra...)
		cfg.Chains[chain] = cc
	}

	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	m := &Manager{pools: map[string]*Pool{}, httpClient: s.httpClient}
	for chain, cc := range cfg.Chains {
		nodes := make([]*node, 0, len(cc.Nodes))
		for _, nc := range cc.Nodes {
			nodes = append(nodes, &node{
				name:     nc.Name,
				baseURL:  nc.BaseURL,
				priority: nc.Priority,
				headers:  nc.Headers,
				timeout:  nc.Timeout.D(),
				client:   s.httpClient,
				lim:      newLimiter(nc.RateRPS, nc.Burst, s.clock),
				brk: newBreaker(
					nc.Breaker.FailThreshold, nc.Breaker.Cooldown.D(),
					nc.Breaker.AuthFailThreshold, nc.Breaker.AuthCooldown.D(),
					s.clock,
				),
			})
		}
		m.pools[chain] = &Pool{
			chain:         chain,
			nodes:         nodes,
			backoffCfg:    cc.Backoff,
			timeout:       cc.Timeout.D(),
			hook:          s.hook,
			clock:         s.clock,
			fallbackCodes: cfg.Defaults.FallbackRPCCodes,
		}
	}
	return m, nil
}

func (m *Manager) Pool(chain string) (*Pool, error) {
	p, ok := m.pools[chain]
	if !ok {
		return nil, ErrUnknownChain
	}
	return p, nil
}

func (m *Manager) Do(ctx context.Context, chain string, req Request) (*Response, error) {
	p, err := m.Pool(chain)
	if err != nil {
		return nil, err
	}
	return p.Do(ctx, req)
}

func (m *Manager) Close() error {
	m.httpClient.CloseIdleConnections()
	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./... -run "TestNew|TestManager" -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add manager.go manager_test.go
git commit -m "feat: Manager construction and chain routing"
```

---

### Task 13: Pool timeout edge — zero timeout default

**Files:**
- Modify: `pool.go` (guard `timeout <= 0`)
- Test: `pool_test.go` (add case)

**Interfaces:**
- Consumes/Produces: no signature change; behavior fix only — when a pool's `timeout` is 0 (unset), default it to a sane bound so `Do` cannot loop forever.

- [ ] **Step 1: Write the failing test**

Append to `pool_test.go`:
```go
func TestPoolZeroTimeoutStillBounded(t *testing.T) {
	c := newFakeClock(time.Unix(0, 0))
	bad := newFakeServer(scriptedResponse{status: 500, body: "x"})
	defer bad.close()

	p := buildPool(c, &recordingHook{}, nil, testNode("a", bad.url(), 100, c, 1000))
	p.timeout = 0 // unset

	_, err := p.Do(context.Background(), Request{Method: "GET"})
	if !errors.Is(err, ErrAllNodesUnavailable) {
		t.Fatalf("expected bounded failure, got %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./... -run TestPoolZeroTimeout -v`
Expected: FAIL or hang (loop never terminates with zero deadline). If it hangs, that confirms the bug.

- [ ] **Step 3: Write minimal implementation**

In `pool.go`, inside `doClassified`, replace the deadline block:
```go
	timeout := p.timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	deadline := p.clock.Now().Add(timeout)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./... -run TestPool -v`
Expected: PASS (all pool tests).

- [ ] **Step 5: Commit**

```bash
git add pool.go pool_test.go
git commit -m "fix: default pool timeout so Do is always bounded"
```

---

### Task 14: Runnable examples + full suite gate

**Files:**
- Create: `example_test.go`
- Create: `README.md`

**Interfaces:**
- Consumes: public API (`New`, `Manager.Do`, `NewRPC`, `RPCClient.Call`).
- Produces: godoc examples `ExampleNew` (compiles + runs against a local server, deterministic output) and a usage `README.md`.

- [ ] **Step 1: Write the example test**

Create `example_test.go`:
```go
package chainpool_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/alimovahedi/chainpool"
)

func ExampleNew() {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`))
	}))
	defer srv.Close()

	m, err := chainpool.New(chainpool.Config{
		Defaults: chainpool.Defaults{RateRPS: 50, Timeout: chainpool.Duration(5 * time.Second)},
		Chains: map[string]chainpool.ChainConfig{
			"ethereum": {Nodes: []chainpool.NodeConfig{
				{Name: "local", BaseURL: srv.URL, Priority: 100, RateRPS: 50},
			}},
		},
	})
	if err != nil {
		panic(err)
	}
	defer m.Close()

	resp, err := m.Do(context.Background(), "ethereum", chainpool.Request{Method: http.MethodGet})
	if err != nil {
		panic(err)
	}
	fmt.Println(resp.StatusCode)
	// Output: 200
}
```

- [ ] **Step 2: Run the example**

Run: `go test ./... -run ExampleNew -v`
Expected: PASS (output matches `200`).

- [ ] **Step 3: Write README**

Create `README.md`:
```markdown
# chainpool

Route blockchain RPC requests across multiple public nodes per chain with
per-node rate limiting, priority, circuit-breaker fallback, and a JSON-RPC
helper. Works for EVM, Solana, TON, and Tron via a generic HTTP transport.

## Install

    go get github.com/alimovahedi/chainpool

## Quick start

```go
m, _ := chainpool.New(cfg)            // cfg from struct or chainpool.LoadConfig("nodes.yaml")
defer m.Close()

resp, err := m.Do(ctx, "ethereum", chainpool.Request{
    Method: http.MethodPost,
    Body:   body,
})

// JSON-RPC helper
pool, _ := m.Pool("ethereum")
rpc := chainpool.NewRPC(pool)
result, err := rpc.Call(ctx, "eth_blockNumber", nil)
```

## Behavior

- Nodes tried highest-priority first.
- Saturated node (rate limit) is skipped, not waited on.
- Node errors (network, 429, 5xx, 401/403) fall back to the next node.
- 401/403 trips the node's breaker immediately (revoked key) with a long cooldown.
- Valid JSON-RPC application errors return to the caller; configurable error
  codes (default `-32603`) are treated as node errors and fall back.
- When all nodes are exhausted, requests back off until the pool timeout, then
  return `AllFailedError` (unwraps to `ErrAllNodesUnavailable`).

See `docs/superpowers/specs/2026-06-29-chainpool-design.md` for the full design.
```

- [ ] **Step 4: Run the full suite with race + coverage**

Run:
```bash
go vet ./...
go test ./... -race -cover
```
Expected: all PASS; coverage ≥ 85%.

- [ ] **Step 5: Commit**

```bash
git add example_test.go README.md
git commit -m "docs: runnable example and README; verify full suite"
```

---

## Self-Review

**Spec coverage:**
- Transparent transport + generic HTTP → Tasks 4, 6. ✅
- JSON-RPC helper + fallback codes → Task 11. ✅
- Rate limit (rps token bucket, skip when saturated) → Tasks 2, 10. ✅
- All-exhausted backoff + timeout error → Tasks 9, 10, 13. ✅
- Error classification (node vs RPC, 401/403 auth) → Tasks 4, 10. ✅
- Circuit breaker (states + auth fast-trip) → Task 3, wired in 10. ✅
- Config struct + YAML/JSON loader + env interpolation + defaults/validation → Task 7. ✅
- Manager + named pools → Task 12. ✅
- Logger + metrics hooks → Tasks 5, wired in 10. ✅
- Global config only, ctx honored → Tasks 6, 10. ✅
- Testing plan (fake server, fake clock, recording hook, examples, race/coverage) → Tasks 1, 3, 10, 14. ✅

**Placeholder scan:** none — every code step contains complete code.

**Type consistency:** `Pool.doClassified`/`respClassifier` defined in Task 10, consumed in Task 11; `FallbackCodes()` produced in Task 10, consumed in Task 11; `recordingHook`/`NopHook` from Task 5 reused across Tasks 10–12; `fakeServer`/`testNode`/`buildPool` from Task 10 reused in Tasks 11–12; `Duration`/config types from Task 7 used in Tasks 8, 12, 14. Consistent.
