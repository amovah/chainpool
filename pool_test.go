package chainpool

import (
	"context"
	"errors"
	"testing"
	"time"
)

// cancelOnResultHook cancels the provided context after the first OnResult call.
// Used to simulate context cancellation between the HTTP attempt and the backoff Sleep.
type cancelOnResultHook struct {
	NopHook
	cancel context.CancelFunc
	fired  bool
}

func (h *cancelOnResultHook) OnResult(node string, status int, err error, latency time.Duration) {
	if !h.fired {
		h.fired = true
		h.cancel()
	}
}

// cancelOnRequestHook cancels the provided context the first time OnRequest is
// called. Used to simulate parent-context cancellation arriving during the
// in-flight attempt on the first node.
type cancelOnRequestHook struct {
	NopHook
	cancel context.CancelFunc
	fired  bool
}

func (h *cancelOnRequestHook) OnRequest(node string, req Request) {
	if !h.fired {
		h.fired = true
		h.cancel()
	}
}

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

// TestDoClassifiedClassifierUpgradesKindReturnToKindNode verifies that a
// respClassifier returning kindNode for an HTTP 200 causes fallback to the
// secondary node, and that the secondary's response is returned.
func TestDoClassifiedClassifierUpgradesKindReturnToKindNode(t *testing.T) {
	c := newFakeClock(time.Unix(0, 0))
	primary := newFakeServer(scriptedResponse{status: 200, body: "primary-body"})
	defer primary.close()
	secondary := newFakeServer(scriptedResponse{status: 200, body: "secondary-body"})
	defer secondary.close()

	hook := &recordingHook{}
	p := buildPool(c, hook, nil,
		testNode("primary", primary.url(), 100, c, 1000),
		testNode("secondary", secondary.url(), 50, c, 1000),
	)

	// Classifier upgrades kindReturn (200) to kindNode only for the primary,
	// forcing fallback to the secondary which is allowed to return normally.
	alwaysNode := respClassifier(func(resp *Response) errKind {
		if resp.Node == "primary" {
			return kindNode
		}
		return kindReturn
	})

	resp, err := p.doClassified(context.Background(), Request{Method: "GET"}, alwaysNode)
	if err != nil {
		t.Fatalf("doClassified err = %v", err)
	}
	if resp.Node != "secondary" {
		t.Fatalf("served by %q, want secondary", resp.Node)
	}
	if primary.count() != 1 {
		t.Fatalf("primary hit count = %d, want 1", primary.count())
	}
	if secondary.count() != 1 {
		t.Fatalf("secondary hit count = %d, want 1", secondary.count())
	}
}

// TestPoolEmptyReturnsErrNoNodes verifies that Do returns ErrNoNodes immediately
// when the pool has no nodes.
func TestPoolEmptyReturnsErrNoNodes(t *testing.T) {
	c := newFakeClock(time.Unix(0, 0))
	p := buildPool(c, &recordingHook{}, nil)
	_, err := p.Do(context.Background(), Request{Method: "GET"})
	if !errors.Is(err, ErrNoNodes) {
		t.Fatalf("err = %v, want ErrNoNodes", err)
	}
}

// TestPoolContextCancelledDuringBackoff verifies that cancelling the context
// between the failed attempt and the backoff sleep causes Do to return
// context.Canceled rather than continuing to retry.
func TestPoolContextCancelledDuringBackoff(t *testing.T) {
	c := newFakeClock(time.Unix(0, 0))
	bad := newFakeServer(scriptedResponse{status: 500, body: "err"})
	defer bad.close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// cancelOnResultHook cancels ctx after the first OnResult so the
	// subsequent fakeClock.Sleep returns context.Canceled.
	hook := &cancelOnResultHook{cancel: cancel}
	p := buildPool(c, hook, nil,
		testNode("bad", bad.url(), 100, c, 1000),
	)
	p.timeout = 10 * time.Minute // long enough not to expire naturally

	_, err := p.Do(ctx, Request{Method: "GET"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

// TestPoolStableSortTriesPrimaryFirst verifies that two nodes with equal
// priority are tried in slice order (stable sort preserves insertion order).
func TestPoolStableSortTriesPrimaryFirst(t *testing.T) {
	c := newFakeClock(time.Unix(0, 0))
	first := newFakeServer(scriptedResponse{status: 200, body: "first"})
	defer first.close()
	second := newFakeServer(scriptedResponse{status: 200, body: "second"})
	defer second.close()

	hook := &recordingHook{}
	// Both nodes have equal priority; stable sort must preserve slice order.
	p := buildPool(c, hook, nil,
		testNode("first", first.url(), 50, c, 1000),
		testNode("second", second.url(), 50, c, 1000),
	)
	resp, err := p.Do(context.Background(), Request{Method: "GET"})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if resp.Node != "first" {
		t.Fatalf("served by %q, want first (stable sort)", resp.Node)
	}
	if second.count() != 0 {
		t.Fatal("second node should not be touched when both have equal priority")
	}
}

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

// TestPoolParentContextCancelledDuringAttempt verifies that when the parent
// context is cancelled while a node attempt is in flight, doClassified returns
// context.Canceled immediately — without hitting the secondary node and without
// leaving the primary's circuit breaker OPEN.
func TestPoolParentContextCancelledDuringAttempt(t *testing.T) {
	c := newFakeClock(time.Unix(0, 0))
	primary := newFakeServer(scriptedResponse{status: 200, body: "primary"})
	defer primary.close()
	secondary := newFakeServer(scriptedResponse{status: 200, body: "secondary"})
	defer secondary.close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// cancelOnRequestHook cancels ctx as soon as the first OnRequest fires,
	// so n.do runs with an already-cancelled parent context.
	hook := &cancelOnRequestHook{cancel: cancel}
	p := buildPool(c, hook, nil,
		testNode("primary", primary.url(), 100, c, 1000),
		testNode("secondary", secondary.url(), 50, c, 1000),
	)
	p.timeout = 10 * time.Minute // far future — must not expire naturally

	_, err := p.Do(ctx, Request{Method: "GET"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if secondary.count() != 0 {
		t.Fatalf("secondary hit count = %d, want 0 (must not be reached after parent cancel)", secondary.count())
	}
	// Primary breaker must not be OPEN — RecordFail must not have been called.
	for _, stat := range p.Stats() {
		if stat.Name == "primary" && stat.State == "open" {
			t.Fatal("primary breaker must not be OPEN after parent context cancellation")
		}
	}
}
