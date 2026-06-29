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
