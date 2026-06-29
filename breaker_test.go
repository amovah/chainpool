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
