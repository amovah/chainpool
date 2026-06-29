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
