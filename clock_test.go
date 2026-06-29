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
