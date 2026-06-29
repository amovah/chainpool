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
