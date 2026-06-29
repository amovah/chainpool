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
