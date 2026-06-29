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
