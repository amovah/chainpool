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
