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
