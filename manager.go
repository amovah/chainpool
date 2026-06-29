package chainpool

import (
	"context"
	"net/http"
)

// Manager holds one Pool per configured chain.
type Manager struct {
	pools      map[string]*Pool
	httpClient *http.Client
}

func New(cfg Config, opts ...Option) (*Manager, error) {
	s := defaultSettings()
	for _, opt := range opts {
		opt(s)
	}

	// Merge programmatic nodes into config before defaults/validation.
	if cfg.Chains == nil {
		cfg.Chains = map[string]ChainConfig{}
	}
	for chain, extra := range s.extraNodes {
		cc := cfg.Chains[chain]
		cc.Nodes = append(cc.Nodes, extra...)
		cfg.Chains[chain] = cc
	}

	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	m := &Manager{pools: map[string]*Pool{}, httpClient: s.httpClient}
	for chain, cc := range cfg.Chains {
		nodes := make([]*node, 0, len(cc.Nodes))
		for _, nc := range cc.Nodes {
			nodes = append(nodes, &node{
				name:     nc.Name,
				baseURL:  nc.BaseURL,
				priority: nc.Priority,
				headers:  nc.Headers,
				timeout:  nc.Timeout.D(),
				client:   s.httpClient,
				lim:      newLimiter(nc.RateRPS, nc.Burst, s.clock),
				brk: newBreaker(
					nc.Breaker.FailThreshold, nc.Breaker.Cooldown.D(),
					nc.Breaker.AuthFailThreshold, nc.Breaker.AuthCooldown.D(),
					s.clock,
				),
			})
		}
		m.pools[chain] = &Pool{
			chain:         chain,
			nodes:         nodes,
			backoffCfg:    cc.Backoff,
			timeout:       cc.Timeout.D(),
			hook:          s.hook,
			clock:         s.clock,
			fallbackCodes: cfg.Defaults.FallbackRPCCodes,
		}
	}
	return m, nil
}

func (m *Manager) Pool(chain string) (*Pool, error) {
	p, ok := m.pools[chain]
	if !ok {
		return nil, ErrUnknownChain
	}
	return p, nil
}

func (m *Manager) Do(ctx context.Context, chain string, req Request) (*Response, error) {
	p, err := m.Pool(chain)
	if err != nil {
		return nil, err
	}
	return p.Do(ctx, req)
}

func (m *Manager) Close() error {
	m.httpClient.CloseIdleConnections()
	return nil
}
