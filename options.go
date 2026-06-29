package chainpool

import "net/http"

type settings struct {
	hook       Hook
	logger     Logger
	clock      Clock
	httpClient *http.Client
	extraNodes map[string][]NodeConfig
}

func defaultSettings() *settings {
	return &settings{
		hook:       NopHook{},
		logger:     NopLogger{},
		clock:      newRealClock(),
		httpClient: &http.Client{},
		extraNodes: map[string][]NodeConfig{},
	}
}

// Option configures a Manager at construction time.
type Option func(*settings)

func WithHook(h Hook) Option { return func(s *settings) { s.hook = h } }

func WithLogger(l Logger) Option { return func(s *settings) { s.logger = l } }

// WithClock injects a clock (test seam).
func WithClock(c Clock) Option { return func(s *settings) { s.clock = c } }

func WithHTTPClient(hc *http.Client) Option { return func(s *settings) { s.httpClient = hc } }

// WithNode appends a node to a chain programmatically.
func WithNode(chain string, n NodeConfig) Option {
	return func(s *settings) { s.extraNodes[chain] = append(s.extraNodes[chain], n) }
}
