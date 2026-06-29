package chainpool

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration parses Go duration strings ("10s", "500ms") from YAML and JSON.
type Duration time.Duration

func (d Duration) D() time.Duration { return time.Duration(d) }

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	return d.parse(s)
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	return d.parse(s)
}

func (d *Duration) parse(s string) error {
	if s == "" {
		*d = 0
		return nil
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("chainpool: invalid duration %q: %w", s, err)
	}
	*d = Duration(parsed)
	return nil
}

type Config struct {
	Chains   map[string]ChainConfig `yaml:"chains" json:"chains"`
	Defaults Defaults               `yaml:"defaults" json:"defaults"`
}

type ChainConfig struct {
	Nodes   []NodeConfig  `yaml:"nodes" json:"nodes"`
	Timeout Duration      `yaml:"timeout" json:"timeout"`
	Backoff BackoffConfig `yaml:"backoff" json:"backoff"`
}

type NodeConfig struct {
	Name     string            `yaml:"name" json:"name"`
	BaseURL  string            `yaml:"url" json:"url"`
	Priority int               `yaml:"priority" json:"priority"`
	RateRPS  float64           `yaml:"rate_rps" json:"rate_rps"`
	Burst    int               `yaml:"burst" json:"burst"`
	Timeout  Duration          `yaml:"timeout" json:"timeout"`
	Headers  map[string]string `yaml:"headers" json:"headers"`
	Breaker  BreakerConfig     `yaml:"breaker" json:"breaker"`
}

type BreakerConfig struct {
	FailThreshold     int      `yaml:"fail_threshold" json:"fail_threshold"`
	Cooldown          Duration `yaml:"cooldown" json:"cooldown"`
	AuthFailThreshold int      `yaml:"auth_fail_threshold" json:"auth_fail_threshold"`
	AuthCooldown      Duration `yaml:"auth_cooldown" json:"auth_cooldown"`
}

type BackoffConfig struct {
	Initial Duration `yaml:"initial" json:"initial"`
	Max     Duration `yaml:"max" json:"max"`
	Factor  float64  `yaml:"factor" json:"factor"`
}

type Defaults struct {
	RateRPS          float64       `yaml:"rate_rps" json:"rate_rps"`
	Burst            int           `yaml:"burst" json:"burst"`
	Timeout          Duration      `yaml:"timeout" json:"timeout"`
	Breaker          BreakerConfig `yaml:"breaker" json:"breaker"`
	Backoff          BackoffConfig `yaml:"backoff" json:"backoff"`
	FallbackRPCCodes []int         `yaml:"fallback_rpc_codes" json:"fallback_rpc_codes"`
}

func LoadConfig(path string) (Config, error) {
	var cfg Config
	raw, err := os.ReadFile(path)
	if err != nil {
		return cfg, fmt.Errorf("chainpool: read config: %w", err)
	}
	expanded := os.Expand(string(raw), os.Getenv)

	switch strings.ToLower(filepath.Ext(path)) {
	case ".yaml", ".yml":
		if err := yaml.Unmarshal([]byte(expanded), &cfg); err != nil {
			return cfg, fmt.Errorf("chainpool: parse yaml: %w", err)
		}
	case ".json":
		if err := json.Unmarshal([]byte(expanded), &cfg); err != nil {
			return cfg, fmt.Errorf("chainpool: parse json: %w", err)
		}
	default:
		return cfg, fmt.Errorf("chainpool: unsupported config extension %q", filepath.Ext(path))
	}

	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func (c *Config) applyDefaults() {
	d := c.Defaults
	if len(d.FallbackRPCCodes) == 0 {
		d.FallbackRPCCodes = []int{-32603}
		c.Defaults = d
	}
	for cname, chain := range c.Chains {
		if chain.Backoff == (BackoffConfig{}) {
			chain.Backoff = d.Backoff
		}
		for i := range chain.Nodes {
			n := &chain.Nodes[i]
			if n.RateRPS == 0 {
				n.RateRPS = d.RateRPS
			}
			if n.Burst == 0 {
				n.Burst = d.Burst
			}
			if n.Timeout == 0 {
				n.Timeout = d.Timeout
			}
			if n.Breaker == (BreakerConfig{}) {
				n.Breaker = d.Breaker
			}
		}
		c.Chains[cname] = chain
	}
}

func (c *Config) validate() error {
	for cname, chain := range c.Chains {
		seen := map[string]bool{}
		for _, n := range chain.Nodes {
			if seen[n.Name] {
				return fmt.Errorf("chainpool: chain %q has duplicate node name %q", cname, n.Name)
			}
			seen[n.Name] = true
			u, err := url.Parse(n.BaseURL)
			if err != nil || u.Scheme == "" || u.Host == "" {
				return fmt.Errorf("chainpool: chain %q node %q has invalid url %q", cname, n.Name, n.BaseURL)
			}
			if n.RateRPS <= 0 {
				return fmt.Errorf("chainpool: chain %q node %q must have rate_rps > 0", cname, n.Name)
			}
		}
	}
	return nil
}
