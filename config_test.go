package chainpool

import (
	"strings"
	"testing"
	"time"
)

func TestDurationParsesYAMLAndJSON(t *testing.T) {
	cfg, err := LoadConfig("testdata/valid.yaml")
	if err != nil {
		t.Fatalf("load yaml: %v", err)
	}
	if cfg.Chains["ethereum"].Timeout.D() != 10*time.Second {
		t.Fatalf("timeout = %v", cfg.Chains["ethereum"].Timeout.D())
	}
}

func TestLoadConfigEnvInterpolation(t *testing.T) {
	t.Setenv("TEST_ALCHEMY_KEY", "secret123")
	cfg, err := LoadConfig("testdata/valid.yaml")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	eth := cfg.Chains["ethereum"]
	if got := eth.Nodes[0].Headers["Authorization"]; got != "Bearer secret123" {
		t.Fatalf("interpolation failed: %q", got)
	}
}

func TestApplyDefaultsFillsNodeFields(t *testing.T) {
	cfg, err := LoadConfig("testdata/valid.yaml")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	ankr := cfg.Chains["ethereum"].Nodes[1] // only name/url/priority set
	if ankr.RateRPS != 10 {
		t.Fatalf("default rate_rps not applied: %v", ankr.RateRPS)
	}
	if ankr.Breaker.FailThreshold != 5 {
		t.Fatalf("default breaker not applied: %+v", ankr.Breaker)
	}
}

func TestLoadConfigJSON(t *testing.T) {
	cfg, err := LoadConfig("testdata/valid.json")
	if err != nil {
		t.Fatalf("load json: %v", err)
	}
	if cfg.Chains["solana"].Nodes[0].Name != "sol" {
		t.Fatal("json node not parsed")
	}
}

func TestValidateRejectsDuplicateNodeNames(t *testing.T) {
	cfg := Config{Chains: map[string]ChainConfig{
		"x": {Nodes: []NodeConfig{
			{Name: "a", BaseURL: "https://a", RateRPS: 1},
			{Name: "a", BaseURL: "https://b", RateRPS: 1},
		}},
	}}
	if err := cfg.validate(); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("expected duplicate error, got %v", err)
	}
}

// TestApplyDefaultsBreakerHardcodedDefaults verifies that applyDefaults
// establishes spec-mandated breaker defaults even when no Defaults block is
// supplied, and that a node with a partial BreakerConfig inherits the missing
// fields rather than keeping zero values (partial-config gap).
func TestApplyDefaultsBreakerHardcodedDefaults(t *testing.T) {
	// Case 1: completely empty Defaults; node with no breaker config.
	cfg := Config{Chains: map[string]ChainConfig{
		"x": {Nodes: []NodeConfig{{Name: "a", BaseURL: "https://a", RateRPS: 1}}},
	}}
	cfg.applyDefaults()
	n := cfg.Chains["x"].Nodes[0]
	if n.Breaker.FailThreshold != 5 {
		t.Errorf("case1 FailThreshold = %d, want 5", n.Breaker.FailThreshold)
	}
	if n.Breaker.Cooldown != Duration(30*time.Second) {
		t.Errorf("case1 Cooldown = %v, want 30s", n.Breaker.Cooldown)
	}
	if n.Breaker.AuthFailThreshold != 1 {
		t.Errorf("case1 AuthFailThreshold = %d, want 1", n.Breaker.AuthFailThreshold)
	}
	if n.Breaker.AuthCooldown != Duration(5*time.Minute) {
		t.Errorf("case1 AuthCooldown = %v, want 5m", n.Breaker.AuthCooldown)
	}

	// Case 2: node with only FailThreshold=9 set must inherit Cooldown and
	// AuthCooldown from the hardcoded defaults (partial-config gap closed).
	cfg2 := Config{Chains: map[string]ChainConfig{
		"x": {Nodes: []NodeConfig{{
			Name: "a", BaseURL: "https://a", RateRPS: 1,
			Breaker: BreakerConfig{FailThreshold: 9},
		}}},
	}}
	cfg2.applyDefaults()
	n2 := cfg2.Chains["x"].Nodes[0]
	if n2.Breaker.FailThreshold != 9 {
		t.Errorf("case2 FailThreshold = %d, want 9 (node value preserved)", n2.Breaker.FailThreshold)
	}
	if n2.Breaker.Cooldown != Duration(30*time.Second) {
		t.Errorf("case2 Cooldown = %v, want 30s", n2.Breaker.Cooldown)
	}
	if n2.Breaker.AuthCooldown != Duration(5*time.Minute) {
		t.Errorf("case2 AuthCooldown = %v, want 5m", n2.Breaker.AuthCooldown)
	}
}

func TestValidateRejectsBadURLAndRPS(t *testing.T) {
	bad := Config{Chains: map[string]ChainConfig{
		"x": {Nodes: []NodeConfig{{Name: "a", BaseURL: "://nope", RateRPS: 1}}},
	}}
	if err := bad.validate(); err == nil {
		t.Fatal("expected bad url error")
	}
	zero := Config{Chains: map[string]ChainConfig{
		"x": {Nodes: []NodeConfig{{Name: "a", BaseURL: "https://a", RateRPS: 0}}},
	}}
	if err := zero.validate(); err == nil {
		t.Fatal("expected rps>0 error")
	}
}
