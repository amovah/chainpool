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
