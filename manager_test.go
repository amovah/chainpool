package chainpool

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNewBuildsPoolsAndRoutes(t *testing.T) {
	srv := newFakeServer(scriptedResponse{status: 200, body: "hello"})
	defer srv.close()

	cfg := Config{
		Defaults: Defaults{RateRPS: 100, Timeout: Duration(2 * time.Second)},
		Chains: map[string]ChainConfig{
			"ethereum": {
				Timeout: Duration(5 * time.Second),
				Nodes:   []NodeConfig{{Name: "n1", BaseURL: srv.url(), Priority: 100, RateRPS: 100}},
			},
		},
	}
	m, err := New(cfg, WithClock(newFakeClock(time.Unix(0, 0))))
	if err != nil {
		t.Fatalf("New err = %v", err)
	}
	defer m.Close()

	resp, err := m.Do(context.Background(), "ethereum", Request{Method: "GET"})
	if err != nil {
		t.Fatalf("Do err = %v", err)
	}
	if string(resp.Body) != "hello" {
		t.Fatalf("body = %q", resp.Body)
	}
}

func TestManagerUnknownChain(t *testing.T) {
	m, err := New(Config{Chains: map[string]ChainConfig{}})
	if err != nil {
		t.Fatalf("New err = %v", err)
	}
	_, err = m.Pool("nope")
	if !errors.Is(err, ErrUnknownChain) {
		t.Fatalf("err = %v, want ErrUnknownChain", err)
	}
}

func TestNewMergesWithNodeOption(t *testing.T) {
	srv := newFakeServer(scriptedResponse{status: 200, body: "x"})
	defer srv.close()
	cfg := Config{
		Defaults: Defaults{RateRPS: 10, Timeout: Duration(time.Second)},
		Chains:   map[string]ChainConfig{"ethereum": {Nodes: []NodeConfig{}}},
	}
	m, err := New(cfg,
		WithClock(newFakeClock(time.Unix(0, 0))),
		WithNode("ethereum", NodeConfig{Name: "extra", BaseURL: srv.url(), Priority: 1, RateRPS: 10}),
	)
	if err != nil {
		t.Fatalf("New err = %v", err)
	}
	defer m.Close()
	p, _ := m.Pool("ethereum")
	if len(p.Stats()) != 1 || p.Stats()[0].Name != "extra" {
		t.Fatalf("WithNode not merged: %+v", p.Stats())
	}
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	cfg := Config{Chains: map[string]ChainConfig{
		"x": {Nodes: []NodeConfig{{Name: "a", BaseURL: "://bad", RateRPS: 1}}},
	}}
	if _, err := New(cfg); err == nil {
		t.Fatal("expected validation error from New")
	}
}
