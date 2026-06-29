package chainpool

import (
	"net/http"
	"testing"
)

func TestOptionsApply(t *testing.T) {
	s := defaultSettings()
	h := &recordingHook{}
	hc := &http.Client{}
	WithHook(h)(s)
	WithLogger(NopLogger{})(s)
	WithHTTPClient(hc)(s)
	WithNode("ethereum", NodeConfig{Name: "extra", BaseURL: "https://x", RateRPS: 1})(s)

	if s.hook != h {
		t.Fatal("WithHook not applied")
	}
	if s.httpClient != hc {
		t.Fatal("WithHTTPClient not applied")
	}
	if len(s.extraNodes["ethereum"]) != 1 {
		t.Fatal("WithNode not applied")
	}
}

func TestDefaultSettingsAreNonNil(t *testing.T) {
	s := defaultSettings()
	if s.hook == nil || s.logger == nil || s.clock == nil || s.httpClient == nil {
		t.Fatal("defaults must be non-nil")
	}
}
