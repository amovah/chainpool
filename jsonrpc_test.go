package chainpool

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRPCCallSuccessReturnsResult(t *testing.T) {
	c := newFakeClock(time.Unix(0, 0))
	srv := newFakeServer(scriptedResponse{status: 200, body: `{"jsonrpc":"2.0","id":1,"result":"0x10"}`})
	defer srv.close()

	p := buildPool(c, &recordingHook{}, []int{-32603}, testNode("n", srv.url(), 100, c, 1000))
	rpc := NewRPC(p)
	res, err := rpc.Call(context.Background(), "eth_blockNumber", nil)
	if err != nil {
		t.Fatalf("Call err = %v", err)
	}
	if string(res) != `"0x10"` {
		t.Fatalf("result = %s", res)
	}
}

func TestRPCCallApplicationErrorReturnsRPCError(t *testing.T) {
	c := newFakeClock(time.Unix(0, 0))
	srv := newFakeServer(scriptedResponse{status: 200, body: `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"reverted"}}`})
	defer srv.close()
	other := newFakeServer(scriptedResponse{status: 200, body: `{"jsonrpc":"2.0","id":1,"result":"x"}`})
	defer other.close()

	p := buildPool(c, &recordingHook{}, []int{-32603}, // -32000 NOT in fallback set
		testNode("n", srv.url(), 100, c, 1000),
		testNode("other", other.url(), 50, c, 1000),
	)
	rpc := NewRPC(p)
	_, err := rpc.Call(context.Background(), "eth_call", nil)
	var re *RPCError
	if !errors.As(err, &re) || re.Code != -32000 {
		t.Fatalf("expected RPCError -32000, got %v", err)
	}
	if other.count() != 0 {
		t.Fatal("non-fallback RPC error must not fall back")
	}
}

func TestRPCCallFallbackCodeTriggersFallback(t *testing.T) {
	c := newFakeClock(time.Unix(0, 0))
	overloaded := newFakeServer(scriptedResponse{status: 200, body: `{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"overloaded"}}`})
	defer overloaded.close()
	good := newFakeServer(scriptedResponse{status: 200, body: `{"jsonrpc":"2.0","id":1,"result":"ok"}`})
	defer good.close()

	hook := &recordingHook{}
	p := buildPool(c, hook, []int{-32603},
		testNode("overloaded", overloaded.url(), 100, c, 1000),
		testNode("good", good.url(), 50, c, 1000),
	)
	rpc := NewRPC(p)
	res, err := rpc.Call(context.Background(), "eth_call", nil)
	if err != nil {
		t.Fatalf("Call err = %v", err)
	}
	if string(res) != `"ok"` {
		t.Fatalf("result = %s, expected fallback to good", res)
	}
	if len(hook.fallbacks) == 0 {
		t.Fatal("expected fallback event for -32603")
	}
}
