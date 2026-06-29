package chainpool

import (
	"errors"
	"testing"
)

func TestClassifyHTTP(t *testing.T) {
	cases := []struct {
		name string
		resp *Response
		err  error
		want errKind
	}{
		{"transport error", nil, errors.New("dial fail"), kindNode},
		{"429", &Response{StatusCode: 429}, nil, kindNode},
		{"500", &Response{StatusCode: 500}, nil, kindNode},
		{"503", &Response{StatusCode: 503}, nil, kindNode},
		{"401 auth", &Response{StatusCode: 401}, nil, kindAuth},
		{"403 auth", &Response{StatusCode: 403}, nil, kindAuth},
		{"400 caller", &Response{StatusCode: 400}, nil, kindReturn},
		{"404 caller", &Response{StatusCode: 404}, nil, kindReturn},
		{"200 ok", &Response{StatusCode: 200}, nil, kindReturn},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyHTTP(tc.resp, tc.err); got != tc.want {
				t.Fatalf("classifyHTTP = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestAllFailedErrorUnwraps(t *testing.T) {
	e := &AllFailedError{Chain: "ethereum", Attempts: []NodeAttempt{{Node: "a", LastStatus: 500}}}
	if !errors.Is(e, ErrAllNodesUnavailable) {
		t.Fatal("AllFailedError should unwrap to ErrAllNodesUnavailable")
	}
	if e.Error() == "" {
		t.Fatal("Error() should be non-empty")
	}
}

func TestRPCErrorImplementsError(t *testing.T) {
	var err error = &RPCError{Code: -32000, Message: "boom"}
	var re *RPCError
	if !errors.As(err, &re) || re.Code != -32000 {
		t.Fatal("RPCError should be unwrappable via errors.As")
	}
}
