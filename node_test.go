package chainpool

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func newHTTPTestNode(name, url string, headers map[string]string, timeout time.Duration) *node {
	c := newFakeClock(time.Unix(0, 0))
	return &node{
		name:    name,
		baseURL: url,
		headers: headers,
		timeout: timeout,
		client:  &http.Client{},
		lim:     newLimiter(1000, 1000, c),
		brk:     newBreaker(5, time.Second, 1, time.Minute, c),
	}
}

func TestNodeDoMergesHeadersAndSetsNode(t *testing.T) {
	var gotAuth, gotExtra string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotExtra = r.Header.Get("X-Trace")
		body, _ := io.ReadAll(r.Body)
		w.WriteHeader(201)
		_, _ = w.Write(append([]byte("echo:"), body...))
	}))
	defer srv.Close()

	n := newHTTPTestNode("n1", srv.URL, map[string]string{"Authorization": "Bearer K"}, 0)
	req := Request{Method: "POST", Path: "/v1", Body: []byte("hi"), Header: http.Header{"X-Trace": []string{"abc"}}}
	resp, err := n.do(context.Background(), req)
	if err != nil {
		t.Fatalf("do err = %v", err)
	}
	if gotAuth != "Bearer K" {
		t.Fatalf("auth header = %q", gotAuth)
	}
	if gotExtra != "abc" {
		t.Fatalf("trace header = %q", gotExtra)
	}
	if resp.StatusCode != 201 || string(resp.Body) != "echo:hi" {
		t.Fatalf("resp = %d %q", resp.StatusCode, resp.Body)
	}
	if resp.Node != "n1" {
		t.Fatalf("resp.Node = %q, want n1", resp.Node)
	}
}

func TestNodeDoPerRequestTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	n := newHTTPTestNode("slow", srv.URL, nil, 10*time.Millisecond)
	_, err := n.do(context.Background(), Request{Method: "GET"})
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestNodeDoRespectsContextCancel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
	}))
	defer srv.Close()

	n := newHTTPTestNode("n", srv.URL, nil, 0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := n.do(ctx, Request{Method: "GET"}); err == nil {
		t.Fatal("expected ctx cancel error")
	}
}
