package chainpool

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"time"
)

// scriptedResponse is one canned reply.
type scriptedResponse struct {
	status int
	body   string
	delay  time.Duration
}

// fakeServer serves a queue of scripted responses; the last one repeats.
type fakeServer struct {
	mu    sync.Mutex
	srv   *httptest.Server
	queue []scriptedResponse
	hits  int
}

func newFakeServer(responses ...scriptedResponse) *fakeServer {
	f := &fakeServer{queue: responses}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		idx := f.hits
		f.hits++
		var resp scriptedResponse
		if idx < len(f.queue) {
			resp = f.queue[idx]
		} else if len(f.queue) > 0 {
			resp = f.queue[len(f.queue)-1]
		} else {
			resp = scriptedResponse{status: 200, body: "{}"}
		}
		f.mu.Unlock()
		if resp.delay > 0 {
			time.Sleep(resp.delay)
		}
		w.WriteHeader(resp.status)
		_, _ = w.Write([]byte(resp.body))
	}))
	return f
}

func (f *fakeServer) url() string { return f.srv.URL }
func (f *fakeServer) close()      { f.srv.Close() }
func (f *fakeServer) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hits
}

// buildPool wires a Pool with the given nodes using a fake clock and recording hook.
func buildPool(clock Clock, hook Hook, fallbackCodes []int, nodes ...*node) *Pool {
	return &Pool{
		chain:         "test",
		nodes:         nodes,
		backoffCfg:    BackoffConfig{Initial: Duration(time.Millisecond), Max: Duration(time.Millisecond), Factor: 2},
		timeout:       5 * time.Second,
		hook:          hook,
		clock:         clock,
		fallbackCodes: fallbackCodes,
	}
}

func testNode(name, url string, priority int, clock Clock, rps float64) *node {
	return &node{
		name:     name,
		baseURL:  url,
		priority: priority,
		timeout:  2 * time.Second,
		client:   &http.Client{},
		lim:      newLimiter(rps, int(rps)+1, clock),
		brk:      newBreaker(2, 30*time.Second, 1, 5*time.Minute, clock),
	}
}
