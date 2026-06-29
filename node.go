package chainpool

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"time"
)

type node struct {
	name     string
	baseURL  string
	priority int
	headers  map[string]string
	timeout  time.Duration
	client   *http.Client
	lim      *limiter
	brk      *breaker
}

// NodeStat is a snapshot of a node's identity and breaker state.
type NodeStat struct {
	Name     string
	Priority int
	State    string
}

func (n *node) do(ctx context.Context, req Request) (*Response, error) {
	rctx := ctx
	if n.timeout > 0 {
		var cancel context.CancelFunc
		rctx, cancel = context.WithTimeout(ctx, n.timeout)
		defer cancel()
	}

	method := req.Method
	if method == "" {
		method = http.MethodGet
	}
	hr, err := http.NewRequestWithContext(rctx, method, n.baseURL+req.Path, bytes.NewReader(req.Body))
	if err != nil {
		return nil, err
	}
	for k, v := range n.headers {
		hr.Header.Set(k, v)
	}
	for k, vs := range req.Header {
		for _, v := range vs {
			hr.Header.Add(k, v)
		}
	}

	resp, err := n.client.Do(hr)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return &Response{
		StatusCode: resp.StatusCode,
		Body:       body,
		Header:     resp.Header,
		Node:       n.name,
	}, nil
}
