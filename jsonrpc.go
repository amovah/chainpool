package chainpool

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sync/atomic"
)

type rpcRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      int64  `json:"id"`
	Method  string `json:"method"`
	Params  any    `json:"params"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int64           `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *RPCError       `json:"error"`
}

// RPCClient is a JSON-RPC convenience layer over a Pool.
type RPCClient struct {
	pool     *Pool
	id       atomic.Int64
	fallback map[int]bool
}

func NewRPC(p *Pool) *RPCClient {
	fb := make(map[int]bool, len(p.FallbackCodes()))
	for _, code := range p.FallbackCodes() {
		fb[code] = true
	}
	return &RPCClient{pool: p, fallback: fb}
}

// Call performs a single JSON-RPC request. It returns the raw result, an
// *RPCError for application-level errors, or a routing error.
func (c *RPCClient) Call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	body, err := json.Marshal(rpcRequest{
		JSONRPC: "2.0",
		ID:      c.id.Add(1),
		Method:  method,
		Params:  params,
	})
	if err != nil {
		return nil, err
	}
	req := Request{
		Method: http.MethodPost,
		Body:   body,
		Header: http.Header{"Content-Type": []string{"application/json"}},
	}

	classifier := func(resp *Response) errKind {
		var rr rpcResponse
		if err := json.Unmarshal(resp.Body, &rr); err != nil {
			return kindReturn // not JSON-RPC shaped; let caller deal with body
		}
		if rr.Error != nil && c.fallback[rr.Error.Code] {
			return kindNode
		}
		return kindReturn
	}

	resp, err := c.pool.doClassified(ctx, req, classifier)
	if err != nil {
		return nil, err
	}

	var rr rpcResponse
	if err := json.Unmarshal(resp.Body, &rr); err != nil {
		return nil, err
	}
	if rr.Error != nil {
		return nil, rr.Error
	}
	return rr.Result, nil
}

// DoRPC routes a raw JSON-RPC body (single object or batch array) through the
// pool and returns the upstream response body verbatim. Fallback-code errors in
// the response trigger node failover, matching RPCClient's classification.
func (p *Pool) DoRPC(ctx context.Context, reqBody []byte) ([]byte, error) {
	fb := make(map[int]bool, len(p.fallbackCodes))
	for _, code := range p.fallbackCodes {
		fb[code] = true
	}

	req := Request{
		Method: http.MethodPost,
		Body:   reqBody,
		Header: http.Header{"Content-Type": []string{"application/json"}},
	}

	classifier := func(resp *Response) errKind {
		body := bytes.TrimSpace(resp.Body)
		if len(body) > 0 && body[0] == '[' {
			var batch []rpcResponse
			if json.Unmarshal(body, &batch) != nil {
				return kindReturn // not JSON-RPC shaped; hand back to caller
			}
			for i := range batch {
				if batch[i].Error != nil && fb[batch[i].Error.Code] {
					return kindNode
				}
			}
			return kindReturn
		}
		var one rpcResponse
		if json.Unmarshal(body, &one) != nil {
			return kindReturn
		}
		if one.Error != nil && fb[one.Error.Code] {
			return kindNode
		}
		return kindReturn
	}

	resp, err := p.doClassified(ctx, req, classifier)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}
