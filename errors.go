package chainpool

import (
	"encoding/json"
	"errors"
	"fmt"
)

var (
	ErrAllNodesUnavailable = errors.New("chainpool: all nodes unavailable")
	ErrNoNodes             = errors.New("chainpool: pool has no nodes")
	ErrUnknownChain        = errors.New("chainpool: unknown chain")
)

// RPCError is a valid JSON-RPC error object returned to the caller (same answer
// on every node, so not subject to fallback unless its code is in the
// pool's fallback set).
type RPCError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *RPCError) Error() string {
	return fmt.Sprintf("chainpool: rpc error %d: %s", e.Code, e.Message)
}

// NodeAttempt records one node's last failure during an exhausted request.
type NodeAttempt struct {
	Node       string
	LastStatus int
	LastErr    error
}

// AllFailedError is returned when every node was unavailable before the
// deadline. It unwraps to ErrAllNodesUnavailable.
type AllFailedError struct {
	Chain    string
	Attempts []NodeAttempt
}

func (e *AllFailedError) Error() string {
	return fmt.Sprintf("chainpool: all %d nodes unavailable for chain %q", len(e.Attempts), e.Chain)
}

func (e *AllFailedError) Unwrap() error { return ErrAllNodesUnavailable }

// errKind drives the pool's per-response decision.
type errKind int

const (
	kindReturn errKind = iota // hand response/err to caller; record breaker success
	kindNode                  // node failure; record breaker fail, fall back
	kindAuth                  // auth failure; record breaker fail (auth), fall back
)

func classifyHTTP(resp *Response, err error) errKind {
	if err != nil {
		return kindNode
	}
	switch {
	case resp.StatusCode == 401 || resp.StatusCode == 403:
		return kindAuth
	case resp.StatusCode == 429:
		return kindNode
	case resp.StatusCode >= 500:
		return kindNode
	default:
		return kindReturn
	}
}
