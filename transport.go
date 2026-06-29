package chainpool

import "net/http"

// Request is a generic HTTP request routed to a node. Path is appended to the
// node's base URL; Header is merged on top of the node's static headers.
type Request struct {
	Method string
	Path   string
	Body   []byte
	Header http.Header
}

// Response is the result served by a node.
type Response struct {
	StatusCode int
	Body       []byte
	Header     http.Header
	Node       string
}
