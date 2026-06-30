# chainpool

Route blockchain RPC requests across multiple public nodes per chain with
per-node rate limiting, priority, circuit-breaker fallback, and a JSON-RPC
helper. Works for EVM, Solana, TON, and Tron via a generic HTTP transport.

## Install

    go get github.com/amovah/chainpool

## Quick start

```go
m, _ := chainpool.New(cfg)            // cfg from struct or chainpool.LoadConfig("nodes.yaml")
defer m.Close()

resp, err := m.Do(ctx, "ethereum", chainpool.Request{
    Method: http.MethodPost,
    Body:   body,
})

// JSON-RPC helper
pool, _ := m.Pool("ethereum")
rpc := chainpool.NewRPC(pool)
result, err := rpc.Call(ctx, "eth_blockNumber", nil)
```

## Behavior

- Nodes tried highest-priority first.
- Saturated node (rate limit) is skipped, not waited on.
- Node errors (network, 429, 5xx, 401/403) fall back to the next node.
- 401/403 trips the node's breaker immediately (revoked key) with a long cooldown.
- Valid JSON-RPC application errors return to the caller; configurable error
  codes (default `-32603`) are treated as node errors and fall back.
- When all nodes are exhausted, requests back off until the pool timeout, then
  return `AllFailedError` (unwraps to `ErrAllNodesUnavailable`).

## Gateway

Expose a `Manager` as a local HTTP JSON-RPC node so any client can reach the pool.
The URL path selects the chain; requests fan out across nodes with the pool's
rate-limiting, circuit-breaking, and failover.

```go
m, _ := chainpool.New(cfg)
go gateway.Serve(ctx, m, "127.0.0.1:8545")

// from anywhere, in any language:
eth, _ := ethclient.Dial("http://localhost:8545/ethereum")
```

Single and batch JSON-RPC requests are forwarded verbatim. Routing failures
surface as HTTP status codes: 404 (unknown chain), 405 (non-POST), 502 (all
nodes unavailable). HTTP only — no WebSocket/subscriptions.
