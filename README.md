# chainpool

Route blockchain RPC requests across multiple public nodes per chain with
per-node rate limiting, priority, circuit-breaker fallback, and a JSON-RPC
helper. Works for EVM, Solana, TON, and Tron via a generic HTTP transport.

## Install

    go get github.com/alimovahedi/chainpool

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

See `docs/superpowers/specs/2026-06-29-chainpool-design.md` for the full design.
