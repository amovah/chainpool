# chainpool — Design Spec

**Date:** 2026-06-29
**Status:** Approved (design phase)

## Summary

`chainpool` is a Go package that routes blockchain RPC requests across multiple
public nodes per chain while respecting each node's rate limit, honoring node
priority, and falling back to other nodes on failure. It supports EVM chains,
Solana, TON, and Tron through a generic HTTP transport with a JSON-RPC helper on
top.

## Goals

- Configure many public nodes per chain (priority + per-node rate limit).
- Send a request once; the package picks a node, respects rate limits, retries,
  and falls back on node failure.
- Per-node priority and per-node configurable rate limit (requests/sec).
- Circuit breaker per node for flaky public endpoints.
- Support EVM, Solana, TON, Tron via generic HTTP (JSON-RPC + REST).

## Non-Goals

- Typed chain-specific client wrappers (no `ethclient`-style helpers). Transparent
  transport only.
- Per-request override knobs (global pool config only; `context.Context` still
  honored for cancel/deadline).
- WebSocket / subscription transport. HTTP only for v1.

## Key Decisions

| Topic | Decision |
|---|---|
| Interface level | Transparent transport — caller hands raw request, gets raw response. |
| Transport scope | Generic HTTP core (method, path, body, headers); JSON-RPC helper layered on top. Supports REST chains (TON/Tron) + JSON-RPC. |
| Rate-limit shape | Requests/sec token bucket per node (`golang.org/x/time/rate`). No concurrent-request cap. |
| Saturated node | Skip to next node immediately (non-blocking `Allow()`), do not wait on a saturated node. |
| All nodes exhausted | Exponential backoff retry until pool timeout, then return timeout error. |
| Error classification | Node errors (network/429/5xx/401/403) → fallback. Valid JSON-RPC application errors → return to caller. |
| RPC error fallback set | Configurable JSON-RPC error codes (e.g. `-32603`) treated as node errors → fallback. |
| Auth failures | 401/403 = this node's auth broken → fallback; trip breaker immediately with longer cooldown; distinct hook. |
| Failed node recovery | Circuit breaker per node: CLOSED → OPEN (consecutive fails) → HALF-OPEN probe → CLOSED/OPEN. |
| Config | Programmatic struct core + YAML/JSON file loader with `${ENV}` interpolation. |
| Multi-chain | `Manager` holds named `Pool`s (one pool per chain). |
| Observability | Pluggable `Logger` interface + metrics/event `Hook` interface. |
| Build approach | Lean: stdlib + `x/time/rate` + `x/sync` + `yaml.v3`; hand-rolled circuit breaker. |

## Architecture

```
Manager ──has many──> Pool ──has many──> Node
                       │                   ├─ Limiter (x/time/rate, rps bucket)
                       │                   └─ Breaker (state machine + clock)
                       └─ selection loop (priority + skip + backoff)

transport.go = how a request hits a node (HTTP executor)
jsonrpc.go   = convenience wrapper producing transport requests + envelope parse
```

Each unit one job:
- **Node** — single endpoint health + throttle (limiter, breaker, static headers).
- **Pool** — pick a node by priority, skip OPEN/saturated, fallback, backoff.
- **Manager** — route by chain name; lifecycle.

### Package layout

```
chainpool/
  manager.go        manager_test.go
  pool.go           pool_test.go
  node.go           node_test.go
  limiter.go        limiter_test.go
  breaker.go        breaker_test.go
  transport.go      transport_test.go
  jsonrpc.go        jsonrpc_test.go
  errors.go         errors_test.go
  config.go         config_test.go
  hooks.go
  options.go        options_test.go
  clock.go                                 // Clock interface + real/fake
  internal/testutil/   // FakeNode (httptest), recordingHook
  example_test.go      // runnable godoc examples
```

## Components & Interfaces

```go
// ── transport ──
type Request struct {
    Method string
    Path   string            // appended to node BaseURL
    Body   []byte
    Header http.Header
}
type Response struct {
    StatusCode int
    Body       []byte
    Header     http.Header
    Node       string         // which node served it
}

// ── Manager ──
func New(cfg Config, opts ...Option) (*Manager, error)
func (m *Manager) Pool(chain string) (*Pool, error)
func (m *Manager) Do(ctx context.Context, chain string, req Request) (*Response, error)
func (m *Manager) Close() error

// ── Pool ──
func (p *Pool) Do(ctx context.Context, req Request) (*Response, error)
func (p *Pool) Stats() []NodeStat

// ── JSON-RPC helper ──
type RPCClient struct{ /* pool */ }
func NewRPC(p *Pool) *RPCClient
func (c *RPCClient) Call(ctx context.Context, method string, params any) (json.RawMessage, error)

// ── hooks ──
type Logger interface{ Log(level, msg string, kv ...any) }
type Hook interface {
    OnRequest(node string, req Request)
    OnFallback(from, to, reason string)
    OnCircuitOpen(node string)
    OnCircuitClose(node string)
    OnAuthFailure(node string)
    OnRateLimited(node string)
    OnResult(node string, status int, err error, latency time.Duration)
}
```

### Node selection (Pool.Do core loop)

1. Compute deadline = min(ctx deadline, pool timeout).
2. Loop until deadline:
   - candidates = nodes sorted by priority desc.
   - for each node:
     - breaker OPEN → skip (`OnFallback` reason "open").
     - `limiter.Allow()` false → skip (`OnRateLimited`).
     - send request (`OnRequest`); record latency (`OnResult`).
     - node error (see classification) → `breaker.RecordFail`, `OnFallback`, continue.
     - else → `breaker.RecordSuccess`, return response.
   - no node sent this pass → `backoff.Wait(ctx)`; loop.
3. Deadline hit → return `AllFailedError` (wraps `ErrAllNodesUnavailable`).

The pool accepts an optional `RespClassifier func(*Response) errKind` so the
JSON-RPC helper can mark a 200-status body carrying a fallback RPC error code as
a node error, re-entering the loop on the next node.

## Data Flow (`RPCClient.Call`)

```
Call(ctx, method, params)
  └─ build body {jsonrpc:"2.0", id:N, method, params}
  └─ Request{POST, Body, Header: content-type json}
  └─ pool.Do(ctx, req, classifier)
       └─ selection loop (above)
  └─ on returned Response: parse envelope
       if "error" object present:
          code ∈ fallbackCodes → already handled as node error inside loop
          else → return *RPCError (no fallback; same answer everywhere)
       else → return result json.RawMessage
```

## Error Handling

```go
var ErrAllNodesUnavailable = errors.New("chainpool: all nodes unavailable")
var ErrNoNodes            = errors.New("chainpool: pool has no nodes")
var ErrUnknownChain       = errors.New("chainpool: unknown chain")

type RPCError struct {
    Code    int
    Message string
    Data    json.RawMessage
}
func (e *RPCError) Error() string

type NodeAttempt struct {
    Node       string
    LastStatus int
    LastErr    error
}
type AllFailedError struct {
    Chain    string
    Attempts []NodeAttempt
}
func (e *AllFailedError) Error() string
func (e *AllFailedError) Unwrap() error // → ErrAllNodesUnavailable
```

### Classification matrix

| Situation | errKind | Breaker | Action |
|---|---|---|---|
| transport err / timeout / conn refused | NODE_ERROR | RecordFail | fallback |
| HTTP 429 | NODE_ERROR | RecordFail | fallback |
| HTTP 5xx | NODE_ERROR | RecordFail | fallback |
| HTTP 401 / 403 (auth) | NODE_ERROR | RecordFail (immediate trip, auth cooldown) + `OnAuthFailure` | fallback |
| HTTP 4xx (non-429, non-auth) | CALLER_ERROR | RecordSuccess | return to caller |
| HTTP 2xx, RPC error code ∈ fallback set | NODE_ERROR | RecordFail | fallback |
| HTTP 2xx, RPC error code ∉ set | OK→caller | RecordSuccess | return `*RPCError` |
| HTTP 2xx valid result | OK | RecordSuccess | return `Response` |
| all exhausted, deadline hit | — | — | `*AllFailedError` |

Callers use `errors.Is(err, ErrAllNodesUnavailable)` and `errors.As(&RPCError{})`.

## Circuit Breaker

Per node, three states with injected clock:

- **CLOSED** — requests flow; count consecutive failures. At `FailThreshold` → OPEN.
- **OPEN** — node skipped; after `Cooldown` elapses → HALF-OPEN.
- **HALF-OPEN** — one probe request allowed; success → CLOSED, failure → OPEN.

Auth failures (401/403) use `AuthFailThreshold` (default 1, immediate trip) and
`AuthCooldown` (default 5m, longer than normal cooldown), so a revoked token
fast-fails but is periodically re-probed in case the key is restored.

## Rate Limiting

Per node, `golang.org/x/time/rate.Limiter` with rate = `RateRPS`, burst =
`Burst` (default `ceil(RateRPS)`). Selection uses non-blocking `Allow()`; a node
with no token available is skipped this pass (decision: skip, do not wait).

## Config Schema

```go
type Config struct {
    Chains   map[string]ChainConfig
    Defaults Defaults
}
type ChainConfig struct {
    Nodes   []NodeConfig
    Timeout Duration       // overall pool deadline
    Backoff BackoffConfig
}
type NodeConfig struct {
    Name     string
    BaseURL  string            // yaml: url
    Priority int               // higher = preferred
    RateRPS  float64           // yaml: rate_rps
    Burst    int
    Timeout  Duration          // per-request HTTP timeout
    Headers  map[string]string // ${ENV} interpolated
    Breaker  BreakerConfig
}
type BreakerConfig struct {
    FailThreshold     int      // default 5
    Cooldown          Duration // default 30s
    AuthFailThreshold int      // default 1
    AuthCooldown      Duration // default 5m
}
type BackoffConfig struct {
    Initial Duration // default 50ms
    Max     Duration // default 2s
    Factor  float64  // default 2.0
}
type Defaults struct {
    RateRPS          float64
    Burst            int
    Timeout          Duration
    Breaker          BreakerConfig
    Backoff          BackoffConfig
    FallbackRPCCodes []int // default [-32603]
}
```

`Duration` is a custom type parsing `"10s"`, `"500ms"` from YAML/JSON.

### Example `nodes.yaml`

```yaml
defaults:
  rate_rps: 10
  timeout: 8s
  breaker: { fail_threshold: 5, cooldown: 30s, auth_fail_threshold: 1, auth_cooldown: 5m }
  backoff: { initial: 50ms, max: 2s, factor: 2.0 }
  fallback_rpc_codes: [-32603, -32005]

chains:
  ethereum:
    timeout: 10s
    nodes:
      - name: alchemy
        url: https://eth-mainnet.g.alchemy.com/v2/x
        priority: 100
        rate_rps: 25
        headers: { Authorization: "Bearer ${ALCHEMY_KEY}" }
      - name: ankr-public
        url: https://rpc.ankr.com/eth
        priority: 50
        rate_rps: 5
  solana:
    nodes:
      - name: sol-main
        url: https://api.mainnet-beta.solana.com
        priority: 10
        rate_rps: 8
```

Loader `LoadConfig(path)`: detect yaml/json by extension → unmarshal → `${ENV}`
interpolation → validate (duplicate names, malformed URL, `rps > 0`) → ready for
`New`.

## Testing Plan

Principle: TDD, deterministic, no real network, no real sleeps.

### Test infra (`internal/testutil`)
- `FakeNode` — `httptest.Server` with a scriptable per-hit response queue
  (`{status, body, delay}`); injects 429→5xx→200 sequences, 401 auth, RPC error bodies.
- `Clock` — interface injected into breaker, backoff, limiter usage. `FakeClock.Advance(d)` drives time.
- `recordingHook` — captures hook events for assertions.

### Per-component tests
| File | Covers |
|---|---|
| `breaker_test` | CLOSED→OPEN at threshold; OPEN skip; cooldown→HALF-OPEN; probe pass→CLOSED, fail→OPEN; auth immediate trip + long cooldown |
| `limiter_test` | `Allow()` within burst; false when drained; refill after clock advance |
| `errors_test` | classify() matrix (every row) |
| `transport_test` | header merge (auth), `${ENV}` interpolation, per-req timeout, ctx cancel |
| `jsonrpc_test` | body build, envelope parse, `*RPCError` extraction, fallback-code → node-error path |
| `config_test` | yaml + json load, defaults merge, env interpolation, validation errors, Duration parse |
| `pool_test` | priority order; skip OPEN; skip saturated→next; all-saturated→backoff→`AllFailedError`; success after fallback; RPC caller-error returns no fallback |
| `manager_test` | route by chain, unknown chain error, Close |
| `options_test` | functional options apply |

### Integration (`pool_test`, multiple FakeNodes)
- 3 nodes pri 100/50/10; node1 5xx → served by node2, `OnFallback` fired.
- node1 drained → node2 used same tick; clock advance → node1 used again.
- node1 401 → breaker OPEN immediate, `OnAuthFailure` fired, long cooldown; node2 serves.
- all nodes 5xx → backoff retries via FakeClock → `AllFailedError` with per-node attempts.
- RPC error `-32700` (not in fallback set) → `*RPCError`, no fallback, node stays CLOSED.

### Quality gates
- Runnable godoc examples (`ExampleNew`, `ExampleRPCClient_Call`).
- Coverage ≥ 85%; `go test -race`; table-driven tests.

## Tech Stack

- Go (stdlib `net/http`, `context`, `encoding/json`)
- `golang.org/x/time/rate` — token bucket limiter
- `golang.org/x/sync` — concurrency helpers if needed
- `gopkg.in/yaml.v3` — YAML config
- Hand-rolled circuit breaker + backoff + clock abstraction
