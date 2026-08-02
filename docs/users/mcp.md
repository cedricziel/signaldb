---
audience: user
type: how-to
status: living
sources:
  - src/mcp-server/src/**
  - src/common/src/config/mod.rs
---

# MCP server (AI agent access)

SignalDB ships a standalone **Model Context Protocol** server, `signaldb-mcp`,
that lets AI agents (Claude Code, Claude.ai, IDE assistants) query your traces
directly. It is a thin, credential-forwarding client: it validates the bearer
token you present and forwards _that same token_ to the router's HTTP API. It
holds no credential of its own, so a request can only ever see what your key is
already allowed to see — tenant isolation stays enforced by the router.

## What it exposes

Tools (available to every authenticated tenant session — there is no role
gating in v1):

| Tool                  | Purpose                                                            |
| --------------------- | ------------------------------------------------------------------ |
| `server_info`         | Confirm connectivity and which tenant your credential resolves to. |
| `search_traces`       | TraceQL search over your tenant's traces.                          |
| `get_trace`           | Fetch a single trace by ID.                                        |
| `discover_attributes` | List queryable tag names, or the values for a tag.                 |

> `search_logs` (LogQL) and `query_metrics` (PromQL) arrive once the Loki and
> Prometheus query endpoints join the generated SDK — tracked on epic #620.

Each query tool accepts an optional `dataset` argument. Omit it to use your
session's default dataset; pass one to target another dataset your tenant may
access (the router validates access and rejects the rest). Large results are
capped and returned with a `truncated: true` flag telling the agent to narrow
the query.

## Running it

The server is off by default. Enable it in `signaldb.toml`:

```toml
[mcp]
enabled = true
bind_address = "127.0.0.1:8228"      # serves MCP at /mcp; loopback by default
router_url = "http://localhost:3000" # the router HTTP API to forward to
```

The server forwards live bearer credentials, so it binds **loopback by
default**. Exposing it off-host means changing `bind_address` to a routable
address **and** putting it behind TLS (direct HTTPS or a trusted terminator).

Then run the standalone binary (or use `./scripts/run-dev.sh services`, which
starts it automatically on `:8228`):

```bash
cargo run --bin signaldb-mcp
# stdio transport for local development (unauthenticated — dev only):
cargo run --bin signaldb-mcp -- --stdio
```

The same settings are available as environment variables:
`SIGNALDB_MCP_ENABLED`, `SIGNALDB_MCP_BIND_ADDRESS`, `SIGNALDB_MCP_ROUTER_URL`.

## Connecting an agent

The server speaks MCP over **Streamable HTTP** at `/mcp`. Authenticate with the
same headers as any SignalDB HTTP caller:

- `Authorization: Bearer <api-key>`
- `X-Tenant-ID: <tenant>`
- `X-Dataset-ID: <dataset>` (optional)

From Claude Code:

```bash
claude mcp add --transport http signaldb http://localhost:8228/mcp \
  --header "Authorization: Bearer sk-your-key" \
  --header "X-Tenant-ID: your-tenant"
```

A request without a valid bearer token and `X-Tenant-ID` is rejected with `401`
before any MCP session is established.

## Example flow

1. `server_info` — confirm you are connected as the expected tenant.
2. `discover_attributes` — list tag names, then values for `service.name`.
3. `search_traces` with `{ .service.name = "checkout" && status = error }` and a
   time range to find failing requests.
4. `get_trace` with an ID from the search results to inspect the full trace.
