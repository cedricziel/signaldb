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

The same settings are available as environment variables (multi-word fields
need the double-underscore form): `SIGNALDB__MCP__ENABLED`,
`SIGNALDB__MCP__BIND_ADDRESS`, `SIGNALDB__MCP__ROUTER_URL`.

## Running as a sidecar (monolithic deployment)

`signaldb-mcp` ships in the monolithic image, so alongside a monolithic
`signaldb` container you can run it as a **sidecar from the same image** with an
entrypoint override — no separate image. The deployment (not the Dockerfile)
makes it reachable: bind a non-loopback address, point it at the monolith's
router by service name, and **publish the port** (`EXPOSE` alone does not
publish anything).

```yaml
services:
  signaldb: # your monolith, serving the router on :3000
    image: ghcr.io/cedricziel/signaldb:main
    volumes: ["./data:/data"]
    working_dir: /data

  signaldb-mcp:
    image: ghcr.io/cedricziel/signaldb:main # same image
    entrypoint: ["/usr/local/bin/signaldb-mcp"]
    environment:
      # 0.0.0.0 so the published port is reachable (loopback is the default)
      SIGNALDB__MCP__BIND_ADDRESS: "0.0.0.0:8228"
      # the monolith's router, by compose service name
      SIGNALDB__MCP__ROUTER_URL: "http://signaldb:3000"
    volumes: ["./data:/data"] # same signaldb.toml (auth) + catalog as the monolith
    working_dir: /data
    ports: ["8228:8228"] # publish it — required for reachability
    depends_on: [signaldb]
    restart: unless-stopped
```

Because it forwards live bearer credentials, a non-loopback bind should sit
behind TLS — front it with your reverse proxy rather than publishing the raw
port to an untrusted network.

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
