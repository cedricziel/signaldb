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

| Tool                  | Purpose                                                             |
| --------------------- | ------------------------------------------------------------------- |
| `server_info`         | Confirm connectivity and which tenant your credential resolves to.  |
| `search_traces`       | TraceQL search over your tenant's traces.                           |
| `get_trace`           | Fetch a single trace by ID.                                         |
| `discover_attributes` | List queryable tag names, or the values for a tag.                  |
| `query_metrics`       | PromQL query over your tenant's metrics (native Prometheus result). |
| `search_logs`         | LogQL query over your tenant's logs (native Loki result).           |
| `query_ir`            | Native Query IR document (the structured, versioned query surface). |

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
`SIGNALDB__MCP__BIND_ADDRESS`, `SIGNALDB__MCP__ROUTER_URL`, and
`SIGNALDB__MCP__ALLOWED_HOSTS` (see below).

### The `Host` allowlist (serving beyond localhost)

The Streamable HTTP transport carries a DNS-rebinding guard that validates the
inbound `Host` header, and by default accepts **only loopback hosts**
(`localhost`, `127.0.0.1`, `::1`). A client that reaches the server by any other
name or IP — a LAN address, a public hostname — is rejected with
`403 Forbidden: Host header is not allowed` _before_ authentication runs. (Node's
`fetch`, which Claude Code uses for HTTP MCP, will not let a client override the
`Host` header, so there is no client-side workaround.)

When you serve the MCP off-localhost, name the reachable authority in the
allowlist. The value is a comma-separated list of `host` or `host:port`
authorities, appended to the loopback defaults:

```bash
# reached as mcp.example.org (behind TLS) and, for a bare LAN sidecar, by IP:port
signaldb-mcp --allowed-hosts mcp.example.org,10.0.0.5:30228
# or via env
SIGNALDB__MCP__ALLOWED_HOSTS="mcp.example.org,10.0.0.5:30228" signaldb-mcp
```

The single value `*` disables the guard entirely. The server still authenticates
every request (bearer + tenant), so `*` drops only the rebinding guard, never
authorization — but prefer an explicit list where you can.

## Running as a sidecar

`signaldb-mcp` ships as its own small image, `ghcr.io/cedricziel/signaldb/mcp`,
so it runs as a sidecar next to a `signaldb` router/monolith without pulling the
full server image. The deployment (not the Dockerfile) makes it reachable: bind
a non-loopback address, point it at the router by service name, and **publish
the port** (`EXPOSE` alone does not publish anything).

```yaml
services:
  signaldb: # your router/monolith, serving the router on :3000
    image: ghcr.io/cedricziel/signaldb:main
    volumes: ["./data:/data"]
    working_dir: /data

  signaldb-mcp:
    image: ghcr.io/cedricziel/signaldb/mcp:main # dedicated MCP image
    # SDK-only + forward-only: no config file or catalog needed, just the
    # router URL. It validates nothing itself — the router does.
    environment:
      # 0.0.0.0 so the published port is reachable (loopback is the default)
      SIGNALDB__MCP__BIND_ADDRESS: "0.0.0.0:8228"
      # the router, by compose service name
      SIGNALDB__MCP__ROUTER_URL: "http://signaldb:3000"
      # the authority clients reach this by — otherwise the Host guard 403s
      # them before auth (see "The Host allowlist" above). Use your TLS
      # hostname, or the bare host:port for a LAN sidecar.
      SIGNALDB__MCP__ALLOWED_HOSTS: "mcp.example.org"
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

Use the URL that matches your deployment: `http://localhost:8228/mcp` for a
loopback dev instance, or your **HTTPS reverse-proxy URL** (e.g.
`https://mcp.example.org/mcp`) for anything off-host — the server forwards live
bearer credentials, so a remote endpoint must be TLS-terminated.

### Claude Code (CLI + IDE)

The first-class path — it passes arbitrary headers, which is how the server
receives the bearer and tenant:

```bash
# local dev instance
claude mcp add --transport http signaldb http://localhost:8228/mcp \
  --header "Authorization: Bearer sk-your-key" \
  --header "X-Tenant-ID: your-tenant"

# deployed behind TLS
claude mcp add --transport http signaldb https://mcp.example.org/mcp \
  --header "Authorization: Bearer sk-your-key" \
  --header "X-Tenant-ID: your-tenant"
```

### Claude.ai and Claude Desktop

Add it under **Settings → Connectors → Add custom connector** with the same
`/mcp` URL. Two constraints:

- The endpoint **must be HTTPS with a valid certificate** — Claude.ai will not
  connect to a raw LAN port, so the TLS reverse proxy is required here, not
  optional.
- Passing the `Authorization`/`X-Tenant-ID` headers depends on the connector
  supporting custom headers. The fully-managed alternative is an OAuth flow,
  which SignalDB's MCP server does not implement in v1 (bearer + tenant headers
  only) — so header-capable clients like Claude Code are the smoothest path
  today.

A request that carries no bearer token or no `X-Tenant-ID` is rejected with
`401` at the MCP server before it reaches the transport. The MCP server does
not validate the credential itself — it forwards it, and the **router** decides
whether it is valid; an invalid or revoked key is rejected downstream and comes
back as a clean MCP tool error.

## Example flow

1. `server_info` — confirm you are connected as the expected tenant.
2. `discover_attributes` — list tag names, then values for `service.name`.
3. `search_traces` with `{ .service.name = "checkout" && status = error }` and a
   time range to find failing requests.
4. `get_trace` with an ID from the search results to inspect the full trace.
