---
audience: user
type: how-to
status: living
sources:
  - src/mcp-server/src/**
  - src/router/src/endpoints/oauth.rs
  - src/common/src/config/mod.rs
---

# MCP server (AI agent access)

SignalDB ships a **Model Context Protocol** server, `signaldb mcp` (a subcommand of the `signaldb` binary),
that lets AI agents (Claude Code, Claude.ai, IDE assistants) query your traces
directly. It is a thin, credential-forwarding client: it validates the bearer
token you present and forwards _that same token_ to the router's HTTP API. It
holds no credential of its own, so a request can only ever see what your key is
already allowed to see — tenant isolation stays enforced by the router.

## What it exposes

Neither family below is hidden from `tools/list` — a call your credential
does not authorize comes back as a clean access-denied tool error, not a
missing tool.

### Query and discovery

Available to every authenticated tenant session — there is no role gating on
these:

| Tool                       | Purpose                                                                                                                                                                                                              |
| -------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `server_info`              | Confirm connectivity and which tenant your credential resolves to.                                                                                                                                                   |
| `discover_datasets`        | The tenant and datasets your credential can access, as a nested Markdown list, marking the session's current default dataset. Filtered to your credential's dataset restriction, if any — a dataset outside it never appears, even by name. Call before passing an explicit `dataset` or `tenant` argument elsewhere.             |
| `search_traces`            | TraceQL search over your tenant's traces.                                                                                                                                                                            |
| `get_trace`                | Fetch a single trace by ID (renders as a waterfall — see below).                                                                                                                                                     |
| `get_profile`              | Fetch a single profile's flamegraph by ID (renders as an interactive flamegraph — see below).                                                                                                                        |
| `discover_attributes`      | List queryable attribute/label names, or the values for one. Signal-aware: `traces` (default, Tempo tags), `logs` (Loki labels), `metrics` (Prometheus labels), `profiles` (Pyroscope labels).                       |
| `discover_metrics`         | List the distinct metric names visible to your tenant.                                                                                                                                                               |
| `discover_fields`          | The queryable fields of a signal source, as logical dotted OTel names with type, `origin`, coverage and approximate cardinality. Answered from the schema registry and maintained statistics — reads no signal data. |
| `discover_field_values`    | Value suggestions for one field. Exact and free for a declared value set; otherwise it names the query that would answer it, and only `sample: true` runs that query.                                                |
| `discover_sources`         | The signal sources available to your tenant, with whether each is queryable.                                                                                                                                         |
| `discover_profile_types`   | List the Pyroscope profile types with data for your tenant (e.g. CPU, heap).                                                                                                                                         |
| `query_metrics`            | PromQL query over your tenant's metrics (native Prometheus result); instant by default, or a range query when `start`/`end` (and optionally `step`) are given.                                                       |
| `search_logs`              | LogQL query over your tenant's logs (native Loki result); instant or range, same as `query_metrics`.                                                                                                                 |
| `search_profiles`          | Search profiles with a Pyroscope selector and a time range; returns the aggregated flame graph (flamebearer encoding).                                                                                               |
| `compare_profiles`         | Compare profiles between two time ranges with a shared Pyroscope selector; returns the differential flame graph.                                                                                                     |
| `profiles_for_trace`       | List the profiles correlated with a trace id.                                                                                                                                                                        |
| `query_ir`                 | Native Query IR document (the structured, versioned query surface).                                                                                                                                                  |
| `list_schema_registries`   | List the schema registries visible to your tenant in precedence order (custom first, then the bundled `signaldb` and `otel` semconv), with definition counts.                                                        |
| `get_schema_registry`      | Fetch one registry's summary and full document by `namespace`/`version`.                                                                                                                                             |
| `resolve_attribute`        | What an attribute key means: every definition across the visible registries, precedence-ordered (`primary` first), with brief, type, examples, deprecation.                                                          |
| `resolve_entity`           | What an entity type (`k8s.pod`, `service`, ...) means: identifying/descriptive attributes, what it extends, associated metrics.                                                                                      |
| `resolve_metric`           | What a metric means: instrument, unit, brief, recorded attributes, associated entities.                                                                                                                              |
| `search_schema`            | Prefix search over attributes, entities, or metrics (`kind`, `prefix`, `limit`) to find the right vocabulary before querying.                                                                                        |
| `create_schema_registry`   | Upload a custom Weaver-model registry document (JSON object) for your tenant (requires `schema:write`).                                                                                                              |
| `replace_schema_registry`  | Replace a custom registry's document by namespace/version (requires `schema:write`; bundled registries refuse).                                                                                                      |
| `validate_schema_registry` | Validate a registry document without storing it; errors carry document paths (requires `schema:write`).                                                                                                              |
| `delete_schema_registry`   | Delete a custom registry by namespace/version (requires `schema:write`; bundled registries refuse).                                                                                                                  |

Each query tool requires a `dataset` argument, targeting the dataset your
tenant may access (the router validates access and rejects the rest). Large
results are capped and returned with a `truncated: true` flag telling the
agent to narrow the query.

Most of these tools also require a `tenant` argument: a confirmation check,
not a way to switch tenants. It must equal the tenant *this specific call's*
credential resolves to (`server_info`, `discover_datasets`), and a mismatch
fails the call with an error naming both tenants, before any request reaches
the router. Both arguments are required rather than optional because one MCP
session (one `mcp-session-id`) can hold credentials for several tenants and
datasets across its calls — there is no single implicit session-wide default
left to fall back to. To reach a second tenant within one session, present a
different credential (`Authorization` bearer token plus `X-Tenant-ID`) on a
later call rather than opening a second connection; the router
independently authenticates each call, up to a bounded number of distinct
identities per session.

### Operational control

Admin-authenticated (the administrative API key, not a tenant key):

| Tool              | Purpose                                       |
| ----------------- | --------------------------------------------- |
| `compact_run`     | Trigger a compaction pass now.                |
| `compact_status`  | Active compaction leases and metrics.         |
| `compact_dry_run` | Plan compaction candidates without executing. |

### Platform administration

Unprefixed, admin-authenticated (the administrative API key can manage
**any** tenant — this is the same credential the `admin` CLI group and the
[admin HTTP API](authentication.md) use):

| Tool                               | Purpose                                                                                                                                                                              |
| ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `list_tenants` / `get_tenant`      | List every tenant, or fetch one by ID.                                                                                                                                               |
| `create_tenant` / `update_tenant`  | Create a tenant, or update its name/default dataset.                                                                                                                                 |
| `delete_tenant`                    | Delete a tenant and everything under it. **Destructive**: requires `confirm` equal to `tenant_id`.                                                                                   |
| `create_user`                      | Create a human user and grant an initial tenant membership.                                                                                                                          |
| `list_datasets` / `create_dataset` | List or create a tenant's datasets.                                                                                                                                                  |
| `delete_dataset`                   | Delete a dataset by ID. **Destructive**: requires `confirm` equal to `dataset_id`.                                                                                                   |
| `list_api_keys`                    | List a tenant's API keys with their scopes and dataset set (or that they're unrestricted). Raw secrets are never returned.                                                                                  |
| `create_api_key`                   | Create an API key carrying explicit `scopes` (required; e.g. `traces:write`, `schema:read`) and an optional `dataset_ids` set. The raw secret is returned exactly once, in this response. |
| `update_api_key_scopes`            | Change a live key's scopes and/or dataset set without rotating its secret; `dataset_ids` replaces the restriction, `clear_dataset_restriction: true` (with no `dataset_ids`) removes it, and omitting both leaves it unchanged — sending both together is rejected before any request is made. Revoked keys are rejected.                                                                        |
| `revoke_api_key`                   | Revoke an API key by ID. **Destructive**: requires `confirm` equal to `key_id`.                                                                                                      |

### Tenant self-management

`tenant_`-prefixed; act as the caller's own identity within its own tenant.
Two sub-groups, by which endpoint they wrap:

**Tenant view, tables, and schemas** (tenant self-service API) — work with a
plain tenant API key, exactly like the query tools above:

| Tool                           | Purpose                                                                                                                                    |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `tenant_info`                  | The caller's own tenant: id, enabled flag, schema configuration (mirrors `signaldb-cli tenant show`).                                      |
| `tenant_list_tables`           | List the tenant's provisioned signal tables. Filtered to your credential's dataset restriction, if any, the same way `discover_datasets` is.                                                                                               |
| `tenant_create_tables`         | Provision (create) the tenant's enabled signal tables — the manual trigger from [table provisioning](../operations/table-provisioning.md). |
| `tenant_list_table_schemas`    | List the tenant's configured table schema types (distinct from `tenant_list_tables`, which lists what is actually provisioned).            |
| `list_available_table_schemas` | List every table schema type SignalDB knows how to provision, regardless of tenant configuration.                                          |

**Datasets, API keys, memberships, and schema** (management API) — need a
management credential: either a human session (a browser session cookie, or
an OAuth access token — see [Claude.ai and ChatGPT](#claudeai-and-chatgpt-oauth-connector)
below) holding the tenant-admin role or the instance-admin flag, or an API
key that carries the **`tenant:manage`** scope for that tenant. Ingest-only
keys and legacy unscoped keys are denied — a deliberate privilege boundary:
an ingest key can write signal data and provision tables, but minting or
revoking _other_ API keys, deleting datasets, or changing memberships is
opt-in, granted only to a tenant admin or a key explicitly scoped for it.
Calling one of these with a key that lacks the scope returns a clean
access-denied error naming the required scope rather than succeeding or
404ing. `tenant:manage` is never granted through OAuth consent; see
[API-key scopes](authentication.md#api-key-scopes).

| Tool                                                   | Purpose                                                                                                  |
| ------------------------------------------------------ | -------------------------------------------------------------------------------------------------------- |
| `tenant_list_datasets` / `tenant_create_dataset`       | List or create the caller's own tenant's datasets.                                                       |
| `tenant_delete_dataset`                                | Delete a dataset by name. **Destructive**: requires `confirm` equal to `dataset_name`.                   |
| `tenant_list_api_keys`                                 | List the caller's own tenant's API keys. Raw secrets are never returned.                                 |
| `tenant_create_api_key`                                | Create an API key for the caller's own tenant. The raw secret is returned exactly once.                  |
| `tenant_update_api_key`                                | Update the scopes and/or dataset set of one of the caller's own tenant's API keys — same `dataset_ids`/`clear_dataset_restriction` semantics as `update_api_key_scopes` above.               |
| `tenant_revoke_api_key`                                | Revoke one of the caller's own tenant's API keys. **Destructive**: requires `confirm` equal to `key_id`. |
| `tenant_list_memberships` / `tenant_upsert_membership` | List the caller's own tenant's memberships, or create/update a member's role.                            |
| `tenant_remove_membership`                             | Remove a member from the caller's own tenant. **Destructive**: requires `confirm` equal to `user_id`.    |
| `tenant_get_schema`                                    | The registered logical (client-visible) and physical (storage) schema for every signal source.           |

Destructive tools carry the MCP `destructiveHint` annotation; read-only tools
carry `readOnlyHint` — a client that inspects `tools/list` annotations can
tell which is which without trying the call.

When the router throttles a tool's downstream request (`429`, the tenant's
query rate limit), the server first lets the SDK's shared retry policy absorb
it — waiting the server-stated `Retry-After` within the policy's bounds (see
[client retry](client-retry.md)) — so a brief burst is invisible to the
agent. Only once retries are exhausted (or the server asks for more than the
policy's 10 s per-attempt ceiling) does the tool fail, with a **throttled
error** distinct from an internal failure: the message starts with
`throttled:` and names the wait (`throttled: search_logs was rate limited; the
server asked to retry in 30s`), and the error `data` carries `retryAfterMs`
(milliseconds; `null` when no wait was stated) plus `http_status: 429`. An
agent should wait that long or narrow the query.

## Prompts

`prompts/list` offers ready-made investigation templates a client can surface
directly (e.g. as a slash command), separate from the tools above:

| Prompt               | Arguments                                              | Purpose                                                                |
| -------------------- | ------------------------------------------------------ | ---------------------------------------------------------------------- |
| `investigate_trace`  | `trace_id` (required)                                  | Seeds a `get_trace` call and a critical-path/error/self-time analysis. |
| `find_recent_errors` | `service` (required), `minutes` (optional, default 15) | Seeds a `search_traces`/`search_logs` sweep for a service's errors.    |
| `build_promql_query` | `metric` (required), `intent` (optional)               | Seeds `discover_metrics` → `query_metrics` for a metric.               |

Each prompt renders into a single text message — pure argument substitution,
no router call, so prompts work even before your credential has been
validated for the session.

Two arguments offer live autocompletion via `completion/complete`, for
clients that ask for suggestions as you type: `find_recent_errors`'s
`service` (backed by Tempo `service.name` tag-value discovery) and
`build_promql_query`'s `metric` (backed by Prometheus `__name__` label
discovery), both scoped to your tenant and filtered by the prefix you've
typed so far. Every other reference/argument returns no suggestions rather
than an error — completions are advisory, so a lookup failure never breaks
the request you're filling in.

## Interactive views (MCP Apps)

Clients that support the [MCP Apps extension][mcp-apps] render two tools as
interactive views instead of raw JSON:

- `get_trace` as a waterfall: span timings, self time versus time spent in
  child spans, per-span attributes, and span events, with a ruler you can
  read span offsets against.
- `get_profile` as a flamegraph: per-frame self/total sample values across
  the call stack, colored by function name, with a tooltip on hover.

Nothing needs configuring. A client that negotiates the extension gets both;
every other client keeps receiving the same JSON text result it always did.

How it works, if you are curious or writing a client:

- The client declares `io.modelcontextprotocol/ui` in its `initialize`
  capabilities, naming `text/html;profile=mcp-app` in that capability's
  `mimeTypes`. A client that declares the extension without naming the type
  cannot render either view, so it keeps the plain-text tool surface.
- The server then marks `get_trace`/`get_profile` with `_meta.ui.resourceUri`
  pointing at `ui://signaldb/trace`/`ui://signaldb/profile` respectively, and
  attaches the trace/flamegraph to the result as `structuredContent` alongside
  the usual text block.
- The client fetches the app's URI with `resources/read` — served as
  `text/html;profile=mcp-app` — and renders it in a sandboxed iframe, handing
  it the tool result.

Each view is a single self-contained HTML document compiled into the binary.
It makes no network requests of its own and cannot reach the router: its only
data is the tool result the client hands it, which keeps it inside the
strictest sandbox hosts apply (`default-src 'none'`). Because the apps are
served over `resources/read`, the server advertises the `resources`
capability; it exposes no data resources, only these UI documents.

[mcp-apps]: https://modelcontextprotocol.io/extensions/apps/overview

## Running it

The server is off by default. Enable it in `signaldb.toml`:

```toml
[mcp]
enabled = true
bind_address = "127.0.0.1:8228"      # serves MCP at /mcp; loopback by default
router_url = "http://localhost:3000" # the router HTTP API to forward to
router_timeout = 30                  # seconds per forwarded request (default 30)
max_concurrent_tool_calls = 8        # tool calls in flight per session (default 8)
```

Each forwarded request is bounded by `router_timeout` (plus a fixed 5s connect
timeout), so a hung router fails the tool call cleanly instead of hanging the
agent indefinitely. Raise it if your agents run slow analytical queries.

That per-request bound alone does not bound one `tools/call`: the SDK's
shared retry policy can spend up to 4 attempts plus 30s of retry sleeps
underneath it, so with the default `router_timeout` a single call could
otherwise run for close to 150s. A total deadline of `router_timeout +
30s` (60s with defaults) wraps the whole call instead: a call still running
past it fails with a distinct `tool call exceeded the Ns deadline` error
(`outcome=error`, `error.type=deadline` — see
[Audit and observability](#audit-and-observability)). Raising
`router_timeout` raises this deadline with it.

`max_concurrent_tool_calls` bounds how many tool calls one MCP session may
have in flight at once (also `--max-concurrent-tool-calls` /
`SIGNALDB__MCP__MAX_CONCURRENT_TOOL_CALLS`). A call that arrives while the
session is at the bound waits up to 2 seconds for an in-flight call to finish
and then fails with the distinct error `too many concurrent tool calls (limit
N); wait for in-flight calls to finish` (JSON-RPC `-32600`, `data.limit = N`)
— the other calls are unaffected, and nothing queues indefinitely. The bound
is per session, so one runaway agent cannot starve another session's tenant.
See [Audit and observability](#audit-and-observability) for how such a call
is logged.

The server forwards live bearer credentials, so it binds **loopback by
default**. Exposing it off-host means changing `bind_address` to a routable
address **and** putting it behind TLS (direct HTTPS or a trusted terminator).

Then run the standalone binary (or use `./scripts/run-dev.sh services`, which
starts it automatically on `:8228`):

```bash
cargo run --bin signaldb -- mcp
# stdio transport for local development (unauthenticated — dev only):
cargo run --bin signaldb -- mcp --stdio
```

The same settings are available as environment variables (multi-word fields
need the double-underscore form): `SIGNALDB__MCP__ENABLED`,
`SIGNALDB__MCP__BIND_ADDRESS`, `SIGNALDB__MCP__ROUTER_URL`,
`SIGNALDB__MCP__ROUTER_TIMEOUT` (seconds, also `--router-timeout`),
`SIGNALDB__MCP__MAX_CONCURRENT_TOOL_CALLS`, and `SIGNALDB__MCP__ALLOWED_HOSTS`
(see below). The sidecar also honours `[self_monitoring]` (from `--config`,
`signaldb.toml`, or `SIGNALDB__SELF_MONITORING__*`) so its own spans, audit
events, and metrics can be exported — see below. It parses the same shared
`Configuration` as every other service, so unrelated sections like
`[wal].max_instances` (see [WAL Persistence](../operations/wal-persistence.md))
are accepted but unused here — the standalone MCP server holds no WAL of its
own.

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
signaldb mcp --allowed-hosts mcp.example.org,10.0.0.5:30228
# or via env
SIGNALDB__MCP__ALLOWED_HOSTS="mcp.example.org,10.0.0.5:30228" signaldb mcp
```

The single value `*` disables the guard entirely. The server still authenticates
every request (bearer + tenant), so `*` drops only the rebinding guard, never
authorization — but prefer an explicit list where you can.

## Running as a sidecar

The MCP server ships as its own image, `ghcr.io/cedricziel/signaldb/mcp`
(the same `signaldb` binary as every other image, with `signaldb mcp` as its
entrypoint), so it runs as a sidecar next to a `signaldb` router/monolith. The
deployment (not the Dockerfile) makes it reachable: bind
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

A request that carries no bearer token or no `X-Tenant-ID` is rejected with
`401` at the MCP server before it reaches the transport. The MCP server does
not validate the credential itself — it forwards it, and the **router** decides
whether it is valid; an invalid or revoked key is rejected downstream and comes
back as a clean MCP tool error.

### Claude.ai and ChatGPT (OAuth connector)

Claude.ai and OpenAI/ChatGPT register a remote MCP server through OAuth 2.1 with
Dynamic Client Registration — no headers, no pre-registration. Add the `/mcp`
URL under **Settings → Connectors → Add custom connector**; the client
discovers SignalDB's authorization server, registers itself, and sends you
through a sign-in + consent screen. On the consent screen you pick **one
tenant** and approve the read scopes it requested; the token it receives is
bound to that tenant. To let a connector reach a second tenant, add it a second
time and grant the other tenant.

After choosing a tenant, the consent screen also offers a dataset choice:
**all datasets** in that tenant (the default — identical to every connector
granted before this choice existed) or **only these datasets**, which reveals
a checklist of the tenant's datasets and requires at least one checked box to
approve. Picking specific datasets binds the token to exactly that set —
queries against any other dataset in the tenant are refused, and a query
naming no dataset at all is rejected rather than silently falling back to the
tenant default when the set has more than one dataset (a single-dataset
restriction resolves to that dataset the same way an unrestricted token
resolves to the tenant default). A refresh preserves whichever restriction the
original grant had. Restricting a grant to specific datasets is refused,
naming the `dataset_restriction_rollout_complete` config key, until an
operator has set `[auth] dataset_restriction_rollout_complete = true` on
every router node — see [Multi-dataset rollout](authentication.md#multi-dataset-rollout);
choosing "all datasets" is unaffected by this and always available.

The endpoint **must be HTTPS with a valid certificate** — these clients will not
connect to a raw LAN port, so the TLS reverse proxy is required here.

**Operator setup.** The authorization server is served by the **router**, off by
default. Enable it and point it at the externally-reachable URLs clients use:

```toml
# router config (signaldb.toml)
[mcp.oauth]
enabled = true
issuer_url = "https://signaldb.example.org"        # this AS, as clients reach it
resource_url = "https://signaldb.example.org/mcp"  # the MCP resource tokens bind to
# access_token_ttl = "1h"; refresh_token_ttl = "30d"; authorization_code_ttl = "60s"
```

The `signaldb mcp` sidecar advertises the same resource so an
unauthenticated request is challenged toward discovery — pass the matching URLs:

```bash
signaldb mcp \
  --oauth-resource-url https://signaldb.example.org/mcp \
  --oauth-issuer-url   https://signaldb.example.org
```

Tokens are opaque, catalog-backed, and audience-bound to `resource_url`;
revoking one is a row delete. The read scopes a token may hold —
`traces:read`, `logs:read`, `metrics:read`, `profiles:read`, `schema:read` —
gate the corresponding query surface (see the
[multi-tenancy](../architecture/overview.md) model); a request with no `scope`
is granted all of them, and `schema:write` is never grantable through OAuth
(a request naming only it is rejected with `invalid_scope`). The
existing `Bearer <api-key>` + `X-Tenant-ID` path is unchanged; OAuth is an
added credential type, not a replacement.

## Audit and observability

Every tool call is audited: after it completes, the server emits exactly one
structured log event (target `signaldb_mcp::audit`) with bounded fields —
never the arguments, the query expression, or the result:

| Field         | Meaning                                                                                                                          |
| ------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| `tool`        | The tool name (`search_traces`, `get_trace`, …).                                                                                 |
| `tenant_id`   | The tenant the router resolved the caller to.                                                                                    |
| `dataset`     | The dataset the call named (its `dataset` argument, else `X-Dataset-ID`); absent when neither is set.                            |
| `session_id`  | The `Mcp-Session-Id` (`stdio` on the stdio transport).                                                                           |
| `outcome`     | `ok`, `truncated` (result cut at the size cap), `denied`, `throttled`, or `error`.                                               |
| `duration_ms` | Wall time of the call, including any wait for a concurrency permit.                                                              |
| `error.type`  | Only for `outcome=error`: `concurrency_limit`, `deadline`, `tool_error`, the router's HTTP status (`500`), or the JSON-RPC code. |

Levels: `ok`, `truncated`, and `throttled` log at `info`; `denied` (the router
rejected the credential or the tenant/dataset access — a `401`/`403`) at
`warn`, so probing is visible; `error` at `error`. A call refused at the
concurrency bound is `outcome=error`, `error.type=concurrency_limit`; a call
still running past the total per-call deadline (see [Running it](#running-it))
is `outcome=error`, `error.type=deadline`.

The same call is one `tools/call {tool}` span (INTERNAL, `gen_ai.tool.name`,
`mcp.session.id`, `signaldb.tenant.id`, `signaldb.dataset.id`; status Error
only for `outcome=error`), the HTTP request that carried it is a `POST /mcp`
server span parented to the client's `traceparent`, and two metrics count the
calls: `signaldb.mcp.tool_calls` by tool and outcome and
`signaldb.mcp.tool_call.duration` by tool (Prometheus:
`signaldb_mcp_tool_calls_total{gen_ai_tool_name,signaldb_mcp_outcome}` and
`signaldb_mcp_tool_call_duration_seconds{gen_ai_tool_name}`). All of it is
exported when `[self_monitoring]` is enabled for the sidecar (service name
`signaldb-mcp`); see `docs/operations/self-monitoring-traces.md`.

## Example flow

1. `server_info` — confirm you are connected as the expected tenant.
2. `discover_attributes` — list tag names, then values for `service.name`.
3. `search_traces` with `{ .service.name = "checkout" && status = error }` and a
   time range to find failing requests.
4. `get_trace` with an ID from the search results to inspect the full trace.

To explore logs or metrics instead: `discover_attributes` with `signal:
"logs"` lists Loki labels (add `tag` for a label's values); `signal:
"metrics"` does the same for Prometheus labels. `discover_metrics` lists
metric names directly, for building a `query_metrics` PromQL expression.

Before filtering or grouping by a name you are unsure of, ask the schema
registry what it means: `resolve_attribute` with `key: "k8s.pod.uid"` (or
`resolve_entity` / `resolve_metric`, or `search_schema` with `kind:
"attribute", prefix: "k8s.pod."`) returns namespace-tagged, precedence-ordered
definitions — a tenant's own conventions (uploaded with
`create_schema_registry`) come first, the bundled OpenTelemetry definition is
kept as an alternative. `discover_*` tells you which names _have data_;
`resolve_*` tells you what they _mean_.

## From the CLI

The same discovery is available outside an agent session, via
`signaldb-sdk` like every other CLI capability:

```bash
# Native surface (Query IR, logical dotted names, no scan)
signaldb-cli discover sources
signaldb-cli discover fields --source logs
signaldb-cli discover values --source traces --field span.kind
signaldb-cli discover values --source traces --field http.route --sample

# Compatibility-dialect view (Tempo tags, Loki/Prometheus labels)
signaldb-cli discover attributes --signal traces --tag service.name
signaldb-cli discover attributes --signal logs
signaldb-cli discover attributes --signal metrics --tag job
signaldb-cli discover metrics
```

`discover fields`/`values`/`sources` are the native surface: they speak the same
logical names as a Query IR document and are answered from metadata rather than
by scanning. `discover values` reads data only when you pass `--sample`, and the
response says so — without it you are told what would answer the question
instead. `discover attributes` remains the dialect-shaped view, for parity with
what Grafana sees. See [the Query IR reference](querying-ir.md#discovery-what-can-i-query).

Schema-registry lookup and custom-registry management mirror the schema tools
(reads need a key with `schema:read`, mutations `schema:write`):

```bash
signaldb-cli schema registry list
signaldb-cli schema registry get otel 1.43.0
signaldb-cli schema attribute get k8s.pod.uid
signaldb-cli schema entity get k8s.pod
signaldb-cli schema metric search k8s.pod. --limit 20
signaldb-cli admin schema validate --file conventions.yaml
signaldb-cli admin schema create --file conventions.yaml     # YAML or JSON
signaldb-cli admin schema replace acme 1.0.0 --file conventions.yaml
signaldb-cli admin schema delete acme 1.0.0
```

`server_info` mirrors `signaldb-cli whoami`; `query_metrics`/`search_logs`'s
range mode mirrors `signaldb query --promql|--logql ... --start ... --end
...`; `get_trace` mirrors `signaldb query --trace-id <id>`:

```bash
signaldb-cli whoami
signaldb-cli query --promql 'up' --start 0 --end 3600 --step 15s
signaldb-cli query --trace-id 4bf92f3577b34da6a3ce929d0e0e4736
```

The `tenant_*` tools mirror `signaldb-cli tenant`: `tenant_info` is `tenant
show`, the table tools are `tenant table ...` (any valid key of the tenant),
and the management tools are `tenant dataset|api-key|membership|schema ...`
(a key carrying `tenant:manage`; destructive verbs prompt on a TTY unless
`--yes`):

```bash
signaldb-cli tenant show --api-key sk-your-key --tenant-id your-tenant
signaldb-cli tenant table list --api-key sk-your-key --tenant-id your-tenant
signaldb-cli tenant table provision --api-key sk-your-key --tenant-id your-tenant
signaldb-cli tenant table schemas --api-key sk-your-key --tenant-id your-tenant
signaldb-cli tenant table available-schemas --api-key sk-your-key
signaldb-cli tenant dataset create staging --api-key sk-manage-key --tenant-id your-tenant
signaldb-cli tenant api-key create --name ci --scope traces:write --api-key sk-manage-key --tenant-id your-tenant
signaldb-cli tenant membership set alice@example.com --role member --api-key sk-manage-key --tenant-id your-tenant
```

Platform administration (`list_tenants`, `create_dataset`, `revoke_api_key`,
...) mirrors the `admin` command group, authenticated with the administrative
key instead of a tenant key — see `signaldb-cli admin --help`.
