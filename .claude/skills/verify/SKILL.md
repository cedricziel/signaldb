---
name: verify
description: How to verify a SignalDB change at runtime - build the monolith, run it against an isolated data dir, and drive the change through its real surface (Query IR over HTTP, MCP over Streamable HTTP, the Explore UI in Playwright). Use with /verify, or whenever a change needs to be observed running rather than tested.
sources:
  - scripts/run-dev.sh
  - src/signaldb-bin/src/main.rs
  - src/mcp-server/src/cli.rs
  - src/ui/vite.config.ts
---

# Verifying a SignalDB change at runtime

Run the real binary and drive the change where a user meets it. A passing
test suite is not verification.

## Build

```bash
CARGO_INCREMENTAL=0 cargo build --profile ci-test --bin signaldb
```

`ci-test` reuses the artifacts CI-style test runs already built, so this takes
1–2 minutes on a warm target dir. Check `df -h /` first; stop below ~8 GB.

## Run against an isolated data dir

Never reuse `.data/`. Write a config into a temp dir. This one turns on
self-monitoring with CPU self-profiling, so `_system/_monitoring` fills with
traces, logs, metrics _and_ profiles within a minute and no producer is needed:

```bash
REPO=$(git rev-parse --show-toplevel); V=$(mktemp -d); mkdir -p $V/data/storage
cat > $V/signaldb.toml <<EOF
[database]
dsn = "sqlite://$V/data/signaldb.db"
[discovery]
dsn = "sqlite://$V/data/signaldb.db"
[storage]
dsn = "file://$V/data/storage"
[schema]
catalog_type = "sql"
catalog_uri = "sqlite://$V/data/iceberg_catalog.db"
[auth]
admin_api_key = "dev-admin-key"
[[auth.tenants]]
id = "dev"
slug = "dev"
name = "Development Tenant"
default_dataset = "local"
[[auth.tenants.api_keys]]
key = "dev-key-123"
name = "Development Key"
[[auth.tenants.datasets]]
id = "local"
slug = "local"
is_default = true
[self_monitoring]
enabled = true
endpoint = "http://localhost:4317"
tenant_id = "_system"
dataset_id = "_monitoring"
trace_sample_ratio = 1.0
profiles_enabled = true
profile_interval = "15s"
[[auth.tenants]]
id = "_system"
slug = "_system"
name = "System (Self-Monitoring)"
default_dataset = "_monitoring"
[[auth.tenants.api_keys]]
key = "dev-admin-key"
name = "Self-Monitoring Key"
[[auth.tenants.datasets]]
id = "_monitoring"
slug = "_monitoring"
is_default = true
EOF
cd $V && nohup $REPO/target/ci-test/signaldb --config $V/signaldb.toml > $V/mono.log 2>&1 &
until curl -sf localhost:3000/health >/dev/null; do sleep 2; done
```

Ports: router HTTP `:3000`, OTLP gRPC `:4317`, OTLP HTTP `:4318`. Service
ports come from CLI flags, not `*_HTTP_ADDR` env vars.

MCP is a sidecar, not part of the monolith (`[mcp].enabled` alone starts
nothing). Run it next to the monolith:

```bash
SIGNALDB__MCP__ENABLED=true SIGNALDB__MCP__BIND_ADDRESS=127.0.0.1:8228 \
SIGNALDB__MCP__ROUTER_URL=http://localhost:3000 \
  nohup $REPO/target/ci-test/signaldb mcp --config $V/signaldb.toml > $V/mcp.log 2>&1 &
```

## Drive it

### Query IR (first-party read path)

```bash
curl -s -X POST localhost:3000/api/v1/query \
  -H 'Authorization: Bearer dev-admin-key' -H 'X-Tenant-ID: _system' \
  -H 'X-Dataset-ID: _monitoring' -H 'Content-Type: application/json' \
  -d '{"irVersion":1,"from":"profiles","range":{"from":"now-10m","to":"now"},
       "result":"table","pipeline":[{"aggregate":{"by":["sample.type"],
       "aggs":[{"fn":"count","as":"n"}]}}]}'
```

- `table`/`rows` columns come back under **physical** names
  (`sample.type` → `sample_type`).
- Need a specific trace shape (cut roots, long spans)? POST OTLP/JSON to
  `localhost:4318/v1/traces` with `Authorization: Bearer dev-key-123` and
  `X-Tenant-ID: dev`. IDs are hex strings. Rows are queryable about 10 s later.

### MCP (Streamable HTTP)

Initialize, send `notifications/initialized`, then `tools/call`, all with
`Authorization: Bearer dev-admin-key`, `Accept: application/json,
text/event-stream` and the `mcp-session-id` header from the initialize
response. Replies are SSE: skip `data:` lines with an empty payload (priming
events) before parsing JSON. `completion/complete` with `ref/prompt` drives
prompt-argument completions.

### Explore UI

```bash
pnpm install --frozen-lockfile --filter ./src/ui...
cd src/ui && npx vite --port 5173 --strictPort &   # proxies to :3000
```

Drive it with Playwright from the workspace install:
`require('<repo>/node_modules/.pnpm/playwright@<ver>/node_modules/playwright')`,
`chromium.launch({ executablePath: '/opt/pw-browsers/chromium-1194/chrome-linux/chrome' })`.
Create a login user first:

```bash
curl -s -X POST localhost:3000/api/v1/users -H 'Authorization: Bearer dev-admin-key' \
  -H 'Content-Type: application/json' \
  -d '{"email":"verify@example.com","password":"verify-pass-123","tenant":"dev"}'
```

Log every response with `status >= 400` while driving a page. Those failures
are usually the finding. The generated client streams request bodies, so
`request.postData()` is empty; read the response body instead.

## Gotchas

- A shared `CARGO_TARGET_DIR` across worktrees can serve stale artifacts;
  `touch` the sources of the crates you changed before building.
- Building regenerates `Cargo.lock`; `git checkout -- Cargo.lock` afterwards.
  It is never committed.
- Compare against a deployed build (e.g. the hive MCP `query_ir` tool) to tell
  a regression from a pre-existing bug.
- Clean up: `pkill -f "$V/signaldb.toml"` and stop vite.
