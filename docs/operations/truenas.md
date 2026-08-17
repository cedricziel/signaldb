---
audience: operator
type: how-to
status: living
sources:
  - deploy/truenas/**
  - Dockerfile
---

# Deploying SignalDB on TrueNAS SCALE

SignalDB runs well as a TrueNAS SCALE **custom app** — a Docker Compose
project managed by the TrueNAS middleware. This page describes the setup the
maintainers run on their own NAS; the exact compose file is in the repo at
[`deploy/truenas/signaldb-app.yaml`](https://github.com/cedricziel/signaldb/blob/main/deploy/truenas/signaldb-app.yaml)
with secrets and hostnames replaced by placeholders.

The app is three services:

| Service    | Image                                                           | Purpose                                                                                                                                                                                                            |
| ---------- | --------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `signaldb` | `ghcr.io/cedricziel/signaldb:main` (or `:main-glibc-profiling`) | The monolith: OTLP ingest, storage, query API, Explore UI, compactor                                                                                                                                               |
| `mcp`      | `ghcr.io/cedricziel/signaldb/mcp:main`                          | [MCP server](../users/mcp.md) sidecar; reaches the monolith at `http://signaldb:3000` on the app network                                                                                                           |
| `otelcol`  | `otel/opentelemetry-collector-contrib`                          | Receives the monolith's [self-monitoring](self-monitoring-traces.md) telemetry, scrapes host/process/container metrics for the SignalDB containers, and writes it all back into the `_system`/`_monitoring` tenant |

Only `signaldb` is required. Drop `mcp` if you don't use MCP, and `otelcol` if
you don't want self-monitoring — in that case point
`SIGNALDB__SELF_MONITORING__ENDPOINT` at the monolith itself or leave
self-monitoring off.

## Prerequisites

1. A dataset for the app data, e.g. `tank/apps/signaldb`, **owned by uid/gid
   1000** (the images run as `1000:1000`). It ends up mounted at `/data` and
   holds `signaldb.toml`, the SQLite catalogs, the WAL and the Parquet storage.
2. A `signaldb.toml` in that dataset. Start from
   [`signaldb.dist.toml`](https://github.com/cedricziel/signaldb/blob/main/signaldb.dist.toml);
   the minimum for a homelab is `[auth]` with one tenant and an API key, plus
   `[self_monitoring] enabled = true` if you want the dogfooding data. The
   monolith uses `working_dir: /data`, so it picks up `./signaldb.toml`
   automatically and merges `SIGNALDB__<SECTION>__<KEY>` environment variables
   over it — anything in the compose `environment:` block overrides the file.
3. Free host ports. The example uses `4317`/`4318` (OTLP), `30200` (query API
   - UI) and `30228` (MCP); TrueNAS prefers high ports for non-well-known
     services, and `3000`/`3100` are usually taken by other apps.

## Install

In the TrueNAS UI: **Apps → Discover Apps → ⋮ → Install via YAML**, name it
`signaldb`, paste the compose file. Or from a shell on the NAS:

```bash
midclt call --job app.create '{
  "custom_app": true,
  "app_name": "signaldb",
  "custom_compose_config_string": "<contents of signaldb-app.yaml as one JSON string>"
}'
```

Before pasting, replace:

- `sk-REPLACE-system-ingest-key` — an **ingest-only** API key valid for the
  `_system` tenant. It appears three times: the browser telemetry key served to
  the UI (world-readable — never use an admin key), and twice in the collector
  config for the scraped host/container metrics.
- `o11y.example.com`, `o11y-mcp.example.com`, `o11y-ingest.example.com` — your
  public hostnames for the UI, the MCP endpoint and the browser OTLP endpoint.
  If nothing is exposed publicly, drop the `FRONTEND__*` and `MCP__OAUTH__*`
  variables and set `SIGNALDB__MCP__ALLOWED_HOSTS` to `<lan-ip>:30228` only.
- `/mnt/tank/apps/signaldb` — your dataset path.

Once running, the Explore UI is at `http://<nas>:30200/ui/`; log in with a
tenant id and API key from your `signaldb.toml`.

## Updating

The compose tracks moving tags (`:main`, `mcp:main`). Two middleware
behaviours matter here:

- **`app.redeploy` does not re-pull a moving tag** — it recreates the
  containers from the locally cached image. Use `app.pull_images` with
  `redeploy: true` instead; it pulls every image in the app and then redeploys:

  ```bash
  midclt call --job app.pull_images signaldb '{"redeploy": true}'
  ```

- **`app.update` expects the full compose config**, not a patch. To change one
  environment variable, fetch the current config with
  `midclt call app.config signaldb`, edit it, and send the whole thing back as
  `custom_compose_config_string`. Editing the files under
  `/mnt/.ix-apps/app_configs/` directly is not supported.

Confirm a deploy landed by **digest**, not by the image's `created` timestamp
(reproducible builds make that field misleading):

```bash
# on the NAS
midclt call app.image.query | jq -r '.[] | select(.repo_tags[]? | test("cedricziel/signaldb")) | "\(.repo_tags[0]) \(.repo_digests[0])"'
# anywhere with docker
docker buildx imagetools inspect ghcr.io/cedricziel/signaldb:main --format '{{.Manifest.Digest}}'
```

For a production install pin release tags (`ghcr.io/cedricziel/signaldb:0.3.0`)
and bump them in the compose instead of pulling `:main`.

## Notes on the example

- **Heap profiling.** `main-glibc-profiling` is the jemalloc build needed for
  `SIGNALDB__SELF_MONITORING__HEAP_PROFILES_ENABLED`; it also needs
  `MALLOC_CONF=prof:true`. With the standard `:main` image, drop both and keep
  CPU profiling (`PROFILES_ENABLED`) only. See [Binaries](binaries.md).
- **`otelcol` runs privileged-ish.** `pid: service:signaldb` + `SYS_PTRACE`
  let its `hostmetrics/process` scraper see the `signaldb` process, and the
  read-only Docker socket feeds `docker_stats`; the `filter/docker_scope`
  processor limits that to this app's containers (TrueNAS names them
  `ix-<app>-<service>-N`). Remove those three settings if you only want the
  OTLP pass-through pipelines.
- **Auth headers through the collector.** The monolith's own exports carry
  `Authorization`/`X-Tenant-ID`/`X-Dataset-ID`; the `headers_setter`
  extension forwards them from the request context, so no tenant is hard-coded
  on that path. Only the scraped metrics (no caller) use the static headers.
- **MCP host guard.** The MCP server rejects non-loopback `Host` headers
  unless listed in `SIGNALDB__MCP__ALLOWED_HOSTS`; a reverse proxy that
  forwards the original `Host` needs the public hostname there, LAN clients
  need `<ip>:<port>`.
- **Memory.** `mem_limit: 6g` pairs with `[querier] memory_limit_mb = 4096` in
  `signaldb.toml`; scale both together.
