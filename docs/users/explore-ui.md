---
audience: user
type: how-to
status: living
sources:
  - src/ui/**
  - src/router/src/ui.rs
  - src/router/src/endpoints/session.rs
---

# Explore UI

SignalDB ships a built-in explore UI for logs, traces, and metrics, served
by the router at `http://<router>:3000/ui/`. It consumes the same
Loki-, Tempo-, and Prometheus-compatible APIs that Grafana uses, so anything
visible in the UI is equally queryable from Grafana.

## What it does

- **Logs** — filter chips compiled to LogQL (with an "edit as text" escape
  hatch), a per-level volume histogram, a virtualized log list with
  per-attribute filter/exclude actions, a fields sidebar, and live tail.
- **Traces** — recent-trace search and open-by-ID, a waterfall with span
  details, and error highlighting.
- **Metrics** — a PromQL box charting range queries.
- **Correlation** — log rows with a `trace_id` open the trace waterfall;
  the span panel links back to logs filtered by that trace.
- Every view is a URL: time range, filters, and selection live in query
  parameters, so views can be bookmarked and shared.

## Signing in

On an embedded deployment (the UI served by the router at `/ui`), the
first query that fails as unauthenticated opens a sign-in form. Enter a
tenant API key, the tenant ID, and optionally a dataset (defaults to the
tenant's default dataset) — the same credentials any API client uses, see
[the authentication reference](authentication.md).

Signing in calls `POST /ui/session`, which validates the credentials and
sets an `HttpOnly`, `SameSite=Strict` session cookie. Subsequent API
requests from the browser authenticate through that cookie, so the key
never lives in page JavaScript, `localStorage`, or URLs. The session
lasts for the browser session; `DELETE /ui/session` (or clearing cookies)
ends it.

Once signed in, the tenant/dataset selector in the top bar shows the
session's tenant read-only and offers the tenant's datasets as a
drop-down (server support permitting; against older servers it falls back
to free-text fields). The explicit tenant/dataset chosen there is still
sent as `X-Tenant-ID`/`X-Dataset-ID` headers and wins over the cookie's
values.

In development the Vite proxy injects credentials from `.env.local`
instead, so no sign-in is needed.

## Availability

Container images (router and monolithic) ship the UI preinstalled. For
source builds, the router serves the directory named by `SIGNALDB_UI_DIR`:

```bash
pnpm install && pnpm ui:build          # builds src/ui/dist
SIGNALDB_UI_DIR=src/ui/dist cargo run --bin signaldb
```

Without `SIGNALDB_UI_DIR`, `/ui` serves a placeholder page. Setting the
variable to a directory without a built UI fails startup on purpose — a
misconfigured deployment should not silently ship without its UI.

## Developing the UI

See [src/ui/README.md](../../src/ui/README.md): `pnpm ui:dev` runs a Vite
dev server with hot reload that proxies API calls to any live SignalDB
instance (local or remote) with credentials injected from `.env.local`.
