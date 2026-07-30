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
first query that fails as unauthenticated opens a sign-in form asking only
for a user email and password. Accounts that belong to a single tenant land
directly in it (on its default dataset); accounts spanning several tenants
pick one from a selector listing each membership by name and role. See
[the authentication reference](authentication.md).

![The post-login tenant selector listing each membership with its name and role](../assets/screenshots/login-tenant-selector.png)

Signing in calls `POST /ui/session`, which validates the credentials and
sets an `HttpOnly`, `Secure`, `SameSite=Strict` cookie containing an opaque
random token. The password and tenant API keys never live in the cookie,
page JavaScript, `localStorage`, or URLs. Sessions expire after 12 hours;
`DELETE /ui/session` revokes the server-side session and clears the cookie.

Once signed in, the tenant/dataset selector offers the user's tenant
memberships and the selected tenant's datasets. The chosen values are sent
as `X-Tenant-ID`/`X-Dataset-ID`; the server validates the tenant against the
current user's memberships.

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
