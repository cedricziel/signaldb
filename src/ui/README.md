# SignalDB UI

Native explore UI for SignalDB: logs, traces, and metrics against the
router's query APIs. Ships embedded in the router binary at `/ui/`.

## Development

```bash
cp src/ui/.env.example src/ui/.env.local   # set target + credentials
pnpm install
pnpm ui:dev                                # http://localhost:5173/ui/
```

The dev server hot-reloads and proxies all API paths (`/loki`, `/tempo`,
`/prometheus`, `/api`, `/ui/session`) to `SIGNALDB_TARGET` — a local
`./scripts/run-dev.sh` instance or any live deployment. Auth headers are
injected by the proxy from `.env.local`, so browser code is identical to
the embedded production build and API keys never reach the client. In the
embedded build there is no proxy; queries rejected with 401 open a login
form that creates an HttpOnly session cookie via `POST /ui/session` (see
`docs/users/explore-ui.md`).

## Commands

```bash
pnpm ui:dev        # dev server with HMR
pnpm ui:build      # typecheck + production build to src/ui/dist
pnpm ui:test       # vitest, single run
pnpm --filter signaldb-ui test:watch
pnpm --filter signaldb-ui test:coverage
pnpm --filter signaldb-ui lint
```
