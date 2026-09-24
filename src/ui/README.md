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
pnpm --filter signaldb-ui test:e2e       # mocked Playwright suite (src/ui/e2e)
pnpm --filter signaldb-ui test:e2e:live  # live Playwright suite (src/ui/e2e-live)
```

## End-to-end tests

- `e2e/` (`pnpm test:e2e`): routing and navigation tests against the
  production build, with mocked API calls. No Rust toolchain needed.
- `e2e-live/` (`pnpm test:e2e:live`): the UI against a real, seeded
  `signaldb` backend. Any rejected API call fails the test. See
  [docs/contributing/ui-e2e-live.md](../../docs/contributing/ui-e2e-live.md).
