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

Two separate Playwright suites, on purpose:

- **`e2e/` (`pnpm test:e2e`, `playwright.config.ts`)** — browser-level
  routing/navigation tests against the production build with no backend.
  Specs mock the specific API calls a scenario needs via `page.route` and
  assert on URL/DOM structure, never on data content. Fast, no Rust
  toolchain required.
- **`e2e-live/` (`pnpm test:e2e:live`, `playwright.live.config.ts`)** — the
  same UI against a **real** `signaldb` monolith, seeded with real OTLP
  traces/logs/metrics via `signal-producer` (see
  `src/signal-producer/src/topology.rs`). Global setup (`e2e-live/global-setup.ts`)
  builds `signaldb`, `signaldb-cli`, and `signal-producer`, starts the
  monolith against a throwaway temp dir (SQLite catalog, local file
  storage, a shrunk `[wal].flush_interval` so seeded data is queryable
  within seconds), bootstraps a login user, seeds the rideshare estate, and
  waits for the data to come back from `POST /api/v1/query` before running
  any spec. Every spec fails on any `/api/v1/*` response ≥400 or uncaught
  page error (`e2e-live/fixtures.ts`), so a Query IR document a page sends
  that the server rejects — e.g. ordering by a physical column instead of a
  logical one — fails the test instead of silently mocking around it. Needs
  a Rust toolchain on `PATH` and takes noticeably longer (a Rust build plus
  a live backend); run it before shipping a change to a page's query logic,
  not on every save.
