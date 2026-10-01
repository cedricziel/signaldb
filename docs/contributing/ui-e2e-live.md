---
audience: contributor
type: how-to
status: living
sources:
  - src/ui/e2e-live/**
  - src/ui/playwright.live.config.ts
---

# Run the UI end-to-end tests against a live backend

The UI has two Playwright suites:

| Suite  | Directory          | Command              | Backend                                           |
| ------ | ------------------ | -------------------- | ------------------------------------------------- |
| Mocked | `src/ui/e2e/`      | `pnpm test:e2e`      | None. Specs mock API calls with `page.route`.     |
| Live   | `src/ui/e2e-live/` | `pnpm test:e2e:live` | A real `signaldb` monolith with seeded telemetry. |

Use the live suite when you change a page that builds a Query IR document.
The mocked suite cannot detect a document that the server rejects, because
it never sends one to a server.

## Prerequisites

- A Rust toolchain on `PATH`.
- Node 24 and pnpm.
- Ports 3000 and 4317 free. The monolith uses fixed ports, so stop any
  local `signaldb` first.

## Run the suite

```bash
pnpm --filter signaldb-ui test:e2e:live
```

The script does these steps:

1. Builds `signaldb`, `signaldb-cli` and `signal-producer` (debug profile),
   then builds the UI bundle.
2. Starts the monolith against a temporary directory with a SQLite catalog,
   local file storage and a 1-second WAL flush interval.
3. Creates a login user, then seeds the `rideshare` estate with
   `signal-producer`.
4. Polls `POST /api/v1/query` until the seeded traces are queryable.
5. Runs the specs in `src/ui/e2e-live/` serially against one backend.
6. Stops the backend and deletes the temporary directory.

## What fails a test

The fixture in `src/ui/e2e-live/fixtures.ts` fails a test when:

- any `/api/v1/*` response has status 400 or higher. The failure message
  contains the request body and the response body.
- the page throws an uncaught error.

Specs also check that seeded data is visible, for example the seeded
service name on the catalog page.

## Add a spec

1. Create `src/ui/e2e-live/<page>.spec.ts`.
2. Import `test` and `expect` from `./fixtures`, not from
   `@playwright/test`. Otherwise the API-failure check does not run.
3. Read the tenant, dataset and seeded service from the `liveEnv` fixture.

## CI

The `UI E2E (live backend)` job in `.github/workflows/ci.yml` runs the suite
when core or UI code changes. It does not compile anything. It downloads
these artifacts from other jobs:

- `musl-binaries-amd64`: the release `signaldb` and `signaldb-cli`
  binaries that the Docker images ship.
- `e2e-tools-amd64`: the `signal-producer` binary.
- `ui-dist`: the production UI bundle.

The job sets `SIGNALDB_E2E_BIN_DIR` to the download directory, and global
setup starts the binaries from there instead of `target/debug`. You can set
the same variable locally to test prebuilt binaries.

When the job fails, download the `playwright-report-live` artifact for
traces and screenshots.
