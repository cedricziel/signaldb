> **Reconciled 2026-10-01.** The UI stopped consuming the Tempo/Loki/
> Prometheus/Pyroscope compatibility APIs altogether: every first-party read
> now goes through the Query IR (`src/api/queryIr.ts` and the `*Ir`/signal
> modules on the generated `queryIr` operation), and `tempo.ts`, `loki.ts`,
> `prom.ts`, and `pyroscope.ts` were deleted instead of migrated. Groups 1–4
> are superseded by that. `session.ts` is half-migrated: the endpoints that
> are in the OpenAPI document go through the generated client, the rest are
> descoped below to a follow-up because they need router OpenAPI work first.

## 1. tempo.ts (no backend dependency)

- [x] 1.1 ~~Confirm generated Tempo operations~~ Superseded: `src/ui/src/api/tempo.ts` no longer exists; trace reads go through the generated `queryIr` (`traceDetail.ts`, `traceGroups.ts`, `traceFacets.ts`, …).
- [x] 1.2 ~~Update `tempo.test.ts`~~ Superseded (file deleted).
- [x] 1.3 ~~Replace `tempoFetch`~~ Superseded (file deleted).
- [x] 1.4 Generated-client errors map to `ApiError` through the shared `unwrapSdkResult` in `src/api/http.ts`, preserving `isAuthError`.
- [x] 1.5 ~~Per-file test/typecheck~~ Superseded (file deleted).

## 2. loki.ts (depends on spec-cover-compat-endpoints)

- [x] 2.1–2.5 Superseded: `src/ui/src/api/loki.ts` no longer exists; log reads go through the generated `queryIr`.

## 3. prom.ts (depends on spec-cover-compat-endpoints)

- [x] 3.1–3.5 Superseded: `src/ui/src/api/prom.ts` no longer exists; metric reads go through the generated `queryIr` (`entityMetricSeries.ts`, `operationSeries.ts`, …).

## 4. pyroscope.ts (depends on spec-cover-compat-endpoints)

- [x] 4.1–4.5 Superseded: `src/ui/src/api/pyroscope.ts` no longer exists; profile reads go through the generated `queryIr` (`profilesIr.ts`).

## 5. session.ts (depends on spec-cover-compat-endpoints)

- [x] 5.1 The generated client is same-origin with default credentials and carries the session cookie: `loginConfig()` and `currentSession()` (cookie-only auth) already run through it in production.
- [ ] 5.2 ~~Update `session.test.ts` to mock the generated client~~ Descoped with 5.3.
- [ ] 5.3 ~~Replace raw `fetch` inside `createSession`, `deleteSession`, `whoami`~~ Descoped to follow-up `ui-session-endpoints-in-openapi`: `POST`/`DELETE /ui/session` have no `#[utoipa::path]` (no generated operation exists), and the documented `whoami` response (`WhoamiIdentityResponse`) omits `datasets`, `default_dataset`, `user`, and `memberships`, which the UI reads. Both need router OpenAPI changes and client regeneration first. Recorded under "Known gaps" in `docs/architecture/openapi-codegen.md`.
- [ ] 5.4 ~~Delete `SessionResult`/`WhoamiResponse`/`WhoamiDataset`~~ Descoped with 5.3 (the generated types don't cover them yet).
- [x] 5.5 Errors map to `ApiError` in both paths: `unwrapSdkResult` for the generated calls, the hand-written `body?.error` extraction for `createSession`.
- [ ] 5.6 ~~Per-file test/typecheck~~ Descoped with 5.3.

## 6. Whole-UI verification

- [x] 6.1 `pnpm --filter ./src/ui typecheck` passes on main with the compat clients gone.
- [ ] 6.2 ~~Playwright e2e for the compat-backed explore views~~ Descoped: those views no longer call the compat clients this check was meant to cover.
- [ ] 6.3 ~~Manual login → signal views → logout smoke~~ Descoped with 5.3 (login/logout are the unmigrated calls).

## 7. Docs

- [x] 7.1 No UI doc or skill names the deleted compat client files (`frontend-instrumentation` only names `telemetry/session.ts`, which is unrelated). The remaining session gap is recorded in `docs/architecture/openapi-codegen.md`.
