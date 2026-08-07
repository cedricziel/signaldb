## 1. tempo.ts (no backend dependency)

- [ ] 1.1 Confirm `src/api/gen/sdk.gen.ts` exposes operations for `search`, `search/tags`, `search/tag/{name}/values`, `traces/{id}` with real types (already true today).
- [ ] 1.2 Update `tempo.test.ts` to mock the generated client instead of `global.fetch`, keeping existing test cases.
- [ ] 1.3 Replace `tempoFetch` with calls to the generated SDK functions inside `tempoGetTrace`/`tempoSearch`; keep `flattenAttrs`/`toSpan`/`rootSpan` and the exported `TempoSpan`/`TraceSummary`/`TempoTrace` types unchanged.
- [ ] 1.4 Wrap generated-client errors into `ApiError` with equivalent `message`/`status` to preserve `isAuthError` behavior for existing callers.
- [ ] 1.5 `pnpm --filter signaldb-ui test` and `pnpm --filter signaldb-ui typecheck` for `tempo.ts`/`tempo.test.ts`.

## 2. loki.ts (depends on spec-cover-compat-endpoints)

- [ ] 2.1 Update `loki.test.ts` to mock the generated client instead of `global.fetch`.
- [ ] 2.2 Replace `lokiFetch` with generated SDK calls inside `lokiQueryLogs`, `lokiQueryHistogram`, `lokiLabels`, `lokiLabelValues`.
- [ ] 2.3 Delete `LokiStreamResult`/`LokiMatrixResult`/`LokiResponse` in favor of the generated response types; keep `LogRow`/`HistogramSeries` and the ns→ms conversion/merge-sort logic.
- [ ] 2.4 Wrap generated-client errors into `ApiError`.
- [ ] 2.5 `pnpm --filter signaldb-ui test` and `typecheck` for `loki.ts`/`loki.test.ts`.

## 3. prom.ts (depends on spec-cover-compat-endpoints)

- [ ] 3.1 Add/update `prom.test.ts` to mock the generated client instead of `global.fetch`.
- [ ] 3.2 Replace raw `fetch` with generated SDK calls inside `promQueryRange`, `promLabelNames`, `promLabelValues`, `promMetricNames`, `promLabelStats`.
- [ ] 3.3 Delete `PromMatrixResult`/`PromResponse`/`PromMetadataResponse`/`LabelStatsResponse` in favor of generated types; keep `PromSeries`, `seriesName`, and `LabelStat` if it's a genuine narrowing with computed fields, otherwise consume the generated type directly.
- [ ] 3.4 Wrap generated-client errors into `ApiError`.
- [ ] 3.5 `pnpm --filter signaldb-ui test` and `typecheck` for `prom.ts`/`prom.test.ts`.

## 4. pyroscope.ts (depends on spec-cover-compat-endpoints)

- [ ] 4.1 Add `pyroscope.test.ts` (none exists today) covering `pyroscopeProfileTypes`, `pyroscopeServices`, `pyroscopeRender`, mocking the generated client.
- [ ] 4.2 Replace `pyroscopeFetch` with generated SDK calls.
- [ ] 4.3 Evaluate the generated `Flamebearer`/`RenderResponse` schema fidelity (per `spec-cover-compat-endpoints`'s risk note); keep hand-written types only if the generated schema is too weak to consume directly.
- [ ] 4.4 Wrap generated-client errors into `ApiError`.
- [ ] 4.5 `pnpm --filter signaldb-ui test` and `typecheck` for `pyroscope.ts`/`pyroscope.test.ts`.

## 5. session.ts (depends on spec-cover-compat-endpoints)

- [ ] 5.1 Verify the generated client sends the `signaldb_session` cookie on same-origin requests without extra config (design.md Open Question); adjust `client.ts`'s config if it doesn't.
- [ ] 5.2 Update `session.test.ts` to mock the generated client instead of `global.fetch`.
- [ ] 5.3 Replace raw `fetch` with generated SDK calls inside `createSession`, `deleteSession`, `whoami`.
- [ ] 5.4 Delete `SessionResult`/`WhoamiResponse`/`WhoamiDataset` and nested whoami types in favor of the generated types; keep `SessionCredentials`/`SessionMembership` only if they add real narrowing.
- [ ] 5.5 Wrap generated-client errors into `ApiError`, preserving the existing login-failure message extraction (`body?.error`).
- [ ] 5.6 `pnpm --filter signaldb-ui test` and `typecheck` for `session.ts`/`session.test.ts`.

## 6. Whole-UI verification

- [ ] 6.1 `pnpm --filter signaldb-ui build` (runs `tsc --noEmit` + `vite build`) to confirm no consumer of these five files broke.
- [ ] 6.2 `pnpm --filter signaldb-ui test:e2e` (Playwright) for the explore views that exercise logs/traces/metrics/profiles/login.
- [ ] 6.3 Manually smoke-test login → signal views → logout against a running dev server, confirming cookie auth still works end-to-end through the generated client.

## 7. Docs

- [ ] 7.1 Update any UI-architecture doc or skill (e.g. `frontend-instrumentation`) that references the old hand-written client files, if it names them.
