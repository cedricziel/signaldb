# Changelog

## [0.3.0](https://github.com/cedricziel/signaldb/compare/signaldb-cli-v0.1.3...signaldb-cli-v0.3.0) (2026-08-08)


### ⚠ BREAKING CHANGES

* **cli+mcp:** signaldb-cli tenant/api-key/dataset commands move under `admin` (e.g. `signaldb-cli admin tenant list`), and queries now require a language flag (`signaldb-cli query --sql|--promql|--logql|--traceql|--ir`). No back-compat aliases are provided (post-1.0).

### Features

* **api:** code-first OpenAPI — generate spec + Rust/TS clients from annotations ([#856](https://github.com/cedricziel/signaldb/issues/856)) ([e34fbfb](https://github.com/cedricziel/signaldb/commit/e34fbfbd094034416f78597c59b306975dd97271))
* **cli+mcp:** CLI & MCP as pure SDK consumers — query --&lt;lang&gt;, admin grouping (Phase 1) ([#892](https://github.com/cedricziel/signaldb/issues/892)) ([92a439e](https://github.com/cedricziel/signaldb/commit/92a439e112da96029733d93db7f274c20c29cbc5))
* metric/label discovery (MCP+CLI+SDK) and prom/loki UI migration ([#1041](https://github.com/cedricziel/signaldb/issues/1041)) ([afcc72e](https://github.com/cedricziel/signaldb/commit/afcc72e9f87a45e74c97171e8919b90868cd54f4))
* native Query IR — versioned structured query surface (query-ir-core) ([#882](https://github.com/cedricziel/signaldb/issues/882)) ([8774ac0](https://github.com/cedricziel/signaldb/commit/8774ac0fbbe4686cb7aa8b0bba73dbc25f185689))


### Code Refactoring

* **cli:** make signaldb-cli depend only on the SDK (+ create_user API) ([#874](https://github.com/cedricziel/signaldb/issues/874)) ([8e5cce5](https://github.com/cedricziel/signaldb/commit/8e5cce56c821d69917b55cc8c21a9a2ef55864b7))


### Tests

* delete tautological tests and rewrite salvageable ones as contract tests ([#961](https://github.com/cedricziel/signaldb/issues/961)) ([b3e884a](https://github.com/cedricziel/signaldb/commit/b3e884ad59b4df853429133d5eef2724a8adcada))
* exercise real implementations instead of test-local copies ([#964](https://github.com/cedricziel/signaldb/issues/964)) ([e142b3d](https://github.com/cedricziel/signaldb/commit/e142b3d006065205c7194fd22c4ca4e182402f55))
* make tests assert what their names promise ([#966](https://github.com/cedricziel/signaldb/issues/966)) ([446ed06](https://github.com/cedricziel/signaldb/commit/446ed062a7480902ef391884b1c2e12f77ddd66f))
* polish medium/low audit findings across the workspace ([#969](https://github.com/cedricziel/signaldb/issues/969)) ([8962f6d](https://github.com/cedricziel/signaldb/commit/8962f6d1d22c8a176d4a1d99376d61b42b1da258))
* replace sleep-based synchronization with deterministic waits ([#968](https://github.com/cedricziel/signaldb/issues/968)) ([6391326](https://github.com/cedricziel/signaldb/commit/6391326013c8620f186e4a63c2cdf3bbdf9ee963))

## [0.1.3](https://github.com/cedricziel/signaldb/compare/signaldb-cli-v0.1.2...signaldb-cli-v0.1.3) (2026-07-30)

## [0.1.2](https://github.com/cedricziel/signaldb/compare/signaldb-cli-v0.1.1...signaldb-cli-v0.1.2) (2026-07-30)


### Features

* add tenant management admin API with OpenAPI spec, SDK, and CLI ([#313](https://github.com/cedricziel/signaldb/issues/313)) ([880c86b](https://github.com/cedricziel/signaldb/commit/880c86b6405a162c84fe88615b7d363585948abd))
* **cli:** add HTTP admin API client for TUI ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** add shell completions with dynamic tenant completion ([#791](https://github.com/cedricziel/signaldb/issues/791)) ([f0133ef](https://github.com/cedricziel/signaldb/commit/f0133ef06fee1a3aea0f9c28e85817df980adc8a))
* **cli:** add terminal UI with traces, logs, metrics, admin, and dashboard tabs ([#458](https://github.com/cedricziel/signaldb/issues/458)) ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** add user bootstrap command ([df8be95](https://github.com/cedricziel/signaldb/commit/df8be951870e83eace8d25c4da21ee02c309fc58))
* **cli:** implement Admin tab with tenant/key/dataset CRUD and confirmations ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** implement Logs tab with Flight SQL query interface ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** implement Metrics tab with sparklines and Flight SQL ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** integrate TUI tabs with help overlay and error handling ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* end-to-end local development experience with CLI query support ([#434](https://github.com/cedricziel/signaldb/issues/434)) ([b95fb15](https://github.com/cedricziel/signaldb/commit/b95fb1595e33dd825f3c4424a88b966dded4808e))


### Continuous Integration

* drop MSRV policy and fix security audit ignores ([#521](https://github.com/cedricziel/signaldb/issues/521)) ([7da71e3](https://github.com/cedricziel/signaldb/commit/7da71e3d78f593a4361f403e2d4be1e426fb8807))
