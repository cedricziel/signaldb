# Changelog

## [0.5.0](https://github.com/cedricziel/signaldb/compare/signaldb-cli-v0.4.1...signaldb-cli-v0.5.0) (2026-10-08)


### ⚠ BREAKING CHANGES

* **mcp-server:** discover_attributes and discover_metrics return the Query IR metadata envelope (logical dotted names, levels, cost) instead of a Tempo tag list, a Loki/Prometheus {status,data} array, or Pyroscope {names}. scope narrows by attribute level rather than routing to the Tempo v2 endpoints, and tag values need a declared set, a statistics sketch, or sample: true.
* **signaldb-cli:** discover attributes and discover metrics print the Query IR metadata envelope (logical dotted names, levels, cost) instead of a Tempo tag list, a Loki/Prometheus {status,data} array, or the PromQL __name__ values. --scope narrows by attribute level rather than routing to the Tempo v2 endpoints, and tag values need a declared set, a statistics sketch, or --sample.
* **tests-integration:** metric tables are recreated as metrics/metric_exemplars; pre-cutover metric data is dropped, not migrated.
* metric tables are recreated as metrics/metric_exemplars; pre-cutover metric data is dropped, not migrated.

### Features

* **cli,mcp:** follow a Query IR live tail through the IR endpoint ([#2150](https://github.com/cedricziel/signaldb/issues/2150)) ([1219d9d](https://github.com/cedricziel/signaldb/commit/1219d9d971107f485ffb7a4737bda901eceaedc0))
* **cli,mcp:** page Query IR results through the IR endpoint ([#2144](https://github.com/cedricziel/signaldb/issues/2144)) ([9fc147d](https://github.com/cedricziel/signaldb/commit/9fc147d416a5329a31040af566424f40db3e5f6c))
* cut metrics over to the typed metrics layout ([#1928](https://github.com/cedricziel/signaldb/issues/1928)) ([e416d6f](https://github.com/cedricziel/signaldb/commit/e416d6f79029d36930b0b98a288517ac18a0b7ff))
* **evals:** build eval sets from traces ([#1847](https://github.com/cedricziel/signaldb/issues/1847)) ([dfe9e60](https://github.com/cedricziel/signaldb/commit/dfe9e608f9629abee1a542f5020f164d193b8faf))
* **evals:** eval sets pages, upload dialog and saving regressions in the UI ([#1871](https://github.com/cedricziel/signaldb/issues/1871)) ([cb05278](https://github.com/cedricziel/signaldb/commit/cb05278065450372e37f51fc149f4ac04a6ad6a7))
* **evals:** list and compare eval runs from MCP and the CLI ([#1866](https://github.com/cedricziel/signaldb/issues/1866)) ([665f458](https://github.com/cedricziel/signaldb/commit/665f4588808aad9ae567df358c7732f70881aec9))
* **evals:** upload eval results and gate CI on them ([#1857](https://github.com/cedricziel/signaldb/issues/1857)) ([175155a](https://github.com/cedricziel/signaldb/commit/175155a3a2d958d59e16a05961206c0b09f1e79a))
* **mcp-server:** discover attributes and metrics through the Query IR ([#2077](https://github.com/cedricziel/signaldb/issues/2077)) ([37d58ea](https://github.com/cedricziel/signaldb/commit/37d58eac6e5ac2b3766b324e494f0c4901812a5a))
* **processors:** diff Test panel output against the server's decoded input ([#1883](https://github.com/cedricziel/signaldb/issues/1883)) ([7711bc5](https://github.com/cedricziel/signaldb/commit/7711bc5bca22d010fb83766449d52ccdd44291e0))
* **query-ir:** differential flamegraph over a baseline window (irVersion 13) ([#2100](https://github.com/cedricziel/signaldb/issues/2100)) ([5e36ddb](https://github.com/cedricziel/signaldb/commit/5e36ddbea6b114c7c2077eca2ee28b9b0fa8089f))
* **query-ir:** trace result envelope (irVersion 12) ([#2063](https://github.com/cedricziel/signaldb/issues/2063)) ([934b6bc](https://github.com/cedricziel/signaldb/commit/934b6bccf5d5fdb12595c07191804091664ae145))
* recognise AI agents as the gen_ai.agent entity ([#1803](https://github.com/cedricziel/signaldb/issues/1803)) ([e0fcaa0](https://github.com/cedricziel/signaldb/commit/e0fcaa0b3cd8133e23e0ccc8f9e28e43986c41f5))
* **router:** eval sets API for offline agent evals ([#1837](https://github.com/cedricziel/signaldb/issues/1837)) ([b3ce35a](https://github.com/cedricziel/signaldb/commit/b3ce35a7fb9671494e4d78cd29b2673f20205531))
* **router:** live-tail Query IR rows and trace results (IR v15) ([#2148](https://github.com/cedricziel/signaldb/issues/2148)) ([57ac3c7](https://github.com/cedricziel/signaldb/commit/57ac3c74646037f2198b86f787da3b7a161d6ffd))
* **router:** paginate Query IR rows and trace results (IR v14) ([#2142](https://github.com/cedricziel/signaldb/issues/2142)) ([67023cc](https://github.com/cedricziel/signaldb/commit/67023cc0f71ab345f23d6278f9a917ee3d349941))
* **router:** report the oldest data a query's source holds ([#2191](https://github.com/cedricziel/signaldb/issues/2191)) ([7911eb5](https://github.com/cedricziel/signaldb/commit/7911eb512868a40075187e47fdce0303cf1507a0))
* **router:** report the retention that applies to a query ([#2182](https://github.com/cedricziel/signaldb/issues/2182)) ([91ca14d](https://github.com/cedricziel/signaldb/commit/91ca14dfc6f4478155ef9095c1b84e4078a6da90))
* **router:** scalar result envelope and metric Series labels ([#2001](https://github.com/cedricziel/signaldb/issues/2001)) ([2d72dc1](https://github.com/cedricziel/signaldb/commit/2d72dc1b719ee880ad87101fc042d21465f3affc))
* **schema-registry:** accept definition/2 custom registry uploads ([#1823](https://github.com/cedricziel/signaldb/issues/1823)) ([a8c8196](https://github.com/cedricziel/signaldb/commit/a8c819648bd5778478be6e86241802ae4f6f880f))
* **signaldb-cli:** discover attributes and metrics through the Query IR ([#2076](https://github.com/cedricziel/signaldb/issues/2076)) ([f398305](https://github.com/cedricziel/signaldb/commit/f398305ed42d5833150e2e19c5f0c7bb90d411d5))


### Bug Fixes

* **router:** read data for sample:true and flag partial discovery statistics ([#2176](https://github.com/cedricziel/signaldb/issues/2176)) ([ab5dcf7](https://github.com/cedricziel/signaldb/commit/ab5dcf78f47567a6e2a8f8a10dcf5dfd06cdfed5))


### Tests

* **tests-integration:** add an end-to-end metrics cutover test ([#1929](https://github.com/cedricziel/signaldb/issues/1929)) ([bb47677](https://github.com/cedricziel/signaldb/commit/bb4767732f40a82ca1af1e4eab88a8dc11580829))


### Build System

* **deps:** bump object from 0.37.3 to 0.39.1 ([#2203](https://github.com/cedricziel/signaldb/issues/2203)) ([b6de77a](https://github.com/cedricziel/signaldb/commit/b6de77a4378388901382c517ed72e7a93c309c48))
* fix the beta test leg for cargo's unused-dependency lints ([#2047](https://github.com/cedricziel/signaldb/issues/2047)) ([6867d69](https://github.com/cedricziel/signaldb/commit/6867d69dceb26f2e55ccfac31e60ae42aad76418))

## [0.4.1](https://github.com/cedricziel/signaldb/compare/signaldb-cli-v0.4.0...signaldb-cli-v0.4.1) (2026-09-23)


### Features

* GitHub App integration for connecting a tenant's repositories ([#1600](https://github.com/cedricziel/signaldb/issues/1600)) ([6c9721e](https://github.com/cedricziel/signaldb/commit/6c9721ef0cf630df85e227a07be6ae30ee263191))
* per-API-key allowed origins for browser (CORS) ingestion ([#1548](https://github.com/cedricziel/signaldb/issues/1548)) ([6e966dd](https://github.com/cedricziel/signaldb/commit/6e966ddaf2740e3648583223828c6af715b6d331))
* per-tenant, per-dataset OTTL telemetry processors ([#1603](https://github.com/cedricziel/signaldb/issues/1603)) ([2fc1022](https://github.com/cedricziel/signaldb/commit/2fc102232b1d925418b02e68393af8917184016e))
* **router:** attach an existing GitHub App installation to a tenant ([#1618](https://github.com/cedricziel/signaldb/issues/1618)) ([0ab8e95](https://github.com/cedricziel/signaldb/commit/0ab8e95581f5213b02e8cded8af5b2b71c827516))
* **router:** serialize API timestamps as native UTC DateTime ([#1643](https://github.com/cedricziel/signaldb/issues/1643)) ([1327fae](https://github.com/cedricziel/signaldb/commit/1327fae5760510f7e2180ab9e323ba6961d8b657))
* source context for stack frames from linked GitHub repositories ([#1601](https://github.com/cedricziel/signaldb/issues/1601)) ([acca49c](https://github.com/cedricziel/signaldb/commit/acca49ca770ab96464b12676211144bc20b5cc7c))


### Code Refactoring

* remove cross-crate dead code ([#1647](https://github.com/cedricziel/signaldb/issues/1647)) ([8b5b1d9](https://github.com/cedricziel/signaldb/commit/8b5b1d98f1150e75a8306beea29bb90465a4f921))

## [0.4.0](https://github.com/cedricziel/signaldb/compare/signaldb-cli-v0.3.0...signaldb-cli-v0.4.0) (2026-09-12)


### Features

* **auth:** OIDC login (relying-party SSO) ([#1485](https://github.com/cedricziel/signaldb/issues/1485)) ([c681bee](https://github.com/cedricziel/signaldb/commit/c681bee369d9a1b636357edf70b6f88f236b96a2))
* implement multi-dataset restriction for API keys and OAuth grants ([#1475](https://github.com/cedricziel/signaldb/issues/1475)) ([11deba9](https://github.com/cedricziel/signaldb/commit/11deba995c6937324576f87e87284a1580faa624))
* **router:** serve query discovery from the registry and statistics ([#1312](https://github.com/cedricziel/signaldb/issues/1312)) ([41d2738](https://github.com/cedricziel/signaldb/commit/41d27384df6e90bd9e9731218e084dd27581e20b))
* **schema-registry:** accept keys= batch resolution on GET /api/v1/schema/metrics ([#1508](https://github.com/cedricziel/signaldb/issues/1508)) ([6facbdc](https://github.com/cedricziel/signaldb/commit/6facbdcd182285bf54c1d2e922724d6bdeb6bae6))
* self-serve connection details for agents ([public] config, /api/v1/connection, MCP connection_info) ([#1474](https://github.com/cedricziel/signaldb/issues/1474)) ([ad78cd1](https://github.com/cedricziel/signaldb/commit/ad78cd1981282426b65b7dcac50ddc38eeea7f80))


### Code Refactoring

* **cli:** quality pass on signaldb-cli TUI (simplify) ([#1328](https://github.com/cedricziel/signaldb/issues/1328)) ([ee11e5f](https://github.com/cedricziel/signaldb/commit/ee11e5ff6c2a5cee658cc84a276e41503049a76c))

## [0.3.0](https://github.com/cedricziel/signaldb/compare/signaldb-cli-v0.1.3...signaldb-cli-v0.3.0) (2026-08-17)

> **Note:** this release jumps `signaldb-cli` from the `0.1.x` line straight to `0.3.0`. `signaldb-cli` now versions in lockstep with the other core crates (`signaldb-bin`, `acceptor`, `router`, `writer`, `querier`, `compactor`, `common`) through a release-please `linked-versions` group named `signaldb-core`, so it adopted the group's highest version. The jump is pure harmonization — there is no additional feature scope behind the skipped `0.2.x` line.


### ⚠ BREAKING CHANGES

* **auth:** POST /api/v1/admin/tenants/{id}/api-keys requires a non-empty `scopes` array; bodies without it are rejected.
* **cli+mcp:** signaldb-cli tenant/api-key/dataset commands move under `admin` (e.g. `signaldb-cli admin tenant list`), and queries now require a language flag (`signaldb-cli query --sql|--promql|--logql|--traceql|--ir`). No back-compat aliases are provided (post-1.0).

### Features

* **api:** code-first OpenAPI — generate spec + Rust/TS clients from annotations ([#856](https://github.com/cedricziel/signaldb/issues/856)) ([e34fbfb](https://github.com/cedricziel/signaldb/commit/e34fbfbd094034416f78597c59b306975dd97271))
* **auth:** schema:read/schema:write API-key scopes, scopes on every key surface ([#1217](https://github.com/cedricziel/signaldb/issues/1217)) ([34c7a28](https://github.com/cedricziel/signaldb/commit/34c7a28e4e62fad7a05089c1a3543739d6e28450))
* **auth:** tenant:manage API-key scope for the tenant management API ([#1266](https://github.com/cedricziel/signaldb/issues/1266)) ([9dfc193](https://github.com/cedricziel/signaldb/commit/9dfc193a85e813b42f8658bf97cbfd30e3b78f2e))
* **cli+mcp:** CLI & MCP as pure SDK consumers — query --&lt;lang&gt;, admin grouping (Phase 1) ([#892](https://github.com/cedricziel/signaldb/issues/892)) ([92a439e](https://github.com/cedricziel/signaldb/commit/92a439e112da96029733d93db7f274c20c29cbc5))
* **clients:** schema registry in SDK, CLI, and MCP ([#1223](https://github.com/cedricziel/signaldb/issues/1223)) ([1838583](https://github.com/cedricziel/signaldb/commit/1838583910be33e03d72b2be15e17d819031c9c5))
* **mcp-admin-tool-parity:** platform-admin and tenant self-management tool/CLI parity ([#1261](https://github.com/cedricziel/signaldb/issues/1261)) ([1eadc72](https://github.com/cedricziel/signaldb/commit/1eadc728ace70aff10fa01aaa8766012ace2df4c))
* metric/label discovery (MCP+CLI+SDK) and prom/loki UI migration ([#1041](https://github.com/cedricziel/signaldb/issues/1041)) ([afcc72e](https://github.com/cedricziel/signaldb/commit/afcc72e9f87a45e74c97171e8919b90868cd54f4))
* native Query IR — versioned structured query surface (query-ir-core) ([#882](https://github.com/cedricziel/signaldb/issues/882)) ([8774ac0](https://github.com/cedricziel/signaldb/commit/8774ac0fbbe4686cb7aa8b0bba73dbc25f185689))
* **query-ir:** add v2 heatmaps ([#1102](https://github.com/cedricziel/signaldb/issues/1102)) ([96184cf](https://github.com/cedricziel/signaldb/commit/96184cf42809a4cbf0e4a15f592cb544dbb7a597))
* **query-ir:** flamegraph result envelope for profiles ([#1144](https://github.com/cedricziel/signaldb/issues/1144)) ([394407f](https://github.com/cedricziel/signaldb/commit/394407f72756b15c97cb6ce6efcf01ce0b61b33b))
* retry throttled requests in every SignalDB client ([#1260](https://github.com/cedricziel/signaldb/issues/1260)) ([3342dcc](https://github.com/cedricziel/signaldb/commit/3342dcced2cbc489adc7bf5076a0c9059b805adb))
* **router:** Pyroscope OpenAPI parity (CLI/MCP/UI/SDK) ([#1268](https://github.com/cedricziel/signaldb/issues/1268)) ([2b54e2d](https://github.com/cedricziel/signaldb/commit/2b54e2d693801a0bfd9afdf4e982abfac6efc955))
* **tenant-table-listing:** list tenant tables from the Iceberg catalog ([#1267](https://github.com/cedricziel/signaldb/issues/1267)) ([5a444c2](https://github.com/cedricziel/signaldb/commit/5a444c261eeab5643d5d2d866385c07e2772ceee))


### Bug Fixes

* address review findings from [#1260](https://github.com/cedricziel/signaldb/issues/1260) ([#1270](https://github.com/cedricziel/signaldb/issues/1270)) ([d5a6ff5](https://github.com/cedricziel/signaldb/commit/d5a6ff50c49644942cfdc4663d7ab7a2d95fe0fb))


### Code Refactoring

* **cli:** make signaldb-cli depend only on the SDK (+ create_user API) ([#874](https://github.com/cedricziel/signaldb/issues/874)) ([8e5cce5](https://github.com/cedricziel/signaldb/commit/8e5cce56c821d69917b55cc8c21a9a2ef55864b7))
* **signaldb-cli:** simplify pass ([#1185](https://github.com/cedricziel/signaldb/issues/1185)) ([b3dcdcd](https://github.com/cedricziel/signaldb/commit/b3dcdcd7e36a7807717a05ff41b6cf6287f35c4a))
* simplify backend workspace (dedup, dead code, redundant clones) ([#1168](https://github.com/cedricziel/signaldb/issues/1168)) ([409b778](https://github.com/cedricziel/signaldb/commit/409b778686a1cea5c54edfba7778c3e9ed3aa29c))


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
