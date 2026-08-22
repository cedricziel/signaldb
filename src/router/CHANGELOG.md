# Changelog

## [0.4.0](https://github.com/cedricziel/signaldb/compare/router-v0.3.0...router-v0.4.0) (2026-08-22)


### Features

* **compactor:** keep a bounded value sketch so discovery can suggest values ([#1329](https://github.com/cedricziel/signaldb/issues/1329)) ([dd64a3d](https://github.com/cedricziel/signaldb/commit/dd64a3dd8a8846499ac75bea818ba938c6ca9a87))
* **query-ir:** add a describe stage and metadata envelope for discovery ([#1309](https://github.com/cedricziel/signaldb/issues/1309)) ([b1d521c](https://github.com/cedricziel/signaldb/commit/b1d521c4151efae251e208a0dc11af08f3d6332f))
* **router:** serve query discovery from the registry and statistics ([#1312](https://github.com/cedricziel/signaldb/issues/1312)) ([41d2738](https://github.com/cedricziel/signaldb/commit/41d27384df6e90bd9e9731218e084dd27581e20b))


### Bug Fixes

* **mcp:** box the SDK error a completion lookup returns ([#1373](https://github.com/cedricziel/signaldb/issues/1373)) ([7df1288](https://github.com/cedricziel/signaldb/commit/7df12883a28c4d6af203c376f6efba70ee537820))
* **query-ir:** stop an unknown group-by field from answering silently ([#1301](https://github.com/cedricziel/signaldb/issues/1301)) ([b4f8464](https://github.com/cedricziel/signaldb/commit/b4f8464f71192f80d407f81e8bd837efd8fafd79))


### Code Refactoring

* dedupe quality cleanups in compactor, router, and acceptor ([#1326](https://github.com/cedricziel/signaldb/issues/1326)) ([beaeff3](https://github.com/cedricziel/signaldb/commit/beaeff3e405b87405ef722b5158d1af99f51b7b0))
* make query-ir and tempo-api standalone, and cover the parser crates ([#1369](https://github.com/cedricziel/signaldb/issues/1369)) ([1a4d78f](https://github.com/cedricziel/signaldb/commit/1a4d78f077616a9c4846cb6c02715b147b5ad1c2))

## [0.3.0](https://github.com/cedricziel/signaldb/compare/router-v0.2.2...router-v0.3.0) (2026-08-17)


### ⚠ BREAKING CHANGES

* **auth:** POST /api/v1/admin/tenants/{id}/api-keys requires a non-empty `scopes` array; bodies without it are rejected.
* **cli+mcp:** signaldb-cli tenant/api-key/dataset commands move under `admin` (e.g. `signaldb-cli admin tenant list`), and queries now require a language flag (`signaldb-cli query --sql|--promql|--logql|--traceql|--ir`). No back-compat aliases are provided (post-1.0).

### Features

* **api:** code-first OpenAPI — generate spec + Rust/TS clients from annotations ([#856](https://github.com/cedricziel/signaldb/issues/856)) ([e34fbfb](https://github.com/cedricziel/signaldb/commit/e34fbfbd094034416f78597c59b306975dd97271))
* **api:** document Tempo trace query endpoints in OpenAPI + SDK ([#861](https://github.com/cedricziel/signaldb/issues/861)) ([a1e0d7f](https://github.com/cedricziel/signaldb/commit/a1e0d7f9f3c355f8bf73da686db1952487c3e046))
* **auth:** schema:read/schema:write API-key scopes, scopes on every key surface ([#1217](https://github.com/cedricziel/signaldb/issues/1217)) ([34c7a28](https://github.com/cedricziel/signaldb/commit/34c7a28e4e62fad7a05089c1a3543739d6e28450))
* **auth:** tenant:manage API-key scope for the tenant management API ([#1266](https://github.com/cedricziel/signaldb/issues/1266)) ([9dfc193](https://github.com/cedricziel/signaldb/commit/9dfc193a85e813b42f8658bf97cbfd30e3b78f2e))
* **cli+mcp:** CLI & MCP as pure SDK consumers — query --&lt;lang&gt;, admin grouping (Phase 1) ([#892](https://github.com/cedricziel/signaldb/issues/892)) ([92a439e](https://github.com/cedricziel/signaldb/commit/92a439e112da96029733d93db7f274c20c29cbc5))
* **logs:** surface trace_id/span_id in log query responses ([#1048](https://github.com/cedricziel/signaldb/issues/1048)) ([5a84a04](https://github.com/cedricziel/signaldb/commit/5a84a04b3582befd76ea5f231b887f2cbed253ea))
* **mcp-admin-tool-parity:** platform-admin and tenant self-management tool/CLI parity ([#1261](https://github.com/cedricziel/signaldb/issues/1261)) ([1eadc72](https://github.com/cedricziel/signaldb/commit/1eadc728ace70aff10fa01aaa8766012ace2df4c))
* **mcp:** OAuth 2.1 + DCR connector support for Claude and OpenAI ([#899](https://github.com/cedricziel/signaldb/issues/899)) ([4d0104a](https://github.com/cedricziel/signaldb/commit/4d0104a608ee392e9b25acf686dcd7359fc37631))
* metric/label discovery (MCP+CLI+SDK) and prom/loki UI migration ([#1041](https://github.com/cedricziel/signaldb/issues/1041)) ([afcc72e](https://github.com/cedricziel/signaldb/commit/afcc72e9f87a45e74c97171e8919b90868cd54f4))
* **model:** add span events to the Span model ([#847](https://github.com/cedricziel/signaldb/issues/847)) ([0dbd6e8](https://github.com/cedricziel/signaldb/commit/0dbd6e8a0701cea0ce9e46c4fc9456d1562e7d31))
* native Query IR — versioned structured query surface (query-ir-core) ([#882](https://github.com/cedricziel/signaldb/issues/882)) ([8774ac0](https://github.com/cedricziel/signaldb/commit/8774ac0fbbe4686cb7aa8b0bba73dbc25f185689))
* one signaldb binary with the services as subcommands ([#1204](https://github.com/cedricziel/signaldb/issues/1204)) ([77f3278](https://github.com/cedricziel/signaldb/commit/77f3278ca445ac9b28bf955b0e482d4366a27c07))
* **querier,router:** surface span events on the single-trace path ([#848](https://github.com/cedricziel/signaldb/issues/848)) ([5b344e9](https://github.com/cedricziel/signaldb/commit/5b344e98b6e787aeca35d68bf18ca5ca92657454))
* **query-ir:** add v2 heatmaps ([#1102](https://github.com/cedricziel/signaldb/issues/1102)) ([96184cf](https://github.com/cedricziel/signaldb/commit/96184cf42809a4cbf0e4a15f592cb544dbb7a597))
* **query-ir:** encode attribute containers as JSON objects ([#1097](https://github.com/cedricziel/signaldb/issues/1097)) ([dad1820](https://github.com/cedricziel/signaldb/commit/dad18208c69bb0450e3f48a450db1b2838255372))
* **query-ir:** flamegraph result envelope for profiles ([#1144](https://github.com/cedricziel/signaldb/issues/1144)) ([394407f](https://github.com/cedricziel/signaldb/commit/394407f72756b15c97cb6ce6efcf01ce0b61b33b))
* **query-ir:** histogram_quantile stage over metrics_histogram ([#1141](https://github.com/cedricziel/signaldb/issues/1141)) ([591efe7](https://github.com/cedricziel/signaldb/commit/591efe752bbd5bc4b3e460c950fcba287cfab5b8))
* Real trace-context parenting for documentLoad + complementary log-record telemetry ([#1117](https://github.com/cedricziel/signaldb/issues/1117)) ([43a7c63](https://github.com/cedricziel/signaldb/commit/43a7c63a42a55aed11df304387d286f4bb5bccb9))
* record Flight query failures as span exceptions + surface reasons ([#846](https://github.com/cedricziel/signaldb/issues/846)) ([20d89f5](https://github.com/cedricziel/signaldb/commit/20d89f51eee05ff25ddfa523053dad7ebc8ea6e2))
* return server trace context and timings on HTTP responses (Server-Timing + traceresponse) ([#918](https://github.com/cedricziel/signaldb/issues/918)) ([453dd20](https://github.com/cedricziel/signaldb/commit/453dd2050eee95f3daf1c96f77e56964e99a2bb1))
* **router:** Pyroscope OpenAPI parity (CLI/MCP/UI/SDK) ([#1268](https://github.com/cedricziel/signaldb/issues/1268)) ([2b54e2d](https://github.com/cedricziel/signaldb/commit/2b54e2d693801a0bfd9afdf4e982abfac6efc955))
* **router:** schema registry API under /api/v1/schema ([#1219](https://github.com/cedricziel/signaldb/issues/1219)) ([71af424](https://github.com/cedricziel/signaldb/commit/71af424a0d96eb3f87198af4c4213bb89106cf28))
* **sdk:** query surface — SDK covers PromQL/LogQL/TraceQL + Flight SQL (Phase 0) ([#890](https://github.com/cedricziel/signaldb/issues/890)) ([1fde946](https://github.com/cedricziel/signaldb/commit/1fde946cc308ef134f01492b72a3fc874e1c8f95))
* **self-monitoring:** runtime-configurable browser telemetry export ([#842](https://github.com/cedricziel/signaldb/issues/842)) ([343b928](https://github.com/cedricziel/signaldb/commit/343b92877d1291406de25923e671ab2a54a98028))
* semconv CLIENT spans on Flight call sites ([#905](https://github.com/cedricziel/signaldb/issues/905)) ([3047cbb](https://github.com/cedricziel/signaldb/commit/3047cbbc68f03e7d586d4a2caabaa2bd7c660ca1))
* signal rate-limit throttling with Retry-After and a generous default burst ([#1256](https://github.com/cedricziel/signaldb/issues/1256)) ([5584f3f](https://github.com/cedricziel/signaldb/commit/5584f3f1ef7461401a7f1bbbf24302308192b43d))
* span.kind facet + TraceQL support ([#1125](https://github.com/cedricziel/signaldb/issues/1125)) ([35735e5](https://github.com/cedricziel/signaldb/commit/35735e5d204b4fb9f89ddce1dd15296bf9ddfe3c))
* **tempo:** back trace tag discovery with real querier data ([#1258](https://github.com/cedricziel/signaldb/issues/1258)) ([4aeda0d](https://github.com/cedricziel/signaldb/commit/4aeda0d3314fbe7b5546f0411657fdc646e301dd))
* **tenant-table-listing:** list tenant tables from the Iceberg catalog ([#1267](https://github.com/cedricziel/signaldb/issues/1267)) ([5a444c2](https://github.com/cedricziel/signaldb/commit/5a444c261eeab5643d5d2d866385c07e2772ceee))
* **tracing:** add server.address and network.peer to RPC spans ([#1111](https://github.com/cedricziel/signaldb/issues/1111)) ([4e64934](https://github.com/cedricziel/signaldb/commit/4e64934814762c25226a3a7529bc9d695035d578))
* **ui:** add user menu and management pages ([#1105](https://github.com/cedricziel/signaldb/issues/1105)) ([c49a93f](https://github.com/cedricziel/signaldb/commit/c49a93ff5d112ce36335c19b12ac3404cdb4a8ba))


### Bug Fixes

* **build:** stop jemalloc heap profiling from crashing musl images ([#1126](https://github.com/cedricziel/signaldb/issues/1126)) ([98b2996](https://github.com/cedricziel/signaldb/commit/98b299660ef31b56d73e079a2477166b415e736e))
* **common,router:** include every known dataset in the tables grouping ([#1269](https://github.com/cedricziel/signaldb/issues/1269)) ([a895618](https://github.com/cedricziel/signaldb/commit/a8956181e5fd7f4cb91432d5f9622175708d2d70))
* **flight:** server.address double-port bug + ops do_action tracing gap ([#1116](https://github.com/cedricziel/signaldb/issues/1116)) ([73e778f](https://github.com/cedricziel/signaldb/commit/73e778f0d936931c86545bfc8722ac7a7403e0e9))
* **flight:** stop the client timeout from masking the querier's query deadline ([#919](https://github.com/cedricziel/signaldb/issues/919)) ([46eee38](https://github.com/cedricziel/signaldb/commit/46eee382468bfd6a5f3c34f8404379e55d68a690))
* **logql:** carry log and resource attributes as structured metadata ([#1094](https://github.com/cedricziel/signaldb/issues/1094)) ([26b9d15](https://github.com/cedricziel/signaldb/commit/26b9d15457ac84c96ba2affe28d3ea520b40c664))
* **mcp:** refresh expired OAuth credentials ([#1100](https://github.com/cedricziel/signaldb/issues/1100)) ([54484e6](https://github.com/cedricziel/signaldb/commit/54484e69083b66e676fcff4e6e4d46fe2c73a766))
* **model:** stop flattening trace hierarchies to root + direct children ([#1018](https://github.com/cedricziel/signaldb/issues/1018)) ([5fee337](https://github.com/cedricziel/signaldb/commit/5fee33711628bf3f041c436c34f363f114ed93fb))
* provision signal tables for every registered dataset, and read an absent one as empty ([#1074](https://github.com/cedricziel/signaldb/issues/1074)) ([9a50ffa](https://github.com/cedricziel/signaldb/commit/9a50ffaa7e404a96cb80d7d3b0cc0850ede00f49))
* **query-ir:** reapply flamegraph Option fix dropped by a stale merge ([#1146](https://github.com/cedricziel/signaldb/issues/1146)) ([811bb11](https://github.com/cedricziel/signaldb/commit/811bb111b8274e85a181203182a6dd462c3c9438))
* restore compactor discovery and WAL pending-gauge accuracy ([#1049](https://github.com/cedricziel/signaldb/issues/1049)) ([b9254b0](https://github.com/cedricziel/signaldb/commit/b9254b065430b092978c2ba8f2e59ec1d3c1ceb8))
* **router:** bound Tempo tag-values queries by time window ([#929](https://github.com/cedricziel/signaldb/issues/929)) ([#979](https://github.com/cedricziel/signaldb/issues/979)) ([7cc301a](https://github.com/cedricziel/signaldb/commit/7cc301adc539a77540682d155425bace30ddc803))
* **router:** materialize a tenant's default dataset as a real row ([#1085](https://github.com/cedricziel/signaldb/issues/1085)) ([9443244](https://github.com/cedricziel/signaldb/commit/94432445328a0489bfd0476aaaba12ba937a2561))
* **router:** return error bodies with messages from signal endpoints ([#921](https://github.com/cedricziel/signaldb/issues/921)) ([#980](https://github.com/cedricziel/signaldb/issues/980)) ([39f50ee](https://github.com/cedricziel/signaldb/commit/39f50eed98715ad3fa2b0a02fcac6dbee68161eb))
* **router:** write the tenant and its default dataset in one transaction ([#1086](https://github.com/cedricziel/signaldb/issues/1086)) ([59bdc70](https://github.com/cedricziel/signaldb/commit/59bdc705d8fddc8253d55466904f59f8f0493060))
* **traces:** span_kind/status_code numeric source of truth + schema evolution engine ([#1235](https://github.com/cedricziel/signaldb/issues/1235)) ([0f8603b](https://github.com/cedricziel/signaldb/commit/0f8603bdb1f39254c83af0c631653a65c8a85e3f))
* **ui:** route Metrics builder default queries through Query IR ([#1138](https://github.com/cedricziel/signaldb/issues/1138)) ([4056261](https://github.com/cedricziel/signaldb/commit/4056261e0d406d5ae73dc2fe20bc136b8e866bb8))


### Performance Improvements

* CPU target features and jemalloc allocator for release builds ([#970](https://github.com/cedricziel/signaldb/issues/970)) ([766e2d1](https://github.com/cedricziel/signaldb/commit/766e2d1c82dad65a674184edaf2e8d67cb4083dd))
* **flight,wal:** compress Flight IPC payloads and WAL entries ([#945](https://github.com/cedricziel/signaldb/issues/945)) ([#998](https://github.com/cedricziel/signaldb/issues/998)) ([efb5ef4](https://github.com/cedricziel/signaldb/commit/efb5ef4bc85e2e77483f4546255b50c564015827))


### Documentation

* flight-communication.md read path now describes the CLIENT hop. ([3047cbb](https://github.com/cedricziel/signaldb/commit/3047cbbc68f03e7d586d4a2caabaa2bd7c660ca1))


### Code Refactoring

* **cli:** make signaldb-cli depend only on the SDK (+ create_user API) ([#874](https://github.com/cedricziel/signaldb/issues/874)) ([8e5cce5](https://github.com/cedricziel/signaldb/commit/8e5cce56c821d69917b55cc8c21a9a2ef55864b7))
* **flight:** decode Flight data dictionary-aware ([#1004](https://github.com/cedricziel/signaldb/issues/1004)) ([94a7a30](https://github.com/cedricziel/signaldb/commit/94a7a30edd81060f2bfc5147dbf3b53307d2de72))
* **logging:** forbid log:: macros in favor of tracing:: ([#1006](https://github.com/cedricziel/signaldb/issues/1006)) ([071ebb4](https://github.com/cedricziel/signaldb/commit/071ebb47d02f2d6e43ccfb60380c00e3be929248))
* **router:** simplify pass ([#1171](https://github.com/cedricziel/signaldb/issues/1171)) ([f5bc591](https://github.com/cedricziel/signaldb/commit/f5bc59161c993cd5c4e1283885fa25f815199e1c))
* simplify backend workspace (dedup, dead code, redundant clones) ([#1168](https://github.com/cedricziel/signaldb/issues/1168)) ([409b778](https://github.com/cedricziel/signaldb/commit/409b778686a1cea5c54edfba7778c3e9ed3aa29c))
* span hygiene sweep and construction guard ([#907](https://github.com/cedricziel/signaldb/issues/907)) ([c1f7b81](https://github.com/cedricziel/signaldb/commit/c1f7b81fbc00ae5fd6c9b948f9fb35c9d5a27d26))


### Tests

* back provisioning tests with a file catalog, not a named memory one ([#1088](https://github.com/cedricziel/signaldb/issues/1088)) ([718b73d](https://github.com/cedricziel/signaldb/commit/718b73df827980e7f40856eb19addacfe4b1b4b8)), closes [#1083](https://github.com/cedricziel/signaldb/issues/1083)
* delete tautological tests and rewrite salvageable ones as contract tests ([#961](https://github.com/cedricziel/signaldb/issues/961)) ([b3e884a](https://github.com/cedricziel/signaldb/commit/b3e884ad59b4df853429133d5eef2724a8adcada))
* make tests assert what their names promise ([#966](https://github.com/cedricziel/signaldb/issues/966)) ([446ed06](https://github.com/cedricziel/signaldb/commit/446ed062a7480902ef391884b1c2e12f77ddd66f))
* pin the in-memory catalog so provisioning tests can't race ([#1083](https://github.com/cedricziel/signaldb/issues/1083)) ([218ff2a](https://github.com/cedricziel/signaldb/commit/218ff2a8db3d4bf37d9cdc53e163dee03f382fda))
* polish medium/low audit findings across the workspace ([#969](https://github.com/cedricziel/signaldb/issues/969)) ([8962f6d](https://github.com/cedricziel/signaldb/commit/8962f6d1d22c8a176d4a1d99376d61b42b1da258))
* replace sleep-based synchronization with deterministic waits ([#968](https://github.com/cedricziel/signaldb/issues/968)) ([6391326](https://github.com/cedricziel/signaldb/commit/6391326013c8620f186e4a63c2cdf3bbdf9ee963))

## [0.2.2](https://github.com/cedricziel/signaldb/compare/router-v0.2.1...router-v0.2.2) (2026-07-30)


### Features

* **router:** add /api/v1/label_stats cardinality endpoint ([#831](https://github.com/cedricziel/signaldb/issues/831)) ([d64fd80](https://github.com/cedricziel/signaldb/commit/d64fd80603f2a33cf2142a0982d54bc6aa547fed))
* **router:** join external callers' traces at the HTTP query boundary ([#838](https://github.com/cedricziel/signaldb/issues/838)) ([2818eb2](https://github.com/cedricziel/signaldb/commit/2818eb246ed7bdd5764bcf0294542b35d5473c8d))

## [0.2.1](https://github.com/cedricziel/signaldb/compare/router-v0.2.0...router-v0.2.1) (2026-07-30)


### Bug Fixes

* **ui:** sign in once — email/password login with a post-login tenant picker ([#794](https://github.com/cedricziel/signaldb/issues/794)) ([1feafbf](https://github.com/cedricziel/signaldb/commit/1feafbfc187069944c34a5903d65552f740c2d3a))

## [0.2.0](https://github.com/cedricziel/signaldb/compare/router-v0.1.0...router-v0.2.0) (2026-07-30)


### ⚠ BREAKING CHANGES

* Minimum supported Rust version is now 1.85.0

### Features

* add Grafana datasource plugin and Docker infrastructure ([#253](https://github.com/cedricziel/signaldb/issues/253)) ([a95cdfe](https://github.com/cedricziel/signaldb/commit/a95cdfe038e0667bc9b563c3b2f7a8bd7b280069))
* Add schema module with Iceberg integration and DSN-based storage ([#162](https://github.com/cedricziel/signaldb/issues/162)) ([60bbb8d](https://github.com/cedricziel/signaldb/commit/60bbb8d09a5ff63e2114c6383e7650c9dfef0d24))
* add tenant management admin API with OpenAPI spec, SDK, and CLI ([#313](https://github.com/cedricziel/signaldb/issues/313)) ([880c86b](https://github.com/cedricziel/signaldb/commit/880c86b6405a162c84fe88615b7d363585948abd))
* **auth:** add human user sessions ([629cf78](https://github.com/cedricziel/signaldb/commit/629cf78741e971bd2b3644f1d0eb08ae2f44feb3))
* **auth:** add scoped tenant self-service ([7830c3d](https://github.com/cedricziel/signaldb/commit/7830c3d706c21480f9767bca8639e5fcb82622bc))
* **auth:** per-tenant query rate limits and API key/dataset quotas ([#609](https://github.com/cedricziel/signaldb/issues/609)) ([f2ae3e9](https://github.com/cedricziel/signaldb/commit/f2ae3e955f05fde7511c344211c3d1613b6a86e9))
* **cli:** add HTTP admin API client for TUI ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** add terminal UI with traces, logs, metrics, admin, and dashboard tabs ([#458](https://github.com/cedricziel/signaldb/issues/458)) ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** implement Admin tab with tenant/key/dataset CRUD and confirmations ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** implement Logs tab with Flight SQL query interface ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** implement Metrics tab with sparklines and Flight SQL ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** integrate TUI tabs with help overlay and error handling ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **discovery:** TTL-filter stale services, reap crashed nodes, round-robin routing ([#600](https://github.com/cedricziel/signaldb/issues/600)) ([6aad9dc](https://github.com/cedricziel/signaldb/commit/6aad9dccbb2120442da5e80cf15f113e0c3d662b))
* embedded UI session auth + tenant-scoped whoami ([#773](https://github.com/cedricziel/signaldb/issues/773)) ([f217064](https://github.com/cedricziel/signaldb/commit/f217064d3f31002132761040bc8a82fe1c5e9c59))
* enable Dokku deployment with working HTTP router and monolithic Docker image ([#312](https://github.com/cedricziel/signaldb/issues/312)) ([4ec9d5c](https://github.com/cedricziel/signaldb/commit/4ec9d5cb4538e0d74278bfd14d51d65da1b2020c))
* end-to-end local development experience with CLI query support ([#434](https://github.com/cedricziel/signaldb/issues/434)) ([b95fb15](https://github.com/cedricziel/signaldb/commit/b95fb1595e33dd825f3c4424a88b966dded4808e))
* **flight:** authenticate Flight ports via internal service key ([#579](https://github.com/cedricziel/signaldb/issues/579)) ([da1b41f](https://github.com/cedricziel/signaldb/commit/da1b41f4698ce9f58348239d789a1678e23353b3)), closes [#544](https://github.com/cedricziel/signaldb/issues/544)
* **flight:** close out Flight port authentication ([#544](https://github.com/cedricziel/signaldb/issues/544)) ([#589](https://github.com/cedricziel/signaldb/issues/589)) ([f8a7b43](https://github.com/cedricziel/signaldb/commit/f8a7b43722fa0024e2b7c01b2243bb9329420f6c))
* implement configurable schemas and tenant management API for SignalDB ([#167](https://github.com/cedricziel/signaldb/issues/167)) ([efe6e09](https://github.com/cedricziel/signaldb/commit/efe6e0952b392ae795232bd05829fe13aaaa10cc))
* implement external Flight service interface for SignalDB router ([#135](https://github.com/cedricziel/signaldb/issues/135)) ([df4ce06](https://github.com/cedricziel/signaldb/commit/df4ce06834b73b9537a2f4c63d1e5cbfceaf3b58))
* implement multi-tenancy with WAL isolation and authentication ([#243](https://github.com/cedricziel/signaldb/issues/243)) ([9a8945f](https://github.com/cedricziel/signaldb/commit/9a8945f06e871a96f5890e194534ae11ebb1f35b))
* implement service catalog-aware Flight transport ([#134](https://github.com/cedricziel/signaldb/issues/134)) ([eebe2b9](https://github.com/cedricziel/signaldb/commit/eebe2b9caa0bb833a7003f581eb9d047c0ab3533))
* implement trace querying functionality for issue [#6](https://github.com/cedricziel/signaldb/issues/6) ([#186](https://github.com/cedricziel/signaldb/issues/186)) ([ea8d9b4](https://github.com/cedricziel/signaldb/commit/ea8d9b47446cdbb89bb05b0a5c048c023d4dde49))
* integrate cargo-machete for unused dependency detection ([#130](https://github.com/cedricziel/signaldb/issues/130)) ([f305d3b](https://github.com/cedricziel/signaldb/commit/f305d3b9a6923ca2f7eca95ee83ed9002ee7cee1))
* **logql:** execute LogQL metric queries end-to-end ([#667](https://github.com/cedricziel/signaldb/issues/667)) ([2fc630d](https://github.com/cedricziel/signaldb/commit/2fc630d34d596d4003b6d148d4ce6b38495dc86b))
* **logql:** group log metric queries by materialized labels ([#740](https://github.com/cedricziel/signaldb/issues/740)) ([139c2e8](https://github.com/cedricziel/signaldb/commit/139c2e85518dc5879e7540b79fae9ea61d6e47d9))
* **logs:** end-to-end LogQL log queries (querier service + router) ([#665](https://github.com/cedricziel/signaldb/issues/665)) ([7e77dcf](https://github.com/cedricziel/signaldb/commit/7e77dcff12f7d9a49afe2c40a4104cbe302f1a48))
* **loki:** add Loki API types crate and router LogQL endpoint skeleton ([#650](https://github.com/cedricziel/signaldb/issues/650)) ([9a938b7](https://github.com/cedricziel/signaldb/commit/9a938b7dea2e404492b54a0481415d2f36881880))
* native explore UI for logs, traces, and metrics ([#768](https://github.com/cedricziel/signaldb/issues/768)) ([5db53c9](https://github.com/cedricziel/signaldb/commit/5db53c9f87b791c1f1d9590c6a1288db376da92b))
* Phase 2 Component Integration with WAL and Flight Services ([#138](https://github.com/cedricziel/signaldb/issues/138)) ([47f4174](https://github.com/cedricziel/signaldb/commit/47f417488c7b0225d031219df94a1d7eb55ff166))
* **profiles:** link profiles to traces across the query surface ([#645](https://github.com/cedricziel/signaldb/issues/645)) ([5430d27](https://github.com/cedricziel/signaldb/commit/5430d27281a66a9d88dea0e8d450f73902307137)), closes [#362](https://github.com/cedricziel/signaldb/issues/362) [#363](https://github.com/cedricziel/signaldb/issues/363)
* PromQL query support — /prometheus API (epic [#328](https://github.com/cedricziel/signaldb/issues/328)) ([#671](https://github.com/cedricziel/signaldb/issues/671)) ([9fe8264](https://github.com/cedricziel/signaldb/commit/9fe8264b0d2fbb3f785779034a6388da5c0cdd95))
* **promql:** group by materialized labels ([#749](https://github.com/cedricziel/signaldb/issues/749)) ([a5171d1](https://github.com/cedricziel/signaldb/commit/a5171d19bd5d22537eba994865ede407705a3a2a))
* **querier, router:** detected_fields discovery endpoint ([#738](https://github.com/cedricziel/signaldb/issues/738)) ([01bb757](https://github.com/cedricziel/signaldb/commit/01bb757889d4e178cad57a8696c6767a50b26384))
* **querier:** apply TraceQL and tag filters on trace search — no more silently unfiltered results ([#596](https://github.com/cedricziel/signaldb/issues/596)) ([fb8f0ba](https://github.com/cedricziel/signaldb/commit/fb8f0ba081aee3dcf5f524deec12851d38a2acf5)), closes [#551](https://github.com/cedricziel/signaldb/issues/551)
* **querier:** surface trace not-found as an explicit Flight status ([#616](https://github.com/cedricziel/signaldb/issues/616)) ([d6daeb6](https://github.com/cedricziel/signaldb/commit/d6daeb6fc63e6a1c49fefdcfb2391750f01dbcc8))
* **router:** Pyroscope-compatible HTTP API ([#644](https://github.com/cedricziel/signaldb/issues/644)) ([dabbede](https://github.com/cedricziel/signaldb/commit/dabbedeebc17ad0d03ac43aa44932b05a37ff857)), closes [#359](https://github.com/cedricziel/signaldb/issues/359)
* **schema:** add materialized-labels config and column-name helper ([#723](https://github.com/cedricziel/signaldb/issues/723)) ([8c213f0](https://github.com/cedricziel/signaldb/commit/8c213f05ced5ecf9b64e7457fff06690c6156bae))
* **self-monitoring:** epic [#447](https://github.com/cedricziel/signaldb/issues/447) — SignalDB observes itself (dogfooding) ([#542](https://github.com/cedricziel/signaldb/issues/542)) ([e6d7b1f](https://github.com/cedricziel/signaldb/commit/e6d7b1fc37f370f534d8780b3a6fe5d180b1ad65))
* **tempo:** honor spss span cap when shaping search results ([#615](https://github.com/cedricziel/signaldb/issues/615)) ([6a1d04b](https://github.com/cedricziel/signaldb/commit/6a1d04bef21c6b4ce85c2547f24cb884b40d8da3))
* **tempo:** honor start/end time hints in single-trace lookup ([#614](https://github.com/cedricziel/signaldb/issues/614)) ([ddb81fb](https://github.com/cedricziel/signaldb/commit/ddb81fbc2803ab5cb87e92b6b36773b3752009b6))


### Bug Fixes

* align Iceberg namespace paths and partition spec (Issue [#185](https://github.com/cedricziel/signaldb/issues/185)) ([#306](https://github.com/cedricziel/signaldb/issues/306)) ([cc7af60](https://github.com/cedricziel/signaldb/commit/cc7af60ad6426eefc0a0de5c628b865306227172))
* **auth:** return 401 for missing credentials instead of 400 ([#775](https://github.com/cedricziel/signaldb/issues/775)) ([33c768c](https://github.com/cedricziel/signaldb/commit/33c768c1082688d1ac525c19f1612b928cab5a03))
* **config:** refuse in-memory discovery/catalog in standalone services ([#599](https://github.com/cedricziel/signaldb/issues/599)) ([c8413ba](https://github.com/cedricziel/signaldb/commit/c8413babe5de5346477bf4d1ff26a7f2fef380bb))
* integration tests ([#123](https://github.com/cedricziel/signaldb/issues/123)) ([0ab5eeb](https://github.com/cedricziel/signaldb/commit/0ab5eeb9a0637483efaae88175ab9648c9dd2fb9))
* preserve OTLP scope/resource metadata and events/links in trace pipeline ([#183](https://github.com/cedricziel/signaldb/issues/183)) ([#307](https://github.com/cedricziel/signaldb/issues/307)) ([dfe04d7](https://github.com/cedricziel/signaldb/commit/dfe04d73d27c0e8820aa8daeed0787d048701865))
* propagate attributes through query path and implement trace search ([#309](https://github.com/cedricziel/signaldb/issues/309)) ([98244d8](https://github.com/cedricziel/signaldb/commit/98244d8f9ebfe623ea07ab240356511acbde5d07))
* resolve beta channel build failures and add temporary table cleanup ([#179](https://github.com/cedricziel/signaldb/issues/179)) ([d5f48dd](https://github.com/cedricziel/signaldb/commit/d5f48dd69cf1026295a825aea00f847c284ebe18))
* **router:** empty query results and error bodies on the query surfaces ([#772](https://github.com/cedricziel/signaldb/issues/772)) ([1a729de](https://github.com/cedricziel/signaldb/commit/1a729dee7f0a63b68fe238f428370c9b5e661a82))
* **router:** stop serving fabricated or empty-stub Tempo responses ([#597](https://github.com/cedricziel/signaldb/issues/597)) ([f8dd559](https://github.com/cedricziel/signaldb/commit/f8dd55925b9eebfb7c15b427a5fa811d481bcb18))


### Performance Improvements

* optimize dependency tree to reduce build times ([#149](https://github.com/cedricziel/signaldb/issues/149)) ([6057f14](https://github.com/cedricziel/signaldb/commit/6057f149c6d1d85a74fc092f53b91393a12fba48))


### Documentation

* add comprehensive per-component README documentation ([#151](https://github.com/cedricziel/signaldb/issues/151)) ([5ea02a2](https://github.com/cedricziel/signaldb/commit/5ea02a2dd660bc03639a3a82c0146f18c147ecfb)), closes [#139](https://github.com/cedricziel/signaldb/issues/139)
* full staleness sweep — match all docs, skills, and READMEs to current code ([#611](https://github.com/cedricziel/signaldb/issues/611)) ([22247b0](https://github.com/cedricziel/signaldb/commit/22247b027d77820481d493c081e29f0df4efd6ed))


### Code Refactoring

* unify Flight data conversion and eliminate double JSON parse ([#308](https://github.com/cedricziel/signaldb/issues/308)) ([b62a081](https://github.com/cedricziel/signaldb/commit/b62a0815782f967d05f748220c51a7ba0a19cd51))


### Tests

* **logql:** end-to-end integration test; fix timestamp unit in conversion ([#669](https://github.com/cedricziel/signaldb/issues/669)) ([7e38037](https://github.com/cedricziel/signaldb/commit/7e3803779f79edf2fd6e21ecb24dfc1db4a85e81)), closes [#378](https://github.com/cedricziel/signaldb/issues/378)


### Continuous Integration

* drop MSRV policy and fix security audit ignores ([#521](https://github.com/cedricziel/signaldb/issues/521)) ([7da71e3](https://github.com/cedricziel/signaldb/commit/7da71e3d78f593a4361f403e2d4be1e426fb8807))

## 0.1.0 (2026-03-02)


### ⚠ BREAKING CHANGES

* Minimum supported Rust version is now 1.85.0

### Features

* add Grafana datasource plugin and Docker infrastructure ([#253](https://github.com/cedricziel/signaldb/issues/253)) ([a95cdfe](https://github.com/cedricziel/signaldb/commit/a95cdfe038e0667bc9b563c3b2f7a8bd7b280069))
* add queue primitives ([#48](https://github.com/cedricziel/signaldb/issues/48)) ([caf4651](https://github.com/cedricziel/signaldb/commit/caf46518c2e7ee574d63617a9210774ed2531739))
* Add schema module with Iceberg integration and DSN-based storage ([#162](https://github.com/cedricziel/signaldb/issues/162)) ([60bbb8d](https://github.com/cedricziel/signaldb/commit/60bbb8d09a5ff63e2114c6383e7650c9dfef0d24))
* add tenant management admin API with OpenAPI spec, SDK, and CLI ([#313](https://github.com/cedricziel/signaldb/issues/313)) ([880c86b](https://github.com/cedricziel/signaldb/commit/880c86b6405a162c84fe88615b7d363585948abd))
* **cli:** add HTTP admin API client for TUI ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** add terminal UI with traces, logs, metrics, admin, and dashboard tabs ([#458](https://github.com/cedricziel/signaldb/issues/458)) ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** implement Admin tab with tenant/key/dataset CRUD and confirmations ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** implement Logs tab with Flight SQL query interface ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** implement Metrics tab with sparklines and Flight SQL ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** integrate TUI tabs with help overlay and error handling ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* convert arrow &lt;&gt; otlp ([#99](https://github.com/cedricziel/signaldb/issues/99)) ([ba65d14](https://github.com/cedricziel/signaldb/commit/ba65d144173d2dbeee22011ded650e834df4f5c9))
* enable Dokku deployment with working HTTP router and monolithic Docker image ([#312](https://github.com/cedricziel/signaldb/issues/312)) ([4ec9d5c](https://github.com/cedricziel/signaldb/commit/4ec9d5cb4538e0d74278bfd14d51d65da1b2020c))
* end-to-end local development experience with CLI query support ([#434](https://github.com/cedricziel/signaldb/issues/434)) ([b95fb15](https://github.com/cedricziel/signaldb/commit/b95fb1595e33dd825f3c4424a88b966dded4808e))
* implement configurable schemas and tenant management API for SignalDB ([#167](https://github.com/cedricziel/signaldb/issues/167)) ([efe6e09](https://github.com/cedricziel/signaldb/commit/efe6e0952b392ae795232bd05829fe13aaaa10cc))
* implement external Flight service interface for SignalDB router ([#135](https://github.com/cedricziel/signaldb/issues/135)) ([df4ce06](https://github.com/cedricziel/signaldb/commit/df4ce06834b73b9537a2f4c63d1e5cbfceaf3b58))
* implement multi-tenancy with WAL isolation and authentication ([#243](https://github.com/cedricziel/signaldb/issues/243)) ([9a8945f](https://github.com/cedricziel/signaldb/commit/9a8945f06e871a96f5890e194534ae11ebb1f35b))
* implement service catalog-aware Flight transport ([#134](https://github.com/cedricziel/signaldb/issues/134)) ([eebe2b9](https://github.com/cedricziel/signaldb/commit/eebe2b9caa0bb833a7003f581eb9d047c0ab3533))
* implement trace querying functionality for issue [#6](https://github.com/cedricziel/signaldb/issues/6) ([#186](https://github.com/cedricziel/signaldb/issues/186)) ([ea8d9b4](https://github.com/cedricziel/signaldb/commit/ea8d9b47446cdbb89bb05b0a5c048c023d4dde49))
* integrate cargo-machete for unused dependency detection ([#130](https://github.com/cedricziel/signaldb/issues/130)) ([f305d3b](https://github.com/cedricziel/signaldb/commit/f305d3b9a6923ca2f7eca95ee83ed9002ee7cee1))
* Phase 2 Component Integration with WAL and Flight Services ([#138](https://github.com/cedricziel/signaldb/issues/138)) ([47f4174](https://github.com/cedricziel/signaldb/commit/47f417488c7b0225d031219df94a1d7eb55ff166))
* store instances in catalog ([#105](https://github.com/cedricziel/signaldb/issues/105)) ([6e92a90](https://github.com/cedricziel/signaldb/commit/6e92a9031a20c04658a1060fa2b7733d5e244f0e))


### Bug Fixes

* align Iceberg namespace paths and partition spec (Issue [#185](https://github.com/cedricziel/signaldb/issues/185)) ([#306](https://github.com/cedricziel/signaldb/issues/306)) ([cc7af60](https://github.com/cedricziel/signaldb/commit/cc7af60ad6426eefc0a0de5c628b865306227172))
* integration tests ([#123](https://github.com/cedricziel/signaldb/issues/123)) ([0ab5eeb](https://github.com/cedricziel/signaldb/commit/0ab5eeb9a0637483efaae88175ab9648c9dd2fb9))
* preserve OTLP scope/resource metadata and events/links in trace pipeline ([#183](https://github.com/cedricziel/signaldb/issues/183)) ([#307](https://github.com/cedricziel/signaldb/issues/307)) ([dfe04d7](https://github.com/cedricziel/signaldb/commit/dfe04d73d27c0e8820aa8daeed0787d048701865))
* propagate attributes through query path and implement trace search ([#309](https://github.com/cedricziel/signaldb/issues/309)) ([98244d8](https://github.com/cedricziel/signaldb/commit/98244d8f9ebfe623ea07ab240356511acbde5d07))
* resolve beta channel build failures and add temporary table cleanup ([#179](https://github.com/cedricziel/signaldb/issues/179)) ([d5f48dd](https://github.com/cedricziel/signaldb/commit/d5f48dd69cf1026295a825aea00f847c284ebe18))
* set version in router ([#85](https://github.com/cedricziel/signaldb/issues/85)) ([4c9adc7](https://github.com/cedricziel/signaldb/commit/4c9adc772bdaf077990592561f1109cd263fbdce))


### Performance Improvements

* optimize dependency tree to reduce build times ([#149](https://github.com/cedricziel/signaldb/issues/149)) ([6057f14](https://github.com/cedricziel/signaldb/commit/6057f149c6d1d85a74fc092f53b91393a12fba48))


### Documentation

* add comprehensive per-component README documentation ([#151](https://github.com/cedricziel/signaldb/issues/151)) ([5ea02a2](https://github.com/cedricziel/signaldb/commit/5ea02a2dd660bc03639a3a82c0146f18c147ecfb)), closes [#139](https://github.com/cedricziel/signaldb/issues/139)


### Code Refactoring

* unify Flight data conversion and eliminate double JSON parse ([#308](https://github.com/cedricziel/signaldb/issues/308)) ([b62a081](https://github.com/cedricziel/signaldb/commit/b62a0815782f967d05f748220c51a7ba0a19cd51))
