# Changelog

## [0.3.0](https://github.com/cedricziel/signaldb/compare/signaldb-sdk-v0.2.2...signaldb-sdk-v0.3.0) (2026-10-03)


### ⚠ BREAKING CHANGES

* **traceql:** `Condition` has a new public `op` field, so code that constructs one must set it.
* **mcp-server:** discover_attributes and discover_metrics return the Query IR metadata envelope (logical dotted names, levels, cost) instead of a Tempo tag list, a Loki/Prometheus {status,data} array, or Pyroscope {names}. scope narrows by attribute level rather than routing to the Tempo v2 endpoints, and tag values need a declared set, a statistics sketch, or sample: true.

### Features

* **common:** merge discovery fields with the type authority's canonical types ([#2074](https://github.com/cedricziel/signaldb/issues/2074)) ([36b7266](https://github.com/cedricziel/signaldb/commit/36b7266ec9442a10aaec8b7eb45d654b6cb27ec7))
* **evals:** build eval sets from traces ([#1847](https://github.com/cedricziel/signaldb/issues/1847)) ([dfe9e60](https://github.com/cedricziel/signaldb/commit/dfe9e608f9629abee1a542f5020f164d193b8faf))
* **evals:** eval sets pages, upload dialog and saving regressions in the UI ([#1871](https://github.com/cedricziel/signaldb/issues/1871)) ([cb05278](https://github.com/cedricziel/signaldb/commit/cb05278065450372e37f51fc149f4ac04a6ad6a7))
* **evals:** upload eval results and gate CI on them ([#1857](https://github.com/cedricziel/signaldb/issues/1857)) ([175155a](https://github.com/cedricziel/signaldb/commit/175155a3a2d958d59e16a05961206c0b09f1e79a))
* **mcp-server:** discover attributes and metrics through the Query IR ([#2077](https://github.com/cedricziel/signaldb/issues/2077)) ([37d58ea](https://github.com/cedricziel/signaldb/commit/37d58eac6e5ac2b3766b324e494f0c4901812a5a))
* **processors:** diff Test panel output against the server's decoded input ([#1883](https://github.com/cedricziel/signaldb/issues/1883)) ([7711bc5](https://github.com/cedricziel/signaldb/commit/7711bc5bca22d010fb83766449d52ccdd44291e0))
* **querier:** add histogram_avg, histogram_stddev and histogram_stdvar ([#2187](https://github.com/cedricziel/signaldb/issues/2187)) ([626a417](https://github.com/cedricziel/signaldb/commit/626a41777f5393eb364a00b9fa211cd674f2eb09))
* **query-ir:** correlate to another signal with semi and anti joins (irVersion 11) ([#2052](https://github.com/cedricziel/signaldb/issues/2052)) ([ad0cd64](https://github.com/cedricziel/signaldb/commit/ad0cd644101af8f140dd7ae89fdc09f94dab9ca2))
* **query-ir:** differential flamegraph over a baseline window (irVersion 13) ([#2100](https://github.com/cedricziel/signaldb/issues/2100)) ([5e36ddb](https://github.com/cedricziel/signaldb/commit/5e36ddbea6b114c7c2077eca2ee28b9b0fa8089f))
* **query-ir:** trace result envelope (irVersion 12) ([#2063](https://github.com/cedricziel/signaldb/issues/2063)) ([934b6bc](https://github.com/cedricziel/signaldb/commit/934b6bccf5d5fdb12595c07191804091664ae145))
* **router:** eval sets API for offline agent evals ([#1837](https://github.com/cedricziel/signaldb/issues/1837)) ([b3ce35a](https://github.com/cedricziel/signaldb/commit/b3ce35a7fb9671494e4d78cd29b2673f20205531))
* **router:** list discovery fields with their canonical authority type ([#2075](https://github.com/cedricziel/signaldb/issues/2075)) ([621aa46](https://github.com/cedricziel/signaldb/commit/621aa4674399a628470b73810e033963288a3866))
* **router:** live-tail Query IR rows and trace results (IR v15) ([#2148](https://github.com/cedricziel/signaldb/issues/2148)) ([57ac3c7](https://github.com/cedricziel/signaldb/commit/57ac3c74646037f2198b86f787da3b7a161d6ffd))
* **router:** paginate Query IR rows and trace results (IR v14) ([#2142](https://github.com/cedricziel/signaldb/issues/2142)) ([67023cc](https://github.com/cedricziel/signaldb/commit/67023cc0f71ab345f23d6278f9a917ee3d349941))
* **router:** publish the Query IR stage grammar as typed OpenAPI schemas ([#2088](https://github.com/cedricziel/signaldb/issues/2088)) ([55e79d8](https://github.com/cedricziel/signaldb/commit/55e79d8fdcb6b5703d1d66801477ae7c714ce6e3))
* **router:** publish UI login/logout and the full whoami response in OpenAPI ([#2110](https://github.com/cedricziel/signaldb/issues/2110)) ([664c8fe](https://github.com/cedricziel/signaldb/commit/664c8fe52a51505ef5fc46a63293e1e35a1a1913))
* **router:** report the retention that applies to a query ([#2182](https://github.com/cedricziel/signaldb/issues/2182)) ([91ca14d](https://github.com/cedricziel/signaldb/commit/91ca14dfc6f4478155ef9095c1b84e4078a6da90))
* **router:** scalar result envelope and metric Series labels ([#2001](https://github.com/cedricziel/signaldb/issues/2001)) ([2d72dc1](https://github.com/cedricziel/signaldb/commit/2d72dc1b719ee880ad87101fc042d21465f3affc))
* **router:** type the Query IR request pipeline as IrStage ([#2095](https://github.com/cedricziel/signaldb/issues/2095)) ([414ed92](https://github.com/cedricziel/signaldb/commit/414ed92b136367e2650b1c8de8f0cd12250cce5b))
* **router:** warn match_incomplete_trace from the query report trailer ([#2090](https://github.com/cedricziel/signaldb/issues/2090)) ([7ee6f46](https://github.com/cedricziel/signaldb/commit/7ee6f464d3512df3a2c4fddc06f90302c3c55b16))
* **schema-registry:** accept definition/2 custom registry uploads ([#1823](https://github.com/cedricziel/signaldb/issues/1823)) ([a8c8196](https://github.com/cedricziel/signaldb/commit/a8c819648bd5778478be6e86241802ae4f6f880f))
* **schema:** let registry metric definitions declare aliases ([#2188](https://github.com/cedricziel/signaldb/issues/2188)) ([0817836](https://github.com/cedricziel/signaldb/commit/08178369ba741145ea42dc6ff3aeb2517e2ad1bc))
* **traceql:** support !=, =~ and !~ and surface search errors over MCP ([#2178](https://github.com/cedricziel/signaldb/issues/2178)) ([ce929f5](https://github.com/cedricziel/signaldb/commit/ce929f5f321e5d826e2ba43edc67d7e26de67659))


### Bug Fixes

* **query-ir:** keep the newest flamegraph profiles and reject inverted windows ([#2098](https://github.com/cedricziel/signaldb/issues/2098)) ([dda9bad](https://github.com/cedricziel/signaldb/commit/dda9bad2c86140ee9be861ee0e4cd5f2788cf8a9))
* **router:** read data for sample:true and flag partial discovery statistics ([#2176](https://github.com/cedricziel/signaldb/issues/2176)) ([ab5dcf7](https://github.com/cedricziel/signaldb/commit/ab5dcf78f47567a6e2a8f8a10dcf5dfd06cdfed5))


### Documentation

* describe PromQL execution through the Query IR ([#2042](https://github.com/cedricziel/signaldb/issues/2042)) ([3375b34](https://github.com/cedricziel/signaldb/commit/3375b344ff68b1c6f1b546bf36f7de00dd508357))


### Build System

* fix the beta test leg for cargo's unused-dependency lints ([#2047](https://github.com/cedricziel/signaldb/issues/2047)) ([6867d69](https://github.com/cedricziel/signaldb/commit/6867d69dceb26f2e55ccfac31e60ae42aad76418))

## [0.2.2](https://github.com/cedricziel/signaldb/compare/signaldb-sdk-v0.2.1...signaldb-sdk-v0.2.2) (2026-09-23)


### Features

* demo mode and a TrueNAS demo app with a trimmed OpenTelemetry Demo ([#1632](https://github.com/cedricziel/signaldb/issues/1632)) ([d6da0cf](https://github.com/cedricziel/signaldb/commit/d6da0cfb53d79b8167d92e0c3689aea65323d97b))
* GitHub App integration for connecting a tenant's repositories ([#1600](https://github.com/cedricziel/signaldb/issues/1600)) ([6c9721e](https://github.com/cedricziel/signaldb/commit/6c9721ef0cf630df85e227a07be6ae30ee263191))
* multi-tenant MCP OAuth grants ([#1541](https://github.com/cedricziel/signaldb/issues/1541)) ([c5b49b0](https://github.com/cedricziel/signaldb/commit/c5b49b018f749a72b639366a18223081cecef7cc))
* per-API-key allowed origins for browser (CORS) ingestion ([#1548](https://github.com/cedricziel/signaldb/issues/1548)) ([6e966dd](https://github.com/cedricziel/signaldb/commit/6e966ddaf2740e3648583223828c6af715b6d331))
* per-tenant, per-dataset OTTL telemetry processors ([#1603](https://github.com/cedricziel/signaldb/issues/1603)) ([2fc1022](https://github.com/cedricziel/signaldb/commit/2fc102232b1d925418b02e68393af8917184016e))
* **router:** attach an existing GitHub App installation to a tenant ([#1618](https://github.com/cedricziel/signaldb/issues/1618)) ([0ab8e95](https://github.com/cedricziel/signaldb/commit/0ab8e95581f5213b02e8cded8af5b2b71c827516))
* **router:** serialize API timestamps as native UTC DateTime ([#1643](https://github.com/cedricziel/signaldb/issues/1643)) ([1327fae](https://github.com/cedricziel/signaldb/commit/1327fae5760510f7e2180ab9e323ba6961d8b657))
* source context for stack frames from linked GitHub repositories ([#1601](https://github.com/cedricziel/signaldb/issues/1601)) ([acca49c](https://github.com/cedricziel/signaldb/commit/acca49ca770ab96464b12676211144bc20b5cc7c))
* **ui:** move the explore UI onto the query IR ([#1627](https://github.com/cedricziel/signaldb/issues/1627)) ([c20ad3e](https://github.com/cedricziel/signaldb/commit/c20ad3e6a91e43ba01c201c6c37faafd376d6b6d))

## [0.2.1](https://github.com/cedricziel/signaldb/compare/signaldb-sdk-v0.2.0...signaldb-sdk-v0.2.1) (2026-09-12)


### Features

* **auth:** OIDC login (relying-party SSO) ([#1485](https://github.com/cedricziel/signaldb/issues/1485)) ([c681bee](https://github.com/cedricziel/signaldb/commit/c681bee369d9a1b636357edf70b6f88f236b96a2))
* **compactor:** keep a bounded value sketch so discovery can suggest values ([#1329](https://github.com/cedricziel/signaldb/issues/1329)) ([dd64a3d](https://github.com/cedricziel/signaldb/commit/dd64a3dd8a8846499ac75bea818ba938c6ca9a87))
* dedicated login page with a login-configuration probe ([#1484](https://github.com/cedricziel/signaldb/issues/1484)) ([d536466](https://github.com/cedricziel/signaldb/commit/d53646688a580256711f0534ae7ed526c58a769a))
* implement multi-dataset restriction for API keys and OAuth grants ([#1475](https://github.com/cedricziel/signaldb/issues/1475)) ([11deba9](https://github.com/cedricziel/signaldb/commit/11deba995c6937324576f87e87284a1580faa624))
* **router:** serve query discovery from the registry and statistics ([#1312](https://github.com/cedricziel/signaldb/issues/1312)) ([41d2738](https://github.com/cedricziel/signaldb/commit/41d27384df6e90bd9e9731218e084dd27581e20b))
* **schema-registry:** accept keys= batch resolution on GET /api/v1/schema/metrics ([#1508](https://github.com/cedricziel/signaldb/issues/1508)) ([6facbdc](https://github.com/cedricziel/signaldb/commit/6facbdcd182285bf54c1d2e922724d6bdeb6bae6))
* self-serve connection details for agents ([public] config, /api/v1/connection, MCP connection_info) ([#1474](https://github.com/cedricziel/signaldb/issues/1474)) ([ad78cd1](https://github.com/cedricziel/signaldb/commit/ad78cd1981282426b65b7dcac50ddc38eeea7f80))


### Bug Fixes

* **auth:** remove the dataset_id legacy shims from multi-dataset-key-restriction ([#1480](https://github.com/cedricziel/signaldb/issues/1480)) ([e8c85de](https://github.com/cedricziel/signaldb/commit/e8c85dedc0a9a73c5a133b952e858603d78c0c36))
* **query-ir:** stop an unknown group-by field from answering silently ([#1301](https://github.com/cedricziel/signaldb/issues/1301)) ([b4f8464](https://github.com/cedricziel/signaldb/commit/b4f8464f71192f80d407f81e8bd837efd8fafd79))

## [0.2.0](https://github.com/cedricziel/signaldb/compare/signaldb-sdk-v0.1.1...signaldb-sdk-v0.2.0) (2026-08-17)


### ⚠ BREAKING CHANGES

* **auth:** POST /api/v1/admin/tenants/{id}/api-keys requires a non-empty `scopes` array; bodies without it are rejected.
* **cli+mcp:** signaldb-cli tenant/api-key/dataset commands move under `admin` (e.g. `signaldb-cli admin tenant list`), and queries now require a language flag (`signaldb-cli query --sql|--promql|--logql|--traceql|--ir`). No back-compat aliases are provided (post-1.0).

### Features

* **api:** code-first OpenAPI — generate spec + Rust/TS clients from annotations ([#856](https://github.com/cedricziel/signaldb/issues/856)) ([e34fbfb](https://github.com/cedricziel/signaldb/commit/e34fbfbd094034416f78597c59b306975dd97271))
* **api:** document Tempo trace query endpoints in OpenAPI + SDK ([#861](https://github.com/cedricziel/signaldb/issues/861)) ([a1e0d7f](https://github.com/cedricziel/signaldb/commit/a1e0d7f9f3c355f8bf73da686db1952487c3e046))
* **auth:** schema:read/schema:write API-key scopes, scopes on every key surface ([#1217](https://github.com/cedricziel/signaldb/issues/1217)) ([34c7a28](https://github.com/cedricziel/signaldb/commit/34c7a28e4e62fad7a05089c1a3543739d6e28450))
* **auth:** tenant:manage API-key scope for the tenant management API ([#1266](https://github.com/cedricziel/signaldb/issues/1266)) ([9dfc193](https://github.com/cedricziel/signaldb/commit/9dfc193a85e813b42f8658bf97cbfd30e3b78f2e))
* **cli+mcp:** CLI & MCP as pure SDK consumers — query --&lt;lang&gt;, admin grouping (Phase 1) ([#892](https://github.com/cedricziel/signaldb/issues/892)) ([92a439e](https://github.com/cedricziel/signaldb/commit/92a439e112da96029733d93db7f274c20c29cbc5))
* **clients:** schema registry in SDK, CLI, and MCP ([#1223](https://github.com/cedricziel/signaldb/issues/1223)) ([1838583](https://github.com/cedricziel/signaldb/commit/1838583910be33e03d72b2be15e17d819031c9c5))
* **mcp-admin-tool-parity:** platform-admin and tenant self-management tool/CLI parity ([#1261](https://github.com/cedricziel/signaldb/issues/1261)) ([1eadc72](https://github.com/cedricziel/signaldb/commit/1eadc728ace70aff10fa01aaa8766012ace2df4c))
* **mcp:** OAuth 2.1 + DCR connector support for Claude and OpenAI ([#899](https://github.com/cedricziel/signaldb/issues/899)) ([4d0104a](https://github.com/cedricziel/signaldb/commit/4d0104a608ee392e9b25acf686dcd7359fc37631))
* metric/label discovery (MCP+CLI+SDK) and prom/loki UI migration ([#1041](https://github.com/cedricziel/signaldb/issues/1041)) ([afcc72e](https://github.com/cedricziel/signaldb/commit/afcc72e9f87a45e74c97171e8919b90868cd54f4))
* native Query IR — versioned structured query surface (query-ir-core) ([#882](https://github.com/cedricziel/signaldb/issues/882)) ([8774ac0](https://github.com/cedricziel/signaldb/commit/8774ac0fbbe4686cb7aa8b0bba73dbc25f185689))
* **query-ir:** add v2 heatmaps ([#1102](https://github.com/cedricziel/signaldb/issues/1102)) ([96184cf](https://github.com/cedricziel/signaldb/commit/96184cf42809a4cbf0e4a15f592cb544dbb7a597))
* **query-ir:** flamegraph result envelope for profiles ([#1144](https://github.com/cedricziel/signaldb/issues/1144)) ([394407f](https://github.com/cedricziel/signaldb/commit/394407f72756b15c97cb6ce6efcf01ce0b61b33b))
* retry throttled requests in every SignalDB client ([#1260](https://github.com/cedricziel/signaldb/issues/1260)) ([3342dcc](https://github.com/cedricziel/signaldb/commit/3342dcced2cbc489adc7bf5076a0c9059b805adb))
* return server trace context and timings on HTTP responses (Server-Timing + traceresponse) ([#918](https://github.com/cedricziel/signaldb/issues/918)) ([453dd20](https://github.com/cedricziel/signaldb/commit/453dd2050eee95f3daf1c96f77e56964e99a2bb1))
* **router:** Pyroscope OpenAPI parity (CLI/MCP/UI/SDK) ([#1268](https://github.com/cedricziel/signaldb/issues/1268)) ([2b54e2d](https://github.com/cedricziel/signaldb/commit/2b54e2d693801a0bfd9afdf4e982abfac6efc955))
* **router:** schema registry API under /api/v1/schema ([#1219](https://github.com/cedricziel/signaldb/issues/1219)) ([71af424](https://github.com/cedricziel/signaldb/commit/71af424a0d96eb3f87198af4c4213bb89106cf28))
* **sdk:** query surface — SDK covers PromQL/LogQL/TraceQL + Flight SQL (Phase 0) ([#890](https://github.com/cedricziel/signaldb/issues/890)) ([1fde946](https://github.com/cedricziel/signaldb/commit/1fde946cc308ef134f01492b72a3fc874e1c8f95))
* signal rate-limit throttling with Retry-After and a generous default burst ([#1256](https://github.com/cedricziel/signaldb/issues/1256)) ([5584f3f](https://github.com/cedricziel/signaldb/commit/5584f3f1ef7461401a7f1bbbf24302308192b43d))
* span.kind facet + TraceQL support ([#1125](https://github.com/cedricziel/signaldb/issues/1125)) ([35735e5](https://github.com/cedricziel/signaldb/commit/35735e5d204b4fb9f89ddce1dd15296bf9ddfe3c))
* **tempo:** back trace tag discovery with real querier data ([#1258](https://github.com/cedricziel/signaldb/issues/1258)) ([4aeda0d](https://github.com/cedricziel/signaldb/commit/4aeda0d3314fbe7b5546f0411657fdc646e301dd))
* **tenant-table-listing:** list tenant tables from the Iceberg catalog ([#1267](https://github.com/cedricziel/signaldb/issues/1267)) ([5a444c2](https://github.com/cedricziel/signaldb/commit/5a444c261eeab5643d5d2d866385c07e2772ceee))
* **ui:** add user menu and management pages ([#1105](https://github.com/cedricziel/signaldb/issues/1105)) ([c49a93f](https://github.com/cedricziel/signaldb/commit/c49a93ff5d112ce36335c19b12ac3404cdb4a8ba))


### Bug Fixes

* address review findings from [#1260](https://github.com/cedricziel/signaldb/issues/1260) ([#1270](https://github.com/cedricziel/signaldb/issues/1270)) ([d5a6ff5](https://github.com/cedricziel/signaldb/commit/d5a6ff50c49644942cfdc4663d7ab7a2d95fe0fb))
* **mcp:** refresh expired OAuth credentials ([#1100](https://github.com/cedricziel/signaldb/issues/1100)) ([54484e6](https://github.com/cedricziel/signaldb/commit/54484e69083b66e676fcff4e6e4d46fe2c73a766))
* **query-ir:** reapply flamegraph Option fix dropped by a stale merge ([#1146](https://github.com/cedricziel/signaldb/issues/1146)) ([811bb11](https://github.com/cedricziel/signaldb/commit/811bb111b8274e85a181203182a6dd462c3c9438))
* **router:** bound Tempo tag-values queries by time window ([#929](https://github.com/cedricziel/signaldb/issues/929)) ([#979](https://github.com/cedricziel/signaldb/issues/979)) ([7cc301a](https://github.com/cedricziel/signaldb/commit/7cc301adc539a77540682d155425bace30ddc803))


### Performance Improvements

* **flight,wal:** compress Flight IPC payloads and WAL entries ([#945](https://github.com/cedricziel/signaldb/issues/945)) ([#998](https://github.com/cedricziel/signaldb/issues/998)) ([efb5ef4](https://github.com/cedricziel/signaldb/commit/efb5ef4bc85e2e77483f4546255b50c564015827))


### Code Refactoring

* **cli:** make signaldb-cli depend only on the SDK (+ create_user API) ([#874](https://github.com/cedricziel/signaldb/issues/874)) ([8e5cce5](https://github.com/cedricziel/signaldb/commit/8e5cce56c821d69917b55cc8c21a9a2ef55864b7))
* **signaldb-sdk:** dedupe Flight metadata insertion, use try_collect, drop manual test runtime ([#1186](https://github.com/cedricziel/signaldb/issues/1186)) ([4191493](https://github.com/cedricziel/signaldb/commit/4191493fe7560a3702877a155fefcd77b370307f))
* **tempo-api:** simplify pass ([#1176](https://github.com/cedricziel/signaldb/issues/1176)) ([10fd364](https://github.com/cedricziel/signaldb/commit/10fd36487971613586084cc1eb29c0dd93a99b9d))


### Tests

* make tests assert what their names promise ([#966](https://github.com/cedricziel/signaldb/issues/966)) ([446ed06](https://github.com/cedricziel/signaldb/commit/446ed062a7480902ef391884b1c2e12f77ddd66f))
* replace sleep-based synchronization with deterministic waits ([#968](https://github.com/cedricziel/signaldb/issues/968)) ([6391326](https://github.com/cedricziel/signaldb/commit/6391326013c8620f186e4a63c2cdf3bbdf9ee963))

## [0.1.1](https://github.com/cedricziel/signaldb/compare/signaldb-sdk-v0.1.0...signaldb-sdk-v0.1.1) (2026-07-30)


### Features

* add tenant management admin API with OpenAPI spec, SDK, and CLI ([#313](https://github.com/cedricziel/signaldb/issues/313)) ([880c86b](https://github.com/cedricziel/signaldb/commit/880c86b6405a162c84fe88615b7d363585948abd))
* **profiles:** link profiles to traces across the query surface ([#645](https://github.com/cedricziel/signaldb/issues/645)) ([5430d27](https://github.com/cedricziel/signaldb/commit/5430d27281a66a9d88dea0e8d450f73902307137)), closes [#362](https://github.com/cedricziel/signaldb/issues/362) [#363](https://github.com/cedricziel/signaldb/issues/363)
* **router:** Pyroscope-compatible HTTP API ([#644](https://github.com/cedricziel/signaldb/issues/644)) ([dabbede](https://github.com/cedricziel/signaldb/commit/dabbedeebc17ad0d03ac43aa44932b05a37ff857)), closes [#359](https://github.com/cedricziel/signaldb/issues/359)


### Continuous Integration

* drop MSRV policy and fix security audit ignores ([#521](https://github.com/cedricziel/signaldb/issues/521)) ([7da71e3](https://github.com/cedricziel/signaldb/commit/7da71e3d78f593a4361f403e2d4be1e426fb8807))
