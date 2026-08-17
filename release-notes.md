:robot: I have created a release *beep* *boop*
---


<details><summary>grafana-plugin: 1.3.0</summary>

## [1.3.0](https://github.com/cedricziel/signaldb/compare/grafana-plugin-v1.2.0...grafana-plugin-v1.3.0) (2026-08-17)


### Features

* **query-ir:** add v2 heatmaps ([#1102](https://github.com/cedricziel/signaldb/issues/1102)) ([96184cf](https://github.com/cedricziel/signaldb/commit/96184cf42809a4cbf0e4a15f592cb544dbb7a597))


### Bug Fixes

* **grafana-plugin:** wire provisioned routerUrl into the Flight client ([#1151](https://github.com/cedricziel/signaldb/issues/1151)) ([ff883e5](https://github.com/cedricziel/signaldb/commit/ff883e543f1b3831c282eadbcd5db97a857099bf)), closes [#977](https://github.com/cedricziel/signaldb/issues/977)


### Performance Improvements

* **flight,wal:** compress Flight IPC payloads and WAL entries ([#945](https://github.com/cedricziel/signaldb/issues/945)) ([#998](https://github.com/cedricziel/signaldb/issues/998)) ([efb5ef4](https://github.com/cedricziel/signaldb/commit/efb5ef4bc85e2e77483f4546255b50c564015827))


### Code Refactoring

* **grafana-plugin:** simplify pass ([#1191](https://github.com/cedricziel/signaldb/issues/1191)) ([38366c9](https://github.com/cedricziel/signaldb/commit/38366c9270b414763f2c9f883f89167909369691))
* simplify backend workspace (dedup, dead code, redundant clones) ([#1168](https://github.com/cedricziel/signaldb/issues/1168)) ([409b778](https://github.com/cedricziel/signaldb/commit/409b778686a1cea5c54edfba7778c3e9ed3aa29c))
* **ui:** simplify pass ([#1192](https://github.com/cedricziel/signaldb/issues/1192)) ([4c67615](https://github.com/cedricziel/signaldb/commit/4c67615500632225aeeaade5cf745dc8607c9c6d))


### Tests

* delete tautological tests and rewrite salvageable ones as contract tests ([#961](https://github.com/cedricziel/signaldb/issues/961)) ([b3e884a](https://github.com/cedricziel/signaldb/commit/b3e884ad59b4df853429133d5eef2724a8adcada))
* polish medium/low audit findings across the workspace ([#969](https://github.com/cedricziel/signaldb/issues/969)) ([8962f6d](https://github.com/cedricziel/signaldb/commit/8962f6d1d22c8a176d4a1d99376d61b42b1da258))
</details>

<details><summary>signaldb-ui: 0.2.0</summary>

## [0.2.0](https://github.com/cedricziel/signaldb/compare/signaldb-ui-v0.1.2...signaldb-ui-v0.2.0) (2026-08-17)


###   BREAKING CHANGES

* **auth:** POST /api/v1/admin/tenants/{id}/api-keys requires a non-empty `scopes` array; bodies without it are rejected.
* **cli+mcp:** signaldb-cli tenant/api-key/dataset commands move under `admin` (e.g. `signaldb-cli admin tenant list`), and queries now require a language flag (`signaldb-cli query --sql|--promql|--logql|--traceql|--ir`). No back-compat aliases are provided (post-1.0).

### Features

* add an Errors & Exceptions tab ([#1167](https://github.com/cedricziel/signaldb/issues/1167)) ([79f3749](https://github.com/cedricziel/signaldb/commit/79f374916a8add7aa47abd0c8569e13c560a2d7c))
* **api:** code-first OpenAPI  generate spec + Rust/TS clients from annotations ([#856](https://github.com/cedricziel/signaldb/issues/856)) ([e34fbfb](https://github.com/cedricziel/signaldb/commit/e34fbfbd094034416f78597c59b306975dd97271))
* **api:** document Tempo trace query endpoints in OpenAPI + SDK ([#861](https://github.com/cedricziel/signaldb/issues/861)) ([a1e0d7f](https://github.com/cedricziel/signaldb/commit/a1e0d7f9f3c355f8bf73da686db1952487c3e046))
* **auth:** schema:read/schema:write API-key scopes, scopes on every key surface ([#1217](https://github.com/cedricziel/signaldb/issues/1217)) ([34c7a28](https://github.com/cedricziel/signaldb/commit/34c7a28e4e62fad7a05089c1a3543739d6e28450))
* **auth:** tenant:manage API-key scope for the tenant management API ([#1266](https://github.com/cedricziel/signaldb/issues/1266)) ([9dfc193](https://github.com/cedricziel/signaldb/commit/9dfc193a85e813b42f8658bf97cbfd30e3b78f2e))
* **cli+mcp:** CLI & MCP as pure SDK consumers  query --{lang}, admin grouping (Phase 1) ([#892](https://github.com/cedricziel/signaldb/issues/892)) ([92a439e](https://github.com/cedricziel/signaldb/commit/92a439e112da96029733d93db7f274c20c29cbc5))
* compute the traces group table on the server, via a scoped IR aggregate ([#1092](https://github.com/cedricziel/signaldb/issues/1092)) ([ec5c284](https://github.com/cedricziel/signaldb/commit/ec5c284cbe57c0ce34da7f295f08502de2493b82))
* **logs:** surface trace_id/span_id in log query responses ([#1048](https://github.com/cedricziel/signaldb/issues/1048)) ([5a84a04](https://github.com/cedricziel/signaldb/commit/5a84a04b3582befd76ea5f231b887f2cbed253ea))
* **mcp-admin-tool-parity:** platform-admin and tenant self-management tool/CLI parity ([#1261](https://github.com/cedricziel/signaldb/issues/1261)) ([1eadc72](https://github.com/cedricziel/signaldb/commit/1eadc728ace70aff10fa01aaa8766012ace2df4c))
* **mcp:** OAuth 2.1 + DCR connector support for Claude and OpenAI ([#899](https://github.com/cedricziel/signaldb/issues/899)) ([4d0104a](https://github.com/cedricziel/signaldb/commit/4d0104a608ee392e9b25acf686dcd7359fc37631))
* metric/label discovery (MCP+CLI+SDK) and prom/loki UI migration ([#1041](https://github.com/cedricziel/signaldb/issues/1041)) ([afcc72e](https://github.com/cedricziel/signaldb/commit/afcc72e9f87a45e74c97171e8919b90868cd54f4))
* native Query IR  versioned structured query surface (query-ir-core) ([#882](https://github.com/cedricziel/signaldb/issues/882)) ([8774ac0](https://github.com/cedricziel/signaldb/commit/8774ac0fbbe4686cb7aa8b0bba73dbc25f185689))
* **query-ir:** add v2 heatmaps ([#1102](https://github.com/cedricziel/signaldb/issues/1102)) ([96184cf](https://github.com/cedricziel/signaldb/commit/96184cf42809a4cbf0e4a15f592cb544dbb7a597))
* **query-ir:** flamegraph result envelope for profiles ([#1144](https://github.com/cedricziel/signaldb/issues/1144)) ([394407f](https://github.com/cedricziel/signaldb/commit/394407f72756b15c97cb6ce6efcf01ce0b61b33b))
* Real trace-context parenting for documentLoad + complementary log-record telemetry ([#1117](https://github.com/cedricziel/signaldb/issues/1117)) ([43a7c63](https://github.com/cedricziel/signaldb/commit/43a7c63a42a55aed11df304387d286f4bb5bccb9))
* retry throttled requests in every SignalDB client ([#1260](https://github.com/cedricziel/signaldb/issues/1260)) ([3342dcc](https://github.com/cedricziel/signaldb/commit/3342dcced2cbc489adc7bf5076a0c9059b805adb))
* return server trace context and timings on HTTP responses (Server-Timing + traceresponse) ([#918](https://github.com/cedricziel/signaldb/issues/918)) ([453dd20](https://github.com/cedricziel/signaldb/commit/453dd2050eee95f3daf1c96f77e56964e99a2bb1))
* **router:** Pyroscope OpenAPI parity (CLI/MCP/UI/SDK) ([#1268](https://github.com/cedricziel/signaldb/issues/1268)) ([2b54e2d](https://github.com/cedricziel/signaldb/commit/2b54e2d693801a0bfd9afdf4e982abfac6efc955))
* **router:** schema registry API under /api/v1/schema ([#1219](https://github.com/cedricziel/signaldb/issues/1219)) ([71af424](https://github.com/cedricziel/signaldb/commit/71af424a0d96eb3f87198af4c4213bb89106cf28))
* **sdk:** query surface  SDK covers PromQL/LogQL/TraceQL + Flight SQL (Phase 0) ([#890](https://github.com/cedricziel/signaldb/issues/890)) ([1fde946](https://github.com/cedricziel/signaldb/commit/1fde946cc308ef134f01492b72a3fc874e1c8f95))
* **self-monitoring:** runtime-configurable browser telemetry export ([#842](https://github.com/cedricziel/signaldb/issues/842)) ([343b928](https://github.com/cedricziel/signaldb/commit/343b92877d1291406de25923e671ab2a54a98028))
* signal rate-limit throttling with Retry-After and a generous default burst ([#1256](https://github.com/cedricziel/signaldb/issues/1256)) ([5584f3f](https://github.com/cedricziel/signaldb/commit/5584f3f1ef7461401a7f1bbbf24302308192b43d))
* span.kind facet + TraceQL support ([#1125](https://github.com/cedricziel/signaldb/issues/1125)) ([35735e5](https://github.com/cedricziel/signaldb/commit/35735e5d204b4fb9f89ddce1dd15296bf9ddfe3c))
* **tempo:** back trace tag discovery with real querier data ([#1258](https://github.com/cedricziel/signaldb/issues/1258)) ([4aeda0d](https://github.com/cedricziel/signaldb/commit/4aeda0d3314fbe7b5546f0411657fdc646e301dd))
* **tenant-table-listing:** list tenant tables from the Iceberg catalog ([#1267](https://github.com/cedricziel/signaldb/issues/1267)) ([5a444c2](https://github.com/cedricziel/signaldb/commit/5a444c261eeab5643d5d2d866385c07e2772ceee))
* **ui:** add a Catalog tab, entities discovered from telemetry ([#1132](https://github.com/cedricziel/signaldb/issues/1132)) ([9f90539](https://github.com/cedricziel/signaldb/commit/9f90539dbf563c496571223e656de0302995c486))
* **ui:** add a faceted search sidebar to the traces tab ([#1076](https://github.com/cedricziel/signaldb/issues/1076)) ([81a8c24](https://github.com/cedricziel/signaldb/commit/81a8c24f455e69816360e18514c97c754d72d90a))
* **ui:** add user menu and management pages ([#1105](https://github.com/cedricziel/signaldb/issues/1105)) ([c49a93f](https://github.com/cedricziel/signaldb/commit/c49a93ff5d112ce36335c19b12ac3404cdb4a8ba))
* **ui:** catalog entity pages are routes (/catalog/:entity/:identity) ([#1234](https://github.com/cedricziel/signaldb/issues/1234)) ([d99569e](https://github.com/cedricziel/signaldb/commit/d99569eca023528575c03dd30e3e27b09ea9a9c8))
* **ui:** facets with a selection first and expanded ([#1289](https://github.com/cedricziel/signaldb/issues/1289)) ([fef70a7](https://github.com/cedricziel/signaldb/commit/fef70a73e4871d5c2a882d32ae8ee8878910991a))
* **ui:** group, sort, and compile span attributes for readability ([#1123](https://github.com/cedricziel/signaldb/issues/1123)) ([2a99ad2](https://github.com/cedricziel/signaldb/commit/2a99ad23fac4002b2c0ec9c50fa694d7013c914d))
* **ui:** make the explore volume charts readable, and give traces one ([#1075](https://github.com/cedricziel/signaldb/issues/1075)) ([91ec80d](https://github.com/cedricziel/signaldb/commit/91ec80da2a8009a6237fa0e939961b00305fd0f3))
* **ui:** make the span-detail and facet/field sidebars resizable ([#1124](https://github.com/cedricziel/signaldb/issues/1124)) ([73023ae](https://github.com/cedricziel/signaldb/commit/73023aefabe9f29b3fe778c0d4110c6ff3512e58))
* **ui:** multi-select span.kind facet with a boundary-kinds default ([#1288](https://github.com/cedricziel/signaldb/issues/1288)) ([d0fdeef](https://github.com/cedricziel/signaldb/commit/d0fdeef1b16783328a5b60b79a478d469e603678))
* **ui:** native Profiles tab + Catalog/Traces UX improvements ([#1164](https://github.com/cedricziel/signaldb/issues/1164)) ([a9d9223](https://github.com/cedricziel/signaldb/commit/a9d9223aa67a419b924513e961cc61d9ac6c97f5))
* **ui:** read trace detail over the Query IR ([#1284](https://github.com/cedricziel/signaldb/issues/1284)) ([7737eeb](https://github.com/cedricziel/signaldb/commit/7737eebb1ac05bb53d2b73c624286041f79d8423))
* **ui:** render span events and exceptions in the trace view ([#849](https://github.com/cedricziel/signaldb/issues/849)) ([5427c05](https://github.com/cedricziel/signaldb/commit/5427c0527c0cd1d7591da3d9077b1aa88714729a))
* **ui:** resolve attribute labels to semantic titles and descriptions ([#1222](https://github.com/cedricziel/signaldb/issues/1222)) ([e54f935](https://github.com/cedricziel/signaldb/commit/e54f935e2872c543b82ec5937757e18abdc4d869))
* **ui:** rich data-point tooltips on every visualization panel ([#1233](https://github.com/cedricziel/signaldb/issues/1233)) ([a781acb](https://github.com/cedricziel/signaldb/commit/a781acb6014a6264d0430a5d3a5173fe7227c6e3))
* **ui:** rich hover tooltip on waterfall spans ([#1279](https://github.com/cedricziel/signaldb/issues/1279)) ([7db94af](https://github.com/cedricziel/signaldb/commit/7db94af9cc94281a62502d9af04bc6711cd95fbf))
* **ui:** schema hub for inspecting and managing registries ([#1221](https://github.com/cedricziel/signaldb/issues/1221)) ([a265f1a](https://github.com/cedricziel/signaldb/commit/a265f1a80575afa22c3b6926181c7f3d9b315cd6))


### Bug Fixes

* address review findings from [#1260](https://github.com/cedricziel/signaldb/issues/1260) ([#1270](https://github.com/cedricziel/signaldb/issues/1270)) ([d5a6ff5](https://github.com/cedricziel/signaldb/commit/d5a6ff50c49644942cfdc4663d7ab7a2d95fe0fb))
* **common,router:** include every known dataset in the tables grouping ([#1269](https://github.com/cedricziel/signaldb/issues/1269)) ([a895618](https://github.com/cedricziel/signaldb/commit/a8956181e5fd7f4cb91432d5f9622175708d2d70))
* **logql:** carry log and resource attributes as structured metadata ([#1094](https://github.com/cedricziel/signaldb/issues/1094)) ([26b9d15](https://github.com/cedricziel/signaldb/commit/26b9d15457ac84c96ba2affe28d3ea520b40c664))
* **mcp:** refresh expired OAuth credentials ([#1100](https://github.com/cedricziel/signaldb/issues/1100)) ([54484e6](https://github.com/cedricziel/signaldb/commit/54484e69083b66e676fcff4e6e4d46fe2c73a766))
* **query-ir:** reapply flamegraph Option fix dropped by a stale merge ([#1146](https://github.com/cedricziel/signaldb/issues/1146)) ([811bb11](https://github.com/cedricziel/signaldb/commit/811bb111b8274e85a181203182a6dd462c3c9438))
* **router:** bound Tempo tag-values queries by time window ([#929](https://github.com/cedricziel/signaldb/issues/929)) ([#979](https://github.com/cedricziel/signaldb/issues/979)) ([7cc301a](https://github.com/cedricziel/signaldb/commit/7cc301adc539a77540682d155425bace30ddc803))
* **ui:** catalog multi-source discovery, trace routing fix, span_kind fix ([#1210](https://github.com/cedricziel/signaldb/issues/1210)) ([84446f2](https://github.com/cedricziel/signaldb/commit/84446f2ef450be67fb14dbb6c4b4feb477ea0d04))
* **ui:** clear XHR timing resources like fetch instrumentation ([#1034](https://github.com/cedricziel/signaldb/issues/1034)) ([a0bcf10](https://github.com/cedricziel/signaldb/commit/a0bcf10ebe8696960f86d267285b6db266c8eb7b))
* **ui:** collapse high-cardinality navigation span name ([#876](https://github.com/cedricziel/signaldb/issues/876)) ([692efb7](https://github.com/cedricziel/signaldb/commit/692efb73eb2a97bc2fa0887575a9cd834a0faf4a))
* **ui:** draw a zero-duration parent span over its subtree ([#1285](https://github.com/cedricziel/signaldb/issues/1285)) ([a08c7b5](https://github.com/cedricziel/signaldb/commit/a08c7b50bc8c8924d4c47dcbf53d537dc9c47c2b))
* **ui:** give trace/group drill-down real history entries, add a not-found page ([#1130](https://github.com/cedricziel/signaldb/issues/1130)) ([fc68b88](https://github.com/cedricziel/signaldb/commit/fc68b88385024e91188459a03fa347ef3545c75b))
* **ui:** keep tenant context sticky across links that drop the query string ([#1226](https://github.com/cedricziel/signaldb/issues/1226)) ([29ac523](https://github.com/cedricziel/signaldb/commit/29ac523ace850847e6090aee9e8d0e3d78d98fc4))
* **ui:** keep the current path when writing the sticky tenant back into the URL ([#1247](https://github.com/cedricziel/signaldb/issues/1247)) ([641f640](https://github.com/cedricziel/signaldb/commit/641f6403557aaab60817e1fd74978d5b20df16c3))
* **ui:** keep the schema tooltip out of scrolling panes ([#1276](https://github.com/cedricziel/signaldb/issues/1276)) ([c765c9d](https://github.com/cedricziel/signaldb/commit/c765c9d26626e023992bab80e6466786c870a42a))
* **ui:** make signal tabs real routes, not a single replaced entry ([#1128](https://github.com/cedricziel/signaldb/issues/1128)) ([acc1339](https://github.com/cedricziel/signaldb/commit/acc13398ba44a018d6d74ac86c86630a6968f8d5))
* **ui:** make the flame graph tooltip follow the pointer ([#1278](https://github.com/cedricziel/signaldb/issues/1278)) ([bfbe6c9](https://github.com/cedricziel/signaldb/commit/bfbe6c9f8d3329a6e709c7b8f33229b577c6d993))
* **ui:** read the trace detail's span duration from duration_nanos ([#1286](https://github.com/cedricziel/signaldb/issues/1286)) ([1281df2](https://github.com/cedricziel/signaldb/commit/1281df2e95d277ac79de1a8a1038b3c8625074e3))
* **ui:** remember the tenant context across tabs so bare deep links resume it ([#1231](https://github.com/cedricziel/signaldb/issues/1231)) ([c794883](https://github.com/cedricziel/signaldb/commit/c7948837a17325b729fe81df45fc9b19d81acfa9))
* **ui:** repair query-IR 400s from Layer 2 logical schema migration ([#1118](https://github.com/cedricziel/signaldb/issues/1118)) ([7e1bea8](https://github.com/cedricziel/signaldb/commit/7e1bea823af0ad26a2aa76ca106d17838dff9596))
* **ui:** route Metrics builder default queries through Query IR ([#1138](https://github.com/cedricziel/signaldb/issues/1138)) ([4056261](https://github.com/cedricziel/signaldb/commit/4056261e0d406d5ae73dc2fe20bc136b8e866bb8))


### Code Refactoring

* **cli:** make signaldb-cli depend only on the SDK (+ create_user API) ([#874](https://github.com/cedricziel/signaldb/issues/874)) ([8e5cce5](https://github.com/cedricziel/signaldb/commit/8e5cce56c821d69917b55cc8c21a9a2ef55864b7))
* **tempo-api:** simplify pass ([#1176](https://github.com/cedricziel/signaldb/issues/1176)) ([10fd364](https://github.com/cedricziel/signaldb/commit/10fd36487971613586084cc1eb29c0dd93a99b9d))
* **ui:** simplify pass ([#1192](https://github.com/cedricziel/signaldb/issues/1192)) ([4c67615](https://github.com/cedricziel/signaldb/commit/4c67615500632225aeeaade5cf745dc8607c9c6d))
</details>

<details><summary>logql: 0.1.2</summary>

## [0.1.2](https://github.com/cedricziel/signaldb/compare/logql-v0.1.1...logql-v0.1.2) (2026-08-17)


### Code Refactoring

* **logql:** simplify pass ([#1179](https://github.com/cedricziel/signaldb/issues/1179)) ([9c83c1b](https://github.com/cedricziel/signaldb/commit/9c83c1ba5a23abaab0107c4c217f6a3a0e6baafe))
</details>

<details><summary>loki-api: 0.1.2</summary>

## [0.1.2](https://github.com/cedricziel/signaldb/compare/loki-api-v0.1.1...loki-api-v0.1.2) (2026-08-17)


### Features

* **logs:** surface trace_id/span_id in log query responses ([#1048](https://github.com/cedricziel/signaldb/issues/1048)) ([5a84a04](https://github.com/cedricziel/signaldb/commit/5a84a04b3582befd76ea5f231b887f2cbed253ea))


### Code Refactoring

* **loki-api:** extract STATUS_SUCCESS constant ([#1180](https://github.com/cedricziel/signaldb/issues/1180)) ([b424031](https://github.com/cedricziel/signaldb/commit/b424031d2b04b9d4aecd46b7c5f8cf27aeaf37ea))
</details>

<details><summary>mcp-server: 0.2.0</summary>

## [0.2.0](https://github.com/cedricziel/signaldb/compare/mcp-server-v0.1.0...mcp-server-v0.2.0) (2026-08-17)


###   BREAKING CHANGES

* **auth:** POST /api/v1/admin/tenants/{id}/api-keys requires a non-empty `scopes` array; bodies without it are rejected.
* **cli+mcp:** signaldb-cli tenant/api-key/dataset commands move under `admin` (e.g. `signaldb-cli admin tenant list`), and queries now require a language flag (`signaldb-cli query --sql|--promql|--logql|--traceql|--ir`). No back-compat aliases are provided (post-1.0).

### Features

* **auth:** schema:read/schema:write API-key scopes, scopes on every key surface ([#1217](https://github.com/cedricziel/signaldb/issues/1217)) ([34c7a28](https://github.com/cedricziel/signaldb/commit/34c7a28e4e62fad7a05089c1a3543739d6e28450))
* **auth:** tenant:manage API-key scope for the tenant management API ([#1266](https://github.com/cedricziel/signaldb/issues/1266)) ([9dfc193](https://github.com/cedricziel/signaldb/commit/9dfc193a85e813b42f8658bf97cbfd30e3b78f2e))
* **cli+mcp:** CLI & MCP as pure SDK consumers  query --{lang}, admin grouping (Phase 1) ([#892](https://github.com/cedricziel/signaldb/issues/892)) ([92a439e](https://github.com/cedricziel/signaldb/commit/92a439e112da96029733d93db7f274c20c29cbc5))
* **clients:** schema registry in SDK, CLI, and MCP ([#1223](https://github.com/cedricziel/signaldb/issues/1223)) ([1838583](https://github.com/cedricziel/signaldb/commit/1838583910be33e03d72b2be15e17d819031c9c5))
* **mcp-admin-tool-parity:** platform-admin and tenant self-management tool/CLI parity ([#1261](https://github.com/cedricziel/signaldb/issues/1261)) ([1eadc72](https://github.com/cedricziel/signaldb/commit/1eadc728ace70aff10fa01aaa8766012ace2df4c))
* **mcp-server:** add prompts and argument completion support ([#1139](https://github.com/cedricziel/signaldb/issues/1139)) ([dbfeac9](https://github.com/cedricziel/signaldb/commit/dbfeac9d43f2b3fb2f207de046702787fdbd0ae0))
* **mcp-server:** get_profile tool with interactive flamegraph view ([#1145](https://github.com/cedricziel/signaldb/issues/1145)) ([7d7beb7](https://github.com/cedricziel/signaldb/commit/7d7beb794028b73f928e4d6e2a03d3ebed00c64e))
* **mcp:** audit, trace, meter, and bound every tool call ([#1255](https://github.com/cedricziel/signaldb/issues/1255)) ([6627df0](https://github.com/cedricziel/signaldb/commit/6627df0f3f2fc0cff97692d3e465c23bc640e5c2))
* **mcp:** make Streamable HTTP Host allowlist configurable ([#881](https://github.com/cedricziel/signaldb/issues/881)) ([a549e7e](https://github.com/cedricziel/signaldb/commit/a549e7e3550967d446bdb05f7f3ea27ce64f07a1))
* **mcp:** OAuth 2.1 + DCR connector support for Claude and OpenAI ([#899](https://github.com/cedricziel/signaldb/issues/899)) ([4d0104a](https://github.com/cedricziel/signaldb/commit/4d0104a608ee392e9b25acf686dcd7359fc37631))
* **mcp:** scaffold standalone signaldb-mcp server with bearer auth ([#864](https://github.com/cedricziel/signaldb/issues/864)) ([0affbf5](https://github.com/cedricziel/signaldb/commit/0affbf5e92a87dabe041b7766fb97cd1f639e73c))
* **mcp:** serve a single-trace waterfall via the MCP Apps extension ([#1016](https://github.com/cedricziel/signaldb/issues/1016)) ([db434c7](https://github.com/cedricziel/signaldb/commit/db434c7de6fa8456e9f59557f0adc9104a3bbd28))
* **mcp:** Tempo-backed read tools (search_traces, get_trace, discover_attributes) ([#863](https://github.com/cedricziel/signaldb/issues/863)) ([3888f5d](https://github.com/cedricziel/signaldb/commit/3888f5d7e292a279c94e72eb871f80a564e56811))
* metric/label discovery (MCP+CLI+SDK) and prom/loki UI migration ([#1041](https://github.com/cedricziel/signaldb/issues/1041)) ([afcc72e](https://github.com/cedricziel/signaldb/commit/afcc72e9f87a45e74c97171e8919b90868cd54f4))
* one signaldb binary with the services as subcommands ([#1204](https://github.com/cedricziel/signaldb/issues/1204)) ([77f3278](https://github.com/cedricziel/signaldb/commit/77f3278ca445ac9b28bf955b0e482d4366a27c07))
* **query-ir:** add v2 heatmaps ([#1102](https://github.com/cedricziel/signaldb/issues/1102)) ([96184cf](https://github.com/cedricziel/signaldb/commit/96184cf42809a4cbf0e4a15f592cb544dbb7a597))
* retry throttled requests in every SignalDB client ([#1260](https://github.com/cedricziel/signaldb/issues/1260)) ([3342dcc](https://github.com/cedricziel/signaldb/commit/3342dcced2cbc489adc7bf5076a0c9059b805adb))
* **router:** Pyroscope OpenAPI parity (CLI/MCP/UI/SDK) ([#1268](https://github.com/cedricziel/signaldb/issues/1268)) ([2b54e2d](https://github.com/cedricziel/signaldb/commit/2b54e2d693801a0bfd9afdf4e982abfac6efc955))
* **tenant-table-listing:** list tenant tables from the Iceberg catalog ([#1267](https://github.com/cedricziel/signaldb/issues/1267)) ([5a444c2](https://github.com/cedricziel/signaldb/commit/5a444c261eeab5643d5d2d866385c07e2772ceee))


### Bug Fixes

* address review findings from [#1260](https://github.com/cedricziel/signaldb/issues/1260) ([#1270](https://github.com/cedricziel/signaldb/issues/1270)) ([d5a6ff5](https://github.com/cedricziel/signaldb/commit/d5a6ff50c49644942cfdc4663d7ab7a2d95fe0fb))
* **mcp-server:** declare query_ir's query param as an object ([#1129](https://github.com/cedricziel/signaldb/issues/1129)) ([d30926d](https://github.com/cedricziel/signaldb/commit/d30926d4027baa38399666cf2a3439ff49e0a438)), closes [#1113](https://github.com/cedricziel/signaldb/issues/1113)
* **mcp-server:** set SEP-2549 cacheHints on tools/resources results ([#1136](https://github.com/cedricziel/signaldb/issues/1136)) ([3a43822](https://github.com/cedricziel/signaldb/commit/3a43822d233fa9a419d56a78831d9033c9a01236))
* **mcp:** add connect and request timeouts to router HTTP client ([#885](https://github.com/cedricziel/signaldb/issues/885)) ([#976](https://github.com/cedricziel/signaldb/issues/976)) ([f0f2182](https://github.com/cedricziel/signaldb/commit/f0f21824b654d57668e2c235f310d3a048a314f4))
* **mcp:** refresh expired OAuth credentials ([#1100](https://github.com/cedricziel/signaldb/issues/1100)) ([54484e6](https://github.com/cedricziel/signaldb/commit/54484e69083b66e676fcff4e6e4d46fe2c73a766))


### Performance Improvements

* CPU target features and jemalloc allocator for release builds ([#970](https://github.com/cedricziel/signaldb/issues/970)) ([766e2d1](https://github.com/cedricziel/signaldb/commit/766e2d1c82dad65a674184edaf2e8d67cb4083dd))


### Code Refactoring

* **mcp-server:** simplify pass ([#1181](https://github.com/cedricziel/signaldb/issues/1181)) ([c192ad2](https://github.com/cedricziel/signaldb/commit/c192ad22934f1f46eb22c463c0a2692f7335fb03))
* **mcp:** make signaldb-mcp depend only on the SDK (forward-only auth) ([#873](https://github.com/cedricziel/signaldb/issues/873)) ([d404af6](https://github.com/cedricziel/signaldb/commit/d404af62bad3872b2a8f722067053d4adc083adb))


### Tests

* make tests assert what their names promise ([#966](https://github.com/cedricziel/signaldb/issues/966)) ([446ed06](https://github.com/cedricziel/signaldb/commit/446ed062a7480902ef391884b1c2e12f77ddd66f))
* replace sleep-based synchronization with deterministic waits ([#968](https://github.com/cedricziel/signaldb/issues/968)) ([6391326](https://github.com/cedricziel/signaldb/commit/6391326013c8620f186e4a63c2cdf3bbdf9ee963))
</details>

<details><summary>prometheus-api: 0.1.3</summary>

## [0.1.3](https://github.com/cedricziel/signaldb/compare/prometheus-api-v0.1.2...prometheus-api-v0.1.3) (2026-08-17)


### Code Refactoring

* **prometheus-api:** extract STATUS_SUCCESS/STATUS_ERROR constants ([#1182](https://github.com/cedricziel/signaldb/issues/1182)) ([130d95c](https://github.com/cedricziel/signaldb/commit/130d95c39933b2d46448ca8ff86f5ddaaf53d47e))
</details>

<details><summary>pyroscope-api: 0.1.2</summary>

## [0.1.2](https://github.com/cedricziel/signaldb/compare/pyroscope-api-v0.1.1...pyroscope-api-v0.1.2) (2026-08-17)


### Features

* **router:** Pyroscope OpenAPI parity (CLI/MCP/UI/SDK) ([#1268](https://github.com/cedricziel/signaldb/issues/1268)) ([2b54e2d](https://github.com/cedricziel/signaldb/commit/2b54e2d693801a0bfd9afdf4e982abfac6efc955))


### Code Refactoring

* **pyroscope-api:** clarify ProfileType::from_type_unit and add missing rename_all ([#1183](https://github.com/cedricziel/signaldb/issues/1183)) ([5fcd576](https://github.com/cedricziel/signaldb/commit/5fcd576ef7a44279c9d7be979ad2566de064817c))
</details>

<details><summary>signal-producer: 0.2.2</summary>

## [0.2.2](https://github.com/cedricziel/signaldb/compare/signal-producer-v0.2.1...signal-producer-v0.2.2) (2026-08-17)


### Bug Fixes

* **signal-producer:** emit realistic span durations instead of zero ([#797](https://github.com/cedricziel/signaldb/issues/797)) ([#974](https://github.com/cedricziel/signaldb/issues/974)) ([20c9120](https://github.com/cedricziel/signaldb/commit/20c9120762aff967590b62ffc5b3bce6c3e9bfea))


### Code Refactoring

* **signal-producer:** remove dead end_ms parameter from kafka_produce ([#1178](https://github.com/cedricziel/signaldb/issues/1178)) ([abb8c7f](https://github.com/cedricziel/signaldb/commit/abb8c7f749cf2963b10bec216d932687b4d89cbf))
</details>

<details><summary>signaldb-api: 0.2.0</summary>

## [0.2.0](https://github.com/cedricziel/signaldb/compare/signaldb-api-v0.1.1...signaldb-api-v0.2.0) (2026-08-17)


###   BREAKING CHANGES

* **auth:** POST /api/v1/admin/tenants/{id}/api-keys requires a non-empty `scopes` array; bodies without it are rejected.

### Features

* **api:** code-first OpenAPI  generate spec + Rust/TS clients from annotations ([#856](https://github.com/cedricziel/signaldb/issues/856)) ([e34fbfb](https://github.com/cedricziel/signaldb/commit/e34fbfbd094034416f78597c59b306975dd97271))
* **auth:** schema:read/schema:write API-key scopes, scopes on every key surface ([#1217](https://github.com/cedricziel/signaldb/issues/1217)) ([34c7a28](https://github.com/cedricziel/signaldb/commit/34c7a28e4e62fad7a05089c1a3543739d6e28450))


### Code Refactoring

* **cli:** make signaldb-cli depend only on the SDK (+ create_user API) ([#874](https://github.com/cedricziel/signaldb/issues/874)) ([8e5cce5](https://github.com/cedricziel/signaldb/commit/8e5cce56c821d69917b55cc8c21a9a2ef55864b7))
* **signaldb-api:** merge extensions.rs into schemas.rs, drop unused Default derives ([#1184](https://github.com/cedricziel/signaldb/issues/1184)) ([665e238](https://github.com/cedricziel/signaldb/commit/665e2381b808f85cb9010ea1ff1acf08bf7cef47))
</details>

<details><summary>signaldb-sdk: 0.2.0</summary>

## [0.2.0](https://github.com/cedricziel/signaldb/compare/signaldb-sdk-v0.1.1...signaldb-sdk-v0.2.0) (2026-08-17)


###   BREAKING CHANGES

* **auth:** POST /api/v1/admin/tenants/{id}/api-keys requires a non-empty `scopes` array; bodies without it are rejected.
* **cli+mcp:** signaldb-cli tenant/api-key/dataset commands move under `admin` (e.g. `signaldb-cli admin tenant list`), and queries now require a language flag (`signaldb-cli query --sql|--promql|--logql|--traceql|--ir`). No back-compat aliases are provided (post-1.0).

### Features

* **api:** code-first OpenAPI  generate spec + Rust/TS clients from annotations ([#856](https://github.com/cedricziel/signaldb/issues/856)) ([e34fbfb](https://github.com/cedricziel/signaldb/commit/e34fbfbd094034416f78597c59b306975dd97271))
* **api:** document Tempo trace query endpoints in OpenAPI + SDK ([#861](https://github.com/cedricziel/signaldb/issues/861)) ([a1e0d7f](https://github.com/cedricziel/signaldb/commit/a1e0d7f9f3c355f8bf73da686db1952487c3e046))
* **auth:** schema:read/schema:write API-key scopes, scopes on every key surface ([#1217](https://github.com/cedricziel/signaldb/issues/1217)) ([34c7a28](https://github.com/cedricziel/signaldb/commit/34c7a28e4e62fad7a05089c1a3543739d6e28450))
* **auth:** tenant:manage API-key scope for the tenant management API ([#1266](https://github.com/cedricziel/signaldb/issues/1266)) ([9dfc193](https://github.com/cedricziel/signaldb/commit/9dfc193a85e813b42f8658bf97cbfd30e3b78f2e))
* **cli+mcp:** CLI & MCP as pure SDK consumers  query --{lang}, admin grouping (Phase 1) ([#892](https://github.com/cedricziel/signaldb/issues/892)) ([92a439e](https://github.com/cedricziel/signaldb/commit/92a439e112da96029733d93db7f274c20c29cbc5))
* **clients:** schema registry in SDK, CLI, and MCP ([#1223](https://github.com/cedricziel/signaldb/issues/1223)) ([1838583](https://github.com/cedricziel/signaldb/commit/1838583910be33e03d72b2be15e17d819031c9c5))
* **mcp-admin-tool-parity:** platform-admin and tenant self-management tool/CLI parity ([#1261](https://github.com/cedricziel/signaldb/issues/1261)) ([1eadc72](https://github.com/cedricziel/signaldb/commit/1eadc728ace70aff10fa01aaa8766012ace2df4c))
* **mcp:** OAuth 2.1 + DCR connector support for Claude and OpenAI ([#899](https://github.com/cedricziel/signaldb/issues/899)) ([4d0104a](https://github.com/cedricziel/signaldb/commit/4d0104a608ee392e9b25acf686dcd7359fc37631))
* metric/label discovery (MCP+CLI+SDK) and prom/loki UI migration ([#1041](https://github.com/cedricziel/signaldb/issues/1041)) ([afcc72e](https://github.com/cedricziel/signaldb/commit/afcc72e9f87a45e74c97171e8919b90868cd54f4))
* native Query IR  versioned structured query surface (query-ir-core) ([#882](https://github.com/cedricziel/signaldb/issues/882)) ([8774ac0](https://github.com/cedricziel/signaldb/commit/8774ac0fbbe4686cb7aa8b0bba73dbc25f185689))
* **query-ir:** add v2 heatmaps ([#1102](https://github.com/cedricziel/signaldb/issues/1102)) ([96184cf](https://github.com/cedricziel/signaldb/commit/96184cf42809a4cbf0e4a15f592cb544dbb7a597))
* **query-ir:** flamegraph result envelope for profiles ([#1144](https://github.com/cedricziel/signaldb/issues/1144)) ([394407f](https://github.com/cedricziel/signaldb/commit/394407f72756b15c97cb6ce6efcf01ce0b61b33b))
* retry throttled requests in every SignalDB client ([#1260](https://github.com/cedricziel/signaldb/issues/1260)) ([3342dcc](https://github.com/cedricziel/signaldb/commit/3342dcced2cbc489adc7bf5076a0c9059b805adb))
* return server trace context and timings on HTTP responses (Server-Timing + traceresponse) ([#918](https://github.com/cedricziel/signaldb/issues/918)) ([453dd20](https://github.com/cedricziel/signaldb/commit/453dd2050eee95f3daf1c96f77e56964e99a2bb1))
* **router:** Pyroscope OpenAPI parity (CLI/MCP/UI/SDK) ([#1268](https://github.com/cedricziel/signaldb/issues/1268)) ([2b54e2d](https://github.com/cedricziel/signaldb/commit/2b54e2d693801a0bfd9afdf4e982abfac6efc955))
* **router:** schema registry API under /api/v1/schema ([#1219](https://github.com/cedricziel/signaldb/issues/1219)) ([71af424](https://github.com/cedricziel/signaldb/commit/71af424a0d96eb3f87198af4c4213bb89106cf28))
* **sdk:** query surface  SDK covers PromQL/LogQL/TraceQL + Flight SQL (Phase 0) ([#890](https://github.com/cedricziel/signaldb/issues/890)) ([1fde946](https://github.com/cedricziel/signaldb/commit/1fde946cc308ef134f01492b72a3fc874e1c8f95))
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
</details>

<details><summary>tempo-api: 0.1.2</summary>

## [0.1.2](https://github.com/cedricziel/signaldb/compare/tempo-api-v0.1.1...tempo-api-v0.1.2) (2026-08-17)


### Features

* **api:** document Tempo trace query endpoints in OpenAPI + SDK ([#861](https://github.com/cedricziel/signaldb/issues/861)) ([a1e0d7f](https://github.com/cedricziel/signaldb/commit/a1e0d7f9f3c355f8bf73da686db1952487c3e046))
* **querier,router:** surface span events on the single-trace path ([#848](https://github.com/cedricziel/signaldb/issues/848)) ([5b344e9](https://github.com/cedricziel/signaldb/commit/5b344e98b6e787aeca35d68bf18ca5ca92657454))
* **tempo:** back trace tag discovery with real querier data ([#1258](https://github.com/cedricziel/signaldb/issues/1258)) ([4aeda0d](https://github.com/cedricziel/signaldb/commit/4aeda0d3314fbe7b5546f0411657fdc646e301dd))


### Bug Fixes

* **writer,tempo-api:** stop leaking Option Debug into logs; accept lowercase Tempo tag scopes ([#1149](https://github.com/cedricziel/signaldb/issues/1149)) ([4a83388](https://github.com/cedricziel/signaldb/commit/4a8338801252c36a948efa10d1a5cfe0d4f7de5a))


### Code Refactoring

* **tempo-api:** simplify pass ([#1176](https://github.com/cedricziel/signaldb/issues/1176)) ([10fd364](https://github.com/cedricziel/signaldb/commit/10fd36487971613586084cc1eb29c0dd93a99b9d))


### Tests

* delete tautological tests and rewrite salvageable ones as contract tests ([#961](https://github.com/cedricziel/signaldb/issues/961)) ([b3e884a](https://github.com/cedricziel/signaldb/commit/b3e884ad59b4df853429133d5eef2724a8adcada))
* polish medium/low audit findings across the workspace ([#969](https://github.com/cedricziel/signaldb/issues/969)) ([8962f6d](https://github.com/cedricziel/signaldb/commit/8962f6d1d22c8a176d4a1d99376d61b42b1da258))
</details>

<details><summary>tests-integration: 0.1.5</summary>

### Dependencies


</details>

<details><summary>acceptor: 0.3.0</summary>

## [0.3.0](https://github.com/cedricziel/signaldb/compare/acceptor-v0.2.1...acceptor-v0.3.0) (2026-08-17)


### Features

* one signaldb binary with the services as subcommands ([#1204](https://github.com/cedricziel/signaldb/issues/1204)) ([77f3278](https://github.com/cedricziel/signaldb/commit/77f3278ca445ac9b28bf955b0e482d4366a27c07))
* **self-monitoring:** runtime-configurable browser telemetry export ([#842](https://github.com/cedricziel/signaldb/issues/842)) ([343b928](https://github.com/cedricziel/signaldb/commit/343b92877d1291406de25923e671ab2a54a98028))
* semconv CLIENT spans on Flight call sites ([#905](https://github.com/cedricziel/signaldb/issues/905)) ([3047cbb](https://github.com/cedricziel/signaldb/commit/3047cbbc68f03e7d586d4a2caabaa2bd7c660ca1))
* semconv self-tracing foundations (resource, span factories, acceptor boundary) ([#903](https://github.com/cedricziel/signaldb/issues/903)) ([dbe4ca2](https://github.com/cedricziel/signaldb/commit/dbe4ca2389ac8db0dba721f66d79db4d0475ed76))
* signal rate-limit throttling with Retry-After and a generous default burst ([#1256](https://github.com/cedricziel/signaldb/issues/1256)) ([5584f3f](https://github.com/cedricziel/signaldb/commit/5584f3f1ef7461401a7f1bbbf24302308192b43d))
* **tracing:** add server.address and network.peer to RPC spans ([#1111](https://github.com/cedricziel/signaldb/issues/1111)) ([4e64934](https://github.com/cedricziel/signaldb/commit/4e64934814762c25226a3a7529bc9d695035d578))


### Bug Fixes

* **acceptor:** accept empty AnyValue objects in OTLP/JSON requests ([#1135](https://github.com/cedricziel/signaldb/issues/1135)) ([6c3a701](https://github.com/cedricziel/signaldb/commit/6c3a701a8234768a9d0d34eceb329be6e149d75d)), closes [#1134](https://github.com/cedricziel/signaldb/issues/1134)
* **acceptor:** accept gzip/zstd-compressed OTLP/gRPC requests ([#1133](https://github.com/cedricziel/signaldb/issues/1133)) ([081968d](https://github.com/cedricziel/signaldb/commit/081968d6eec8d3497c43893f6ec5d8aa7ebad1a3)), closes [#1131](https://github.com/cedricziel/signaldb/issues/1131)
* **acceptor:** adopt upstream's AnyValue deserialize fix verbatim ([#1140](https://github.com/cedricziel/signaldb/issues/1140)) ([31e9fa8](https://github.com/cedricziel/signaldb/commit/31e9fa8f78aba4fe2142b165966acad45d9f45bc))
* **acceptor:** dead-letter poison WAL entries in the retry consumer ([#1015](https://github.com/cedricziel/signaldb/issues/1015)) ([866821c](https://github.com/cedricziel/signaldb/commit/866821c68793361c26f4a313423d00457777b739))
* **acceptor:** dead-letter poison WAL entries on first failure ([#1059](https://github.com/cedricziel/signaldb/issues/1059)) ([9d43c85](https://github.com/cedricziel/signaldb/commit/9d43c85445cb8c6d1bcb19279e29015680dc3fd4))
* **acceptor:** dead-letter writer-rejected WAL entries instead of wedging the retry pass ([#1063](https://github.com/cedricziel/signaldb/issues/1063)) ([7fc6ada](https://github.com/cedricziel/signaldb/commit/7fc6ada1ea922784220789f304fb3f8448ff8ef1)), closes [#1060](https://github.com/cedricziel/signaldb/issues/1060)
* **acceptor:** reject exports on OTLP conversion failure instead of ACKing empty batches ([#926](https://github.com/cedricziel/signaldb/issues/926)) ([#981](https://github.com/cedricziel/signaldb/issues/981)) ([02c0a3b](https://github.com/cedricziel/signaldb/commit/02c0a3b99fdc1327595ad8a0bf8434de1977615d))
* **build:** stop jemalloc heap profiling from crashing musl images ([#1126](https://github.com/cedricziel/signaldb/issues/1126)) ([98b2996](https://github.com/cedricziel/signaldb/commit/98b299660ef31b56d73e079a2477166b415e736e))
* **flight:** set explicit gRPC message-size limits and chunk oversized batches ([#990](https://github.com/cedricziel/signaldb/issues/990)) ([6499175](https://github.com/cedricziel/signaldb/commit/6499175d0e6402e1350ad28803d0b08954e43fe1))
* metrics without service.name land as 'unknown'; boot log flood demoted to debug ([#1227](https://github.com/cedricziel/signaldb/issues/1227)) ([7b5ea34](https://github.com/cedricziel/signaldb/commit/7b5ea343096ea8a7c0f62575029ac1e838ec514c))
* **model:** stop flattening trace hierarchies to root + direct children ([#1018](https://github.com/cedricziel/signaldb/issues/1018)) ([5fee337](https://github.com/cedricziel/signaldb/commit/5fee33711628bf3f041c436c34f363f114ed93fb))


### Performance Improvements

* CPU target features and jemalloc allocator for release builds ([#970](https://github.com/cedricziel/signaldb/issues/970)) ([766e2d1](https://github.com/cedricziel/signaldb/commit/766e2d1c82dad65a674184edaf2e8d67cb4083dd))
* **flight,wal:** compress Flight IPC payloads and WAL entries ([#945](https://github.com/cedricziel/signaldb/issues/945)) ([#998](https://github.com/cedricziel/signaldb/issues/998)) ([efb5ef4](https://github.com/cedricziel/signaldb/commit/efb5ef4bc85e2e77483f4546255b50c564015827))


### Documentation

* flight-communication.md read path now describes the CLIENT hop. ([3047cbb](https://github.com/cedricziel/signaldb/commit/3047cbbc68f03e7d586d4a2caabaa2bd7c660ca1))
* **openspec:** backfill OTLP ingest specs + profiles HTTP test coverage ([#852](https://github.com/cedricziel/signaldb/issues/852)) ([3382a3e](https://github.com/cedricziel/signaldb/commit/3382a3e939f21b11dfa550bd8d3b250251044d06))


### Code Refactoring

* **acceptor:** simplify pass ([#1170](https://github.com/cedricziel/signaldb/issues/1170)) ([3d7f263](https://github.com/cedricziel/signaldb/commit/3d7f263d200316cfa3d339496dedda5c9045bcc3))
* **logging:** forbid log:: macros in favor of tracing:: ([#1006](https://github.com/cedricziel/signaldb/issues/1006)) ([071ebb4](https://github.com/cedricziel/signaldb/commit/071ebb47d02f2d6e43ccfb60380c00e3be929248))
* simplify backend workspace (dedup, dead code, redundant clones) ([#1168](https://github.com/cedricziel/signaldb/issues/1168)) ([409b778](https://github.com/cedricziel/signaldb/commit/409b778686a1cea5c54edfba7778c3e9ed3aa29c))
* span hygiene sweep and construction guard ([#907](https://github.com/cedricziel/signaldb/issues/907)) ([c1f7b81](https://github.com/cedricziel/signaldb/commit/c1f7b81fbc00ae5fd6c9b948f9fb35c9d5a27d26))


### Tests

* polish medium/low audit findings across the workspace ([#969](https://github.com/cedricziel/signaldb/issues/969)) ([8962f6d](https://github.com/cedricziel/signaldb/commit/8962f6d1d22c8a176d4a1d99376d61b42b1da258))
</details>

<details><summary>common: 0.3.0</summary>

## [0.3.0](https://github.com/cedricziel/signaldb/compare/common-v0.2.1...common-v0.3.0) (2026-08-17)


###   BREAKING CHANGES

* **auth:** POST /api/v1/admin/tenants/{id}/api-keys requires a non-empty `scopes` array; bodies without it are rejected.
* **compactor:** [compactor.orphan_cleanup] revalidate_before_delete no longer exists. Note that a leftover key is silently ignored rather than rejected -- the design assumed unknown keys fail config parsing, but neither config struct sets serde(deny_unknown_fields), and adding it is not a safe drive-by because figment's env provider populates the same structs. Documented in the compactor configuration reference; tightening the structs deserves its own change.
* **compactor:** [compactor] min_input_file_size_kb is replaced by max_input_file_size_kb (semantics inverted) and max_files_per_job is removed. No backward-compat alias is provided.

### Features

* add an Errors & Exceptions tab ([#1167](https://github.com/cedricziel/signaldb/issues/1167)) ([79f3749](https://github.com/cedricziel/signaldb/commit/79f374916a8add7aa47abd0c8569e13c560a2d7c))
* **api:** code-first OpenAPI  generate spec + Rust/TS clients from annotations ([#856](https://github.com/cedricziel/signaldb/issues/856)) ([e34fbfb](https://github.com/cedricziel/signaldb/commit/e34fbfbd094034416f78597c59b306975dd97271))
* **auth:** schema:read/schema:write API-key scopes, scopes on every key surface ([#1217](https://github.com/cedricziel/signaldb/issues/1217)) ([34c7a28](https://github.com/cedricziel/signaldb/commit/34c7a28e4e62fad7a05089c1a3543739d6e28450))
* **auth:** tenant:manage API-key scope for the tenant management API ([#1266](https://github.com/cedricziel/signaldb/issues/1266)) ([9dfc193](https://github.com/cedricziel/signaldb/commit/9dfc193a85e813b42f8658bf97cbfd30e3b78f2e))
* **common:** bundled + custom schema registries with a precedence resolver ([#1218](https://github.com/cedricziel/signaldb/issues/1218)) ([41580e6](https://github.com/cedricziel/signaldb/commit/41580e675abba5a5c6fb1cdcbb1ec390dea3a5ac))
* **compactor:** reclaim metadata backlog and enable orphan cleanup by default ([#1008](https://github.com/cedricziel/signaldb/issues/1008)) ([908ea79](https://github.com/cedricziel/signaldb/commit/908ea798e78a6d2dd90396f56e584275e9dfc9b3))
* compute the traces group table on the server, via a scoped IR aggregate ([#1092](https://github.com/cedricziel/signaldb/issues/1092)) ([ec5c284](https://github.com/cedricziel/signaldb/commit/ec5c284cbe57c0ce34da7f295f08502de2493b82))
* DB client spans, query stage spans, compactor job spans ([#906](https://github.com/cedricziel/signaldb/issues/906)) ([04a4c4e](https://github.com/cedricziel/signaldb/commit/04a4c4e5788cf6531e0421b50b523b04ac4db38b))
* **iceberg:** tune the Parquet writer properties now that they are honored ([#1025](https://github.com/cedricziel/signaldb/issues/1025)) ([219132a](https://github.com/cedricziel/signaldb/commit/219132a3eb1bba1c15975245081ad4a2d54eb7d1))
* **mcp-admin-tool-parity:** platform-admin and tenant self-management tool/CLI parity ([#1261](https://github.com/cedricziel/signaldb/issues/1261)) ([1eadc72](https://github.com/cedricziel/signaldb/commit/1eadc728ace70aff10fa01aaa8766012ace2df4c))
* **mcp:** audit, trace, meter, and bound every tool call ([#1255](https://github.com/cedricziel/signaldb/issues/1255)) ([6627df0](https://github.com/cedricziel/signaldb/commit/6627df0f3f2fc0cff97692d3e465c23bc640e5c2))
* **mcp:** OAuth 2.1 + DCR connector support for Claude and OpenAI ([#899](https://github.com/cedricziel/signaldb/issues/899)) ([4d0104a](https://github.com/cedricziel/signaldb/commit/4d0104a608ee392e9b25acf686dcd7359fc37631))
* **mcp:** scaffold standalone signaldb-mcp server with bearer auth ([#864](https://github.com/cedricziel/signaldb/issues/864)) ([0affbf5](https://github.com/cedricziel/signaldb/commit/0affbf5e92a87dabe041b7766fb97cd1f639e73c))
* **model:** add span events to the Span model ([#847](https://github.com/cedricziel/signaldb/issues/847)) ([0dbd6e8](https://github.com/cedricziel/signaldb/commit/0dbd6e8a0701cea0ce9e46c4fc9456d1562e7d31))
* native Query IR  versioned structured query surface (query-ir-core) ([#882](https://github.com/cedricziel/signaldb/issues/882)) ([8774ac0](https://github.com/cedricziel/signaldb/commit/8774ac0fbbe4686cb7aa8b0bba73dbc25f185689))
* one signaldb binary with the services as subcommands ([#1204](https://github.com/cedricziel/signaldb/issues/1204)) ([77f3278](https://github.com/cedricziel/signaldb/commit/77f3278ca445ac9b28bf955b0e482d4366a27c07))
* **otel-native-schema:** Layer 2 logical schema foundation ([#1104](https://github.com/cedricziel/signaldb/issues/1104)) ([af66060](https://github.com/cedricziel/signaldb/commit/af6606016430645693a0d524d3f15d9db4a52ead))
* **querier,router:** surface span events on the single-trace path ([#848](https://github.com/cedricziel/signaldb/issues/848)) ([5b344e9](https://github.com/cedricziel/signaldb/commit/5b344e98b6e787aeca35d68bf18ca5ca92657454))
* **query-ir:** add v2 heatmaps ([#1102](https://github.com/cedricziel/signaldb/issues/1102)) ([96184cf](https://github.com/cedricziel/signaldb/commit/96184cf42809a4cbf0e4a15f592cb544dbb7a597))
* **query-ir:** address whole attribute containers by OTel scope ([#1283](https://github.com/cedricziel/signaldb/issues/1283)) ([ddec28e](https://github.com/cedricziel/signaldb/commit/ddec28ee4b4ba191b329765392b9942690c65d14))
* **query-ir:** flamegraph result envelope for profiles ([#1144](https://github.com/cedricziel/signaldb/issues/1144)) ([394407f](https://github.com/cedricziel/signaldb/commit/394407f72756b15c97cb6ce6efcf01ce0b61b33b))
* **query-ir:** histogram_quantile stage over metrics_histogram ([#1141](https://github.com/cedricziel/signaldb/issues/1141)) ([591efe7](https://github.com/cedricziel/signaldb/commit/591efe752bbd5bc4b3e460c950fcba287cfab5b8))
* **query-ir:** span_events field returns a span's events list ([#1281](https://github.com/cedricziel/signaldb/issues/1281)) ([dafc3e9](https://github.com/cedricziel/signaldb/commit/dafc3e932b1aa9840a3eb9d9fbec9f4f2c730da0))
* record Flight query failures as span exceptions + surface reasons ([#846](https://github.com/cedricziel/signaldb/issues/846)) ([20d89f5](https://github.com/cedricziel/signaldb/commit/20d89f51eee05ff25ddfa523053dad7ebc8ea6e2))
* return server trace context and timings on HTTP responses (Server-Timing + traceresponse) ([#918](https://github.com/cedricziel/signaldb/issues/918)) ([453dd20](https://github.com/cedricziel/signaldb/commit/453dd2050eee95f3daf1c96f77e56964e99a2bb1))
* **router:** schema registry API under /api/v1/schema ([#1219](https://github.com/cedricziel/signaldb/issues/1219)) ([71af424](https://github.com/cedricziel/signaldb/commit/71af424a0d96eb3f87198af4c4213bb89106cf28))
* **schema-model:** vendored OTel semconv + Weaver-model parser/resolver/validator ([#1215](https://github.com/cedricziel/signaldb/issues/1215)) ([e278456](https://github.com/cedricziel/signaldb/commit/e278456ea6b6f93473176ba61bddbfa2e6b3ed43))
* **schema:** fold metrics and profiles into schemas.toml ([#1237](https://github.com/cedricziel/signaldb/issues/1237)) ([b340c92](https://github.com/cedricziel/signaldb/commit/b340c92004fce17586db476d96882d04d4c707b0))
* **schema:** size and extend trace_id/span_id bloom filters ([#1045](https://github.com/cedricziel/signaldb/issues/1045)) ([2e0e352](https://github.com/cedricziel/signaldb/commit/2e0e352db80701185fe8fb4f467f2931e25ee0c8))
* **self-monitoring:** heap self-profiling as OTLP profiles ([#840](https://github.com/cedricziel/signaldb/issues/840)) ([31fb7f1](https://github.com/cedricziel/signaldb/commit/31fb7f1f12fbfb8315f76efe62215c5c1b0cc575))
* **self-monitoring:** name HTTP server spans per OTel semantic conventions ([#844](https://github.com/cedricziel/signaldb/issues/844)) ([4815f7e](https://github.com/cedricziel/signaldb/commit/4815f7ecac36a56ef1869b6fd41ad0c015331bc1))
* **self-monitoring:** runtime-configurable browser telemetry export ([#842](https://github.com/cedricziel/signaldb/issues/842)) ([343b928](https://github.com/cedricziel/signaldb/commit/343b92877d1291406de25923e671ab2a54a98028))
* **self-monitoring:** version the SignalDB schema URL with the release ([#1275](https://github.com/cedricziel/signaldb/issues/1275)) ([5fa05df](https://github.com/cedricziel/signaldb/commit/5fa05dfb6b9eedeec37aaeb866f8af223a707795))
* semconv CLIENT spans on Flight call sites ([#905](https://github.com/cedricziel/signaldb/issues/905)) ([3047cbb](https://github.com/cedricziel/signaldb/commit/3047cbbc68f03e7d586d4a2caabaa2bd7c660ca1))
* semconv registry, weaver live-check harness, ops docs ([#908](https://github.com/cedricziel/signaldb/issues/908)) ([05f4c52](https://github.com/cedricziel/signaldb/commit/05f4c52fb89d82c3c0dd0321425cad6736652f34))
* semconv self-tracing foundations (resource, span factories, acceptor boundary) ([#903](https://github.com/cedricziel/signaldb/issues/903)) ([dbe4ca2](https://github.com/cedricziel/signaldb/commit/dbe4ca2389ac8db0dba721f66d79db4d0475ed76))
* signal rate-limit throttling with Retry-After and a generous default burst ([#1256](https://github.com/cedricziel/signaldb/issues/1256)) ([5584f3f](https://github.com/cedricziel/signaldb/commit/5584f3f1ef7461401a7f1bbbf24302308192b43d))
* source-agnostic tenant registry (admin-API tenants queryable without restart) ([#853](https://github.com/cedricziel/signaldb/issues/853)) ([c685935](https://github.com/cedricziel/signaldb/commit/c6859353a739fefcdc45f56cc0c7899193a6086a))
* span.kind facet + TraceQL support ([#1125](https://github.com/cedricziel/signaldb/issues/1125)) ([35735e5](https://github.com/cedricziel/signaldb/commit/35735e5d204b4fb9f89ddce1dd15296bf9ddfe3c))
* **tenant-table-listing:** list tenant tables from the Iceberg catalog ([#1267](https://github.com/cedricziel/signaldb/issues/1267)) ([5a444c2](https://github.com/cedricziel/signaldb/commit/5a444c261eeab5643d5d2d866385c07e2772ceee))
* **tracing:** add server.address and network.peer to RPC spans ([#1111](https://github.com/cedricziel/signaldb/issues/1111)) ([4e64934](https://github.com/cedricziel/signaldb/commit/4e64934814762c25226a3a7529bc9d695035d578))
* **ui:** add user menu and management pages ([#1105](https://github.com/cedricziel/signaldb/issues/1105)) ([c49a93f](https://github.com/cedricziel/signaldb/commit/c49a93ff5d112ce36335c19b12ac3404cdb4a8ba))
* **ui:** native Profiles tab + Catalog/Traces UX improvements ([#1164](https://github.com/cedricziel/signaldb/issues/1164)) ([a9d9223](https://github.com/cedricziel/signaldb/commit/a9d9223aa67a419b924513e961cc61d9ac6c97f5))
* **writer:** ack ingest on WAL flush, commit to Iceberg asynchronously ([#893](https://github.com/cedricziel/signaldb/issues/893)) ([fffdbb1](https://github.com/cedricziel/signaldb/commit/fffdbb109c48893bb2725a8afd3e2e740968a152))
* **writer:** bound Iceberg metadata growth via delete-after-commit ([#895](https://github.com/cedricziel/signaldb/issues/895)) ([35ce5c7](https://github.com/cedricziel/signaldb/commit/35ce5c7aa18aa4f12d3e62c4f34221c849f973f3))
* **writer:** coalesce Iceberg commits with a per-table floor + force-commit primitive ([#891](https://github.com/cedricziel/signaldb/issues/891)) ([ad47bb6](https://github.com/cedricziel/signaldb/commit/ad47bb6867dd5cf622701b5778ef9f94e7b60923))
* zero-config first boot  auto-provision default tenant and print API key once ([#995](https://github.com/cedricziel/signaldb/issues/995)) ([5116c8d](https://github.com/cedricziel/signaldb/commit/5116c8d9f22950447373f74c99b17488900db00d)), closes [#796](https://github.com/cedricziel/signaldb/issues/796)


### Bug Fixes

* **acceptor:** dead-letter poison WAL entries on first failure ([#1059](https://github.com/cedricziel/signaldb/issues/1059)) ([9d43c85](https://github.com/cedricziel/signaldb/commit/9d43c85445cb8c6d1bcb19279e29015680dc3fd4))
* **acceptor:** dead-letter writer-rejected WAL entries instead of wedging the retry pass ([#1063](https://github.com/cedricziel/signaldb/issues/1063)) ([7fc6ada](https://github.com/cedricziel/signaldb/commit/7fc6ada1ea922784220789f304fb3f8448ff8ef1)), closes [#1060](https://github.com/cedricziel/signaldb/issues/1060)
* **acceptor:** reject exports on OTLP conversion failure instead of ACKing empty batches ([#926](https://github.com/cedricziel/signaldb/issues/926)) ([#981](https://github.com/cedricziel/signaldb/issues/981)) ([02c0a3b](https://github.com/cedricziel/signaldb/commit/02c0a3b99fdc1327595ad8a0bf8434de1977615d))
* address CodeRabbit review on the tenant registry ([#853](https://github.com/cedricziel/signaldb/issues/853) follow-up) ([#855](https://github.com/cedricziel/signaldb/issues/855)) ([d5011ec](https://github.com/cedricziel/signaldb/commit/d5011ecc4a6101c8a51d5944a9480dff8b19d6a8))
* **catalog:** enable WAL journaling on SQLite catalogs to stop metrics-ingest stalls ([#858](https://github.com/cedricziel/signaldb/issues/858)) ([9865762](https://github.com/cedricziel/signaldb/commit/9865762f259d4f7841e2b8f48e46355f67de5c5d))
* **common,router:** include every known dataset in the tables grouping ([#1269](https://github.com/cedricziel/signaldb/issues/1269)) ([a895618](https://github.com/cedricziel/signaldb/commit/a8956181e5fd7f4cb91432d5f9622175708d2d70))
* **common:** make `cargo test -p common` compile on its own ([#1087](https://github.com/cedricziel/signaldb/issues/1087)) ([baac410](https://github.com/cedricziel/signaldb/commit/baac410ac8e46c0d4f97e9d75e42e09a390598a6))
* **common:** resolve a tenant's default dataset even without a dataset row ([#1082](https://github.com/cedricziel/signaldb/issues/1082)) ([055733f](https://github.com/cedricziel/signaldb/commit/055733f7e2d0e016091a987836fab2e788540e82))
* **compactor:** bound the rewrite's DataFusion fan-out ([#1067](https://github.com/cedricziel/signaldb/issues/1067)) ([9fc7dde](https://github.com/cedricziel/signaldb/commit/9fc7ddeea7497ce4e63fac2f60b11d77d66c621c)), closes [#1064](https://github.com/cedricziel/signaldb/issues/1064)
* **compactor:** cover profiles in retention, snapshot expiration, and orphan cleanup ([#1021](https://github.com/cedricziel/signaldb/issues/1021)) ([3bcc644](https://github.com/cedricziel/signaldb/commit/3bcc644438874392d75e4f048fa6380614a4e935)), closes [#1014](https://github.com/cedricziel/signaldb/issues/1014)
* **compactor:** decline partitions whose inputs exceed the job budget ([#1069](https://github.com/cedricziel/signaldb/issues/1069)) ([8373ff7](https://github.com/cedricziel/signaldb/commit/8373ff71195a3dedcd11e650a39410bff4fdfe1e))
* **compactor:** derive orphan live-file set from retained snapshots, not snapshot age ([#1007](https://github.com/cedricziel/signaldb/issues/1007)) ([8835c71](https://github.com/cedricziel/signaldb/commit/8835c71335333247d7215f839f7c62d510c3453a))
* **compactor:** finish the partition-scoped lifecycle rework ([#1091](https://github.com/cedricziel/signaldb/issues/1091)) ([1f38df8](https://github.com/cedricziel/signaldb/commit/1f38df867abdac033b2504a6acea031467d7fafe))
* **compactor:** re-validate unconditionally before deleting orphans ([#1020](https://github.com/cedricziel/signaldb/issues/1020)) ([5634ab8](https://github.com/cedricziel/signaldb/commit/5634ab820f68d3ed8e24dc4e45ae120dadd15b3b))
* **compactor:** select small files for compaction via max input size ([#934](https://github.com/cedricziel/signaldb/issues/934)) ([#975](https://github.com/cedricziel/signaldb/issues/975)) ([2ea86f8](https://github.com/cedricziel/signaldb/commit/2ea86f875d87be703d552844faaa9734ee0e7b2a))
* **compactor:** use a FairSpillPool for compaction and queries ([#1068](https://github.com/cedricziel/signaldb/issues/1068)) ([6b7bd13](https://github.com/cedricziel/signaldb/commit/6b7bd1368ac4444f785be14b8c29d92629295ee2))
* **conversion:** clamp span duration to zero when end &lt; start ([#927](https://github.com/cedricziel/signaldb/issues/927)) ([#978](https://github.com/cedricziel/signaldb/issues/978)) ([71ad488](https://github.com/cedricziel/signaldb/commit/71ad488aa43fa195592d9a8c9e89f2827dfe92ca))
* **flight:** server.address double-port bug + ops do_action tracing gap ([#1116](https://github.com/cedricziel/signaldb/issues/1116)) ([73e778f](https://github.com/cedricziel/signaldb/commit/73e778f0d936931c86545bfc8722ac7a7403e0e9))
* **flight:** set explicit gRPC message-size limits and chunk oversized batches ([#990](https://github.com/cedricziel/signaldb/issues/990)) ([6499175](https://github.com/cedricziel/signaldb/commit/6499175d0e6402e1350ad28803d0b08954e43fe1))
* **flight:** stop the client timeout from masking the querier's query deadline ([#919](https://github.com/cedricziel/signaldb/issues/919)) ([46eee38](https://github.com/cedricziel/signaldb/commit/46eee382468bfd6a5f3c34f8404379e55d68a690))
* **iceberg:** backfill metadata pruning properties on pre-existing tables ([#973](https://github.com/cedricziel/signaldb/issues/973)) ([f40fce2](https://github.com/cedricziel/signaldb/commit/f40fce2db23f5e8af79b5fac03e70dd3f2a4ad7b)), closes [#959](https://github.com/cedricziel/signaldb/issues/959)
* **iceberg:** pass S3 storage config explicitly instead of mutating process env ([#948](https://github.com/cedricziel/signaldb/issues/948)) ([#988](https://github.com/cedricziel/signaldb/issues/988)) ([06af739](https://github.com/cedricziel/signaldb/commit/06af73969d302c36be46b90f521ef18688cbecf3))
* **logql:** carry log and resource attributes as structured metadata ([#1094](https://github.com/cedricziel/signaldb/issues/1094)) ([26b9d15](https://github.com/cedricziel/signaldb/commit/26b9d15457ac84c96ba2affe28d3ea520b40c664))
* **mcp:** add connect and request timeouts to router HTTP client ([#885](https://github.com/cedricziel/signaldb/issues/885)) ([#976](https://github.com/cedricziel/signaldb/issues/976)) ([f0f2182](https://github.com/cedricziel/signaldb/commit/f0f21824b654d57668e2c235f310d3a048a314f4))
* metrics without service.name land as 'unknown'; boot log flood demoted to debug ([#1227](https://github.com/cedricziel/signaldb/issues/1227)) ([7b5ea34](https://github.com/cedricziel/signaldb/commit/7b5ea343096ea8a7c0f62575029ac1e838ec514c))
* **metrics:** carry NaN/±Inf values through the wire format instead of dead-lettering ([#1239](https://github.com/cedricziel/signaldb/issues/1239)) ([9e38b3a](https://github.com/cedricziel/signaldb/commit/9e38b3a993b6d632d7c67f498f2f489ea97e6636)), closes [#1061](https://github.com/cedricziel/signaldb/issues/1061)
* **model:** stop flattening trace hierarchies to root + direct children ([#1018](https://github.com/cedricziel/signaldb/issues/1018)) ([5fee337](https://github.com/cedricziel/signaldb/commit/5fee33711628bf3f041c436c34f363f114ed93fb))
* **otlp:** preserve bytes and profiles interned values ([#1099](https://github.com/cedricziel/signaldb/issues/1099)) ([5e132ba](https://github.com/cedricziel/signaldb/commit/5e132bac9cc2335962be9418360a3c124ed0c409))
* provision signal tables for every registered dataset, and read an absent one as empty ([#1074](https://github.com/cedricziel/signaldb/issues/1074)) ([9a50ffa](https://github.com/cedricziel/signaldb/commit/9a50ffaa7e404a96cb80d7d3b0cc0850ede00f49))
* **query-ir:** correct histogram_quantile rate-mode series grouping ([#1142](https://github.com/cedricziel/signaldb/issues/1142)) ([6f66d04](https://github.com/cedricziel/signaldb/commit/6f66d0435f552a99ca5776fccbe2f7b52cf02fb6))
* restore compactor discovery and WAL pending-gauge accuracy ([#1049](https://github.com/cedricziel/signaldb/issues/1049)) ([b9254b0](https://github.com/cedricziel/signaldb/commit/b9254b065430b092978c2ba8f2e59ec1d3c1ceb8))
* **router:** materialize a tenant's default dataset as a real row ([#1085](https://github.com/cedricziel/signaldb/issues/1085)) ([9443244](https://github.com/cedricziel/signaldb/commit/94432445328a0489bfd0476aaaba12ba937a2561))
* **router:** write the tenant and its default dataset in one transaction ([#1086](https://github.com/cedricziel/signaldb/issues/1086)) ([59bdc70](https://github.com/cedricziel/signaldb/commit/59bdc705d8fddc8253d55466904f59f8f0493060))
* **self-monitoring:** default to parent-based trace sampler ([#843](https://github.com/cedricziel/signaldb/issues/843)) ([d6c12b1](https://github.com/cedricziel/signaldb/commit/d6c12b1aeb060a8438f857451bda61ee0d8828b9))
* **self-monitoring:** drop signaldb.* prefix from process/system gauges ([#1212](https://github.com/cedricziel/signaldb/issues/1212)) ([aee6733](https://github.com/cedricziel/signaldb/commit/aee6733bb4006762c740744445be7508d993a427))
* **self-monitoring:** stop emitting non-semconv bridge attributes on spans ([#967](https://github.com/cedricziel/signaldb/issues/967)) ([0b82ef4](https://github.com/cedricziel/signaldb/commit/0b82ef4256936e30462e946d62d9452ab1155e5c))
* **telemetry:** emit int-typed registry attributes as i64 ([#1013](https://github.com/cedricziel/signaldb/issues/1013)) ([be67718](https://github.com/cedricziel/signaldb/commit/be677184819e5cbe700d253a03e59cd2bffa7ba8))
* **traces:** span_kind/status_code numeric source of truth + schema evolution engine ([#1235](https://github.com/cedricziel/signaldb/issues/1235)) ([0f8603b](https://github.com/cedricziel/signaldb/commit/0f8603bdb1f39254c83af0c631653a65c8a85e3f))
* **ui:** catalog multi-source discovery, trace routing fix, span_kind fix ([#1210](https://github.com/cedricziel/signaldb/issues/1210)) ([84446f2](https://github.com/cedricziel/signaldb/commit/84446f2ef450be67fb14dbb6c4b4feb477ea0d04))
* **ui:** route Metrics builder default queries through Query IR ([#1138](https://github.com/cedricziel/signaldb/issues/1138)) ([4056261](https://github.com/cedricziel/signaldb/commit/4056261e0d406d5ae73dc2fe20bc136b8e866bb8))
* **wal:** bounds-check entry range before reading segment data ([#871](https://github.com/cedricziel/signaldb/issues/871)) ([bc36a94](https://github.com/cedricziel/signaldb/commit/bc36a9493c04ba0f285c05119a54f64ef7e82da5))
* **wal:** carry tenant/dataset/signal on WAL failure telemetry ([#866](https://github.com/cedricziel/signaldb/issues/866)) ([a023dbb](https://github.com/cedricziel/signaldb/commit/a023dbb54822964d44f7c22864391eb2af957a58))
* **wal:** offset-authoritative writes + data-size rotation ([#865](https://github.com/cedricziel/signaldb/issues/865)) ([#883](https://github.com/cedricziel/signaldb/issues/883)) ([31be2cf](https://github.com/cedricziel/signaldb/commit/31be2cfe46f67c56a479fb4b65b1dc5f4412414d))
* **wal:** skip and quarantine corrupt entries on replay instead of aborting ([#1093](https://github.com/cedricziel/signaldb/issues/1093)) ([a14db18](https://github.com/cedricziel/signaldb/commit/a14db18d0b0aee7dcb9113f5a49d0e27a8d47eef)), closes [#1033](https://github.com/cedricziel/signaldb/issues/1033)
* **writer:** derive flush scope from request metadata, not the action body ([#897](https://github.com/cedricziel/signaldb/issues/897)) ([cd94186](https://github.com/cedricziel/signaldb/commit/cd9418653c1f90812ffee4a0688dd947039dbbeb))


### Performance Improvements

* **flight,wal:** compress Flight IPC payloads and WAL entries ([#945](https://github.com/cedricziel/signaldb/issues/945)) ([#998](https://github.com/cedricziel/signaldb/issues/998)) ([efb5ef4](https://github.com/cedricziel/signaldb/commit/efb5ef4bc85e2e77483f4546255b50c564015827))
* **flight:** skip redundant discovery lookup and memoize capability discovery ([#940](https://github.com/cedricziel/signaldb/issues/940)) ([#989](https://github.com/cedricziel/signaldb/issues/989)) ([a4720ca](https://github.com/cedricziel/signaldb/commit/a4720ca5d9dc5f541c2dc814cb533934cb023c14))
* **iceberg:** stop carrying useless column bounds in every manifest entry ([#1023](https://github.com/cedricziel/signaldb/issues/1023)) ([3a77a4e](https://github.com/cedricziel/signaldb/commit/3a77a4e513808ae9299e8bf93579e2dbb26b9977))
* **querier:** enable statistics-based file grouping and Parquet filter pushdown ([#937](https://github.com/cedricziel/signaldb/issues/937)) ([#987](https://github.com/cedricziel/signaldb/issues/987)) ([7d4aefb](https://github.com/cedricziel/signaldb/commit/7d4aefb855061ea2a07c6536eee28385a49a6722))
* **wal:** batch index persistence in mark_processed_many ([#943](https://github.com/cedricziel/signaldb/issues/943)) ([#984](https://github.com/cedricziel/signaldb/issues/984)) ([41a91cd](https://github.com/cedricziel/signaldb/commit/41a91cd4938286a39c120e642f0b11261b813ab7))
* **wal:** index entries by id to kill O(n) SipHash scans in the WAL loop ([#1148](https://github.com/cedricziel/signaldb/issues/1148)) ([5d91b5d](https://github.com/cedricziel/signaldb/commit/5d91b5d21bc9d051400db5d70fa3d14edadf71b5)), closes [#1112](https://github.com/cedricziel/signaldb/issues/1112)


### Documentation

* flight-communication.md read path now describes the CLIENT hop. ([3047cbb](https://github.com/cedricziel/signaldb/commit/3047cbbc68f03e7d586d4a2caabaa2bd7c660ca1))


### Code Refactoring

* **common:** simplify pass ([#1169](https://github.com/cedricziel/signaldb/issues/1169)) ([45e455f](https://github.com/cedricziel/signaldb/commit/45e455fdfb27c65fa4f6da5f2d58c559b7feb71d))
* **compactor:** partition-scoped compaction with delta commits ([#1017](https://github.com/cedricziel/signaldb/issues/1017)) ([52dc957](https://github.com/cedricziel/signaldb/commit/52dc9572a10378d6d69f653d1a78a4cf4d2f1407))
* **flight:** decode Flight data dictionary-aware ([#1004](https://github.com/cedricziel/signaldb/issues/1004)) ([94a7a30](https://github.com/cedricziel/signaldb/commit/94a7a30edd81060f2bfc5147dbf3b53307d2de72))
* **iceberg:** configure the catalog pool instead of working around it ([#1024](https://github.com/cedricziel/signaldb/issues/1024)) ([68be19f](https://github.com/cedricziel/signaldb/commit/68be19f7327fc2660a6b45df26c29084fee6ce42))
* **logging:** forbid log:: macros in favor of tracing:: ([#1006](https://github.com/cedricziel/signaldb/issues/1006)) ([071ebb4](https://github.com/cedricziel/signaldb/commit/071ebb47d02f2d6e43ccfb60380c00e3be929248))
* simplify backend workspace (dedup, dead code, redundant clones) ([#1168](https://github.com/cedricziel/signaldb/issues/1168)) ([409b778](https://github.com/cedricziel/signaldb/commit/409b778686a1cea5c54edfba7778c3e9ed3aa29c))
* span hygiene sweep and construction guard ([#907](https://github.com/cedricziel/signaldb/issues/907)) ([c1f7b81](https://github.com/cedricziel/signaldb/commit/c1f7b81fbc00ae5fd6c9b948f9fb35c9d5a27d26))


### Tests

* add performance benchmark suite (write + read critical paths) ([#879](https://github.com/cedricziel/signaldb/issues/879)) ([149c5e1](https://github.com/cedricziel/signaldb/commit/149c5e19952c88e442d594b281ee9befbe4929d1))
* back provisioning tests with a file catalog, not a named memory one ([#1088](https://github.com/cedricziel/signaldb/issues/1088)) ([718b73d](https://github.com/cedricziel/signaldb/commit/718b73df827980e7f40856eb19addacfe4b1b4b8)), closes [#1083](https://github.com/cedricziel/signaldb/issues/1083)
* delete tautological tests and rewrite salvageable ones as contract tests ([#961](https://github.com/cedricziel/signaldb/issues/961)) ([b3e884a](https://github.com/cedricziel/signaldb/commit/b3e884ad59b4df853429133d5eef2724a8adcada))
* make tests assert what their names promise ([#966](https://github.com/cedricziel/signaldb/issues/966)) ([446ed06](https://github.com/cedricziel/signaldb/commit/446ed062a7480902ef391884b1c2e12f77ddd66f))
* pin the in-memory catalog so provisioning tests can't race ([#1083](https://github.com/cedricziel/signaldb/issues/1083)) ([218ff2a](https://github.com/cedricziel/signaldb/commit/218ff2a8db3d4bf37d9cdc53e163dee03f382fda))
* polish medium/low audit findings across the workspace ([#969](https://github.com/cedricziel/signaldb/issues/969)) ([8962f6d](https://github.com/cedricziel/signaldb/commit/8962f6d1d22c8a176d4a1d99376d61b42b1da258))
* replace sleep-based synchronization with deterministic waits ([#968](https://github.com/cedricziel/signaldb/issues/968)) ([6391326](https://github.com/cedricziel/signaldb/commit/6391326013c8620f186e4a63c2cdf3bbdf9ee963))
* **self-monitoring:** pin span-event bridge path against code.module.name ([#956](https://github.com/cedricziel/signaldb/issues/956)) ([#986](https://github.com/cedricziel/signaldb/issues/986)) ([c88e403](https://github.com/cedricziel/signaldb/commit/c88e40385868979350a29fb33a6dd2bcbf9839b5))
* **wal:** concurrency round-trip guard for entry byte-integrity ([#868](https://github.com/cedricziel/signaldb/issues/868)) ([0eab821](https://github.com/cedricziel/signaldb/commit/0eab8218443b458239a8a0fa456f68e5a67ea7dd))


### Continuous Integration

* declare the testing feature on the test that needs it ([#1090](https://github.com/cedricziel/signaldb/issues/1090)) ([96aed27](https://github.com/cedricziel/signaldb/commit/96aed27918c1c950080980416b3b30a508b010af)), closes [#1089](https://github.com/cedricziel/signaldb/issues/1089)
</details>

<details><summary>compactor: 0.3.0</summary>

## [0.3.0](https://github.com/cedricziel/signaldb/compare/compactor-v0.2.1...compactor-v0.3.0) (2026-08-17)


###   BREAKING CHANGES

* **compactor:** [compactor.orphan_cleanup] revalidate_before_delete no longer exists. Note that a leftover key is silently ignored rather than rejected -- the design assumed unknown keys fail config parsing, but neither config struct sets serde(deny_unknown_fields), and adding it is not a safe drive-by because figment's env provider populates the same structs. Documented in the compactor configuration reference; tightening the structs deserves its own change.
* **compactor:** [compactor] min_input_file_size_kb is replaced by max_input_file_size_kb (semantics inverted) and max_files_per_job is removed. No backward-compat alias is provided.

### Features

* **compactor:** reclaim metadata backlog and enable orphan cleanup by default ([#1008](https://github.com/cedricziel/signaldb/issues/1008)) ([908ea79](https://github.com/cedricziel/signaldb/commit/908ea798e78a6d2dd90396f56e584275e9dfc9b3))
* **compactor:** warn on incoherent memory settings and document sizing ([#1081](https://github.com/cedricziel/signaldb/issues/1081)) ([b0a4bb0](https://github.com/cedricziel/signaldb/commit/b0a4bb0740430fad36129b2c40a5c0dc9c2f111d)), closes [#1064](https://github.com/cedricziel/signaldb/issues/1064)
* DB client spans, query stage spans, compactor job spans ([#906](https://github.com/cedricziel/signaldb/issues/906)) ([04a4c4e](https://github.com/cedricziel/signaldb/commit/04a4c4e5788cf6531e0421b50b523b04ac4db38b))
* **iceberg:** tune the Parquet writer properties now that they are honored ([#1025](https://github.com/cedricziel/signaldb/issues/1025)) ([219132a](https://github.com/cedricziel/signaldb/commit/219132a3eb1bba1c15975245081ad4a2d54eb7d1))
* one signaldb binary with the services as subcommands ([#1204](https://github.com/cedricziel/signaldb/issues/1204)) ([77f3278](https://github.com/cedricziel/signaldb/commit/77f3278ca445ac9b28bf955b0e482d4366a27c07))
* semconv RPC server spans on Flight boundaries ([#904](https://github.com/cedricziel/signaldb/issues/904)) ([a791f45](https://github.com/cedricziel/signaldb/commit/a791f45edf5b1650cc9091d1acf481175060628a))
* source-agnostic tenant registry (admin-API tenants queryable without restart) ([#853](https://github.com/cedricziel/signaldb/issues/853)) ([c685935](https://github.com/cedricziel/signaldb/commit/c6859353a739fefcdc45f56cc0c7899193a6086a))
* **tracing:** add server.address and network.peer to RPC spans ([#1111](https://github.com/cedricziel/signaldb/issues/1111)) ([4e64934](https://github.com/cedricziel/signaldb/commit/4e64934814762c25226a3a7529bc9d695035d578))


### Bug Fixes

* address CodeRabbit review on the tenant registry ([#853](https://github.com/cedricziel/signaldb/issues/853) follow-up) ([#855](https://github.com/cedricziel/signaldb/issues/855)) ([d5011ec](https://github.com/cedricziel/signaldb/commit/d5011ecc4a6101c8a51d5944a9480dff8b19d6a8))
* **compactor:** bound the rewrite's DataFusion fan-out ([#1067](https://github.com/cedricziel/signaldb/issues/1067)) ([9fc7dde](https://github.com/cedricziel/signaldb/commit/9fc7ddeea7497ce4e63fac2f60b11d77d66c621c)), closes [#1064](https://github.com/cedricziel/signaldb/issues/1064)
* **compactor:** classify catalog CAS failures as retryable conflicts ([#1155](https://github.com/cedricziel/signaldb/issues/1155)) ([0c9e7c4](https://github.com/cedricziel/signaldb/commit/0c9e7c4788a5189f827e5072ce3b5d01d0d520de))
* **compactor:** cool down recently-failed partitions to stop wasted reselection ([#1156](https://github.com/cedricziel/signaldb/issues/1156)) ([baa8e5d](https://github.com/cedricziel/signaldb/commit/baa8e5d05a0abcc762deb91cf97c6562f5f666e8))
* **compactor:** cover profiles in retention, snapshot expiration, and orphan cleanup ([#1021](https://github.com/cedricziel/signaldb/issues/1021)) ([3bcc644](https://github.com/cedricziel/signaldb/commit/3bcc644438874392d75e4f048fa6380614a4e935)), closes [#1014](https://github.com/cedricziel/signaldb/issues/1014)
* **compactor:** decline partitions whose inputs exceed the job budget ([#1069](https://github.com/cedricziel/signaldb/issues/1069)) ([8373ff7](https://github.com/cedricziel/signaldb/commit/8373ff71195a3dedcd11e650a39410bff4fdfe1e))
* **compactor:** derive orphan live-file set from retained snapshots, not snapshot age ([#1007](https://github.com/cedricziel/signaldb/issues/1007)) ([8835c71](https://github.com/cedricziel/signaldb/commit/8835c71335333247d7215f839f7c62d510c3453a))
* **compactor:** finish the partition-scoped lifecycle rework ([#1091](https://github.com/cedricziel/signaldb/issues/1091)) ([1f38df8](https://github.com/cedricziel/signaldb/commit/1f38df867abdac033b2504a6acea031467d7fafe))
* **compactor:** log commit failures with their full cause chain ([#1050](https://github.com/cedricziel/signaldb/issues/1050)) ([61704a0](https://github.com/cedricziel/signaldb/commit/61704a0f327eb20878c6a40c78a7aefee5462443))
* **compactor:** re-validate unconditionally before deleting orphans ([#1020](https://github.com/cedricziel/signaldb/issues/1020)) ([5634ab8](https://github.com/cedricziel/signaldb/commit/5634ab820f68d3ed8e24dc4e45ae120dadd15b3b))
* **compactor:** read partition values from manifest entries, not file paths ([#930](https://github.com/cedricziel/signaldb/issues/930)) ([#991](https://github.com/cedricziel/signaldb/issues/991)) ([2f7e79b](https://github.com/cedricziel/signaldb/commit/2f7e79b86bd5a1884604d9441692b92ac17e665f))
* **compactor:** retry transient failures, fail fast on terminal ones ([#1157](https://github.com/cedricziel/signaldb/issues/1157)) ([3aa5be7](https://github.com/cedricziel/signaldb/commit/3aa5be75fec3aec9a1ef1f1d0d1e67e9e6cb4a2e))
* **compactor:** select small files for compaction via max input size ([#934](https://github.com/cedricziel/signaldb/issues/934)) ([#975](https://github.com/cedricziel/signaldb/issues/975)) ([2ea86f8](https://github.com/cedricziel/signaldb/commit/2ea86f875d87be703d552844faaa9734ee0e7b2a))
* **compactor:** sort profiles, unify signal classification, dead-code cleanup ([#1177](https://github.com/cedricziel/signaldb/issues/1177)) ([ead68fc](https://github.com/cedricziel/signaldb/commit/ead68fc51b87d7ee5d1b26647907213f7f7755ef))
* **compactor:** use a FairSpillPool for compaction and queries ([#1068](https://github.com/cedricziel/signaldb/issues/1068)) ([6b7bd13](https://github.com/cedricziel/signaldb/commit/6b7bd1368ac4444f785be14b8c29d92629295ee2))
* metrics without service.name land as 'unknown'; boot log flood demoted to debug ([#1227](https://github.com/cedricziel/signaldb/issues/1227)) ([7b5ea34](https://github.com/cedricziel/signaldb/commit/7b5ea343096ea8a7c0f62575029ac1e838ec514c))
* **monolith:** run the full compactor lifecycle loop, not just planning ([#1005](https://github.com/cedricziel/signaldb/issues/1005)) ([2e751fb](https://github.com/cedricziel/signaldb/commit/2e751fb5849ce596f3dca7366624ee65e4def3ac))
* provision signal tables for every registered dataset, and read an absent one as empty ([#1074](https://github.com/cedricziel/signaldb/issues/1074)) ([9a50ffa](https://github.com/cedricziel/signaldb/commit/9a50ffaa7e404a96cb80d7d3b0cc0850ede00f49))
* **telemetry:** emit int-typed registry attributes as i64 ([#1013](https://github.com/cedricziel/signaldb/issues/1013)) ([be67718](https://github.com/cedricziel/signaldb/commit/be677184819e5cbe700d253a03e59cd2bffa7ba8))
* **telemetry:** register retention span-event attributes and whitelist unremovable bridge attrs for weaver live-check ([#1009](https://github.com/cedricziel/signaldb/issues/1009)) ([da74098](https://github.com/cedricziel/signaldb/commit/da74098adf02b64500a032b860c0c5aad8af93ad))


### Performance Improvements

* **compactor:** stream the rewrite instead of collecting the partition ([#1080](https://github.com/cedricziel/signaldb/issues/1080)) ([da7fa82](https://github.com/cedricziel/signaldb/commit/da7fa82c0edc3832f2272b4f5fc3872c7b7d8476))
* CPU target features and jemalloc allocator for release builds ([#970](https://github.com/cedricziel/signaldb/issues/970)) ([766e2d1](https://github.com/cedricziel/signaldb/commit/766e2d1c82dad65a674184edaf2e8d67cb4083dd))
* **flight,wal:** compress Flight IPC payloads and WAL entries ([#945](https://github.com/cedricziel/signaldb/issues/945)) ([#998](https://github.com/cedricziel/signaldb/issues/998)) ([efb5ef4](https://github.com/cedricziel/signaldb/commit/efb5ef4bc85e2e77483f4546255b50c564015827))


### Documentation

* **compactor:** reframe phase-3 docs as retention & lifecycle ([#854](https://github.com/cedricziel/signaldb/issues/854)) ([6961887](https://github.com/cedricziel/signaldb/commit/6961887e5dce725744e4cdfb347ec7dbda7b252a))


### Code Refactoring

* **compactor:** detect self-authored commit conflicts via typed errors ([#951](https://github.com/cedricziel/signaldb/issues/951)) ([#996](https://github.com/cedricziel/signaldb/issues/996)) ([28bccd1](https://github.com/cedricziel/signaldb/commit/28bccd18d3fb1342389627e3f2608f5eb45533e1))
* **compactor:** partition-scoped compaction with delta commits ([#1017](https://github.com/cedricziel/signaldb/issues/1017)) ([52dc957](https://github.com/cedricziel/signaldb/commit/52dc9572a10378d6d69f653d1a78a4cf4d2f1407))
* **compactor:** run lifecycle cycles as independent tasks ([#1026](https://github.com/cedricziel/signaldb/issues/1026)) ([0b0f02a](https://github.com/cedricziel/signaldb/commit/0b0f02a6875b5dba5e853821a5e45319b92b8455))


### Tests

* delete tautological tests and rewrite salvageable ones as contract tests ([#961](https://github.com/cedricziel/signaldb/issues/961)) ([b3e884a](https://github.com/cedricziel/signaldb/commit/b3e884ad59b4df853429133d5eef2724a8adcada))
* exercise real implementations instead of test-local copies ([#964](https://github.com/cedricziel/signaldb/issues/964)) ([e142b3d](https://github.com/cedricziel/signaldb/commit/e142b3d006065205c7194fd22c4ca4e182402f55))
* polish medium/low audit findings across the workspace ([#969](https://github.com/cedricziel/signaldb/issues/969)) ([8962f6d](https://github.com/cedricziel/signaldb/commit/8962f6d1d22c8a176d4a1d99376d61b42b1da258))
* replace sleep-based synchronization with deterministic waits ([#968](https://github.com/cedricziel/signaldb/issues/968)) ([6391326](https://github.com/cedricziel/signaldb/commit/6391326013c8620f186e4a63c2cdf3bbdf9ee963))
</details>

<details><summary>querier: 0.3.0</summary>

## [0.3.0](https://github.com/cedricziel/signaldb/compare/querier-v0.2.1...querier-v0.3.0) (2026-08-17)


### Features

* add an Errors & Exceptions tab ([#1167](https://github.com/cedricziel/signaldb/issues/1167)) ([79f3749](https://github.com/cedricziel/signaldb/commit/79f374916a8add7aa47abd0c8569e13c560a2d7c))
* compute the traces group table on the server, via a scoped IR aggregate ([#1092](https://github.com/cedricziel/signaldb/issues/1092)) ([ec5c284](https://github.com/cedricziel/signaldb/commit/ec5c284cbe57c0ce34da7f295f08502de2493b82))
* DB client spans, query stage spans, compactor job spans ([#906](https://github.com/cedricziel/signaldb/issues/906)) ([04a4c4e](https://github.com/cedricziel/signaldb/commit/04a4c4e5788cf6531e0421b50b523b04ac4db38b))
* **model:** add span events to the Span model ([#847](https://github.com/cedricziel/signaldb/issues/847)) ([0dbd6e8](https://github.com/cedricziel/signaldb/commit/0dbd6e8a0701cea0ce9e46c4fc9456d1562e7d31))
* native Query IR  versioned structured query surface (query-ir-core) ([#882](https://github.com/cedricziel/signaldb/issues/882)) ([8774ac0](https://github.com/cedricziel/signaldb/commit/8774ac0fbbe4686cb7aa8b0bba73dbc25f185689))
* one signaldb binary with the services as subcommands ([#1204](https://github.com/cedricziel/signaldb/issues/1204)) ([77f3278](https://github.com/cedricziel/signaldb/commit/77f3278ca445ac9b28bf955b0e482d4366a27c07))
* **otel-native-schema:** Layer 2 logical schema foundation ([#1104](https://github.com/cedricziel/signaldb/issues/1104)) ([af66060](https://github.com/cedricziel/signaldb/commit/af6606016430645693a0d524d3f15d9db4a52ead))
* **querier,router:** surface span events on the single-trace path ([#848](https://github.com/cedricziel/signaldb/issues/848)) ([5b344e9](https://github.com/cedricziel/signaldb/commit/5b344e98b6e787aeca35d68bf18ca5ca92657454))
* **querier:** record every do_get failure as a span exception ([#878](https://github.com/cedricziel/signaldb/issues/878)) ([39d76bd](https://github.com/cedricziel/signaldb/commit/39d76bd13a9e92b08b8b55c8dabf62f58863fab7))
* **query-ir:** add v2 heatmaps ([#1102](https://github.com/cedricziel/signaldb/issues/1102)) ([96184cf](https://github.com/cedricziel/signaldb/commit/96184cf42809a4cbf0e4a15f592cb544dbb7a597))
* **query-ir:** address whole attribute containers by OTel scope ([#1283](https://github.com/cedricziel/signaldb/issues/1283)) ([ddec28e](https://github.com/cedricziel/signaldb/commit/ddec28ee4b4ba191b329765392b9942690c65d14))
* **query-ir:** flamegraph result envelope for profiles ([#1144](https://github.com/cedricziel/signaldb/issues/1144)) ([394407f](https://github.com/cedricziel/signaldb/commit/394407f72756b15c97cb6ce6efcf01ce0b61b33b))
* **query-ir:** histogram_quantile stage over metrics_histogram ([#1141](https://github.com/cedricziel/signaldb/issues/1141)) ([591efe7](https://github.com/cedricziel/signaldb/commit/591efe752bbd5bc4b3e460c950fcba287cfab5b8))
* **query-ir:** make the logs source an OTel-native LogRecord ([#1096](https://github.com/cedricziel/signaldb/issues/1096)) ([8a82472](https://github.com/cedricziel/signaldb/commit/8a824725a87e741eec0016a322d68c37d95d6c77))
* **query-ir:** span_events field returns a span's events list ([#1281](https://github.com/cedricziel/signaldb/issues/1281)) ([dafc3e9](https://github.com/cedricziel/signaldb/commit/dafc3e932b1aa9840a3eb9d9fbec9f4f2c730da0))
* record Flight query failures as span exceptions + surface reasons ([#846](https://github.com/cedricziel/signaldb/issues/846)) ([20d89f5](https://github.com/cedricziel/signaldb/commit/20d89f51eee05ff25ddfa523053dad7ebc8ea6e2))
* semconv CLIENT spans on Flight call sites ([#905](https://github.com/cedricziel/signaldb/issues/905)) ([3047cbb](https://github.com/cedricziel/signaldb/commit/3047cbbc68f03e7d586d4a2caabaa2bd7c660ca1))
* semconv RPC server spans on Flight boundaries ([#904](https://github.com/cedricziel/signaldb/issues/904)) ([a791f45](https://github.com/cedricziel/signaldb/commit/a791f45edf5b1650cc9091d1acf481175060628a))
* source-agnostic tenant registry (admin-API tenants queryable without restart) ([#853](https://github.com/cedricziel/signaldb/issues/853)) ([c685935](https://github.com/cedricziel/signaldb/commit/c6859353a739fefcdc45f56cc0c7899193a6086a))
* span.kind facet + TraceQL support ([#1125](https://github.com/cedricziel/signaldb/issues/1125)) ([35735e5](https://github.com/cedricziel/signaldb/commit/35735e5d204b4fb9f89ddce1dd15296bf9ddfe3c))
* **tempo:** back trace tag discovery with real querier data ([#1258](https://github.com/cedricziel/signaldb/issues/1258)) ([4aeda0d](https://github.com/cedricziel/signaldb/commit/4aeda0d3314fbe7b5546f0411657fdc646e301dd))
* **tracing:** add server.address and network.peer to RPC spans ([#1111](https://github.com/cedricziel/signaldb/issues/1111)) ([4e64934](https://github.com/cedricziel/signaldb/commit/4e64934814762c25226a3a7529bc9d695035d578))


### Bug Fixes

* address CodeRabbit review on the tenant registry ([#853](https://github.com/cedricziel/signaldb/issues/853) follow-up) ([#855](https://github.com/cedricziel/signaldb/issues/855)) ([d5011ec](https://github.com/cedricziel/signaldb/commit/d5011ecc4a6101c8a51d5944a9480dff8b19d6a8))
* **build:** stop jemalloc heap profiling from crashing musl images ([#1126](https://github.com/cedricziel/signaldb/issues/1126)) ([98b2996](https://github.com/cedricziel/signaldb/commit/98b299660ef31b56d73e079a2477166b415e736e))
* **compactor:** use a FairSpillPool for compaction and queries ([#1068](https://github.com/cedricziel/signaldb/issues/1068)) ([6b7bd13](https://github.com/cedricziel/signaldb/commit/6b7bd1368ac4444f785be14b8c29d92629295ee2))
* **logql:** carry log and resource attributes as structured metadata ([#1094](https://github.com/cedricziel/signaldb/issues/1094)) ([26b9d15](https://github.com/cedricziel/signaldb/commit/26b9d15457ac84c96ba2affe28d3ea520b40c664))
* **metrics:** carry NaN/±Inf values through the wire format instead of dead-lettering ([#1239](https://github.com/cedricziel/signaldb/issues/1239)) ([9e38b3a](https://github.com/cedricziel/signaldb/commit/9e38b3a993b6d632d7c67f498f2f489ea97e6636)), closes [#1061](https://github.com/cedricziel/signaldb/issues/1061)
* **model:** stop flattening trace hierarchies to root + direct children ([#1018](https://github.com/cedricziel/signaldb/issues/1018)) ([5fee337](https://github.com/cedricziel/signaldb/commit/5fee33711628bf3f041c436c34f363f114ed93fb))
* provision signal tables for every registered dataset, and read an absent one as empty ([#1074](https://github.com/cedricziel/signaldb/issues/1074)) ([9a50ffa](https://github.com/cedricziel/signaldb/commit/9a50ffaa7e404a96cb80d7d3b0cc0850ede00f49))
* **querier:** bound, order, and project the trace search scan ([#928](https://github.com/cedricziel/signaldb/issues/928)) ([#985](https://github.com/cedricziel/signaldb/issues/985)) ([b3c94d8](https://github.com/cedricziel/signaldb/commit/b3c94d8a62c06f7f9bca455c7e73e9a24b38f9e6))
* **querier:** coerce mismatched column types before unioning the metrics tables ([#1240](https://github.com/cedricziel/signaldb/issues/1240)) ([b0d7eb1](https://github.com/cedricziel/signaldb/commit/b0d7eb10734b724416b8679180c4c4129d5ab5df)), closes [#1206](https://github.com/cedricziel/signaldb/issues/1206)
* **querier:** reject out-of-range time bounds instead of saturating to a sentinel ([#920](https://github.com/cedricziel/signaldb/issues/920)) ([dc6990e](https://github.com/cedricziel/signaldb/commit/dc6990eb72d99cb23185faf2a373b2a22e403a93))
* **query-ir:** correct histogram_quantile rate-mode series grouping ([#1142](https://github.com/cedricziel/signaldb/issues/1142)) ([6f66d04](https://github.com/cedricziel/signaldb/commit/6f66d0435f552a99ca5776fccbe2f7b52cf02fb6))
* **query-ir:** reapply flamegraph Option fix dropped by a stale merge ([#1146](https://github.com/cedricziel/signaldb/issues/1146)) ([811bb11](https://github.com/cedricziel/signaldb/commit/811bb111b8274e85a181203182a6dd462c3c9438))
* **ui:** route Metrics builder default queries through Query IR ([#1138](https://github.com/cedricziel/signaldb/issues/1138)) ([4056261](https://github.com/cedricziel/signaldb/commit/4056261e0d406d5ae73dc2fe20bc136b8e866bb8))


### Performance Improvements

* CPU target features and jemalloc allocator for release builds ([#970](https://github.com/cedricziel/signaldb/issues/970)) ([766e2d1](https://github.com/cedricziel/signaldb/commit/766e2d1c82dad65a674184edaf2e8d67cb4083dd))
* **flight,wal:** compress Flight IPC payloads and WAL entries ([#945](https://github.com/cedricziel/signaldb/issues/945)) ([#998](https://github.com/cedricziel/signaldb/issues/998)) ([efb5ef4](https://github.com/cedricziel/signaldb/commit/efb5ef4bc85e2e77483f4546255b50c564015827))
* **querier:** cache per-tenant session state keyed by (tenant, dataset) ([#1001](https://github.com/cedricziel/signaldb/issues/1001)) ([7964f8e](https://github.com/cedricziel/signaldb/commit/7964f8ef6cd52c7755bb7089ddca673d56c04af8))
* **querier:** enable statistics-based file grouping and Parquet filter pushdown ([#937](https://github.com/cedricziel/signaldb/issues/937)) ([#987](https://github.com/cedricziel/signaldb/issues/987)) ([7d4aefb](https://github.com/cedricziel/signaldb/commit/7d4aefb855061ea2a07c6536eee28385a49a6722))
* **querier:** guard PromQL group cardinality and hoist histogram bounds parsing ([#1000](https://github.com/cedricziel/signaldb/issues/1000)) ([eb50c04](https://github.com/cedricziel/signaldb/commit/eb50c04791cddd8253a3e30b10ee9a0b05a36e12))
* **querier:** handle scalar args and Utf8View bodies in ir_extract UDF ([#1003](https://github.com/cedricziel/signaldb/issues/1003)) ([7304140](https://github.com/cedricziel/signaldb/commit/7304140b065017c2d41804886502cf13764a143d))
* **querier:** hoist column downcasts out of trace assembly row loops ([#999](https://github.com/cedricziel/signaldb/issues/999)) ([7b1c9e0](https://github.com/cedricziel/signaldb/commit/7b1c9e03ba0d501713a616d8d5c3f78128642edc))


### Documentation

* flight-communication.md read path now describes the CLIENT hop. ([3047cbb](https://github.com/cedricziel/signaldb/commit/3047cbbc68f03e7d586d4a2caabaa2bd7c660ca1))


### Code Refactoring

* **logging:** forbid log:: macros in favor of tracing:: ([#1006](https://github.com/cedricziel/signaldb/issues/1006)) ([071ebb4](https://github.com/cedricziel/signaldb/commit/071ebb47d02f2d6e43ccfb60380c00e3be929248))
* **querier:** simplify pass ([#1175](https://github.com/cedricziel/signaldb/issues/1175)) ([d7c380b](https://github.com/cedricziel/signaldb/commit/d7c380b6b18021c00c4c29141be43662dcca9e27))
* simplify backend workspace (dedup, dead code, redundant clones) ([#1168](https://github.com/cedricziel/signaldb/issues/1168)) ([409b778](https://github.com/cedricziel/signaldb/commit/409b778686a1cea5c54edfba7778c3e9ed3aa29c))


### Tests

* polish medium/low audit findings across the workspace ([#969](https://github.com/cedricziel/signaldb/issues/969)) ([8962f6d](https://github.com/cedricziel/signaldb/commit/8962f6d1d22c8a176d4a1d99376d61b42b1da258))
* **querier:** pin that the coercing provider pushes only un-coerced filters down ([#1242](https://github.com/cedricziel/signaldb/issues/1242)) ([98d7c33](https://github.com/cedricziel/signaldb/commit/98d7c338fa8a09b4088abcd8c263008bb671e959))
</details>

<details><summary>router: 0.3.0</summary>

## [0.3.0](https://github.com/cedricziel/signaldb/compare/router-v0.2.2...router-v0.3.0) (2026-08-17)


###   BREAKING CHANGES

* **auth:** POST /api/v1/admin/tenants/{id}/api-keys requires a non-empty `scopes` array; bodies without it are rejected.
* **cli+mcp:** signaldb-cli tenant/api-key/dataset commands move under `admin` (e.g. `signaldb-cli admin tenant list`), and queries now require a language flag (`signaldb-cli query --sql|--promql|--logql|--traceql|--ir`). No back-compat aliases are provided (post-1.0).

### Features

* **api:** code-first OpenAPI  generate spec + Rust/TS clients from annotations ([#856](https://github.com/cedricziel/signaldb/issues/856)) ([e34fbfb](https://github.com/cedricziel/signaldb/commit/e34fbfbd094034416f78597c59b306975dd97271))
* **api:** document Tempo trace query endpoints in OpenAPI + SDK ([#861](https://github.com/cedricziel/signaldb/issues/861)) ([a1e0d7f](https://github.com/cedricziel/signaldb/commit/a1e0d7f9f3c355f8bf73da686db1952487c3e046))
* **auth:** schema:read/schema:write API-key scopes, scopes on every key surface ([#1217](https://github.com/cedricziel/signaldb/issues/1217)) ([34c7a28](https://github.com/cedricziel/signaldb/commit/34c7a28e4e62fad7a05089c1a3543739d6e28450))
* **auth:** tenant:manage API-key scope for the tenant management API ([#1266](https://github.com/cedricziel/signaldb/issues/1266)) ([9dfc193](https://github.com/cedricziel/signaldb/commit/9dfc193a85e813b42f8658bf97cbfd30e3b78f2e))
* **cli+mcp:** CLI & MCP as pure SDK consumers  query --{lang}, admin grouping (Phase 1) ([#892](https://github.com/cedricziel/signaldb/issues/892)) ([92a439e](https://github.com/cedricziel/signaldb/commit/92a439e112da96029733d93db7f274c20c29cbc5))
* **logs:** surface trace_id/span_id in log query responses ([#1048](https://github.com/cedricziel/signaldb/issues/1048)) ([5a84a04](https://github.com/cedricziel/signaldb/commit/5a84a04b3582befd76ea5f231b887f2cbed253ea))
* **mcp-admin-tool-parity:** platform-admin and tenant self-management tool/CLI parity ([#1261](https://github.com/cedricziel/signaldb/issues/1261)) ([1eadc72](https://github.com/cedricziel/signaldb/commit/1eadc728ace70aff10fa01aaa8766012ace2df4c))
* **mcp:** OAuth 2.1 + DCR connector support for Claude and OpenAI ([#899](https://github.com/cedricziel/signaldb/issues/899)) ([4d0104a](https://github.com/cedricziel/signaldb/commit/4d0104a608ee392e9b25acf686dcd7359fc37631))
* metric/label discovery (MCP+CLI+SDK) and prom/loki UI migration ([#1041](https://github.com/cedricziel/signaldb/issues/1041)) ([afcc72e](https://github.com/cedricziel/signaldb/commit/afcc72e9f87a45e74c97171e8919b90868cd54f4))
* **model:** add span events to the Span model ([#847](https://github.com/cedricziel/signaldb/issues/847)) ([0dbd6e8](https://github.com/cedricziel/signaldb/commit/0dbd6e8a0701cea0ce9e46c4fc9456d1562e7d31))
* native Query IR  versioned structured query surface (query-ir-core) ([#882](https://github.com/cedricziel/signaldb/issues/882)) ([8774ac0](https://github.com/cedricziel/signaldb/commit/8774ac0fbbe4686cb7aa8b0bba73dbc25f185689))
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
* **sdk:** query surface  SDK covers PromQL/LogQL/TraceQL + Flight SQL (Phase 0) ([#890](https://github.com/cedricziel/signaldb/issues/890)) ([1fde946](https://github.com/cedricziel/signaldb/commit/1fde946cc308ef134f01492b72a3fc874e1c8f95))
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
</details>

<details><summary>signaldb-bin: 0.3.0</summary>

## [0.3.0](https://github.com/cedricziel/signaldb/compare/signaldb-bin-v0.1.3...signaldb-bin-v0.3.0) (2026-08-17)


### Features

* **api:** code-first OpenAPI  generate spec + Rust/TS clients from annotations ([#856](https://github.com/cedricziel/signaldb/issues/856)) ([e34fbfb](https://github.com/cedricziel/signaldb/commit/e34fbfbd094034416f78597c59b306975dd97271))
* **mcp:** audit, trace, meter, and bound every tool call ([#1255](https://github.com/cedricziel/signaldb/issues/1255)) ([6627df0](https://github.com/cedricziel/signaldb/commit/6627df0f3f2fc0cff97692d3e465c23bc640e5c2))
* one signaldb binary with the services as subcommands ([#1204](https://github.com/cedricziel/signaldb/issues/1204)) ([77f3278](https://github.com/cedricziel/signaldb/commit/77f3278ca445ac9b28bf955b0e482d4366a27c07))
* **self-monitoring:** runtime-configurable browser telemetry export ([#842](https://github.com/cedricziel/signaldb/issues/842)) ([343b928](https://github.com/cedricziel/signaldb/commit/343b92877d1291406de25923e671ab2a54a98028))
* source-agnostic tenant registry (admin-API tenants queryable without restart) ([#853](https://github.com/cedricziel/signaldb/issues/853)) ([c685935](https://github.com/cedricziel/signaldb/commit/c6859353a739fefcdc45f56cc0c7899193a6086a))
* **writer:** coalesce Iceberg commits with a per-table floor + force-commit primitive ([#891](https://github.com/cedricziel/signaldb/issues/891)) ([ad47bb6](https://github.com/cedricziel/signaldb/commit/ad47bb6867dd5cf622701b5778ef9f94e7b60923))
* zero-config first boot  auto-provision default tenant and print API key once ([#995](https://github.com/cedricziel/signaldb/issues/995)) ([5116c8d](https://github.com/cedricziel/signaldb/commit/5116c8d9f22950447373f74c99b17488900db00d)), closes [#796](https://github.com/cedricziel/signaldb/issues/796)


### Bug Fixes

* **build:** stop jemalloc heap profiling from crashing musl images ([#1126](https://github.com/cedricziel/signaldb/issues/1126)) ([98b2996](https://github.com/cedricziel/signaldb/commit/98b299660ef31b56d73e079a2477166b415e736e))
* **flight:** set explicit gRPC message-size limits and chunk oversized batches ([#990](https://github.com/cedricziel/signaldb/issues/990)) ([6499175](https://github.com/cedricziel/signaldb/commit/6499175d0e6402e1350ad28803d0b08954e43fe1))
* **monolith:** run the full compactor lifecycle loop, not just planning ([#1005](https://github.com/cedricziel/signaldb/issues/1005)) ([2e751fb](https://github.com/cedricziel/signaldb/commit/2e751fb5849ce596f3dca7366624ee65e4def3ac))
* provision signal tables for every registered dataset, and read an absent one as empty ([#1074](https://github.com/cedricziel/signaldb/issues/1074)) ([9a50ffa](https://github.com/cedricziel/signaldb/commit/9a50ffaa7e404a96cb80d7d3b0cc0850ede00f49))
* **router:** materialize a tenant's default dataset as a real row ([#1085](https://github.com/cedricziel/signaldb/issues/1085)) ([9443244](https://github.com/cedricziel/signaldb/commit/94432445328a0489bfd0476aaaba12ba937a2561))


### Performance Improvements

* CPU target features and jemalloc allocator for release builds ([#970](https://github.com/cedricziel/signaldb/issues/970)) ([766e2d1](https://github.com/cedricziel/signaldb/commit/766e2d1c82dad65a674184edaf2e8d67cb4083dd))
* **flight,wal:** compress Flight IPC payloads and WAL entries ([#945](https://github.com/cedricziel/signaldb/issues/945)) ([#998](https://github.com/cedricziel/signaldb/issues/998)) ([efb5ef4](https://github.com/cedricziel/signaldb/commit/efb5ef4bc85e2e77483f4546255b50c564015827))


### Code Refactoring

* **logging:** forbid log:: macros in favor of tracing:: ([#1006](https://github.com/cedricziel/signaldb/issues/1006)) ([071ebb4](https://github.com/cedricziel/signaldb/commit/071ebb47d02f2d6e43ccfb60380c00e3be929248))
* **signaldb-bin:** derive-Clone AcceptorResources instead of field-by-field, drop duplicate flight_addr ([#1187](https://github.com/cedricziel/signaldb/issues/1187)) ([3fff739](https://github.com/cedricziel/signaldb/commit/3fff7395b33baae3f0197fa56abfe1083adffb0d))
</details>

<details><summary>signaldb-cli: 0.3.0</summary>

## [0.3.0](https://github.com/cedricziel/signaldb/compare/signaldb-cli-v0.1.3...signaldb-cli-v0.3.0) (2026-08-17)


###   BREAKING CHANGES

* **auth:** POST /api/v1/admin/tenants/{id}/api-keys requires a non-empty `scopes` array; bodies without it are rejected.
* **cli+mcp:** signaldb-cli tenant/api-key/dataset commands move under `admin` (e.g. `signaldb-cli admin tenant list`), and queries now require a language flag (`signaldb-cli query --sql|--promql|--logql|--traceql|--ir`). No back-compat aliases are provided (post-1.0).

### Features

* **api:** code-first OpenAPI  generate spec + Rust/TS clients from annotations ([#856](https://github.com/cedricziel/signaldb/issues/856)) ([e34fbfb](https://github.com/cedricziel/signaldb/commit/e34fbfbd094034416f78597c59b306975dd97271))
* **auth:** schema:read/schema:write API-key scopes, scopes on every key surface ([#1217](https://github.com/cedricziel/signaldb/issues/1217)) ([34c7a28](https://github.com/cedricziel/signaldb/commit/34c7a28e4e62fad7a05089c1a3543739d6e28450))
* **auth:** tenant:manage API-key scope for the tenant management API ([#1266](https://github.com/cedricziel/signaldb/issues/1266)) ([9dfc193](https://github.com/cedricziel/signaldb/commit/9dfc193a85e813b42f8658bf97cbfd30e3b78f2e))
* **cli+mcp:** CLI & MCP as pure SDK consumers  query --{lang}, admin grouping (Phase 1) ([#892](https://github.com/cedricziel/signaldb/issues/892)) ([92a439e](https://github.com/cedricziel/signaldb/commit/92a439e112da96029733d93db7f274c20c29cbc5))
* **clients:** schema registry in SDK, CLI, and MCP ([#1223](https://github.com/cedricziel/signaldb/issues/1223)) ([1838583](https://github.com/cedricziel/signaldb/commit/1838583910be33e03d72b2be15e17d819031c9c5))
* **mcp-admin-tool-parity:** platform-admin and tenant self-management tool/CLI parity ([#1261](https://github.com/cedricziel/signaldb/issues/1261)) ([1eadc72](https://github.com/cedricziel/signaldb/commit/1eadc728ace70aff10fa01aaa8766012ace2df4c))
* metric/label discovery (MCP+CLI+SDK) and prom/loki UI migration ([#1041](https://github.com/cedricziel/signaldb/issues/1041)) ([afcc72e](https://github.com/cedricziel/signaldb/commit/afcc72e9f87a45e74c97171e8919b90868cd54f4))
* native Query IR  versioned structured query surface (query-ir-core) ([#882](https://github.com/cedricziel/signaldb/issues/882)) ([8774ac0](https://github.com/cedricziel/signaldb/commit/8774ac0fbbe4686cb7aa8b0bba73dbc25f185689))
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
</details>

<details><summary>writer: 0.3.0</summary>

## [0.3.0](https://github.com/cedricziel/signaldb/compare/writer-v0.2.1...writer-v0.3.0) (2026-08-17)


### Features

* one signaldb binary with the services as subcommands ([#1204](https://github.com/cedricziel/signaldb/issues/1204)) ([77f3278](https://github.com/cedricziel/signaldb/commit/77f3278ca445ac9b28bf955b0e482d4366a27c07))
* **otel-native-schema:** Layer 2 logical schema foundation ([#1104](https://github.com/cedricziel/signaldb/issues/1104)) ([af66060](https://github.com/cedricziel/signaldb/commit/af6606016430645693a0d524d3f15d9db4a52ead))
* semconv RPC server spans on Flight boundaries ([#904](https://github.com/cedricziel/signaldb/issues/904)) ([a791f45](https://github.com/cedricziel/signaldb/commit/a791f45edf5b1650cc9091d1acf481175060628a))
* **tracing:** add server.address and network.peer to RPC spans ([#1111](https://github.com/cedricziel/signaldb/issues/1111)) ([4e64934](https://github.com/cedricziel/signaldb/commit/4e64934814762c25226a3a7529bc9d695035d578))
* **writer:** ack ingest on WAL flush, commit to Iceberg asynchronously ([#893](https://github.com/cedricziel/signaldb/issues/893)) ([fffdbb1](https://github.com/cedricziel/signaldb/commit/fffdbb109c48893bb2725a8afd3e2e740968a152))
* **writer:** bound Iceberg metadata growth via delete-after-commit ([#895](https://github.com/cedricziel/signaldb/issues/895)) ([35ce5c7](https://github.com/cedricziel/signaldb/commit/35ce5c7aa18aa4f12d3e62c4f34221c849f973f3))
* **writer:** coalesce Iceberg commits with a per-table floor + force-commit primitive ([#891](https://github.com/cedricziel/signaldb/issues/891)) ([ad47bb6](https://github.com/cedricziel/signaldb/commit/ad47bb6867dd5cf622701b5778ef9f94e7b60923))


### Bug Fixes

* **acceptor:** dead-letter writer-rejected WAL entries instead of wedging the retry pass ([#1063](https://github.com/cedricziel/signaldb/issues/1063)) ([7fc6ada](https://github.com/cedricziel/signaldb/commit/7fc6ada1ea922784220789f304fb3f8448ff8ef1)), closes [#1060](https://github.com/cedricziel/signaldb/issues/1060)
* **build:** stop jemalloc heap profiling from crashing musl images ([#1126](https://github.com/cedricziel/signaldb/issues/1126)) ([98b2996](https://github.com/cedricziel/signaldb/commit/98b299660ef31b56d73e079a2477166b415e736e))
* **common:** resolve a tenant's default dataset even without a dataset row ([#1082](https://github.com/cedricziel/signaldb/issues/1082)) ([055733f](https://github.com/cedricziel/signaldb/commit/055733f7e2d0e016091a987836fab2e788540e82))
* metrics without service.name land as 'unknown'; boot log flood demoted to debug ([#1227](https://github.com/cedricziel/signaldb/issues/1227)) ([7b5ea34](https://github.com/cedricziel/signaldb/commit/7b5ea343096ea8a7c0f62575029ac1e838ec514c))
* **metrics:** carry NaN/±Inf values through the wire format instead of dead-lettering ([#1239](https://github.com/cedricziel/signaldb/issues/1239)) ([9e38b3a](https://github.com/cedricziel/signaldb/commit/9e38b3a993b6d632d7c67f498f2f489ea97e6636)), closes [#1061](https://github.com/cedricziel/signaldb/issues/1061)
* provision signal tables for every registered dataset, and read an absent one as empty ([#1074](https://github.com/cedricziel/signaldb/issues/1074)) ([9a50ffa](https://github.com/cedricziel/signaldb/commit/9a50ffaa7e404a96cb80d7d3b0cc0850ede00f49))
* **telemetry:** emit int-typed registry attributes as i64 ([#1013](https://github.com/cedricziel/signaldb/issues/1013)) ([be67718](https://github.com/cedricziel/signaldb/commit/be677184819e5cbe700d253a03e59cd2bffa7ba8))
* **traces:** span_kind/status_code numeric source of truth + schema evolution engine ([#1235](https://github.com/cedricziel/signaldb/issues/1235)) ([0f8603b](https://github.com/cedricziel/signaldb/commit/0f8603bdb1f39254c83af0c631653a65c8a85e3f))
* **wal:** carry tenant/dataset/signal on WAL failure telemetry ([#866](https://github.com/cedricziel/signaldb/issues/866)) ([a023dbb](https://github.com/cedricziel/signaldb/commit/a023dbb54822964d44f7c22864391eb2af957a58))
* **writer,tempo-api:** stop leaking Option Debug into logs; accept lowercase Tempo tag scopes ([#1149](https://github.com/cedricziel/signaldb/issues/1149)) ([4a83388](https://github.com/cedricziel/signaldb/commit/4a8338801252c36a948efa10d1a5cfe0d4f7de5a))
* **writer:** derive flush scope from request metadata, not the action body ([#897](https://github.com/cedricziel/signaldb/issues/897)) ([cd94186](https://github.com/cedricziel/signaldb/commit/cd9418653c1f90812ffee4a0688dd947039dbbeb))
* **writer:** table-schema-consistency check across all built-in tables ([#1241](https://github.com/cedricziel/signaldb/issues/1241)) ([4a392a8](https://github.com/cedricziel/signaldb/commit/4a392a8e6059c3f9b9798b4def4894ebb1d8e97a))
* **writer:** use registry attribute names on reconciler provisioning counters ([#1152](https://github.com/cedricziel/signaldb/issues/1152)) ([aa15a68](https://github.com/cedricziel/signaldb/commit/aa15a684317399e8356498f0d44c9f8bb3e9d98f))


### Performance Improvements

* CPU target features and jemalloc allocator for release builds ([#970](https://github.com/cedricziel/signaldb/issues/970)) ([766e2d1](https://github.com/cedricziel/signaldb/commit/766e2d1c82dad65a674184edaf2e8d67cb4083dd))
* **flight,wal:** compress Flight IPC payloads and WAL entries ([#945](https://github.com/cedricziel/signaldb/issues/945)) ([#998](https://github.com/cedricziel/signaldb/issues/998)) ([efb5ef4](https://github.com/cedricziel/signaldb/commit/efb5ef4bc85e2e77483f4546255b50c564015827))
* **wal:** batch index persistence in mark_processed_many ([#943](https://github.com/cedricziel/signaldb/issues/943)) ([#984](https://github.com/cedricziel/signaldb/issues/984)) ([41a91cd](https://github.com/cedricziel/signaldb/commit/41a91cd4938286a39c120e642f0b11261b813ab7))
* **writer:** compile trace v1-&gt;v2 materialization into a resolved-once plan ([#1245](https://github.com/cedricziel/signaldb/issues/1245)) ([a8910ee](https://github.com/cedricziel/signaldb/commit/a8910eee63c470b277629b8c3ed586acec8467b1))


### Code Refactoring

* **flight:** decode Flight data dictionary-aware ([#1004](https://github.com/cedricziel/signaldb/issues/1004)) ([94a7a30](https://github.com/cedricziel/signaldb/commit/94a7a30edd81060f2bfc5147dbf3b53307d2de72))
* **logging:** forbid log:: macros in favor of tracing:: ([#1006](https://github.com/cedricziel/signaldb/issues/1006)) ([071ebb4](https://github.com/cedricziel/signaldb/commit/071ebb47d02f2d6e43ccfb60380c00e3be929248))
* simplify backend workspace (dedup, dead code, redundant clones) ([#1168](https://github.com/cedricziel/signaldb/issues/1168)) ([409b778](https://github.com/cedricziel/signaldb/commit/409b778686a1cea5c54edfba7778c3e9ed3aa29c))
* span hygiene sweep and construction guard ([#907](https://github.com/cedricziel/signaldb/issues/907)) ([c1f7b81](https://github.com/cedricziel/signaldb/commit/c1f7b81fbc00ae5fd6c9b948f9fb35c9d5a27d26))
* **writer:** simplify pass ([#1173](https://github.com/cedricziel/signaldb/issues/1173)) ([162985e](https://github.com/cedricziel/signaldb/commit/162985e3e249658e08c145bb33624f537177a013))


### Tests

* delete tautological tests and rewrite salvageable ones as contract tests ([#961](https://github.com/cedricziel/signaldb/issues/961)) ([b3e884a](https://github.com/cedricziel/signaldb/commit/b3e884ad59b4df853429133d5eef2724a8adcada))
* make swallow-and-fallback integration tests fail on real failures ([#965](https://github.com/cedricziel/signaldb/issues/965)) ([a6720ba](https://github.com/cedricziel/signaldb/commit/a6720ba4d84b933e59f14490a2aca41f19d38779))
* polish medium/low audit findings across the workspace ([#969](https://github.com/cedricziel/signaldb/issues/969)) ([8962f6d](https://github.com/cedricziel/signaldb/commit/8962f6d1d22c8a176d4a1d99376d61b42b1da258))
</details>

---
This PR was generated with [Release Please](https://github.com/googleapis/release-please). See [documentation](https://github.com/googleapis/release-please#release-please).