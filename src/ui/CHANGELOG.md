# Changelog

## [0.2.0](https://github.com/cedricziel/signaldb/compare/signaldb-ui-v0.1.2...signaldb-ui-v0.2.0) (2026-08-17)


### ⚠ BREAKING CHANGES

* **auth:** POST /api/v1/admin/tenants/{id}/api-keys requires a non-empty `scopes` array; bodies without it are rejected.
* **cli+mcp:** signaldb-cli tenant/api-key/dataset commands move under `admin` (e.g. `signaldb-cli admin tenant list`), and queries now require a language flag (`signaldb-cli query --sql|--promql|--logql|--traceql|--ir`). No back-compat aliases are provided (post-1.0).

### Features

* add an Errors & Exceptions tab ([#1167](https://github.com/cedricziel/signaldb/issues/1167)) ([79f3749](https://github.com/cedricziel/signaldb/commit/79f374916a8add7aa47abd0c8569e13c560a2d7c))
* **api:** code-first OpenAPI — generate spec + Rust/TS clients from annotations ([#856](https://github.com/cedricziel/signaldb/issues/856)) ([e34fbfb](https://github.com/cedricziel/signaldb/commit/e34fbfbd094034416f78597c59b306975dd97271))
* **api:** document Tempo trace query endpoints in OpenAPI + SDK ([#861](https://github.com/cedricziel/signaldb/issues/861)) ([a1e0d7f](https://github.com/cedricziel/signaldb/commit/a1e0d7f9f3c355f8bf73da686db1952487c3e046))
* **auth:** schema:read/schema:write API-key scopes, scopes on every key surface ([#1217](https://github.com/cedricziel/signaldb/issues/1217)) ([34c7a28](https://github.com/cedricziel/signaldb/commit/34c7a28e4e62fad7a05089c1a3543739d6e28450))
* **auth:** tenant:manage API-key scope for the tenant management API ([#1266](https://github.com/cedricziel/signaldb/issues/1266)) ([9dfc193](https://github.com/cedricziel/signaldb/commit/9dfc193a85e813b42f8658bf97cbfd30e3b78f2e))
* **cli+mcp:** CLI & MCP as pure SDK consumers — query --&lt;lang&gt;, admin grouping (Phase 1) ([#892](https://github.com/cedricziel/signaldb/issues/892)) ([92a439e](https://github.com/cedricziel/signaldb/commit/92a439e112da96029733d93db7f274c20c29cbc5))
* compute the traces group table on the server, via a scoped IR aggregate ([#1092](https://github.com/cedricziel/signaldb/issues/1092)) ([ec5c284](https://github.com/cedricziel/signaldb/commit/ec5c284cbe57c0ce34da7f295f08502de2493b82))
* **logs:** surface trace_id/span_id in log query responses ([#1048](https://github.com/cedricziel/signaldb/issues/1048)) ([5a84a04](https://github.com/cedricziel/signaldb/commit/5a84a04b3582befd76ea5f231b887f2cbed253ea))
* **mcp-admin-tool-parity:** platform-admin and tenant self-management tool/CLI parity ([#1261](https://github.com/cedricziel/signaldb/issues/1261)) ([1eadc72](https://github.com/cedricziel/signaldb/commit/1eadc728ace70aff10fa01aaa8766012ace2df4c))
* **mcp:** OAuth 2.1 + DCR connector support for Claude and OpenAI ([#899](https://github.com/cedricziel/signaldb/issues/899)) ([4d0104a](https://github.com/cedricziel/signaldb/commit/4d0104a608ee392e9b25acf686dcd7359fc37631))
* metric/label discovery (MCP+CLI+SDK) and prom/loki UI migration ([#1041](https://github.com/cedricziel/signaldb/issues/1041)) ([afcc72e](https://github.com/cedricziel/signaldb/commit/afcc72e9f87a45e74c97171e8919b90868cd54f4))
* native Query IR — versioned structured query surface (query-ir-core) ([#882](https://github.com/cedricziel/signaldb/issues/882)) ([8774ac0](https://github.com/cedricziel/signaldb/commit/8774ac0fbbe4686cb7aa8b0bba73dbc25f185689))
* **query-ir:** add v2 heatmaps ([#1102](https://github.com/cedricziel/signaldb/issues/1102)) ([96184cf](https://github.com/cedricziel/signaldb/commit/96184cf42809a4cbf0e4a15f592cb544dbb7a597))
* **query-ir:** flamegraph result envelope for profiles ([#1144](https://github.com/cedricziel/signaldb/issues/1144)) ([394407f](https://github.com/cedricziel/signaldb/commit/394407f72756b15c97cb6ce6efcf01ce0b61b33b))
* Real trace-context parenting for documentLoad + complementary log-record telemetry ([#1117](https://github.com/cedricziel/signaldb/issues/1117)) ([43a7c63](https://github.com/cedricziel/signaldb/commit/43a7c63a42a55aed11df304387d286f4bb5bccb9))
* retry throttled requests in every SignalDB client ([#1260](https://github.com/cedricziel/signaldb/issues/1260)) ([3342dcc](https://github.com/cedricziel/signaldb/commit/3342dcced2cbc489adc7bf5076a0c9059b805adb))
* return server trace context and timings on HTTP responses (Server-Timing + traceresponse) ([#918](https://github.com/cedricziel/signaldb/issues/918)) ([453dd20](https://github.com/cedricziel/signaldb/commit/453dd2050eee95f3daf1c96f77e56964e99a2bb1))
* **router:** Pyroscope OpenAPI parity (CLI/MCP/UI/SDK) ([#1268](https://github.com/cedricziel/signaldb/issues/1268)) ([2b54e2d](https://github.com/cedricziel/signaldb/commit/2b54e2d693801a0bfd9afdf4e982abfac6efc955))
* **router:** schema registry API under /api/v1/schema ([#1219](https://github.com/cedricziel/signaldb/issues/1219)) ([71af424](https://github.com/cedricziel/signaldb/commit/71af424a0d96eb3f87198af4c4213bb89106cf28))
* **sdk:** query surface — SDK covers PromQL/LogQL/TraceQL + Flight SQL (Phase 0) ([#890](https://github.com/cedricziel/signaldb/issues/890)) ([1fde946](https://github.com/cedricziel/signaldb/commit/1fde946cc308ef134f01492b72a3fc874e1c8f95))
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

## [0.1.2](https://github.com/cedricziel/signaldb/compare/signaldb-ui-v0.1.1...signaldb-ui-v0.1.2) (2026-07-30)


### Features

* CPU self-profiling + Profiles tab in the Explore UI ([#835](https://github.com/cedricziel/signaldb/issues/835)) ([9434734](https://github.com/cedricziel/signaldb/commit/94347345da14db950c760db21ad8516f9fcbac92))
* **ui:** cardinality warnings in the metrics builder ([#834](https://github.com/cedricziel/signaldb/issues/834)) ([7dc8d8a](https://github.com/cedricziel/signaldb/commit/7dc8d8af033fa10bc1155402137a8ff7166bb218))
* **ui:** group-first traces view with selectable dimensions, RED columns, and drill-in ([#824](https://github.com/cedricziel/signaldb/issues/824)) ([65112aa](https://github.com/cedricziel/signaldb/commit/65112aae4f6bbc572543d9102c822089701a3a0c))
* **ui:** instrument browser frontend with OpenTelemetry ([#830](https://github.com/cedricziel/signaldb/issues/830)) ([2bb21de](https://github.com/cedricziel/signaldb/commit/2bb21de6515d3da4756668a9753f94f7eff6ccc1))
* **ui:** metrics explore visual query builder ([#828](https://github.com/cedricziel/signaldb/issues/828)) ([673f0d9](https://github.com/cedricziel/signaldb/commit/673f0d95781ec06e2d2d0f75f6023d20d0159abb))

## [0.1.1](https://github.com/cedricziel/signaldb/compare/signaldb-ui-v0.1.0...signaldb-ui-v0.1.1) (2026-07-30)


### Features

* **auth:** add scoped tenant self-service ([7830c3d](https://github.com/cedricziel/signaldb/commit/7830c3d706c21480f9767bca8639e5fcb82622bc))
* embedded UI session auth + tenant-scoped whoami ([#773](https://github.com/cedricziel/signaldb/issues/773)) ([f217064](https://github.com/cedricziel/signaldb/commit/f217064d3f31002132761040bc8a82fe1c5e9c59))
* native explore UI for logs, traces, and metrics ([#768](https://github.com/cedricziel/signaldb/issues/768)) ([5db53c9](https://github.com/cedricziel/signaldb/commit/5db53c9f87b791c1f1d9590c6a1288db376da92b))
* **ui:** use human account sessions ([c35d405](https://github.com/cedricziel/signaldb/commit/c35d405838ca97df26e50cd9fab83630ac4e2b7c))


### Bug Fixes

* **ui:** sign in once — email/password login with a post-login tenant picker ([#794](https://github.com/cedricziel/signaldb/issues/794)) ([1feafbf](https://github.com/cedricziel/signaldb/commit/1feafbfc187069944c34a5903d65552f740c2d3a))
