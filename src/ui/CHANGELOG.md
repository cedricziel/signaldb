# Changelog

## [0.2.0](https://github.com/cedricziel/signaldb/compare/signaldb-ui-v0.1.2...signaldb-ui-v0.2.0) (2026-08-08)


### ⚠ BREAKING CHANGES

* **cli+mcp:** signaldb-cli tenant/api-key/dataset commands move under `admin` (e.g. `signaldb-cli admin tenant list`), and queries now require a language flag (`signaldb-cli query --sql|--promql|--logql|--traceql|--ir`). No back-compat aliases are provided (post-1.0).

### Features

* **api:** code-first OpenAPI — generate spec + Rust/TS clients from annotations ([#856](https://github.com/cedricziel/signaldb/issues/856)) ([e34fbfb](https://github.com/cedricziel/signaldb/commit/e34fbfbd094034416f78597c59b306975dd97271))
* **api:** document Tempo trace query endpoints in OpenAPI + SDK ([#861](https://github.com/cedricziel/signaldb/issues/861)) ([a1e0d7f](https://github.com/cedricziel/signaldb/commit/a1e0d7f9f3c355f8bf73da686db1952487c3e046))
* **cli+mcp:** CLI & MCP as pure SDK consumers — query --&lt;lang&gt;, admin grouping (Phase 1) ([#892](https://github.com/cedricziel/signaldb/issues/892)) ([92a439e](https://github.com/cedricziel/signaldb/commit/92a439e112da96029733d93db7f274c20c29cbc5))
* **logs:** surface trace_id/span_id in log query responses ([#1048](https://github.com/cedricziel/signaldb/issues/1048)) ([5a84a04](https://github.com/cedricziel/signaldb/commit/5a84a04b3582befd76ea5f231b887f2cbed253ea))
* **mcp:** OAuth 2.1 + DCR connector support for Claude and OpenAI ([#899](https://github.com/cedricziel/signaldb/issues/899)) ([4d0104a](https://github.com/cedricziel/signaldb/commit/4d0104a608ee392e9b25acf686dcd7359fc37631))
* metric/label discovery (MCP+CLI+SDK) and prom/loki UI migration ([#1041](https://github.com/cedricziel/signaldb/issues/1041)) ([afcc72e](https://github.com/cedricziel/signaldb/commit/afcc72e9f87a45e74c97171e8919b90868cd54f4))
* native Query IR — versioned structured query surface (query-ir-core) ([#882](https://github.com/cedricziel/signaldb/issues/882)) ([8774ac0](https://github.com/cedricziel/signaldb/commit/8774ac0fbbe4686cb7aa8b0bba73dbc25f185689))
* return server trace context and timings on HTTP responses (Server-Timing + traceresponse) ([#918](https://github.com/cedricziel/signaldb/issues/918)) ([453dd20](https://github.com/cedricziel/signaldb/commit/453dd2050eee95f3daf1c96f77e56964e99a2bb1))
* **sdk:** query surface — SDK covers PromQL/LogQL/TraceQL + Flight SQL (Phase 0) ([#890](https://github.com/cedricziel/signaldb/issues/890)) ([1fde946](https://github.com/cedricziel/signaldb/commit/1fde946cc308ef134f01492b72a3fc874e1c8f95))
* **self-monitoring:** runtime-configurable browser telemetry export ([#842](https://github.com/cedricziel/signaldb/issues/842)) ([343b928](https://github.com/cedricziel/signaldb/commit/343b92877d1291406de25923e671ab2a54a98028))
* **ui:** add a faceted search sidebar to the traces tab ([#1076](https://github.com/cedricziel/signaldb/issues/1076)) ([81a8c24](https://github.com/cedricziel/signaldb/commit/81a8c24f455e69816360e18514c97c754d72d90a))
* **ui:** make the explore volume charts readable, and give traces one ([#1075](https://github.com/cedricziel/signaldb/issues/1075)) ([91ec80d](https://github.com/cedricziel/signaldb/commit/91ec80da2a8009a6237fa0e939961b00305fd0f3))
* **ui:** render span events and exceptions in the trace view ([#849](https://github.com/cedricziel/signaldb/issues/849)) ([5427c05](https://github.com/cedricziel/signaldb/commit/5427c0527c0cd1d7591da3d9077b1aa88714729a))


### Bug Fixes

* **router:** bound Tempo tag-values queries by time window ([#929](https://github.com/cedricziel/signaldb/issues/929)) ([#979](https://github.com/cedricziel/signaldb/issues/979)) ([7cc301a](https://github.com/cedricziel/signaldb/commit/7cc301adc539a77540682d155425bace30ddc803))
* **ui:** clear XHR timing resources like fetch instrumentation ([#1034](https://github.com/cedricziel/signaldb/issues/1034)) ([a0bcf10](https://github.com/cedricziel/signaldb/commit/a0bcf10ebe8696960f86d267285b6db266c8eb7b))
* **ui:** collapse high-cardinality navigation span name ([#876](https://github.com/cedricziel/signaldb/issues/876)) ([692efb7](https://github.com/cedricziel/signaldb/commit/692efb73eb2a97bc2fa0887575a9cd834a0faf4a))


### Code Refactoring

* **cli:** make signaldb-cli depend only on the SDK (+ create_user API) ([#874](https://github.com/cedricziel/signaldb/issues/874)) ([8e5cce5](https://github.com/cedricziel/signaldb/commit/8e5cce56c821d69917b55cc8c21a9a2ef55864b7))

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
