:robot: I have created a release *beep* *boop*
---


<details><summary>grafana-plugin: 1.2.1</summary>

## [1.2.1](https://github.com/cedricziel/signaldb/compare/grafana-plugin-v1.2.0...grafana-plugin-v1.2.1) (2026-08-08)


### Performance Improvements

* **flight,wal:** compress Flight IPC payloads and WAL entries ([#945](https://github.com/cedricziel/signaldb/issues/945)) ([#998](https://github.com/cedricziel/signaldb/issues/998)) ([efb5ef4](https://github.com/cedricziel/signaldb/commit/efb5ef4bc85e2e77483f4546255b50c564015827))


### Tests

* delete tautological tests and rewrite salvageable ones as contract tests ([#961](https://github.com/cedricziel/signaldb/issues/961)) ([b3e884a](https://github.com/cedricziel/signaldb/commit/b3e884ad59b4df853429133d5eef2724a8adcada))
* polish medium/low audit findings across the workspace ([#969](https://github.com/cedricziel/signaldb/issues/969)) ([8962f6d](https://github.com/cedricziel/signaldb/commit/8962f6d1d22c8a176d4a1d99376d61b42b1da258))
</details>

<details><summary>signaldb-ui: 0.2.0</summary>

## [0.2.0](https://github.com/cedricziel/signaldb/compare/signaldb-ui-v0.1.2...signaldb-ui-v0.2.0) (2026-08-08)


###   BREAKING CHANGES

* **cli+mcp:** signaldb-cli tenant/api-key/dataset commands move under `admin` (e.g. `signaldb-cli admin tenant list`), and queries now require a language flag (`signaldb-cli query --sql|--promql|--logql|--traceql|--ir`). No back-compat aliases are provided (post-1.0).

### Features

* **api:** code-first OpenAPI  generate spec + Rust/TS clients from annotations ([#856](https://github.com/cedricziel/signaldb/issues/856)) ([e34fbfb](https://github.com/cedricziel/signaldb/commit/e34fbfbd094034416f78597c59b306975dd97271))
* **api:** document Tempo trace query endpoints in OpenAPI + SDK ([#861](https://github.com/cedricziel/signaldb/issues/861)) ([a1e0d7f](https://github.com/cedricziel/signaldb/commit/a1e0d7f9f3c355f8bf73da686db1952487c3e046))
* **cli+mcp:** CLI & MCP as pure SDK consumers  query --&lt;lang&gt;, admin grouping (Phase 1) ([#892](https://github.com/cedricziel/signaldb/issues/892)) ([92a439e](https://github.com/cedricziel/signaldb/commit/92a439e112da96029733d93db7f274c20c29cbc5))
* **logs:** surface trace_id/span_id in log query responses ([#1048](https://github.com/cedricziel/signaldb/issues/1048)) ([5a84a04](https://github.com/cedricziel/signaldb/commit/5a84a04b3582befd76ea5f231b887f2cbed253ea))
* **mcp:** OAuth 2.1 + DCR connector support for Claude and OpenAI ([#899](https://github.com/cedricziel/signaldb/issues/899)) ([4d0104a](https://github.com/cedricziel/signaldb/commit/4d0104a608ee392e9b25acf686dcd7359fc37631))
* metric/label discovery (MCP+CLI+SDK) and prom/loki UI migration ([#1041](https://github.com/cedricziel/signaldb/issues/1041)) ([afcc72e](https://github.com/cedricziel/signaldb/commit/afcc72e9f87a45e74c97171e8919b90868cd54f4))
* native Query IR  versioned structured query surface (query-ir-core) ([#882](https://github.com/cedricziel/signaldb/issues/882)) ([8774ac0](https://github.com/cedricziel/signaldb/commit/8774ac0fbbe4686cb7aa8b0bba73dbc25f185689))
* return server trace context and timings on HTTP responses (Server-Timing + traceresponse) ([#918](https://github.com/cedricziel/signaldb/issues/918)) ([453dd20](https://github.com/cedricziel/signaldb/commit/453dd2050eee95f3daf1c96f77e56964e99a2bb1))
* **sdk:** query surface  SDK covers PromQL/LogQL/TraceQL + Flight SQL (Phase 0) ([#890](https://github.com/cedricziel/signaldb/issues/890)) ([1fde946](https://github.com/cedricziel/signaldb/commit/1fde946cc308ef134f01492b72a3fc874e1c8f95))
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
</details>

<details><summary>loki-api: 0.1.2</summary>

## [0.1.2](https://github.com/cedricziel/signaldb/compare/loki-api-v0.1.1...loki-api-v0.1.2) (2026-08-08)


### Features

* **logs:** surface trace_id/span_id in log query responses ([#1048](https://github.com/cedricziel/signaldb/issues/1048)) ([5a84a04](https://github.com/cedricziel/signaldb/commit/5a84a04b3582befd76ea5f231b887f2cbed253ea))
</details>

<details><summary>mcp-server: 0.2.0</summary>

## [0.2.0](https://github.com/cedricziel/signaldb/compare/mcp-server-v0.1.0...mcp-server-v0.2.0) (2026-08-08)


###   BREAKING CHANGES

* **cli+mcp:** signaldb-cli tenant/api-key/dataset commands move under `admin` (e.g. `signaldb-cli admin tenant list`), and queries now require a language flag (`signaldb-cli query --sql|--promql|--logql|--traceql|--ir`). No back-compat aliases are provided (post-1.0).

### Features

* **cli+mcp:** CLI & MCP as pure SDK consumers  query --&lt;lang&gt;, admin grouping (Phase 1) ([#892](https://github.com/cedricziel/signaldb/issues/892)) ([92a439e](https://github.com/cedricziel/signaldb/commit/92a439e112da96029733d93db7f274c20c29cbc5))
* **mcp:** make Streamable HTTP Host allowlist configurable ([#881](https://github.com/cedricziel/signaldb/issues/881)) ([a549e7e](https://github.com/cedricziel/signaldb/commit/a549e7e3550967d446bdb05f7f3ea27ce64f07a1))
* **mcp:** OAuth 2.1 + DCR connector support for Claude and OpenAI ([#899](https://github.com/cedricziel/signaldb/issues/899)) ([4d0104a](https://github.com/cedricziel/signaldb/commit/4d0104a608ee392e9b25acf686dcd7359fc37631))
* **mcp:** scaffold standalone signaldb-mcp server with bearer auth ([#864](https://github.com/cedricziel/signaldb/issues/864)) ([0affbf5](https://github.com/cedricziel/signaldb/commit/0affbf5e92a87dabe041b7766fb97cd1f639e73c))
* **mcp:** serve a single-trace waterfall via the MCP Apps extension ([#1016](https://github.com/cedricziel/signaldb/issues/1016)) ([db434c7](https://github.com/cedricziel/signaldb/commit/db434c7de6fa8456e9f59557f0adc9104a3bbd28))
* **mcp:** Tempo-backed read tools (search_traces, get_trace, discover_attributes) ([#863](https://github.com/cedricziel/signaldb/issues/863)) ([3888f5d](https://github.com/cedricziel/signaldb/commit/3888f5d7e292a279c94e72eb871f80a564e56811))
* metric/label discovery (MCP+CLI+SDK) and prom/loki UI migration ([#1041](https://github.com/cedricziel/signaldb/issues/1041)) ([afcc72e](https://github.com/cedricziel/signaldb/commit/afcc72e9f87a45e74c97171e8919b90868cd54f4))


### Bug Fixes

* **mcp:** add connect and request timeouts to router HTTP client ([#885](https://github.com/cedricziel/signaldb/issues/885)) ([#976](https://github.com/cedricziel/signaldb/issues/976)) ([f0f2182](https://github.com/cedricziel/signaldb/commit/f0f21824b654d57668e2c235f310d3a048a314f4))


### Performance Improvements

* CPU target features and jemalloc allocator for release builds ([#970](https://github.com/cedricziel/signaldb/issues/970)) ([766e2d1](https://github.com/cedricziel/signaldb/commit/766e2d1c82dad65a674184edaf2e8d67cb4083dd))


### Code Refactoring

* **mcp:** make signaldb-mcp depend only on the SDK (forward-only auth) ([#873](https://github.com/cedricziel/signaldb/issues/873)) ([d404af6](https://github.com/cedricziel/signaldb/commit/d404af62bad3872b2a8f722067053d4adc083adb))


### Tests

* make tests assert what their names promise ([#966](https://github.com/cedricziel/signaldb/issues/966)) ([446ed06](https://github.com/cedricziel/signaldb/commit/446ed062a7480902ef391884b1c2e12f77ddd66f))
* replace sleep-based synchronization with deterministic waits ([#968](https://github.com/cedricziel/signaldb/issues/968)) ([6391326](https://github.com/cedricziel/signaldb/commit/6391326013c8620f186e4a63c2cdf3bbdf9ee963))
</details>

<details><summary>signal-producer: 0.2.2</summary>

## [0.2.2](https://github.com/cedricziel/signaldb/compare/signal-producer-v0.2.1...signal-producer-v0.2.2) (2026-08-08)


### Bug Fixes

* **signal-producer:** emit realistic span durations instead of zero ([#797](https://github.com/cedricziel/signaldb/issues/797)) ([#974](https://github.com/cedricziel/signaldb/issues/974)) ([20c9120](https://github.com/cedricziel/signaldb/commit/20c9120762aff967590b62ffc5b3bce6c3e9bfea))
</details>

<details><summary>signaldb-api: 0.1.2</summary>

## [0.1.2](https://github.com/cedricziel/signaldb/compare/signaldb-api-v0.1.1...signaldb-api-v0.1.2) (2026-08-08)


### Features

* **api:** code-first OpenAPI  generate spec + Rust/TS clients from annotations ([#856](https://github.com/cedricziel/signaldb/issues/856)) ([e34fbfb](https://github.com/cedricziel/signaldb/commit/e34fbfbd094034416f78597c59b306975dd97271))


### Code Refactoring

* **cli:** make signaldb-cli depend only on the SDK (+ create_user API) ([#874](https://github.com/cedricziel/signaldb/issues/874)) ([8e5cce5](https://github.com/cedricziel/signaldb/commit/8e5cce56c821d69917b55cc8c21a9a2ef55864b7))
</details>

<details><summary>signaldb-sdk: 0.2.0</summary>

## [0.2.0](https://github.com/cedricziel/signaldb/compare/signaldb-sdk-v0.1.1...signaldb-sdk-v0.2.0) (2026-08-08)


###   BREAKING CHANGES

* **cli+mcp:** signaldb-cli tenant/api-key/dataset commands move under `admin` (e.g. `signaldb-cli admin tenant list`), and queries now require a language flag (`signaldb-cli query --sql|--promql|--logql|--traceql|--ir`). No back-compat aliases are provided (post-1.0).

### Features

* **api:** code-first OpenAPI  generate spec + Rust/TS clients from annotations ([#856](https://github.com/cedricziel/signaldb/issues/856)) ([e34fbfb](https://github.com/cedricziel/signaldb/commit/e34fbfbd094034416f78597c59b306975dd97271))
* **api:** document Tempo trace query endpoints in OpenAPI + SDK ([#861](https://github.com/cedricziel/signaldb/issues/861)) ([a1e0d7f](https://github.com/cedricziel/signaldb/commit/a1e0d7f9f3c355f8bf73da686db1952487c3e046))
* **cli+mcp:** CLI & MCP as pure SDK consumers  query --&lt;lang&gt;, admin grouping (Phase 1) ([#892](https://github.com/cedricziel/signaldb/issues/892)) ([92a439e](https://github.com/cedricziel/signaldb/commit/92a439e112da96029733d93db7f274c20c29cbc5))
* **mcp:** OAuth 2.1 + DCR connector support for Claude and OpenAI ([#899](https://github.com/cedricziel/signaldb/issues/899)) ([4d0104a](https://github.com/cedricziel/signaldb/commit/4d0104a608ee392e9b25acf686dcd7359fc37631))
* metric/label discovery (MCP+CLI+SDK) and prom/loki UI migration ([#1041](https://github.com/cedricziel/signaldb/issues/1041)) ([afcc72e](https://github.com/cedricziel/signaldb/commit/afcc72e9f87a45e74c97171e8919b90868cd54f4))
* native Query IR  versioned structured query surface (query-ir-core) ([#882](https://github.com/cedricziel/signaldb/issues/882)) ([8774ac0](https://github.com/cedricziel/signaldb/commit/8774ac0fbbe4686cb7aa8b0bba73dbc25f185689))
* return server trace context and timings on HTTP responses (Server-Timing + traceresponse) ([#918](https://github.com/cedricziel/signaldb/issues/918)) ([453dd20](https://github.com/cedricziel/signaldb/commit/453dd2050eee95f3daf1c96f77e56964e99a2bb1))
* **sdk:** query surface  SDK covers PromQL/LogQL/TraceQL + Flight SQL (Phase 0) ([#890](https://github.com/cedricziel/signaldb/issues/890)) ([1fde946](https://github.com/cedricziel/signaldb/commit/1fde946cc308ef134f01492b72a3fc874e1c8f95))


### Bug Fixes

* **router:** bound Tempo tag-values queries by time window ([#929](https://github.com/cedricziel/signaldb/issues/929)) ([#979](https://github.com/cedricziel/signaldb/issues/979)) ([7cc301a](https://github.com/cedricziel/signaldb/commit/7cc301adc539a77540682d155425bace30ddc803))


### Performance Improvements

* **flight,wal:** compress Flight IPC payloads and WAL entries ([#945](https://github.com/cedricziel/signaldb/issues/945)) ([#998](https://github.com/cedricziel/signaldb/issues/998)) ([efb5ef4](https://github.com/cedricziel/signaldb/commit/efb5ef4bc85e2e77483f4546255b50c564015827))


### Code Refactoring

* **cli:** make signaldb-cli depend only on the SDK (+ create_user API) ([#874](https://github.com/cedricziel/signaldb/issues/874)) ([8e5cce5](https://github.com/cedricziel/signaldb/commit/8e5cce56c821d69917b55cc8c21a9a2ef55864b7))


### Tests

* make tests assert what their names promise ([#966](https://github.com/cedricziel/signaldb/issues/966)) ([446ed06](https://github.com/cedricziel/signaldb/commit/446ed062a7480902ef391884b1c2e12f77ddd66f))
* replace sleep-based synchronization with deterministic waits ([#968](https://github.com/cedricziel/signaldb/issues/968)) ([6391326](https://github.com/cedricziel/signaldb/commit/6391326013c8620f186e4a63c2cdf3bbdf9ee963))
</details>

<details><summary>tempo-api: 0.1.2</summary>

## [0.1.2](https://github.com/cedricziel/signaldb/compare/tempo-api-v0.1.1...tempo-api-v0.1.2) (2026-08-08)


### Features

* **api:** document Tempo trace query endpoints in OpenAPI + SDK ([#861](https://github.com/cedricziel/signaldb/issues/861)) ([a1e0d7f](https://github.com/cedricziel/signaldb/commit/a1e0d7f9f3c355f8bf73da686db1952487c3e046))
* **querier,router:** surface span events on the single-trace path ([#848](https://github.com/cedricziel/signaldb/issues/848)) ([5b344e9](https://github.com/cedricziel/signaldb/commit/5b344e98b6e787aeca35d68bf18ca5ca92657454))


### Tests

* delete tautological tests and rewrite salvageable ones as contract tests ([#961](https://github.com/cedricziel/signaldb/issues/961)) ([b3e884a](https://github.com/cedricziel/signaldb/commit/b3e884ad59b4df853429133d5eef2724a8adcada))
* polish medium/low audit findings across the workspace ([#969](https://github.com/cedricziel/signaldb/issues/969)) ([8962f6d](https://github.com/cedricziel/signaldb/commit/8962f6d1d22c8a176d4a1d99376d61b42b1da258))
</details>

<details><summary>tests-integration: 0.1.5</summary>

### Dependencies


</details>

<details><summary>acceptor: 0.3.0</summary>

## [0.3.0](https://github.com/cedricziel/signaldb/compare/acceptor-v0.2.1...acceptor-v0.3.0) (2026-08-08)


### Features

* **self-monitoring:** runtime-configurable browser telemetry export ([#842](https://github.com/cedricziel/signaldb/issues/842)) ([343b928](https://github.com/cedricziel/signaldb/commit/343b92877d1291406de25923e671ab2a54a98028))
* semconv CLIENT spans on Flight call sites ([#905](https://github.com/cedricziel/signaldb/issues/905)) ([3047cbb](https://github.com/cedricziel/signaldb/commit/3047cbbc68f03e7d586d4a2caabaa2bd7c660ca1))
* semconv self-tracing foundations (resource, span factories, acceptor boundary) ([#903](https://github.com/cedricziel/signaldb/issues/903)) ([dbe4ca2](https://github.com/cedricziel/signaldb/commit/dbe4ca2389ac8db0dba721f66d79db4d0475ed76))


### Bug Fixes

* **acceptor:** dead-letter poison WAL entries in the retry consumer ([#1015](https://github.com/cedricziel/signaldb/issues/1015)) ([866821c](https://github.com/cedricziel/signaldb/commit/866821c68793361c26f4a313423d00457777b739))
* **acceptor:** dead-letter poison WAL entries on first failure ([#1059](https://github.com/cedricziel/signaldb/issues/1059)) ([9d43c85](https://github.com/cedricziel/signaldb/commit/9d43c85445cb8c6d1bcb19279e29015680dc3fd4))
* **acceptor:** dead-letter writer-rejected WAL entries instead of wedging the retry pass ([#1063](https://github.com/cedricziel/signaldb/issues/1063)) ([7fc6ada](https://github.com/cedricziel/signaldb/commit/7fc6ada1ea922784220789f304fb3f8448ff8ef1)), closes [#1060](https://github.com/cedricziel/signaldb/issues/1060)
* **acceptor:** reject exports on OTLP conversion failure instead of ACKing empty batches ([#926](https://github.com/cedricziel/signaldb/issues/926)) ([#981](https://github.com/cedricziel/signaldb/issues/981)) ([02c0a3b](https://github.com/cedricziel/signaldb/commit/02c0a3b99fdc1327595ad8a0bf8434de1977615d))
* **flight:** set explicit gRPC message-size limits and chunk oversized batches ([#990](https://github.com/cedricziel/signaldb/issues/990)) ([6499175](https://github.com/cedricziel/signaldb/commit/6499175d0e6402e1350ad28803d0b08954e43fe1))
* **model:** stop flattening trace hierarchies to root + direct children ([#1018](https://github.com/cedricziel/signaldb/issues/1018)) ([5fee337](https://github.com/cedricziel/signaldb/commit/5fee33711628bf3f041c436c34f363f114ed93fb))


### Performance Improvements

* CPU target features and jemalloc allocator for release builds ([#970](https://github.com/cedricziel/signaldb/issues/970)) ([766e2d1](https://github.com/cedricziel/signaldb/commit/766e2d1c82dad65a674184edaf2e8d67cb4083dd))
* **flight,wal:** compress Flight IPC payloads and WAL entries ([#945](https://github.com/cedricziel/signaldb/issues/945)) ([#998](https://github.com/cedricziel/signaldb/issues/998)) ([efb5ef4](https://github.com/cedricziel/signaldb/commit/efb5ef4bc85e2e77483f4546255b50c564015827))


### Documentation

* flight-communication.md read path now describes the CLIENT hop. ([3047cbb](https://github.com/cedricziel/signaldb/commit/3047cbbc68f03e7d586d4a2caabaa2bd7c660ca1))
* **openspec:** backfill OTLP ingest specs + profiles HTTP test coverage ([#852](https://github.com/cedricziel/signaldb/issues/852)) ([3382a3e](https://github.com/cedricziel/signaldb/commit/3382a3e939f21b11dfa550bd8d3b250251044d06))


### Code Refactoring

* **logging:** forbid log:: macros in favor of tracing:: ([#1006](https://github.com/cedricziel/signaldb/issues/1006)) ([071ebb4](https://github.com/cedricziel/signaldb/commit/071ebb47d02f2d6e43ccfb60380c00e3be929248))
* span hygiene sweep and construction guard ([#907](https://github.com/cedricziel/signaldb/issues/907)) ([c1f7b81](https://github.com/cedricziel/signaldb/commit/c1f7b81fbc00ae5fd6c9b948f9fb35c9d5a27d26))


### Tests

* polish medium/low audit findings across the workspace ([#969](https://github.com/cedricziel/signaldb/issues/969)) ([8962f6d](https://github.com/cedricziel/signaldb/commit/8962f6d1d22c8a176d4a1d99376d61b42b1da258))
</details>

<details><summary>common: 0.3.0</summary>

## [0.3.0](https://github.com/cedricziel/signaldb/compare/common-v0.2.1...common-v0.3.0) (2026-08-08)


###   BREAKING CHANGES

* **compactor:** [compactor.orphan_cleanup] revalidate_before_delete no longer exists. Note that a leftover key is silently ignored rather than rejected -- the design assumed unknown keys fail config parsing, but neither config struct sets serde(deny_unknown_fields), and adding it is not a safe drive-by because figment's env provider populates the same structs. Documented in the compactor configuration reference; tightening the structs deserves its own change.
* **compactor:** [compactor] min_input_file_size_kb is replaced by max_input_file_size_kb (semantics inverted) and max_files_per_job is removed. No backward-compat alias is provided.

### Features

* **api:** code-first OpenAPI  generate spec + Rust/TS clients from annotations ([#856](https://github.com/cedricziel/signaldb/issues/856)) ([e34fbfb](https://github.com/cedricziel/signaldb/commit/e34fbfbd094034416f78597c59b306975dd97271))
* **compactor:** reclaim metadata backlog and enable orphan cleanup by default ([#1008](https://github.com/cedricziel/signaldb/issues/1008)) ([908ea79](https://github.com/cedricziel/signaldb/commit/908ea798e78a6d2dd90396f56e584275e9dfc9b3))
* DB client spans, query stage spans, compactor job spans ([#906](https://github.com/cedricziel/signaldb/issues/906)) ([04a4c4e](https://github.com/cedricziel/signaldb/commit/04a4c4e5788cf6531e0421b50b523b04ac4db38b))
* **iceberg:** tune the Parquet writer properties now that they are honored ([#1025](https://github.com/cedricziel/signaldb/issues/1025)) ([219132a](https://github.com/cedricziel/signaldb/commit/219132a3eb1bba1c15975245081ad4a2d54eb7d1))
* **mcp:** OAuth 2.1 + DCR connector support for Claude and OpenAI ([#899](https://github.com/cedricziel/signaldb/issues/899)) ([4d0104a](https://github.com/cedricziel/signaldb/commit/4d0104a608ee392e9b25acf686dcd7359fc37631))
* **mcp:** scaffold standalone signaldb-mcp server with bearer auth ([#864](https://github.com/cedricziel/signaldb/issues/864)) ([0affbf5](https://github.com/cedricziel/signaldb/commit/0affbf5e92a87dabe041b7766fb97cd1f639e73c))
* **model:** add span events to the Span model ([#847](https://github.com/cedricziel/signaldb/issues/847)) ([0dbd6e8](https://github.com/cedricziel/signaldb/commit/0dbd6e8a0701cea0ce9e46c4fc9456d1562e7d31))
* native Query IR  versioned structured query surface (query-ir-core) ([#882](https://github.com/cedricziel/signaldb/issues/882)) ([8774ac0](https://github.com/cedricziel/signaldb/commit/8774ac0fbbe4686cb7aa8b0bba73dbc25f185689))
* **querier,router:** surface span events on the single-trace path ([#848](https://github.com/cedricziel/signaldb/issues/848)) ([5b344e9](https://github.com/cedricziel/signaldb/commit/5b344e98b6e787aeca35d68bf18ca5ca92657454))
* record Flight query failures as span exceptions + surface reasons ([#846](https://github.com/cedricziel/signaldb/issues/846)) ([20d89f5](https://github.com/cedricziel/signaldb/commit/20d89f51eee05ff25ddfa523053dad7ebc8ea6e2))
* return server trace context and timings on HTTP responses (Server-Timing + traceresponse) ([#918](https://github.com/cedricziel/signaldb/issues/918)) ([453dd20](https://github.com/cedricziel/signaldb/commit/453dd2050eee95f3daf1c96f77e56964e99a2bb1))
* **schema:** size and extend trace_id/span_id bloom filters ([#1045](https://github.com/cedricziel/signaldb/issues/1045)) ([2e0e352](https://github.com/cedricziel/signaldb/commit/2e0e352db80701185fe8fb4f467f2931e25ee0c8))
* **self-monitoring:** heap self-profiling as OTLP profiles ([#840](https://github.com/cedricziel/signaldb/issues/840)) ([31fb7f1](https://github.com/cedricziel/signaldb/commit/31fb7f1f12fbfb8315f76efe62215c5c1b0cc575))
* **self-monitoring:** name HTTP server spans per OTel semantic conventions ([#844](https://github.com/cedricziel/signaldb/issues/844)) ([4815f7e](https://github.com/cedricziel/signaldb/commit/4815f7ecac36a56ef1869b6fd41ad0c015331bc1))
* **self-monitoring:** runtime-configurable browser telemetry export ([#842](https://github.com/cedricziel/signaldb/issues/842)) ([343b928](https://github.com/cedricziel/signaldb/commit/343b92877d1291406de25923e671ab2a54a98028))
* semconv CLIENT spans on Flight call sites ([#905](https://github.com/cedricziel/signaldb/issues/905)) ([3047cbb](https://github.com/cedricziel/signaldb/commit/3047cbbc68f03e7d586d4a2caabaa2bd7c660ca1))
* semconv registry, weaver live-check harness, ops docs ([#908](https://github.com/cedricziel/signaldb/issues/908)) ([05f4c52](https://github.com/cedricziel/signaldb/commit/05f4c52fb89d82c3c0dd0321425cad6736652f34))
* semconv self-tracing foundations (resource, span factories, acceptor boundary) ([#903](https://github.com/cedricziel/signaldb/issues/903)) ([dbe4ca2](https://github.com/cedricziel/signaldb/commit/dbe4ca2389ac8db0dba721f66d79db4d0475ed76))
* source-agnostic tenant registry (admin-API tenants queryable without restart) ([#853](https://github.com/cedricziel/signaldb/issues/853)) ([c685935](https://github.com/cedricziel/signaldb/commit/c6859353a739fefcdc45f56cc0c7899193a6086a))
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
* **common:** make `cargo test -p common` compile on its own ([#1087](https://github.com/cedricziel/signaldb/issues/1087)) ([baac410](https://github.com/cedricziel/signaldb/commit/baac410ac8e46c0d4f97e9d75e42e09a390598a6))
* **common:** resolve a tenant's default dataset even without a dataset row ([#1082](https://github.com/cedricziel/signaldb/issues/1082)) ([055733f](https://github.com/cedricziel/signaldb/commit/055733f7e2d0e016091a987836fab2e788540e82))
* **compactor:** bound the rewrite's DataFusion fan-out ([#1067](https://github.com/cedricziel/signaldb/issues/1067)) ([9fc7dde](https://github.com/cedricziel/signaldb/commit/9fc7ddeea7497ce4e63fac2f60b11d77d66c621c)), closes [#1064](https://github.com/cedricziel/signaldb/issues/1064)
* **compactor:** cover profiles in retention, snapshot expiration, and orphan cleanup ([#1021](https://github.com/cedricziel/signaldb/issues/1021)) ([3bcc644](https://github.com/cedricziel/signaldb/commit/3bcc644438874392d75e4f048fa6380614a4e935)), closes [#1014](https://github.com/cedricziel/signaldb/issues/1014)
* **compactor:** decline partitions whose inputs exceed the job budget ([#1069](https://github.com/cedricziel/signaldb/issues/1069)) ([8373ff7](https://github.com/cedricziel/signaldb/commit/8373ff71195a3dedcd11e650a39410bff4fdfe1e))
* **compactor:** derive orphan live-file set from retained snapshots, not snapshot age ([#1007](https://github.com/cedricziel/signaldb/issues/1007)) ([8835c71](https://github.com/cedricziel/signaldb/commit/8835c71335333247d7215f839f7c62d510c3453a))
* **compactor:** re-validate unconditionally before deleting orphans ([#1020](https://github.com/cedricziel/signaldb/issues/1020)) ([5634ab8](https://github.com/cedricziel/signaldb/commit/5634ab820f68d3ed8e24dc4e45ae120dadd15b3b))
* **compactor:** select small files for compaction via max input size ([#934](https://github.com/cedricziel/signaldb/issues/934)) ([#975](https://github.com/cedricziel/signaldb/issues/975)) ([2ea86f8](https://github.com/cedricziel/signaldb/commit/2ea86f875d87be703d552844faaa9734ee0e7b2a))
* **compactor:** use a FairSpillPool for compaction and queries ([#1068](https://github.com/cedricziel/signaldb/issues/1068)) ([6b7bd13](https://github.com/cedricziel/signaldb/commit/6b7bd1368ac4444f785be14b8c29d92629295ee2))
* **conversion:** clamp span duration to zero when end &lt; start ([#927](https://github.com/cedricziel/signaldb/issues/927)) ([#978](https://github.com/cedricziel/signaldb/issues/978)) ([71ad488](https://github.com/cedricziel/signaldb/commit/71ad488aa43fa195592d9a8c9e89f2827dfe92ca))
* **flight:** set explicit gRPC message-size limits and chunk oversized batches ([#990](https://github.com/cedricziel/signaldb/issues/990)) ([6499175](https://github.com/cedricziel/signaldb/commit/6499175d0e6402e1350ad28803d0b08954e43fe1))
* **flight:** stop the client timeout from masking the querier's query deadline ([#919](https://github.com/cedricziel/signaldb/issues/919)) ([46eee38](https://github.com/cedricziel/signaldb/commit/46eee382468bfd6a5f3c34f8404379e55d68a690))
* **iceberg:** backfill metadata pruning properties on pre-existing tables ([#973](https://github.com/cedricziel/signaldb/issues/973)) ([f40fce2](https://github.com/cedricziel/signaldb/commit/f40fce2db23f5e8af79b5fac03e70dd3f2a4ad7b)), closes [#959](https://github.com/cedricziel/signaldb/issues/959)
* **iceberg:** pass S3 storage config explicitly instead of mutating process env ([#948](https://github.com/cedricziel/signaldb/issues/948)) ([#988](https://github.com/cedricziel/signaldb/issues/988)) ([06af739](https://github.com/cedricziel/signaldb/commit/06af73969d302c36be46b90f521ef18688cbecf3))
* **mcp:** add connect and request timeouts to router HTTP client ([#885](https://github.com/cedricziel/signaldb/issues/885)) ([#976](https://github.com/cedricziel/signaldb/issues/976)) ([f0f2182](https://github.com/cedricziel/signaldb/commit/f0f21824b654d57668e2c235f310d3a048a314f4))
* **model:** stop flattening trace hierarchies to root + direct children ([#1018](https://github.com/cedricziel/signaldb/issues/1018)) ([5fee337](https://github.com/cedricziel/signaldb/commit/5fee33711628bf3f041c436c34f363f114ed93fb))
* provision signal tables for every registered dataset, and read an absent one as empty ([#1074](https://github.com/cedricziel/signaldb/issues/1074)) ([9a50ffa](https://github.com/cedricziel/signaldb/commit/9a50ffaa7e404a96cb80d7d3b0cc0850ede00f49))
* restore compactor discovery and WAL pending-gauge accuracy ([#1049](https://github.com/cedricziel/signaldb/issues/1049)) ([b9254b0](https://github.com/cedricziel/signaldb/commit/b9254b065430b092978c2ba8f2e59ec1d3c1ceb8))
* **router:** materialize a tenant's default dataset as a real row ([#1085](https://github.com/cedricziel/signaldb/issues/1085)) ([9443244](https://github.com/cedricziel/signaldb/commit/94432445328a0489bfd0476aaaba12ba937a2561))
* **router:** write the tenant and its default dataset in one transaction ([#1086](https://github.com/cedricziel/signaldb/issues/1086)) ([59bdc70](https://github.com/cedricziel/signaldb/commit/59bdc705d8fddc8253d55466904f59f8f0493060))
* **self-monitoring:** default to parent-based trace sampler ([#843](https://github.com/cedricziel/signaldb/issues/843)) ([d6c12b1](https://github.com/cedricziel/signaldb/commit/d6c12b1aeb060a8438f857451bda61ee0d8828b9))
* **self-monitoring:** stop emitting non-semconv bridge attributes on spans ([#967](https://github.com/cedricziel/signaldb/issues/967)) ([0b82ef4](https://github.com/cedricziel/signaldb/commit/0b82ef4256936e30462e946d62d9452ab1155e5c))
* **telemetry:** emit int-typed registry attributes as i64 ([#1013](https://github.com/cedricziel/signaldb/issues/1013)) ([be67718](https://github.com/cedricziel/signaldb/commit/be677184819e5cbe700d253a03e59cd2bffa7ba8))
* **wal:** bounds-check entry range before reading segment data ([#871](https://github.com/cedricziel/signaldb/issues/871)) ([bc36a94](https://github.com/cedricziel/signaldb/commit/bc36a9493c04ba0f285c05119a54f64ef7e82da5))
* **wal:** carry tenant/dataset/signal on WAL failure telemetry ([#866](https://github.com/cedricziel/signaldb/issues/866)) ([a023dbb](https://github.com/cedricziel/signaldb/commit/a023dbb54822964d44f7c22864391eb2af957a58))
* **wal:** offset-authoritative writes + data-size rotation ([#865](https://github.com/cedricziel/signaldb/issues/865)) ([#883](https://github.com/cedricziel/signaldb/issues/883)) ([31be2cf](https://github.com/cedricziel/signaldb/commit/31be2cfe46f67c56a479fb4b65b1dc5f4412414d))
* **writer:** derive flush scope from request metadata, not the action body ([#897](https://github.com/cedricziel/signaldb/issues/897)) ([cd94186](https://github.com/cedricziel/signaldb/commit/cd9418653c1f90812ffee4a0688dd947039dbbeb))


### Performance Improvements

* **flight,wal:** compress Flight IPC payloads and WAL entries ([#945](https://github.com/cedricziel/signaldb/issues/945)) ([#998](https://github.com/cedricziel/signaldb/issues/998)) ([efb5ef4](https://github.com/cedricziel/signaldb/commit/efb5ef4bc85e2e77483f4546255b50c564015827))
* **flight:** skip redundant discovery lookup and memoize capability discovery ([#940](https://github.com/cedricziel/signaldb/issues/940)) ([#989](https://github.com/cedricziel/signaldb/issues/989)) ([a4720ca](https://github.com/cedricziel/signaldb/commit/a4720ca5d9dc5f541c2dc814cb533934cb023c14))
* **iceberg:** stop carrying useless column bounds in every manifest entry ([#1023](https://github.com/cedricziel/signaldb/issues/1023)) ([3a77a4e](https://github.com/cedricziel/signaldb/commit/3a77a4e513808ae9299e8bf93579e2dbb26b9977))
* **querier:** enable statistics-based file grouping and Parquet filter pushdown ([#937](https://github.com/cedricziel/signaldb/issues/937)) ([#987](https://github.com/cedricziel/signaldb/issues/987)) ([7d4aefb](https://github.com/cedricziel/signaldb/commit/7d4aefb855061ea2a07c6536eee28385a49a6722))
* **wal:** batch index persistence in mark_processed_many ([#943](https://github.com/cedricziel/signaldb/issues/943)) ([#984](https://github.com/cedricziel/signaldb/issues/984)) ([41a91cd](https://github.com/cedricziel/signaldb/commit/41a91cd4938286a39c120e642f0b11261b813ab7))


### Documentation

* flight-communication.md read path now describes the CLIENT hop. ([3047cbb](https://github.com/cedricziel/signaldb/commit/3047cbbc68f03e7d586d4a2caabaa2bd7c660ca1))


### Code Refactoring

* **compactor:** partition-scoped compaction with delta commits ([#1017](https://github.com/cedricziel/signaldb/issues/1017)) ([52dc957](https://github.com/cedricziel/signaldb/commit/52dc9572a10378d6d69f653d1a78a4cf4d2f1407))
* **flight:** decode Flight data dictionary-aware ([#1004](https://github.com/cedricziel/signaldb/issues/1004)) ([94a7a30](https://github.com/cedricziel/signaldb/commit/94a7a30edd81060f2bfc5147dbf3b53307d2de72))
* **iceberg:** configure the catalog pool instead of working around it ([#1024](https://github.com/cedricziel/signaldb/issues/1024)) ([68be19f](https://github.com/cedricziel/signaldb/commit/68be19f7327fc2660a6b45df26c29084fee6ce42))
* **logging:** forbid log:: macros in favor of tracing:: ([#1006](https://github.com/cedricziel/signaldb/issues/1006)) ([071ebb4](https://github.com/cedricziel/signaldb/commit/071ebb47d02f2d6e43ccfb60380c00e3be929248))
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

## [0.3.0](https://github.com/cedricziel/signaldb/compare/compactor-v0.2.1...compactor-v0.3.0) (2026-08-08)


###   BREAKING CHANGES

* **compactor:** [compactor.orphan_cleanup] revalidate_before_delete no longer exists. Note that a leftover key is silently ignored rather than rejected -- the design assumed unknown keys fail config parsing, but neither config struct sets serde(deny_unknown_fields), and adding it is not a safe drive-by because figment's env provider populates the same structs. Documented in the compactor configuration reference; tightening the structs deserves its own change.
* **compactor:** [compactor] min_input_file_size_kb is replaced by max_input_file_size_kb (semantics inverted) and max_files_per_job is removed. No backward-compat alias is provided.

### Features

* **compactor:** reclaim metadata backlog and enable orphan cleanup by default ([#1008](https://github.com/cedricziel/signaldb/issues/1008)) ([908ea79](https://github.com/cedricziel/signaldb/commit/908ea798e78a6d2dd90396f56e584275e9dfc9b3))
* **compactor:** warn on incoherent memory settings and document sizing ([#1081](https://github.com/cedricziel/signaldb/issues/1081)) ([b0a4bb0](https://github.com/cedricziel/signaldb/commit/b0a4bb0740430fad36129b2c40a5c0dc9c2f111d)), closes [#1064](https://github.com/cedricziel/signaldb/issues/1064)
* DB client spans, query stage spans, compactor job spans ([#906](https://github.com/cedricziel/signaldb/issues/906)) ([04a4c4e](https://github.com/cedricziel/signaldb/commit/04a4c4e5788cf6531e0421b50b523b04ac4db38b))
* **iceberg:** tune the Parquet writer properties now that they are honored ([#1025](https://github.com/cedricziel/signaldb/issues/1025)) ([219132a](https://github.com/cedricziel/signaldb/commit/219132a3eb1bba1c15975245081ad4a2d54eb7d1))
* semconv RPC server spans on Flight boundaries ([#904](https://github.com/cedricziel/signaldb/issues/904)) ([a791f45](https://github.com/cedricziel/signaldb/commit/a791f45edf5b1650cc9091d1acf481175060628a))
* source-agnostic tenant registry (admin-API tenants queryable without restart) ([#853](https://github.com/cedricziel/signaldb/issues/853)) ([c685935](https://github.com/cedricziel/signaldb/commit/c6859353a739fefcdc45f56cc0c7899193a6086a))


### Bug Fixes

* address CodeRabbit review on the tenant registry ([#853](https://github.com/cedricziel/signaldb/issues/853) follow-up) ([#855](https://github.com/cedricziel/signaldb/issues/855)) ([d5011ec](https://github.com/cedricziel/signaldb/commit/d5011ecc4a6101c8a51d5944a9480dff8b19d6a8))
* **compactor:** bound the rewrite's DataFusion fan-out ([#1067](https://github.com/cedricziel/signaldb/issues/1067)) ([9fc7dde](https://github.com/cedricziel/signaldb/commit/9fc7ddeea7497ce4e63fac2f60b11d77d66c621c)), closes [#1064](https://github.com/cedricziel/signaldb/issues/1064)
* **compactor:** cover profiles in retention, snapshot expiration, and orphan cleanup ([#1021](https://github.com/cedricziel/signaldb/issues/1021)) ([3bcc644](https://github.com/cedricziel/signaldb/commit/3bcc644438874392d75e4f048fa6380614a4e935)), closes [#1014](https://github.com/cedricziel/signaldb/issues/1014)
* **compactor:** decline partitions whose inputs exceed the job budget ([#1069](https://github.com/cedricziel/signaldb/issues/1069)) ([8373ff7](https://github.com/cedricziel/signaldb/commit/8373ff71195a3dedcd11e650a39410bff4fdfe1e))
* **compactor:** derive orphan live-file set from retained snapshots, not snapshot age ([#1007](https://github.com/cedricziel/signaldb/issues/1007)) ([8835c71](https://github.com/cedricziel/signaldb/commit/8835c71335333247d7215f839f7c62d510c3453a))
* **compactor:** log commit failures with their full cause chain ([#1050](https://github.com/cedricziel/signaldb/issues/1050)) ([61704a0](https://github.com/cedricziel/signaldb/commit/61704a0f327eb20878c6a40c78a7aefee5462443))
* **compactor:** re-validate unconditionally before deleting orphans ([#1020](https://github.com/cedricziel/signaldb/issues/1020)) ([5634ab8](https://github.com/cedricziel/signaldb/commit/5634ab820f68d3ed8e24dc4e45ae120dadd15b3b))
* **compactor:** read partition values from manifest entries, not file paths ([#930](https://github.com/cedricziel/signaldb/issues/930)) ([#991](https://github.com/cedricziel/signaldb/issues/991)) ([2f7e79b](https://github.com/cedricziel/signaldb/commit/2f7e79b86bd5a1884604d9441692b92ac17e665f))
* **compactor:** select small files for compaction via max input size ([#934](https://github.com/cedricziel/signaldb/issues/934)) ([#975](https://github.com/cedricziel/signaldb/issues/975)) ([2ea86f8](https://github.com/cedricziel/signaldb/commit/2ea86f875d87be703d552844faaa9734ee0e7b2a))
* **compactor:** use a FairSpillPool for compaction and queries ([#1068](https://github.com/cedricziel/signaldb/issues/1068)) ([6b7bd13](https://github.com/cedricziel/signaldb/commit/6b7bd1368ac4444f785be14b8c29d92629295ee2))
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

## [0.3.0](https://github.com/cedricziel/signaldb/compare/querier-v0.2.1...querier-v0.3.0) (2026-08-08)


### Features

* DB client spans, query stage spans, compactor job spans ([#906](https://github.com/cedricziel/signaldb/issues/906)) ([04a4c4e](https://github.com/cedricziel/signaldb/commit/04a4c4e5788cf6531e0421b50b523b04ac4db38b))
* **model:** add span events to the Span model ([#847](https://github.com/cedricziel/signaldb/issues/847)) ([0dbd6e8](https://github.com/cedricziel/signaldb/commit/0dbd6e8a0701cea0ce9e46c4fc9456d1562e7d31))
* native Query IR  versioned structured query surface (query-ir-core) ([#882](https://github.com/cedricziel/signaldb/issues/882)) ([8774ac0](https://github.com/cedricziel/signaldb/commit/8774ac0fbbe4686cb7aa8b0bba73dbc25f185689))
* **querier,router:** surface span events on the single-trace path ([#848](https://github.com/cedricziel/signaldb/issues/848)) ([5b344e9](https://github.com/cedricziel/signaldb/commit/5b344e98b6e787aeca35d68bf18ca5ca92657454))
* **querier:** record every do_get failure as a span exception ([#878](https://github.com/cedricziel/signaldb/issues/878)) ([39d76bd](https://github.com/cedricziel/signaldb/commit/39d76bd13a9e92b08b8b55c8dabf62f58863fab7))
* record Flight query failures as span exceptions + surface reasons ([#846](https://github.com/cedricziel/signaldb/issues/846)) ([20d89f5](https://github.com/cedricziel/signaldb/commit/20d89f51eee05ff25ddfa523053dad7ebc8ea6e2))
* semconv CLIENT spans on Flight call sites ([#905](https://github.com/cedricziel/signaldb/issues/905)) ([3047cbb](https://github.com/cedricziel/signaldb/commit/3047cbbc68f03e7d586d4a2caabaa2bd7c660ca1))
* semconv RPC server spans on Flight boundaries ([#904](https://github.com/cedricziel/signaldb/issues/904)) ([a791f45](https://github.com/cedricziel/signaldb/commit/a791f45edf5b1650cc9091d1acf481175060628a))
* source-agnostic tenant registry (admin-API tenants queryable without restart) ([#853](https://github.com/cedricziel/signaldb/issues/853)) ([c685935](https://github.com/cedricziel/signaldb/commit/c6859353a739fefcdc45f56cc0c7899193a6086a))


### Bug Fixes

* address CodeRabbit review on the tenant registry ([#853](https://github.com/cedricziel/signaldb/issues/853) follow-up) ([#855](https://github.com/cedricziel/signaldb/issues/855)) ([d5011ec](https://github.com/cedricziel/signaldb/commit/d5011ecc4a6101c8a51d5944a9480dff8b19d6a8))
* **compactor:** use a FairSpillPool for compaction and queries ([#1068](https://github.com/cedricziel/signaldb/issues/1068)) ([6b7bd13](https://github.com/cedricziel/signaldb/commit/6b7bd1368ac4444f785be14b8c29d92629295ee2))
* **model:** stop flattening trace hierarchies to root + direct children ([#1018](https://github.com/cedricziel/signaldb/issues/1018)) ([5fee337](https://github.com/cedricziel/signaldb/commit/5fee33711628bf3f041c436c34f363f114ed93fb))
* provision signal tables for every registered dataset, and read an absent one as empty ([#1074](https://github.com/cedricziel/signaldb/issues/1074)) ([9a50ffa](https://github.com/cedricziel/signaldb/commit/9a50ffaa7e404a96cb80d7d3b0cc0850ede00f49))
* **querier:** bound, order, and project the trace search scan ([#928](https://github.com/cedricziel/signaldb/issues/928)) ([#985](https://github.com/cedricziel/signaldb/issues/985)) ([b3c94d8](https://github.com/cedricziel/signaldb/commit/b3c94d8a62c06f7f9bca455c7e73e9a24b38f9e6))
* **querier:** reject out-of-range time bounds instead of saturating to a sentinel ([#920](https://github.com/cedricziel/signaldb/issues/920)) ([dc6990e](https://github.com/cedricziel/signaldb/commit/dc6990eb72d99cb23185faf2a373b2a22e403a93))


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


### Tests

* polish medium/low audit findings across the workspace ([#969](https://github.com/cedricziel/signaldb/issues/969)) ([8962f6d](https://github.com/cedricziel/signaldb/commit/8962f6d1d22c8a176d4a1d99376d61b42b1da258))
</details>

<details><summary>router: 0.3.0</summary>

## [0.3.0](https://github.com/cedricziel/signaldb/compare/router-v0.2.2...router-v0.3.0) (2026-08-08)


###   BREAKING CHANGES

* **cli+mcp:** signaldb-cli tenant/api-key/dataset commands move under `admin` (e.g. `signaldb-cli admin tenant list`), and queries now require a language flag (`signaldb-cli query --sql|--promql|--logql|--traceql|--ir`). No back-compat aliases are provided (post-1.0).

### Features

* **api:** code-first OpenAPI  generate spec + Rust/TS clients from annotations ([#856](https://github.com/cedricziel/signaldb/issues/856)) ([e34fbfb](https://github.com/cedricziel/signaldb/commit/e34fbfbd094034416f78597c59b306975dd97271))
* **api:** document Tempo trace query endpoints in OpenAPI + SDK ([#861](https://github.com/cedricziel/signaldb/issues/861)) ([a1e0d7f](https://github.com/cedricziel/signaldb/commit/a1e0d7f9f3c355f8bf73da686db1952487c3e046))
* **cli+mcp:** CLI & MCP as pure SDK consumers  query --&lt;lang&gt;, admin grouping (Phase 1) ([#892](https://github.com/cedricziel/signaldb/issues/892)) ([92a439e](https://github.com/cedricziel/signaldb/commit/92a439e112da96029733d93db7f274c20c29cbc5))
* **logs:** surface trace_id/span_id in log query responses ([#1048](https://github.com/cedricziel/signaldb/issues/1048)) ([5a84a04](https://github.com/cedricziel/signaldb/commit/5a84a04b3582befd76ea5f231b887f2cbed253ea))
* **mcp:** OAuth 2.1 + DCR connector support for Claude and OpenAI ([#899](https://github.com/cedricziel/signaldb/issues/899)) ([4d0104a](https://github.com/cedricziel/signaldb/commit/4d0104a608ee392e9b25acf686dcd7359fc37631))
* metric/label discovery (MCP+CLI+SDK) and prom/loki UI migration ([#1041](https://github.com/cedricziel/signaldb/issues/1041)) ([afcc72e](https://github.com/cedricziel/signaldb/commit/afcc72e9f87a45e74c97171e8919b90868cd54f4))
* **model:** add span events to the Span model ([#847](https://github.com/cedricziel/signaldb/issues/847)) ([0dbd6e8](https://github.com/cedricziel/signaldb/commit/0dbd6e8a0701cea0ce9e46c4fc9456d1562e7d31))
* native Query IR  versioned structured query surface (query-ir-core) ([#882](https://github.com/cedricziel/signaldb/issues/882)) ([8774ac0](https://github.com/cedricziel/signaldb/commit/8774ac0fbbe4686cb7aa8b0bba73dbc25f185689))
* **querier,router:** surface span events on the single-trace path ([#848](https://github.com/cedricziel/signaldb/issues/848)) ([5b344e9](https://github.com/cedricziel/signaldb/commit/5b344e98b6e787aeca35d68bf18ca5ca92657454))
* record Flight query failures as span exceptions + surface reasons ([#846](https://github.com/cedricziel/signaldb/issues/846)) ([20d89f5](https://github.com/cedricziel/signaldb/commit/20d89f51eee05ff25ddfa523053dad7ebc8ea6e2))
* return server trace context and timings on HTTP responses (Server-Timing + traceresponse) ([#918](https://github.com/cedricziel/signaldb/issues/918)) ([453dd20](https://github.com/cedricziel/signaldb/commit/453dd2050eee95f3daf1c96f77e56964e99a2bb1))
* **sdk:** query surface  SDK covers PromQL/LogQL/TraceQL + Flight SQL (Phase 0) ([#890](https://github.com/cedricziel/signaldb/issues/890)) ([1fde946](https://github.com/cedricziel/signaldb/commit/1fde946cc308ef134f01492b72a3fc874e1c8f95))
* **self-monitoring:** runtime-configurable browser telemetry export ([#842](https://github.com/cedricziel/signaldb/issues/842)) ([343b928](https://github.com/cedricziel/signaldb/commit/343b92877d1291406de25923e671ab2a54a98028))
* semconv CLIENT spans on Flight call sites ([#905](https://github.com/cedricziel/signaldb/issues/905)) ([3047cbb](https://github.com/cedricziel/signaldb/commit/3047cbbc68f03e7d586d4a2caabaa2bd7c660ca1))


### Bug Fixes

* **flight:** stop the client timeout from masking the querier's query deadline ([#919](https://github.com/cedricziel/signaldb/issues/919)) ([46eee38](https://github.com/cedricziel/signaldb/commit/46eee382468bfd6a5f3c34f8404379e55d68a690))
* **model:** stop flattening trace hierarchies to root + direct children ([#1018](https://github.com/cedricziel/signaldb/issues/1018)) ([5fee337](https://github.com/cedricziel/signaldb/commit/5fee33711628bf3f041c436c34f363f114ed93fb))
* provision signal tables for every registered dataset, and read an absent one as empty ([#1074](https://github.com/cedricziel/signaldb/issues/1074)) ([9a50ffa](https://github.com/cedricziel/signaldb/commit/9a50ffaa7e404a96cb80d7d3b0cc0850ede00f49))
* restore compactor discovery and WAL pending-gauge accuracy ([#1049](https://github.com/cedricziel/signaldb/issues/1049)) ([b9254b0](https://github.com/cedricziel/signaldb/commit/b9254b065430b092978c2ba8f2e59ec1d3c1ceb8))
* **router:** bound Tempo tag-values queries by time window ([#929](https://github.com/cedricziel/signaldb/issues/929)) ([#979](https://github.com/cedricziel/signaldb/issues/979)) ([7cc301a](https://github.com/cedricziel/signaldb/commit/7cc301adc539a77540682d155425bace30ddc803))
* **router:** materialize a tenant's default dataset as a real row ([#1085](https://github.com/cedricziel/signaldb/issues/1085)) ([9443244](https://github.com/cedricziel/signaldb/commit/94432445328a0489bfd0476aaaba12ba937a2561))
* **router:** return error bodies with messages from signal endpoints ([#921](https://github.com/cedricziel/signaldb/issues/921)) ([#980](https://github.com/cedricziel/signaldb/issues/980)) ([39f50ee](https://github.com/cedricziel/signaldb/commit/39f50eed98715ad3fa2b0a02fcac6dbee68161eb))
* **router:** write the tenant and its default dataset in one transaction ([#1086](https://github.com/cedricziel/signaldb/issues/1086)) ([59bdc70](https://github.com/cedricziel/signaldb/commit/59bdc705d8fddc8253d55466904f59f8f0493060))


### Performance Improvements

* CPU target features and jemalloc allocator for release builds ([#970](https://github.com/cedricziel/signaldb/issues/970)) ([766e2d1](https://github.com/cedricziel/signaldb/commit/766e2d1c82dad65a674184edaf2e8d67cb4083dd))
* **flight,wal:** compress Flight IPC payloads and WAL entries ([#945](https://github.com/cedricziel/signaldb/issues/945)) ([#998](https://github.com/cedricziel/signaldb/issues/998)) ([efb5ef4](https://github.com/cedricziel/signaldb/commit/efb5ef4bc85e2e77483f4546255b50c564015827))


### Documentation

* flight-communication.md read path now describes the CLIENT hop. ([3047cbb](https://github.com/cedricziel/signaldb/commit/3047cbbc68f03e7d586d4a2caabaa2bd7c660ca1))


### Code Refactoring

* **cli:** make signaldb-cli depend only on the SDK (+ create_user API) ([#874](https://github.com/cedricziel/signaldb/issues/874)) ([8e5cce5](https://github.com/cedricziel/signaldb/commit/8e5cce56c821d69917b55cc8c21a9a2ef55864b7))
* **flight:** decode Flight data dictionary-aware ([#1004](https://github.com/cedricziel/signaldb/issues/1004)) ([94a7a30](https://github.com/cedricziel/signaldb/commit/94a7a30edd81060f2bfc5147dbf3b53307d2de72))
* **logging:** forbid log:: macros in favor of tracing:: ([#1006](https://github.com/cedricziel/signaldb/issues/1006)) ([071ebb4](https://github.com/cedricziel/signaldb/commit/071ebb47d02f2d6e43ccfb60380c00e3be929248))
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

## [0.3.0](https://github.com/cedricziel/signaldb/compare/signaldb-bin-v0.1.3...signaldb-bin-v0.3.0) (2026-08-08)


### Features

* **api:** code-first OpenAPI  generate spec + Rust/TS clients from annotations ([#856](https://github.com/cedricziel/signaldb/issues/856)) ([e34fbfb](https://github.com/cedricziel/signaldb/commit/e34fbfbd094034416f78597c59b306975dd97271))
* **self-monitoring:** runtime-configurable browser telemetry export ([#842](https://github.com/cedricziel/signaldb/issues/842)) ([343b928](https://github.com/cedricziel/signaldb/commit/343b92877d1291406de25923e671ab2a54a98028))
* source-agnostic tenant registry (admin-API tenants queryable without restart) ([#853](https://github.com/cedricziel/signaldb/issues/853)) ([c685935](https://github.com/cedricziel/signaldb/commit/c6859353a739fefcdc45f56cc0c7899193a6086a))
* **writer:** coalesce Iceberg commits with a per-table floor + force-commit primitive ([#891](https://github.com/cedricziel/signaldb/issues/891)) ([ad47bb6](https://github.com/cedricziel/signaldb/commit/ad47bb6867dd5cf622701b5778ef9f94e7b60923))
* zero-config first boot  auto-provision default tenant and print API key once ([#995](https://github.com/cedricziel/signaldb/issues/995)) ([5116c8d](https://github.com/cedricziel/signaldb/commit/5116c8d9f22950447373f74c99b17488900db00d)), closes [#796](https://github.com/cedricziel/signaldb/issues/796)


### Bug Fixes

* **flight:** set explicit gRPC message-size limits and chunk oversized batches ([#990](https://github.com/cedricziel/signaldb/issues/990)) ([6499175](https://github.com/cedricziel/signaldb/commit/6499175d0e6402e1350ad28803d0b08954e43fe1))
* **monolith:** run the full compactor lifecycle loop, not just planning ([#1005](https://github.com/cedricziel/signaldb/issues/1005)) ([2e751fb](https://github.com/cedricziel/signaldb/commit/2e751fb5849ce596f3dca7366624ee65e4def3ac))
* provision signal tables for every registered dataset, and read an absent one as empty ([#1074](https://github.com/cedricziel/signaldb/issues/1074)) ([9a50ffa](https://github.com/cedricziel/signaldb/commit/9a50ffaa7e404a96cb80d7d3b0cc0850ede00f49))
* **router:** materialize a tenant's default dataset as a real row ([#1085](https://github.com/cedricziel/signaldb/issues/1085)) ([9443244](https://github.com/cedricziel/signaldb/commit/94432445328a0489bfd0476aaaba12ba937a2561))


### Performance Improvements

* CPU target features and jemalloc allocator for release builds ([#970](https://github.com/cedricziel/signaldb/issues/970)) ([766e2d1](https://github.com/cedricziel/signaldb/commit/766e2d1c82dad65a674184edaf2e8d67cb4083dd))
* **flight,wal:** compress Flight IPC payloads and WAL entries ([#945](https://github.com/cedricziel/signaldb/issues/945)) ([#998](https://github.com/cedricziel/signaldb/issues/998)) ([efb5ef4](https://github.com/cedricziel/signaldb/commit/efb5ef4bc85e2e77483f4546255b50c564015827))


### Code Refactoring

* **logging:** forbid log:: macros in favor of tracing:: ([#1006](https://github.com/cedricziel/signaldb/issues/1006)) ([071ebb4](https://github.com/cedricziel/signaldb/commit/071ebb47d02f2d6e43ccfb60380c00e3be929248))
</details>

<details><summary>signaldb-cli: 0.3.0</summary>

## [0.3.0](https://github.com/cedricziel/signaldb/compare/signaldb-cli-v0.1.3...signaldb-cli-v0.3.0) (2026-08-08)


###   BREAKING CHANGES

* **cli+mcp:** signaldb-cli tenant/api-key/dataset commands move under `admin` (e.g. `signaldb-cli admin tenant list`), and queries now require a language flag (`signaldb-cli query --sql|--promql|--logql|--traceql|--ir`). No back-compat aliases are provided (post-1.0).

### Features

* **api:** code-first OpenAPI  generate spec + Rust/TS clients from annotations ([#856](https://github.com/cedricziel/signaldb/issues/856)) ([e34fbfb](https://github.com/cedricziel/signaldb/commit/e34fbfbd094034416f78597c59b306975dd97271))
* **cli+mcp:** CLI & MCP as pure SDK consumers  query --&lt;lang&gt;, admin grouping (Phase 1) ([#892](https://github.com/cedricziel/signaldb/issues/892)) ([92a439e](https://github.com/cedricziel/signaldb/commit/92a439e112da96029733d93db7f274c20c29cbc5))
* metric/label discovery (MCP+CLI+SDK) and prom/loki UI migration ([#1041](https://github.com/cedricziel/signaldb/issues/1041)) ([afcc72e](https://github.com/cedricziel/signaldb/commit/afcc72e9f87a45e74c97171e8919b90868cd54f4))
* native Query IR  versioned structured query surface (query-ir-core) ([#882](https://github.com/cedricziel/signaldb/issues/882)) ([8774ac0](https://github.com/cedricziel/signaldb/commit/8774ac0fbbe4686cb7aa8b0bba73dbc25f185689))


### Code Refactoring

* **cli:** make signaldb-cli depend only on the SDK (+ create_user API) ([#874](https://github.com/cedricziel/signaldb/issues/874)) ([8e5cce5](https://github.com/cedricziel/signaldb/commit/8e5cce56c821d69917b55cc8c21a9a2ef55864b7))


### Tests

* delete tautological tests and rewrite salvageable ones as contract tests ([#961](https://github.com/cedricziel/signaldb/issues/961)) ([b3e884a](https://github.com/cedricziel/signaldb/commit/b3e884ad59b4df853429133d5eef2724a8adcada))
* exercise real implementations instead of test-local copies ([#964](https://github.com/cedricziel/signaldb/issues/964)) ([e142b3d](https://github.com/cedricziel/signaldb/commit/e142b3d006065205c7194fd22c4ca4e182402f55))
* make tests assert what their names promise ([#966](https://github.com/cedricziel/signaldb/issues/966)) ([446ed06](https://github.com/cedricziel/signaldb/commit/446ed062a7480902ef391884b1c2e12f77ddd66f))
* polish medium/low audit findings across the workspace ([#969](https://github.com/cedricziel/signaldb/issues/969)) ([8962f6d](https://github.com/cedricziel/signaldb/commit/8962f6d1d22c8a176d4a1d99376d61b42b1da258))
* replace sleep-based synchronization with deterministic waits ([#968](https://github.com/cedricziel/signaldb/issues/968)) ([6391326](https://github.com/cedricziel/signaldb/commit/6391326013c8620f186e4a63c2cdf3bbdf9ee963))
</details>

<details><summary>writer: 0.3.0</summary>

## [0.3.0](https://github.com/cedricziel/signaldb/compare/writer-v0.2.1...writer-v0.3.0) (2026-08-08)


### Features

* semconv RPC server spans on Flight boundaries ([#904](https://github.com/cedricziel/signaldb/issues/904)) ([a791f45](https://github.com/cedricziel/signaldb/commit/a791f45edf5b1650cc9091d1acf481175060628a))
* **writer:** ack ingest on WAL flush, commit to Iceberg asynchronously ([#893](https://github.com/cedricziel/signaldb/issues/893)) ([fffdbb1](https://github.com/cedricziel/signaldb/commit/fffdbb109c48893bb2725a8afd3e2e740968a152))
* **writer:** bound Iceberg metadata growth via delete-after-commit ([#895](https://github.com/cedricziel/signaldb/issues/895)) ([35ce5c7](https://github.com/cedricziel/signaldb/commit/35ce5c7aa18aa4f12d3e62c4f34221c849f973f3))
* **writer:** coalesce Iceberg commits with a per-table floor + force-commit primitive ([#891](https://github.com/cedricziel/signaldb/issues/891)) ([ad47bb6](https://github.com/cedricziel/signaldb/commit/ad47bb6867dd5cf622701b5778ef9f94e7b60923))


### Bug Fixes

* **acceptor:** dead-letter writer-rejected WAL entries instead of wedging the retry pass ([#1063](https://github.com/cedricziel/signaldb/issues/1063)) ([7fc6ada](https://github.com/cedricziel/signaldb/commit/7fc6ada1ea922784220789f304fb3f8448ff8ef1)), closes [#1060](https://github.com/cedricziel/signaldb/issues/1060)
* **common:** resolve a tenant's default dataset even without a dataset row ([#1082](https://github.com/cedricziel/signaldb/issues/1082)) ([055733f](https://github.com/cedricziel/signaldb/commit/055733f7e2d0e016091a987836fab2e788540e82))
* provision signal tables for every registered dataset, and read an absent one as empty ([#1074](https://github.com/cedricziel/signaldb/issues/1074)) ([9a50ffa](https://github.com/cedricziel/signaldb/commit/9a50ffaa7e404a96cb80d7d3b0cc0850ede00f49))
* **telemetry:** emit int-typed registry attributes as i64 ([#1013](https://github.com/cedricziel/signaldb/issues/1013)) ([be67718](https://github.com/cedricziel/signaldb/commit/be677184819e5cbe700d253a03e59cd2bffa7ba8))
* **wal:** carry tenant/dataset/signal on WAL failure telemetry ([#866](https://github.com/cedricziel/signaldb/issues/866)) ([a023dbb](https://github.com/cedricziel/signaldb/commit/a023dbb54822964d44f7c22864391eb2af957a58))
* **writer:** derive flush scope from request metadata, not the action body ([#897](https://github.com/cedricziel/signaldb/issues/897)) ([cd94186](https://github.com/cedricziel/signaldb/commit/cd9418653c1f90812ffee4a0688dd947039dbbeb))


### Performance Improvements

* CPU target features and jemalloc allocator for release builds ([#970](https://github.com/cedricziel/signaldb/issues/970)) ([766e2d1](https://github.com/cedricziel/signaldb/commit/766e2d1c82dad65a674184edaf2e8d67cb4083dd))
* **flight,wal:** compress Flight IPC payloads and WAL entries ([#945](https://github.com/cedricziel/signaldb/issues/945)) ([#998](https://github.com/cedricziel/signaldb/issues/998)) ([efb5ef4](https://github.com/cedricziel/signaldb/commit/efb5ef4bc85e2e77483f4546255b50c564015827))
* **wal:** batch index persistence in mark_processed_many ([#943](https://github.com/cedricziel/signaldb/issues/943)) ([#984](https://github.com/cedricziel/signaldb/issues/984)) ([41a91cd](https://github.com/cedricziel/signaldb/commit/41a91cd4938286a39c120e642f0b11261b813ab7))


### Code Refactoring

* **flight:** decode Flight data dictionary-aware ([#1004](https://github.com/cedricziel/signaldb/issues/1004)) ([94a7a30](https://github.com/cedricziel/signaldb/commit/94a7a30edd81060f2bfc5147dbf3b53307d2de72))
* **logging:** forbid log:: macros in favor of tracing:: ([#1006](https://github.com/cedricziel/signaldb/issues/1006)) ([071ebb4](https://github.com/cedricziel/signaldb/commit/071ebb47d02f2d6e43ccfb60380c00e3be929248))
* span hygiene sweep and construction guard ([#907](https://github.com/cedricziel/signaldb/issues/907)) ([c1f7b81](https://github.com/cedricziel/signaldb/commit/c1f7b81fbc00ae5fd6c9b948f9fb35c9d5a27d26))


### Tests

* delete tautological tests and rewrite salvageable ones as contract tests ([#961](https://github.com/cedricziel/signaldb/issues/961)) ([b3e884a](https://github.com/cedricziel/signaldb/commit/b3e884ad59b4df853429133d5eef2724a8adcada))
* make swallow-and-fallback integration tests fail on real failures ([#965](https://github.com/cedricziel/signaldb/issues/965)) ([a6720ba](https://github.com/cedricziel/signaldb/commit/a6720ba4d84b933e59f14490a2aca41f19d38779))
* polish medium/low audit findings across the workspace ([#969](https://github.com/cedricziel/signaldb/issues/969)) ([8962f6d](https://github.com/cedricziel/signaldb/commit/8962f6d1d22c8a176d4a1d99376d61b42b1da258))
</details>

---
This PR was generated with [Release Please](https://github.com/googleapis/release-please). See [documentation](https://github.com/googleapis/release-please#release-please).