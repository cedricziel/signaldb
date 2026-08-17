# Changelog

## [0.2.0](https://github.com/cedricziel/signaldb/compare/mcp-server-v0.1.0...mcp-server-v0.2.0) (2026-08-17)


### ⚠ BREAKING CHANGES

* **auth:** POST /api/v1/admin/tenants/{id}/api-keys requires a non-empty `scopes` array; bodies without it are rejected.
* **cli+mcp:** signaldb-cli tenant/api-key/dataset commands move under `admin` (e.g. `signaldb-cli admin tenant list`), and queries now require a language flag (`signaldb-cli query --sql|--promql|--logql|--traceql|--ir`). No back-compat aliases are provided (post-1.0).

### Features

* **auth:** schema:read/schema:write API-key scopes, scopes on every key surface ([#1217](https://github.com/cedricziel/signaldb/issues/1217)) ([34c7a28](https://github.com/cedricziel/signaldb/commit/34c7a28e4e62fad7a05089c1a3543739d6e28450))
* **auth:** tenant:manage API-key scope for the tenant management API ([#1266](https://github.com/cedricziel/signaldb/issues/1266)) ([9dfc193](https://github.com/cedricziel/signaldb/commit/9dfc193a85e813b42f8658bf97cbfd30e3b78f2e))
* **cli+mcp:** CLI & MCP as pure SDK consumers — query --&lt;lang&gt;, admin grouping (Phase 1) ([#892](https://github.com/cedricziel/signaldb/issues/892)) ([92a439e](https://github.com/cedricziel/signaldb/commit/92a439e112da96029733d93db7f274c20c29cbc5))
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
