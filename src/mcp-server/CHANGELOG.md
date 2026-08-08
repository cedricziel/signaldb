# Changelog

## [0.2.0](https://github.com/cedricziel/signaldb/compare/mcp-server-v0.1.0...mcp-server-v0.2.0) (2026-08-08)


### ⚠ BREAKING CHANGES

* **cli+mcp:** signaldb-cli tenant/api-key/dataset commands move under `admin` (e.g. `signaldb-cli admin tenant list`), and queries now require a language flag (`signaldb-cli query --sql|--promql|--logql|--traceql|--ir`). No back-compat aliases are provided (post-1.0).

### Features

* **cli+mcp:** CLI & MCP as pure SDK consumers — query --&lt;lang&gt;, admin grouping (Phase 1) ([#892](https://github.com/cedricziel/signaldb/issues/892)) ([92a439e](https://github.com/cedricziel/signaldb/commit/92a439e112da96029733d93db7f274c20c29cbc5))
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
