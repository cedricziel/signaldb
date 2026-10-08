# Changelog

## [0.3.0](https://github.com/cedricziel/signaldb/compare/mcp-server-v0.2.2...mcp-server-v0.3.0) (2026-10-08)


### ⚠ BREAKING CHANGES

* **traceql:** `Condition` has a new public `op` field, so code that constructs one must set it.
* **mcp-server:** discover_attributes and discover_metrics return the Query IR metadata envelope (logical dotted names, levels, cost) instead of a Tempo tag list, a Loki/Prometheus {status,data} array, or Pyroscope {names}. scope narrows by attribute level rather than routing to the Tempo v2 endpoints, and tag values need a declared set, a statistics sketch, or sample: true.
* `"from": "metrics_histogram"` is rejected as an unknown source. Use `"from": "metrics"`; add a `metric.type = histogram` filter where only histogram rows are wanted. `histogram_quantile` on `metrics` already reads histogram rows only.

### Features

* **cli,mcp:** follow a Query IR live tail through the IR endpoint ([#2150](https://github.com/cedricziel/signaldb/issues/2150)) ([1219d9d](https://github.com/cedricziel/signaldb/commit/1219d9d971107f485ffb7a4737bda901eceaedc0))
* **cli,mcp:** page Query IR results through the IR endpoint ([#2144](https://github.com/cedricziel/signaldb/issues/2144)) ([9fc147d](https://github.com/cedricziel/signaldb/commit/9fc147d416a5329a31040af566424f40db3e5f6c))
* **evals:** build eval sets from traces ([#1847](https://github.com/cedricziel/signaldb/issues/1847)) ([dfe9e60](https://github.com/cedricziel/signaldb/commit/dfe9e608f9629abee1a542f5020f164d193b8faf))
* **evals:** list and compare eval runs from MCP and the CLI ([#1866](https://github.com/cedricziel/signaldb/issues/1866)) ([665f458](https://github.com/cedricziel/signaldb/commit/665f4588808aad9ae567df358c7732f70881aec9))
* **evals:** upload eval results and gate CI on them ([#1857](https://github.com/cedricziel/signaldb/issues/1857)) ([175155a](https://github.com/cedricziel/signaldb/commit/175155a3a2d958d59e16a05961206c0b09f1e79a))
* **mcp-server:** discover attributes and metrics through the Query IR ([#2077](https://github.com/cedricziel/signaldb/issues/2077)) ([37d58ea](https://github.com/cedricziel/signaldb/commit/37d58eac6e5ac2b3766b324e494f0c4901812a5a))
* **mcp:** add a list_services tool ([#2180](https://github.com/cedricziel/signaldb/issues/2180)) ([1e048d5](https://github.com/cedricziel/signaldb/commit/1e048d5a2da962559256a17d5311bdcf26760450)), closes [#2172](https://github.com/cedricziel/signaldb/issues/2172)
* **mcp:** serve the query-ir reference as fetchable sections ([#2179](https://github.com/cedricziel/signaldb/issues/2179)) ([afc4ff2](https://github.com/cedricziel/signaldb/commit/afc4ff28e47bd8b69c227fc35cb8a556e8482527)), closes [#2173](https://github.com/cedricziel/signaldb/issues/2173)
* **processors:** diff Test panel output against the server's decoded input ([#1883](https://github.com/cedricziel/signaldb/issues/1883)) ([7711bc5](https://github.com/cedricziel/signaldb/commit/7711bc5bca22d010fb83766449d52ccdd44291e0))
* **querier:** add the exemplars IR source over metric_exemplars ([#1946](https://github.com/cedricziel/signaldb/issues/1946)) ([5c2c348](https://github.com/cedricziel/signaldb/commit/5c2c3484fc7e4f25219ab05e0813ca0b9c4e3c0c))
* recognise AI agents as the gen_ai.agent entity ([#1803](https://github.com/cedricziel/signaldb/issues/1803)) ([e0fcaa0](https://github.com/cedricziel/signaldb/commit/e0fcaa0b3cd8133e23e0ccc8f9e28e43986c41f5))
* remove the metrics_histogram IR source ([#1945](https://github.com/cedricziel/signaldb/issues/1945)) ([c16f82c](https://github.com/cedricziel/signaldb/commit/c16f82c100199ef0f297e366208c07d8242501c5))
* **router:** eval sets API for offline agent evals ([#1837](https://github.com/cedricziel/signaldb/issues/1837)) ([b3ce35a](https://github.com/cedricziel/signaldb/commit/b3ce35a7fb9671494e4d78cd29b2673f20205531))
* **router:** list discovery fields with their canonical authority type ([#2075](https://github.com/cedricziel/signaldb/issues/2075)) ([621aa46](https://github.com/cedricziel/signaldb/commit/621aa4674399a628470b73810e033963288a3866))
* **router:** publish UI login/logout and the full whoami response in OpenAPI ([#2110](https://github.com/cedricziel/signaldb/issues/2110)) ([664c8fe](https://github.com/cedricziel/signaldb/commit/664c8fe52a51505ef5fc46a63293e1e35a1a1913))
* **router:** type the Query IR request pipeline as IrStage ([#2095](https://github.com/cedricziel/signaldb/issues/2095)) ([414ed92](https://github.com/cedricziel/signaldb/commit/414ed92b136367e2650b1c8de8f0cd12250cce5b))
* show span links in the Query IR, MCP get_trace and the trace view ([#2177](https://github.com/cedricziel/signaldb/issues/2177)) ([b873eab](https://github.com/cedricziel/signaldb/commit/b873eabee7f7e4e836a530a3fc75fe5261cafb49))
* **traceql:** support !=, =~ and !~ and surface search errors over MCP ([#2178](https://github.com/cedricziel/signaldb/issues/2178)) ([ce929f5](https://github.com/cedricziel/signaldb/commit/ce929f5f321e5d826e2ba43edc67d7e26de67659))


### Bug Fixes

* **mcp:** name every missing field in query_ir document errors ([#1798](https://github.com/cedricziel/signaldb/issues/1798)) ([bcf8804](https://github.com/cedricziel/signaldb/commit/bcf8804e36438aee133171d11a16a46557d6eacf))
* **mcp:** read get_trace over the Query IR ([#1810](https://github.com/cedricziel/signaldb/issues/1810)) ([8032175](https://github.com/cedricziel/signaldb/commit/8032175d56649cebfd078f9ce3a361f6e60c1d8f))
* **mcp:** report profiles_for_trace durations and cover the tool end to end ([#2131](https://github.com/cedricziel/signaldb/issues/2131)) ([760bcbb](https://github.com/cedricziel/signaldb/commit/760bcbbe5c251560e4b3928d8949157ad942bded))
* **mcp:** serve sessionless requests from clients that skip initialize ([#2198](https://github.com/cedricziel/signaldb/issues/2198)) ([b6387f5](https://github.com/cedricziel/signaldb/commit/b6387f58f4f8c9036f99a9bdfaf68d32351b76fc))
* **query-ir:** resolve trace.id/span.id on traces and logs, name bad predicates over MCP ([#2206](https://github.com/cedricziel/signaldb/issues/2206)) ([b462073](https://github.com/cedricziel/signaldb/commit/b462073340a278c6ac5f4335afe4f18b5398ebce))
* **router:** read data for sample:true and flag partial discovery statistics ([#2176](https://github.com/cedricziel/signaldb/issues/2176)) ([ab5dcf7](https://github.com/cedricziel/signaldb/commit/ab5dcf78f47567a6e2a8f8a10dcf5dfd06cdfed5))


### Code Refactoring

* **mcp-server:** compare profiles through the Query IR baseline ([#2103](https://github.com/cedricziel/signaldb/issues/2103)) ([153704e](https://github.com/cedricziel/signaldb/commit/153704eb82aa6955f197ec354f9abcb5bc0b3d7e))
* **mcp-server:** complete prompt arguments through the Query IR ([#2097](https://github.com/cedricziel/signaldb/issues/2097)) ([3ab363d](https://github.com/cedricziel/signaldb/commit/3ab363d67579e4d75cb3bb551f5325ae270938bc))
* **mcp-server:** discover profile types through the Query IR ([#2101](https://github.com/cedricziel/signaldb/issues/2101)) ([b6ff25a](https://github.com/cedricziel/signaldb/commit/b6ff25a19b4e4d0578231c9ceeec249d88237d35))
* **mcp-server:** list a trace's profiles through the Query IR ([#2104](https://github.com/cedricziel/signaldb/issues/2104)) ([a351006](https://github.com/cedricziel/signaldb/commit/a351006e33b8cdba9c06c319c1fa5e53c26b6428))
* **mcp-server:** search profiles through the Query IR flamegraph ([#2102](https://github.com/cedricziel/signaldb/issues/2102)) ([26db597](https://github.com/cedricziel/signaldb/commit/26db597571d076b420eb4011510e5189f3e2cfb8))


### Build System

* **deps:** bump object from 0.37.3 to 0.39.1 ([#2203](https://github.com/cedricziel/signaldb/issues/2203)) ([b6de77a](https://github.com/cedricziel/signaldb/commit/b6de77a4378388901382c517ed72e7a93c309c48))
* fix the beta test leg for cargo's unused-dependency lints ([#2047](https://github.com/cedricziel/signaldb/issues/2047)) ([6867d69](https://github.com/cedricziel/signaldb/commit/6867d69dceb26f2e55ccfac31e60ae42aad76418))

## [0.2.2](https://github.com/cedricziel/signaldb/compare/mcp-server-v0.2.1...mcp-server-v0.2.2) (2026-09-23)


### Features

* GitHub App integration for connecting a tenant's repositories ([#1600](https://github.com/cedricziel/signaldb/issues/1600)) ([6c9721e](https://github.com/cedricziel/signaldb/commit/6c9721ef0cf630df85e227a07be6ae30ee263191))
* **mcp-server:** add skill:// resource for query_ir guidance ([#1549](https://github.com/cedricziel/signaldb/issues/1549)) ([ff173a2](https://github.com/cedricziel/signaldb/commit/ff173a24508ad01500e5182ebe1a5c73ffc91bfa))
* **mcp:** add optional ui_base_url config for MCP server ([#1551](https://github.com/cedricziel/signaldb/issues/1551)) ([a90c8e0](https://github.com/cedricziel/signaldb/commit/a90c8e0cecbae181bcced4bc301ab619fc2a7352))
* **mcp:** add search_trace_groups tool ([#1556](https://github.com/cedricziel/signaldb/issues/1556)) ([3575a73](https://github.com/cedricziel/signaldb/commit/3575a736724b4f27e2fc87b06d3fc41cc2351ec1))
* **mcp:** deep-link enrichment for search_traces, get_trace, search_logs ([#1554](https://github.com/cedricziel/signaldb/issues/1554)) ([2bb94a7](https://github.com/cedricziel/signaldb/commit/2bb94a7c041277cd641f5c6635dfba68fb8bfba5))
* **mcp:** serve skills as a discoverable catalog ([#1628](https://github.com/cedricziel/signaldb/issues/1628)) ([6462251](https://github.com/cedricziel/signaldb/commit/6462251b4e8437f6e70d1753144d440e7d1b4dc1))
* multi-tenant MCP OAuth grants ([#1541](https://github.com/cedricziel/signaldb/issues/1541)) ([c5b49b0](https://github.com/cedricziel/signaldb/commit/c5b49b018f749a72b639366a18223081cecef7cc))
* per-API-key allowed origins for browser (CORS) ingestion ([#1548](https://github.com/cedricziel/signaldb/issues/1548)) ([6e966dd](https://github.com/cedricziel/signaldb/commit/6e966ddaf2740e3648583223828c6af715b6d331))
* per-tenant, per-dataset OTTL telemetry processors ([#1603](https://github.com/cedricziel/signaldb/issues/1603)) ([2fc1022](https://github.com/cedricziel/signaldb/commit/2fc102232b1d925418b02e68393af8917184016e))
* **router:** attach an existing GitHub App installation to a tenant ([#1618](https://github.com/cedricziel/signaldb/issues/1618)) ([0ab8e95](https://github.com/cedricziel/signaldb/commit/0ab8e95581f5213b02e8cded8af5b2b71c827516))
* source context for stack frames from linked GitHub repositories ([#1601](https://github.com/cedricziel/signaldb/issues/1601)) ([acca49c](https://github.com/cedricziel/signaldb/commit/acca49ca770ab96464b12676211144bc20b5cc7c))


### Bug Fixes

* **mcp:** forward the selected tenant for multi-tenant credentials ([#1604](https://github.com/cedricziel/signaldb/issues/1604)) ([2bd6d81](https://github.com/cedricziel/signaldb/commit/2bd6d815b46d1aaffc46d6e442d04e24bdae3fab))

## [0.2.1](https://github.com/cedricziel/signaldb/compare/mcp-server-v0.2.0...mcp-server-v0.2.1) (2026-09-12)


### Features

* implement multi-dataset restriction for API keys and OAuth grants ([#1475](https://github.com/cedricziel/signaldb/issues/1475)) ([11deba9](https://github.com/cedricziel/signaldb/commit/11deba995c6937324576f87e87284a1580faa624))
* **mcp:** add dataset/tenant discovery tool and tenant confirmation scoping ([#1439](https://github.com/cedricziel/signaldb/issues/1439)) ([ab10083](https://github.com/cedricziel/signaldb/commit/ab1008366850918f8fcfe17b55576c16193eff7b))
* **mcp:** let one session span multiple tenants and datasets ([#1441](https://github.com/cedricziel/signaldb/issues/1441)) ([bc9e6c2](https://github.com/cedricziel/signaldb/commit/bc9e6c2c255037be1de9a7c940f9f2ecba0aa750))
* **querier:** accept bare dotted metric names in PromQL ([#1517](https://github.com/cedricziel/signaldb/issues/1517)) ([889224f](https://github.com/cedricziel/signaldb/commit/889224f6eafd5cab9a6344986ebe56adeb20f5c3))
* **router:** serve query discovery from the registry and statistics ([#1312](https://github.com/cedricziel/signaldb/issues/1312)) ([41d2738](https://github.com/cedricziel/signaldb/commit/41d27384df6e90bd9e9731218e084dd27581e20b))
* **schema-registry:** accept keys= batch resolution on GET /api/v1/schema/metrics ([#1508](https://github.com/cedricziel/signaldb/issues/1508)) ([6facbdc](https://github.com/cedricziel/signaldb/commit/6facbdcd182285bf54c1d2e922724d6bdeb6bae6))
* self-serve connection details for agents ([public] config, /api/v1/connection, MCP connection_info) ([#1474](https://github.com/cedricziel/signaldb/issues/1474)) ([ad78cd1](https://github.com/cedricziel/signaldb/commit/ad78cd1981282426b65b7dcac50ddc38eeea7f80))


### Bug Fixes

* **mcp:** box the SDK error a completion lookup returns ([#1373](https://github.com/cedricziel/signaldb/issues/1373)) ([7df1288](https://github.com/cedricziel/signaldb/commit/7df12883a28c4d6af203c376f6efba70ee537820))


### Code Refactoring

* quality cleanups across writer, mcp-server, schema-model, and tests-integration ([#1330](https://github.com/cedricziel/signaldb/issues/1330)) ([cee4018](https://github.com/cedricziel/signaldb/commit/cee401872f96e2a6961edc1dd3714fa394a56c31))

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
