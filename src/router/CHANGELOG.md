# Changelog

## [0.2.0](https://github.com/cedricziel/signaldb/compare/router-v0.1.0...router-v0.2.0) (2026-07-24)


### ⚠ BREAKING CHANGES

* Minimum supported Rust version is now 1.85.0

### Features

* add Grafana datasource plugin and Docker infrastructure ([#253](https://github.com/cedricziel/signaldb/issues/253)) ([a95cdfe](https://github.com/cedricziel/signaldb/commit/a95cdfe038e0667bc9b563c3b2f7a8bd7b280069))
* add queue primitives ([#48](https://github.com/cedricziel/signaldb/issues/48)) ([caf4651](https://github.com/cedricziel/signaldb/commit/caf46518c2e7ee574d63617a9210774ed2531739))
* Add schema module with Iceberg integration and DSN-based storage ([#162](https://github.com/cedricziel/signaldb/issues/162)) ([60bbb8d](https://github.com/cedricziel/signaldb/commit/60bbb8d09a5ff63e2114c6383e7650c9dfef0d24))
* add tenant management admin API with OpenAPI spec, SDK, and CLI ([#313](https://github.com/cedricziel/signaldb/issues/313)) ([880c86b](https://github.com/cedricziel/signaldb/commit/880c86b6405a162c84fe88615b7d363585948abd))
* **auth:** per-tenant query rate limits and API key/dataset quotas ([#609](https://github.com/cedricziel/signaldb/issues/609)) ([f2ae3e9](https://github.com/cedricziel/signaldb/commit/f2ae3e955f05fde7511c344211c3d1613b6a86e9))
* **cli:** add HTTP admin API client for TUI ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** add terminal UI with traces, logs, metrics, admin, and dashboard tabs ([#458](https://github.com/cedricziel/signaldb/issues/458)) ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** implement Admin tab with tenant/key/dataset CRUD and confirmations ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** implement Logs tab with Flight SQL query interface ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** implement Metrics tab with sparklines and Flight SQL ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** integrate TUI tabs with help overlay and error handling ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* convert arrow &lt;&gt; otlp ([#99](https://github.com/cedricziel/signaldb/issues/99)) ([ba65d14](https://github.com/cedricziel/signaldb/commit/ba65d144173d2dbeee22011ded650e834df4f5c9))
* **discovery:** TTL-filter stale services, reap crashed nodes, round-robin routing ([#600](https://github.com/cedricziel/signaldb/issues/600)) ([6aad9dc](https://github.com/cedricziel/signaldb/commit/6aad9dccbb2120442da5e80cf15f113e0c3d662b))
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
* **logs:** end-to-end LogQL log queries (querier service + router) ([#665](https://github.com/cedricziel/signaldb/issues/665)) ([7e77dcf](https://github.com/cedricziel/signaldb/commit/7e77dcff12f7d9a49afe2c40a4104cbe302f1a48))
* **loki:** add Loki API types crate and router LogQL endpoint skeleton ([#650](https://github.com/cedricziel/signaldb/issues/650)) ([9a938b7](https://github.com/cedricziel/signaldb/commit/9a938b7dea2e404492b54a0481415d2f36881880))
* Phase 2 Component Integration with WAL and Flight Services ([#138](https://github.com/cedricziel/signaldb/issues/138)) ([47f4174](https://github.com/cedricziel/signaldb/commit/47f417488c7b0225d031219df94a1d7eb55ff166))
* **profiles:** link profiles to traces across the query surface ([#645](https://github.com/cedricziel/signaldb/issues/645)) ([5430d27](https://github.com/cedricziel/signaldb/commit/5430d27281a66a9d88dea0e8d450f73902307137)), closes [#362](https://github.com/cedricziel/signaldb/issues/362) [#363](https://github.com/cedricziel/signaldb/issues/363)
* PromQL query support — /prometheus API (epic [#328](https://github.com/cedricziel/signaldb/issues/328)) ([#671](https://github.com/cedricziel/signaldb/issues/671)) ([9fe8264](https://github.com/cedricziel/signaldb/commit/9fe8264b0d2fbb3f785779034a6388da5c0cdd95))
* **querier:** apply TraceQL and tag filters on trace search — no more silently unfiltered results ([#596](https://github.com/cedricziel/signaldb/issues/596)) ([fb8f0ba](https://github.com/cedricziel/signaldb/commit/fb8f0ba081aee3dcf5f524deec12851d38a2acf5)), closes [#551](https://github.com/cedricziel/signaldb/issues/551)
* **querier:** surface trace not-found as an explicit Flight status ([#616](https://github.com/cedricziel/signaldb/issues/616)) ([d6daeb6](https://github.com/cedricziel/signaldb/commit/d6daeb6fc63e6a1c49fefdcfb2391750f01dbcc8))
* **router:** Pyroscope-compatible HTTP API ([#644](https://github.com/cedricziel/signaldb/issues/644)) ([dabbede](https://github.com/cedricziel/signaldb/commit/dabbedeebc17ad0d03ac43aa44932b05a37ff857)), closes [#359](https://github.com/cedricziel/signaldb/issues/359)
* **self-monitoring:** epic [#447](https://github.com/cedricziel/signaldb/issues/447) — SignalDB observes itself (dogfooding) ([#542](https://github.com/cedricziel/signaldb/issues/542)) ([e6d7b1f](https://github.com/cedricziel/signaldb/commit/e6d7b1fc37f370f534d8780b3a6fe5d180b1ad65))
* store instances in catalog ([#105](https://github.com/cedricziel/signaldb/issues/105)) ([6e92a90](https://github.com/cedricziel/signaldb/commit/6e92a9031a20c04658a1060fa2b7733d5e244f0e))
* **tempo:** honor spss span cap when shaping search results ([#615](https://github.com/cedricziel/signaldb/issues/615)) ([6a1d04b](https://github.com/cedricziel/signaldb/commit/6a1d04bef21c6b4ce85c2547f24cb884b40d8da3))
* **tempo:** honor start/end time hints in single-trace lookup ([#614](https://github.com/cedricziel/signaldb/issues/614)) ([ddb81fb](https://github.com/cedricziel/signaldb/commit/ddb81fbc2803ab5cb87e92b6b36773b3752009b6))


### Bug Fixes

* align Iceberg namespace paths and partition spec (Issue [#185](https://github.com/cedricziel/signaldb/issues/185)) ([#306](https://github.com/cedricziel/signaldb/issues/306)) ([cc7af60](https://github.com/cedricziel/signaldb/commit/cc7af60ad6426eefc0a0de5c628b865306227172))
* **config:** refuse in-memory discovery/catalog in standalone services ([#599](https://github.com/cedricziel/signaldb/issues/599)) ([c8413ba](https://github.com/cedricziel/signaldb/commit/c8413babe5de5346477bf4d1ff26a7f2fef380bb))
* integration tests ([#123](https://github.com/cedricziel/signaldb/issues/123)) ([0ab5eeb](https://github.com/cedricziel/signaldb/commit/0ab5eeb9a0637483efaae88175ab9648c9dd2fb9))
* preserve OTLP scope/resource metadata and events/links in trace pipeline ([#183](https://github.com/cedricziel/signaldb/issues/183)) ([#307](https://github.com/cedricziel/signaldb/issues/307)) ([dfe04d7](https://github.com/cedricziel/signaldb/commit/dfe04d73d27c0e8820aa8daeed0787d048701865))
* propagate attributes through query path and implement trace search ([#309](https://github.com/cedricziel/signaldb/issues/309)) ([98244d8](https://github.com/cedricziel/signaldb/commit/98244d8f9ebfe623ea07ab240356511acbde5d07))
* resolve beta channel build failures and add temporary table cleanup ([#179](https://github.com/cedricziel/signaldb/issues/179)) ([d5f48dd](https://github.com/cedricziel/signaldb/commit/d5f48dd69cf1026295a825aea00f847c284ebe18))
* **router:** stop serving fabricated or empty-stub Tempo responses ([#597](https://github.com/cedricziel/signaldb/issues/597)) ([f8dd559](https://github.com/cedricziel/signaldb/commit/f8dd55925b9eebfb7c15b427a5fa811d481bcb18))
* set version in router ([#85](https://github.com/cedricziel/signaldb/issues/85)) ([4c9adc7](https://github.com/cedricziel/signaldb/commit/4c9adc772bdaf077990592561f1109cd263fbdce))


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
