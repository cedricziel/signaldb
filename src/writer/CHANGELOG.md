# Changelog

## [0.2.0](https://github.com/cedricziel/signaldb/compare/writer-v0.1.0...writer-v0.2.0) (2026-07-24)


### ⚠ BREAKING CHANGES

* Minimum supported Rust version is now 1.85.0

### Features

* add Grafana datasource plugin and Docker infrastructure ([#253](https://github.com/cedricziel/signaldb/issues/253)) ([a95cdfe](https://github.com/cedricziel/signaldb/commit/a95cdfe038e0667bc9b563c3b2f7a8bd7b280069))
* add queue primitives ([#48](https://github.com/cedricziel/signaldb/issues/48)) ([caf4651](https://github.com/cedricziel/signaldb/commit/caf46518c2e7ee574d63617a9210774ed2531739))
* Add schema module with Iceberg integration and DSN-based storage ([#162](https://github.com/cedricziel/signaldb/issues/162)) ([60bbb8d](https://github.com/cedricziel/signaldb/commit/60bbb8d09a5ff63e2114c6383e7650c9dfef0d24))
* **auth:** add tenant ID validation and naming consistency ([#180](https://github.com/cedricziel/signaldb/issues/180)) ([#318](https://github.com/cedricziel/signaldb/issues/318)) ([2c2146a](https://github.com/cedricziel/signaldb/commit/2c2146a579e978842b0af48f2445485d3fb7a1e4))
* **cli:** add HTTP admin API client for TUI ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** add terminal UI with traces, logs, metrics, admin, and dashboard tabs ([#458](https://github.com/cedricziel/signaldb/issues/458)) ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** implement Admin tab with tenant/key/dataset CRUD and confirmations ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** implement Logs tab with Flight SQL query interface ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** implement Metrics tab with sparklines and Flight SQL ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** integrate TUI tabs with help overlay and error handling ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **compactor:** complete epic [#432](https://github.com/cedricziel/signaldb/issues/432) — real compaction, multi-instance tests, observability ([#540](https://github.com/cedricziel/signaldb/issues/540)) ([ed95e20](https://github.com/cedricziel/signaldb/commit/ed95e2062a05b7386d05188c89a754a3606fc428))
* **compactor:** Phase 3 - Retention & Lifecycle Management ([#467](https://github.com/cedricziel/signaldb/issues/467)) ([28acc8d](https://github.com/cedricziel/signaldb/commit/28acc8d215f029fe0b81dcd9b916f29ccdea60d6))
* complete all-signal pipeline (traces, logs, metrics) with producer, transforms, and monolithic discovery fix ([#435](https://github.com/cedricziel/signaldb/issues/435)) ([b973458](https://github.com/cedricziel/signaldb/commit/b9734582edd68436c4ccb3891c3767726a37f433))
* **flight:** authenticate Flight ports via internal service key ([#579](https://github.com/cedricziel/signaldb/issues/579)) ([da1b41f](https://github.com/cedricziel/signaldb/commit/da1b41f4698ce9f58348239d789a1678e23353b3)), closes [#544](https://github.com/cedricziel/signaldb/issues/544)
* **iceberg:** profiles table schema and config toggle ([#633](https://github.com/cedricziel/signaldb/issues/633)) ([9203530](https://github.com/cedricziel/signaldb/commit/920353022c1a58c5ee667954d3356bb7d481836f)), closes [#351](https://github.com/cedricziel/signaldb/issues/351)
* implement Iceberg table writer adapter to replace direct Parquet writes ([#175](https://github.com/cedricziel/signaldb/issues/175)) ([a55cc3d](https://github.com/cedricziel/signaldb/commit/a55cc3dbd06d955ee82d64e002abab588102df04))
* implement multi-tenancy with WAL isolation and authentication ([#243](https://github.com/cedricziel/signaldb/issues/243)) ([9a8945f](https://github.com/cedricziel/signaldb/commit/9a8945f06e871a96f5890e194534ae11ebb1f35b))
* implement trace querying functionality for issue [#6](https://github.com/cedricziel/signaldb/issues/6) ([#186](https://github.com/cedricziel/signaldb/issues/186)) ([ea8d9b4](https://github.com/cedricziel/signaldb/commit/ea8d9b47446cdbb89bb05b0a5c048c023d4dde49))
* integrate cargo-machete for unused dependency detection ([#130](https://github.com/cedricziel/signaldb/issues/130)) ([f305d3b](https://github.com/cedricziel/signaldb/commit/f305d3b9a6923ca2f7eca95ee83ed9002ee7cee1))
* Phase 2 Component Integration with WAL and Flight Services ([#138](https://github.com/cedricziel/signaldb/issues/138)) ([47f4174](https://github.com/cedricziel/signaldb/commit/47f417488c7b0225d031219df94a1d7eb55ff166))
* **querier,writer:** unify table reference format and shared CatalogManager ([#395](https://github.com/cedricziel/signaldb/issues/395)) ([9928f26](https://github.com/cedricziel/signaldb/commit/9928f266766d1de1d2276e5724a27ef29b1128da))
* **self-monitoring:** epic [#447](https://github.com/cedricziel/signaldb/issues/447) — SignalDB observes itself (dogfooding) ([#542](https://github.com/cedricziel/signaldb/issues/542)) ([e6d7b1f](https://github.com/cedricziel/signaldb/commit/e6d7b1fc37f370f534d8780b3a6fe5d180b1ad65))
* store instances in catalog ([#105](https://github.com/cedricziel/signaldb/issues/105)) ([6e92a90](https://github.com/cedricziel/signaldb/commit/6e92a9031a20c04658a1060fa2b7733d5e244f0e))
* **wal:** add WriteProfiles operation and per-signal profiles WAL ([#632](https://github.com/cedricziel/signaldb/issues/632)) ([9a938af](https://github.com/cedricziel/signaldb/commit/9a938af0a213ace259e2c5e6ca1d16123ecdc99e)), closes [#348](https://github.com/cedricziel/signaldb/issues/348)
* **writer:** persist profiles to the Iceberg profiles table ([#637](https://github.com/cedricziel/signaldb/issues/637)) ([5dedbdc](https://github.com/cedricziel/signaldb/commit/5dedbdcbba5080071f859c964ca88ac808685e7e)), closes [#353](https://github.com/cedricziel/signaldb/issues/353)


### Bug Fixes

* align Iceberg namespace paths and partition spec (Issue [#185](https://github.com/cedricziel/signaldb/issues/185)) ([#306](https://github.com/cedricziel/signaldb/issues/306)) ([cc7af60](https://github.com/cedricziel/signaldb/commit/cc7af60ad6426eefc0a0de5c628b865306227172))
* **ci:** resolve clippy 1.97 lints, security advisories, and ethnum build failure ([#516](https://github.com/cedricziel/signaldb/issues/516)) ([b21c459](https://github.com/cedricziel/signaldb/commit/b21c4596f361d14dad147447cc19da4156fb81da))
* **config:** refuse in-memory discovery/catalog in standalone services ([#599](https://github.com/cedricziel/signaldb/issues/599)) ([c8413ba](https://github.com/cedricziel/signaldb/commit/c8413babe5de5346477bf4d1ff26a7f2fef380bb))
* preserve OTLP scope/resource metadata and events/links in trace pipeline ([#183](https://github.com/cedricziel/signaldb/issues/183)) ([#307](https://github.com/cedricziel/signaldb/issues/307)) ([dfe04d7](https://github.com/cedricziel/signaldb/commit/dfe04d73d27c0e8820aa8daeed0787d048701865))
* resolve beta channel build failures and add temporary table cleanup ([#179](https://github.com/cedricziel/signaldb/issues/179)) ([d5f48dd](https://github.com/cedricziel/signaldb/commit/d5f48dd69cf1026295a825aea00f847c284ebe18))
* **wal:** implement proper WAL segment cleanup and processed state persistence ([#252](https://github.com/cedricziel/signaldb/issues/252)) ([b3e73ff](https://github.com/cedricziel/signaldb/commit/b3e73ffe84eaa638b75b3c07c8d194801c8fcfe7))
* **writer:** harden the write path against panics and silent task death ([#605](https://github.com/cedricziel/signaldb/issues/605)) ([ca716db](https://github.com/cedricziel/signaldb/commit/ca716dbc6b2321a4eb838ff3d8031b69e1ec6075))
* **writer:** idempotent WAL-to-Iceberg commits — no duplicate rows on crash replay ([#592](https://github.com/cedricziel/signaldb/issues/592)) ([c43437b](https://github.com/cedricziel/signaldb/commit/c43437b16b4bdd575f84565fa9b0fdd40d969291))
* **writer:** remove the unverified SQL INSERT write path ([#608](https://github.com/cedricziel/signaldb/issues/608)) ([f727170](https://github.com/cedricziel/signaldb/commit/f7271705e4d967efc0796c23f15dca9da44a2f71))
* **writer:** use relative table locations to prevent path duplication in Iceberg ([#436](https://github.com/cedricziel/signaldb/issues/436)) ([3bbc0a6](https://github.com/cedricziel/signaldb/commit/3bbc0a697956067e812b08fca8e0f051667d9c7e))


### Documentation

* full staleness sweep — match all docs, skills, and READMEs to current code ([#611](https://github.com/cedricziel/signaldb/issues/611)) ([22247b0](https://github.com/cedricziel/signaldb/commit/22247b027d77820481d493c081e29f0df4efd6ed))
* refresh skills after iceberg catalog refactoring ([#460](https://github.com/cedricziel/signaldb/issues/460)) ([24bfa8c](https://github.com/cedricziel/signaldb/commit/24bfa8c8281080887cb2e3b7cdc13a357b7d4231))


### Code Refactoring

* consolidate Iceberg crate and rename schema_bridge to catalog ([#310](https://github.com/cedricziel/signaldb/issues/310)) ([571d89e](https://github.com/cedricziel/signaldb/commit/571d89ea45037a40fd701163f519afc130e58a2c))
* **iceberg:** centralize catalog management with CatalogManager ([#459](https://github.com/cedricziel/signaldb/issues/459)) ([730ceba](https://github.com/cedricziel/signaldb/commit/730cebaa994deb84478ad10f6b9a511e50201d7e))
* remove dead connection pooling abstraction from writer ([#311](https://github.com/cedricziel/signaldb/issues/311)) ([57d8e2d](https://github.com/cedricziel/signaldb/commit/57d8e2d58c61aaa7e03b4aec71d4be0640764f16))
* unify Flight data conversion and eliminate double JSON parse ([#308](https://github.com/cedricziel/signaldb/issues/308)) ([b62a081](https://github.com/cedricziel/signaldb/commit/b62a0815782f967d05f748220c51a7ba0a19cd51))


### Continuous Integration

* drop MSRV policy and fix security audit ignores ([#521](https://github.com/cedricziel/signaldb/issues/521)) ([7da71e3](https://github.com/cedricziel/signaldb/commit/7da71e3d78f593a4361f403e2d4be1e426fb8807))

## 0.1.0 (2026-03-02)


### ⚠ BREAKING CHANGES

* Minimum supported Rust version is now 1.85.0

### Features

* add Grafana datasource plugin and Docker infrastructure ([#253](https://github.com/cedricziel/signaldb/issues/253)) ([a95cdfe](https://github.com/cedricziel/signaldb/commit/a95cdfe038e0667bc9b563c3b2f7a8bd7b280069))
* add queue primitives ([#48](https://github.com/cedricziel/signaldb/issues/48)) ([caf4651](https://github.com/cedricziel/signaldb/commit/caf46518c2e7ee574d63617a9210774ed2531739))
* Add schema module with Iceberg integration and DSN-based storage ([#162](https://github.com/cedricziel/signaldb/issues/162)) ([60bbb8d](https://github.com/cedricziel/signaldb/commit/60bbb8d09a5ff63e2114c6383e7650c9dfef0d24))
* **auth:** add tenant ID validation and naming consistency ([#180](https://github.com/cedricziel/signaldb/issues/180)) ([#318](https://github.com/cedricziel/signaldb/issues/318)) ([2c2146a](https://github.com/cedricziel/signaldb/commit/2c2146a579e978842b0af48f2445485d3fb7a1e4))
* **cli:** add HTTP admin API client for TUI ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** add terminal UI with traces, logs, metrics, admin, and dashboard tabs ([#458](https://github.com/cedricziel/signaldb/issues/458)) ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** implement Admin tab with tenant/key/dataset CRUD and confirmations ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** implement Logs tab with Flight SQL query interface ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** implement Metrics tab with sparklines and Flight SQL ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** integrate TUI tabs with help overlay and error handling ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **compactor:** Phase 3 - Retention & Lifecycle Management ([#467](https://github.com/cedricziel/signaldb/issues/467)) ([28acc8d](https://github.com/cedricziel/signaldb/commit/28acc8d215f029fe0b81dcd9b916f29ccdea60d6))
* complete all-signal pipeline (traces, logs, metrics) with producer, transforms, and monolithic discovery fix ([#435](https://github.com/cedricziel/signaldb/issues/435)) ([b973458](https://github.com/cedricziel/signaldb/commit/b9734582edd68436c4ccb3891c3767726a37f433))
* implement Iceberg table writer adapter to replace direct Parquet writes ([#175](https://github.com/cedricziel/signaldb/issues/175)) ([a55cc3d](https://github.com/cedricziel/signaldb/commit/a55cc3dbd06d955ee82d64e002abab588102df04))
* implement multi-tenancy with WAL isolation and authentication ([#243](https://github.com/cedricziel/signaldb/issues/243)) ([9a8945f](https://github.com/cedricziel/signaldb/commit/9a8945f06e871a96f5890e194534ae11ebb1f35b))
* implement trace querying functionality for issue [#6](https://github.com/cedricziel/signaldb/issues/6) ([#186](https://github.com/cedricziel/signaldb/issues/186)) ([ea8d9b4](https://github.com/cedricziel/signaldb/commit/ea8d9b47446cdbb89bb05b0a5c048c023d4dde49))
* integrate cargo-machete for unused dependency detection ([#130](https://github.com/cedricziel/signaldb/issues/130)) ([f305d3b](https://github.com/cedricziel/signaldb/commit/f305d3b9a6923ca2f7eca95ee83ed9002ee7cee1))
* Phase 2 Component Integration with WAL and Flight Services ([#138](https://github.com/cedricziel/signaldb/issues/138)) ([47f4174](https://github.com/cedricziel/signaldb/commit/47f417488c7b0225d031219df94a1d7eb55ff166))
* **querier,writer:** unify table reference format and shared CatalogManager ([#395](https://github.com/cedricziel/signaldb/issues/395)) ([9928f26](https://github.com/cedricziel/signaldb/commit/9928f266766d1de1d2276e5724a27ef29b1128da))
* store instances in catalog ([#105](https://github.com/cedricziel/signaldb/issues/105)) ([6e92a90](https://github.com/cedricziel/signaldb/commit/6e92a9031a20c04658a1060fa2b7733d5e244f0e))


### Bug Fixes

* align Iceberg namespace paths and partition spec (Issue [#185](https://github.com/cedricziel/signaldb/issues/185)) ([#306](https://github.com/cedricziel/signaldb/issues/306)) ([cc7af60](https://github.com/cedricziel/signaldb/commit/cc7af60ad6426eefc0a0de5c628b865306227172))
* preserve OTLP scope/resource metadata and events/links in trace pipeline ([#183](https://github.com/cedricziel/signaldb/issues/183)) ([#307](https://github.com/cedricziel/signaldb/issues/307)) ([dfe04d7](https://github.com/cedricziel/signaldb/commit/dfe04d73d27c0e8820aa8daeed0787d048701865))
* resolve beta channel build failures and add temporary table cleanup ([#179](https://github.com/cedricziel/signaldb/issues/179)) ([d5f48dd](https://github.com/cedricziel/signaldb/commit/d5f48dd69cf1026295a825aea00f847c284ebe18))
* **wal:** implement proper WAL segment cleanup and processed state persistence ([#252](https://github.com/cedricziel/signaldb/issues/252)) ([b3e73ff](https://github.com/cedricziel/signaldb/commit/b3e73ffe84eaa638b75b3c07c8d194801c8fcfe7))
* **writer:** use relative table locations to prevent path duplication in Iceberg ([#436](https://github.com/cedricziel/signaldb/issues/436)) ([3bbc0a6](https://github.com/cedricziel/signaldb/commit/3bbc0a697956067e812b08fca8e0f051667d9c7e))


### Documentation

* refresh skills after iceberg catalog refactoring ([#460](https://github.com/cedricziel/signaldb/issues/460)) ([24bfa8c](https://github.com/cedricziel/signaldb/commit/24bfa8c8281080887cb2e3b7cdc13a357b7d4231))


### Code Refactoring

* consolidate Iceberg crate and rename schema_bridge to catalog ([#310](https://github.com/cedricziel/signaldb/issues/310)) ([571d89e](https://github.com/cedricziel/signaldb/commit/571d89ea45037a40fd701163f519afc130e58a2c))
* **iceberg:** centralize catalog management with CatalogManager ([#459](https://github.com/cedricziel/signaldb/issues/459)) ([730ceba](https://github.com/cedricziel/signaldb/commit/730cebaa994deb84478ad10f6b9a511e50201d7e))
* remove dead connection pooling abstraction from writer ([#311](https://github.com/cedricziel/signaldb/issues/311)) ([57d8e2d](https://github.com/cedricziel/signaldb/commit/57d8e2d58c61aaa7e03b4aec71d4be0640764f16))
* unify Flight data conversion and eliminate double JSON parse ([#308](https://github.com/cedricziel/signaldb/issues/308)) ([b62a081](https://github.com/cedricziel/signaldb/commit/b62a0815782f967d05f748220c51a7ba0a19cd51))
