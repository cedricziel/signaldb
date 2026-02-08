# Changelog

## 0.1.0 (2026-02-08)


### ⚠ BREAKING CHANGES

* **heraclitus:** Minimum supported Rust version is now 1.86.0
* Minimum supported Rust version is now 1.85.0

### Features

* **acceptor:** add Prometheus remote_write handler ([d5bb53d](https://github.com/cedricziel/signaldb/commit/d5bb53d5a301d3993cb0e374af86282f807c350c))
* **acceptor:** add Prometheus remote_write ingestion endpoint ([#342](https://github.com/cedricziel/signaldb/issues/342)) ([d5bb53d](https://github.com/cedricziel/signaldb/commit/d5bb53d5a301d3993cb0e374af86282f807c350c))
* add global config ([#32](https://github.com/cedricziel/signaldb/issues/32)) ([fbb9a40](https://github.com/cedricziel/signaldb/commit/fbb9a407d45ae8f606334fc4154caee7ae4a12d9))
* add Grafana datasource plugin and Docker infrastructure ([#253](https://github.com/cedricziel/signaldb/issues/253)) ([a95cdfe](https://github.com/cedricziel/signaldb/commit/a95cdfe038e0667bc9b563c3b2f7a8bd7b280069))
* add infrastructure for querying traces ([#7](https://github.com/cedricziel/signaldb/issues/7)) ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
* add querier ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
* add queue primitives ([#48](https://github.com/cedricziel/signaldb/issues/48)) ([caf4651](https://github.com/cedricziel/signaldb/commit/caf46518c2e7ee574d63617a9210774ed2531739))
* Add schema module with Iceberg integration and DSN-based storage ([#162](https://github.com/cedricziel/signaldb/issues/162)) ([60bbb8d](https://github.com/cedricziel/signaldb/commit/60bbb8d09a5ff63e2114c6383e7650c9dfef0d24))
* add schemas for metrics, logs, traces ([#97](https://github.com/cedricziel/signaldb/issues/97)) ([1569d73](https://github.com/cedricziel/signaldb/commit/1569d73ec09cf68ca8745a5ba107b15d763c970b))
* add single trace api endpoint ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
* add tenant management admin API with OpenAPI spec, SDK, and CLI ([#313](https://github.com/cedricziel/signaldb/issues/313)) ([880c86b](https://github.com/cedricziel/signaldb/commit/880c86b6405a162c84fe88615b7d363585948abd))
* **auth:** add tenant ID validation and naming consistency ([#180](https://github.com/cedricziel/signaldb/issues/180)) ([#318](https://github.com/cedricziel/signaldb/issues/318)) ([2c2146a](https://github.com/cedricziel/signaldb/commit/2c2146a579e978842b0af48f2445485d3fb7a1e4))
* **cli:** add HTTP admin API client for TUI ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** add terminal UI with traces, logs, metrics, admin, and dashboard tabs ([#458](https://github.com/cedricziel/signaldb/issues/458)) ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** implement Admin tab with tenant/key/dataset CRUD and confirmations ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** implement Logs tab with Flight SQL query interface ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** implement Metrics tab with sparklines and Flight SQL ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** integrate TUI tabs with help overlay and error handling ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **compactor:** Phase 1 - Dry-run compaction planner ([#462](https://github.com/cedricziel/signaldb/issues/462)) ([a0ad75f](https://github.com/cedricziel/signaldb/commit/a0ad75f5478be94786d77e732a1b8db319ae8650))
* compile tempopb ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
* complete OTLP trace events and links conversion implementation ([#131](https://github.com/cedricziel/signaldb/issues/131)) ([3ad7f9a](https://github.com/cedricziel/signaldb/commit/3ad7f9ab0b3288c4ce1bac288d6a4b1377e8a794)), closes [#98](https://github.com/cedricziel/signaldb/issues/98)
* convert arrow &lt;&gt; otlp ([#99](https://github.com/cedricziel/signaldb/issues/99)) ([ba65d14](https://github.com/cedricziel/signaldb/commit/ba65d144173d2dbeee22011ded650e834df4f5c9))
* create simple write path ([#4](https://github.com/cedricziel/signaldb/issues/4)) ([8ce08ba](https://github.com/cedricziel/signaldb/commit/8ce08ba53b8499c90bba270b2f9cd8e6c5e18c3f))
* end-to-end local development experience with CLI query support ([#434](https://github.com/cedricziel/signaldb/issues/434)) ([b95fb15](https://github.com/cedricziel/signaldb/commit/b95fb1595e33dd825f3c4424a88b966dded4808e))
* generate client + server stubs ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
* **heraclitus:** add Kafka-compatible server with Apache Arrow/Parquet storage ([#212](https://github.com/cedricziel/signaldb/issues/212)) ([8ad74df](https://github.com/cedricziel/signaldb/commit/8ad74df27ab246816a7871ad55d87d32dfac954b))
* implement configurable schemas and tenant management API for SignalDB ([#167](https://github.com/cedricziel/signaldb/issues/167)) ([efe6e09](https://github.com/cedricziel/signaldb/commit/efe6e0952b392ae795232bd05829fe13aaaa10cc))
* implement Iceberg table writer adapter to replace direct Parquet writes ([#175](https://github.com/cedricziel/signaldb/issues/175)) ([a55cc3d](https://github.com/cedricziel/signaldb/commit/a55cc3dbd06d955ee82d64e002abab588102df04))
* implement multi-tenancy with WAL isolation and authentication ([#243](https://github.com/cedricziel/signaldb/issues/243)) ([9a8945f](https://github.com/cedricziel/signaldb/commit/9a8945f06e871a96f5890e194534ae11ebb1f35b))
* implement service catalog-aware Flight transport ([#134](https://github.com/cedricziel/signaldb/issues/134)) ([eebe2b9](https://github.com/cedricziel/signaldb/commit/eebe2b9caa0bb833a7003f581eb9d047c0ab3533))
* implement trace querying functionality for issue [#6](https://github.com/cedricziel/signaldb/issues/6) ([#186](https://github.com/cedricziel/signaldb/issues/186)) ([ea8d9b4](https://github.com/cedricziel/signaldb/commit/ea8d9b47446cdbb89bb05b0a5c048c023d4dde49))
* integrate cargo-husky for pre-commit git hooks ([#150](https://github.com/cedricziel/signaldb/issues/150)) ([7a0d6e5](https://github.com/cedricziel/signaldb/commit/7a0d6e572f231d69a0464ca04a78cbc51c7b93ad))
* integrate cargo-machete for unused dependency detection ([#130](https://github.com/cedricziel/signaldb/issues/130)) ([f305d3b](https://github.com/cedricziel/signaldb/commit/f305d3b9a6923ca2f7eca95ee83ed9002ee7cee1))
* Phase 2 Component Integration with WAL and Flight Services ([#138](https://github.com/cedricziel/signaldb/issues/138)) ([47f4174](https://github.com/cedricziel/signaldb/commit/47f417488c7b0225d031219df94a1d7eb55ff166))
* **querier,writer:** unify table reference format and shared CatalogManager ([#395](https://github.com/cedricziel/signaldb/issues/395)) ([9928f26](https://github.com/cedricziel/signaldb/commit/9928f266766d1de1d2276e5724a27ef29b1128da))
* separate tempo-api into client and server ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
* store instances in catalog ([#105](https://github.com/cedricziel/signaldb/issues/105)) ([6e92a90](https://github.com/cedricziel/signaldb/commit/6e92a9031a20c04658a1060fa2b7733d5e244f0e))
* use in-memory SQLite as default for service discovery catalog ([#136](https://github.com/cedricziel/signaldb/issues/136)) ([3aeaa22](https://github.com/cedricziel/signaldb/commit/3aeaa22ec89b21528ad311b73648a4cd840c1ced))


### Bug Fixes

* adapt exports to tempopb ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
* align Iceberg namespace paths and partition spec (Issue [#185](https://github.com/cedricziel/signaldb/issues/185)) ([#306](https://github.com/cedricziel/signaldb/issues/306)) ([cc7af60](https://github.com/cedricziel/signaldb/commit/cc7af60ad6426eefc0a0de5c628b865306227172))
* **catalog:** replace unwrap() with proper error propagation for datetime parsing ([#315](https://github.com/cedricziel/signaldb/issues/315)) ([89725c2](https://github.com/cedricziel/signaldb/commit/89725c294ecad6e1095b26984cc036d1b8a40e2a))
* correct environment variable parsing using double underscore separator ([#128](https://github.com/cedricziel/signaldb/issues/128)) ([2b731a5](https://github.com/cedricziel/signaldb/commit/2b731a5bb3007d0b84fb172a2d939bbbd4fd0cb7))
* correctly build tempopb ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
* integration tests ([#123](https://github.com/cedricziel/signaldb/issues/123)) ([0ab5eeb](https://github.com/cedricziel/signaldb/commit/0ab5eeb9a0637483efaae88175ab9648c9dd2fb9))
* preserve OTLP scope/resource metadata and events/links in trace pipeline ([#183](https://github.com/cedricziel/signaldb/issues/183)) ([#307](https://github.com/cedricziel/signaldb/issues/307)) ([dfe04d7](https://github.com/cedricziel/signaldb/commit/dfe04d73d27c0e8820aa8daeed0787d048701865))
* **prometheus:** improve target_info generation logic ([d5bb53d](https://github.com/cedricziel/signaldb/commit/d5bb53d5a301d3993cb0e374af86282f807c350c))
* propagate attributes through query path and implement trace search ([#309](https://github.com/cedricziel/signaldb/issues/309)) ([98244d8](https://github.com/cedricziel/signaldb/commit/98244d8f9ebfe623ea07ab240356511acbde5d07))
* resolve beta channel build failures and add temporary table cleanup ([#179](https://github.com/cedricziel/signaldb/issues/179)) ([d5f48dd](https://github.com/cedricziel/signaldb/commit/d5f48dd69cf1026295a825aea00f847c284ebe18))
* set crates versions ([#21](https://github.com/cedricziel/signaldb/issues/21)) ([2113160](https://github.com/cedricziel/signaldb/commit/21131604ba4d7277f2b9e7f26b103ac03ef1bcaf))
* test_iceberg_sql_catalog_basic_operations by removing unimplemented namespace operations ([#251](https://github.com/cedricziel/signaldb/issues/251)) ([98de7ed](https://github.com/cedricziel/signaldb/commit/98de7ed43a3663ef6670cc8bb3c25e37b11832f6))
* **wal:** implement proper WAL segment cleanup and processed state persistence ([#252](https://github.com/cedricziel/signaldb/issues/252)) ([b3e73ff](https://github.com/cedricziel/signaldb/commit/b3e73ffe84eaa638b75b3c07c8d194801c8fcfe7))


### Documentation

* add comprehensive per-component README documentation ([#151](https://github.com/cedricziel/signaldb/issues/151)) ([5ea02a2](https://github.com/cedricziel/signaldb/commit/5ea02a2dd660bc03639a3a82c0146f18c147ecfb)), closes [#139](https://github.com/cedricziel/signaldb/issues/139)
* refresh skills after iceberg catalog refactoring ([#460](https://github.com/cedricziel/signaldb/issues/460)) ([24bfa8c](https://github.com/cedricziel/signaldb/commit/24bfa8c8281080887cb2e3b7cdc13a357b7d4231))


### Code Refactoring

* consolidate Iceberg crate and rename schema_bridge to catalog ([#310](https://github.com/cedricziel/signaldb/issues/310)) ([571d89e](https://github.com/cedricziel/signaldb/commit/571d89ea45037a40fd701163f519afc130e58a2c))
* extract Heraclitus to separate repository ([#240](https://github.com/cedricziel/signaldb/issues/240)) ([f0bfcec](https://github.com/cedricziel/signaldb/commit/f0bfcec7e26fbda82270b6ead696ec84ebde41e1))
* **iceberg:** centralize catalog management with CatalogManager ([#459](https://github.com/cedricziel/signaldb/issues/459)) ([730ceba](https://github.com/cedricziel/signaldb/commit/730cebaa994deb84478ad10f6b9a511e50201d7e))
* remove obsolete NATS-based discovery infrastructure ([#132](https://github.com/cedricziel/signaldb/issues/132)) ([1e0fc55](https://github.com/cedricziel/signaldb/commit/1e0fc55b8b7657fd14163dc45caa508ecb0af355))


### Tests

* add unit test for search_result serialization and deserialization ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
