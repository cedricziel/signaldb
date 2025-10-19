# Changelog

## 0.1.0 (2025-10-19)


### ⚠ BREAKING CHANGES

* **heraclitus:** Minimum supported Rust version is now 1.86.0
* Minimum supported Rust version is now 1.85.0

### Features

* add global config ([#32](https://github.com/cedricziel/signaldb/issues/32)) ([fbb9a40](https://github.com/cedricziel/signaldb/commit/fbb9a407d45ae8f606334fc4154caee7ae4a12d9))
* add infrastructure for querying traces ([#7](https://github.com/cedricziel/signaldb/issues/7)) ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
* add querier ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
* add queue primitives ([#48](https://github.com/cedricziel/signaldb/issues/48)) ([caf4651](https://github.com/cedricziel/signaldb/commit/caf46518c2e7ee574d63617a9210774ed2531739))
* Add schema module with Iceberg integration and DSN-based storage ([#162](https://github.com/cedricziel/signaldb/issues/162)) ([60bbb8d](https://github.com/cedricziel/signaldb/commit/60bbb8d09a5ff63e2114c6383e7650c9dfef0d24))
* add schemas for metrics, logs, traces ([#97](https://github.com/cedricziel/signaldb/issues/97)) ([1569d73](https://github.com/cedricziel/signaldb/commit/1569d73ec09cf68ca8745a5ba107b15d763c970b))
* add single trace api endpoint ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
* compile tempopb ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
* complete OTLP trace events and links conversion implementation ([#131](https://github.com/cedricziel/signaldb/issues/131)) ([3ad7f9a](https://github.com/cedricziel/signaldb/commit/3ad7f9ab0b3288c4ce1bac288d6a4b1377e8a794)), closes [#98](https://github.com/cedricziel/signaldb/issues/98)
* convert arrow &lt;&gt; otlp ([#99](https://github.com/cedricziel/signaldb/issues/99)) ([ba65d14](https://github.com/cedricziel/signaldb/commit/ba65d144173d2dbeee22011ded650e834df4f5c9))
* create simple write path ([#4](https://github.com/cedricziel/signaldb/issues/4)) ([8ce08ba](https://github.com/cedricziel/signaldb/commit/8ce08ba53b8499c90bba270b2f9cd8e6c5e18c3f))
* generate client + server stubs ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
* **heraclitus:** add Kafka-compatible server with Apache Arrow/Parquet storage ([#212](https://github.com/cedricziel/signaldb/issues/212)) ([8ad74df](https://github.com/cedricziel/signaldb/commit/8ad74df27ab246816a7871ad55d87d32dfac954b))
* implement configurable schemas and tenant management API for SignalDB ([#167](https://github.com/cedricziel/signaldb/issues/167)) ([efe6e09](https://github.com/cedricziel/signaldb/commit/efe6e0952b392ae795232bd05829fe13aaaa10cc))
* implement Iceberg table writer adapter to replace direct Parquet writes ([#175](https://github.com/cedricziel/signaldb/issues/175)) ([a55cc3d](https://github.com/cedricziel/signaldb/commit/a55cc3dbd06d955ee82d64e002abab588102df04))
* implement service catalog-aware Flight transport ([#134](https://github.com/cedricziel/signaldb/issues/134)) ([eebe2b9](https://github.com/cedricziel/signaldb/commit/eebe2b9caa0bb833a7003f581eb9d047c0ab3533))
* implement trace querying functionality for issue [#6](https://github.com/cedricziel/signaldb/issues/6) ([#186](https://github.com/cedricziel/signaldb/issues/186)) ([ea8d9b4](https://github.com/cedricziel/signaldb/commit/ea8d9b47446cdbb89bb05b0a5c048c023d4dde49))
* integrate cargo-husky for pre-commit git hooks ([#150](https://github.com/cedricziel/signaldb/issues/150)) ([7a0d6e5](https://github.com/cedricziel/signaldb/commit/7a0d6e572f231d69a0464ca04a78cbc51c7b93ad))
* integrate cargo-machete for unused dependency detection ([#130](https://github.com/cedricziel/signaldb/issues/130)) ([f305d3b](https://github.com/cedricziel/signaldb/commit/f305d3b9a6923ca2f7eca95ee83ed9002ee7cee1))
* Phase 2 Component Integration with WAL and Flight Services ([#138](https://github.com/cedricziel/signaldb/issues/138)) ([47f4174](https://github.com/cedricziel/signaldb/commit/47f417488c7b0225d031219df94a1d7eb55ff166))
* separate tempo-api into client and server ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
* store instances in catalog ([#105](https://github.com/cedricziel/signaldb/issues/105)) ([6e92a90](https://github.com/cedricziel/signaldb/commit/6e92a9031a20c04658a1060fa2b7733d5e244f0e))
* use in-memory SQLite as default for service discovery catalog ([#136](https://github.com/cedricziel/signaldb/issues/136)) ([3aeaa22](https://github.com/cedricziel/signaldb/commit/3aeaa22ec89b21528ad311b73648a4cd840c1ced))


### Bug Fixes

* adapt exports to tempopb ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
* correct environment variable parsing using double underscore separator ([#128](https://github.com/cedricziel/signaldb/issues/128)) ([2b731a5](https://github.com/cedricziel/signaldb/commit/2b731a5bb3007d0b84fb172a2d939bbbd4fd0cb7))
* correctly build tempopb ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
* integration tests ([#123](https://github.com/cedricziel/signaldb/issues/123)) ([0ab5eeb](https://github.com/cedricziel/signaldb/commit/0ab5eeb9a0637483efaae88175ab9648c9dd2fb9))
* resolve beta channel build failures and add temporary table cleanup ([#179](https://github.com/cedricziel/signaldb/issues/179)) ([d5f48dd](https://github.com/cedricziel/signaldb/commit/d5f48dd69cf1026295a825aea00f847c284ebe18))
* set crates versions ([#21](https://github.com/cedricziel/signaldb/issues/21)) ([2113160](https://github.com/cedricziel/signaldb/commit/21131604ba4d7277f2b9e7f26b103ac03ef1bcaf))


### Documentation

* add comprehensive per-component README documentation ([#151](https://github.com/cedricziel/signaldb/issues/151)) ([5ea02a2](https://github.com/cedricziel/signaldb/commit/5ea02a2dd660bc03639a3a82c0146f18c147ecfb)), closes [#139](https://github.com/cedricziel/signaldb/issues/139)


### Code Refactoring

* extract Heraclitus to separate repository ([#240](https://github.com/cedricziel/signaldb/issues/240)) ([f0bfcec](https://github.com/cedricziel/signaldb/commit/f0bfcec7e26fbda82270b6ead696ec84ebde41e1))
* remove obsolete NATS-based discovery infrastructure ([#132](https://github.com/cedricziel/signaldb/issues/132)) ([1e0fc55](https://github.com/cedricziel/signaldb/commit/1e0fc55b8b7657fd14163dc45caa508ecb0af355))


### Tests

* add unit test for search_result serialization and deserialization ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
