# Changelog

## 0.1.0 (2025-10-19)


### Features

* add infrastructure for querying traces ([#7](https://github.com/cedricziel/signaldb/issues/7)) ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
* add querier ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
* add queue primitives ([#48](https://github.com/cedricziel/signaldb/issues/48)) ([caf4651](https://github.com/cedricziel/signaldb/commit/caf46518c2e7ee574d63617a9210774ed2531739))
* add single trace api endpoint ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
* compile tempopb ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
* generate client + server stubs ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
* implement Iceberg table writer adapter to replace direct Parquet writes ([#175](https://github.com/cedricziel/signaldb/issues/175)) ([a55cc3d](https://github.com/cedricziel/signaldb/commit/a55cc3dbd06d955ee82d64e002abab588102df04))
* implement trace querying functionality for issue [#6](https://github.com/cedricziel/signaldb/issues/6) ([#186](https://github.com/cedricziel/signaldb/issues/186)) ([ea8d9b4](https://github.com/cedricziel/signaldb/commit/ea8d9b47446cdbb89bb05b0a5c048c023d4dde49))
* integrate cargo-machete for unused dependency detection ([#130](https://github.com/cedricziel/signaldb/issues/130)) ([f305d3b](https://github.com/cedricziel/signaldb/commit/f305d3b9a6923ca2f7eca95ee83ed9002ee7cee1))
* separate tempo-api into client and server ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
* store instances in catalog ([#105](https://github.com/cedricziel/signaldb/issues/105)) ([6e92a90](https://github.com/cedricziel/signaldb/commit/6e92a9031a20c04658a1060fa2b7733d5e244f0e))


### Bug Fixes

* adapt exports to tempopb ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
* correctly build tempopb ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
* resolve beta channel build failures and add temporary table cleanup ([#179](https://github.com/cedricziel/signaldb/issues/179)) ([d5f48dd](https://github.com/cedricziel/signaldb/commit/d5f48dd69cf1026295a825aea00f847c284ebe18))
* set crates versions ([#21](https://github.com/cedricziel/signaldb/issues/21)) ([2113160](https://github.com/cedricziel/signaldb/commit/21131604ba4d7277f2b9e7f26b103ac03ef1bcaf))


### Documentation

* add comprehensive per-component README documentation ([#151](https://github.com/cedricziel/signaldb/issues/151)) ([5ea02a2](https://github.com/cedricziel/signaldb/commit/5ea02a2dd660bc03639a3a82c0146f18c147ecfb)), closes [#139](https://github.com/cedricziel/signaldb/issues/139)


### Tests

* add unit test for search_result serialization and deserialization ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
