# Changelog

## 0.1.0 (2025-10-19)


### ⚠ BREAKING CHANGES

* Minimum supported Rust version is now 1.85.0

### Features

* add queue primitives ([#48](https://github.com/cedricziel/signaldb/issues/48)) ([caf4651](https://github.com/cedricziel/signaldb/commit/caf46518c2e7ee574d63617a9210774ed2531739))
* Add schema module with Iceberg integration and DSN-based storage ([#162](https://github.com/cedricziel/signaldb/issues/162)) ([60bbb8d](https://github.com/cedricziel/signaldb/commit/60bbb8d09a5ff63e2114c6383e7650c9dfef0d24))
* convert arrow &lt;&gt; otlp ([#99](https://github.com/cedricziel/signaldb/issues/99)) ([ba65d14](https://github.com/cedricziel/signaldb/commit/ba65d144173d2dbeee22011ded650e834df4f5c9))
* implement configurable schemas and tenant management API for SignalDB ([#167](https://github.com/cedricziel/signaldb/issues/167)) ([efe6e09](https://github.com/cedricziel/signaldb/commit/efe6e0952b392ae795232bd05829fe13aaaa10cc))
* implement external Flight service interface for SignalDB router ([#135](https://github.com/cedricziel/signaldb/issues/135)) ([df4ce06](https://github.com/cedricziel/signaldb/commit/df4ce06834b73b9537a2f4c63d1e5cbfceaf3b58))
* implement service catalog-aware Flight transport ([#134](https://github.com/cedricziel/signaldb/issues/134)) ([eebe2b9](https://github.com/cedricziel/signaldb/commit/eebe2b9caa0bb833a7003f581eb9d047c0ab3533))
* implement trace querying functionality for issue [#6](https://github.com/cedricziel/signaldb/issues/6) ([#186](https://github.com/cedricziel/signaldb/issues/186)) ([ea8d9b4](https://github.com/cedricziel/signaldb/commit/ea8d9b47446cdbb89bb05b0a5c048c023d4dde49))
* integrate cargo-machete for unused dependency detection ([#130](https://github.com/cedricziel/signaldb/issues/130)) ([f305d3b](https://github.com/cedricziel/signaldb/commit/f305d3b9a6923ca2f7eca95ee83ed9002ee7cee1))
* Phase 2 Component Integration with WAL and Flight Services ([#138](https://github.com/cedricziel/signaldb/issues/138)) ([47f4174](https://github.com/cedricziel/signaldb/commit/47f417488c7b0225d031219df94a1d7eb55ff166))
* store instances in catalog ([#105](https://github.com/cedricziel/signaldb/issues/105)) ([6e92a90](https://github.com/cedricziel/signaldb/commit/6e92a9031a20c04658a1060fa2b7733d5e244f0e))


### Bug Fixes

* integration tests ([#123](https://github.com/cedricziel/signaldb/issues/123)) ([0ab5eeb](https://github.com/cedricziel/signaldb/commit/0ab5eeb9a0637483efaae88175ab9648c9dd2fb9))
* resolve beta channel build failures and add temporary table cleanup ([#179](https://github.com/cedricziel/signaldb/issues/179)) ([d5f48dd](https://github.com/cedricziel/signaldb/commit/d5f48dd69cf1026295a825aea00f847c284ebe18))
* set version in router ([#85](https://github.com/cedricziel/signaldb/issues/85)) ([4c9adc7](https://github.com/cedricziel/signaldb/commit/4c9adc772bdaf077990592561f1109cd263fbdce))


### Performance Improvements

* optimize dependency tree to reduce build times ([#149](https://github.com/cedricziel/signaldb/issues/149)) ([6057f14](https://github.com/cedricziel/signaldb/commit/6057f149c6d1d85a74fc092f53b91393a12fba48))


### Documentation

* add comprehensive per-component README documentation ([#151](https://github.com/cedricziel/signaldb/issues/151)) ([5ea02a2](https://github.com/cedricziel/signaldb/commit/5ea02a2dd660bc03639a3a82c0146f18c147ecfb)), closes [#139](https://github.com/cedricziel/signaldb/issues/139)
