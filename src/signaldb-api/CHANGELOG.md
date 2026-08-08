# Changelog

## [0.1.2](https://github.com/cedricziel/signaldb/compare/signaldb-api-v0.1.1...signaldb-api-v0.1.2) (2026-08-08)


### Features

* **api:** code-first OpenAPI — generate spec + Rust/TS clients from annotations ([#856](https://github.com/cedricziel/signaldb/issues/856)) ([e34fbfb](https://github.com/cedricziel/signaldb/commit/e34fbfbd094034416f78597c59b306975dd97271))


### Code Refactoring

* **cli:** make signaldb-cli depend only on the SDK (+ create_user API) ([#874](https://github.com/cedricziel/signaldb/issues/874)) ([8e5cce5](https://github.com/cedricziel/signaldb/commit/8e5cce56c821d69917b55cc8c21a9a2ef55864b7))

## [0.1.1](https://github.com/cedricziel/signaldb/compare/signaldb-api-v0.1.0...signaldb-api-v0.1.1) (2026-07-30)


### Features

* add tenant management admin API with OpenAPI spec, SDK, and CLI ([#313](https://github.com/cedricziel/signaldb/issues/313)) ([880c86b](https://github.com/cedricziel/signaldb/commit/880c86b6405a162c84fe88615b7d363585948abd))
* **profiles:** link profiles to traces across the query surface ([#645](https://github.com/cedricziel/signaldb/issues/645)) ([5430d27](https://github.com/cedricziel/signaldb/commit/5430d27281a66a9d88dea0e8d450f73902307137)), closes [#362](https://github.com/cedricziel/signaldb/issues/362) [#363](https://github.com/cedricziel/signaldb/issues/363)
* **router:** Pyroscope-compatible HTTP API ([#644](https://github.com/cedricziel/signaldb/issues/644)) ([dabbede](https://github.com/cedricziel/signaldb/commit/dabbedeebc17ad0d03ac43aa44932b05a37ff857)), closes [#359](https://github.com/cedricziel/signaldb/issues/359)


### Continuous Integration

* drop MSRV policy and fix security audit ignores ([#521](https://github.com/cedricziel/signaldb/issues/521)) ([7da71e3](https://github.com/cedricziel/signaldb/commit/7da71e3d78f593a4361f403e2d4be1e426fb8807))
