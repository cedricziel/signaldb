# Changelog

## [0.1.1](https://github.com/cedricziel/signaldb/compare/0.1.0...v0.1.1) (2026-07-24)


### Features

* **acceptor:** add Prometheus remote_write handler ([d5bb53d](https://github.com/cedricziel/signaldb/commit/d5bb53d5a301d3993cb0e374af86282f807c350c))
* **acceptor:** add Prometheus remote_write ingestion endpoint ([#342](https://github.com/cedricziel/signaldb/issues/342)) ([d5bb53d](https://github.com/cedricziel/signaldb/commit/d5bb53d5a301d3993cb0e374af86282f807c350c))
* **acceptor:** per-tenant ingest rate limits (requests/sec + bytes/sec) ([#594](https://github.com/cedricziel/signaldb/issues/594)) ([448e165](https://github.com/cedricziel/signaldb/commit/448e16562e350379221e761637e3af83b0db2330))
* add Grafana datasource plugin and Docker infrastructure ([#253](https://github.com/cedricziel/signaldb/issues/253)) ([a95cdfe](https://github.com/cedricziel/signaldb/commit/a95cdfe038e0667bc9b563c3b2f7a8bd7b280069))
* **cli:** add HTTP admin API client for TUI ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** add terminal UI with traces, logs, metrics, admin, and dashboard tabs ([#458](https://github.com/cedricziel/signaldb/issues/458)) ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** implement Admin tab with tenant/key/dataset CRUD and confirmations ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** implement Logs tab with Flight SQL query interface ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** implement Metrics tab with sparklines and Flight SQL ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** integrate TUI tabs with help overlay and error handling ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* complete all-signal pipeline (traces, logs, metrics) with producer, transforms, and monolithic discovery fix ([#435](https://github.com/cedricziel/signaldb/issues/435)) ([b973458](https://github.com/cedricziel/signaldb/commit/b9734582edd68436c4ccb3891c3767726a37f433))
* enable Dokku deployment with working HTTP router and monolithic Docker image ([#312](https://github.com/cedricziel/signaldb/issues/312)) ([4ec9d5c](https://github.com/cedricziel/signaldb/commit/4ec9d5cb4538e0d74278bfd14d51d65da1b2020c))
* end-to-end local development experience with CLI query support ([#434](https://github.com/cedricziel/signaldb/issues/434)) ([b95fb15](https://github.com/cedricziel/signaldb/commit/b95fb1595e33dd825f3c4424a88b966dded4808e))
* **flight:** authenticate Flight ports via internal service key ([#579](https://github.com/cedricziel/signaldb/issues/579)) ([da1b41f](https://github.com/cedricziel/signaldb/commit/da1b41f4698ce9f58348239d789a1678e23353b3)), closes [#544](https://github.com/cedricziel/signaldb/issues/544)
* implement Iceberg table writer adapter to replace direct Parquet writes ([#175](https://github.com/cedricziel/signaldb/issues/175)) ([a55cc3d](https://github.com/cedricziel/signaldb/commit/a55cc3dbd06d955ee82d64e002abab588102df04))
* **querier,writer:** unify table reference format and shared CatalogManager ([#395](https://github.com/cedricziel/signaldb/issues/395)) ([9928f26](https://github.com/cedricziel/signaldb/commit/9928f266766d1de1d2276e5724a27ef29b1128da))
* **querier:** enforce resource limits on query execution ([#593](https://github.com/cedricziel/signaldb/issues/593)) ([b1c6341](https://github.com/cedricziel/signaldb/commit/b1c634157d4b669df81224242c21a4e05938fca5))
* **quotas:** per-tenant storage quotas backed by Iceberg usage accounting ([#634](https://github.com/cedricziel/signaldb/issues/634)) ([38a77dc](https://github.com/cedricziel/signaldb/commit/38a77dca6e3474fd148ad28eeca6f4bdfd59ae75))
* **self-monitoring:** epic [#447](https://github.com/cedricziel/signaldb/issues/447) — SignalDB observes itself (dogfooding) ([#542](https://github.com/cedricziel/signaldb/issues/542)) ([e6d7b1f](https://github.com/cedricziel/signaldb/commit/e6d7b1fc37f370f534d8780b3a6fe5d180b1ad65))
* **signaldb-bin:** integrate compactor service into monolithic mode ([#464](https://github.com/cedricziel/signaldb/issues/464)) ([dbc1bd3](https://github.com/cedricziel/signaldb/commit/dbc1bd35f10679fa835e4ac3687bfd967e77b2c9))
* start querier service in monolithic mode ([#430](https://github.com/cedricziel/signaldb/issues/430)) ([ddcc177](https://github.com/cedricziel/signaldb/commit/ddcc177dfca165e0119dc81e68243dc4d27b7465)), closes [#418](https://github.com/cedricziel/signaldb/issues/418)


### Bug Fixes

* **prometheus:** improve target_info generation logic ([d5bb53d](https://github.com/cedricziel/signaldb/commit/d5bb53d5a301d3993cb0e374af86282f807c350c))
* resolve beta channel build failures and add temporary table cleanup ([#179](https://github.com/cedricziel/signaldb/issues/179)) ([d5f48dd](https://github.com/cedricziel/signaldb/commit/d5f48dd69cf1026295a825aea00f847c284ebe18))


### Documentation

* refresh skills after iceberg catalog refactoring ([#460](https://github.com/cedricziel/signaldb/issues/460)) ([24bfa8c](https://github.com/cedricziel/signaldb/commit/24bfa8c8281080887cb2e3b7cdc13a357b7d4231))


### Code Refactoring

* **iceberg:** centralize catalog management with CatalogManager ([#459](https://github.com/cedricziel/signaldb/issues/459)) ([730ceba](https://github.com/cedricziel/signaldb/commit/730cebaa994deb84478ad10f6b9a511e50201d7e))


### Continuous Integration

* drop MSRV policy and fix security audit ignores ([#521](https://github.com/cedricziel/signaldb/issues/521)) ([7da71e3](https://github.com/cedricziel/signaldb/commit/7da71e3d78f593a4361f403e2d4be1e426fb8807))

## 0.1.0 (2026-03-02)


### Features

* **acceptor:** add Prometheus remote_write handler ([d5bb53d](https://github.com/cedricziel/signaldb/commit/d5bb53d5a301d3993cb0e374af86282f807c350c))
* **acceptor:** add Prometheus remote_write ingestion endpoint ([#342](https://github.com/cedricziel/signaldb/issues/342)) ([d5bb53d](https://github.com/cedricziel/signaldb/commit/d5bb53d5a301d3993cb0e374af86282f807c350c))
* add Grafana datasource plugin and Docker infrastructure ([#253](https://github.com/cedricziel/signaldb/issues/253)) ([a95cdfe](https://github.com/cedricziel/signaldb/commit/a95cdfe038e0667bc9b563c3b2f7a8bd7b280069))
* **cli:** add HTTP admin API client for TUI ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** add terminal UI with traces, logs, metrics, admin, and dashboard tabs ([#458](https://github.com/cedricziel/signaldb/issues/458)) ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** implement Admin tab with tenant/key/dataset CRUD and confirmations ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** implement Logs tab with Flight SQL query interface ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** implement Metrics tab with sparklines and Flight SQL ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** integrate TUI tabs with help overlay and error handling ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* complete all-signal pipeline (traces, logs, metrics) with producer, transforms, and monolithic discovery fix ([#435](https://github.com/cedricziel/signaldb/issues/435)) ([b973458](https://github.com/cedricziel/signaldb/commit/b9734582edd68436c4ccb3891c3767726a37f433))
* enable Dokku deployment with working HTTP router and monolithic Docker image ([#312](https://github.com/cedricziel/signaldb/issues/312)) ([4ec9d5c](https://github.com/cedricziel/signaldb/commit/4ec9d5cb4538e0d74278bfd14d51d65da1b2020c))
* end-to-end local development experience with CLI query support ([#434](https://github.com/cedricziel/signaldb/issues/434)) ([b95fb15](https://github.com/cedricziel/signaldb/commit/b95fb1595e33dd825f3c4424a88b966dded4808e))
* implement Iceberg table writer adapter to replace direct Parquet writes ([#175](https://github.com/cedricziel/signaldb/issues/175)) ([a55cc3d](https://github.com/cedricziel/signaldb/commit/a55cc3dbd06d955ee82d64e002abab588102df04))
* **querier,writer:** unify table reference format and shared CatalogManager ([#395](https://github.com/cedricziel/signaldb/issues/395)) ([9928f26](https://github.com/cedricziel/signaldb/commit/9928f266766d1de1d2276e5724a27ef29b1128da))
* **signaldb-bin:** integrate compactor service into monolithic mode ([#464](https://github.com/cedricziel/signaldb/issues/464)) ([dbc1bd3](https://github.com/cedricziel/signaldb/commit/dbc1bd35f10679fa835e4ac3687bfd967e77b2c9))
* start querier service in monolithic mode ([#430](https://github.com/cedricziel/signaldb/issues/430)) ([ddcc177](https://github.com/cedricziel/signaldb/commit/ddcc177dfca165e0119dc81e68243dc4d27b7465)), closes [#418](https://github.com/cedricziel/signaldb/issues/418)


### Bug Fixes

* **prometheus:** improve target_info generation logic ([d5bb53d](https://github.com/cedricziel/signaldb/commit/d5bb53d5a301d3993cb0e374af86282f807c350c))
* resolve beta channel build failures and add temporary table cleanup ([#179](https://github.com/cedricziel/signaldb/issues/179)) ([d5f48dd](https://github.com/cedricziel/signaldb/commit/d5f48dd69cf1026295a825aea00f847c284ebe18))


### Documentation

* refresh skills after iceberg catalog refactoring ([#460](https://github.com/cedricziel/signaldb/issues/460)) ([24bfa8c](https://github.com/cedricziel/signaldb/commit/24bfa8c8281080887cb2e3b7cdc13a357b7d4231))


### Code Refactoring

* **iceberg:** centralize catalog management with CatalogManager ([#459](https://github.com/cedricziel/signaldb/issues/459)) ([730ceba](https://github.com/cedricziel/signaldb/commit/730cebaa994deb84478ad10f6b9a511e50201d7e))
