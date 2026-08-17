# Changelog

## [1.3.0](https://github.com/cedricziel/signaldb/compare/grafana-plugin-v1.2.0...grafana-plugin-v1.3.0) (2026-08-17)


### Features

* **query-ir:** add v2 heatmaps ([#1102](https://github.com/cedricziel/signaldb/issues/1102)) ([96184cf](https://github.com/cedricziel/signaldb/commit/96184cf42809a4cbf0e4a15f592cb544dbb7a597))


### Bug Fixes

* **grafana-plugin:** wire provisioned routerUrl into the Flight client ([#1151](https://github.com/cedricziel/signaldb/issues/1151)) ([ff883e5](https://github.com/cedricziel/signaldb/commit/ff883e543f1b3831c282eadbcd5db97a857099bf)), closes [#977](https://github.com/cedricziel/signaldb/issues/977)


### Performance Improvements

* **flight,wal:** compress Flight IPC payloads and WAL entries ([#945](https://github.com/cedricziel/signaldb/issues/945)) ([#998](https://github.com/cedricziel/signaldb/issues/998)) ([efb5ef4](https://github.com/cedricziel/signaldb/commit/efb5ef4bc85e2e77483f4546255b50c564015827))


### Code Refactoring

* **grafana-plugin:** simplify pass ([#1191](https://github.com/cedricziel/signaldb/issues/1191)) ([38366c9](https://github.com/cedricziel/signaldb/commit/38366c9270b414763f2c9f883f89167909369691))
* simplify backend workspace (dedup, dead code, redundant clones) ([#1168](https://github.com/cedricziel/signaldb/issues/1168)) ([409b778](https://github.com/cedricziel/signaldb/commit/409b778686a1cea5c54edfba7778c3e9ed3aa29c))
* **ui:** simplify pass ([#1192](https://github.com/cedricziel/signaldb/issues/1192)) ([4c67615](https://github.com/cedricziel/signaldb/commit/4c67615500632225aeeaade5cf745dc8607c9c6d))


### Tests

* delete tautological tests and rewrite salvageable ones as contract tests ([#961](https://github.com/cedricziel/signaldb/issues/961)) ([b3e884a](https://github.com/cedricziel/signaldb/commit/b3e884ad59b4df853429133d5eef2724a8adcada))
* polish medium/low audit findings across the workspace ([#969](https://github.com/cedricziel/signaldb/issues/969)) ([8962f6d](https://github.com/cedricziel/signaldb/commit/8962f6d1d22c8a176d4a1d99376d61b42b1da258))

## [1.2.0](https://github.com/cedricziel/signaldb/compare/grafana-plugin-v1.1.0...grafana-plugin-v1.2.0) (2026-07-30)


### Features

* add Grafana datasource plugin and Docker infrastructure ([#253](https://github.com/cedricziel/signaldb/issues/253)) ([a95cdfe](https://github.com/cedricziel/signaldb/commit/a95cdfe038e0667bc9b563c3b2f7a8bd7b280069))
* **ci:** add Grafana plugin CI, Dependabot, and release-please integration ([#277](https://github.com/cedricziel/signaldb/issues/277)) ([005c0e5](https://github.com/cedricziel/signaldb/commit/005c0e549874184d2da439f4065fce6d72eac7d8))
* **grafana-plugin:** implement Flight client for SignalDB queries ([#168](https://github.com/cedricziel/signaldb/issues/168)) ([#302](https://github.com/cedricziel/signaldb/issues/302)) ([7c26881](https://github.com/cedricziel/signaldb/commit/7c268817b3cbd15dd6291b8050f076b34c1a4884))
* **grafana-plugin:** implement real metrics/logs queries and authentication ([#305](https://github.com/cedricziel/signaldb/issues/305)) ([f6dbd1a](https://github.com/cedricziel/signaldb/commit/f6dbd1a21f55436f0d3649d04dc93a3322706fa8))
* **grafana:** add profiles support to the datasource plugin ([#647](https://github.com/cedricziel/signaldb/issues/647)) ([98c398b](https://github.com/cedricziel/signaldb/commit/98c398b8998263b1871f778b10c6c7fae64501ce)), closes [#364](https://github.com/cedricziel/signaldb/issues/364)


### Bug Fixes

* **ci:** use explicit version in grafana-plugin to fix release-please ([#270](https://github.com/cedricziel/signaldb/issues/270)) ([dd21e9d](https://github.com/cedricziel/signaldb/commit/dd21e9d9d5eb89ea3aac62986a4dca97e1ff842c))


### Build System

* cut workspace build times (split plugin workspace, drop AWS SDK, slim pre-commit) ([#640](https://github.com/cedricziel/signaldb/issues/640)) ([00eda72](https://github.com/cedricziel/signaldb/commit/00eda7285905fb869cba45426cea51d7755db5c1))


### Continuous Integration

* drop MSRV policy and fix security audit ignores ([#521](https://github.com/cedricziel/signaldb/issues/521)) ([7da71e3](https://github.com/cedricziel/signaldb/commit/7da71e3d78f593a4361f403e2d4be1e426fb8807))

## [1.1.0](https://github.com/cedricziel/signaldb/compare/grafana-plugin-v1.0.0...grafana-plugin-v1.1.0) (2026-03-02)


### Features

* add Grafana datasource plugin and Docker infrastructure ([#253](https://github.com/cedricziel/signaldb/issues/253)) ([a95cdfe](https://github.com/cedricziel/signaldb/commit/a95cdfe038e0667bc9b563c3b2f7a8bd7b280069))
* **ci:** add Grafana plugin CI, Dependabot, and release-please integration ([#277](https://github.com/cedricziel/signaldb/issues/277)) ([005c0e5](https://github.com/cedricziel/signaldb/commit/005c0e549874184d2da439f4065fce6d72eac7d8))
* **grafana-plugin:** implement Flight client for SignalDB queries ([#168](https://github.com/cedricziel/signaldb/issues/168)) ([#302](https://github.com/cedricziel/signaldb/issues/302)) ([7c26881](https://github.com/cedricziel/signaldb/commit/7c268817b3cbd15dd6291b8050f076b34c1a4884))
* **grafana-plugin:** implement real metrics/logs queries and authentication ([#305](https://github.com/cedricziel/signaldb/issues/305)) ([f6dbd1a](https://github.com/cedricziel/signaldb/commit/f6dbd1a21f55436f0d3649d04dc93a3322706fa8))


### Bug Fixes

* **ci:** use explicit version in grafana-plugin to fix release-please ([#270](https://github.com/cedricziel/signaldb/issues/270)) ([dd21e9d](https://github.com/cedricziel/signaldb/commit/dd21e9d9d5eb89ea3aac62986a4dca97e1ff842c))

## 1.0.0 (Unreleased)

Initial release.
