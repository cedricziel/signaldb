# Changelog

## [0.3.0](https://github.com/cedricziel/signaldb/compare/traceql-parser-v0.2.0...traceql-parser-v0.3.0) (2026-10-03)


### ⚠ BREAKING CHANGES

* **traceql:** `Condition` has a new public `op` field, so code that constructs one must set it.

### Features

* **traceql:** support !=, =~ and !~ and surface search errors over MCP ([#2178](https://github.com/cedricziel/signaldb/issues/2178)) ([ce929f5](https://github.com/cedricziel/signaldb/commit/ce929f5f321e5d826e2ba43edc67d7e26de67659))


### Build System

* fix the beta test leg for cargo's unused-dependency lints ([#2047](https://github.com/cedricziel/signaldb/issues/2047)) ([6867d69](https://github.com/cedricziel/signaldb/commit/6867d69dceb26f2e55ccfac31e60ae42aad76418))

## [0.2.0](https://github.com/cedricziel/signaldb/compare/traceql-parser-v0.1.0...traceql-parser-v0.2.0) (2026-08-26)


### ⚠ BREAKING CHANGES

* **traceql:** GET /api/search now answers 400 instead of 501 for a q that cannot be parsed as TraceQL (notbraces, { foo }, { zzz = 1 }). Splitting the parser forced the question of where the rejection line sits, and answering 'not implemented' to a malformed query left clients unable to distinguish a wrong query from one SignalDB cannot yet run. Valid TraceQL using an unimplemented construct (||, !=, =~, duration) still answers 501, and no rejection moved the other way.

### Code Refactoring

* make query-ir and tempo-api standalone, and cover the parser crates ([#1369](https://github.com/cedricziel/signaldb/issues/1369)) ([1a4d78f](https://github.com/cedricziel/signaldb/commit/1a4d78f077616a9c4846cb6c02715b147b5ad1c2))
* **traceql:** extract the TraceQL parser into a standalone crate ([#1361](https://github.com/cedricziel/signaldb/issues/1361)) ([62e33e0](https://github.com/cedricziel/signaldb/commit/62e33e08c55e6cf24fe97049e19c2d91709236ee))


### Continuous Integration

* **ql:** publish the parser crates on their own release train ([#1362](https://github.com/cedricziel/signaldb/issues/1362)) ([bfc2162](https://github.com/cedricziel/signaldb/commit/bfc216206d779b5a556b7513a882e06dc77bf116))
