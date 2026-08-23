---
audience: contributor
type: reference
status: living
sources:
  - src/logql/**
  - src/traceql/**
  - src/ql-ir/**
  - src/query-ir/**
  - src/loki-api/**
  - src/prometheus-api/**
  - src/pyroscope-api/**
  - src/tempo-api/**
  - scripts/check-leaf-purity.sh
  - release-please-config.json
---

# Compatibility Crates

SignalDB speaks several query languages and HTTP APIs it did not invent —
Tempo's, Loki's, Prometheus's, Pyroscope's. The crates implementing them follow
two rules that do not apply to the rest of the workspace.

## Rule 1: a query-language crate parses, and nothing else

`logql` and `traceql` lex, parse, and validate **syntax**. They may not know a
column name, a catalog, a tenant, an attribute promotion, or a storage format.
Their only dependency is `thiserror`.

Everything downstream of the AST — mapping a selector onto a column, choosing
between a materialized column and an attribute map, building a DataFusion
expression — used to live in the querier directly; since `ir-single-lowering`
it targets the query IR instead. `ql-ir` (`src/ql-ir/`) lowers a parsed
LogQL/TraceQL query onto a `query-ir` document — still no Arrow, no
DataFusion, no tenant/catalog access, just a structured description of the
query — and the querier's single planner
(`querier/src/query/ir_planner.rs::plan_document`) does the rest, the same
planner the native `POST /api/v1/query` surface uses. `querier/src/query/{logql,logql_metric,search_filter}.rs`
now hold only what the IR still can't express (LogQL constructs `ql_ir`
refuses as `Inexpressible`) and response assembly; `search_filter.rs` in
particular is down to parsing Tempo's `tags` HTTP parameter, no lowering at
all.

The reason is a property worth protecting: **whether a query is valid depends on
the query text alone.** Not on which tenant asked, not on what has been ingested,
not on which attributes happen to be promoted today. That makes a parser usable
in a CI check, an editor, a WASM build in the browser, or a downstream project —
none of which can run a query engine.

PromQL has no crate of ours; the third-party `promql-parser` supplies the
grammar and the querier lowers it.

### How the rule is enforced

Not by review. Two CI checks in `Check & Lint`:

- **`./scripts/check-leaf-purity.sh`** reads `cargo metadata` and fails if a
  leaf crate depends on a workspace member, a `path`/`git` source, or the FDAP
  stack. It covers `logql-parser`, `traceql-parser`, and `query-ir` — the same
  invariant for all three, whether or not the crate is published.
- **`cargo publish --dry-run`** fails on missing metadata or packaging problems.

Both are needed. The dry-run is _not_ a purity check — it accepts
`datafusion = "54"` without complaint, because that is a perfectly publishable
dependency. Only the first check enforces the rule.

### Rejection classes

A parser distinguishes two failures, and callers map them to different statuses:

| Class         | Meaning                                     | HTTP |
| ------------- | ------------------------------------------- | ---- |
| `Syntax`      | not the language at all                     | 400  |
| `Unsupported` | valid in the language, not implemented here | 501  |

Collapsing them leaves a user unable to tell a wrong query from one SignalDB
cannot yet run. Never silently drop an unsupported construct: a partially
applied filter returns _more_ rows than asked for while still looking like a
successful query.

## Rule 2: a compatibility crate carries its upstream's licence

A crate that re-implements another project's language or HTTP API takes **that
project's licence**, not the one convenient for us.

| Crate                  | Re-implements     | Licence    |
| ---------------------- | ----------------- | ---------- |
| `logql`, `loki-api`    | Grafana Loki      | AGPL-3.0   |
| `traceql`, `tempo-api` | Grafana Tempo     | AGPL-3.0   |
| `pyroscope-api`        | Grafana Pyroscope | AGPL-3.0   |
| `prometheus-api`       | Prometheus        | Apache-2.0 |

First-party crates — `common`, `query-ir`, the services — are SignalDB's own
design, re-implement nobody, and stay AGPL-3.0 regardless. `query-ir` is a
separate crate for the same reason the parsers are (a document can be built and
validated without the query engine), but it is **not published**: nobody outside
SignalDB has a use for SignalDB's own query surface.

AGPL narrows who can depend on a published crate. That is the correct
consequence of implementing an AGPL project's language, not a problem to
engineer around with a permissive re-licence.

## Publishing

`logql-parser` and `traceql-parser` publish to crates.io from
`.github/workflows/release-please.yml`. Both are marked
`"separate-pull-requests": true` in `release-please-config.json` — a
**per-package** option — so each gets its own release PR and ships without
waiting on a product release, while every other package keeps sharing one. The
`publish-ql-crates` job is gated on each crate's own `--release_created`
output, so releasing one never republishes the other. The package names carry a `-parser` suffix because a bare
`logql` was taken in 2022; `[lib] name` keeps the import path short, and the
root manifest's `package` key keeps the dependency name stable:

```toml
logql = { path = "src/logql", package = "logql-parser" }
```

Public enums are `#[non_exhaustive]` so that teaching a parser a new construct
is additive for consumers. Match on them with a fallback arm that reports the
construct as unsupported — the parser can be newer than the code lowering it.
AST _structs_ stay constructible; building a query by hand is legitimate.
