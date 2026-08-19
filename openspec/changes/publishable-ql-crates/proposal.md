## Why

SignalDB speaks four compatibility query surfaces, and exactly one of them has a
crate. `logql` (3,299 lines: token, lexer, AST, metric AST, recursive-descent
parser) depends on `thiserror` and nothing else — no `common`, no DataFusion, no
SignalDB column names. It is already the shape we want.

TraceQL is not. Its parser does not exist as a parser: it is 593 lines inside
`querier/src/query/search_filter.rs` where recognising `{ .service.name = "api" }`
is interleaved with building `datafusion::logical_expr::Expr`, keyed on
`common::schema::materialized_column_name` and erroring as `QuerierError`. There
is no way to ask "is this TraceQL valid?" without linking the query engine, and
no way for the UI, the SDK, or anyone outside this repo to reuse the grammar.

The rule this change establishes is narrow and testable: **a QL crate lexes,
parses, and validates syntax. It never knows a column name, a catalog, or a
tenant.** Everything downstream of the AST — column mapping, promotion,
DataFusion lowering — stays in the querier. That boundary is what makes the
crates publishable, and publishing them is what forces the boundary to stay
honest: a crate that leaks a product type cannot be published, so CI catches the
regression instead of a reviewer.

There is precedent already stated in the codebase: `schema-model` is documented
as "deliberately dependency-light: it is used by `common` at runtime and by its
`build.rs`". This change applies the same discipline to the query languages.

## What Changes

### A new `traceql` crate — a parser written, not moved

`search_filter.rs` splits along the line between recognising a query and
executing one:

| Moves to `traceql`                                                                                             | Stays in `querier`                                                                                                                                           |
| -------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `parse_traceql`, `split_top_level_and`, `parse_traceql_clause`, `take_value`                                   | `Condition::to_expr` (becomes a free function — no inherent impls on foreign types)                                                                          |
| `Selector`, `Condition`, `FilterValue` (the AST)                                                               | `materialized_expr`, `map_attribute_expr`, `attribute_expr`                                                                                                  |
| the intrinsic vocabulary (`name`, `status`, `kind`, `resource.service.name`, `span.`/`resource.`/`.` prefixes) | `AttrContext`, `MaterializedColumns`, promotion routing                                                                                                      |
| the supported/unsupported subset decision                                                                      | the `QuerierError` mapping and HTTP status                                                                                                                   |
|                                                                                                                | `parse_tags` — Tempo's logfmt `tags` parameter is an _HTTP parameter format_, not TraceQL. It keeps using the `traceql` AST but is not part of the language. |

The extraction is behaviour-preserving. Every accepted query stays accepted,
every rejected query stays rejected **with the same message and the same HTTP
status**, pinned by tests that move with the code.

The one substantive addition: `traceql::ParseError` must distinguish
_syntactically invalid_ from _valid TraceQL we do not lower yet_, because the
querier maps those to 400 and 501 respectively and that distinction currently
lives in `QuerierError::{InvalidInput, Unsupported}`. A parser that collapses
them would silently change every unsupported-operator response from 501 to 400.

### `logql` gains publishing metadata and a stability contract

No code change. It gains what a published crate needs and none of these
manifests currently carry: `repository`, `readme`, `keywords`, `categories`,
`documentation`, a README, and `#[non_exhaustive]` on its public enums.

### Publishing machinery, which does not exist today

`release-please` cuts GitHub releases and tags; **no workflow runs `cargo
publish`** and no `CARGO_REGISTRY_TOKEN` is configured. `tempo-api` has carried
`publish = true` since it was written and has never been published. This change
adds a publish job gated on the release tag, plus `cargo publish --dry-run` in
PR CI so missing metadata fails before a release, not during one.

### Each crate tracks the licence of the language it implements

A compatibility front-end is a re-implementation of somebody else's published
language, so it takes that project's licence rather than one chosen for our own
convenience. Verified against the upstream `LICENSE` files:

| Our crate        | Implements | Upstream            | Upstream licence | Ours today     | Action                                      |
| ---------------- | ---------- | ------------------- | ---------------- | -------------- | ------------------------------------------- |
| `logql-parser`   | LogQL      | `grafana/loki`      | AGPL-3.0         | AGPL-3.0       | keep — already correct                      |
| `traceql-parser` | TraceQL    | `grafana/tempo`     | AGPL-3.0         | (new)          | AGPL-3.0                                    |
| `loki-api`       | Loki HTTP  | `grafana/loki`      | AGPL-3.0         | AGPL-3.0       | keep                                        |
| `pyroscope-api`  | Pyroscope  | `grafana/pyroscope` | AGPL-3.0         | AGPL-3.0       | keep                                        |
| `prometheus-api` | Prom HTTP  | `prometheus`        | **Apache-2.0**   | AGPL-3.0       | stricter than upstream; out of scope, noted |
| `tempo-api`      | Tempo API  | `grafana/tempo`     | AGPL-3.0         | **Apache-2.0** | **mismatch — see below**                    |

This settles the licence question by rule rather than by preference, and it
removes it as a blocker: `logql` is already AGPL-3.0 and stays there, `traceql`
is born AGPL-3.0. AGPL narrows who can depend on the published crates, and that
is the correct consequence of implementing an AGPL project's language — not a
cost to engineer around.

**`tempo-api` needs a look, and this change does not fix it.** It declares
`license = "Apache-2.0"` and `publish = true`, while Grafana Tempo is AGPL-3.0
— and `src/tempo-api/proto/tempo.proto` is a copy of Tempo's `tempopb`
definitions, from which `src/tempo-api/src/generated/tempopb.rs` is generated.
A vendored file from an AGPL repository re-declared as Apache-2.0 is a
discrepancy that should be resolved before anything in this repo is published,
including by the machinery this change adds. It is called out here, and left to
its own change: the two QL crates do not depend on `tempo-api`, so nothing in
§1–6 is blocked on it. **Task 5.4 sets `publish = false` on `tempo-api` in the
meantime**, so the new publish job cannot ship it by accident.

## One blocker found while scoping

### The name `logql` is taken on crates.io

```
crates.io/crates/logql   created 2022-01-24, last updated 2022-05-19
                         11,305 downloads, 44 recent, unrelated project
```

`traceql`, `traceql-parser`, `logql-parser`, `signaldb-logql`, and `tempo-api`
are all free. crates.io has no reliable transfer path for an inactive name.

**Recommendation:** publish as `logql-parser` / `traceql-parser`, keeping
`[lib] name = "logql"` so every existing `use logql::…` site is untouched — the
package name is the crates.io identity, the lib name is the import path.
Descriptive names also travel better than `signaldb-`-prefixed ones for a
library whose whole point is reuse outside SignalDB.

## The QL crates release standalone

Today every package releases on one train: `separate-pull-requests` is `false`,
so one release PR covers all 21 packages. The QL crates should not ride it —
a parser fix should be able to reach crates.io without waiting for, or dragging
along, a `signaldb-core` release.

They are already independent in two of the three respects that matter:
`include-component-in-tag` is `true` (so tags are `logql-v0.1.2`), and neither
crate is in the `linked-versions` `signaldb-core` group, so their versions
already move on their own (`logql` at 0.1.2 while the core sits at 0.3.0). What
is missing is a release train of their own.

**Approach: a second release-please instance**, with its own
`release-please-config.ql.json` and `.release-please-manifest.ql.json` covering
only `src/logql` and `src/traceql`, in its own workflow that runs `cargo
publish` when it cuts a release. Flipping the existing `separate-pull-requests`
to `true` would instead split all 21 packages into individual release PRs — a
change to the whole project's release process, made as a side effect of adding
two crates. See design D9 for the mechanics and the two gotchas (distinct
release-PR labels; the `cargo-workspace` plugin no longer sees these crates).

## Explicitly scoped out

- **PromQL.** We do not own a PromQL parser — `querier/query/promql.rs` is
  _lowering_ over the third-party `promql-parser` crate. There is nothing to
  extract and nothing to publish. (The router's use of `logql::tokenize` to
  parse PromQL `step` durations stays: LogQL durations _are_ Go durations, and
  once `logql` is a published library that reuse is a documented API call rather
  than a reach across a module.)
- **Pyroscope.** Label selectors, no query language.
- **`query-ir` — stays exactly where it is.** It is SignalDB's own query
  surface, not a re-implementation of anyone else's language, so neither the
  purity rule nor the licence-tracking rule applies to it: it stays inside
  `common` and stays AGPL-3.0. (Its coupling to the rest of `common` is three
  lines — `Filterability` twice and `materialized_column_name` once — so
  extracting it later remains cheap if a reason ever appears. A parse-only QL
  crate does not depend on the IR, so nothing here needs it.)
- **Lowering in the QL crates** (`logql::LogQuery → ir::Document` client-side).
  Deliberately deferred — see design D6. It would collapse the querier's two
  parallel lowering paths, but it requires the IR crate first and it contradicts
  the parse-only rule this change is establishing. Decide it on its own merits,
  later.
- **UI / CLI / HTTP surface parity.** Per the surface-parity rule, stated
  explicitly rather than skipped: this change ships **no user-facing surface**.
  It is a library boundary, and the Tempo search API's observable behaviour must
  be identical before and after. Regression coverage replaces parity tasks. (A
  WASM build of the parsers for in-editor syntax validation in the Explore UI is
  the obvious payoff, and is a follow-up, not this change.)

## BREAKING: malformed TraceQL becomes 400 instead of 501

Scoping the extraction surfaced that the current parser answers **501 Not
Implemented for input that is not TraceQL at all** — `q=notbraces`,
`q={ foo }`, `q={ zzz = 1 }` — because every structural rejection in
`parse_traceql` is built as `QuerierError::Unsupported`. Only bad _value
literals_ produce a 400. "We have not implemented your syntactically invalid
query" is the wrong answer to a client error, and it makes the error class
useless for telling a Grafana user "your query is wrong" apart from "SignalDB
cannot do that yet".

This change fixes it, which makes it a deliberate Tempo-surface change:

| `q`                                       | Today | After   | Why                          |
| ----------------------------------------- | ----- | ------- | ---------------------------- |
| `notbraces` — no spanset                  | 501   | **400** | not parseable as TraceQL     |
| `{ foo }` — no comparison                 | 501   | **400** | not parseable as TraceQL     |
| `{ zzz = 1 }` — unknown selector spelling | 501   | **400** | not a legal selector         |
| `{ .a != "b" }`, `>=`, `=~`, …            | 501   | 501     | valid TraceQL, unimplemented |
| `{ a } \|\| { b }`                        | 501   | 501     | valid TraceQL, unimplemented |
| `{ duration > 100ms }`                    | 501   | 501     | valid TraceQL, unimplemented |
| `{ .a = "unterminated }`                  | 400   | 400     | unchanged                    |
| `{ .a = @@@ }`                            | 400   | 400     | unchanged                    |

**The delta is strictly one-directional: only 501 → 400, never 400 → 501.**
Escaped string literals (`{ .a = "he said \"hi\"" }`) are legal TraceQL we
cannot lex, so by the rule above they "should" become 501 — they are
deliberately left at 400, because moving a client error into the
not-implemented class serves nobody and widens the blast radius for no gain.
That inconsistency is documented in the crate rather than silently carried.

Nothing else about the surface moves: the accepted grammar is identical, the
rejection _messages_ are preserved verbatim, and every 200 response is
unchanged. Neither crate has ever been published, so the `logql` →
`logql-parser` rename breaks no external consumer; it is a package-name choice
made _before_ first publication.

## Capabilities

### New Capabilities

- `query-language-frontends`: the contract for SignalDB's compatibility query
  languages as standalone front-ends — what syntax each accepts and rejects,
  that the accept/reject decision depends on the query text alone and never on
  tenant, catalog, or storage state, that unsupported-but-valid syntax is
  distinguishable from invalid syntax, and the purity and stability guarantees
  of the published crates.

### Modified Capabilities

None. No existing capability's observable behaviour changes.

## Impact

- **Crates**: `traceql` (new, leaf); `logql` (manifest, `#[non_exhaustive]`,
  README — no logic change); `querier` (depends on `traceql`; `search_filter.rs`
  reduces to lowering; `QuerierError` gains a `From<traceql::ParseError>`);
  `router` (no change — it never called the TraceQL parser directly);
  `tests-integration` (Tempo search regression coverage).
- **Workspace**: `Cargo.toml` `members` + `default-members`; a new
  `release-please-config.ql.json` + `.release-please-manifest.ql.json` and
  `.github/workflows/release-ql-crates.yml` (the QL release train, which also
  publishes); `src/logql` removed from the main
  `release-please-config.json`/manifest; `tempo-api` set `publish = false`
  pending its licence review.
- **Issues**: no existing issue tracks this; file one and link it here.
- **API surfaces**: none. No ingest, Flight wire, OpenAPI, SDK, or on-disk
  change.
- **Config**: none.
