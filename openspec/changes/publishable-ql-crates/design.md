## Context

The workspace's internal dependency graph is already clean at the leaves:

```text
logql          -> (none)          loki-api       -> (none)
tempo-api      -> (none)          prometheus-api -> (none)
schema-model   -> (none)          pyroscope-api  -> (none)

common         -> schema-model
querier        -> common, logql, tempo-api
router         -> common, logql, loki-api, prometheus-api, pyroscope-api,
                  schema-model, signaldb-api, tempo-api
```

No compatibility crate depends on a product crate today. The premise holds — but
it holds trivially, because only LogQL was ever given a crate. The coupling this
change addresses is not in the crates; it is in the ~4,800 lines of lowering
inside `querier/src/query/`, one slice of which (`search_filter.rs`) is a parser
that never got separated from its lowering.

**Constraint:** FDAP version alignment applies to everything that touches Arrow
or DataFusion types. This change moves code in the opposite direction — the
extracted parser touches neither, which is precisely what makes it extractable.
The lowering that remains in `querier` keeps using DataFusion's re-exported
Arrow/Parquet types, unchanged.

**Constraint:** no Flight wire schema, WAL, or Iceberg layout is touched. There
is no migration and nothing to roll back beyond a normal revert.

## Goals / Non-Goals

**Goals**

- One rule, mechanically enforced: a QL crate's dependency list contains no
  workspace crate, no DataFusion, no Arrow.
- A TraceQL parser that can answer "is this valid?" without a querier.
- Preserve Tempo search behaviour exactly — including which malformed queries
  return 400 and which unsupported ones return 501.
- Publishable artifacts, if the licence decision allows.

**Non-Goals**

- Lowering to Query IR from the QL crates (D6).
- Extracting `query_ir` from `common` (its own change).
- Extending the supported TraceQL subset. The subset is small and stays small in
  this change; growing it after the parser exists is much cheaper, which is part
  of the point.

## Decisions

### D1 — Crate naming: package name for crates.io, lib name for imports

`logql` is occupied on crates.io (created 2022-01-24, last published
2022-05-19, 11,305 downloads, unrelated). `traceql`, `traceql-parser`,
`logql-parser`, and `signaldb-logql` are all free.

**Decision (confirmed):** package `logql-parser` with `[lib] name = "logql"`;
package `traceql-parser` with `[lib] name = "traceql"`.

```toml
[package]
name = "logql-parser"        # crates.io identity
[lib]
name = "logql"               # `use logql::…` — every call site unchanged
```

_Alternatives rejected:_ `signaldb-logql` ties a general-purpose grammar
implementation to a product name and discourages exactly the reuse that
justifies publishing. Requesting the squatted name has no reliable crates.io
process and would block the change on a third party.

_Risk:_ a package/lib name mismatch surprises readers. Mitigate with a comment
in each manifest stating why, and a line in the crate-level `//!` docs.

### D2 — Licence: track the upstream project whose language the crate implements

A compatibility front-end re-implements a language that another project defined
and published. The licence follows that project, not our convenience. Verified
by fetching each upstream `LICENSE`:

| Language / API | Upstream            | Licence    | Our crate                 |
| -------------- | ------------------- | ---------- | ------------------------- |
| LogQL          | `grafana/loki`      | AGPL-3.0   | `logql-parser`            |
| TraceQL        | `grafana/tempo`     | AGPL-3.0   | `traceql-parser`          |
| Pyroscope      | `grafana/pyroscope` | AGPL-3.0   | `pyroscope-api`           |
| PromQL / Prom  | `prometheus`        | Apache-2.0 | (no crate — D6/scope-out) |

**Decision:** `logql-parser` stays `AGPL-3.0` (already correct); `traceql-parser`
is born `AGPL-3.0`. No relicensing, no consent to gather, no `cargo deny`
allowlist change, and no open question blocking the release.

The consequence — AGPL narrows who can depend on the published crates — is
accepted, not mitigated. These crates implement AGPL projects' languages; a
permissive re-licence of that work would be the wrong call regardless of how
much wider it would spread. Publishing still delivers what this change is for:
the crates become independently consumable artifacts, and the act of publishing
is what mechanically enforces their purity (D7/D8).

The author has confirmed they do not object to relicensing where the rule calls
for it, so applying it needs no consent-gathering and is not an open question.

**The rule also corrects two existing crates**, which this change does as a
one-line manifest edit each:

- **`tempo-api`: `Apache-2.0` → `AGPL-3.0`.** Grafana Tempo is AGPL-3.0, and
  `src/tempo-api/proto/tempo.proto` is a copy of Tempo's `tempopb` protobuf
  definitions from which `src/tempo-api/src/generated/tempopb.rs` is generated
  by `build.rs`. A vendored file from an AGPL repository re-declared as
  Apache-2.0 is the one genuine discrepancy in the table, and it carries
  `publish = true` today. It also stays `publish = false` until it has real
  publication metadata (task 5.4) — correcting the licence and wiring it for
  release are separate jobs, and only the first belongs here.

  **What actually blocks `tempo-api` from ever publishing is its `build.rs`,
  not its metadata.** The script writes generated `.proto` files into
  `src/tempo-api/proto/**` and generated Rust into `src/tempo-api/src/generated/`
  — both inside the package directory rather than `OUT_DIR`, which `cargo
publish` rejects as a dirty package — and it reads
  `../../opentelemetry-proto`, a submodule _outside_ the crate root that a
  published `.crate` archive cannot contain, so the packaged build script would
  panic on any consumer's machine. It also requires `protoc` at consumer build
  time and decides staleness by mtime, which git checkouts make
  non-deterministic. The fix is to move generation into `xtask`, where the
  `write_or_check` + `cargo run -p xtask -- check` pattern already exists in CI
  for the OpenAPI-derived SDK and TypeScript clients. That is its own change:
  it shares no file with this one, and a proto-generation regression must not be
  able to block a parser extraction.

- **`prometheus-api`: `AGPL-3.0` → `Apache-2.0`**, tracking Prometheus. Stricter
  than upstream is legal and harmless, but the rule is "track the equivalent",
  and applying it selectively only where it tightens would not be the rule.

Neither crate is a QL front-end and neither is published by this change; they
are corrected here because the rule that motivates the change applies to them
and the fix is trivial. `loki-api` and `pyroscope-api` are already correct.

**`query-ir` is out of the rule entirely.** It is SignalDB's own query surface,
not a re-implementation of anyone's language, so it tracks nothing: it stays
AGPL-3.0 and stays inside `common`. This change neither extracts it nor
relicenses it (see the proposal's scope-out).

### D3 — The boundary is the AST; errors carry the 400/501 distinction

```text
   ┌─────────────────────────────────────────────────────────────┐
   │  traceql  (leaf: thiserror only)                            │
   │                                                             │
   │   "{ .service.name = \"api\" && span.http.method = \"GET\" }"│
   │        │                                                    │
   │        ▼  parse_traceql                                     │
   │   Vec<Condition> { selector: Selector, value: FilterValue }  │
   │        │                                                    │
   │   Err(ParseError::Syntax)      ← malformed  → 400           │
   │   Err(ParseError::Unsupported) ← valid, not lowered → 501   │
   └────────┼────────────────────────────────────────────────────┘
            ▼
   ┌─────────────────────────────────────────────────────────────┐
   │  querier::query::search_filter  (lowering)                  │
   │   to_expr(&Condition, &AttrContext) -> Result<Expr, …>      │
   │     ├─ materialized column  (label_<key>)   ← promotion     │
   │     ├─ map get_field        (map attrs)                     │
   │     └─ JSON substring       (legacy tables)                 │
   └─────────────────────────────────────────────────────────────┘
```

Two consequences worth stating before someone hits them:

1. **`Condition::to_expr` cannot stay an inherent method.** `Condition` becomes a
   foreign type, so lowering becomes a free function
   `search_filter::to_expr(cond, ctx)` (or a private extension trait). Purely
   mechanical, but it touches every call site.
2. **`ParseError` needs two variants today, and the split is not where it
   sits.** The parser currently builds `QuerierError::Unsupported` for
   _every_ structural rejection — including input that is not TraceQL at all —
   and reserves `InvalidInput` for bad value literals. So `q=notbraces` answers 501. The extracted parser draws the line by the language instead:

   | `ParseError` variant | Meaning                                  | Maps to                            |
   | -------------------- | ---------------------------------------- | ---------------------------------- |
   | `Syntax`             | not parseable as TraceQL                 | `QuerierError::InvalidInput` → 400 |
   | `Unsupported`        | valid TraceQL, construct not implemented | `QuerierError::Unsupported` → 501  |
   | _any future variant_ | unknown to this build                    | `QuerierError::Unsupported` → 501  |

   Two variants is the count today, not a ceiling: D5 marks `ParseError`
   `#[non_exhaustive]`, so a newer parser may add a class this build predates.
   The `From` impl therefore needs a third arm, and it maps to **501**, not 400
   — a rejection this build cannot interpret is our gap, not the client's
   mistake, and 400 would tell them to fix a query that may be perfectly valid.

   Three rejections move 501 → 400 as a result (no spanset braces; a clause with
   no comparison operator; an unknown selector spelling). That is the BREAKING
   delta the proposal tabulates, and it is the whole reason the split is worth
   drawing here rather than preserved as-is.

   **The delta is one-directional by construction.** Escaped string literals are
   legal TraceQL that our lexer rejects, so the rule would classify them
   `Unsupported` (501) where they are `InvalidInput` (400) today. They stay
   `Syntax`/400: moving a client error into the not-implemented class helps
   nobody and enlarges the change. The crate documents that carve-out on the
   variant, so the next reader finds a stated exception rather than an
   inconsistency.

   A test pins the status for one query of each class, on both sides of the
   boundary — `traceql-parser` asserts the variant, `tests-integration` asserts
   the HTTP status end to end.

### D4 — `parse_tags` stays in the querier

Tempo's `tags` parameter is space-separated logfmt `key=value` pairs supplied in
a URL query string. It is an HTTP parameter encoding, not a TraceQL construct —
it just happens to produce the same `Condition` values. Moving it would put an
HTTP concern in a language crate.

It stays in `querier`, importing the AST from `traceql`. If it ever wants a
home of its own, that home is `tempo-api` (wire-format types), not `traceql`.

### D5 — `#[non_exhaustive]` before first publish, but not on everything

`Selector`, `FilterValue`, `ParseError`, and LogQL's `Token`, `PipelineStage`,
`RangeFunction`, `AggregationFunction`, `BinOp`, `MatchOp`, `LineFilterOp`,
`FilterOp` are public enums that the LogQL/TraceQL parity work adds variants to.
Post-publication each added variant is a breaking release unless the enum is
`#[non_exhaustive]`. That is free now and impossible later, so it is a task in
this change rather than a note for the future.

**Structs need the same question asked, with the opposite answer in most
cases.** `#[non_exhaustive]` on a struct forbids downstream literal
construction, so it is not a free win — it depends on whether consumers build
the type or only read it:

| Type                                                                | Marked? | Why                                                                                |
| ------------------------------------------------------------------- | ------- | ---------------------------------------------------------------------------------- |
| `LexError`, `ParseError` (LogQL)                                    | yes     | errors gain context; nobody constructs them                                        |
| `traceql::Condition`                                                | no      | a matcher _is_ a selector and a value; the Tempo `tags` path builds them           |
| LogQL AST structs (`LogQuery`, `StreamSelector`, `LabelMatcher`, …) | no      | building a query by hand is a legitimate use; `querier/query/logs.rs` already does |

Sealing the AST structs would break `logs.rs` at compile time and would remove
a real capability from the published crate for no benefit. Adding a field to
them stays a breaking change, which at 0.x is a minor bump and an honest signal.

**The cost lands on our own lowering.** Every `#[non_exhaustive]` enum forces a
wildcard arm wherever the querier matches it — eleven of them in
`query/logql.rs` and `query/logql_metric.rs`. Those arms return "recognised but
not lowered by this build" rather than guessing, which is correct precisely
_because_ of D9: the parsers release on their own train, so the querier really
can be compiled against a parser that is ahead of it. Without D9 this would be
speculative future-proofing and the compile error would be the better outcome.

### D6 — Parse-only, and what that forgoes

This change fixes the crates at lex/parse/validate-syntax. The alternative —
having the crates also lower their AST to a Query IR document — would collapse
the querier's two parallel lowering paths (four QL lowerings targeting
DataFusion `Expr` directly, plus `ir_planner.rs` at 5,383 lines targeting it
independently) into one, and would let the SDK, CLI, and a WASM UI build produce
executable queries client-side.

It is not done here because:

- it requires `query-ir` to be a leaf crate first (its own change), and
- it contradicts the rule this change is establishing, so adopting both at once
  would leave the boundary undefined.

What parse-only _does_ buy immediately: syntax validation and highlighting
anywhere the crates compile, including WASM — replacing, for instance, the UI's
current `buildPromQL.ts` string concatenation with a real grammar. That
follow-up is unblocked by this change and does not need the IR.

### D7 — Publishing is gated, dry-run first

There is no `cargo publish` anywhere in `.github/workflows/` today and no
`CARGO_REGISTRY_TOKEN`. The missing metadata is real: none of the manifests
carry `repository`, `readme`, `keywords`, `categories`, or `documentation`, so a
publish attempt today fails on metadata, during a release, when it is most
expensive.

**Decision:** two pieces, in this order.

1. `cargo publish --dry-run -p <crate>` runs in PR CI for each publishable
   crate. This catches missing metadata, uncommitted files, and path-dependency
   leaks on the PR, not on the tag. It is also the mechanical enforcement of the
   purity rule: a QL crate that grows a `path` dependency on a workspace crate
   cannot be dry-run published, so CI fails.
2. A `publish-crates.yml` job triggered on the release-please tag, running
   `cargo publish` per crate with `CARGO_REGISTRY_TOKEN`. Both crates are leaves,
   so there is no inter-crate publish ordering to get right.

The publish step is driven by the QL crates' own release train (D9), so a parser
release does not wait on the monorepo's.

### D9 — Standalone release PRs for the QL crates

The QL crates are already half-independent: `include-component-in-tag` is `true`
(tags read `logql-parser-v0.1.2`) and neither is in the `linked-versions`
`signaldb-core` group, so their versions already move on their own — `logql`
reached 0.1.2 while the core group sat at 0.3.0, *while sharing the main
config*. What they share with everything else is the release _PR_.

**`separate-pull-requests` is a per-package option**, not root-only. The schema
settles it: `packages.*` refs `ReleaserConfigOptions`, and
`separate-pull-requests` is one of its 34 properties. So the two crates can be
marked standalone inside the existing config while the other eighteen keep
sharing a single release PR.

**Decision: one config, two packages marked standalone.**

```json
"src/logql":   { "release-type": "rust", "component": "logql-parser",
                 "separate-pull-requests": true },
"src/traceql": { "release-type": "rust", "component": "traceql-parser",
                 "separate-pull-requests": true }
```

`.github/workflows/release-please.yml` gains a `publish-ql-crates` job gated on
the per-package `src/logql--release_created` / `src/traceql--release_created`
outputs — each crate on its own, so releasing one never republishes the other.
Both are leaves, so there is no publish ordering. The heavy jobs (musl, docker,
UI, plugin) stay gated on `src/signaldb-bin--release_created` and skip when only
a parser releases.

`"release-type": "rust"` is stated explicitly on both: manifest mode defaults to
the Node strategy, which would look for a `package.json` instead of
`Cargo.toml`.

**Rejected: a second release-please instance** with its own config, manifest,
and workflow. It was built that way first, on the mistaken belief that
`separate-pull-requests` was root-only. Everything that made it awkward was
self-inflicted by the split: the two instances would each try to manage the
other's release PR unless given distinct `label`/`release-label` values, the
`cargo-workspace` plugin could not be carried over without proposing bumps for
packages the main train owns, and two configs plus two manifests had to be kept
from ever listing the same package. A per-package flag has none of that.

**What the split would have bought**, and this loses: a parser release is now
blocked if the shared release *PR* is unmergeable. Except it is not — that is
the whole point of `separate-pull-requests` on these two packages: they get
their own PR, which merges independently of the core one.

### D8 — Enforcing the rule after the change lands

A boundary that only a reviewer checks decays. Three cheap guards, in
increasing order of strictness:

- `cargo publish --dry-run` in CI (D7) — catches an unpublishable _package_: a
  bare `path` dependency, missing metadata, uncommitted files.
- **An explicit purity assertion, because the dry-run is not one.**
  **Verified during implementation:** with `datafusion.workspace = true` added
  to `src/traceql/Cargo.toml`, `cargo publish --dry-run -p traceql-parser`
  reports `Packaged 9 files` and exits clean. The dry-run checks that a crate
  _can_ be published, not that it stayed pure — and a dependency on the query
  engine is perfectly publishable. The guard that actually enforces the rule
  (`xtask/ql-purity.sh`) reads `cargo metadata` and fails if any dependency is
  a workspace member, has a `path`/`git` source, or is in the FDAP set. It was
  proven red against the same probe before being wired into CI. Without it, the
  boundary rests on reviewer attention, which is what D8 exists to avoid.

`cargo deny` needs no allowlist work: AGPL-3.0 and Apache-2.0 are both already
present in the workspace, and D2 introduces no new licence identifier.

## Risks / Trade-offs

| Risk                                                        | Mitigation                                                                                                                                                                           |
| ----------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Extraction silently changes a Tempo search response         | Move the existing tests with the code; add integration coverage asserting one 400 case and one 501 case end-to-end through the router before touching anything (task 1.x, TDD-first) |
| `ParseError` collapses the 400/501 distinction              | D3; pinned by test                                                                                                                                                                   |
| Published AST churns as LogQL/TraceQL parity work continues | `#[non_exhaustive]` (D5); 0.x versioning until the grammar settles                                                                                                                   |
| AGPL narrows who can depend on the published crates         | Accepted, not mitigated — it is the licence of the languages being implemented (D2)                                                                                                  |
| Two release-please instances fight over release PRs         | Distinct `label`/`release-label` on the new workflow (D9, gotcha 1)                                                                                                                  |
| A `logql` bump silently stops nudging its dependents        | Both depend by `path` and neither is published; stated as a comment in the config (D9, gotcha 2)                                                                                     |
| Package/lib name mismatch confuses contributors             | Comment in the manifest and in the crate docs (D1)                                                                                                                                   |
| A future contributor adds `common` to a QL crate            | D8                                                                                                                                                                                   |

## Migration Plan

None. No persisted format, wire schema, or API changes. Revert is an ordinary
`git revert` — until the first `cargo publish`, after which a published version
cannot be unpublished (only yanked). That asymmetry is the reason D7 puts the
dry-run in PR CI and the real publish behind a release tag.

## Open Questions

1. Should `promql-parser` (third-party) be re-exported from a thin
   `signaldb`-side crate for symmetry, or is "we don't own that grammar" the
   honest answer? Current recommendation: the honest answer; add no crate.
