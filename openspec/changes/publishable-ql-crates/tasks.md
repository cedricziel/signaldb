# Tasks

## 1. Pin current behaviour before moving anything

- [ ] 1.1 Add integration coverage in `tests-integration` for Tempo trace search
      that asserts, end to end through the router: an accepted `q` returns the
      expected traces; a malformed `q` returns 400; a valid-but-unsupported `q`
      (e.g. `{ .foo != "bar" }`) returns 501 with the operator named. Run
      `cargo test -p tests-integration` and confirm it passes **before** any
      extraction — this is the regression net, not a failing test.
- [ ] 1.2 Inventory the existing unit tests in
      `src/querier/src/query/search_filter.rs`, splitting them into
      parse-only (move to `traceql`) and lowering (stay). Record the split in
      the PR description so reviewers can verify nothing was dropped.

## 2. The `traceql` crate — TDD

- [ ] 2.1 Create `src/traceql` (`traceql-parser` package, `[lib] name =
  "traceql"`, `thiserror` only). Add to workspace `members` and
      `default-members`. Empty `lib.rs`; confirm `cargo build -p traceql-parser`
      succeeds and `cargo tree -p traceql-parser` shows no workspace crate.
- [ ] 2.2 **Failing test first**: port the parse-only tests from 1.2 into
      `src/traceql/tests/`, plus two new ones asserting
      `ParseError::Syntax` vs `ParseError::Unsupported` are returned for a
      malformed and an unsupported query respectively. Confirm they fail to
      compile/run for the stated reason (no parser yet).
- [ ] 2.3 Implement `Selector`, `FilterValue`, `Condition`, `ParseError`
      (`#[non_exhaustive]`, thiserror) and `parse_traceql`, moving
      `split_top_level_and`, `parse_traceql_clause`, `take_value`, and
      `unscoped_selector` from `search_filter.rs`. Preserve every error message
      string verbatim. `cargo test -p traceql-parser` green.
- [ ] 2.4 Module-level `//!` docs stating the supported subset, the
      `Syntax`/`Unsupported` contract, and the package/lib name mismatch (D1).

## 3. Rewire the querier

- [ ] 3.1 Add `traceql-parser` to `src/querier/Cargo.toml`. Add
      `impl From<traceql::ParseError> for QuerierError` mapping `Syntax →
  InvalidInput` and `Unsupported → Unsupported`.
- [ ] 3.2 Reduce `search_filter.rs` to lowering: convert `Condition::to_expr`
      to a free function `to_expr(&Condition, &AttrContext)` (foreign type — no
      inherent impl) and update every call site. Keep `materialized_expr`,
      `map_attribute_expr`, `attribute_expr`, and `parse_tags` (D4).
- [ ] 3.3 `cargo test -p querier` green; `cargo test -p tests-integration` green
      — 1.1 must still pass unmodified. If a test needed editing, the extraction
      changed behaviour: stop and reconcile.

## 4. `logql` publishing readiness (no logic change)

- [ ] 4.1 Rename the package to `logql-parser` with `[lib] name = "logql"`.
      Update `src/querier/Cargo.toml` and `src/router/Cargo.toml` dependency
      names; no `use logql::…` site changes. Confirm with
      `cargo test -p querier -p router`.
- [ ] 4.2 Add `#[non_exhaustive]` to the public enums (`Token`, `PipelineStage`,
      `RangeFunction`, `AggregationFunction`, `BinOp`, `MatchOp`, `LineFilterOp`,
      `FilterOp`, `LexError`, `ParseError`, …). `cargo test -p logql-parser`
      green.
- [ ] 4.3 Add `README.md` to `src/logql` and `src/traceql` (what the crate is,
      supported subset, licence, "extracted from SignalDB" pointer).
- [ ] 4.4 Add `repository`, `readme`, `keywords`, `categories`, and
      `documentation` to both manifests.

## 5. Licences track upstream (D2)

- [ ] 5.1 Confirm `logql-parser` and `traceql-parser` are both `AGPL-3.0`
      (matching Loki and Tempo). No relicensing, no `cargo deny` allowlist
      change — verify with `cargo deny check`.
- [ ] 5.2 `tempo-api`: `license = "Apache-2.0"` → `"AGPL-3.0"`, matching Grafana
      Tempo, whose `tempopb` protobuf definitions `src/tempo-api/proto/tempo.proto`
      copies. Update `src/tempo-api/README.md` if it states a licence.
- [ ] 5.3 `prometheus-api`: `license = "AGPL-3.0"` → `"Apache-2.0"`, matching
      Prometheus.
- [ ] 5.4 `tempo-api`: `publish = true` → `false`. It has no publication
      metadata and is not part of this change's release train; wiring it for
      release is its own job.
- [ ] 5.5 Add a short note to `docs/contributing/` (task 7.3) stating the rule —
      a compat crate takes the licence of the project whose language or API it
      re-implements; first-party crates (`common`, `query-ir`, the services)
      stay AGPL-3.0 regardless.

## 6. Standalone release train and publication (D9)

- [ ] 6.1 Create `release-please-config.ql.json` and
      `.release-please-manifest.ql.json` covering only `src/logql` and
      `src/traceql`. Carry `logql`'s current version (`0.1.2`) over verbatim.
      Set an explicit `component` per package (`logql-parser`,
      `traceql-parser`) so tags match the crates.io names.
- [ ] 6.2 Remove `src/logql` from `release-please-config.json` and
      `.release-please-manifest.json`. Add a comment recording that the
      `cargo-workspace` plugin no longer sees these crates (D9, gotcha 2).
- [ ] 6.3 Add `.github/workflows/release-ql-crates.yml`: release-please with the
      `.ql` config/manifest and **distinct `label` / `release-label` inputs**
      (D9, gotcha 1 — sharing `autorelease: pending` makes the two instances
      fight over each other's PRs).
- [ ] 6.4 In that workflow, gate a `cargo publish` step per crate on the
      per-package `--release_created` output, using `CARGO_REGISTRY_TOKEN`. Both
      crates are leaves — no publish ordering.
- [ ] 6.5 Add `cargo publish --dry-run -p logql-parser -p traceql-parser` to
      `.github/workflows/ci.yml` (D7/D8). Verify it fails when a `path`
      dependency on a workspace crate is added, then revert the probe.
- [ ] 6.6 Configure the `CARGO_REGISTRY_TOKEN` repository secret.
- [ ] 6.7 `cargo machete --with-metadata`, `cargo fmt`,
      `cargo clippy --workspace --all-targets --all-features`,
      `cargo deny check`.
- [ ] 6.8 After the first release: verify `cargo add logql-parser` /
      `traceql-parser` works from an empty scratch crate outside the workspace,
      that `cargo tree` shows no SignalDB dependency, and that the main release
      train is unaffected (its next release PR still covers the other packages
      and does not reference the QL crates).

## 7. Docs and skills

- [ ] 7.1 Update the `crate-map` skill: `traceql` is a new workspace member and
      the `logql`/`traceql` package-vs-lib naming needs a line.
- [ ] 7.2 Update the `tempo-api` skill where it describes TraceQL support, so
      the supported subset points at the crate that now owns it.
- [ ] 7.3 Add a docs page (route via the `docs` skill — `docs/contributing/`
      audience) covering the QL front-end rule: parse/validate only, no product
      dependency, how the CI guard enforces it, and where lowering lives.
- [ ] 7.4 Run the docs-freshness gate **after committing**, and again after any
      fix (it diffs committed history and cascades code → doc → skill).

## 8. Ship

- [ ] 8.1 Run `/simplify` over the changed code.
- [ ] 8.2 File the tracking issue this change lacks and add `Closes #N` to the
      PR body. Split into a stack if the diff exceeds ~500 lines — the natural
      seam is §1–3 (extraction, behaviour-preserving) / §4–5 (publishing
      readiness) / §6 (publication).
- [ ] 8.3 Open the PR; check for CodeRabbit findings and act on them.
