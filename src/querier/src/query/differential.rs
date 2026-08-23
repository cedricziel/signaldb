//! # Compat-vs-IR differential harness (`ir-single-lowering`, design D2)
//!
//! Before any compat endpoint was rerouted onto [`super::ir_planner`], this
//! module lowered the same query through **both** paths — the existing
//! per-language lowering and `ql_ir::{traceql_to_ir, logql_to_ir}` +
//! [`super::ir_planner::plan_document`] — over an identical fixture, and
//! asserted the two agreed. A difference was a finding to explain, not a
//! failure to route around (design D2): it was triaged into exactly one of
//! three outcomes below, never silently papered over by loosening the
//! comparison.
//!
//! ## Status after §5 (task 5.4)
//!
//! §5 deleted the old TraceQL lowering (`search_filter.rs`'s `to_expr` and
//! `trace.rs`'s dedicated filter loop) once every difference above was
//! triaged and the rollout switch (D3) proved the IR path agreed. There is
//! no old TraceQL/tags path left to differentially compare against, so every
//! TraceQL/tags test below is now an **expected-result regression pin on the
//! single (IR) lowering** — what the old-vs-new comparison last proved true,
//! kept as a fixed expectation rather than a live comparison. The module
//! keeps its name and stays a **permanent regression suite**: LogQL still
//! has a real old-vs-new differential for the enumerated D5 fallback set
//! (`KNOWN_INEXPRESSIBLE_LOGQL`, below) until a future change closes that
//! gap too (§5 only deletes what `ql_ir` already covers, task 5.2) — so
//! "differential" still describes part of what this module does, and the
//! trace-side tests that lost their comparison are the harness's own
//! regression net for the deletion, not dead weight to relocate.
//!
//! ## Comparison rules
//!
//! - **Filter-plan equivalence** compares `DataFrame::into_optimized_plan()`
//!   (via `display_indent()` strings), not the raw expression tree: the two
//!   paths legitimately build different-shaped expressions that DataFusion
//!   normalises to the same plan.
//! - Both sides are made comparable **only by construction**: the same
//!   `SessionContext`/fixture table, the same explicit time window, and the
//!   same explicit projection (a document's `fields` is overridden to name
//!   exactly the columns the old side selects) — never by editing plan text
//!   after the fact. `ql_ir::{traceql_to_ir, logql_to_ir}` never set
//!   `fields`, so every helper below sets it post-hoc to the fixed minimal
//!   column each test needs; this isolates the comparison to *filter*
//!   lowering, which is what the two paths actually duplicate today.
//!   Projection lowering (`apply_projection` vs. the compat result
//!   assembly) is not itself duplicated code and is out of scope here — see
//!   task 2.3b, deferred until the rollout switch (§3/§4) makes an
//!   endpoint-level comparison possible.
//! - **Rejection classification** (task 2.3a) compares `Accept` /
//!   `Reject(InvalidInput)` / `Reject(Unsupported)` between the two paths for
//!   every corpus query, including ones that never produce a plan to diff.
//!   `ql_ir`'s `LowerError::Inexpressible` where the old path accepts is not
//!   a failure — it is the D5 fallback set, named in
//!   [`KNOWN_INEXPRESSIBLE_LOGQL`].
//!
//! ## Corpus
//!
//! Seeded from (task 2.1):
//!
//! 1. `tests-integration/tests/router_tempo_endpoints.rs`'s
//!    `test_search_filters_are_applied` — 7 TraceQL `q` strings, 3 `tags`
//!    strings (`TRACEQL_CORPUS_CLASS`, `TAGS_CORPUS`).
//! 2. `tests-integration/tests/logql_queries.rs` — 4 LogQL strings, one of
//!    them a metric query (`LOGQL_LOG_CORPUS`, `LOGQL_METRIC_CORPUS_ROWS`).
//! 3. `src/ql-ir/tests/{traceql,logql}.rs` — every query string their test
//!    functions assert on, folded into the same four consts (dominates the
//!    count: it is what caught the two real findings below).
//!
//! **`tests-integration/tests/query_parity.rs` contributed nothing.** Its
//! name suggested a query-language corpus (design.md and tasks.md both cite
//! it), but its actual content (verified before writing this harness, per
//! the project's "check the premise" convention) is CLI/MCP *operation*
//! surface parity — no TraceQL/LogQL/PromQL query text anywhere in the file.
//! That premise was stale by the time this change was proposed; the corpus
//! below draws from the two files that do carry query text instead.
//!
//! Total corpus: 18 TraceQL `q` strings + 6 `tags` strings + 8 LogQL log
//! queries + 7 LogQL metric queries = **39 queries**, plus the seven
//! adversarial cases (task 2.2) below.
//!
//! ## Triage table (task 2.4)
//!
//! | Query | Outcome | Note |
//! |---|---|---|
//! | `{ status = error }`, `{ kind = server }` | **(1) `ql_ir` was wrong, fixed** | `traceql_to_ir` passed the TraceQL spelling through verbatim; the traces table stores these enums Title-cased (`status_code_to_str`/`span_kind_to_str`). Fixed in `ql-ir::traceql_lower` (see `normalize_status`/`normalize_kind`); the pinned `ql-ir` unit test was updated to the corrected values. Commit: `fix(ql-ir): normalize TraceQL status/kind values to their stored casing`. |
//! | **Every LogQL line filter** (`|=`/`!=`/`|~`/`!~`) | **(1) `LogicalSchema` was wrong, fixed in §4 (D6)** | `ql_ir::logql_lower::line_filter` lowers to `Leaf{field:"body", op:Contains}`, but `LogicalSchema::core()` marked `logs.body` `RetrievalOnly` — `plan_document`'s validation rejected every one of these documents. This was *not* the D5 fallback case: `logql_to_ir` returns `Ok`, not `Inexpressible`, so the rejection surfaced only after the point a §4 fallback switch would check. Fixed in `LogicalSchema::core()` (task 4.0a): `body` is filterable for string operators now, resolving to `ValueType::String` like any other string field — ordered/numeric operators get no special allowance. Pinned in `logql_line_filter_agrees_on_optimized_plan`. Commit: `feat(query-ir): make the log body filterable for string operators`. |
//! | A LogQL stream-selector `!=`/`!~`/`=""` against a key some rows lack | **(1) `ql_ir` was wrong, fixed in §4 (D9)** | The old LogQL lowering's `!=`/`!~` explicitly matches an absent key (`e.is_null().or(e.not_eq(...))`, documented "mirroring the JSON path"); `ir_planner`'s `Predicate::Not` was a plain `not(...)`, which is NULL for a NULL input — the IR's stated Kleene semantics ("absent satisfies neither `field = x` nor `not(field = x)`"), not what Loki's compat surface promises. Fixed in `ql_ir::logql_lower::matcher` (task 4.0c): a negative matcher now ORs in "the field does not exist" explicitly — `!=` → `or[ne, not(exists)]`, `!~` → `or[not(regex), not(exists)]`, `=""` → `or[eq "", not(exists)]` (the old path never special-cased plain `Eq` against `""`, so that one is a new-path-only regression test, not an old/new pin — see `empty_string_equality_matches_an_absent_field_on_the_new_path`). `=~` stays a plain `regex`, the one documented corner. Pinned in `adversarial_absent_value_semantics_agrees` and `absent_value_semantics_also_agree_for_negative_regex`. Commit: `fix(ql-ir): keep Loki's absent-matches semantics for negative matchers`. |
//! | A LogQL stream-selector `!=`/`!~` against a **dedicated-column** label (`service_name`, `level`, `trace_id`, `span_id`) whose column is NULL on some rows | **(1) old path was wrong, fixed in §4 (review finding on #1393)** | D9 was applied to the JSON/map-attribute path (`map_attribute_expr`) and the materialized-label path (`materialized_label_expr`) but not to `logql.rs::column_expr`, the third of the three — its `Neq`/`Nre` lowered as plain `not_eq`/`not(regexp_like)` with no NULL handling, so a NULL dedicated column silently stayed excluded from a negative match while its two siblings (and the new path, which applies D9 uniformly regardless of which container a label resolves to) already included it. This was the *old* path's own inconsistency between three near-identical functions, not a genuine new-vs-old difference — Loki's absent-matches promise carries no dedicated-column exception. Fixed in `column_expr` to match its siblings. Pinned in `dedicated_column_negative_matchers_also_match_a_null_row`. Commit: `fix(querier): match absent dedicated columns for negative LogQL matchers`. |
//! | `{ .service.name = "api" }`, `{ .http.method = "GET" }` (TraceQL, unscoped), `{k8s_namespace="prod"}` (LogQL, no known column) | **(3) genuinely different meaning — reported, not picked** | The old path ORed the match across every container (`map_attribute_expr("span_attributes",..).or(map_attribute_expr("resource_attributes",..))`) — matched if *any* container had the value. `ir_planner`'s bare-name resolution COALESCEs across containers by priority (span/log, then scope, then resource) and compares *once* — if a higher-priority container has the key at all (regardless of value), lower-priority containers are never consulted. The compat surfaces adopted the IR's rule (D8) — now the deliberate, single behaviour, pinned in `unscoped_attribute_coalesces_by_container_priority` and `unscoped_attribute_coalescing_reproduces_at_the_endpoint` (traces) with a fixture where the same key holds *different* values in two containers. The LogQL corpus's `{k8s_namespace="prod"}` hits the identical mechanism (skipped via `KNOWN_COMBINING_DIVERGENCE_LOGQL_LOG`, no separate pinned test — the old LogQL path stays in place until §5.2). |
//! | `{ span.http.method = "GET" }` against a table with a promoted `label_http_method` column | **(1) `ir_planner` was wrong, fixed in §3** | The old `search_filter::to_expr` (deleted in §5) checked `materialized_column_name(key)` against the **bare** attribute key (`"http.method"` → `label_http_method`), matching how the compactor actually names promoted columns (`attr_promotion::materialized_keys_of` keys off the raw `attr_key`, never the TraceQL-scoped spelling). `ir_planner::SchemaResolver::column_for` used to compute `materialized_column_name(field)` against the **scope-qualified** logical field (`"span.http.method"` → `label_span_http_method`), which no real promoted column is ever named — so `ir_planner` always took the `get_field` (JsonPath) branch for a scope-qualified attribute, never the promoted column, even when one existed. Fixed in `ir_planner::SchemaResolver::column_for` (task 3.0): strip the scope qualifier before materializing, the same way `Lowering::qualified_attr` already does for the unpromoted extraction path. Pinned in `promoted_attribute_resolves_to_its_column`. Commit: `fix(querier): resolve promoted columns for scope-qualified IR fields`. |
//! | `sum by (StatusCode) (count_over_time(...))` (mixed-case attribute grouping) | **(1) old path is wrong, still open (issue #1392)** | #1070's fix (`ident()` not `col()` for a group-column alias) only touched `ir_planner.rs`. The *old* LogQL metric path (`logs.rs`'s `execute_plan`) has the identical unfixed bug for grouping by a mixed-case attribute label, but this query is `Accept` (not the D5 fallback set), so §5.3's switch removal made the old path's bug unreachable through the public API — filed as issue #1392 (task 4.0d), not fixed here, and no longer pin-able as a live divergence; `mixed_case_grouping_label_groups_correctly` now pins only the IR path's correct grouping. |
//! | `count_over_time(...)`/`rate(...)`/`sum_over_time(...)` etc. with **no** outer `by` | **(1) `ql_ir` was wrong, fixed in §4 (D7)** | Real Loki returns one series per matching *stream* for a bare range aggregation. The old path implemented that by defaulting the range aggregate's grouping to `SERIES_COLUMNS` (`service_name`, `severity_text`) when ungrouped. `ql_ir::logql_to_ir` used to emit `by: []` for the same shape, collapsing every matching row into one count. Fixed in `ql_ir::logql_lower::lower_metric_query` (task 4.0b): an empty grouping now defaults to `ql_ir::STREAM_IDENTITY` (`["service.name", "severity_text"]`), pinned against `logs.rs::SERIES_COLUMNS` through the real `SchemaResolver` by `ql_ir_stream_identity_matches_series_columns` (this crate has no access to the real schema, so the mapping is asserted on the querier side). Pinned in `adversarial_ungrouped_range_aggregation_default_grouping_agrees` (two `api` rows of different severities: both paths now produce 2 rows). Commit: `fix(ql-ir): group an ungrouped range aggregation by the stream identity`. |
//! | `sum(count_over_time(...))` (an **explicit** vector aggregation with no `by()`) | **(1) `ql_ir` was wrong, fixed (CodeRabbit review on #1393); old path also wrong, filed as #1394** | D7's stream-identity default was first applied too broadly: `lower_metric_query` treated *any* empty grouping — including `vector_grouping`'s `Ok(Vec::new())` for an explicit `v.grouping == None` — as "apply the default", when an explicit ungrouped vector aggregation means the opposite (collapse to one series, same as an empty `by` anywhere else). Fixed by applying `STREAM_IDENTITY` only in the bare-`MetricQuery::Range` arm, pinned in `ql-ir`'s `explicitly_ungrouped_vector_aggregation_collapses_to_one_series`. Checking that fix against the old path (not assumed) surfaced a *second*, independent finding: `logs.rs::execute_plan` never collapses an ungrouped `sum(...)` either — `sum`'s `outer_agg` stays `None` ("folds into the grouped range aggregate"), so the old path's grouping branch treats it identically to a bare range aggregation (`SERIES_COLUMNS`, no reduction pass), producing one row per stream instead of one row total. This is the old path's *own* bug (filed as #1394, sibling to #1392) — but this query is `Accept`, not the D5 fallback set, so §5.3's switch removal made the bug unreachable through the public API, and it can no longer be pinned as a live divergence. `explicitly_ungrouped_vector_aggregation_collapses_to_one_row` now pins only the IR path's correct collapse. |
//! | every other corpus query | **match** | See the `*_corpus_*` tests below. |
//!
//! ## `KNOWN_INEXPRESSIBLE_LOGQL` (task 2.3a)
//!
//! LogQL metric queries `ql_ir::logql_to_ir` refuses (`LowerError::Inexpressible`)
//! while the old path (`plan_metric_query` + `LogsService::execute_plan`)
//! accepts — the D5 fallback set. Every entry here is asserted by
//! `logql_metric_known_inexpressible_matches_old_path_accepts` to (a) still
//! be accepted by the old path and (b) still be refused by `ql_ir`, so this
//! list cannot silently go stale in either direction.
//!
//! | Query | Reason (D5) |
//! |---|---|
//! | `sum(rate({a="b"}[5m])) / sum(rate({c="d"}[5m]))` | Cross-series arithmetic: needs computation *between* series: the IR aggregates within one. |
//! | `label_replace(rate({a="b"}[5m]), "x", "$1", "y", "(.*)")` | Rewrites a label on the finished matrix; no IR stage does this. |
//! | `avg by (service_name) (count_over_time({a="b"}[1m]))` | `avg` of counts does not reproduce over partial results the way `sum`/`min`/`max` do (non-collapsible outer aggregation). |
//!
//! ## Open questions answered (tasks 2.5, 2.6)
//!
//! **2.5 (aggregates):** optimized-plan string equality does **not** hold for
//! LogQL metric queries — the old path's `execute_plan` and the IR's
//! `lower_aggregate` build structurally different aggregate/sort trees (the
//! old path's per-plan grouping-column resolution, topk/sort/label_replace
//! post-passes, and vector-binary join logic have no IR-side counterpart to
//! diff against). The weaker equivalence `logql_metric_row_level_equivalence`
//! implements instead: execute both over the identical fixture, project onto
//! the columns both share (`bucket`, group labels, a value column — value
//! columns are named differently on each side and are normalised before
//! comparison, documented at the call site), sort, and compare rows.
//!
//! **2.6 (expression-shape tests):** grepped `search_filter.rs` and
//! `logql.rs`/`logql_metric.rs`/`logs.rs` for an assertion on `{:?}`/`Debug`
//! output of a lowered expression. Only `search_filter.rs`'s own unit tests
//! (`status_maps_to_storage_values`, `attribute_expr_matches_serialized_fragment`,
//! `map_attribute_tables_use_get_field_extraction`,
//! `materialized_attribute_routes_to_its_column`) do this, and they exercise
//! exactly the lowering half task 5.1 deletes along with them — nothing
//! outside `search_filter.rs` asserts on its shape. No test needed rewriting
//! for this change; §5's deletion takes those tests with the code.

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, BooleanArray, Float64Array, MapBuilder, MapFieldNames, RecordBatch, StringArray,
    StringBuilder, TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use datafusion::catalog::memory::{MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::catalog::{CatalogProvider, MemTable, SchemaProvider};
use datafusion::prelude::{DataFrame, SessionContext};

use common::query_ir::{Document, FieldResolver, Range, Resolved, ResultEnvelope, Stage};
use logql::Expr as LogqlExpr;

use super::MetricQueryParams;
use super::SearchQueryParams;
use super::error::QuerierError;
use super::ir_planner::{SchemaResolver, SourcePlan, plan_document};
use super::logql::{AttrContext, log_query_filter_with_columns};
use super::logs::{LogsService, materialized_columns_of};
use super::search_filter;
use super::table_lookup::optional_table;
use super::trace::TraceService;

const TENANT: &str = "t";
const DATASET: &str = "d";
/// The fixed window every corpus query is planned over. `ql_ir` accepts a
/// bare integer string as absolute nanoseconds (`parse_timestamp_literal`).
const FROM_NS: &str = "0";
const TO_NS: &str = "1000";
const FROM: i64 = 0;
const TO: i64 = 1000;

// ---------------------------------------------------------------------------
// Corpus (task 2.1)
// ---------------------------------------------------------------------------

/// Tempo `tags` (logfmt) strings, from `test_search_filters_are_applied`,
/// plus three adversarial values (blocker found in review of PR #1391) that
/// a text round-trip through TraceQL mishandled: `parse_tags`/`take_value`
/// have no escape syntax of their own, so a backslash, an embedded `"`, and
/// a literal `&&` inside a key all pass straight through as ordinary
/// characters, but TraceQL's own string-literal grammar does not accept
/// them unchanged (see `tags_to_ir`'s module doc).
const TAGS_CORPUS: &[&str] = &[
    "service.name=filter-test-service",
    "service.name=does-not-exist",
    "justaword",
    r"file.path=C:\Users\foo",
    r#"weird.key=va"lue"#,
    "weird&&key=value",
];

/// LogQL log-query strings (no metric functions). Sources: `logql_queries.rs`
/// (URL-decoded) and `src/ql-ir/tests/logql.rs`'s corpus.
const LOGQL_LOG_CORPUS: &[&str] = &[
    r#"{service_name="api"}"#,
    r#"{service_name="api"} |= "boom""#,
    r#"{service_name="api"} |~ "err.*""#,
    r#"{service_name="api"} !="boom""#,
    r#"{service_name="api", level="error"} |= "timeout""#,
    r#"{service="api"}"#,
    r#"{job="api"}"#,
    r#"{k8s_namespace="prod"}"#,
];

/// The D5 fallback set (task 2.3a): valid LogQL metric queries the old path
/// lowers but `ql_ir::logql_to_ir` refuses as `Inexpressible` today. See the
/// module doc's table for why each is refused.
const KNOWN_INEXPRESSIBLE_LOGQL: &[(&str, &str)] = &[
    (
        r#"sum(rate({a="b"}[5m])) / sum(rate({c="d"}[5m]))"#,
        "cross-series arithmetic: the IR aggregates within one series, not between two",
    ),
    (
        r#"label_replace(rate({a="b"}[5m]), "x", "$1", "y", "(.*)")"#,
        "label_replace rewrites a label on the finished matrix; no IR stage does this",
    ),
    (
        r#"avg by (service_name) (count_over_time({a="b"}[1m]))"#,
        "avg of counts does not reproduce over partial results the way sum/min/max do",
    ),
];

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn map_field_named(name: &str) -> Field {
    let entries = Field::new(
        "entries",
        DataType::Struct(Fields::from(vec![
            Field::new("keys", DataType::Utf8, false),
            Field::new("values", DataType::Utf8, true),
        ])),
        false,
    );
    Field::new(name, DataType::Map(Arc::new(entries), false), true)
}

fn build_map(pairs: &[&[(&str, &str)]]) -> Arc<dyn Array> {
    let names = MapFieldNames {
        entry: "entries".to_string(),
        key: "keys".to_string(),
        value: "values".to_string(),
    };
    let mut b = MapBuilder::new(Some(names), StringBuilder::new(), StringBuilder::new());
    for row in pairs {
        for (k, v) in *row {
            b.keys().append_value(k);
            b.values().append_value(v);
        }
        b.append(true).unwrap();
    }
    Arc::new(b.finish())
}

fn register(ctx: &SessionContext, table: &str, schema: Arc<Schema>, batch: RecordBatch) {
    let mem = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    let sp = Arc::new(MemorySchemaProvider::new());
    sp.register_table(table.to_string(), Arc::new(mem)).unwrap();
    let cat = Arc::new(MemoryCatalogProvider::new());
    cat.register_schema(DATASET, sp).unwrap();
    ctx.register_catalog(TENANT, cat);
}

/// A `traces` table shaped for the TraceQL/tags corpus: intrinsics
/// (`span_name`, `service_name`, `status_code`, `span_kind`) stored in the
/// real conversion's Title-cased form, plus `span_attributes` /
/// `resource_attributes` maps carrying the keys the corpus filters on.
fn traces_fixture() -> SessionContext {
    let schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
        Field::new("parent_span_id", DataType::Utf8, true),
        Field::new("span_name", DataType::Utf8, false),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("start_time_unix_nano", DataType::Int64, false),
        Field::new("duration_nanos", DataType::Int64, false),
        Field::new("status_code", DataType::Utf8, true),
        Field::new("span_kind", DataType::Utf8, true),
        map_field_named("span_attributes"),
        map_field_named("resource_attributes"),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["t0", "t1"])),
            Arc::new(StringArray::from(vec!["s0", "s1"])),
            Arc::new(StringArray::from(vec![None, Some("s0")])),
            Arc::new(StringArray::from(vec!["GET /api", "POST /x"])),
            Arc::new(StringArray::from(vec!["api", "web"])),
            Arc::new(datafusion::arrow::array::Int64Array::from(vec![10_i64, 20])),
            Arc::new(datafusion::arrow::array::Int64Array::from(vec![
                100_i64, 200,
            ])),
            Arc::new(StringArray::from(vec![Some("Error"), Some("Ok")])),
            Arc::new(StringArray::from(vec![Some("Server"), Some("Internal")])),
            build_map(&[&[("http.method", "GET"), ("http.status_code", "500")], &[]]),
            build_map(&[&[("k8s.pod.name", "p"), ("service.name", "api")], &[]]),
        ],
    )
    .unwrap();
    let ctx = SessionContext::new();
    register(&ctx, "traces", schema, batch);
    ctx
}

/// Like [`traces_fixture`], plus a `label_http_method` column mirroring the
/// compactor's promotion of the bare attribute key `http.method` (see
/// `attr_promotion::materialized_keys_of`) — promotion duplicates the value
/// into a column without removing it from `span_attributes`, so both stay
/// present. Used by the promoted-attribute adversarial case.
fn traces_promoted_fixture() -> SessionContext {
    let mut fields = vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
        Field::new("parent_span_id", DataType::Utf8, true),
        Field::new("span_name", DataType::Utf8, false),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("start_time_unix_nano", DataType::Int64, false),
        Field::new("duration_nanos", DataType::Int64, false),
        Field::new("status_code", DataType::Utf8, true),
        Field::new("span_kind", DataType::Utf8, true),
        map_field_named("span_attributes"),
        map_field_named("resource_attributes"),
    ];
    fields.push(Field::new("label_http_method", DataType::Utf8, true));
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["t0", "t1"])),
            Arc::new(StringArray::from(vec!["s0", "s1"])),
            Arc::new(StringArray::from(vec![None, Some("s0")])),
            Arc::new(StringArray::from(vec!["GET /api", "POST /x"])),
            Arc::new(StringArray::from(vec!["api", "web"])),
            Arc::new(datafusion::arrow::array::Int64Array::from(vec![10_i64, 20])),
            Arc::new(datafusion::arrow::array::Int64Array::from(vec![
                100_i64, 200,
            ])),
            Arc::new(StringArray::from(vec![Some("Error"), Some("Ok")])),
            Arc::new(StringArray::from(vec![Some("Server"), Some("Internal")])),
            // The map keeps the same key/value promotion duplicated it from.
            build_map(&[&[("http.method", "GET")], &[("http.method", "POST")]]),
            build_map(&[&[], &[]]),
            Arc::new(StringArray::from(vec![Some("GET"), Some("POST")])),
        ],
    )
    .unwrap();
    let ctx = SessionContext::new();
    register(&ctx, "traces", schema, batch);
    ctx
}

/// A `logs` table for the LogQL corpus: `service_name`/`severity_text`
/// dedicated columns, a `duration` numeric *attribute* (for `unwrap`,
/// stored in `log_attributes` like any other unpromoted label — LogQL's
/// `unwrap <label>` names a logical field, resolved the same
/// container-coalescing way any other bare attribute reference is; see
/// `ql_ir::logql_lower` and `ir_planner::Lowering::attr_expr`), and
/// `log_attributes`/`resource_attributes` maps. Row 2 deliberately lacks
/// `region` (an attribute present on the other rows) for the absent-value
/// adversarial case.
fn logs_fixture() -> SessionContext {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("body", DataType::Utf8, true),
        Field::new("service_name", DataType::Utf8, true),
        Field::new("severity_text", DataType::Utf8, true),
        Field::new("trace_id", DataType::Utf8, true),
        Field::new("span_id", DataType::Utf8, true),
        map_field_named("log_attributes"),
        map_field_named("resource_attributes"),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(TimestampNanosecondArray::from(vec![10_i64, 20, 30])),
            Arc::new(StringArray::from(vec![
                "boom: connection refused",
                "request served",
                "slow response",
            ])),
            Arc::new(StringArray::from(vec!["api", "api", "web"])),
            Arc::new(StringArray::from(vec!["ERROR", "INFO", "WARN"])),
            Arc::new(StringArray::from(vec![Some("t1"), Some("t2"), Some("t3")])),
            Arc::new(StringArray::from(vec![Some("s1"), Some("s2"), Some("s3")])),
            build_map(&[
                &[("region", "eu"), ("duration", "1.5")],
                &[("region", "eu"), ("duration", "2.5")],
                // Row 2 has no `region` key at all — the absent-value case.
                &[("duration", "0.5")],
            ]),
            build_map(&[&[], &[], &[]]),
        ],
    )
    .unwrap();
    let ctx = SessionContext::new();
    register(&ctx, "logs", schema, batch);
    ctx
}

/// A `logs` table with a `label_k8s_namespace` materialized column
/// (bare-key promotion) whose value collides with nothing; used only to
/// carry mixed-case grouping labels for the `#1070` adversarial case (the
/// bug was in `ir_planner`'s aggregate `by`-column referencing, exercised
/// identically regardless of which table backs it, so a small dedicated
/// fixture keeps that test independent of the rest of the corpus).
fn logs_mixed_case_fixture() -> SessionContext {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("body", DataType::Utf8, true),
        Field::new("service_name", DataType::Utf8, true),
        Field::new("severity_text", DataType::Utf8, true),
        Field::new("trace_id", DataType::Utf8, true),
        Field::new("span_id", DataType::Utf8, true),
        map_field_named("log_attributes"),
        map_field_named("resource_attributes"),
        Field::new("label_StatusCode", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(TimestampNanosecondArray::from(vec![10_i64, 20])),
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(StringArray::from(vec!["api", "api"])),
            Arc::new(StringArray::from(vec!["INFO", "INFO"])),
            Arc::new(StringArray::from(vec![Some("t1"), Some("t2")])),
            Arc::new(StringArray::from(vec![Some("s1"), Some("s2")])),
            build_map(&[&[], &[]]),
            build_map(&[&[], &[]]),
            Arc::new(StringArray::from(vec!["200", "500"])),
        ],
    )
    .unwrap();
    let ctx = SessionContext::new();
    register(&ctx, "logs", schema, batch);
    ctx
}

fn is_map_column(df: &DataFrame, column: &str) -> bool {
    df.schema()
        .fields()
        .iter()
        .any(|f| f.name() == column && matches!(f.data_type(), DataType::Map(_, _)))
}

// ---------------------------------------------------------------------------
// Accept/reject classification (task 2.3a)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Class {
    Accept,
    Invalid,
    Unsupported,
}

fn class_of_querier_err(e: &QuerierError) -> Class {
    match e {
        QuerierError::InvalidInput(_) => Class::Invalid,
        QuerierError::Unsupported(_) => Class::Unsupported,
        other => panic!("unexpected error class in differential harness: {other}"),
    }
}

/// The TraceQL path's classification: `ql_ir::traceql_to_ir` alone (the
/// harness does not call `plan_document` here — the document may still be
/// rejected once resolved against a schema, which the filter-plan tests
/// below cover for the accepted corpus already).
fn new_traceql_class(q: &str) -> Class {
    match ql_ir::traceql_to_ir(q, FROM_NS, TO_NS) {
        Ok(_) => Class::Accept,
        Err(ql_ir::LowerError::Parse(pe)) => class_of_querier_err(&QuerierError::from(pe)),
        Err(ql_ir::LowerError::Inexpressible(_)) => Class::Unsupported,
        Err(other) => panic!("unexpected TraceQL lowering error: {other}"),
    }
}

/// D4's `Condition`-to-IR shim, task 3.2's `tags_to_ir::conditions_to_predicate` —
/// the real production lowering, not a stand-in. (An earlier version of this
/// harness rendered the condition back into TraceQL text before that shim
/// existed; the blocker found in review of PR #1391 was exactly that a text
/// round-trip is unsound, so this now calls the real shim instead — see
/// `tags_to_ir`'s module doc.)
fn tags_condition_class(condition: &traceql::Condition) -> Class {
    match super::tags_to_ir::conditions_to_predicate(std::slice::from_ref(condition)) {
        Ok(_) => Class::Accept,
        Err(e) => class_of_querier_err(&e),
    }
}

fn old_logql_log_class(q: &str, ctx: &AttrContext) -> Class {
    let parsed = match logql::parse(q) {
        Ok(LogqlExpr::Log(l)) => l,
        Ok(LogqlExpr::Metric(_)) => panic!("{q}: expected a log query, got a metric query"),
        Ok(other) => panic!("{q}: unexpected LogQL form {other:?}"),
        Err(e) => {
            return class_of_querier_err(&QuerierError::InvalidInput(e.to_string()));
        }
    };
    match log_query_filter_with_columns(&parsed, ctx) {
        Ok(_) => Class::Accept,
        Err(e) => class_of_querier_err(&e),
    }
}

fn new_logql_log_class(q: &str) -> Class {
    match ql_ir::logql_to_ir(q, FROM_NS, TO_NS) {
        Ok(_) => Class::Accept,
        Err(ql_ir::LowerError::ParseLogql(e)) => {
            class_of_querier_err(&QuerierError::InvalidInput(e.to_string()))
        }
        Err(ql_ir::LowerError::Inexpressible(_)) => Class::Unsupported,
        Err(other) => panic!("unexpected LogQL lowering error: {other}"),
    }
}

// ---------------------------------------------------------------------------
// Filter-plan comparison (task 2.3)
// ---------------------------------------------------------------------------

/// The TraceQL search plan: `traceql_to_ir` then `plan_document`, with
/// `fields` overridden to a fixed minimal projection (comparison rules).
async fn new_traceql_plan(
    ctx: &SessionContext,
    q: &str,
    fields: &[&str],
) -> Result<String, QuerierError> {
    let mut doc = ql_ir::traceql_to_ir(q, FROM_NS, TO_NS)
        .map_err(|e| QuerierError::InvalidInput(e.to_string()))?;
    set_fields(&mut doc, fields);
    let (df, _) = plan_document(ctx, &doc, TENANT, DATASET, 0)
        .await?
        .expect("traces table is registered");
    Ok(optimized_plan_text(df))
}

/// The `tags` plan: `tags_to_ir::conditions_to_predicate` (task 3.2's real
/// shim, not a text round-trip) conjoined into one document, with `fields`
/// set to a fixed minimal projection — the same shape
/// `build_search_dataframe` builds for `tags` alone.
async fn new_tags_plan(
    ctx: &SessionContext,
    conditions: &[traceql::Condition],
    fields: &[&str],
) -> Result<String, QuerierError> {
    let predicate = super::tags_to_ir::conditions_to_predicate(conditions)?;
    let doc = Document {
        ir_version: 1,
        from: "traces".to_string(),
        range: Range {
            from: serde_json::Value::String(FROM_NS.to_string()),
            to: serde_json::Value::String(TO_NS.to_string()),
        },
        result: ResultEnvelope::Rows,
        fields: Some(fields.iter().map(|s| s.to_string()).collect()),
        pipeline: vec![Stage::Where(predicate)],
    };
    let (df, _) = plan_document(ctx, &doc, TENANT, DATASET, 0)
        .await?
        .expect("traces table is registered");
    Ok(optimized_plan_text(df))
}

async fn old_logql_log_plan(
    ctx: &SessionContext,
    q: &str,
    fields: &[&str],
) -> Result<String, QuerierError> {
    let parsed = match logql::parse(q).map_err(|e| QuerierError::InvalidInput(e.to_string()))? {
        LogqlExpr::Log(l) => l,
        other => panic!("{q}: expected a log query, got {other:?}"),
    };
    let mut df = optional_table(ctx, TENANT, DATASET, "logs")
        .await?
        .expect("logs table is registered");
    let attr_ctx = AttrContext {
        materialized: materialized_columns_of(&df),
        map_attrs: is_map_column(&df, "log_attributes"),
        attr_tokens: false,
    };
    df = super::table_lookup::time_window(df, FROM, TO)?;
    if let Some(filter) = log_query_filter_with_columns(&parsed, &attr_ctx)? {
        df = df.filter(filter)?;
    }
    df = df.select_columns(fields)?;
    Ok(optimized_plan_text(df))
}

async fn new_logql_log_plan(
    ctx: &SessionContext,
    q: &str,
    fields: &[&str],
) -> Result<String, QuerierError> {
    let mut doc = ql_ir::logql_to_ir(q, FROM_NS, TO_NS)
        .map_err(|e| QuerierError::InvalidInput(e.to_string()))?;
    set_fields(&mut doc, fields);
    let (df, _) = plan_document(ctx, &doc, TENANT, DATASET, 0)
        .await?
        .expect("logs table is registered");
    Ok(optimized_plan_text(df))
}

fn set_fields(doc: &mut Document, fields: &[&str]) {
    doc.fields = Some(fields.iter().map(|s| s.to_string()).collect());
}

fn optimized_plan_text(df: DataFrame) -> String {
    df.into_optimized_plan()
        .expect("plan optimizes")
        .display_indent()
        .to_string()
}

fn assert_plans_match(q: &str, old: &str, new: &str) {
    assert_eq!(
        old, new,
        "\n{q}\nold plan:\n{old}\nnew plan:\n{new}\nsee differential.rs's triage table for known exceptions"
    );
}

// ---------------------------------------------------------------------------
// Tests: TraceQL / tags
//
// §5 deleted the old lowering these tests used to compare against
// (`search_filter::to_expr`); each is now an expected-result pin on the
// single (IR) lowering instead of an old-vs-new comparison. The expected
// class/plan/row-count each asserts is exactly what both sides agreed on
// before the deletion (see git history for the prior old-vs-new form).
// ---------------------------------------------------------------------------

/// Every TraceQL `q` corpus query paired with its accept/reject
/// classification, pinned so a change to `ql_ir::traceql_to_ir` that starts
/// accepting or rejecting one of these is a deliberate decision, not a
/// silent drift. Determined empirically (`traceql::parse`'s `Syntax`/
/// `Unsupported` split maps to `Invalid`/`Unsupported` — see `error.rs`'s
/// `From<traceql::ParseError>`). Sources: `router_tempo_endpoints.rs`'s
/// `test_search_filters_are_applied` (the first 7) and
/// `src/ql-ir/tests/traceql.rs`'s corpus (the rest, deduplicated).
const TRACEQL_CORPUS_CLASS: &[(&str, Class)] = &[
    (r#"{ span.http.method = "GET" }"#, Class::Accept),
    ("{ duration > 100ms }", Class::Unsupported),
    (r#"{ span.x != "y" }"#, Class::Unsupported),
    (r#"{ span.a = "1" || span.b = "2" }"#, Class::Unsupported),
    ("notbraces", Class::Invalid),
    ("{ foo }", Class::Invalid),
    ("{ zzz = 1 }", Class::Invalid),
    (r#"{ name = "GET /api" }"#, Class::Accept),
    ("{ status = error }", Class::Accept),
    ("{ kind = server }", Class::Accept),
    (r#"{ resource.service.name = "api" }"#, Class::Accept),
    (r#"{ .service.name = "api" }"#, Class::Accept),
    (r#"{ resource.k8s.pod.name = "p" }"#, Class::Accept),
    (r#"{ .http.method = "GET" }"#, Class::Accept),
    (
        r#"{ resource.service.name = "api" && span.http.method = "GET" }"#,
        Class::Accept,
    ),
    ("{ span.http.status_code = 500 }", Class::Accept),
    ("{ span.ok = true }", Class::Accept),
    ("{}", Class::Accept),
];

#[tokio::test]
async fn traceql_corpus_classification_is_pinned() {
    for (q, expected) in TRACEQL_CORPUS_CLASS {
        assert_eq!(
            new_traceql_class(q),
            *expected,
            "{q}: classification drifted from its pinned expectation"
        );
    }
}

/// Every accepted `TRACEQL_CORPUS_CLASS` query plans without error. The plans
/// themselves are no longer pinned as fixed text (that only ever proved
/// agreement with the deleted old path); the fixed-text pins that still
/// carry independent value — a promoted column, a physical-column collision
/// — have their own dedicated tests below.
#[tokio::test]
async fn traceql_corpus_filters_plan_successfully() {
    let ctx = traces_fixture();
    let fields = ["trace_id"];
    for (q, class) in TRACEQL_CORPUS_CLASS {
        if *class != Class::Accept {
            continue;
        }
        new_traceql_plan(&ctx, q, &fields)
            .await
            .unwrap_or_else(|e| panic!("{q}: an accepted query must still plan: {e}"));
    }
}

/// An unscoped attribute present in two containers with two *different*
/// values (D8 of `ir-single-lowering`): the IR planner coalesces bare-field
/// resolution by container priority (span/log, then scope, then resource)
/// and compares once, rather than matching if *any* container has the
/// value — the compat surfaces' deliberate, single behaviour now (design
/// D8), not a divergence to reconcile.
#[tokio::test]
async fn unscoped_attribute_coalesces_by_container_priority() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
        Field::new("parent_span_id", DataType::Utf8, true),
        Field::new("span_name", DataType::Utf8, false),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("start_time_unix_nano", DataType::Int64, false),
        Field::new("duration_nanos", DataType::Int64, false),
        Field::new("status_code", DataType::Utf8, true),
        Field::new("span_kind", DataType::Utf8, true),
        map_field_named("span_attributes"),
        map_field_named("resource_attributes"),
    ]));
    // http.method="POST" at span scope, "GET" at resource scope — the same
    // key, two different values, both present.
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["t0"])),
            Arc::new(StringArray::from(vec!["s0"])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec!["POST /api"])),
            Arc::new(StringArray::from(vec!["api"])),
            Arc::new(datafusion::arrow::array::Int64Array::from(vec![10_i64])),
            Arc::new(datafusion::arrow::array::Int64Array::from(vec![100_i64])),
            Arc::new(StringArray::from(vec![Some("Ok")])),
            Arc::new(StringArray::from(vec![Some("Internal")])),
            build_map(&[&[("http.method", "POST")]]),
            build_map(&[&[("http.method", "GET")]]),
        ],
    )
    .unwrap();
    let ctx = SessionContext::new();
    register(&ctx, "traces", schema, batch);

    let q = r#"{ .http.method = "GET" }"#;
    let fields = ["trace_id"];

    let mut doc = ql_ir::traceql_to_ir(q, FROM_NS, TO_NS).unwrap();
    set_fields(&mut doc, &fields);
    let (df, _) = plan_document(&ctx, &doc, TENANT, DATASET, 0)
        .await
        .unwrap()
        .unwrap();
    let count: usize = df
        .collect()
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(
        count, 0,
        "coalesce takes span's \"POST\" first and never looks at resource's \"GET\""
    );
}

#[tokio::test]
async fn tags_corpus_classification_is_pinned() {
    for tags in TAGS_CORPUS {
        let expected = if *tags == "justaword" {
            Class::Invalid
        } else {
            Class::Accept
        };
        let class = match search_filter::parse_tags(tags) {
            Err(e) => class_of_querier_err(&e),
            Ok(conditions) => {
                let mut worst = Class::Accept;
                for c in &conditions {
                    let class = tags_condition_class(c);
                    if class != Class::Accept {
                        worst = class;
                        break;
                    }
                }
                worst
            }
        };
        assert_eq!(class, expected, "{tags}: classification drifted");
    }
}

#[tokio::test]
async fn tags_corpus_filters_plan_successfully() {
    let ctx = traces_fixture();
    let fields = ["trace_id"];
    for tags in TAGS_CORPUS {
        let Ok(conditions) = search_filter::parse_tags(tags) else {
            continue;
        };
        new_tags_plan(&ctx, &conditions, &fields)
            .await
            .unwrap_or_else(|e| panic!("{tags}: an accepted tags expression must still plan: {e}"));
    }
}

// ---------------------------------------------------------------------------
// Tests: LogQL (log queries)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn logql_log_corpus_rejections_agree() {
    let ctx = AttrContext::default();
    for q in LOGQL_LOG_CORPUS {
        assert_eq!(
            old_logql_log_class(q, &ctx),
            new_logql_log_class(q),
            "{q}: old/new rejection classification disagree"
        );
    }
}

/// `{k8s_namespace="prod"}` is the same OR-vs-COALESCE combining-semantics
/// divergence `adversarial_unscoped_attribute_combining_semantics_diverge`
/// pins for TraceQL: `k8s_namespace` has no dedicated column or logical
/// name, so both paths coalesce it across `log_attributes`/
/// `resource_attributes`, with the same OR-vs-COALESCE mismatch. No separate
/// pinned test — the mechanism is identical and already demonstrated there.
const KNOWN_COMBINING_DIVERGENCE_LOGQL_LOG: &[&str] = &[r#"{k8s_namespace="prod"}"#];

/// Every LogQL **line filter** (`|=`, `!=`, `|~`, `!~`) — D6 fixed the
/// finding this corpus used to pin (`logql_line_filter_agrees_on_optimized_plan`
/// below), so these agree at the plan level like every other corpus query
/// and need no special-case skip here anymore.
#[tokio::test]
async fn logql_log_corpus_filters_agree_on_optimized_plan() {
    let ctx = logs_fixture();
    let fields = ["timestamp", "body"];
    for q in LOGQL_LOG_CORPUS {
        if KNOWN_COMBINING_DIVERGENCE_LOGQL_LOG.contains(q) {
            continue;
        }
        if old_logql_log_class(q, &AttrContext::default()) != Class::Accept {
            continue;
        }
        let old = old_logql_log_plan(&ctx, q, &fields)
            .await
            .unwrap_or_else(|e| {
                panic!("{q}: old path should accept per its own classification: {e}")
            });
        let new = new_logql_log_plan(&ctx, q, &fields)
            .await
            .unwrap_or_else(|e| panic!("{q}: new path rejected an old-accepted query: {e}"));
        assert_plans_match(q, &old, &new);
    }
}

/// **Was the harness's most consequential finding, now fixed.** Every LogQL
/// line filter (`|=`/`!=`/`|~`/`!~` — LogQL's defining feature) lowers, via
/// `ql_ir::logql_lower::line_filter`, to a `Leaf { field: "body", op:
/// Contains, .. }` predicate. `LogicalSchema::core()` used to mark
/// `logs.body` `RetrievalOnly`, so `query_ir::validate` rejected the
/// resulting document outright — `InvalidInput("field 'body' is retrievable
/// but cannot be used in a predicate")` — even though `ql_ir::logql_to_ir`
/// itself returned `Ok`, not `Inexpressible` (so this was never the D5
/// fallback case; the rejection surfaced only at `plan_document`'s
/// validation, after the point a fallback switch would check).
///
/// Fixed in §4 (D6, task 4.0a): `body` is filterable for string operators —
/// `LogicalSchema::core()` no longer marks it `RetrievalOnly`. These four
/// queries now plan identically on both paths, pinned here at the
/// optimized-plan level like `adversarial_promoted_attribute_agrees_on_plan`
/// pins D10's fix.
#[tokio::test]
async fn logql_line_filter_agrees_on_optimized_plan() {
    let ctx = logs_fixture();
    let fields = ["timestamp", "body"];
    for q in [
        r#"{service_name="api"} |= "boom""#,
        r#"{service_name="api"} |~ "err.*""#,
        r#"{service_name="api"} !="boom""#,
        r#"{service_name="api", level="error"} |= "timeout""#,
    ] {
        assert_eq!(
            old_logql_log_class(q, &AttrContext::default()),
            Class::Accept,
            "{q}: old path should still accept this query"
        );
        assert_eq!(
            new_logql_log_class(q),
            Class::Accept,
            "{q}: ql_ir should still lower this (Ok, not Inexpressible)"
        );
        let old = old_logql_log_plan(&ctx, q, &fields)
            .await
            .unwrap_or_else(|e| {
                panic!("{q}: old path should accept per its own classification: {e}")
            });
        let new = new_logql_log_plan(&ctx, q, &fields).await.unwrap_or_else(|e| {
            panic!(
                "{q}: if this rejects again, body's filterability regressed — update the triage table, don't just delete this assertion: {e}"
            )
        });
        assert_plans_match(q, &old, &new);
    }
}

// ---------------------------------------------------------------------------
// Tests: LogQL metric queries — row-level equivalence (task 2.5)
// ---------------------------------------------------------------------------

/// One (bucket, value) pair from a matrix batch, keyed by every group column
/// present so rows compare independent of column ordering.
#[derive(Debug, PartialEq)]
struct MetricRow {
    key: Vec<String>,
    value: f64,
}

/// A minimal one-row matrix batch: `bucket` (nanosecond timestamp), one
/// group column (aliased to whatever `group_col` names), and `value`.
fn one_row_metric_batch(
    bucket_ns: i64,
    group_col: &str,
    group_value: &str,
    value: f64,
) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "bucket",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(group_col, DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampNanosecondArray::from(vec![bucket_ns])),
            Arc::new(StringArray::from(vec![group_value])),
            Arc::new(Float64Array::from(vec![value])),
        ],
    )
    .unwrap()
}

/// `metric_rows`' row key must include the time bucket, not just the group
/// columns and value — two runs whose (group, value) pairs land in
/// different time buckets are a real divergence, not agreement, and must
/// not compare equal (found by CodeRabbit review on this PR).
#[test]
fn metric_rows_key_distinguishes_different_buckets() {
    let earlier = one_row_metric_batch(0, "service_name", "api", 5.0);
    let later = one_row_metric_batch(1_000, "service_name", "api", 5.0);

    let earlier_rows = metric_rows(std::slice::from_ref(&earlier), &["service_name"], "value");
    let later_rows = metric_rows(std::slice::from_ref(&later), &["service_name"], "value");

    assert_ne!(
        earlier_rows, later_rows,
        "same (group, value) in different buckets must not compare equal"
    );
}

fn metric_rows(batches: &[RecordBatch], group_cols: &[&str], value_col: &str) -> Vec<MetricRow> {
    let mut rows = Vec::new();
    for batch in batches {
        // The value column's numeric type varies by aggregate (`count` is
        // Int64, `avg`/`sum_over_time` are Float64); cast rather than assume,
        // since the point of this comparison is the values, not their type.
        let raw = batch
            .column_by_name(value_col)
            .unwrap_or_else(|| panic!("missing value column '{value_col}'"));
        let casted = datafusion::arrow::compute::cast(raw, &DataType::Float64)
            .unwrap_or_else(|e| panic!("value column '{value_col}' is not numeric: {e}"));
        let value_array = casted
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("just cast to Float64");
        // Both sides bucket by `date_bin(...).alias("bucket")` — see
        // `Lowering::lower_aggregate` and `logs.rs::execute_plan`. Included
        // in the row key so two runs whose (group, value) pairs land in
        // different time buckets don't compare as equal (this module doc's
        // "bucket, the group labels, and a value column" claim).
        let bucket_array = batch
            .column_by_name("bucket")
            .unwrap_or_else(|| panic!("missing bucket column"))
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap_or_else(|| panic!("bucket column is not a nanosecond timestamp"));
        let group_arrays: Vec<&StringArray> = group_cols
            .iter()
            .map(|c| {
                batch
                    .column_by_name(c)
                    .unwrap_or_else(|| panic!("missing group column '{c}'"))
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap_or_else(|| panic!("group column '{c}' is not a string"))
            })
            .collect();
        for row in 0..batch.num_rows() {
            let key = std::iter::once(bucket_array.value(row).to_string())
                .chain(group_arrays.iter().map(|a| a.value(row).to_string()))
                .collect();
            rows.push(MetricRow {
                key,
                value: value_array.value(row),
            });
        }
    }
    rows.sort_by(|a, b| {
        a.key
            .cmp(&b.key)
            .then(a.value.partial_cmp(&b.value).unwrap())
    });
    rows
}

#[tokio::test]
async fn logql_metric_corpus_row_level_equivalence() {
    let ctx = logs_fixture();
    for (q, expected_rows) in LOGQL_METRIC_CORPUS_ROWS {
        let mut doc = ql_ir::logql_to_ir(q, FROM_NS, TO_NS)
            .unwrap_or_else(|e| panic!("{q}: metric path rejected an accepted query: {e}"));
        // The IR's `Series` result returns the aggregate output unprojected
        // (`apply_projection`'s early-return for `series_shaped`), so no
        // `fields` override is needed or honoured here.
        let (df, _) = plan_document(&ctx, &doc, TENANT, DATASET, 0)
            .await
            .unwrap_or_else(|e| panic!("{q}: metric path failed to plan: {e}"))
            .expect("logs table is registered");
        let batches = df
            .collect()
            .await
            .unwrap_or_else(|e| panic!("{q}: metric path failed to execute: {e}"));

        let group_cols = grouping_columns(&mut doc);
        let mut rows = metric_rows(&batches, &group_cols, "value");
        rows.sort_by(|a, b| {
            a.key
                .cmp(&b.key)
                .then(a.value.partial_cmp(&b.value).unwrap())
        });
        let mut expected: Vec<MetricRow> = expected_rows
            .iter()
            .map(|(key, value)| MetricRow {
                key: key.iter().map(|s| s.to_string()).collect(),
                value: *value,
            })
            .collect();
        expected.sort_by(|a, b| {
            a.key
                .cmp(&b.key)
                .then(a.value.partial_cmp(&b.value).unwrap())
        });
        assert_eq!(
            rows, expected,
            "{q}: metric rows drifted from their pinned expectation (grouped by {group_cols:?})"
        );
    }
}

/// LogQL metric-query corpus, each paired with its expected rows over
/// [`logs_fixture`], pinned so a regression in bucketing/grouping/aggregation
/// shows up as a row-content mismatch rather than a silent behaviour change.
/// Sources: `logql_queries.rs`'s `count_over_time` query and
/// `src/ql-ir/tests/logql.rs`'s corpus. Values captured from the IR path
/// itself (this is a regression pin, not an old-vs-new comparison — every
/// one of these queries is `Accept`, not part of the D5 fallback set, so
/// there is no old path left to compare against; §5.4 of
/// `ir-single-lowering`). Every fixture row lands in bucket `"0"`
/// (`date_bin` with a 1000ns step over timestamps 10/20/30ms — well inside
/// one bucket).
#[allow(clippy::type_complexity)]
const LOGQL_METRIC_CORPUS_ROWS: &[(&str, &[(&[&str], f64)])] = &[
    (
        r#"count_over_time({service_name="api"}[1h])"#,
        &[(&["0", "api", "ERROR"], 1.0), (&["0", "api", "INFO"], 1.0)],
    ),
    (
        r#"rate({service_name="api"}[5m])"#,
        &[
            (&["0", "api", "ERROR"], 1.0 / 300.0),
            (&["0", "api", "INFO"], 1.0 / 300.0),
        ],
    ),
    (
        r#"sum by (service_name) (count_over_time({service_name="api"}[1m]))"#,
        &[(&["0", "api"], 2.0)],
    ),
    (
        r#"sum_over_time({service_name="api"} | unwrap duration [5m])"#,
        &[(&["0", "api", "ERROR"], 1.5), (&["0", "api", "INFO"], 2.5)],
    ),
    (
        r#"avg_over_time({service_name="api"} | unwrap duration [5m])"#,
        &[(&["0", "api", "ERROR"], 1.5), (&["0", "api", "INFO"], 2.5)],
    ),
    (
        r#"min by (service_name) (min_over_time({service_name="api"} | unwrap duration [1m]))"#,
        &[(&["0", "api"], 1.5)],
    ),
    (
        r#"max by (service_name) (max_over_time({service_name="api"} | unwrap duration [1m]))"#,
        &[(&["0", "api"], 2.5)],
    ),
];

/// **The second major finding.** A LogQL range aggregation with no outer
/// vector aggregation (`count_over_time(...)` alone, not `sum by (..)
/// (count_over_time(...))`) is, in real Loki, one series *per matching
/// stream* — every distinct label combination the selector matches gets its
/// own point. The old path implemented that by defaulting
/// `range_group_cols` to `SERIES_COLUMNS` (`service_name`, `severity_text`)
/// when `group_labels` is empty (`logs.rs::execute_plan`).
///
/// Fixed in §4 (D7, task 4.0b): `ql_ir::logql_to_ir` now defaults an
/// ungrouped range aggregation's `by` to the same stream identity
/// (`ql_ir::STREAM_IDENTITY`, pinned against `SERIES_COLUMNS` by
/// `ql_ir_stream_identity_matches_series_columns` below) instead of emitting
/// `by: vec![]`. Demonstrated here: the fixture's two `api` rows (one
/// `ERROR`, one `INFO` — two distinct `service_name`+`severity_text`
/// streams) now produce two rows on both paths.
#[tokio::test]
async fn adversarial_ungrouped_range_aggregation_default_grouping_agrees() {
    let ctx = logs_fixture();
    let q = r#"count_over_time({service_name="api"}[1h])"#;

    let doc = ql_ir::logql_to_ir(q, FROM_NS, TO_NS).unwrap_or_else(|e| panic!("{q}: {e}"));
    let (df, _) = plan_document(&ctx, &doc, TENANT, DATASET, 0)
        .await
        .unwrap_or_else(|e| panic!("{q}: failed to plan: {e}"))
        .expect("logs table is registered");
    let batches = df
        .collect()
        .await
        .unwrap_or_else(|e| panic!("{q}: failed to execute: {e}"));

    let group_cols = ["service_name", "severity_text"];
    let rows = metric_rows(&batches, &group_cols, "value");
    assert_eq!(
        rows.len(),
        2,
        "one row per (service_name, severity_text) stream — two api rows, two severities: {rows:?}"
    );
}

/// CodeRabbit finding on #1393: distinct from a *bare* range aggregation
/// (above), an *explicit* vector aggregation with no `by()` —
/// `sum(count_over_time(...))` — must not default to the stream identity;
/// `vector_grouping` returning an empty grouping for `v.grouping == None`
/// means "collapse", not "no grouping specified yet". `lower_metric_query`
/// keeps the two cases distinct.
///
/// Checking that fix against the old path empirically (not assumed, per
/// CodeRabbit's own suggestion) surfaced a *second*, independent finding,
/// still true today: `logql_metric.rs::plan_metric_query` documents that
/// `sum`'s `outer_agg` stays `None` ("folds into the grouped range
/// aggregate"), so `logs.rs::execute_plan`'s grouping branch (`None if
/// out_group_cols.is_empty() => SERIES_COLUMNS`) treats an ungrouped
/// `sum(...)` identically to a *bare* range aggregation — same
/// `SERIES_COLUMNS` grouping, no second reduction pass — producing one row
/// per stream instead of one row total (filed as #1394, sibling to #1392).
/// This query is `Accept`, not part of the D5 fallback set, so §5 made the
/// old path's #1394 bug unreachable through the public API — it can no
/// longer be pinned as a live old-vs-new divergence, only recorded as
/// history (module doc, "Findings left as they are"). This is now a plain
/// regression pin on the IR path's correct behaviour.
#[tokio::test]
async fn explicitly_ungrouped_vector_aggregation_collapses_to_one_row() {
    let ctx = logs_fixture();
    let q = r#"sum(count_over_time({service_name="api"}[1h]))"#;

    let doc = ql_ir::logql_to_ir(q, FROM_NS, TO_NS).unwrap_or_else(|e| panic!("{q}: {e}"));
    let (df, _) = plan_document(&ctx, &doc, TENANT, DATASET, 0)
        .await
        .unwrap_or_else(|e| panic!("{q}: failed to plan: {e}"))
        .expect("logs table is registered");
    let batches = df
        .collect()
        .await
        .unwrap_or_else(|e| panic!("{q}: failed to execute: {e}"));

    // No group columns declared (an explicit, ungrouped vector aggregation).
    let rows = metric_rows(&batches, &[], "value");
    assert_eq!(
        rows,
        vec![MetricRow {
            key: vec!["0".to_string()],
            value: 2.0
        }],
        "should collapse to one row (the sum across both api streams): {rows:?}"
    );
}

/// Pins `ql_ir::STREAM_IDENTITY` (D7's default grouping) against
/// `logs.rs::SERIES_COLUMNS` (the old path's stream-identity constant),
/// resolved through the real `SchemaResolver`/`LogicalSchema` rather than
/// compared as string literals — so a rename on either side that breaks the
/// mapping fails here, not silently at query time. `ql-ir` has no access to
/// the real schema (it lowers to logical names only), so this pin lives on
/// the querier side.
#[tokio::test]
async fn ql_ir_stream_identity_matches_series_columns() {
    let ctx = logs_fixture();
    let df = optional_table(&ctx, TENANT, DATASET, "logs")
        .await
        .unwrap()
        .expect("logs table is registered");
    let source = SourcePlan::for_source("logs").expect("logs source plan");
    let resolver = SchemaResolver::new(df.schema(), &source);
    let resolved: Vec<String> = ql_ir::STREAM_IDENTITY
        .iter()
        .map(|field| match resolver.resolve("", field) {
            Some(Resolved::Column { name, .. }) => name,
            other => panic!("{field}: expected a direct column resolution, got {other:?}"),
        })
        .collect();
    assert_eq!(
        resolved,
        super::logs::SERIES_COLUMNS
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>(),
        "ql_ir::STREAM_IDENTITY must resolve to exactly logs.rs::SERIES_COLUMNS, in order"
    );
}

/// The `by` grouping labels a metric document declares, aliased the way
/// `Lowering::lower_aggregate` names them (`safe_ident`, which is the
/// identity for an already-valid identifier such as `service_name`).
/// Every corpus metric query below either groups explicitly by
/// `service_name` (one column) or, ungrouped, now defaults to the stream
/// identity (D7: `service_name`, `severity_text` — two columns, exactly
/// `logs.rs::SERIES_COLUMNS`) rather than collapsing to none.
fn grouping_columns(doc: &mut Document) -> Vec<&'static str> {
    for stage in &doc.pipeline {
        if let common::query_ir::Stage::Aggregate(agg) = stage {
            return match agg.by.len() {
                0 => Vec::new(),
                1 => vec!["service_name"],
                _ => vec!["service_name", "severity_text"],
            };
        }
    }
    Vec::new()
}

fn clone_ctx(ctx: &SessionContext) -> SessionContext {
    // `SessionContext` is cheaply `Clone` (an `Arc` around shared state); the
    // catalog registered on `ctx` is visible from the clone.
    ctx.clone()
}

#[tokio::test]
async fn logql_metric_known_inexpressible_matches_old_path_accepts() {
    let ctx = logs_fixture();
    for (q, reason) in KNOWN_INEXPRESSIBLE_LOGQL {
        let err = ql_ir::logql_to_ir(q, FROM_NS, TO_NS).expect_err(&format!(
            "{q}: expected Inexpressible ({reason}), lowered instead"
        ));
        assert!(
            matches!(err, ql_ir::LowerError::Inexpressible(_)),
            "{q}: expected Inexpressible, got {err:?}"
        );

        let LogqlExpr::Metric(metric) = logql::parse(q).unwrap_or_else(|e| panic!("{q}: {e}"))
        else {
            panic!("{q}: expected a metric query");
        };
        let svc = LogsService::new(clone_ctx(&ctx));
        let params = MetricQueryParams {
            query: q.to_string(),
            start: FROM,
            end: TO,
            step: 1_000,
        };
        svc.query_metric(&params, TENANT, DATASET)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "{q}: D5 fallback requires the old path to still accept this query, but it failed: {e}"
                )
            });
        let _ = metric; // parsed only to assert it *is* a metric query.
    }
}

// ---------------------------------------------------------------------------
// Adversarial cases (task 2.2)
// ---------------------------------------------------------------------------

/// Promoted attribute, scope-qualified field: `ir_planner::SchemaResolver::column_for`
/// strips the scope qualifier before materializing a promoted column's name
/// (D10 of `ir-single-lowering`, task 3.0), so `span.http.method` resolves
/// to the promoted `label_http_method` column rather than falling back to
/// `get_field` JSON-path extraction.
#[tokio::test]
async fn promoted_attribute_resolves_to_its_column() {
    let ctx = traces_promoted_fixture();
    let q = r#"{ span.http.method = "GET" }"#;
    let fields = ["trace_id"];

    let plan = new_traceql_plan(&ctx, q, &fields).await.unwrap();
    assert!(
        plan.contains("label_http_method"),
        "must route to the promoted column:\n{plan}"
    );
    assert!(
        !plan.contains("get_field"),
        "must not fall back to json-path extraction once a promoted column exists:\n{plan}"
    );
}

/// The three tags escaping-hazard values added to `TAGS_CORPUS` (blocker
/// found in review of PR #1391), executed end to end: a backslash, an
/// embedded `"`, and a `&&` in a key all reach the filter unchanged through
/// `tags_to_ir`.
#[tokio::test]
async fn tags_escaping_values_match_the_fixture_row() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
        Field::new("parent_span_id", DataType::Utf8, true),
        Field::new("span_name", DataType::Utf8, false),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("start_time_unix_nano", DataType::Int64, false),
        Field::new("duration_nanos", DataType::Int64, false),
        Field::new("status_code", DataType::Utf8, true),
        Field::new("span_kind", DataType::Utf8, true),
        map_field_named("span_attributes"),
        map_field_named("resource_attributes"),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["t0"])),
            Arc::new(StringArray::from(vec!["s0"])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec!["GET /a"])),
            Arc::new(StringArray::from(vec!["api"])),
            Arc::new(datafusion::arrow::array::Int64Array::from(vec![10_i64])),
            Arc::new(datafusion::arrow::array::Int64Array::from(vec![100_i64])),
            Arc::new(StringArray::from(vec![Some("Ok")])),
            Arc::new(StringArray::from(vec![Some("Internal")])),
            build_map(&[&[
                ("file.path", r"C:\Users\foo"),
                ("weird.key", "va\"lue"),
                ("weird&&key", "value"),
            ]]),
            build_map(&[&[]]),
        ],
    )
    .unwrap();
    let ctx = SessionContext::new();
    register(&ctx, "traces", schema, batch);
    let fields = ["trace_id"];

    for tags in [
        r"file.path=C:\Users\foo",
        r#"weird.key=va"lue"#,
        "weird&&key=value",
    ] {
        let conditions = search_filter::parse_tags(tags).unwrap();
        let predicate = super::tags_to_ir::conditions_to_predicate(&conditions)
            .unwrap_or_else(|e| panic!("{tags}: should be expressible: {e}"));
        let doc = Document {
            ir_version: 1,
            from: "traces".to_string(),
            range: Range {
                from: serde_json::Value::String(FROM_NS.to_string()),
                to: serde_json::Value::String(TO_NS.to_string()),
            },
            result: ResultEnvelope::Rows,
            fields: Some(fields.iter().map(|s| s.to_string()).collect()),
            pipeline: vec![Stage::Where(predicate)],
        };
        let (df, _) = plan_document(&ctx, &doc, TENANT, DATASET, 0)
            .await
            .unwrap()
            .unwrap();
        let rows: usize = df
            .collect()
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(rows, 1, "{tags}: should match the fixture's one row");
    }
}

/// An attribute key colliding with a physical column name
/// (`span_attributes["service_name"]` disagrees with the real
/// `service_name` column): filtering the *logical* `service.name` field
/// must resolve to the physical column, never the colliding attribute.
#[tokio::test]
async fn attribute_key_collision_resolves_to_the_physical_column() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
        Field::new("parent_span_id", DataType::Utf8, true),
        Field::new("span_name", DataType::Utf8, false),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("start_time_unix_nano", DataType::Int64, false),
        Field::new("duration_nanos", DataType::Int64, false),
        Field::new("status_code", DataType::Utf8, true),
        Field::new("span_kind", DataType::Utf8, true),
        map_field_named("span_attributes"),
        map_field_named("resource_attributes"),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["t0"])),
            Arc::new(StringArray::from(vec!["s0"])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec!["GET /api"])),
            Arc::new(StringArray::from(vec!["api"])),
            Arc::new(datafusion::arrow::array::Int64Array::from(vec![10_i64])),
            Arc::new(datafusion::arrow::array::Int64Array::from(vec![100_i64])),
            Arc::new(StringArray::from(vec![Some("Ok")])),
            Arc::new(StringArray::from(vec![Some("Internal")])),
            // The colliding entry: a different value under the same name.
            build_map(&[&[("service_name", "not-the-real-service")]]),
            build_map(&[&[]]),
        ],
    )
    .unwrap();
    let ctx = SessionContext::new();
    register(&ctx, "traces", schema, batch);

    let q = r#"{ resource.service.name = "api" }"#;
    let fields = ["trace_id"];

    let plan = new_traceql_plan(&ctx, q, &fields).await.unwrap();
    assert!(
        !plan.contains("get_field"),
        "must resolve service.name to the physical column, not the colliding attribute:\n{plan}"
    );
}

/// #1070's fix (`Lowering::lower_aggregate` referencing its group-column
/// alias via `ident()` instead of `col()`) only touched `ir_planner.rs`. This
/// adversarial case went looking for the same bug class in the *old* LogQL
/// metric path (`logs.rs`'s `execute_plan` grouping-column resolution) — and
/// found it still there: grouping by a mixed-case *attribute* label (as
/// opposed to a well-known dedicated-column label) failed on the old path
/// with `No field named label_statuscode. Did you mean
/// 't.d.logs.label_StatusCode'?`, the same lowercased-unquoted-identifier
/// bug #1070 fixed on the IR side.
///
/// This query is `Accept`, not part of the D5 fallback set, so §5 made the
/// old path's #1070-class bug (issue #1392) unreachable through the public
/// API — the same as #1394 above, it can no longer be pinned as a live
/// divergence. This is now a plain regression pin on the IR path's correct
/// grouping.
#[tokio::test]
async fn mixed_case_grouping_label_groups_correctly() {
    let ctx = logs_mixed_case_fixture();
    let q = r#"sum by (StatusCode) (count_over_time({service_name="api"}[1m]))"#;

    let doc = ql_ir::logql_to_ir(q, FROM_NS, TO_NS).unwrap_or_else(|e| panic!("{q}: {e}"));
    let (df, _) = plan_document(&ctx, &doc, TENANT, DATASET, 0)
        .await
        .unwrap_or_else(|e| panic!("{q}: failed to plan: {e}"))
        .expect("logs table is registered");
    let batches = df
        .collect()
        .await
        .unwrap_or_else(|e| panic!("{q}: failed to execute (a #1070 regression): {e}"));
    let rows = metric_rows(&batches, &["StatusCode"], "value");
    assert_eq!(
        rows.len(),
        2,
        "should group the two StatusCode values: {rows:?}"
    );
}

/// **Was the pinned divergence (triage table case 3), now fixed by D9.** Old
/// LogQL's `!=` explicitly matches rows where the key is absent
/// (`is_null().or(not_eq(...))`); `ir_planner`'s plain `not(...)` follows
/// Kleene semantics, where an absent key satisfies neither `=` nor `!=`. The
/// two paths' *plans* still legitimately differ in shape (`or[ne,
/// not(exists)]` vs. `is_null().or(not_eq(...))` are not the same
/// expression tree), so this compares row-level output — same rule D2
/// applies to the metric path — rather than optimized-plan text.
#[tokio::test]
async fn adversarial_absent_value_semantics_agrees() {
    let ctx = logs_fixture();
    // Row 2 ("slow response", service `web`) has no `region` key at all —
    // the other two rows (service `api`) both have `region="eu"`.
    let q = r#"{region!="eu"}"#;

    let old_bodies = old_logql_log_bodies(&ctx, q).await;
    let new_bodies = new_logql_log_bodies(&ctx, q).await;
    assert!(
        old_bodies.contains(&"slow response".to_string()),
        "old path's != should include the row with no `region` key at all: {old_bodies:?}"
    );
    assert!(
        new_bodies.contains(&"slow response".to_string()),
        "if this now excludes the absent-key row, D9's fix regressed — update the triage table, don't just delete this assertion: {new_bodies:?}"
    );
    let mut old_sorted = old_bodies.clone();
    old_sorted.sort();
    let mut new_sorted = new_bodies.clone();
    new_sorted.sort();
    assert_eq!(
        old_sorted, new_sorted,
        "{q}: old/new row sets disagree beyond the absent-key case"
    );
}

/// The same D9 fix for `!~` (`materialized_label_expr`'s `Nre` branch is
/// `is_null().or(not(regexp_like(...)))` on the old path too, so this is a
/// genuine old/new agreement, unlike `=""` below).
#[tokio::test]
async fn absent_value_semantics_also_agree_for_negative_regex() {
    let ctx = logs_fixture();
    let q = r#"{region!~"e.*"}"#;
    let old_bodies = old_logql_log_bodies(&ctx, q).await;
    let new_bodies = new_logql_log_bodies(&ctx, q).await;
    assert!(
        old_bodies.contains(&"slow response".to_string()),
        "{q}: old path should include the row with no `region` key at all: {old_bodies:?}"
    );
    assert!(
        new_bodies.contains(&"slow response".to_string()),
        "{q}: new path should also include the absent-key row (D9): {new_bodies:?}"
    );
    let mut old_sorted = old_bodies.clone();
    old_sorted.sort();
    let mut new_sorted = new_bodies.clone();
    new_sorted.sort();
    assert_eq!(
        old_sorted, new_sorted,
        "{q}: old/new row sets disagree beyond the absent-key case"
    );
}

/// `{region=""}` is *not* an old/new agreement case: the old path's `Eq`
/// branch (`map_attribute_expr`/`materialized_label_expr`) has no
/// absent-matching special case at all — only `Neq`/`Nre` do — so
/// `region=""` against a row missing `region` entirely evaluates to NULL on
/// the old path and is excluded. D9 gives the *new* path real Loki
/// semantics here (`or[eq "", not(exists)]`) without claiming the old path
/// ever matched; this is a forward-looking regression test on the new path
/// alone, not a pinned agreement.
#[tokio::test]
async fn empty_string_equality_matches_an_absent_field_on_the_new_path() {
    let ctx = logs_fixture();
    let new_bodies = new_logql_log_bodies(&ctx, r#"{region=""}"#).await;
    assert!(
        new_bodies.contains(&"slow response".to_string()),
        "new path's region=\"\" should include the row with no `region` key at all (D9): {new_bodies:?}"
    );
}

/// A `logs` table with a NULL `severity_text` on one row — pins D9's
/// absent-matches fix for the **well-known dedicated-column** labels
/// (`service_name`/`level`/`trace_id`/`span_id`, resolved by
/// `logs.rs::column_for_label`), as distinct from the JSON/map-attribute
/// path `logs_fixture()`'s `region` case already covers.
fn logs_null_dedicated_column_fixture() -> SessionContext {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("body", DataType::Utf8, true),
        Field::new("service_name", DataType::Utf8, true),
        Field::new("severity_text", DataType::Utf8, true),
        Field::new("trace_id", DataType::Utf8, true),
        Field::new("span_id", DataType::Utf8, true),
        map_field_named("log_attributes"),
        map_field_named("resource_attributes"),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(TimestampNanosecondArray::from(vec![10_i64, 20])),
            Arc::new(StringArray::from(vec!["has severity", "no severity"])),
            Arc::new(StringArray::from(vec!["api", "api"])),
            Arc::new(StringArray::from(vec![Some("ERROR"), None])),
            Arc::new(StringArray::from(vec![Some("t1"), Some("t2")])),
            Arc::new(StringArray::from(vec![Some("s1"), Some("s2")])),
            build_map(&[&[], &[]]),
            build_map(&[&[], &[]]),
        ],
    )
    .unwrap();
    let ctx = SessionContext::new();
    register(&ctx, "logs", schema, batch);
    ctx
}

/// Review finding on #1393: `logql.rs::column_expr` (used for the
/// dedicated-column labels `column_for_label` resolves — `service_name`,
/// `level`/`severity`/`detected_level`, `trace_id`, `span_id`) lowered
/// `!=`/`!~` as a plain `not_eq`/`not(regexp_like)` with no NULL handling,
/// unlike its siblings `map_attribute_expr` (attributes) and
/// `materialized_label_expr` (materialized labels), which both already
/// match absent via `is_null().or(...)`. D9 promises Loki's absent-matches
/// semantics uniformly — "a missing label is the empty string" carries no
/// exception for a dedicated column — so this was the *old* path's own bug
/// (triage table outcome 1), not a genuine difference from the new path
/// (which already applies D9 uniformly via `ql_ir::logql_lower::matcher`,
/// regardless of whether a label resolves to a dedicated column). Fixed in
/// `column_expr`.
#[tokio::test]
async fn dedicated_column_negative_matchers_also_match_a_null_row() {
    let ctx = logs_null_dedicated_column_fixture();
    for q in [r#"{level!="ERROR"}"#, r#"{level!~"ERR.*"}"#] {
        let mut old_bodies = old_logql_log_bodies(&ctx, q).await;
        let mut new_bodies = new_logql_log_bodies(&ctx, q).await;
        old_bodies.sort();
        new_bodies.sort();
        assert!(
            old_bodies.contains(&"no severity".to_string()),
            "{q}: old path should include the row with a NULL severity_text: {old_bodies:?}"
        );
        assert_eq!(
            old_bodies, new_bodies,
            "{q}: old/new row sets disagree on the NULL-severity_text row"
        );
    }
}

/// The `body` values a document's `Rows` plan produces, in row order.
fn body_values(batches: &[RecordBatch]) -> Vec<String> {
    batches
        .iter()
        .flat_map(|b| {
            let bodies = b
                .column_by_name("body")
                .expect("body column projected")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("body is a string column");
            (0..b.num_rows()).map(|i| bodies.value(i).to_string())
        })
        .collect()
}

async fn old_logql_log_bodies(ctx: &SessionContext, q: &str) -> Vec<String> {
    let fields = ["body"];
    let df = old_logql_log_query_df(ctx, q, &fields).await;
    body_values(&df.collect().await.unwrap())
}

async fn new_logql_log_bodies(ctx: &SessionContext, q: &str) -> Vec<String> {
    let fields = ["body"];
    let mut doc = ql_ir::logql_to_ir(q, FROM_NS, TO_NS).unwrap();
    set_fields(&mut doc, &fields);
    let (df, _) = plan_document(ctx, &doc, TENANT, DATASET, 0)
        .await
        .unwrap()
        .unwrap();
    body_values(&df.collect().await.unwrap())
}

async fn old_logql_log_query_df(ctx: &SessionContext, q: &str, fields: &[&str]) -> DataFrame {
    let parsed = match logql::parse(q).unwrap() {
        LogqlExpr::Log(l) => l,
        other => panic!("expected a log query, got {other:?}"),
    };
    let mut df = optional_table(ctx, TENANT, DATASET, "logs")
        .await
        .unwrap()
        .unwrap();
    let attr_ctx = AttrContext {
        materialized: materialized_columns_of(&df),
        map_attrs: is_map_column(&df, "log_attributes"),
        attr_tokens: false,
    };
    df = super::table_lookup::time_window(df, FROM, TO).unwrap();
    if let Some(filter) = log_query_filter_with_columns(&parsed, &attr_ctx).unwrap() {
        df = df.filter(filter).unwrap();
    }
    df.select_columns(fields).unwrap()
}

// ---------------------------------------------------------------------------
// Endpoint-level regression: `TraceService::find_traces_with_tenant` over the
// corpus and the promoted-attribute/combining-semantics adversarial cases.
// §5 deleted the old lowering these tests used to compare against with the
// rollout switch on vs off; each is now a single-path expected-result pin —
// proof the compat layer's own result assembly, downstream of the plan,
// still produces a sane response (module doc's original task 2.3b rationale
// for testing at the endpoint and not just the plan).
// ---------------------------------------------------------------------------

/// A `traces` table with the full schema [`super::trace::TraceService`]'s
/// search assembly reads (unlike [`traces_fixture`], which only carries what
/// the plan-comparison tests above project) — same two rows as
/// [`traces_fixture`], so every `TRACEQL_CORPUS_CLASS`/`TAGS_CORPUS` query that
/// matches there matches identically here.
fn traces_endpoint_fixture() -> SessionContext {
    let schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
        Field::new("parent_span_id", DataType::Utf8, true),
        Field::new("span_name", DataType::Utf8, false),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("span_kind", DataType::Utf8, true),
        Field::new("status_code", DataType::Utf8, true),
        Field::new("is_root", DataType::Boolean, false),
        Field::new("start_time_unix_nano", DataType::Int64, false),
        Field::new("duration_nanos", DataType::Int64, false),
        map_field_named("span_attributes"),
        map_field_named("resource_attributes"),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["t0", "t1"])),
            Arc::new(StringArray::from(vec!["s0", "s1"])),
            Arc::new(StringArray::from(vec![None, Some("s0")])),
            Arc::new(StringArray::from(vec!["GET /api", "POST /x"])),
            Arc::new(StringArray::from(vec!["api", "web"])),
            Arc::new(StringArray::from(vec![Some("Server"), Some("Internal")])),
            Arc::new(StringArray::from(vec![Some("Error"), Some("Ok")])),
            Arc::new(BooleanArray::from(vec![true, false])),
            Arc::new(datafusion::arrow::array::Int64Array::from(vec![10_i64, 20])),
            Arc::new(datafusion::arrow::array::Int64Array::from(vec![
                100_i64, 200,
            ])),
            build_map(&[&[("http.method", "GET"), ("http.status_code", "500")], &[]]),
            build_map(&[&[("k8s.pod.name", "p"), ("service.name", "api")], &[]]),
        ],
    )
    .unwrap();
    let ctx = SessionContext::new();
    register(&ctx, "traces", schema, batch);
    ctx
}

async fn search_traces(
    ctx: &SessionContext,
    query: SearchQueryParams,
) -> Result<Vec<common::model::trace::Trace>, QuerierError> {
    TraceService::new(ctx.clone(), "traces".to_string())
        .find_traces_with_tenant(query, TENANT, DATASET)
        .await
}

fn q_only(q: &str) -> SearchQueryParams {
    SearchQueryParams {
        q: Some(q.to_string()),
        ..Default::default()
    }
}

fn tags_only(tags: &str) -> SearchQueryParams {
    SearchQueryParams {
        tags: Some(tags.to_string()),
        ..Default::default()
    }
}

/// Every accepted `TRACEQL_CORPUS_CLASS` query must still produce an assembled
/// search result end to end (not merely plan successfully — 2.3, above —
/// the compat layer's own result assembly sits downstream of the plan).
#[tokio::test]
async fn traceql_corpus_searches_successfully_at_the_endpoint() {
    let ctx = traces_endpoint_fixture();
    for (q, class) in TRACEQL_CORPUS_CLASS {
        if *class != Class::Accept {
            continue;
        }
        search_traces(&ctx, q_only(q))
            .await
            .unwrap_or_else(|e| panic!("{q}: an accepted query must still search: {e}"));
    }
}

/// Same, for the `tags` corpus.
#[tokio::test]
async fn tags_corpus_searches_successfully_at_the_endpoint() {
    let ctx = traces_endpoint_fixture();
    for tags in TAGS_CORPUS {
        if *tags == "justaword" {
            continue;
        }
        search_traces(&ctx, tags_only(tags))
            .await
            .unwrap_or_else(|e| {
                panic!("{tags}: an accepted tags expression must still search: {e}")
            });
    }
}

/// `q` and `tags` together become one conjoined document (task 3.3): `q`
/// narrows to `service.name = "api"` (both fixture rows match), `tags`
/// narrows further to `http.method = "GET"` (only `t0`).
#[tokio::test]
async fn q_and_tags_together_narrow_the_search() {
    let ctx = traces_endpoint_fixture();
    let query = SearchQueryParams {
        q: Some(r#"{ resource.service.name = "api" }"#.to_string()),
        tags: Some("http.method=GET".to_string()),
        ..Default::default()
    };
    let traces = search_traces(&ctx, query).await.unwrap();
    let ids: Vec<&str> = traces.iter().map(|t| t.trace_id.as_str()).collect();
    assert_eq!(ids, vec!["t0"]);
}

/// D8's unscoped-attribute combining rule
/// (`unscoped_attribute_coalesces_by_container_priority`, above) reproduces
/// at the endpoint level too: the coalesce-by-priority resolution takes the
/// span-scope value first and never reaches the resource-scope match.
#[tokio::test]
async fn unscoped_attribute_coalescing_reproduces_at_the_endpoint() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
        Field::new("parent_span_id", DataType::Utf8, true),
        Field::new("span_name", DataType::Utf8, false),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("span_kind", DataType::Utf8, true),
        Field::new("status_code", DataType::Utf8, true),
        Field::new("is_root", DataType::Boolean, false),
        Field::new("start_time_unix_nano", DataType::Int64, false),
        Field::new("duration_nanos", DataType::Int64, false),
        map_field_named("span_attributes"),
        map_field_named("resource_attributes"),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["t0"])),
            Arc::new(StringArray::from(vec!["s0"])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec!["POST /api"])),
            Arc::new(StringArray::from(vec!["api"])),
            Arc::new(StringArray::from(vec![Some("Internal")])),
            Arc::new(StringArray::from(vec![Some("Ok")])),
            Arc::new(BooleanArray::from(vec![true])),
            Arc::new(datafusion::arrow::array::Int64Array::from(vec![10_i64])),
            Arc::new(datafusion::arrow::array::Int64Array::from(vec![100_i64])),
            build_map(&[&[("http.method", "POST")]]),
            build_map(&[&[("http.method", "GET")]]),
        ],
    )
    .unwrap();
    let ctx = SessionContext::new();
    register(&ctx, "traces", schema, batch);

    let traces = search_traces(&ctx, q_only(r#"{ .http.method = "GET" }"#))
        .await
        .unwrap();
    assert_eq!(
        traces.len(),
        0,
        "coalesce takes span's \"POST\" first and never looks at resource's \"GET\""
    );
}

// ---------------------------------------------------------------------------
// Task 2.3b (logs half): endpoint-level regression — `LogsService::query_metric`.
// §5.3 removed the rollout switch (there is one path now, IR-first with the
// D5 fallback), so these are single-path expected-result pins on the two
// schema-parity corrections `LogsService::query_metric_via_ir` applies
// post-`plan_document` (see its doc comment) — a regression in either would
// otherwise only show up as silently-wrong numbers at the router, not a
// planning error.
// ---------------------------------------------------------------------------

/// D5/4.1: `ql_ir::logql_to_ir` sets a range aggregate's bucket width from the
/// LogQL range literal (`[1h]` here), not the caller's `step` parameter —
/// `query_metric_via_ir` overrides it post-lowering. Demonstrated with a
/// `step` (10ns) far finer than the `[1h]` literal: the fixture's two `api`
/// rows land at `timestamp` 10 and 20, ten nanoseconds apart, so a
/// `step`-correct plan produces two one-nanosecond-resolution buckets (one
/// row each). If the override were dropped, bucketing would use `~1h` in
/// nanoseconds instead — both rows would collapse into a single bucket of
/// count 2.
#[tokio::test]
async fn query_metric_via_ir_buckets_by_the_callers_step_not_the_range_literal() {
    let ctx = logs_fixture();
    let svc = LogsService::new(ctx);
    let params = MetricQueryParams {
        query: r#"count_over_time({service_name="api"}[1h])"#.to_string(),
        start: FROM,
        end: TO,
        step: 10,
    };
    let batches = svc.query_metric(&params, TENANT, DATASET).await.unwrap();
    let group_cols = ["service_name", "severity_text"];
    let rows = metric_rows(&batches, &group_cols, "value");
    assert_eq!(
        rows.len(),
        2,
        "a step of 10ns should split the two api rows (10ns apart) into two buckets"
    );
}

/// D5/4.1: `ir_planner::agg_expr`'s `count` aggregate is Arrow `Int64`
/// (uncast) unless a `rate` divisor promotes it, but the router's
/// `batches_to_matrix` requires `Float64` (`downcast_ref::<Float64Array>`,
/// silently reading `0.0` for any other type). `query_metric_via_ir` casts
/// explicitly post-plan; this pins that the cast actually lands, not just
/// that the *numbers* happen to compare equal after `metric_rows`' own
/// defensive cast (which would mask exactly this regression).
#[tokio::test]
async fn query_metric_via_ir_value_column_is_float64() {
    let ctx = logs_fixture();
    let svc = LogsService::new(ctx);
    let params = MetricQueryParams {
        query: r#"count_over_time({service_name="api"}[1h])"#.to_string(),
        start: FROM,
        end: TO,
        step: 1_000,
    };
    let batches = svc.query_metric(&params, TENANT, DATASET).await.unwrap();
    let value_type = batches.iter().find_map(|b| {
        b.schema()
            .column_with_name("value")
            .map(|(_, f)| f.data_type().clone())
    });
    assert_eq!(
        value_type,
        Some(DataType::Float64),
        "the value column must be Float64, matching what the router requires"
    );
}
