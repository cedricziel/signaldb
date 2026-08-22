//! # Compat-vs-IR differential harness (`ir-single-lowering`, design D2)
//!
//! Before any compat endpoint is rerouted onto [`super::ir_planner`], this
//! module lowers the same query through **both** paths — the existing
//! per-language lowering (`search_filter.rs`, `logql.rs`/`logql_metric.rs`)
//! and `ql_ir::{traceql_to_ir, logql_to_ir}` + [`super::ir_planner::plan_document`]
//! — over an identical fixture, and asserts the two agree. A difference is a
//! finding to explain, not a failure to route around (design D2): it is
//! triaged into exactly one of three outcomes below, never silently
//! papered over by loosening the comparison.
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
//!    strings (`TRACEQL_CORPUS`, `TAGS_CORPUS`).
//! 2. `tests-integration/tests/logql_queries.rs` — 4 LogQL strings, one of
//!    them a metric query (`LOGQL_LOG_CORPUS`, `LOGQL_METRIC_CORPUS`).
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
//! Total corpus: 18 TraceQL `q` strings + 3 `tags` strings + 8 LogQL log
//! queries + 7 LogQL metric queries = **36 queries**, plus the six
//! adversarial cases (task 2.2) below.
//!
//! ## Triage table (task 2.4)
//!
//! | Query | Outcome | Note |
//! |---|---|---|
//! | `{ status = error }`, `{ kind = server }` | **(1) `ql_ir` was wrong, fixed** | `traceql_to_ir` passed the TraceQL spelling through verbatim; the traces table stores these enums Title-cased (`status_code_to_str`/`span_kind_to_str`). Fixed in `ql-ir::traceql_lower` (see `normalize_status`/`normalize_kind`); the pinned `ql-ir` unit test was updated to the corrected values. Commit: `fix(ql-ir): normalize TraceQL status/kind values to their stored casing`. |
//! | **Every LogQL line filter** (`|=`/`!=`/`|~`/`!~`) | **(3) the biggest open finding — reported, not picked** | `ql_ir::logql_lower::line_filter` lowers to `Leaf{field:"body", op:Contains}`, but `LogicalSchema::core()` marks `logs.body` `RetrievalOnly` (deliberately, per `ir_planner`'s own `retrieval_only_metadata_cannot_be_used_in_predicates` test) — `plan_document`'s validation rejects every one of these documents. This is *not* the D5 fallback case: `logql_to_ir` returns `Ok`, not `Inexpressible`, so the rejection surfaces only after the point a §4 fallback switch would check. `ql-ir`'s own `every_lowered_document_validates` test missed this because it validates against a permissive `InMemoryResolver`, never the real `SchemaResolver`. Pinned in `logql_line_filter_is_rejected_by_the_real_schema`. Whoever builds §4 needs to decide: relax `body`'s retrievability (for `contains`/`regex` ops only?), or add a plan-time-Inexpressible-equivalent the switch can also fall back on. |
//! | A LogQL stream-selector `!=` against a key some rows lack | **(3) genuinely different meaning — reported, not picked** | The old LogQL lowering's `!=`/`!~` explicitly matches an absent key (`e.is_null().or(e.not_eq(...))`, documented "mirroring the JSON path"); `ir_planner`'s `Predicate::Not` is a plain `not(...)`, which is NULL for a NULL input — the IR's stated Kleene semantics ("absent satisfies neither `field = x` nor `not(field = x)`"). Pinned as `adversarial_absent_value_semantics_diverge` below, which asserts the *divergence* rather than agreement. Reported for a product decision (does the compat surface promise Loki's negation-matches-absent behaviour, or the IR's three-valued one) rather than silently choosing a side. |
//! | `{ .service.name = "api" }`, `{ .http.method = "GET" }` (TraceQL, unscoped), `{k8s_namespace="prod"}` (LogQL, no known column) | **(3) genuinely different meaning — reported, not picked** | The old path ORs the match across every container (`map_attribute_expr("span_attributes",..).or(map_attribute_expr("resource_attributes",..))`) — matches if *any* container has the value. `ir_planner`'s bare-name resolution COALESCEs across containers by priority (span/log, then scope, then resource) and compares *once* — if a higher-priority container has the key at all (regardless of value), lower-priority containers are never consulted. Pinned in `adversarial_unscoped_attribute_combining_semantics_diverge` with a fixture where the same key holds *different* values in two containers, which the corpus's own fixture data happened not to exercise (both sides agreed there only because one container lacked the key entirely). The LogQL corpus's `{k8s_namespace="prod"}` hits the identical mechanism (skipped via `KNOWN_COMBINING_DIVERGENCE_LOGQL_LOG`, no separate pinned test). Reported: which combining rule the compat surface should keep is a product question. |
//! | `{ span.http.method = "GET" }` against a table with a promoted `label_http_method` column | **(1) `ir_planner` was wrong, fixed in §3** | `search_filter::to_expr` checks `materialized_column_name(key)` against the **bare** attribute key (`"http.method"` → `label_http_method`), matching how the compactor actually names promoted columns (`attr_promotion::materialized_keys_of` keys off the raw `attr_key`, never the TraceQL-scoped spelling). `ir_planner::SchemaResolver::column_for` used to compute `materialized_column_name(field)` against the **scope-qualified** logical field (`"span.http.method"` → `label_span_http_method`), which no real promoted column is ever named — so `ir_planner` always took the `get_field` (JsonPath) branch for a scope-qualified attribute, never the promoted column, even when one existed. Fixed in `ir_planner::SchemaResolver::column_for` (task 3.0): strip the scope qualifier before materializing, the same way `Lowering::qualified_attr` already does for the unpromoted extraction path. The pin moved from a row-level comparison to a plan-level one, now that the plans genuinely agree — see `adversarial_promoted_attribute_agrees_on_plan`. Commit: `fix(querier): resolve promoted columns for scope-qualified IR fields`. |
//! | `sum by (StatusCode) (count_over_time(...))` (mixed-case attribute grouping) | **(1) old path is wrong, still open** | #1070's fix (`ident()` not `col()` for a group-column alias) only touched `ir_planner.rs`. The *old* LogQL metric path (`logs.rs`'s `execute_plan`) has the identical unfixed bug for grouping by a mixed-case attribute label. New path (routed through `plan_document`) already handles it correctly. Pinned in `adversarial_mixed_case_grouping_label_old_path_still_has_1070`; reported as a still-open bug in `logs.rs`, independent of this change, not fixed here. |
//! | `count_over_time(...)`/`rate(...)`/`sum_over_time(...)` etc. with **no** outer `by` | **(3) genuinely different meaning — reported, not picked** | Real Loki returns one series per matching *stream* for a bare range aggregation. The old path approximates this by defaulting the range aggregate's grouping to `SERIES_COLUMNS` (`service_name`, `severity_text`) when ungrouped. `ql_ir::logql_to_ir` emits `by: []` for the same shape, and `ir_planner` groups by nothing — collapsing every matching row into one count. Pinned in `adversarial_ungrouped_range_aggregation_default_grouping_diverges` (two `api` rows of different severities: old produces 2 rows, new produces 1). This is likely the single biggest behavioural gap for real dashboards (per-stream matrices are the common case), and — like the line-filter finding above — needs a design decision (does the IR need a "natural series identity" default?) rather than a pick made here. |
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
    Array, Float64Array, MapBuilder, MapFieldNames, RecordBatch, StringArray, StringBuilder,
    TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use datafusion::catalog::memory::{MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::catalog::{CatalogProvider, MemTable, SchemaProvider};
use datafusion::prelude::{DataFrame, SessionContext, col, lit};

use common::query_ir::Document;
use logql::Expr as LogqlExpr;

use super::MetricQueryParams;
use super::error::QuerierError;
use super::ir_planner::plan_document;
use super::logql::{AttrContext, log_query_filter_with_columns};
use super::logs::{LogsService, materialized_columns_of};
use super::search_filter;
use super::table_lookup::optional_table;

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

/// TraceQL `q` strings. Sources: `router_tempo_endpoints.rs`'s
/// `test_search_filters_are_applied` (the first 7) and
/// `src/ql-ir/tests/traceql.rs`'s corpus (the rest, deduplicated).
const TRACEQL_CORPUS: &[&str] = &[
    r#"{ span.http.method = "GET" }"#,
    "{ duration > 100ms }",
    r#"{ span.x != "y" }"#,
    r#"{ span.a = "1" || span.b = "2" }"#,
    "notbraces",
    "{ foo }",
    "{ zzz = 1 }",
    r#"{ name = "GET /api" }"#,
    "{ status = error }",
    "{ kind = server }",
    r#"{ resource.service.name = "api" }"#,
    r#"{ .service.name = "api" }"#,
    r#"{ resource.k8s.pod.name = "p" }"#,
    r#"{ .http.method = "GET" }"#,
    r#"{ resource.service.name = "api" && span.http.method = "GET" }"#,
    "{ span.http.status_code = 500 }",
    "{ span.ok = true }",
    "{}",
];

/// Tempo `tags` (logfmt) strings, from `test_search_filters_are_applied`.
const TAGS_CORPUS: &[&str] = &[
    "service.name=filter-test-service",
    "service.name=does-not-exist",
    "justaword",
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

/// LogQL metric-query strings. Sources: `logql_queries.rs`'s
/// `count_over_time` query and `src/ql-ir/tests/logql.rs`'s corpus.
const LOGQL_METRIC_CORPUS: &[&str] = &[
    r#"count_over_time({service_name="api"}[1h])"#,
    r#"rate({service_name="api"}[5m])"#,
    r#"sum by (service_name) (count_over_time({service_name="api"}[1m]))"#,
    r#"sum_over_time({service_name="api"} | unwrap duration [5m])"#,
    r#"avg_over_time({service_name="api"} | unwrap duration [5m])"#,
    r#"min by (service_name) (min_over_time({service_name="api"} | unwrap duration [1m]))"#,
    r#"max by (service_name) (max_over_time({service_name="api"} | unwrap duration [1m]))"#,
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
/// dedicated columns, a `duration` numeric attribute (for `unwrap`), and
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
        // `unwrap <label>` (old and new) resolves against a physical/
        // materialized column, not an attribute-map extraction — see
        // `column_for_label`/`materialized_column_name` in `logql.rs` and
        // the equivalent in `ir_planner`'s resolver.
        Field::new("duration", DataType::Float64, true),
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
            Arc::new(Float64Array::from(vec![Some(1.5), Some(2.5), Some(0.5)])),
            build_map(&[
                &[("region", "eu")],
                &[("region", "eu")],
                // Row 2 has no `region` key at all — the absent-value case.
                &[],
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

/// The old TraceQL path's classification: parse, then lower every condition.
fn old_traceql_class(q: &str, ctx: &AttrContext) -> Class {
    let conditions = match traceql::parse(q) {
        Ok(c) => c,
        Err(e) => return class_of_querier_err(&QuerierError::from(e)),
    };
    for c in &conditions {
        if let Err(e) = search_filter::to_expr(c, ctx) {
            return class_of_querier_err(&e);
        }
    }
    Class::Accept
}

/// The new TraceQL path's classification: `ql_ir::traceql_to_ir` alone (the
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

fn old_tags_class(tags: &str) -> Class {
    match search_filter::parse_tags(tags) {
        Ok(_) => Class::Accept,
        Err(e) => class_of_querier_err(&e),
    }
}

/// The stand-in for D4's `Condition`-to-IR shim (task 3.2), which does not
/// exist yet — this change lands the harness *before* any endpoint moves.
/// Tags share TraceQL's intrinsic vocabulary by construction
/// (`search_filter::tags_selector`), so a tags condition can be lowered by
/// asking `ql_ir` for exactly the field name TraceQL's matching selector
/// would use, then wrapping the raw value as a string leaf. This is
/// deliberately not exported: it exists to give the harness something to
/// diff before D4 is built, not to anticipate D4's own shape.
fn tags_condition_class(condition: &traceql::Condition) -> Class {
    // A tags condition is always a single equality; round-trip it through a
    // one-condition TraceQL spanset so the *same* `ql_ir` code path (not a
    // duplicate of it) decides field naming and value normalization.
    let rendered = match &condition.selector {
        traceql::Selector::ServiceName => format!(
            "{{ resource.service.name = {} }}",
            filter_value_literal(&condition.value)
        ),
        traceql::Selector::SpanName => {
            format!("{{ name = {} }}", filter_value_literal(&condition.value))
        }
        traceql::Selector::Status => {
            format!("{{ status = {} }}", filter_value_literal(&condition.value))
        }
        traceql::Selector::Kind => {
            format!("{{ kind = {} }}", filter_value_literal(&condition.value))
        }
        traceql::Selector::AnyAttribute(key) => {
            format!("{{ .{key} = {} }}", filter_value_literal(&condition.value))
        }
        other => panic!("tags_condition_class: unexpected selector {other:?}"),
    };
    new_traceql_class(&rendered)
}

fn filter_value_literal(v: &traceql::FilterValue) -> String {
    match v {
        traceql::FilterValue::String(s) => format!("{s:?}"),
        traceql::FilterValue::Number(n) => n.clone(),
        traceql::FilterValue::Bool(b) => b.to_string(),
        other => panic!("unexpected tags filter value {other:?}"),
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

/// The old TraceQL search plan: scan, the fixture's time window, every
/// condition folded in, then the fixed projection — the minimal comparable
/// shape (module doc's comparison rules).
async fn old_traceql_plan(
    ctx: &SessionContext,
    q: &str,
    fields: &[&str],
) -> Result<String, QuerierError> {
    let conditions = traceql::parse(q)?;
    let mut df = optional_table(ctx, TENANT, DATASET, "traces")
        .await?
        .expect("traces table is registered");
    df = df
        .filter(col("start_time_unix_nano").gt_eq(lit(FROM)))?
        .filter(col("start_time_unix_nano").lt_eq(lit(TO)))?;
    let attr_ctx = AttrContext {
        materialized: materialized_columns_of(&df),
        map_attrs: is_map_column(&df, "span_attributes"),
        attr_tokens: false,
    };
    for c in &conditions {
        df = df.filter(search_filter::to_expr(c, &attr_ctx)?)?;
    }
    df = df.select_columns(fields)?;
    Ok(optimized_plan_text(df))
}

/// The new TraceQL plan: `traceql_to_ir` then `plan_document`, with `fields`
/// overridden to the same projection the old side used (comparison rules).
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
// ---------------------------------------------------------------------------

#[tokio::test]
async fn traceql_corpus_rejections_agree() {
    let ctx = AttrContext::default();
    for q in TRACEQL_CORPUS {
        assert_eq!(
            old_traceql_class(q, &ctx),
            new_traceql_class(q),
            "{q}: old/new rejection classification disagree"
        );
    }
}

/// Corpus queries whose plans are known, triaged exceptions (module doc's
/// triage table) rather than agreement — pinned by their own dedicated test
/// below, and skipped here so this test still asserts everything else.
const KNOWN_PLAN_DIVERGENCE_TRACEQL: &[&str] = &[
    // Unscoped attribute match across containers: old ORs a match across
    // every container; `ir_planner` COALESCEs to the first present
    // container's value, then compares once. Different combining semantics
    // when a key exists in two containers with two different values — see
    // `adversarial_unscoped_attribute_combining_semantics_diverge`.
    r#"{ .service.name = "api" }"#,
    r#"{ .http.method = "GET" }"#,
];

#[tokio::test]
async fn traceql_corpus_filters_agree_on_optimized_plan() {
    let ctx = traces_fixture();
    let fields = ["trace_id"];
    for q in TRACEQL_CORPUS {
        if KNOWN_PLAN_DIVERGENCE_TRACEQL.contains(q) {
            continue;
        }
        // Only compare a plan for queries both sides accept — a rejection has
        // no plan to diff (covered by the rejections test above).
        if old_traceql_class(q, &AttrContext::default()) != Class::Accept {
            continue;
        }
        let old = old_traceql_plan(&ctx, q, &fields)
            .await
            .unwrap_or_else(|e| {
                panic!("{q}: old path should accept per its own classification: {e}")
            });
        let new = new_traceql_plan(&ctx, q, &fields)
            .await
            .unwrap_or_else(|e| panic!("{q}: new path rejected an old-accepted query: {e}"));
        assert_plans_match(q, &old, &new);
    }
}

/// Pins the divergence `KNOWN_PLAN_DIVERGENCE_TRACEQL` skips (triage table
/// case 3): an unscoped attribute present in two containers with two
/// *different* values. Old path: OR across every container — matches if
/// *any* container has the value. New path: COALESCE by container priority
/// (span, then scope, then resource) — compares only the first container
/// that has the key at all, ignoring the others even if they would match.
/// Reported rather than fixed — this is `ir_planner`'s general coalescing
/// strategy for every unscoped/bare field, not specific to TraceQL.
#[tokio::test]
async fn adversarial_unscoped_attribute_combining_semantics_diverge() {
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

    let old_rows = old_traceql_query_df(&ctx, q, &fields)
        .await
        .collect()
        .await
        .unwrap();
    let old_count: usize = old_rows.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        old_count, 1,
        "old path: OR across containers should match the resource-scope GET"
    );

    let mut doc = ql_ir::traceql_to_ir(q, FROM_NS, TO_NS).unwrap();
    set_fields(&mut doc, &fields);
    let (new_df, _) = plan_document(&ctx, &doc, TENANT, DATASET, 0)
        .await
        .unwrap()
        .unwrap();
    let new_count: usize = new_df
        .collect()
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(
        new_count, 0,
        "new path: coalesce takes span's \"POST\" first and never looks at resource's \"GET\" (the tracked gap — if this now matches, update the triage table)"
    );
}

async fn old_traceql_query_df(ctx: &SessionContext, q: &str, fields: &[&str]) -> DataFrame {
    let conditions = traceql::parse(q).unwrap();
    let mut df = optional_table(ctx, TENANT, DATASET, "traces")
        .await
        .unwrap()
        .unwrap();
    let attr_ctx = AttrContext {
        materialized: materialized_columns_of(&df),
        map_attrs: is_map_column(&df, "span_attributes"),
        attr_tokens: false,
    };
    for c in &conditions {
        df = df
            .filter(search_filter::to_expr(c, &attr_ctx).unwrap())
            .unwrap();
    }
    df.select_columns(fields).unwrap()
}

#[tokio::test]
async fn tags_corpus_rejections_agree() {
    for tags in TAGS_CORPUS {
        let old = old_tags_class(tags);
        let new = match search_filter::parse_tags(tags) {
            // `parse_tags` is shared, so a parse rejection is identical by
            // construction — reuse `old`'s already-computed classification
            // rather than reparsing.
            Err(_) => old,
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
        assert_eq!(
            old, new,
            "{tags}: old/new rejection classification disagree"
        );
    }
}

#[tokio::test]
async fn tags_corpus_filters_agree_on_optimized_plan() {
    let ctx = traces_fixture();
    let fields = ["trace_id"];
    for tags in TAGS_CORPUS {
        let Ok(conditions) = search_filter::parse_tags(tags) else {
            continue;
        };
        // Old plan: scan + window + each condition's `to_expr`, same shape as
        // `old_traceql_plan`.
        let mut old_df = optional_table(&ctx, TENANT, DATASET, "traces")
            .await
            .unwrap()
            .unwrap();
        let attr_ctx = AttrContext {
            materialized: materialized_columns_of(&old_df),
            map_attrs: true,
            attr_tokens: false,
        };
        old_df = old_df
            .filter(col("start_time_unix_nano").gt_eq(lit(FROM)))
            .unwrap()
            .filter(col("start_time_unix_nano").lt_eq(lit(TO)))
            .unwrap();
        for c in &conditions {
            old_df = old_df
                .filter(search_filter::to_expr(c, &attr_ctx).unwrap())
                .unwrap();
        }
        let old = optimized_plan_text(old_df.select_columns(&fields).unwrap());

        // New plan: each condition rendered as a one-leaf TraceQL spanset
        // (the D4 stand-in, see `tags_condition_class`), conjoined.
        let rendered: Vec<String> = conditions
            .iter()
            .map(|c| match &c.selector {
                traceql::Selector::AnyAttribute(key) => {
                    format!(".{key} = {}", filter_value_literal(&c.value))
                }
                traceql::Selector::ServiceName => {
                    format!("resource.service.name = {}", filter_value_literal(&c.value))
                }
                traceql::Selector::SpanName => format!("name = {}", filter_value_literal(&c.value)),
                other => panic!("unexpected tags selector {other:?}"),
            })
            .collect();
        let spanset = format!("{{ {} }}", rendered.join(" && "));
        let new = new_traceql_plan(&ctx, &spanset, &fields)
            .await
            .unwrap_or_else(|e| {
                panic!("{tags} (as {spanset}): new path rejected an old-accepted query: {e}")
            });
        assert_plans_match(tags, &old, &new);
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

/// Corpus queries whose plans are known, triaged exceptions rather than
/// agreement — every LogQL **line filter** (`|=`, `!=`, `|~`, `!~`), pinned
/// by `logql_line_filter_is_rejected_by_the_real_schema` below rather than
/// compared here (the new path does not produce a plan for these at all).
const KNOWN_PLAN_DIVERGENCE_LOGQL_LOG: &[&str] = &[
    r#"{service_name="api"} |= "boom""#,
    r#"{service_name="api"} |~ "err.*""#,
    r#"{service_name="api"} !="boom""#,
    r#"{service_name="api", level="error"} |= "timeout""#,
];

/// `{k8s_namespace="prod"}` is the same OR-vs-COALESCE combining-semantics
/// divergence `adversarial_unscoped_attribute_combining_semantics_diverge`
/// pins for TraceQL: `k8s_namespace` has no dedicated column or logical
/// name, so both paths coalesce it across `log_attributes`/
/// `resource_attributes`, with the same OR-vs-COALESCE mismatch. No separate
/// pinned test — the mechanism is identical and already demonstrated there.
const KNOWN_COMBINING_DIVERGENCE_LOGQL_LOG: &[&str] = &[r#"{k8s_namespace="prod"}"#];

#[tokio::test]
async fn logql_log_corpus_filters_agree_on_optimized_plan() {
    let ctx = logs_fixture();
    let fields = ["timestamp", "body"];
    for q in LOGQL_LOG_CORPUS {
        if KNOWN_PLAN_DIVERGENCE_LOGQL_LOG.contains(q)
            || KNOWN_COMBINING_DIVERGENCE_LOGQL_LOG.contains(q)
        {
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

/// **The most consequential finding this harness produced.** Every LogQL
/// line filter (`|=`/`!=`/`|~`/`!~` — LogQL's defining feature) lowers, via
/// `ql_ir::logql_lower::line_filter`, to a `Leaf { field: "body", op:
/// Contains, .. }` predicate. `LogicalSchema::core()` marks `logs.body`
/// `RetrievalOnly` (pinned by `ir_planner`'s own
/// `retrieval_only_metadata_cannot_be_used_in_predicates` test), so
/// `query_ir::validate` — called from `plan_document`, which every compat
/// query would route through once §4 lands — rejects the resulting document
/// outright: `InvalidInput("field 'body' is retrievable but cannot be used
/// in a predicate")`.
///
/// This is not the D5 fallback case: `ql_ir::logql_to_ir` returns `Ok`, not
/// `Inexpressible` — the rejection only surfaces later, at `plan_document`'s
/// validation, which is *after* the point §4's per-signal switch was
/// designed to check for a fallback trigger. `ql-ir`'s own
/// `every_lowered_document_validates` test (src/ql-ir/tests/logql.rs)
/// validates the same shape of document successfully only because it uses a
/// permissive `InMemoryResolver` that does not mark `body` retrieval-only —
/// it never exercises the real `LogicalSchema::core()`/`SchemaResolver` this
/// harness does, so it could not have caught this.
///
/// Reported, not fixed: relaxing `body`'s retrieval-only status changes a
/// deliberate, tested guard in `query-ir-core`'s filterability contract, and
/// deciding how (loosen it for `contains`/`regex` ops only? add an
/// IR-plan-time Inexpressible-equivalent so §4's switch can still fall back?)
/// is a design decision for whoever builds §4, not this change.
#[tokio::test]
async fn logql_line_filter_is_rejected_by_the_real_schema() {
    let ctx = logs_fixture();
    for q in KNOWN_PLAN_DIVERGENCE_LOGQL_LOG {
        let doc = ql_ir::logql_to_ir(q, FROM_NS, TO_NS).unwrap_or_else(|e| {
            panic!("{q}: ql_ir should still lower this (Ok, not Inexpressible): {e}")
        });
        let err = plan_document(&ctx, &doc, TENANT, DATASET, 0)
            .await
            .expect_err(&format!(
                "{q}: if this now plans, body's retrievability was relaxed — update the triage table, don't just delete this assertion"
            ));
        assert!(
            matches!(err, QuerierError::InvalidInput(ref msg) if msg.contains("body")),
            "{q}: expected the body-retrievability rejection, got {err}"
        );
        // The old path, unaffected, still accepts every one of these.
        assert_eq!(
            old_logql_log_class(q, &AttrContext::default()),
            Class::Accept,
            "{q}: old path should still accept this query"
        );
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

/// Corpus metric queries with no explicit outer `by` grouping — pinned by
/// `adversarial_ungrouped_range_aggregation_default_grouping_diverges`
/// rather than compared here, since the two sides disagree on how many
/// output rows an ungrouped query even produces (see that test's doc).
const KNOWN_GROUPING_DIVERGENCE_LOGQL_METRIC: &[&str] = &[
    r#"count_over_time({service_name="api"}[1h])"#,
    r#"rate({service_name="api"}[5m])"#,
    r#"sum_over_time({service_name="api"} | unwrap duration [5m])"#,
    r#"avg_over_time({service_name="api"} | unwrap duration [5m])"#,
];

/// Not a finding: the old LogQL metric path's `unwrap <label>` resolves the
/// label as a **bare physical column reference** (`col(label)` verbatim, no
/// materialized-name mapping — see `logql_metric.rs`'s `unwrap_label`), so a
/// comparable fixture needs a real `duration` column. But `ir_planner`'s
/// resolver then refuses that same bare name as a *physical column or
/// storage detail* (`SchemaResolver::is_physical_name`) unless
/// `LogicalSchema::core()` declares it for `logs` — which it does not
/// (`duration` is declared only for `traces`). Building a fixture where
/// `unwrap`'s target satisfies both sides at once needs either a
/// `LogicalSchema::core()` change (a `query-ir` schema decision, not a
/// fixture one) or deeper changes to how `ir_planner` resolves an
/// aggregate's `of` field — out of reach for this harness in the time this
/// change budgeted for it. Skipped rather than asserted either way.
const SKIPPED_LOGQL_METRIC_UNWRAP_GROUPED: &[&str] = &[
    r#"min by (service_name) (min_over_time({service_name="api"} | unwrap duration [1m]))"#,
    r#"max by (service_name) (max_over_time({service_name="api"} | unwrap duration [1m]))"#,
];

#[tokio::test]
async fn logql_metric_corpus_row_level_equivalence() {
    let ctx = logs_fixture();
    let params = MetricQueryParams {
        query: String::new(),
        start: FROM,
        end: TO,
        step: 1_000,
    };
    for q in LOGQL_METRIC_CORPUS {
        if KNOWN_GROUPING_DIVERGENCE_LOGQL_METRIC.contains(q)
            || SKIPPED_LOGQL_METRIC_UNWRAP_GROUPED.contains(q)
        {
            continue;
        }
        let old_svc = LogsService::new(clone_ctx(&ctx));
        let old_params = MetricQueryParams {
            query: q.to_string(),
            ..params.clone()
        };
        let old_batches = old_svc
            .query_metric(&old_params, TENANT, DATASET)
            .await
            .unwrap_or_else(|e| panic!("{q}: old metric path failed: {e}"));

        let mut doc = ql_ir::logql_to_ir(q, FROM_NS, TO_NS)
            .unwrap_or_else(|e| panic!("{q}: new metric path rejected an old-accepted query: {e}"));
        // The IR's `Series` result returns the aggregate output unprojected
        // (`apply_projection`'s early-return for `series_shaped`), so no
        // `fields` override is needed or honoured here.
        let (df, _) = plan_document(&ctx, &doc, TENANT, DATASET, 0)
            .await
            .unwrap_or_else(|e| panic!("{q}: new metric path failed to plan: {e}"))
            .expect("logs table is registered");
        let new_batches = df
            .collect()
            .await
            .unwrap_or_else(|e| panic!("{q}: new metric path failed to execute: {e}"));

        // Both sides group by `service.name`/`service_name` when the query
        // asks for it, else the fixture's implicit series identity
        // (`service_name`). The value column is named differently per side
        // (old: the LogQL-chosen alias; new: the IR aggregate's `as_name`,
        // which `ql_ir::logql_lower` always calls `value` for a bare range
        // aggregate) — normalized here rather than in production code, since
        // it is purely a column-naming difference this harness needs to see
        // past, not a lowering behaviour either side should change.
        let group_cols = grouping_columns(&mut doc);
        let old_value_col = old_value_column(q);
        let old_rows = metric_rows(&old_batches, &group_cols, old_value_col);
        let new_rows = metric_rows(&new_batches, &group_cols, "value");
        assert_eq!(
            old_rows, new_rows,
            "{q}: old/new metric rows disagree (sorted, grouped by {group_cols:?})"
        );
    }
}

/// **The second major finding.** A LogQL range aggregation with no outer
/// vector aggregation (`count_over_time(...)` alone, not `sum by (..)
/// (count_over_time(...))`) is, in real Loki, one series *per matching
/// stream* — every distinct label combination the selector matches gets its
/// own point. The old path approximates this by defaulting
/// `range_group_cols` to `SERIES_COLUMNS` (`service_name`, `severity_text`)
/// when `group_labels` is empty (`logs.rs::execute_plan`). `ql_ir`'s
/// `logql_to_ir` emits `Aggregate { by: vec![], .. }` for the same query —
/// `Lowering::lower_aggregate` groups by nothing, collapsing every matching
/// row into one overall count. Demonstrated here: the fixture's two `api`
/// rows (one `ERROR`, one `INFO` — two distinct `service_name`+
/// `severity_text` streams) produce two rows of count 1 on the old path and
/// one row of count 2 on the new path.
///
/// Reported, not fixed: giving the IR an implicit "natural series identity"
/// grouping is a real design question (which columns constitute a
/// "stream"? every dedicated LogQL label column, always? does an IR-native
/// `count_over_time`-shaped query want the same default?) well beyond this
/// harness's job of finding the gap.
#[tokio::test]
async fn adversarial_ungrouped_range_aggregation_default_grouping_diverges() {
    let ctx = logs_fixture();
    let q = r#"count_over_time({service_name="api"}[1h])"#;

    let old_svc = LogsService::new(clone_ctx(&ctx));
    let old_batches = old_svc
        .query_metric(
            &MetricQueryParams {
                query: q.to_string(),
                start: FROM,
                end: TO,
                step: 1_000,
            },
            TENANT,
            DATASET,
        )
        .await
        .unwrap_or_else(|e| panic!("{q}: old path failed: {e}"));
    let old_row_count: usize = old_batches.iter().map(|b| b.num_rows()).sum();

    let doc = ql_ir::logql_to_ir(q, FROM_NS, TO_NS).unwrap_or_else(|e| panic!("{q}: {e}"));
    let (df, _) = plan_document(&ctx, &doc, TENANT, DATASET, 0)
        .await
        .unwrap_or_else(|e| panic!("{q}: new path failed to plan: {e}"))
        .expect("logs table is registered");
    let new_batches = df
        .collect()
        .await
        .unwrap_or_else(|e| panic!("{q}: new path failed to execute: {e}"));
    let new_row_count: usize = new_batches.iter().map(|b| b.num_rows()).sum();

    assert_eq!(
        old_row_count, 2,
        "old path: one row per (service_name, severity_text) stream — two api rows, two severities"
    );
    assert_eq!(
        new_row_count, 1,
        "if this now matches the old path's per-stream row count, the default-grouping gap was closed — update the triage table, don't just delete this assertion"
    );
}

/// The `by` grouping labels a metric document declares, aliased the way
/// `Lowering::lower_aggregate` names them (`safe_ident`, which is the
/// identity for an already-valid identifier such as `service_name`).
fn grouping_columns(doc: &mut Document) -> Vec<&'static str> {
    for stage in &doc.pipeline {
        if let common::query_ir::Stage::Aggregate(agg) = stage {
            // Every corpus metric query below groups by `service_name` when
            // it groups at all; an ungrouped query collapses to one series
            // with no group columns in its output at all.
            return if agg.by.is_empty() {
                Vec::new()
            } else {
                vec!["service_name"]
            };
        }
    }
    Vec::new()
}

/// The old path's per-query value-column alias — `logs.rs::execute_plan`
/// names the output column after the metric function
/// (`super::logql_metric::Aggregate`'s Display, not reproduced here); every
/// corpus query above resolves to `"value"` in the shipped querier's matrix
/// assembly, confirmed by `logql_metric_corpus_row_level_equivalence`
/// failing loudly (a missing-column panic, not a silent pass) if that ever
/// stops being true.
fn old_value_column(_q: &str) -> &'static str {
    "value"
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

/// Promoted attribute, scope-qualified field: after the D10 fix
/// (`ir_planner::SchemaResolver::column_for` strips the scope qualifier
/// before materializing a promoted column's name, task 3.0), both paths
/// resolve `span.http.method` to the promoted `label_http_method` column —
/// this moved from triage table case (2) "both correct, plans differ" to
/// case (1) "`ir_planner` was wrong, fixed in §3", so the comparison is now
/// at the optimized-plan level, like every other agreeing case, rather than
/// the weaker row-level one it needed while the plans still legitimately
/// differed.
#[tokio::test]
async fn adversarial_promoted_attribute_agrees_on_plan() {
    let ctx = traces_promoted_fixture();
    let q = r#"{ span.http.method = "GET" }"#;
    let fields = ["trace_id"];

    let old = old_traceql_plan(&ctx, q, &fields).await.unwrap();
    let new = new_traceql_plan(&ctx, q, &fields).await.unwrap();
    assert!(
        old.contains("label_http_method") && new.contains("label_http_method"),
        "both paths should route to the promoted column:\nold:\n{old}\nnew:\n{new}"
    );
    assert!(
        !old.contains("get_field") && !new.contains("get_field"),
        "neither path should fall back to json-path extraction once a promoted column exists:\nold:\n{old}\nnew:\n{new}"
    );
    assert_plans_match(q, &old, &new);
}

/// An attribute key colliding with a physical column name
/// (`span_attributes["service_name"]` disagrees with the real
/// `service_name` column): filtering the *logical* `service.name` field
/// must resolve to the physical column on both paths, never the colliding
/// attribute.
#[tokio::test]
async fn adversarial_attribute_key_collides_with_physical_column() {
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

    let old = old_traceql_plan(&ctx, q, &fields).await.unwrap();
    let new = new_traceql_plan(&ctx, q, &fields).await.unwrap();
    assert!(
        !old.contains("get_field") && !new.contains("get_field"),
        "both paths must resolve service.name to the physical column, not the colliding attribute:\nold:\n{old}\nnew:\n{new}"
    );
    assert_plans_match(q, &old, &new);
}

/// #1070's fix (`Lowering::lower_aggregate` referencing its group-column
/// alias via `ident()` instead of `col()`) only touched `ir_planner.rs`.
/// This adversarial case went looking for the same bug class in the *old*
/// LogQL metric path (`logs.rs`'s `execute_plan` grouping-column resolution)
/// — and found it still there: grouping by a mixed-case *attribute* label
/// (as opposed to a well-known dedicated-column label) fails on the old path
/// with `No field named label_statuscode. Did you mean 't.d.logs.label_StatusCode'?`,
/// the same lowercased-unquoted-identifier bug #1070 fixed on the IR side.
/// New path handles it correctly (proving #1070's fix generalizes to the
/// grouping path once queries are routed through `plan_document`). Reported
/// as a still-open bug in `logs.rs`, not fixed here — out of scope for this
/// harness change, and independent of the `ir-single-lowering` seam.
#[tokio::test]
async fn adversarial_mixed_case_grouping_label_old_path_still_has_1070() {
    let ctx = logs_mixed_case_fixture();
    let q = r#"sum by (StatusCode) (count_over_time({service_name="api"}[1m]))"#;

    let old_svc = LogsService::new(clone_ctx(&ctx));
    let old_err = old_svc
        .query_metric(
            &MetricQueryParams {
                query: q.to_string(),
                start: FROM,
                end: TO,
                step: 1_000,
            },
            TENANT,
            DATASET,
        )
        .await
        .expect_err(
            "if this now succeeds, the old path's own #1070-class bug was fixed — update the triage table, don't just delete this assertion",
        );
    assert!(
        matches!(old_err, QuerierError::QueryFailed(_)),
        "{q}: expected the old path's still-open #1070-class grouping bug, got {old_err}"
    );

    let doc = ql_ir::logql_to_ir(q, FROM_NS, TO_NS).unwrap_or_else(|e| panic!("{q}: {e}"));
    let (df, _) = plan_document(&ctx, &doc, TENANT, DATASET, 0)
        .await
        .unwrap_or_else(|e| panic!("{q}: new path failed to plan: {e}"))
        .expect("logs table is registered");
    let new_batches = df
        .collect()
        .await
        .unwrap_or_else(|e| panic!("{q}: new path failed to execute (a #1070 regression): {e}"));
    let new_rows = metric_rows(&new_batches, &["StatusCode"], "value");
    assert_eq!(
        new_rows.len(),
        2,
        "new path should group the two StatusCode values: {new_rows:?}"
    );
}

/// The pinned divergence (triage table case 3): old LogQL's `!=` explicitly
/// matches rows where the key is absent; `ir_planner`'s `not(...)` follows
/// Kleene semantics, where an absent key satisfies neither `=` nor `!=`.
/// This asserts the *difference*, not agreement — see the module doc; this
/// is the one finding this change reports rather than resolves.
#[tokio::test]
async fn adversarial_absent_value_semantics_diverge() {
    let ctx = logs_fixture();
    // Row 2 ("slow response", service `web`) has no `region` key at all —
    // the other two rows (service `api`) both have `region="eu"`.
    let q = r#"{region!="eu"}"#;
    let fields = ["body"];

    let old = old_logql_log_plan(&ctx, q, &fields).await.unwrap();
    let new = new_logql_log_plan(&ctx, q, &fields).await.unwrap();
    assert_ne!(
        old, new,
        "if this now matches, the semantics were reconciled — update the triage table, don't just delete this assertion:\n{old}"
    );

    // Concretely: row 2 (no `region` key) is absent from a row's map. Old
    // includes it (absent satisfies `!=`); new excludes it (Kleene).
    let old_bodies = old_logql_log_bodies(&ctx, q).await;
    let new_bodies = new_logql_log_bodies(&ctx, q).await;
    assert!(
        old_bodies.contains(&"slow response".to_string()),
        "old path's != should include the row with no `region` key at all: {old_bodies:?}"
    );
    assert!(
        !new_bodies.contains(&"slow response".to_string()),
        "new path's != should exclude the row with no `region` key at all (Kleene semantics): {new_bodies:?}"
    );
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
