use anyhow::{Result, anyhow};
use chrono::{DateTime, Datelike, Timelike};
use common::flight::conversion::UNKNOWN_SERVICE_NAME;
use common::iceberg::schemas::TYPED_METRIC_VERSION;
use common::schema::SCHEMA_DEFINITIONS;
use common::schema::resource_identity::resource_identity_from_json;
use common::schema::schema_parser::ResolvedSchema;
use common::schema::series_id::{SeriesKey, metric_series_id};
use datafusion::arrow::{
    array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Float64Array, Int32Array,
        Int64Array, ListArray, StringArray, StructArray, TimestampNanosecondArray, UInt32Array,
        UInt64Array,
    },
    datatypes::{ArrowPrimitiveType, DataType, Field, Float64Type, Int64Type, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use std::collections::HashMap;
use std::sync::Arc;

/// Arrow field metadata key stamped on every materialized `label_<key>`
/// column with the attribute key it was built from -- the WAL-batch
/// counterpart of [`common::iceberg::evolution::label_doc`], which records
/// the same origin key on the committed Iceberg column's `doc`.
/// `LabelColumnReconciliation::apply` (writer/src/storage/iceberg.rs)
/// resolves a batch's label columns by this key rather than by name, so a
/// name collision between config generations cannot misroute a value (see
/// #1534). A batch with no such metadata predates this change and falls
/// back to the old name-based guard.
pub(crate) const LABEL_ORIGIN_KEY_METADATA: &str = "signaldb.origin_key";

/// Struct to hold all extracted metadata from Flight messages
#[derive(Debug, Clone)]
pub struct FlightMetadata {
    pub schema_version: String,
    pub signal_type: Option<String>,
    pub target_table: Option<String>,
    pub tenant_id: Option<String>,
    pub dataset_id: Option<String>,
    /// W3C trace context of the sending service, for distributed tracing
    pub traceparent: Option<String>,
    pub tracestate: Option<String>,
    /// The content fingerprint of the batch this `do_put` carries (issue
    /// #1734), shared by every copy of it however many acceptor WAL entries
    /// hold one. Used to dedup a copy that reaches this writer again; absent
    /// for acceptors that predate this field, which get non-deduped
    /// behavior.
    pub ingest_id: Option<uuid::Uuid>,
}

/// Extract all metadata from Flight metadata bytes
/// Returns FlightMetadata with schema_version, signal_type, and target_table (if present)
pub fn extract_flight_metadata(metadata: &[u8]) -> Result<FlightMetadata> {
    let metadata_str =
        std::str::from_utf8(metadata).map_err(|e| anyhow!("Invalid UTF-8 in metadata: {}", e))?;

    let metadata_json: serde_json::Value = serde_json::from_str(metadata_str)
        .map_err(|e| anyhow!("Invalid JSON in metadata: {}", e))?;

    let schema_version = metadata_json
        .get("schema_version")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| anyhow!("Missing schema_version in metadata"))?;

    let signal_type = metadata_json
        .get("signal_type")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let target_table = metadata_json
        .get("target_table")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let tenant_id = metadata_json
        .get("tenant_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let dataset_id = metadata_json
        .get("dataset_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let traceparent = metadata_json
        .get("traceparent")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let tracestate = metadata_json
        .get("tracestate")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    // A present-but-unparseable ingest_id is the sender's fault and recurs
    // identically on every retry (same rationale as the other metadata
    // fields here) -- reject rather than silently treat it as absent, which
    // would revert to non-deduped behavior without telling anyone.
    let ingest_id = match metadata_json.get("ingest_id") {
        None | Some(serde_json::Value::Null) => None,
        Some(v) => {
            let s = v
                .as_str()
                .ok_or_else(|| anyhow!("ingest_id must be a string"))?;
            Some(
                uuid::Uuid::parse_str(s)
                    .map_err(|e| anyhow!("Invalid ingest_id in metadata: {}", e))?,
            )
        }
    };

    Ok(FlightMetadata {
        schema_version,
        signal_type,
        target_table,
        tenant_id,
        dataset_id,
        traceparent,
        tracestate,
        ingest_id,
    })
}

/// `signal_type` metadata this writer does not recognize. Deterministic in
/// its input, so the same value fails identically on every retry: it must be
/// rejected as `invalid_argument` at ingest, never silently defaulted to
/// `WriteTraces` — the previous fallback accepted a batch into the wrong
/// WAL, where it would fail transform/coercion on every commit cycle
/// (#1060 class).
#[derive(Debug, thiserror::Error)]
#[error("unknown signal_type {0:?}")]
pub struct UnknownSignalType(pub Option<String>);

/// Determine the WAL operation for a batch's `signal_type` metadata, or
/// reject it as unroutable.
pub fn determine_wal_operation(
    signal_type: Option<&str>,
) -> Result<common::wal::WalOperation, UnknownSignalType> {
    match signal_type {
        Some("traces") => Ok(common::wal::WalOperation::WriteTraces),
        Some("logs") => Ok(common::wal::WalOperation::WriteLogs),
        Some("metrics") => Ok(common::wal::WalOperation::WriteMetrics),
        Some("profiles") => Ok(common::wal::WalOperation::WriteProfiles),
        other => Err(UnknownSignalType(other.map(str::to_string))),
    }
}

/// Transform a trace RecordBatch from v1 to v2 schema
/// A compiled column extraction rule: given the incoming v1 batch, produce
/// the target v2 column. Always whole-batch-in, whole-array-out -- never
/// per-row dispatch. A boxed closure (not a bare `fn` pointer) because
/// field-parameterized rules (e.g. "read this UInt64 column, whichever one,
/// cast to Int64") need to capture the source column name.
type Extractor = Box<dyn Fn(&RecordBatch) -> Result<ArrayRef> + Send + Sync>;

/// The compiled v1->v2 materialization plan: one extractor per target
/// column, in target-schema field order, resolved once and reused for
/// every batch.
struct TraceV1ToV2Plan {
    schema: Arc<Schema>,
    extractors: Vec<Extractor>,
}

fn direct_extractor(name: &str) -> Extractor {
    let name = name.to_string();
    Box::new(move |batch| get_column_by_name(batch, &name))
}

fn nullable_int32_extractor(name: &str) -> Extractor {
    let name = name.to_string();
    Box::new(move |batch| get_column_by_name_or_null(batch, &name, &DataType::Int32))
}

fn nullable_int64_extractor(name: &str) -> Extractor {
    let name = name.to_string();
    Box::new(move |batch| get_column_by_name_or_null(batch, &name, &DataType::Int64))
}

/// Reads a same-named source column and casts UInt64 -> Int64 (Iceberg has
/// no unsigned type). Also used for a renamed source (`duration_nanos` <-
/// `duration_nano`) by passing the v1 source name instead of the target's
/// own name.
fn cast_uint64_to_int64_extractor(source_name: &str) -> Extractor {
    let source_name = source_name.to_string();
    Box::new(move |batch| {
        let col = get_column_by_name(batch, &source_name)?;
        let uint_array = col
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| anyhow!("{source_name} is not UInt64Array"))?;
        let values: Vec<Option<i64>> = (0..uint_array.len())
            .map(|i| {
                if uint_array.is_null(i) {
                    None
                } else {
                    Some(uint_array.value(i) as i64)
                }
            })
            .collect();
        Ok(Arc::new(Int64Array::from(values)) as ArrayRef)
    })
}

fn serialize_list_extractor(name: &'static str) -> Extractor {
    Box::new(move |batch| {
        let col = get_column_by_name(batch, name)?;
        serialize_list_array_to_json_strings(&col, batch.num_rows(), name)
    })
}

fn timestamp_from_start_time_extractor() -> Extractor {
    Box::new(|batch| {
        let start_times = get_column_by_name(batch, "start_time_unix_nano")?;
        let uint_array = start_times
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| anyhow!("start_time_unix_nano is not UInt64Array"))?;
        let timestamps: Vec<Option<i64>> = (0..uint_array.len())
            .map(|i| {
                if uint_array.is_null(i) {
                    None
                } else {
                    Some(uint_array.value(i) as i64)
                }
            })
            .collect();
        Ok(Arc::new(TimestampNanosecondArray::from(timestamps)) as ArrayRef)
    })
}

fn date_day_from_start_time_extractor() -> Extractor {
    Box::new(|batch| {
        let start_times = get_column_by_name(batch, "start_time_unix_nano")?;
        let uint_array = start_times
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| anyhow!("start_time_unix_nano is not UInt64Array"))?;
        let dates: Vec<Option<i32>> = (0..uint_array.len())
            .map(|i| {
                let nanos = uint_array.value(i);
                let secs = (nanos / 1_000_000_000) as i64;
                let dt = DateTime::from_timestamp(secs, 0)?;
                Some((dt.naive_utc().date().num_days_from_ce() - 719163) as i32)
            })
            .collect();
        Ok(Arc::new(Date32Array::from(dates)) as ArrayRef)
    })
}

fn hour_from_start_time_extractor() -> Extractor {
    Box::new(|batch| {
        let start_times = get_column_by_name(batch, "start_time_unix_nano")?;
        let uint_array = start_times
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| anyhow!("start_time_unix_nano is not UInt64Array"))?;
        let hours: Vec<Option<i32>> = (0..uint_array.len())
            .map(|i| {
                let nanos = uint_array.value(i);
                let secs = (nanos / 1_000_000_000) as i64;
                let dt = DateTime::from_timestamp(secs, 0)?;
                Some(dt.hour() as i32)
            })
            .collect();
        Ok(Arc::new(Int32Array::from(hours)) as ArrayRef)
    })
}

/// Resolves the compiled plan against `physical-v4` -- the last version
/// with `span_attributes`/`resource_attributes`/`scope_attributes` as
/// JSON-string columns, not `SCHEMA_DEFINITIONS.current_trace_version()`.
/// This plan's job is only to bridge the wire's v1 shape to that stable
/// intermediate shape; [`IcebergTableWriter::append_batches_with_marker`]'s
/// typed-container splitting handles the v4 -> v5 (typed layout) hop
/// afterward, generically, from whatever table schema is actually current
/// -- it does not need (and, for a `map<string,long>` typed column,
/// [`create_arrow_schema_from_resolved`] cannot build) this plan to track
/// the typed layout itself.
fn build_trace_v1_to_v2_plan() -> Result<TraceV1ToV2Plan> {
    let v2_schema = SCHEMA_DEFINITIONS.resolve_trace_schema("physical-v4")?;
    build_trace_v1_to_v2_plan_for(&v2_schema)
}

/// Selection logic mirrors what was previously an inline per-batch match
/// in `transform_trace_v1_to_v2` -- moved here so it runs once, not once
/// per batch. Returns `Err` (never panics) on a field with no matching
/// rule, so a bad rule reference fails at plan construction, not
/// mid-ingest. Split out from [`build_trace_v1_to_v2_plan`] so tests can
/// exercise selection against a fabricated schema, not just the real one.
fn build_trace_v1_to_v2_plan_for(v2_schema: &ResolvedSchema) -> Result<TraceV1ToV2Plan> {
    let schema = create_arrow_schema_from_resolved(v2_schema)?;

    let mut extractors = Vec::with_capacity(v2_schema.fields.len());
    for field in &v2_schema.fields {
        let extractor: Extractor = match field.name.as_str() {
            // Direct mappings (same name in v1 and v2)
            "trace_id" | "span_id" | "parent_span_id" | "service_name" | "span_kind"
            | "status_code" | "status_message" | "is_root" => direct_extractor(&field.name),

            // #1208 columns: nullable, and the column itself may be absent
            // entirely from the incoming v1 batch -- a v1 producer older
            // than this column (an acceptor mid-rolling-upgrade, or a test
            // fixture building a wire batch by hand) must not be rejected
            // wholesale for it. Missing means "all null", same as any row
            // whose value happens to be null.
            "span_kind_number" | "status_code_number" => nullable_int32_extractor(&field.name),
            "dropped_attributes_count" | "dropped_events_count" | "dropped_links_count" => {
                nullable_int64_extractor(&field.name)
            }

            // UInt64 fields that need to be converted to Int64 for Iceberg compatibility
            "start_time_unix_nano" | "end_time_unix_nano" => {
                cast_uint64_to_int64_extractor(&field.name)
            }

            // Complex types converted to JSON strings
            "events" => serialize_list_extractor("events"),
            "links" => serialize_list_extractor("links"),

            // Renamed fields
            "span_name" => direct_extractor("name"),
            "duration_nanos" => cast_uint64_to_int64_extractor("duration_nano"),
            "span_attributes" => direct_extractor("attributes_json"),
            "resource_attributes" => direct_extractor("resource_json"),

            // Computed fields
            "timestamp" => timestamp_from_start_time_extractor(),
            "date_day" => date_day_from_start_time_extractor(),
            "hour" => hour_from_start_time_extractor(),

            // #1340: digest of the resource's attribute set, derived from
            // the same `resource_json` column `resource_attributes` reads.
            "resource_identity" => Box::new(resource_identity_from_resource_json_column),

            // Scope and resource metadata fields - present in v1 schema
            "trace_state"
            | "resource_schema_url"
            | "scope_name"
            | "scope_version"
            | "scope_schema_url"
            | "scope_attributes" => direct_extractor(&field.name),

            other => return Err(anyhow!("Unknown field in v2 schema: {other}")),
        };
        extractors.push(extractor);
    }

    Ok(TraceV1ToV2Plan { schema, extractors })
}

static TRACE_V1_TO_V2_PLAN: std::sync::OnceLock<TraceV1ToV2Plan> = std::sync::OnceLock::new();

/// Eagerly builds and caches the trace v1->v2 materialization plan. Called
/// once at writer startup (`IcebergTableWriter::new`, scoped to the traces
/// table) so a bad extraction-rule reference fails the process before it
/// serves traffic, rather than on the first ingested batch under
/// `panic = "abort"`.
pub fn warm_trace_v1_to_v2_plan() -> Result<()> {
    trace_v1_to_v2_plan().map(|_| ())
}

fn trace_v1_to_v2_plan() -> Result<&'static TraceV1ToV2Plan> {
    if let Some(plan) = TRACE_V1_TO_V2_PLAN.get() {
        return Ok(plan);
    }
    let plan = build_trace_v1_to_v2_plan()?;
    let _ = TRACE_V1_TO_V2_PLAN.set(plan);
    Ok(TRACE_V1_TO_V2_PLAN
        .get()
        .expect("just initialized above, or by a racing thread's successful set()"))
}

pub fn transform_trace_v1_to_v2(batch: RecordBatch, labels: &[String]) -> Result<RecordBatch> {
    let plan = trace_v1_to_v2_plan()?;

    let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(plan.extractors.len());
    for extractor in &plan.extractors {
        new_columns.push(extractor(&batch)?);
    }

    // Promote configured attribute keys into `label_<key>` columns (dropped
    // by coercion for tables that predate them).
    let (label_fields, label_columns) =
        materialized_label_columns(&batch, batch.num_rows(), labels)?;
    let out_schema = extend_schema_with_labels(
        plan.schema.clone(),
        label_fields,
        &mut new_columns,
        label_columns,
    );

    let result = RecordBatch::try_new(out_schema, new_columns)
        .map_err(|e| anyhow!("Failed to create transformed RecordBatch: {}", e))?;

    Ok(result)
}

/// Get column by name from RecordBatch
fn get_column_by_name(batch: &RecordBatch, name: &str) -> Result<ArrayRef> {
    batch
        .schema()
        .column_with_name(name)
        .map(|(idx, _)| batch.column(idx).clone())
        .ok_or_else(|| anyhow!("Column '{}' not found in batch", name))
}

/// Like [`get_column_by_name`], but a wholly absent column reads as
/// all-null rather than an error -- for columns added to the wire schema
/// after older producers exist (a v1 acceptor mid-rolling-upgrade, or a
/// hand-built test fixture) may still send batches without them.
fn get_column_by_name_or_null(
    batch: &RecordBatch,
    name: &str,
    data_type: &DataType,
) -> Result<ArrayRef> {
    match batch.schema().column_with_name(name) {
        Some((idx, _)) => Ok(batch.column(idx).clone()),
        None => Ok(datafusion::arrow::array::new_null_array(
            data_type,
            batch.num_rows(),
        )),
    }
}

/// Builds the `resource_identity` column from a batch's `resource_json`
/// string column: `resource_identity_from_json` of each row, null when the
/// source row is null or its JSON is not an object. Shared by every
/// transform (traces, logs, and metrics/profiles) that derives
/// `resource_identity` from a `resource_json` column.
fn resource_identity_from_resource_json_column(batch: &RecordBatch) -> Result<ArrayRef> {
    let col = get_column_by_name(batch, "resource_json")?;
    let str_array = col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow!("resource_json is not StringArray"))?;
    let values: Vec<Option<String>> = (0..str_array.len())
        .map(|i| {
            if str_array.is_null(i) {
                None
            } else {
                resource_identity_from_json(str_array.value(i))
            }
        })
        .collect();
    Ok(Arc::new(StringArray::from(values)))
}

/// Serialize a ListArray (containing StructArrays) to JSON string arrays
/// Each row's list of structs becomes a JSON array string like '[{"name":"event1",...},...]'
fn serialize_list_array_to_json_strings(
    col: &ArrayRef,
    num_rows: usize,
    field_name: &str,
) -> Result<ArrayRef> {
    let list_array = col
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| anyhow!("{field_name} is not a ListArray"))?;

    let mut json_strings: Vec<Option<String>> = Vec::with_capacity(num_rows);

    for row in 0..num_rows {
        if list_array.is_null(row) {
            json_strings.push(Some("[]".to_string()));
            continue;
        }

        let list_values = list_array.value(row);
        let struct_array = list_values
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| anyhow!("{field_name} list items are not StructArray"))?;

        let mut items = Vec::new();
        for i in 0..struct_array.len() {
            let mut obj = serde_json::Map::new();
            for (col_idx, field) in struct_array.fields().iter().enumerate() {
                let col_array = struct_array.column(col_idx);
                let value = if col_array.is_null(i) {
                    serde_json::Value::Null
                } else if let Some(str_arr) = col_array.as_any().downcast_ref::<StringArray>() {
                    serde_json::Value::String(str_arr.value(i).to_string())
                } else if let Some(uint_arr) = col_array.as_any().downcast_ref::<UInt64Array>() {
                    serde_json::Value::Number(uint_arr.value(i).into())
                } else {
                    serde_json::Value::Null
                };
                obj.insert(field.name().clone(), value);
            }
            items.push(serde_json::Value::Object(obj));
        }

        json_strings.push(Some(
            serde_json::to_string(&items).unwrap_or_else(|_| "[]".to_string()),
        ));
    }

    Ok(Arc::new(StringArray::from(json_strings)))
}

/// Create Arrow schema from resolved schema
fn create_arrow_schema_from_resolved(resolved: &ResolvedSchema) -> Result<Arc<Schema>> {
    let mut fields = Vec::new();

    for field in &resolved.fields {
        let data_type = match field.field_type.as_str() {
            "string" => DataType::Utf8,
            "int32" => DataType::Int32,
            "int64" => DataType::Int64,
            "uint64" => DataType::Int64, // Map uint64 to Int64 for Iceberg compatibility
            "double" => DataType::Float64,
            "boolean" => DataType::Boolean,
            "timestamp_ns" => {
                DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Nanosecond, None)
            }
            "date" => DataType::Date32,
            // The transform emits attribute maps as JSON strings; the
            // writer's schema coercion converts them to MapArray for
            // tables whose stored schema declares a map.
            "map<string,string>" => DataType::Utf8,
            "list<struct>" => {
                // For now, treat as string (will be handled properly later)
                DataType::Utf8
            }
            // Element field name/nullability ("item"/nullable) mirrors
            // iceberg-rust-spec's Iceberg->Arrow list conversion
            // (iceberg-rust-spec/src/arrow/schema.rs), so batches built here
            // stay schema-compatible with a table created from this type.
            "list<int64>" => DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            "list<double>" => DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
            _ => return Err(anyhow!("Unsupported field type: {}", field.field_type)),
        };

        fields.push(Field::new(&field.name, data_type, !field.required));
    }

    Ok(Arc::new(Schema::new(fields)))
}

/// Render a JSON attribute value as the string stored in a materialized
/// column (bare for scalars, so it matches how the querier compares).
fn attr_value_to_string(v: &serde_json::Value) -> Option<String> {
    match v {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Null => None,
        other => Some(other.to_string()),
    }
}

/// Parse a JSON-string column into per-row objects, optionally descending
/// into a nested key (e.g. scope's `attributes`).
fn parse_attr_objects(
    batch: &RecordBatch,
    column: &str,
    nested_key: Option<&str>,
) -> Result<Vec<Option<serde_json::Map<String, serde_json::Value>>>> {
    let col = get_column_by_name(batch, column)?;
    let arr = col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow!("{column} is not StringArray"))?;
    Ok((0..arr.len())
        .map(|i| {
            if arr.is_null(i) {
                return None;
            }
            let root: serde_json::Value = serde_json::from_str(arr.value(i)).ok()?;
            let obj = match nested_key {
                Some(k) => root.get(k)?.clone(),
                None => root,
            };
            match obj {
                serde_json::Value::Object(m) => Some(m),
                _ => None,
            }
        })
        .collect())
}

/// One row's parsed attribute object (or `None` when absent / not an object).
type AttrMap = serde_json::Map<String, serde_json::Value>;

/// Build the `label_<key>` columns for the configured materialized labels
/// from a batch's `resource_json` / `scope_json` / `attributes_json`
/// columns. Empty when nothing is configured.
fn materialized_label_columns(
    batch: &RecordBatch,
    num_rows: usize,
    labels: &[String],
) -> Result<(Vec<Field>, Vec<ArrayRef>)> {
    if labels.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }

    // Attribute sources, in precedence order (resource → scope → record).
    let resource = parse_attr_objects(batch, "resource_json", None)?;
    let scope = parse_attr_objects(batch, "scope_json", Some("attributes"))?;
    let record = parse_attr_objects(batch, "attributes_json", None)?;

    Ok(label_columns_from_maps(
        &resource, &scope, &record, num_rows, labels,
    ))
}

/// Build materialized-label columns from already-serialized attribute JSON
/// strings (the metrics transforms explode data points and hold per-row
/// resource/scope/record attribute JSON rather than a batch column).
fn materialized_label_columns_from_json(
    resource: &[Option<String>],
    scope: &[Option<String>],
    record: &[Option<String>],
    labels: &[String],
) -> (Vec<Field>, Vec<ArrayRef>) {
    if labels.is_empty() {
        return (Vec::new(), Vec::new());
    }
    let parse = |rows: &[Option<String>]| -> Vec<Option<AttrMap>> {
        rows.iter()
            .map(|s| {
                s.as_ref()
                    .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
                    .and_then(|v| match v {
                        serde_json::Value::Object(m) => Some(m),
                        _ => None,
                    })
            })
            .collect()
    };
    let (r, sc, rec) = (parse(resource), parse(scope), parse(record));
    label_columns_from_maps(&r, &sc, &rec, resource.len(), labels)
}

/// Build one `label_<key>` column per label from per-row attribute maps,
/// taking each value from resource, then scope, then record (first
/// non-null). An exact-duplicate label collapses to a single column; two
/// distinct labels that sanitize to the same candidate name (#1448) get
/// distinct columns via
/// [`resolve_label_columns_fresh`](common::iceberg::evolution::resolve_label_columns_fresh),
/// the same canonical-order resolution the table-creation path
/// (`ResolvedSchema::build_iceberg_schema`) uses, so both agree on the
/// column for a given configured list regardless of the order its keys
/// happen to be iterated in.
///
/// This ingest path resolves before a batch is even written to WAL,
/// deliberately decoupled from any catalog round trip (see
/// `flight_iceberg.rs`'s module doc), so unlike
/// `common::iceberg::evolution::add_label_columns` it cannot consult a
/// table's actual committed schema -- see `resolve_label_columns_fresh`'s
/// doc for what that leaves unprotected.
fn label_columns_from_maps(
    resource: &[Option<AttrMap>],
    scope: &[Option<AttrMap>],
    record: &[Option<AttrMap>],
    num_rows: usize,
    labels: &[String],
) -> (Vec<Field>, Vec<ArrayRef>) {
    let mut fields = Vec::new();
    let mut columns: Vec<ArrayRef> = Vec::new();
    for (label, name) in common::iceberg::evolution::resolve_label_columns_fresh(labels) {
        let values: Vec<Option<String>> = (0..num_rows)
            .map(|i| {
                for src in [resource.get(i), scope.get(i), record.get(i)] {
                    if let Some(Some(map)) = src
                        && let Some(v) = map.get(&label)
                        && let Some(s) = attr_value_to_string(v)
                    {
                        return Some(s);
                    }
                }
                None
            })
            .collect();
        let field = Field::new(&name, DataType::Utf8, true).with_metadata(HashMap::from([(
            LABEL_ORIGIN_KEY_METADATA.to_string(),
            label,
        )]));
        fields.push(field);
        columns.push(Arc::new(StringArray::from(values)));
    }
    (fields, columns)
}

/// Append materialized-label fields/columns to a base batch schema. Returns
/// the base schema unchanged when there are no label fields.
fn extend_schema_with_labels(
    base: Arc<Schema>,
    label_fields: Vec<Field>,
    columns: &mut Vec<ArrayRef>,
    label_columns: Vec<ArrayRef>,
) -> Arc<Schema> {
    if label_fields.is_empty() {
        return base;
    }
    let mut fields: Vec<Field> = base.fields().iter().map(|f| (**f).clone()).collect();
    fields.extend(label_fields);
    columns.extend(label_columns);
    Arc::new(Schema::new(fields))
}

pub fn transform_logs_v1_to_iceberg(batch: RecordBatch, labels: &[String]) -> Result<RecordBatch> {
    let v1_schema = SCHEMA_DEFINITIONS.resolve_log_schema("physical-v3")?;
    let arrow_schema = create_arrow_schema_from_resolved(&v1_schema)?;

    let num_rows = batch.num_rows();
    let mut new_columns: Vec<ArrayRef> = Vec::new();

    // Pre-extract both timestamp columns so we can fall back from
    // time_unix_nano to observed_time_unix_nano per the OTLP spec:
    // "If time_unix_nano is 0, receivers SHOULD use observed_time_unix_nano."
    let time_col = get_column_by_name(&batch, "time_unix_nano")?;
    let time_array = time_col
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| anyhow!("time_unix_nano is not UInt64Array"))?;
    let observed_col = get_column_by_name(&batch, "observed_time_unix_nano")?;
    let observed_array = observed_col
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| anyhow!("observed_time_unix_nano is not UInt64Array"))?;

    let effective_nanos: Vec<u64> = (0..time_array.len())
        .map(|i| {
            let t = if time_array.is_null(i) {
                0
            } else {
                time_array.value(i)
            };
            if t != 0 {
                return t;
            }
            if observed_array.is_null(i) {
                0
            } else {
                observed_array.value(i)
            }
        })
        .collect();

    for field in &v1_schema.fields {
        let column: ArrayRef = match field.name.as_str() {
            "timestamp" => {
                let values: Vec<Option<i64>> = effective_nanos
                    .iter()
                    .map(|&nanos| Some(nanos as i64))
                    .collect();
                Arc::new(TimestampNanosecondArray::from(values))
            }
            "observed_timestamp" => {
                let col = get_column_by_name(&batch, "observed_time_unix_nano")?;
                let uint_array = col
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| anyhow!("observed_time_unix_nano is not UInt64Array"))?;
                let values: Vec<Option<i64>> = (0..uint_array.len())
                    .map(|i| {
                        if uint_array.is_null(i) {
                            None
                        } else {
                            Some(uint_array.value(i) as i64)
                        }
                    })
                    .collect();
                Arc::new(TimestampNanosecondArray::from(values))
            }
            "trace_id" => {
                let col = get_column_by_name(&batch, "trace_id")?;
                let bin_array = col
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| anyhow!("trace_id is not BinaryArray"))?;
                let values: Vec<Option<String>> = (0..bin_array.len())
                    .map(|i| {
                        if bin_array.is_null(i) {
                            None
                        } else {
                            let bytes = bin_array.value(i);
                            if bytes.is_empty() {
                                None
                            } else {
                                Some(hex::encode(bytes))
                            }
                        }
                    })
                    .collect();
                Arc::new(StringArray::from(values))
            }
            "span_id" => {
                let col = get_column_by_name(&batch, "span_id")?;
                let bin_array = col
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| anyhow!("span_id is not BinaryArray"))?;
                let values: Vec<Option<String>> = (0..bin_array.len())
                    .map(|i| {
                        if bin_array.is_null(i) {
                            None
                        } else {
                            let bytes = bin_array.value(i);
                            if bytes.is_empty() {
                                None
                            } else {
                                Some(hex::encode(bytes))
                            }
                        }
                    })
                    .collect();
                Arc::new(StringArray::from(values))
            }
            "trace_flags" => {
                let col = get_column_by_name(&batch, "flags")?;
                let uint_array = col
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .ok_or_else(|| anyhow!("flags is not UInt32Array"))?;
                let values: Vec<Option<i32>> = (0..uint_array.len())
                    .map(|i| {
                        if uint_array.is_null(i) {
                            None
                        } else {
                            Some(uint_array.value(i) as i32)
                        }
                    })
                    .collect();
                Arc::new(Int32Array::from(values))
            }
            "severity_text" | "severity_number" | "service_name" | "body" => {
                get_column_by_name(&batch, &field.name)?
            }
            "resource_schema_url" => {
                let col = get_column_by_name(&batch, "resource_json")?;
                let str_array = col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| anyhow!("resource_json is not StringArray"))?;
                let values: Vec<Option<String>> = (0..str_array.len())
                    .map(|i| {
                        if str_array.is_null(i) {
                            return None;
                        }
                        let json_str = str_array.value(i);
                        serde_json::from_str::<serde_json::Value>(json_str)
                            .ok()
                            .and_then(|v| {
                                v.get("schema_url")
                                    .and_then(|s| s.as_str())
                                    .map(String::from)
                            })
                    })
                    .collect();
                Arc::new(StringArray::from(values))
            }
            "resource_attributes" => {
                let col = get_column_by_name(&batch, "resource_json")?;
                let str_array = col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| anyhow!("resource_json is not StringArray"))?;
                let values: Vec<Option<String>> = (0..str_array.len())
                    .map(|i| {
                        if str_array.is_null(i) {
                            return None;
                        }
                        let json_str = str_array.value(i);
                        match serde_json::from_str::<serde_json::Value>(json_str) {
                            Ok(v) => {
                                if let Some(attrs) = v.get("attributes") {
                                    Some(attrs.to_string())
                                } else {
                                    Some(json_str.to_string())
                                }
                            }
                            Err(_) => Some(json_str.to_string()),
                        }
                    })
                    .collect();
                Arc::new(StringArray::from(values))
            }
            "scope_schema_url" => {
                let col = get_column_by_name(&batch, "scope_json")?;
                let str_array = col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| anyhow!("scope_json is not StringArray"))?;
                let values: Vec<Option<String>> = (0..str_array.len())
                    .map(|i| {
                        if str_array.is_null(i) {
                            return None;
                        }
                        serde_json::from_str::<serde_json::Value>(str_array.value(i))
                            .ok()
                            .and_then(|v| {
                                v.get("schema_url")
                                    .and_then(|s| s.as_str())
                                    .map(String::from)
                            })
                    })
                    .collect();
                Arc::new(StringArray::from(values))
            }
            "scope_name" => {
                let col = get_column_by_name(&batch, "scope_json")?;
                let str_array = col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| anyhow!("scope_json is not StringArray"))?;
                let values: Vec<Option<String>> = (0..str_array.len())
                    .map(|i| {
                        if str_array.is_null(i) {
                            return None;
                        }
                        serde_json::from_str::<serde_json::Value>(str_array.value(i))
                            .ok()
                            .and_then(|v| v.get("name").and_then(|s| s.as_str()).map(String::from))
                    })
                    .collect();
                Arc::new(StringArray::from(values))
            }
            "scope_version" => {
                let col = get_column_by_name(&batch, "scope_json")?;
                let str_array = col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| anyhow!("scope_json is not StringArray"))?;
                let values: Vec<Option<String>> = (0..str_array.len())
                    .map(|i| {
                        if str_array.is_null(i) {
                            return None;
                        }
                        serde_json::from_str::<serde_json::Value>(str_array.value(i))
                            .ok()
                            .and_then(|v| {
                                v.get("version").and_then(|s| s.as_str()).map(String::from)
                            })
                    })
                    .collect();
                Arc::new(StringArray::from(values))
            }
            "scope_attributes" => {
                let col = get_column_by_name(&batch, "scope_json")?;
                let str_array = col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| anyhow!("scope_json is not StringArray"))?;
                let values: Vec<Option<String>> = (0..str_array.len())
                    .map(|i| {
                        if str_array.is_null(i) {
                            return None;
                        }
                        serde_json::from_str::<serde_json::Value>(str_array.value(i))
                            .ok()
                            .and_then(|v| v.get("attributes").map(|a| a.to_string()))
                    })
                    .collect();
                Arc::new(StringArray::from(values))
            }
            "log_attributes" => get_column_by_name(&batch, "attributes_json")?,
            "resource_identity" => resource_identity_from_resource_json_column(&batch)?,
            "event_name" => get_column_by_name_or_null(&batch, "event_name", &DataType::Utf8)?,
            "dropped_attributes_count" => {
                let col = get_column_by_name_or_null(
                    &batch,
                    "dropped_attributes_count",
                    &DataType::UInt32,
                )?;
                let uint_array = col
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .ok_or_else(|| anyhow!("dropped_attributes_count is not UInt32Array"))?;
                Arc::new(
                    uint_array
                        .iter()
                        .map(|v| v.map(i64::from))
                        .collect::<Int64Array>(),
                )
            }
            "date_day" => {
                let dates: Vec<Option<i32>> = effective_nanos
                    .iter()
                    .map(|&nanos| {
                        let secs = (nanos / 1_000_000_000) as i64;
                        let dt = DateTime::from_timestamp(secs, 0)?;
                        Some(dt.naive_utc().date().num_days_from_ce() - 719163)
                    })
                    .collect();
                Arc::new(Date32Array::from(dates))
            }
            "hour" => {
                let hours: Vec<Option<i32>> = effective_nanos
                    .iter()
                    .map(|&nanos| {
                        let secs = (nanos / 1_000_000_000) as i64;
                        let dt = DateTime::from_timestamp(secs, 0)?;
                        Some(dt.hour() as i32)
                    })
                    .collect();
                Arc::new(Int32Array::from(hours))
            }
            _ => return Err(anyhow!("Unknown field in logs schema: {}", field.name)),
        };
        new_columns.push(column);
    }

    // Promote configured attribute keys into dedicated `label_<key>`
    // columns. `coerce_batch_to_schema` later drops these for tables that
    // predate the columns, so this is safe regardless of table age.
    let (label_fields, label_columns) = materialized_label_columns(&batch, num_rows, labels)?;
    let out_schema =
        extend_schema_with_labels(arrow_schema, label_fields, &mut new_columns, label_columns);

    let result = RecordBatch::try_new(out_schema, new_columns)
        .map_err(|e| anyhow!("Failed to create transformed log RecordBatch: {}", e))?;

    tracing::debug!(
        "Log transformation complete: {} input rows -> {} output columns",
        num_rows,
        result.num_columns()
    );

    Ok(result)
}

#[derive(Clone, Default)]
struct ResourceContext {
    service_name: Option<String>,
    resource_schema_url: Option<String>,
    resource_attributes: Option<String>,
    resource_identity: Option<String>,
}

#[derive(Clone, Default)]
struct ScopeContext {
    scope_name: Option<String>,
    scope_version: Option<String>,
    scope_schema_url: Option<String>,
    scope_attributes: Option<String>,
    scope_dropped_attr_count: i32,
}

pub fn create_metrics_gauge_arrow_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(
            "start_timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("metric_description", DataType::Utf8, true),
        Field::new("metric_unit", DataType::Utf8, true),
        Field::new("value", DataType::Float64, false),
        Field::new("flags", DataType::Int32, true),
        Field::new("resource_schema_url", DataType::Utf8, true),
        Field::new("resource_attributes", DataType::Utf8, true),
        Field::new("scope_name", DataType::Utf8, true),
        Field::new("scope_version", DataType::Utf8, true),
        Field::new("scope_schema_url", DataType::Utf8, true),
        Field::new("scope_attributes", DataType::Utf8, true),
        Field::new("scope_dropped_attr_count", DataType::Int32, true),
        Field::new("attributes", DataType::Utf8, true),
        Field::new("exemplars", DataType::Utf8, true),
        Field::new("date_day", DataType::Date32, false),
        Field::new("hour", DataType::Int32, false),
        Field::new("resource_identity", DataType::Utf8, true),
    ]))
}

// No longer called by a transform (metrics now flow through
// `transform_metrics_to_wide`); kept `pub` rather than deleted because
// `schema_consistency` below still pins it against `schemas.toml`.
pub fn create_metrics_sum_arrow_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(
            "start_timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("metric_description", DataType::Utf8, true),
        Field::new("metric_unit", DataType::Utf8, true),
        Field::new("value", DataType::Float64, false),
        Field::new("flags", DataType::Int32, true),
        Field::new("aggregation_temporality", DataType::Int32, false),
        Field::new("is_monotonic", DataType::Boolean, false),
        Field::new("resource_schema_url", DataType::Utf8, true),
        Field::new("resource_attributes", DataType::Utf8, true),
        Field::new("scope_name", DataType::Utf8, true),
        Field::new("scope_version", DataType::Utf8, true),
        Field::new("scope_schema_url", DataType::Utf8, true),
        Field::new("scope_attributes", DataType::Utf8, true),
        Field::new("scope_dropped_attr_count", DataType::Int32, true),
        Field::new("attributes", DataType::Utf8, true),
        Field::new("exemplars", DataType::Utf8, true),
        Field::new("date_day", DataType::Date32, false),
        Field::new("hour", DataType::Int32, false),
        Field::new("resource_identity", DataType::Utf8, true),
    ]))
}

fn create_metrics_histogram_arrow_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(
            "start_timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("metric_description", DataType::Utf8, true),
        Field::new("metric_unit", DataType::Utf8, true),
        Field::new("count", DataType::Int64, false),
        Field::new("sum", DataType::Float64, true),
        Field::new("min", DataType::Float64, true),
        Field::new("max", DataType::Float64, true),
        Field::new("bucket_counts", DataType::Utf8, true),
        Field::new("explicit_bounds", DataType::Utf8, true),
        Field::new("flags", DataType::Int32, true),
        Field::new("aggregation_temporality", DataType::Int32, false),
        Field::new("resource_schema_url", DataType::Utf8, true),
        Field::new("resource_attributes", DataType::Utf8, true),
        Field::new("scope_name", DataType::Utf8, true),
        Field::new("scope_version", DataType::Utf8, true),
        Field::new("scope_schema_url", DataType::Utf8, true),
        Field::new("scope_attributes", DataType::Utf8, true),
        Field::new("scope_dropped_attr_count", DataType::Int32, true),
        Field::new("attributes", DataType::Utf8, true),
        Field::new("exemplars", DataType::Utf8, true),
        Field::new("date_day", DataType::Date32, false),
        Field::new("hour", DataType::Int32, false),
        Field::new("resource_identity", DataType::Utf8, true),
    ]))
}

fn get_typed_column<'a, T>(batch: &'a RecordBatch, name: &str) -> Result<&'a T>
where
    T: Array + 'static,
{
    let (idx, _) = batch
        .schema()
        .column_with_name(name)
        .ok_or_else(|| anyhow!("Column '{}' not found in batch", name))?;

    batch
        .column(idx)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| anyhow!("Column '{}' has unexpected type", name))
}

fn string_value_ref(array: &StringArray, row: usize) -> Option<&str> {
    if array.is_null(row) {
        None
    } else {
        Some(array.value(row))
    }
}

fn string_value(array: &StringArray, row: usize) -> Option<String> {
    string_value_ref(array, row).map(ToString::to_string)
}

fn int32_value(array: &Int32Array, row: usize) -> Option<i32> {
    if array.is_null(row) {
        None
    } else {
        Some(array.value(row))
    }
}

fn bool_value(array: &BooleanArray, row: usize) -> Option<bool> {
    if array.is_null(row) {
        None
    } else {
        Some(array.value(row))
    }
}

fn parse_data_points(data_json: Option<&str>) -> Vec<serde_json::Value> {
    let Some(data_json) = data_json else {
        return Vec::new();
    };

    match serde_json::from_str::<serde_json::Value>(data_json) {
        Ok(serde_json::Value::Array(points)) => points,
        _ => Vec::new(),
    }
}

fn json_to_u64(value: Option<&serde_json::Value>) -> Option<u64> {
    value.and_then(|v| {
        v.as_u64()
            .or_else(|| v.as_i64().and_then(|n| u64::try_from(n).ok()))
    })
}

fn json_to_i64(value: Option<&serde_json::Value>) -> Option<i64> {
    value.and_then(|v| {
        v.as_i64()
            .or_else(|| v.as_u64().and_then(|n| i64::try_from(n).ok()))
    })
}

fn json_to_i32(value: Option<&serde_json::Value>) -> Option<i32> {
    value.and_then(|v| {
        v.as_i64()
            .and_then(|n| i32::try_from(n).ok())
            .or_else(|| v.as_u64().and_then(|n| i32::try_from(n).ok()))
    })
}

/// Numeric field of a v1 data point. Understands the `"NaN"` / `"+Inf"` /
/// `"-Inf"` sentinels the acceptor writes for non-finite doubles
/// (`common::flight::conversion::f64_to_json`), which JSON cannot carry as
/// numbers.
fn json_to_f64(value: Option<&serde_json::Value>) -> Option<f64> {
    value.and_then(common::flight::conversion::json_to_f64)
}

/// The `value` column of `metrics_gauge` / `metrics_sum` is non-nullable. A
/// point with no value (absent or `null`) is stored as NaN — the honest "no
/// number here" that Float64 can express — instead of leaving a null that
/// makes the whole batch fail `RecordBatch::try_new` and pins the WAL entry
/// forever (#1061).
fn point_value(point: &serde_json::Value) -> f64 {
    json_to_f64(point.get("value")).unwrap_or(f64::NAN)
}

fn serialize_json(value: Option<&serde_json::Value>) -> Option<String> {
    value.and_then(|v| {
        if v.is_null() {
            None
        } else {
            serde_json::to_string(v).ok()
        }
    })
}

fn serialize_json_array(value: Option<&serde_json::Value>) -> Option<String> {
    value.and_then(|v| {
        if v.is_array() {
            serde_json::to_string(v).ok()
        } else {
            None
        }
    })
}

fn temporal_from_nanos(nanos: Option<u64>) -> (Option<i64>, Option<i32>, Option<i32>) {
    let Some(nanos) = nanos else {
        return (None, None, None);
    };

    let nanos_i64 = i64::try_from(nanos).ok();
    let secs = i64::try_from(nanos / 1_000_000_000).ok();

    let Some(secs) = secs else {
        return (nanos_i64, None, None);
    };

    let Some(dt) = DateTime::from_timestamp(secs, 0) else {
        return (nanos_i64, None, None);
    };

    (
        nanos_i64,
        Some(dt.naive_utc().date().num_days_from_ce() - 719163),
        Some(dt.hour() as i32),
    )
}

fn extract_service_name(
    resource_obj: &serde_json::Map<String, serde_json::Value>,
) -> Option<String> {
    if let Some(service_name) = resource_obj
        .get("service.name")
        .and_then(|value| value.as_str())
    {
        return Some(service_name.to_string());
    }

    resource_obj
        .get("attributes")
        .and_then(|value| value.as_object())
        .and_then(|attributes| attributes.get("service.name"))
        .and_then(|value| value.as_str())
        .map(ToString::to_string)
}

fn extract_resource_context(resource_json: Option<&str>) -> ResourceContext {
    let unknown = || Some(UNKNOWN_SERVICE_NAME.to_string());
    let Some(resource_json) = resource_json else {
        return ResourceContext {
            service_name: unknown(),
            ..ResourceContext::default()
        };
    };

    let Ok(parsed) = serde_json::from_str::<serde_json::Value>(resource_json) else {
        return ResourceContext {
            service_name: unknown(),
            resource_schema_url: None,
            resource_attributes: Some(resource_json.to_string()),
            resource_identity: None,
        };
    };

    let serde_json::Value::Object(obj) = parsed else {
        return ResourceContext {
            service_name: unknown(),
            resource_schema_url: None,
            resource_attributes: Some(resource_json.to_string()),
            resource_identity: None,
        };
    };

    let resource_attributes = if obj.get("attributes").is_some() {
        serialize_json(obj.get("attributes"))
    } else {
        Some(resource_json.to_string())
    };

    ResourceContext {
        // Never `None`: the Iceberg `service_name` column is non-nullable and
        // a missing `service.name` must not dead-letter the batch.
        service_name: extract_service_name(&obj).or_else(unknown),
        resource_schema_url: obj
            .get("schema_url")
            .and_then(|value| value.as_str())
            .map(ToString::to_string),
        resource_attributes,
        // Delegates to the shared envelope classifier rather than
        // re-deriving it here: an `attributes` key alone doesn't mean
        // envelope-shaped (a flat resource may legitimately carry its own
        // `attributes` attribute), and duplicating that disambiguation
        // risked drifting from `resource_identity_from_json`'s rule -- as it
        // already had, once that rule went from "has an `attributes` key" to
        // "every key is an envelope key" (see its doc comment).
        resource_identity: resource_identity_from_json(resource_json),
    }
}

fn extract_scope_context(scope_json: Option<&str>) -> ScopeContext {
    let Some(scope_json) = scope_json else {
        return ScopeContext {
            scope_dropped_attr_count: 0,
            ..ScopeContext::default()
        };
    };

    let Ok(parsed) = serde_json::from_str::<serde_json::Value>(scope_json) else {
        return ScopeContext {
            scope_dropped_attr_count: 0,
            ..ScopeContext::default()
        };
    };

    let serde_json::Value::Object(obj) = parsed else {
        return ScopeContext {
            scope_dropped_attr_count: 0,
            ..ScopeContext::default()
        };
    };

    ScopeContext {
        scope_name: obj
            .get("name")
            .and_then(|value| value.as_str())
            .map(ToString::to_string),
        scope_version: obj
            .get("version")
            .and_then(|value| value.as_str())
            .map(ToString::to_string),
        scope_schema_url: obj
            .get("schema_url")
            .and_then(|value| value.as_str())
            .map(ToString::to_string),
        scope_attributes: serialize_json(obj.get("attributes")),
        scope_dropped_attr_count: json_to_i32(obj.get("dropped_attributes_count")).unwrap_or(0),
    }
}

pub fn transform_metrics_histogram_v1_to_iceberg(
    batch: RecordBatch,
    labels: &[String],
) -> Result<RecordBatch> {
    let output_schema = create_metrics_histogram_arrow_schema();

    let name_array = get_typed_column::<StringArray>(&batch, "name")?;
    let description_array = get_typed_column::<StringArray>(&batch, "description")?;
    let unit_array = get_typed_column::<StringArray>(&batch, "unit")?;
    let resource_json_array = get_typed_column::<StringArray>(&batch, "resource_json")?;
    let scope_json_array = get_typed_column::<StringArray>(&batch, "scope_json")?;
    let data_json_array = get_typed_column::<StringArray>(&batch, "data_json")?;
    let aggregation_temporality_array =
        get_typed_column::<Int32Array>(&batch, "aggregation_temporality")?;

    let mut timestamps: Vec<Option<i64>> = Vec::new();
    let mut start_timestamps: Vec<Option<i64>> = Vec::new();
    let mut service_names: Vec<Option<String>> = Vec::new();
    let mut metric_names: Vec<Option<String>> = Vec::new();
    let mut metric_descriptions: Vec<Option<String>> = Vec::new();
    let mut metric_units: Vec<Option<String>> = Vec::new();
    let mut counts: Vec<Option<i64>> = Vec::new();
    let mut sums: Vec<Option<f64>> = Vec::new();
    let mut mins: Vec<Option<f64>> = Vec::new();
    let mut maxes: Vec<Option<f64>> = Vec::new();
    let mut bucket_counts: Vec<Option<String>> = Vec::new();
    let mut explicit_bounds: Vec<Option<String>> = Vec::new();
    let mut flags: Vec<Option<i32>> = Vec::new();
    let mut aggregation_temporalities: Vec<Option<i32>> = Vec::new();
    let mut resource_schema_urls: Vec<Option<String>> = Vec::new();
    let mut resource_attributes: Vec<Option<String>> = Vec::new();
    let mut scope_names: Vec<Option<String>> = Vec::new();
    let mut scope_versions: Vec<Option<String>> = Vec::new();
    let mut scope_schema_urls: Vec<Option<String>> = Vec::new();
    let mut scope_attributes: Vec<Option<String>> = Vec::new();
    let mut scope_dropped_attr_counts: Vec<Option<i32>> = Vec::new();
    let mut attributes: Vec<Option<String>> = Vec::new();
    let mut exemplars: Vec<Option<String>> = Vec::new();
    let mut date_days: Vec<Option<i32>> = Vec::new();
    let mut hours: Vec<Option<i32>> = Vec::new();
    let mut resource_identities: Vec<Option<String>> = Vec::new();

    for row in 0..batch.num_rows() {
        let metric_name = string_value(name_array, row);
        let metric_description = string_value(description_array, row);
        let metric_unit = string_value(unit_array, row);
        let aggregation_temporality = int32_value(aggregation_temporality_array, row);

        let resource_context = extract_resource_context(string_value_ref(resource_json_array, row));
        let scope_context = extract_scope_context(string_value_ref(scope_json_array, row));
        let data_points = parse_data_points(string_value_ref(data_json_array, row));

        for point in data_points {
            let (timestamp, date_day, hour) =
                temporal_from_nanos(json_to_u64(point.get("time_unix_nano")));
            let (start_timestamp, _, _) =
                temporal_from_nanos(json_to_u64(point.get("start_time_unix_nano")));

            timestamps.push(timestamp);
            start_timestamps.push(start_timestamp);
            service_names.push(resource_context.service_name.clone());
            metric_names.push(metric_name.clone());
            metric_descriptions.push(metric_description.clone());
            metric_units.push(metric_unit.clone());
            counts.push(json_to_i64(point.get("count")));
            sums.push(json_to_f64(point.get("sum")));
            mins.push(json_to_f64(point.get("min")));
            maxes.push(json_to_f64(point.get("max")));
            bucket_counts.push(serialize_json_array(point.get("bucket_counts")));
            explicit_bounds.push(serialize_json_array(point.get("explicit_bounds")));
            flags.push(json_to_i32(point.get("flags")));
            aggregation_temporalities.push(aggregation_temporality);
            resource_schema_urls.push(resource_context.resource_schema_url.clone());
            resource_attributes.push(resource_context.resource_attributes.clone());
            scope_names.push(scope_context.scope_name.clone());
            scope_versions.push(scope_context.scope_version.clone());
            scope_schema_urls.push(scope_context.scope_schema_url.clone());
            scope_attributes.push(scope_context.scope_attributes.clone());
            scope_dropped_attr_counts.push(Some(scope_context.scope_dropped_attr_count));
            attributes.push(serialize_json(point.get("attributes")));
            exemplars.push(serialize_json(point.get("exemplars")));
            date_days.push(date_day);
            hours.push(hour);
            resource_identities.push(resource_context.resource_identity.clone());
        }
    }

    let (label_fields, label_columns) = materialized_label_columns_from_json(
        &resource_attributes,
        &scope_attributes,
        &attributes,
        labels,
    );
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(TimestampNanosecondArray::from(timestamps)),
        Arc::new(TimestampNanosecondArray::from(start_timestamps)),
        Arc::new(StringArray::from(service_names)),
        Arc::new(StringArray::from(metric_names)),
        Arc::new(StringArray::from(metric_descriptions)),
        Arc::new(StringArray::from(metric_units)),
        Arc::new(Int64Array::from(counts)),
        Arc::new(Float64Array::from(sums)),
        Arc::new(Float64Array::from(mins)),
        Arc::new(Float64Array::from(maxes)),
        Arc::new(StringArray::from(bucket_counts)),
        Arc::new(StringArray::from(explicit_bounds)),
        Arc::new(Int32Array::from(flags)),
        Arc::new(Int32Array::from(aggregation_temporalities)),
        Arc::new(StringArray::from(resource_schema_urls)),
        Arc::new(StringArray::from(resource_attributes)),
        Arc::new(StringArray::from(scope_names)),
        Arc::new(StringArray::from(scope_versions)),
        Arc::new(StringArray::from(scope_schema_urls)),
        Arc::new(StringArray::from(scope_attributes)),
        Arc::new(Int32Array::from(scope_dropped_attr_counts)),
        Arc::new(StringArray::from(attributes)),
        Arc::new(StringArray::from(exemplars)),
        Arc::new(Date32Array::from(date_days)),
        Arc::new(Int32Array::from(hours)),
        Arc::new(StringArray::from(resource_identities)),
    ];
    let out_schema =
        extend_schema_with_labels(output_schema, label_fields, &mut columns, label_columns);
    RecordBatch::try_new(out_schema, columns).map_err(|e| {
        anyhow!(
            "Failed to create transformed metrics_histogram RecordBatch: {}",
            e
        )
    })
}

fn create_metrics_exponential_histogram_arrow_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(
            "start_timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("metric_description", DataType::Utf8, true),
        Field::new("metric_unit", DataType::Utf8, true),
        Field::new("count", DataType::Int64, false),
        Field::new("sum", DataType::Float64, true),
        Field::new("min", DataType::Float64, true),
        Field::new("max", DataType::Float64, true),
        Field::new("scale", DataType::Int32, true),
        Field::new("zero_count", DataType::Int64, true),
        Field::new("positive_offset", DataType::Int32, true),
        Field::new("positive_bucket_counts", DataType::Utf8, true), // JSON array string
        Field::new("negative_offset", DataType::Int32, true),
        Field::new("negative_bucket_counts", DataType::Utf8, true), // JSON array string
        Field::new("flags", DataType::Int32, true),
        Field::new("aggregation_temporality", DataType::Int32, false),
        Field::new("zero_threshold", DataType::Float64, true),
        Field::new("resource_schema_url", DataType::Utf8, true),
        Field::new("resource_attributes", DataType::Utf8, true),
        Field::new("scope_name", DataType::Utf8, true),
        Field::new("scope_version", DataType::Utf8, true),
        Field::new("scope_schema_url", DataType::Utf8, true),
        Field::new("scope_attributes", DataType::Utf8, true),
        Field::new("scope_dropped_attr_count", DataType::Int32, true),
        Field::new("attributes", DataType::Utf8, true),
        Field::new("exemplars", DataType::Utf8, true),
        Field::new("date_day", DataType::Date32, false),
        Field::new("hour", DataType::Int32, false),
        Field::new("resource_identity", DataType::Utf8, true),
    ]))
}

fn create_metrics_summary_arrow_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(
            "start_timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("metric_description", DataType::Utf8, true),
        Field::new("metric_unit", DataType::Utf8, true),
        Field::new("count", DataType::Int64, false),
        Field::new("sum", DataType::Float64, false),
        Field::new("quantile_values", DataType::Utf8, true), // JSON array of {quantile, value}
        Field::new("flags", DataType::Int32, true),
        Field::new("resource_schema_url", DataType::Utf8, true),
        Field::new("resource_attributes", DataType::Utf8, true),
        Field::new("scope_name", DataType::Utf8, true),
        Field::new("scope_version", DataType::Utf8, true),
        Field::new("scope_schema_url", DataType::Utf8, true),
        Field::new("scope_attributes", DataType::Utf8, true),
        Field::new("scope_dropped_attr_count", DataType::Int32, true),
        Field::new("attributes", DataType::Utf8, true),
        Field::new("exemplars", DataType::Utf8, true),
        Field::new("date_day", DataType::Date32, false),
        Field::new("hour", DataType::Int32, false),
        Field::new("resource_identity", DataType::Utf8, true),
    ]))
}

pub fn transform_metrics_exponential_histogram_v1_to_iceberg(
    batch: RecordBatch,
    labels: &[String],
) -> Result<RecordBatch> {
    let output_schema = create_metrics_exponential_histogram_arrow_schema();

    let name_array = get_typed_column::<StringArray>(&batch, "name")?;
    let description_array = get_typed_column::<StringArray>(&batch, "description")?;
    let unit_array = get_typed_column::<StringArray>(&batch, "unit")?;
    let resource_json_array = get_typed_column::<StringArray>(&batch, "resource_json")?;
    let scope_json_array = get_typed_column::<StringArray>(&batch, "scope_json")?;
    let data_json_array = get_typed_column::<StringArray>(&batch, "data_json")?;
    let aggregation_temporality_array =
        get_typed_column::<Int32Array>(&batch, "aggregation_temporality")?;

    let mut timestamps: Vec<Option<i64>> = Vec::new();
    let mut start_timestamps: Vec<Option<i64>> = Vec::new();
    let mut service_names: Vec<Option<String>> = Vec::new();
    let mut metric_names: Vec<Option<String>> = Vec::new();
    let mut metric_descriptions: Vec<Option<String>> = Vec::new();
    let mut metric_units: Vec<Option<String>> = Vec::new();
    let mut counts: Vec<Option<i64>> = Vec::new();
    let mut sums: Vec<Option<f64>> = Vec::new();
    let mut mins: Vec<Option<f64>> = Vec::new();
    let mut maxes: Vec<Option<f64>> = Vec::new();
    let mut scales: Vec<Option<i32>> = Vec::new();
    let mut zero_counts: Vec<Option<i64>> = Vec::new();
    let mut positive_offsets: Vec<Option<i32>> = Vec::new();
    let mut positive_bucket_counts: Vec<Option<String>> = Vec::new();
    let mut negative_offsets: Vec<Option<i32>> = Vec::new();
    let mut negative_bucket_counts: Vec<Option<String>> = Vec::new();
    let mut flags: Vec<Option<i32>> = Vec::new();
    let mut aggregation_temporalities: Vec<Option<i32>> = Vec::new();
    let mut zero_thresholds: Vec<Option<f64>> = Vec::new();
    let mut resource_schema_urls: Vec<Option<String>> = Vec::new();
    let mut resource_attributes: Vec<Option<String>> = Vec::new();
    let mut scope_names: Vec<Option<String>> = Vec::new();
    let mut scope_versions: Vec<Option<String>> = Vec::new();
    let mut scope_schema_urls: Vec<Option<String>> = Vec::new();
    let mut scope_attributes: Vec<Option<String>> = Vec::new();
    let mut scope_dropped_attr_counts: Vec<Option<i32>> = Vec::new();
    let mut attributes: Vec<Option<String>> = Vec::new();
    let mut exemplars: Vec<Option<String>> = Vec::new();
    let mut date_days: Vec<Option<i32>> = Vec::new();
    let mut hours: Vec<Option<i32>> = Vec::new();
    let mut resource_identities: Vec<Option<String>> = Vec::new();

    for row in 0..batch.num_rows() {
        let metric_name = string_value(name_array, row);
        let metric_description = string_value(description_array, row);
        let metric_unit = string_value(unit_array, row);
        let aggregation_temporality = int32_value(aggregation_temporality_array, row);

        let resource_context = extract_resource_context(string_value_ref(resource_json_array, row));
        let scope_context = extract_scope_context(string_value_ref(scope_json_array, row));
        let data_points = parse_data_points(string_value_ref(data_json_array, row));

        for point in data_points {
            let (timestamp, date_day, hour) =
                temporal_from_nanos(json_to_u64(point.get("time_unix_nano")));
            let (start_timestamp, _, _) =
                temporal_from_nanos(json_to_u64(point.get("start_time_unix_nano")));

            timestamps.push(timestamp);
            start_timestamps.push(start_timestamp);
            service_names.push(resource_context.service_name.clone());
            metric_names.push(metric_name.clone());
            metric_descriptions.push(metric_description.clone());
            metric_units.push(metric_unit.clone());
            counts.push(json_to_i64(point.get("count")));
            sums.push(json_to_f64(point.get("sum")));
            mins.push(json_to_f64(point.get("min")));
            maxes.push(json_to_f64(point.get("max")));
            scales.push(json_to_i32(point.get("scale")));
            zero_counts.push(json_to_i64(point.get("zero_count")));

            let positive = point.get("positive");
            positive_offsets.push(positive.and_then(|p| json_to_i32(p.get("offset"))));
            positive_bucket_counts
                .push(positive.and_then(|p| serialize_json_array(p.get("bucket_counts"))));

            let negative = point.get("negative");
            negative_offsets.push(negative.and_then(|p| json_to_i32(p.get("offset"))));
            negative_bucket_counts
                .push(negative.and_then(|p| serialize_json_array(p.get("bucket_counts"))));

            flags.push(json_to_i32(point.get("flags")));
            aggregation_temporalities.push(aggregation_temporality);
            zero_thresholds.push(json_to_f64(point.get("zero_threshold")));
            resource_schema_urls.push(resource_context.resource_schema_url.clone());
            resource_attributes.push(resource_context.resource_attributes.clone());
            scope_names.push(scope_context.scope_name.clone());
            scope_versions.push(scope_context.scope_version.clone());
            scope_schema_urls.push(scope_context.scope_schema_url.clone());
            scope_attributes.push(scope_context.scope_attributes.clone());
            scope_dropped_attr_counts.push(Some(scope_context.scope_dropped_attr_count));
            attributes.push(serialize_json(point.get("attributes")));
            exemplars.push(serialize_json(point.get("exemplars")));
            date_days.push(date_day);
            hours.push(hour);
            resource_identities.push(resource_context.resource_identity.clone());
        }
    }

    let (label_fields, label_columns) = materialized_label_columns_from_json(
        &resource_attributes,
        &scope_attributes,
        &attributes,
        labels,
    );
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(TimestampNanosecondArray::from(timestamps)),
        Arc::new(TimestampNanosecondArray::from(start_timestamps)),
        Arc::new(StringArray::from(service_names)),
        Arc::new(StringArray::from(metric_names)),
        Arc::new(StringArray::from(metric_descriptions)),
        Arc::new(StringArray::from(metric_units)),
        Arc::new(Int64Array::from(counts)),
        Arc::new(Float64Array::from(sums)),
        Arc::new(Float64Array::from(mins)),
        Arc::new(Float64Array::from(maxes)),
        Arc::new(Int32Array::from(scales)),
        Arc::new(Int64Array::from(zero_counts)),
        Arc::new(Int32Array::from(positive_offsets)),
        Arc::new(StringArray::from(positive_bucket_counts)),
        Arc::new(Int32Array::from(negative_offsets)),
        Arc::new(StringArray::from(negative_bucket_counts)),
        Arc::new(Int32Array::from(flags)),
        Arc::new(Int32Array::from(aggregation_temporalities)),
        Arc::new(Float64Array::from(zero_thresholds)),
        Arc::new(StringArray::from(resource_schema_urls)),
        Arc::new(StringArray::from(resource_attributes)),
        Arc::new(StringArray::from(scope_names)),
        Arc::new(StringArray::from(scope_versions)),
        Arc::new(StringArray::from(scope_schema_urls)),
        Arc::new(StringArray::from(scope_attributes)),
        Arc::new(Int32Array::from(scope_dropped_attr_counts)),
        Arc::new(StringArray::from(attributes)),
        Arc::new(StringArray::from(exemplars)),
        Arc::new(Date32Array::from(date_days)),
        Arc::new(Int32Array::from(hours)),
        Arc::new(StringArray::from(resource_identities)),
    ];
    let out_schema =
        extend_schema_with_labels(output_schema, label_fields, &mut columns, label_columns);
    RecordBatch::try_new(out_schema, columns).map_err(|e| {
        anyhow!(
            "Failed to create transformed metrics_exponential_histogram RecordBatch: {}",
            e
        )
    })
}

pub fn transform_metrics_summary_v1_to_iceberg(
    batch: RecordBatch,
    labels: &[String],
) -> Result<RecordBatch> {
    let output_schema = create_metrics_summary_arrow_schema();

    let name_array = get_typed_column::<StringArray>(&batch, "name")?;
    let description_array = get_typed_column::<StringArray>(&batch, "description")?;
    let unit_array = get_typed_column::<StringArray>(&batch, "unit")?;
    let resource_json_array = get_typed_column::<StringArray>(&batch, "resource_json")?;
    let scope_json_array = get_typed_column::<StringArray>(&batch, "scope_json")?;
    let data_json_array = get_typed_column::<StringArray>(&batch, "data_json")?;

    let mut timestamps: Vec<Option<i64>> = Vec::new();
    let mut start_timestamps: Vec<Option<i64>> = Vec::new();
    let mut service_names: Vec<Option<String>> = Vec::new();
    let mut metric_names: Vec<Option<String>> = Vec::new();
    let mut metric_descriptions: Vec<Option<String>> = Vec::new();
    let mut metric_units: Vec<Option<String>> = Vec::new();
    let mut counts: Vec<Option<i64>> = Vec::new();
    let mut sums: Vec<Option<f64>> = Vec::new();
    let mut quantile_values: Vec<Option<String>> = Vec::new();
    let mut flags: Vec<Option<i32>> = Vec::new();
    let mut resource_schema_urls: Vec<Option<String>> = Vec::new();
    let mut resource_attributes: Vec<Option<String>> = Vec::new();
    let mut scope_names: Vec<Option<String>> = Vec::new();
    let mut scope_versions: Vec<Option<String>> = Vec::new();
    let mut scope_schema_urls: Vec<Option<String>> = Vec::new();
    let mut scope_attributes: Vec<Option<String>> = Vec::new();
    let mut scope_dropped_attr_counts: Vec<Option<i32>> = Vec::new();
    let mut attributes: Vec<Option<String>> = Vec::new();
    let mut exemplars: Vec<Option<String>> = Vec::new();
    let mut date_days: Vec<Option<i32>> = Vec::new();
    let mut hours: Vec<Option<i32>> = Vec::new();
    let mut resource_identities: Vec<Option<String>> = Vec::new();

    for row in 0..batch.num_rows() {
        let metric_name = string_value(name_array, row);
        let metric_description = string_value(description_array, row);
        let metric_unit = string_value(unit_array, row);

        let resource_context = extract_resource_context(string_value_ref(resource_json_array, row));
        let scope_context = extract_scope_context(string_value_ref(scope_json_array, row));
        let data_points = parse_data_points(string_value_ref(data_json_array, row));

        for point in data_points {
            let (timestamp, date_day, hour) =
                temporal_from_nanos(json_to_u64(point.get("time_unix_nano")));
            let (start_timestamp, _, _) =
                temporal_from_nanos(json_to_u64(point.get("start_time_unix_nano")));

            timestamps.push(timestamp);
            start_timestamps.push(start_timestamp);
            service_names.push(resource_context.service_name.clone());
            metric_names.push(metric_name.clone());
            metric_descriptions.push(metric_description.clone());
            metric_units.push(metric_unit.clone());
            counts.push(json_to_i64(point.get("count")));
            sums.push(json_to_f64(point.get("sum")));
            quantile_values.push(serialize_json_array(point.get("quantile_values")));
            flags.push(json_to_i32(point.get("flags")));
            resource_schema_urls.push(resource_context.resource_schema_url.clone());
            resource_attributes.push(resource_context.resource_attributes.clone());
            scope_names.push(scope_context.scope_name.clone());
            scope_versions.push(scope_context.scope_version.clone());
            scope_schema_urls.push(scope_context.scope_schema_url.clone());
            scope_attributes.push(scope_context.scope_attributes.clone());
            scope_dropped_attr_counts.push(Some(scope_context.scope_dropped_attr_count));
            attributes.push(serialize_json(point.get("attributes")));
            exemplars.push(serialize_json(point.get("exemplars")));
            date_days.push(date_day);
            hours.push(hour);
            resource_identities.push(resource_context.resource_identity.clone());
        }
    }

    let (label_fields, label_columns) = materialized_label_columns_from_json(
        &resource_attributes,
        &scope_attributes,
        &attributes,
        labels,
    );
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(TimestampNanosecondArray::from(timestamps)),
        Arc::new(TimestampNanosecondArray::from(start_timestamps)),
        Arc::new(StringArray::from(service_names)),
        Arc::new(StringArray::from(metric_names)),
        Arc::new(StringArray::from(metric_descriptions)),
        Arc::new(StringArray::from(metric_units)),
        Arc::new(Int64Array::from(counts)),
        Arc::new(Float64Array::from(sums)),
        Arc::new(StringArray::from(quantile_values)),
        Arc::new(Int32Array::from(flags)),
        Arc::new(StringArray::from(resource_schema_urls)),
        Arc::new(StringArray::from(resource_attributes)),
        Arc::new(StringArray::from(scope_names)),
        Arc::new(StringArray::from(scope_versions)),
        Arc::new(StringArray::from(scope_schema_urls)),
        Arc::new(StringArray::from(scope_attributes)),
        Arc::new(Int32Array::from(scope_dropped_attr_counts)),
        Arc::new(StringArray::from(attributes)),
        Arc::new(StringArray::from(exemplars)),
        Arc::new(Date32Array::from(date_days)),
        Arc::new(Int32Array::from(hours)),
        Arc::new(StringArray::from(resource_identities)),
    ];
    let out_schema =
        extend_schema_with_labels(output_schema, label_fields, &mut columns, label_columns);
    RecordBatch::try_new(out_schema, columns).map_err(|e| {
        anyhow!(
            "Failed to create transformed metrics_summary RecordBatch: {}",
            e
        )
    })
}

/// The Arrow schema a transform emits for `resolved`, with each typed
/// attribute container collapsed back to one JSON-string column: placement
/// in `storage/iceberg.rs` splits it into the typed columns at commit.
fn create_wide_transform_schema(resolved: ResolvedSchema) -> Result<Arc<Schema>> {
    use common::schema::schema_parser::ResolvedField;
    use common::schema::typed_attributes;

    let mut collapsed: Vec<ResolvedField> = Vec::new();
    let mut containers_seen: Vec<&str> = Vec::new();
    for field in &resolved.fields {
        let container = field
            .name
            .strip_suffix("_residue")
            .or_else(|| typed_attributes::canonical_of_home_column(&field.name).map(|(c, _)| c));
        let Some(container) = container else {
            collapsed.push(field.clone());
            continue;
        };
        if containers_seen.contains(&container) {
            continue;
        }
        containers_seen.push(container);
        collapsed.push(ResolvedField {
            name: container.to_string(),
            field_type: "string".to_string(),
            required: false,
            computed: None,
            physical_only: false,
            field_id: field.field_id,
        });
    }

    create_arrow_schema_from_resolved(&ResolvedSchema {
        fields: collapsed,
        ..resolved
    })
}

fn json_array_of<T>(
    value: Option<&serde_json::Value>,
    decode: impl Fn(Option<&serde_json::Value>) -> Option<T>,
) -> Option<Vec<Option<T>>> {
    let array = value?.as_array()?;
    Some(array.iter().map(|v| decode(Some(v))).collect())
}

type OptionalF64List = Option<Vec<Option<f64>>>;

fn summary_quantile_lists(value: Option<&serde_json::Value>) -> (OptionalF64List, OptionalF64List) {
    let Some(entries) = value.and_then(|v| v.as_array()).filter(|a| !a.is_empty()) else {
        return (None, None);
    };
    let mut quantiles = Vec::with_capacity(entries.len());
    let mut values = Vec::with_capacity(entries.len());
    for entry in entries {
        quantiles.push(json_to_f64(entry.get("quantile")));
        values.push(json_to_f64(entry.get("value")));
    }
    (Some(quantiles), Some(values))
}

#[derive(Default)]
struct WidePointFields {
    value: Option<f64>,
    count: Option<i64>,
    sum: Option<f64>,
    min: Option<f64>,
    max: Option<f64>,
    explicit_bounds: Option<Vec<Option<f64>>>,
    bucket_counts: Option<Vec<Option<i64>>>,
    scale: Option<i32>,
    zero_count: Option<i64>,
    zero_threshold: Option<f64>,
    positive_offset: Option<i32>,
    positive_bucket_counts: Option<Vec<Option<i64>>>,
    negative_offset: Option<i32>,
    negative_bucket_counts: Option<Vec<Option<i64>>>,
    quantiles: Option<Vec<Option<f64>>>,
    quantile_values: Option<Vec<Option<f64>>>,
    aggregation_temporality: Option<i32>,
    is_monotonic: Option<bool>,
}

fn bucket_side(side: Option<&serde_json::Value>) -> (Option<i32>, Option<Vec<Option<i64>>>) {
    (
        side.and_then(|s| json_to_i32(s.get("offset"))),
        side.and_then(|s| json_array_of(s.get("bucket_counts"), json_to_i64)),
    )
}

/// The wire carries temporality and monotonicity on every row; only the
/// types OTLP defines them for keep them (temporality: sum and both
/// histograms, monotonicity: sum).
fn wide_point_fields(
    metric_type: &str,
    point: &serde_json::Value,
    aggregation_temporality: Option<i32>,
    is_monotonic: Option<bool>,
) -> WidePointFields {
    match metric_type {
        "gauge" => WidePointFields {
            value: Some(point_value(point)),
            ..Default::default()
        },
        "sum" => WidePointFields {
            value: Some(point_value(point)),
            aggregation_temporality,
            is_monotonic,
            ..Default::default()
        },
        "histogram" => WidePointFields {
            count: json_to_i64(point.get("count")),
            sum: json_to_f64(point.get("sum")),
            min: json_to_f64(point.get("min")),
            max: json_to_f64(point.get("max")),
            explicit_bounds: json_array_of(point.get("explicit_bounds"), json_to_f64),
            bucket_counts: json_array_of(point.get("bucket_counts"), json_to_i64),
            aggregation_temporality,
            ..Default::default()
        },
        "exponential_histogram" => {
            let (positive_offset, positive_bucket_counts) = bucket_side(point.get("positive"));
            let (negative_offset, negative_bucket_counts) = bucket_side(point.get("negative"));
            WidePointFields {
                count: json_to_i64(point.get("count")),
                sum: json_to_f64(point.get("sum")),
                min: json_to_f64(point.get("min")),
                max: json_to_f64(point.get("max")),
                scale: json_to_i32(point.get("scale")),
                zero_count: json_to_i64(point.get("zero_count")),
                zero_threshold: json_to_f64(point.get("zero_threshold")),
                positive_offset,
                positive_bucket_counts,
                negative_offset,
                negative_bucket_counts,
                aggregation_temporality,
                ..Default::default()
            }
        }
        "summary" => {
            let (quantiles, quantile_values) = summary_quantile_lists(point.get("quantile_values"));
            WidePointFields {
                count: json_to_i64(point.get("count")),
                sum: json_to_f64(point.get("sum")),
                quantiles,
                quantile_values,
                ..Default::default()
            }
        }
        _ => WidePointFields {
            value: json_to_f64(point.get("value")),
            ..Default::default()
        },
    }
}

fn primitive_list_column<'a, T: ArrowPrimitiveType>(
    values: impl Iterator<Item = &'a Option<Vec<Option<T::Native>>>>,
) -> ArrayRef {
    use datafusion::arrow::array::{ListBuilder, PrimitiveBuilder};
    let mut builder = ListBuilder::new(PrimitiveBuilder::<T>::new());
    for value in values {
        match value {
            Some(list) => {
                builder.values().extend(list.iter().copied());
                builder.append(true);
            }
            None => builder.append(false),
        }
    }
    Arc::new(builder.finish())
}

fn str_column<'a, T>(items: &'a [T], f: impl Fn(&'a T) -> Option<&'a str>) -> ArrayRef {
    Arc::new(items.iter().map(f).collect::<StringArray>())
}

fn point_series_id(key: &SeriesKey<'_>, point: &serde_json::Value) -> String {
    static EMPTY_ATTRS: std::sync::LazyLock<serde_json::Map<String, serde_json::Value>> =
        std::sync::LazyLock::new(serde_json::Map::new);
    let attrs = point
        .get("attributes")
        .and_then(|v| v.as_object())
        .unwrap_or(&EMPTY_ATTRS);
    metric_series_id(key, attrs)
}

struct WideMetric {
    name: String,
    description: Option<String>,
    unit: Option<String>,
    metric_type: String,
    resource: ResourceContext,
    scope: ScopeContext,
}

impl WideMetric {
    fn series_key(&self) -> SeriesKey<'_> {
        SeriesKey {
            metric_name: &self.name,
            metric_type: &self.metric_type,
            resource_identity: self.resource.resource_identity.as_deref(),
            scope_name: self.scope.scope_name.as_deref(),
            scope_version: self.scope.scope_version.as_deref(),
        }
    }
}

struct WidePoint {
    metric: usize,
    timestamp: Option<i64>,
    start_timestamp: Option<i64>,
    date_day: Option<i32>,
    hour: Option<i32>,
    series_id: String,
    flags: Option<i32>,
    attributes: Option<String>,
    fields: WidePointFields,
}

static METRICS_WIDE_SCHEMA: std::sync::LazyLock<Result<Arc<Schema>, String>> =
    std::sync::LazyLock::new(|| {
        let resolved = SCHEMA_DEFINITIONS
            .resolve_table_schema(&SCHEMA_DEFINITIONS.metrics, TYPED_METRIC_VERSION)
            .map_err(|e| e.to_string())?;
        create_wide_transform_schema(resolved).map_err(|e| e.to_string())
    });

/// A wire metrics batch of any mix of metric types -> one `metrics` row per
/// data point.
pub fn transform_metrics_to_wide(batch: RecordBatch, labels: &[String]) -> Result<RecordBatch> {
    let output_schema = METRICS_WIDE_SCHEMA
        .clone()
        .map_err(|e| anyhow!("failed to build the metrics wide schema: {e}"))?;

    let name_array = get_typed_column::<StringArray>(&batch, "name")?;
    let description_array = get_typed_column::<StringArray>(&batch, "description")?;
    let unit_array = get_typed_column::<StringArray>(&batch, "unit")?;
    let resource_json_array = get_typed_column::<StringArray>(&batch, "resource_json")?;
    let scope_json_array = get_typed_column::<StringArray>(&batch, "scope_json")?;
    let data_json_array = get_typed_column::<StringArray>(&batch, "data_json")?;
    let metric_type_array = get_typed_column::<StringArray>(&batch, "metric_type")?;
    let aggregation_temporality_array =
        get_typed_column::<Int32Array>(&batch, "aggregation_temporality")?;
    let is_monotonic_array = get_typed_column::<BooleanArray>(&batch, "is_monotonic")?;

    let mut metrics: Vec<WideMetric> = Vec::with_capacity(batch.num_rows());
    let mut points: Vec<WidePoint> = Vec::new();
    for row in 0..batch.num_rows() {
        let metric = WideMetric {
            name: string_value(name_array, row).unwrap_or_default(),
            description: string_value(description_array, row),
            unit: string_value(unit_array, row),
            metric_type: string_value(metric_type_array, row).unwrap_or_default(),
            resource: extract_resource_context(string_value_ref(resource_json_array, row)),
            scope: extract_scope_context(string_value_ref(scope_json_array, row)),
        };
        let aggregation_temporality = int32_value(aggregation_temporality_array, row);
        let is_monotonic = bool_value(is_monotonic_array, row);

        for point in parse_data_points(string_value_ref(data_json_array, row)) {
            let (timestamp, date_day, hour) =
                temporal_from_nanos(json_to_u64(point.get("time_unix_nano")));
            let (start_timestamp, _, _) =
                temporal_from_nanos(json_to_u64(point.get("start_time_unix_nano")));
            points.push(WidePoint {
                metric: metrics.len(),
                timestamp,
                start_timestamp,
                date_day,
                hour,
                series_id: point_series_id(&metric.series_key(), &point),
                flags: json_to_i32(point.get("flags")),
                attributes: serialize_json(point.get("attributes")),
                fields: wide_point_fields(
                    &metric.metric_type,
                    &point,
                    aggregation_temporality,
                    is_monotonic,
                ),
            });
        }
        metrics.push(metric);
    }

    let f64s = |f: fn(&WidePointFields) -> Option<f64>| -> ArrayRef {
        Arc::new(
            points
                .iter()
                .map(|p| f(&p.fields))
                .collect::<Float64Array>(),
        )
    };
    let i64s = |f: fn(&WidePointFields) -> Option<i64>| -> ArrayRef {
        Arc::new(points.iter().map(|p| f(&p.fields)).collect::<Int64Array>())
    };
    let i32s = |f: &dyn Fn(&WidePoint) -> Option<i32>| -> ArrayRef {
        Arc::new(points.iter().map(f).collect::<Int32Array>())
    };
    let owned = |f: &dyn Fn(&WidePoint) -> Option<String>| -> Vec<Option<String>> {
        points.iter().map(f).collect()
    };
    let resource_attributes = owned(&|p| metrics[p.metric].resource.resource_attributes.clone());
    let scope_attributes = owned(&|p| metrics[p.metric].scope.scope_attributes.clone());
    let attributes = owned(&|p| p.attributes.clone());

    let (label_fields, label_columns) = materialized_label_columns_from_json(
        &resource_attributes,
        &scope_attributes,
        &attributes,
        labels,
    );
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(
            points
                .iter()
                .map(|p| p.timestamp)
                .collect::<TimestampNanosecondArray>(),
        ),
        Arc::new(
            points
                .iter()
                .map(|p| p.start_timestamp)
                .collect::<TimestampNanosecondArray>(),
        ),
        str_column(&points, |p| {
            metrics[p.metric].resource.service_name.as_deref()
        }),
        str_column(&points, |p| Some(metrics[p.metric].name.as_str())),
        str_column(&points, |p| metrics[p.metric].description.as_deref()),
        str_column(&points, |p| metrics[p.metric].unit.as_deref()),
        str_column(&points, |p| Some(metrics[p.metric].metric_type.as_str())),
        str_column(&points, |p| Some(p.series_id.as_str())),
        f64s(|f| f.value),
        i64s(|f| f.count),
        f64s(|f| f.sum),
        f64s(|f| f.min),
        f64s(|f| f.max),
        primitive_list_column::<Float64Type>(points.iter().map(|p| &p.fields.explicit_bounds)),
        primitive_list_column::<Int64Type>(points.iter().map(|p| &p.fields.bucket_counts)),
        i32s(&|p| p.fields.scale),
        i64s(|f| f.zero_count),
        f64s(|f| f.zero_threshold),
        i32s(&|p| p.fields.positive_offset),
        primitive_list_column::<Int64Type>(points.iter().map(|p| &p.fields.positive_bucket_counts)),
        i32s(&|p| p.fields.negative_offset),
        primitive_list_column::<Int64Type>(points.iter().map(|p| &p.fields.negative_bucket_counts)),
        primitive_list_column::<Float64Type>(points.iter().map(|p| &p.fields.quantiles)),
        primitive_list_column::<Float64Type>(points.iter().map(|p| &p.fields.quantile_values)),
        i32s(&|p| p.flags),
        i32s(&|p| p.fields.aggregation_temporality),
        Arc::new(
            points
                .iter()
                .map(|p| p.fields.is_monotonic)
                .collect::<BooleanArray>(),
        ),
        str_column(&points, |p| {
            metrics[p.metric].resource.resource_schema_url.as_deref()
        }),
        Arc::new(StringArray::from(resource_attributes)),
        str_column(&points, |p| metrics[p.metric].scope.scope_name.as_deref()),
        str_column(&points, |p| {
            metrics[p.metric].scope.scope_version.as_deref()
        }),
        str_column(&points, |p| {
            metrics[p.metric].scope.scope_schema_url.as_deref()
        }),
        Arc::new(StringArray::from(scope_attributes)),
        i32s(&|p| Some(metrics[p.metric].scope.scope_dropped_attr_count)),
        Arc::new(StringArray::from(attributes)),
        str_column(&points, |p| {
            metrics[p.metric].resource.resource_identity.as_deref()
        }),
        Arc::new(points.iter().map(|p| p.date_day).collect::<Date32Array>()),
        i32s(&|p| p.hour),
    ];
    let out_schema =
        extend_schema_with_labels(output_schema, label_fields, &mut columns, label_columns);
    RecordBatch::try_new(out_schema, columns)
        .map_err(|e| anyhow!("Failed to create transformed metrics RecordBatch: {}", e))
}

/// The `metric_exemplars` transform schema, resolved once rather than per batch.
static METRIC_EXEMPLARS_SCHEMA: std::sync::LazyLock<Result<Arc<Schema>, String>> =
    std::sync::LazyLock::new(|| {
        let resolved = SCHEMA_DEFINITIONS
            .resolve_table_schema(&SCHEMA_DEFINITIONS.metric_exemplars, TYPED_METRIC_VERSION)
            .map_err(|e| e.to_string())?;
        create_wide_transform_schema(resolved).map_err(|e| e.to_string())
    });

struct WideExemplar {
    metric: usize,
    timestamp: Option<i64>,
    point_timestamp: Option<i64>,
    date_day: Option<i32>,
    hour: Option<i32>,
    series_id: String,
    value: Option<f64>,
    trace_id: Option<String>,
    span_id: Option<String>,
    filtered_attributes: Option<String>,
}

/// A wire metrics batch -> one `metric_exemplars` row per exemplar across
/// every point. No exemplars is a zero-row batch, not an error.
pub fn transform_metric_exemplars(batch: RecordBatch) -> Result<RecordBatch> {
    let output_schema = METRIC_EXEMPLARS_SCHEMA
        .clone()
        .map_err(|e| anyhow!("failed to build the metric_exemplars schema: {e}"))?;

    let name_array = get_typed_column::<StringArray>(&batch, "name")?;
    let resource_json_array = get_typed_column::<StringArray>(&batch, "resource_json")?;
    let scope_json_array = get_typed_column::<StringArray>(&batch, "scope_json")?;
    let data_json_array = get_typed_column::<StringArray>(&batch, "data_json")?;
    let metric_type_array = get_typed_column::<StringArray>(&batch, "metric_type")?;

    let mut metrics: Vec<WideMetric> = Vec::with_capacity(batch.num_rows());
    let mut exemplars: Vec<WideExemplar> = Vec::new();
    for row in 0..batch.num_rows() {
        let metric = WideMetric {
            name: string_value(name_array, row).unwrap_or_default(),
            description: None,
            unit: None,
            metric_type: string_value(metric_type_array, row).unwrap_or_default(),
            resource: extract_resource_context(string_value_ref(resource_json_array, row)),
            scope: extract_scope_context(string_value_ref(scope_json_array, row)),
        };

        for point in parse_data_points(string_value_ref(data_json_array, row)) {
            let (point_timestamp, _, _) =
                temporal_from_nanos(json_to_u64(point.get("time_unix_nano")));
            let Some(point_exemplars) = point.get("exemplars").and_then(|v| v.as_array()) else {
                continue;
            };
            let series_id = point_series_id(&metric.series_key(), &point);

            for exemplar in point_exemplars {
                let (timestamp, date_day, hour) =
                    temporal_from_nanos(json_to_u64(exemplar.get("time_unix_nano")));
                exemplars.push(WideExemplar {
                    metric: metrics.len(),
                    timestamp,
                    point_timestamp,
                    date_day,
                    hour,
                    series_id: series_id.clone(),
                    value: json_to_f64(exemplar.get("value")),
                    trace_id: exemplar
                        .get("trace_id")
                        .and_then(|v| v.as_str())
                        .map(ToString::to_string),
                    span_id: exemplar
                        .get("span_id")
                        .and_then(|v| v.as_str())
                        .map(ToString::to_string),
                    filtered_attributes: serialize_json(exemplar.get("filtered_attributes")),
                });
            }
        }
        metrics.push(metric);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(
            exemplars
                .iter()
                .map(|e| e.timestamp)
                .collect::<TimestampNanosecondArray>(),
        ),
        Arc::new(
            exemplars
                .iter()
                .map(|e| e.point_timestamp)
                .collect::<TimestampNanosecondArray>(),
        ),
        str_column(&exemplars, |e| {
            metrics[e.metric].resource.service_name.as_deref()
        }),
        str_column(&exemplars, |e| Some(metrics[e.metric].name.as_str())),
        str_column(&exemplars, |e| Some(metrics[e.metric].metric_type.as_str())),
        str_column(&exemplars, |e| Some(e.series_id.as_str())),
        Arc::new(exemplars.iter().map(|e| e.value).collect::<Float64Array>()),
        str_column(&exemplars, |e| e.trace_id.as_deref()),
        str_column(&exemplars, |e| e.span_id.as_deref()),
        str_column(&exemplars, |e| e.filtered_attributes.as_deref()),
        str_column(&exemplars, |e| {
            metrics[e.metric].resource.resource_identity.as_deref()
        }),
        Arc::new(
            exemplars
                .iter()
                .map(|e| e.date_day)
                .collect::<Date32Array>(),
        ),
        Arc::new(exemplars.iter().map(|e| e.hour).collect::<Int32Array>()),
    ];
    RecordBatch::try_new(output_schema, columns).map_err(|e| {
        anyhow!(
            "Failed to create transformed metric_exemplars RecordBatch: {}",
            e
        )
    })
}

/// Target Arrow schema for the profiles Iceberg table
pub fn create_profiles_arrow_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("profile_id", DataType::Utf8, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("duration_nano", DataType::Int64, false),
        Field::new("sample_type", DataType::Utf8, false),
        Field::new("sample_unit", DataType::Utf8, false),
        Field::new("period_type", DataType::Utf8, true),
        Field::new("period_unit", DataType::Utf8, true),
        Field::new("period", DataType::Int64, true),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("stacktraces_json", DataType::Utf8, false),
        Field::new("samples_json", DataType::Utf8, false),
        Field::new("resource_attributes", DataType::Utf8, true),
        Field::new("scope_attributes", DataType::Utf8, true),
        Field::new("profile_attributes", DataType::Utf8, true),
        Field::new("trace_id", DataType::Utf8, true),
        Field::new("span_id", DataType::Utf8, true),
        Field::new("date_day", DataType::Date32, false),
        Field::new("hour", DataType::Int32, false),
        Field::new("resource_identity", DataType::Utf8, true),
    ]))
}

/// Transform a profiles RecordBatch from the Flight wire format (v1) to the
/// Iceberg storage schema: binary identifiers become hex strings (joinable
/// with traces/logs), `time_unix_nano` becomes `timestamp` plus computed
/// `date_day`/`hour` partition helper columns.
pub fn transform_profiles_v1_to_iceberg(
    batch: RecordBatch,
    labels: &[String],
) -> Result<RecordBatch> {
    let schema = create_profiles_arrow_schema();
    let num_rows = batch.num_rows();

    let time_col = get_column_by_name(&batch, "time_unix_nano")?;
    let time_array = time_col
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| anyhow!("time_unix_nano is not UInt64Array"))?;
    let nanos: Vec<u64> = (0..num_rows)
        .map(|i| {
            if time_array.is_null(i) {
                0
            } else {
                time_array.value(i)
            }
        })
        .collect();

    let binary_as_hex = |name: &str| -> Result<ArrayRef> {
        let col = get_column_by_name(&batch, name)?;
        let bin_array = col
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| anyhow!("{name} is not BinaryArray"))?;
        let values: Vec<Option<String>> = (0..bin_array.len())
            .map(|i| {
                if bin_array.is_null(i) {
                    return None;
                }
                let bytes = bin_array.value(i);
                if bytes.is_empty() {
                    None
                } else {
                    Some(hex::encode(bytes))
                }
            })
            .collect();
        Ok(Arc::new(StringArray::from(values)))
    };

    // A required hex identifier: null/empty encodes as the empty string
    // rather than null so the column satisfies the required constraint.
    let required_hex = |name: &str| -> Result<ArrayRef> {
        let col = get_column_by_name(&batch, name)?;
        let bin_array = col
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| anyhow!("{name} is not BinaryArray"))?;
        let values: Vec<String> = (0..bin_array.len())
            .map(|i| {
                if bin_array.is_null(i) {
                    String::new()
                } else {
                    hex::encode(bin_array.value(i))
                }
            })
            .collect();
        Ok(Arc::new(StringArray::from(values)))
    };

    let mut new_columns: Vec<ArrayRef> = Vec::new();
    for field in schema.fields() {
        let column: ArrayRef = match field.name().as_str() {
            "profile_id" => required_hex("profile_id")?,
            "timestamp" => {
                let values: Vec<Option<i64>> = nanos.iter().map(|&n| Some(n as i64)).collect();
                Arc::new(TimestampNanosecondArray::from(values))
            }
            "duration_nano" => {
                let col = get_column_by_name(&batch, "duration_nano")?;
                let uint_array = col
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| anyhow!("duration_nano is not UInt64Array"))?;
                let values: Vec<i64> = (0..uint_array.len())
                    .map(|i| {
                        if uint_array.is_null(i) {
                            0
                        } else {
                            uint_array.value(i) as i64
                        }
                    })
                    .collect();
                Arc::new(Int64Array::from(values))
            }
            "sample_type" => get_column_by_name(&batch, "sample_type_type")?,
            "sample_unit" => get_column_by_name(&batch, "sample_type_unit")?,
            "period_type" => get_column_by_name(&batch, "period_type_type")?,
            "period_unit" => get_column_by_name(&batch, "period_type_unit")?,
            "period" | "service_name" | "stacktraces_json" | "samples_json" => {
                get_column_by_name(&batch, field.name())?
            }
            "resource_attributes" => get_column_by_name(&batch, "resource_json")?,
            "scope_attributes" => get_column_by_name(&batch, "scope_json")?,
            "profile_attributes" => get_column_by_name(&batch, "attributes_json")?,
            "trace_id" => binary_as_hex("trace_id")?,
            "span_id" => binary_as_hex("span_id")?,
            "date_day" => {
                let dates: Vec<Option<i32>> = nanos
                    .iter()
                    .map(|&n| {
                        let secs = (n / 1_000_000_000) as i64;
                        let dt = DateTime::from_timestamp(secs, 0)?;
                        Some(dt.naive_utc().date().num_days_from_ce() - 719163)
                    })
                    .collect();
                Arc::new(Date32Array::from(dates))
            }
            "hour" => {
                let hours: Vec<Option<i32>> = nanos
                    .iter()
                    .map(|&n| {
                        let secs = (n / 1_000_000_000) as i64;
                        let dt = DateTime::from_timestamp(secs, 0)?;
                        Some(dt.hour() as i32)
                    })
                    .collect();
                Arc::new(Int32Array::from(hours))
            }
            "resource_identity" => resource_identity_from_resource_json_column(&batch)?,
            other => return Err(anyhow!("Unknown field in profiles schema: {other}")),
        };
        new_columns.push(column);
    }

    let (label_fields, label_columns) = materialized_label_columns(&batch, num_rows, labels)?;
    let out_schema =
        extend_schema_with_labels(schema, label_fields, &mut new_columns, label_columns);

    RecordBatch::try_new(out_schema, new_columns)
        .map_err(|e| anyhow!("Failed to create transformed profiles batch: {e}"))
}

pub fn transform_for_signal(
    signal_type: Option<&str>,
    target_table: Option<&str>,
    batch: RecordBatch,
    materialized: &common::config::MaterializedLabels,
) -> Result<RecordBatch> {
    let m = materialized;
    match (signal_type, target_table) {
        (Some("traces"), _) => transform_trace_v1_to_v2(batch, &m.traces),
        (Some("logs"), _) => transform_logs_v1_to_iceberg(batch, &m.logs),
        (Some("profiles"), _) => transform_profiles_v1_to_iceberg(batch, &m.profiles),
        (Some("metrics"), Some("metrics_histogram")) => {
            transform_metrics_histogram_v1_to_iceberg(batch, &m.metrics)
        }
        (Some("metrics"), Some("metrics_exponential_histogram")) => {
            transform_metrics_exponential_histogram_v1_to_iceberg(batch, &m.metrics)
        }
        (Some("metrics"), Some("metrics_summary")) => {
            transform_metrics_summary_v1_to_iceberg(batch, &m.metrics)
        }
        // An unknown metrics `target_table` is rejected by `routing::route`
        // before `do_put` reaches this transform (W4), so this arm is
        // unreachable from the ingest path. No other caller exists.
        _ => Ok(batch),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arrow_schema_from_resolved_maps_typed_lists_to_arrow_list_columns() {
        use common::schema::schema_parser::ResolvedField;

        let field = |name: &str, field_type: &str| ResolvedField {
            name: name.to_string(),
            field_type: field_type.to_string(),
            required: false,
            computed: None,
            physical_only: false,
            field_id: 0,
        };
        let resolved = ResolvedSchema {
            version: "v1".to_string(),
            description: "test".to_string(),
            fields: vec![
                field("bucket_counts", "list<int64>"),
                field("explicit_bounds", "list<double>"),
            ],
            partition_by: vec![],
        };
        let arrow_schema = create_arrow_schema_from_resolved(&resolved).unwrap();

        let counts = arrow_schema.field_with_name("bucket_counts").unwrap();
        let DataType::List(element) = counts.data_type() else {
            panic!(
                "bucket_counts should be a List, got {:?}",
                counts.data_type()
            );
        };
        assert_eq!(*element.data_type(), DataType::Int64);
        assert!(element.is_nullable());

        let bounds = arrow_schema.field_with_name("explicit_bounds").unwrap();
        let DataType::List(element) = bounds.data_type() else {
            panic!(
                "explicit_bounds should be a List, got {:?}",
                bounds.data_type()
            );
        };
        assert_eq!(*element.data_type(), DataType::Float64);
    }

    #[test]
    fn materialized_label_column_carries_its_origin_key_and_survives_an_ipc_round_trip() {
        let (fields, columns) = materialized_label_columns_from_json(
            &[Some(r#"{"http.method":"GET"}"#.to_string())],
            &[None],
            &[None],
            &["http.method".to_string()],
        );
        assert_eq!(fields.len(), 1);
        assert_eq!(
            fields[0].metadata().get(LABEL_ORIGIN_KEY_METADATA),
            Some(&"http.method".to_string()),
            "materialized label field must carry its origin key in metadata"
        );

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(fields.clone())),
            columns.into_iter().map(|c| c as ArrayRef).collect(),
        )
        .unwrap();

        // WAL persists batches through this same IPC path.
        let bytes = common::wal::record_batch_to_bytes(&batch).unwrap();
        let round_tripped = common::wal::bytes_to_record_batch(&bytes).unwrap();
        let round_tripped_field = round_tripped.schema().field(0).clone();
        assert_eq!(
            round_tripped_field
                .metadata()
                .get(LABEL_ORIGIN_KEY_METADATA),
            Some(&"http.method".to_string()),
            "origin key metadata must survive an Arrow IPC write/read round trip"
        );
    }

    #[test]
    fn transform_trace_v1_to_v2_carries_the_1208_columns_through() {
        use common::flight::conversion::otlp_traces_to_arrow;
        use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
        use opentelemetry_proto::tonic::trace::v1::{
            ResourceSpans, ScopeSpans, Span as OtelSpan, Status,
        };

        let span = OtelSpan {
            trace_id: vec![0xab; 16],
            span_id: vec![0xcd; 8],
            parent_span_id: vec![],
            name: "checkout".to_string(),
            kind: 2, // Server
            start_time_unix_nano: 1_700_000_000_000_000_000,
            end_time_unix_nano: 1_700_000_000_100_000_000,
            attributes: vec![],
            dropped_attributes_count: 3,
            events: vec![],
            dropped_events_count: 5,
            links: vec![],
            dropped_links_count: 7,
            status: Some(Status {
                code: 2, // Error
                message: "boom".to_string(),
            }),
            flags: 0,
            trace_state: String::new(),
        };
        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: None,
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![span],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let wire_batch = otlp_traces_to_arrow(&request).expect("conversion should succeed");
        let v2_batch = transform_trace_v1_to_v2(wire_batch, &[]).unwrap();

        let get_i32 = |name: &str| {
            let (idx, _) = v2_batch.schema().column_with_name(name).unwrap();
            v2_batch
                .column(idx)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0)
        };
        let get_i64 = |name: &str| {
            let (idx, _) = v2_batch.schema().column_with_name(name).unwrap();
            v2_batch
                .column(idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        };

        assert_eq!(get_i32("span_kind_number"), 2);
        assert_eq!(get_i32("status_code_number"), 2);
        assert_eq!(get_i64("dropped_attributes_count"), 3);
        assert_eq!(get_i64("dropped_events_count"), 5);
        assert_eq!(get_i64("dropped_links_count"), 7);
    }

    #[test]
    fn transform_trace_v1_to_v2_tolerates_a_v1_batch_missing_the_1208_columns() {
        // A v1 batch that predates #1208's columns entirely (an acceptor
        // mid-rolling-upgrade, or a hand-built test fixture) must not be
        // rejected wholesale -- missing reads as null, same as a present
        // column with a null value.
        use common::flight::conversion::otlp_traces_to_arrow;
        use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
        use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span as OtelSpan};

        let span = OtelSpan {
            trace_id: vec![0xab; 16],
            span_id: vec![0xcd; 8],
            name: "checkout".to_string(),
            kind: 2,
            start_time_unix_nano: 1,
            end_time_unix_nano: 2,
            ..Default::default()
        };
        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: None,
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![span],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        let full_batch = otlp_traces_to_arrow(&request).expect("conversion should succeed");

        // Simulate a producer without #1208's columns by projecting them out.
        let new_columns = [
            "span_kind_number",
            "status_code_number",
            "dropped_attributes_count",
            "dropped_events_count",
            "dropped_links_count",
        ];
        let keep: Vec<usize> = full_batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| !new_columns.contains(&f.name().as_str()))
            .map(|(i, _)| i)
            .collect();
        let reduced = full_batch
            .project(&keep)
            .expect("projection should succeed");
        assert_eq!(
            reduced.num_columns(),
            full_batch.num_columns() - new_columns.len()
        );

        let v2_batch = transform_trace_v1_to_v2(reduced, &[])
            .expect("a v1 batch missing #1208's columns must not error");

        for name in new_columns {
            let (idx, _) = v2_batch.schema().column_with_name(name).unwrap();
            assert!(
                v2_batch.column(idx).is_null(0),
                "missing column '{name}' should read as null, not error"
            );
        }
    }

    #[test]
    fn build_trace_v1_to_v2_plan_for_errors_on_an_unregistered_field() {
        use common::schema::schema_parser::ResolvedField;

        let bogus_schema = ResolvedSchema {
            version: "test-only".to_string(),
            description: "fixture with a field no extraction rule covers".to_string(),
            fields: vec![ResolvedField {
                name: "totally_unregistered_field".to_string(),
                field_type: "string".to_string(),
                required: false,
                computed: None,
                physical_only: false,
                field_id: 1,
            }],
            partition_by: vec![],
        };

        let Err(err) = build_trace_v1_to_v2_plan_for(&bogus_schema) else {
            panic!("a field with no matching extraction rule must fail plan construction");
        };
        assert!(
            err.to_string().contains("totally_unregistered_field"),
            "error should name the offending field: {err}"
        );
    }

    #[test]
    fn trace_v1_to_v2_plan_is_built_once_and_reused() {
        // OnceLock reuse, proven by pointer identity rather than a call
        // counter -- the plan is a shared process-wide static regardless of
        // test execution order, so this must hold no matter which test
        // (this one or `warm_trace_v1_to_v2_plan` elsewhere) initializes it
        // first.
        let first = trace_v1_to_v2_plan().expect("plan should build");
        let second = trace_v1_to_v2_plan().expect("plan should build");
        assert!(
            std::ptr::eq(first, second),
            "repeated calls must reuse the cached plan, not rebuild it"
        );
    }

    #[test]
    fn warm_trace_v1_to_v2_plan_succeeds_against_the_real_schema() {
        warm_trace_v1_to_v2_plan().expect("the real current traces schema must resolve a plan");
    }

    #[test]
    fn transform_trace_v1_to_v2_produces_the_complete_expected_physical_v4_batch() {
        // Full-batch golden check (CodeRabbit review, PR #1230): schema
        // field names/order/types/nullability, not just a hand-picked
        // values/types/nullability subset for a few fields -- a plan bug
        // that reorders or drops a field would not be caught by checking
        // individual field values alone.
        use common::flight::conversion::otlp_traces_to_arrow;
        use datafusion::arrow::datatypes::{DataType, TimeUnit};
        use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
        use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};
        use opentelemetry_proto::tonic::resource::v1::Resource;
        use opentelemetry_proto::tonic::trace::v1::{
            ResourceSpans, ScopeSpans, Span as OtelSpan, Status,
        };

        let span = OtelSpan {
            trace_id: vec![0xab; 16],
            span_id: vec![0xcd; 8],
            parent_span_id: vec![],
            name: "checkout".to_string(),
            kind: 2, // Server
            start_time_unix_nano: 1_700_000_000_000_000_000,
            end_time_unix_nano: 1_700_000_000_100_000_000,
            attributes: vec![KeyValue {
                key: "http.method".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue("GET".to_string())),
                }),
                ..Default::default()
            }],
            dropped_attributes_count: 3,
            events: vec![],
            dropped_events_count: 5,
            links: vec![],
            dropped_links_count: 7,
            status: Some(Status {
                code: 2, // Error
                message: "boom".to_string(),
            }),
            flags: 0,
            trace_state: String::new(),
        };
        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("checkout-svc".to_string())),
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![span],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let wire_batch = otlp_traces_to_arrow(&request).expect("conversion should succeed");
        let v2_batch = transform_trace_v1_to_v2(wire_batch, &[]).expect("transform should succeed");

        // Expected physical-v3 field order: v1 base (renamed/typed per v2),
        // then v2's computed additions, then v3's numeric additions.
        let expected: &[(&str, DataType, bool)] = &[
            ("trace_id", DataType::Utf8, false),
            ("span_id", DataType::Utf8, false),
            ("parent_span_id", DataType::Utf8, true),
            ("span_name", DataType::Utf8, false),
            ("service_name", DataType::Utf8, false),
            ("start_time_unix_nano", DataType::Int64, false),
            ("end_time_unix_nano", DataType::Int64, false),
            ("duration_nanos", DataType::Int64, false),
            ("span_kind", DataType::Utf8, false),
            ("status_code", DataType::Utf8, false),
            ("status_message", DataType::Utf8, true),
            ("is_root", DataType::Boolean, false),
            ("span_attributes", DataType::Utf8, true),
            ("resource_attributes", DataType::Utf8, true),
            ("events", DataType::Utf8, true),
            ("links", DataType::Utf8, true),
            ("trace_state", DataType::Utf8, true),
            ("resource_schema_url", DataType::Utf8, true),
            ("scope_name", DataType::Utf8, true),
            ("scope_version", DataType::Utf8, true),
            ("scope_schema_url", DataType::Utf8, true),
            ("scope_attributes", DataType::Utf8, true),
            (
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            ("date_day", DataType::Date32, false),
            ("hour", DataType::Int32, false),
            ("span_kind_number", DataType::Int32, true),
            ("status_code_number", DataType::Int32, true),
            ("dropped_attributes_count", DataType::Int64, true),
            ("dropped_events_count", DataType::Int64, true),
            ("dropped_links_count", DataType::Int64, true),
            ("resource_identity", DataType::Utf8, true),
        ];

        let batch_schema = v2_batch.schema();
        let actual: Vec<(String, DataType, bool)> = batch_schema
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type().clone(), f.is_nullable()))
            .collect();
        assert_eq!(
            actual,
            expected
                .iter()
                .map(|(n, t, nu)| (n.to_string(), t.clone(), *nu))
                .collect::<Vec<_>>(),
            "full physical-v4 schema (names, order, types, nullability) must match exactly"
        );

        assert_eq!(v2_batch.num_rows(), 1);
        let get_str = |name: &str| {
            let (idx, _) = v2_batch.schema().column_with_name(name).unwrap();
            v2_batch
                .column(idx)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .unwrap()
                .value(0)
                .to_string()
        };
        assert_eq!(get_str("span_name"), "checkout", "renamed from v1's `name`");
        assert_eq!(get_str("service_name"), "checkout-svc");
        assert!(
            get_str("span_attributes").contains("http.method"),
            "renamed from v1's `attributes_json`"
        );
        assert_eq!(
            get_str("resource_identity"),
            resource_identity_from_json(r#"{"service.name":"checkout-svc"}"#).unwrap(),
            "digest of the span's resource attribute set"
        );
    }

    #[test]
    fn transform_trace_v1_to_v2_resource_identity_groups_spans_by_resource() {
        use common::flight::conversion::otlp_traces_to_arrow;
        use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
        use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};
        use opentelemetry_proto::tonic::resource::v1::Resource;
        use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span as OtelSpan};

        fn resource(service_name: &str) -> Resource {
            Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue(service_name.to_string())),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }
        }

        fn span(name: &str) -> OtelSpan {
            OtelSpan {
                trace_id: vec![0xab; 16],
                span_id: vec![0xcd; 8],
                name: name.to_string(),
                kind: 2,
                start_time_unix_nano: 1,
                end_time_unix_nano: 2,
                ..Default::default()
            }
        }

        let request = ExportTraceServiceRequest {
            resource_spans: vec![
                ResourceSpans {
                    resource: Some(resource("checkout")),
                    scope_spans: vec![ScopeSpans {
                        scope: None,
                        spans: vec![span("first"), span("second")],
                        schema_url: String::new(),
                    }],
                    schema_url: String::new(),
                },
                ResourceSpans {
                    resource: Some(resource("billing")),
                    scope_spans: vec![ScopeSpans {
                        scope: None,
                        spans: vec![span("third")],
                        schema_url: String::new(),
                    }],
                    schema_url: String::new(),
                },
            ],
        };

        let wire_batch = otlp_traces_to_arrow(&request).expect("conversion should succeed");
        let v2_batch = transform_trace_v1_to_v2(wire_batch, &[]).expect("transform should succeed");

        let identities = v2_batch
            .column(v2_batch.schema().index_of("resource_identity").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(identities.len(), 3);
        assert_eq!(
            identities.value(0),
            identities.value(1),
            "first two spans share the checkout resource"
        );
        assert_ne!(
            identities.value(0),
            identities.value(2),
            "third span has a different resource"
        );
        assert_eq!(
            identities.value(0),
            resource_identity_from_json(r#"{"service.name":"checkout"}"#).unwrap()
        );
    }

    #[test]
    fn transform_trace_v1_to_v2_null_resource_json_yields_null_resource_identity() {
        use common::flight::conversion::otlp_traces_to_arrow;
        use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
        use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span as OtelSpan};

        let span = OtelSpan {
            trace_id: vec![0xab; 16],
            span_id: vec![0xcd; 8],
            name: "checkout".to_string(),
            kind: 2,
            start_time_unix_nano: 1,
            end_time_unix_nano: 2,
            ..Default::default()
        };
        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: None,
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![span],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        let wire_batch = otlp_traces_to_arrow(&request).expect("conversion should succeed");

        // `otlp_traces_to_arrow` always produces a resource_json string (an
        // empty-resource span still gets "{}"); force an actual null to
        // exercise the extractor's null-source path, which a hand-built v1
        // batch (or an older wire producer) can still send.
        let resource_json_idx = wire_batch
            .schema()
            .index_of("resource_json")
            .expect("resource_json column must exist");
        let mut columns: Vec<ArrayRef> = wire_batch.columns().to_vec();
        columns[resource_json_idx] = Arc::new(StringArray::from(vec![None::<&str>]));
        let wire_batch = RecordBatch::try_new(wire_batch.schema(), columns).unwrap();

        let v2_batch = transform_trace_v1_to_v2(wire_batch, &[]).expect("transform should succeed");
        let identities = v2_batch
            .column(v2_batch.schema().index_of("resource_identity").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(identities.is_null(0));
    }

    #[test]
    fn transform_profiles_wire_batch_to_iceberg_schema() {
        use common::model::profile::{Frame, Profile, ProfileLink, Sample, Stacktrace, ValueType};

        let profile = Profile {
            profile_id: [0xab; 16],
            time_unix_nano: 1_700_003_600_000_000_000, // 2023-11-14T23:13:20Z area
            duration_nano: 10_000_000_000,
            sample_type: ValueType {
                type_: "cpu".to_string(),
                unit: "nanoseconds".to_string(),
            },
            period_type: None,
            period: 0,
            service_name: "checkout".to_string(),
            stacktraces: vec![Stacktrace {
                frames: vec![Frame {
                    function_name: "work".to_string(),
                    ..Frame::default()
                }],
            }],
            samples: vec![Sample {
                stacktrace_index: 0,
                values: vec![100],
                link_index: Some(0),
                ..Sample::default()
            }],
            links: vec![ProfileLink {
                trace_id: [0x11; 16],
                span_id: [0x22; 8],
            }],
            ..Profile::default()
        };

        let wire_batch = common::flight::conversion::profiles_to_arrow(&[profile]);
        let iceberg_batch = transform_profiles_v1_to_iceberg(wire_batch, &[]).unwrap();

        assert_eq!(iceberg_batch.num_rows(), 1);
        let schema = iceberg_batch.schema();
        for name in [
            "profile_id",
            "timestamp",
            "sample_type",
            "sample_unit",
            "stacktraces_json",
            "samples_json",
            "trace_id",
            "span_id",
            "date_day",
            "hour",
        ] {
            assert!(schema.index_of(name).is_ok(), "missing column {name}");
        }

        let profile_ids = iceberg_batch
            .column(schema.index_of("profile_id").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(profile_ids.value(0), "ab".repeat(16));

        let trace_ids = iceberg_batch
            .column(schema.index_of("trace_id").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(trace_ids.value(0), "11".repeat(16));

        // Storage batches pass through unchanged (idempotent transform guard
        // keys on the wire-only time_unix_nano column).
        assert!(schema.index_of("time_unix_nano").is_err());
    }

    /// A minimal profile carrying the given resource attributes (`None`
    /// leaves the resource unset, as an OTLP profile without a resource
    /// processor would).
    fn profile_with_resource(
        resource_attributes: Option<serde_json::Value>,
    ) -> common::model::profile::Profile {
        use common::model::profile::{Frame, Profile, Sample, Stacktrace, ValueType};
        Profile {
            profile_id: [0xab; 16],
            time_unix_nano: 1_700_000_000_000_000_000,
            duration_nano: 1,
            sample_type: ValueType {
                type_: "cpu".to_string(),
                unit: "nanoseconds".to_string(),
            },
            service_name: "checkout".to_string(),
            stacktraces: vec![Stacktrace {
                frames: vec![Frame {
                    function_name: "work".to_string(),
                    ..Frame::default()
                }],
            }],
            samples: vec![Sample {
                stacktrace_index: 0,
                values: vec![1],
                ..Sample::default()
            }],
            resource_attributes,
            ..Profile::default()
        }
    }

    #[test]
    fn profiles_transform_resource_identity_groups_by_resource() {
        let profiles = vec![
            profile_with_resource(Some(serde_json::json!({"service.name": "checkout"}))),
            profile_with_resource(Some(serde_json::json!({"service.name": "checkout"}))),
            profile_with_resource(Some(serde_json::json!({"service.name": "billing"}))),
        ];
        let wire_batch = common::flight::conversion::profiles_to_arrow(&profiles);
        let result = transform_profiles_v1_to_iceberg(wire_batch, &[]).unwrap();
        let identities = result
            .column(result.schema().index_of("resource_identity").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(identities.len(), 3);
        assert_eq!(
            identities.value(0),
            identities.value(1),
            "first two profiles share the checkout resource"
        );
        assert_ne!(
            identities.value(0),
            identities.value(2),
            "third profile has a different resource"
        );
        assert_eq!(
            identities.value(0),
            resource_identity_from_json(r#"{"service.name":"checkout"}"#).unwrap()
        );
    }

    #[test]
    fn profiles_transform_null_resource_json_yields_null_resource_identity() {
        let wire_batch =
            common::flight::conversion::profiles_to_arrow(&[profile_with_resource(None)]);
        let result = transform_profiles_v1_to_iceberg(wire_batch, &[]).unwrap();
        let identities = result
            .column(result.schema().index_of("resource_identity").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(identities.is_null(0));
    }

    #[test]
    fn determine_wal_operation_maps_profiles() {
        assert!(matches!(
            determine_wal_operation(Some("profiles")).unwrap(),
            common::wal::WalOperation::WriteProfiles
        ));
    }

    /// W4: an unrecognized `signal_type` must be rejected, not silently
    /// routed to `WriteTraces` — that batch can never commit under a
    /// signal it did not ask for.
    #[test]
    fn determine_wal_operation_rejects_unknown_signal_type() {
        assert!(determine_wal_operation(Some("foo")).is_err());
        assert!(determine_wal_operation(None).is_err());
    }

    #[test]
    fn extract_flight_metadata_reads_schema_version() {
        let metadata = r#"{"schema_version": "v1", "signal_type": "traces"}"#;
        let parsed = extract_flight_metadata(metadata.as_bytes()).unwrap();
        assert_eq!(parsed.schema_version, "v1");
    }

    #[test]
    fn extract_flight_metadata_errors_on_missing_schema_version() {
        let metadata = r#"{"signal_type": "traces"}"#;
        let result = extract_flight_metadata(metadata.as_bytes());
        assert!(result.is_err());
    }

    fn make_log_flight_batch(
        time_unix_nanos: &[u64],
        observed_time_unix_nanos: &[u64],
    ) -> RecordBatch {
        let n = time_unix_nanos.len();
        make_log_flight_batch_with_attrs(
            time_unix_nanos,
            observed_time_unix_nanos,
            vec![None; n],
            vec![None; n],
            vec![None; n],
        )
    }

    /// A minimal v1 metrics batch (as the acceptor produces it) with one
    /// gauge/sum data point and the given resource JSON.
    fn metrics_v1_batch(resource_json: Option<&str>) -> RecordBatch {
        metrics_v1_batch_with_points(
            resource_json,
            r#"[{"time_unix_nano":1700000001000000000,"start_time_unix_nano":1700000000000000000,"value":0.5,"attributes":{}}]"#,
        )
    }

    /// Like `metrics_v1_batch`, with the gauge/sum `data_json` points given.
    fn metrics_v1_batch_with_points(resource_json: Option<&str>, data_json: &str) -> RecordBatch {
        use datafusion::arrow::array::{BooleanArray, Int32Array};
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("description", DataType::Utf8, true),
            Field::new("unit", DataType::Utf8, true),
            Field::new("start_time_unix_nano", DataType::UInt64, true),
            Field::new("time_unix_nano", DataType::UInt64, false),
            Field::new("attributes_json", DataType::Utf8, true),
            Field::new("resource_json", DataType::Utf8, true),
            Field::new("scope_json", DataType::Utf8, true),
            Field::new("metric_type", DataType::Utf8, false),
            Field::new("data_json", DataType::Utf8, false),
            Field::new("aggregation_temporality", DataType::Int32, true),
            Field::new("is_monotonic", DataType::Boolean, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["system.cpu.utilization"])),
                Arc::new(StringArray::from(vec![Some("cpu")])),
                Arc::new(StringArray::from(vec![Some("1")])),
                Arc::new(UInt64Array::from(vec![Some(1_700_000_000_000_000_000u64)])),
                Arc::new(UInt64Array::from(vec![1_700_000_001_000_000_000u64])),
                Arc::new(StringArray::from(vec![Some("{}")])),
                Arc::new(StringArray::from(vec![resource_json])),
                Arc::new(StringArray::from(vec![Some(r#"{"name":"hostmetrics"}"#)])),
                Arc::new(StringArray::from(vec!["gauge"])),
                Arc::new(StringArray::from(vec![data_json])),
                Arc::new(Int32Array::from(vec![Some(2)])),
                Arc::new(BooleanArray::from(vec![Some(false)])),
            ],
        )
        .unwrap()
    }

    /// The remaining four metrics representations share `extract_resource_context`
    /// and the same `resource_identity` population code path as
    /// `metrics_gauge` (asserted above with the full grouping/null
    /// behaviour); this checks each one declares the column, nullable, so a
    /// transform that forgot to add it to its own
    /// `create_metrics_*_arrow_schema()` fails here instead of only at
    /// `schema_consistency`.
    #[test]
    fn remaining_metrics_transforms_declare_a_nullable_resource_identity_column() {
        for (name, schema) in [
            ("metrics_sum", create_metrics_sum_arrow_schema()),
            ("metrics_histogram", create_metrics_histogram_arrow_schema()),
            (
                "metrics_exponential_histogram",
                create_metrics_exponential_histogram_arrow_schema(),
            ),
            ("metrics_summary", create_metrics_summary_arrow_schema()),
        ] {
            let field = schema
                .field_with_name("resource_identity")
                .unwrap_or_else(|_| panic!("{name}: missing resource_identity column"));
            assert!(
                field.is_nullable(),
                "{name}: resource_identity must be nullable"
            );
            assert_eq!(field.data_type(), &DataType::Utf8, "{name}");
        }
    }

    fn make_log_flight_batch_with_attrs(
        time_unix_nanos: &[u64],
        observed_time_unix_nanos: &[u64],
        resource_json: Vec<Option<&str>>,
        scope_json: Vec<Option<&str>>,
        attributes_json: Vec<Option<&str>>,
    ) -> RecordBatch {
        use datafusion::arrow::array::BinaryArray;
        use datafusion::arrow::datatypes::Fields;

        let n = time_unix_nanos.len();
        let schema = Arc::new(Schema::new(Fields::from(vec![
            Field::new("time_unix_nano", DataType::UInt64, false),
            Field::new("observed_time_unix_nano", DataType::UInt64, false),
            Field::new("severity_number", DataType::Int32, true),
            Field::new("severity_text", DataType::Utf8, true),
            Field::new("body", DataType::Utf8, true),
            Field::new("trace_id", DataType::Binary, true),
            Field::new("span_id", DataType::Binary, true),
            Field::new("flags", DataType::UInt32, true),
            Field::new("attributes_json", DataType::Utf8, true),
            Field::new("resource_json", DataType::Utf8, true),
            Field::new("scope_json", DataType::Utf8, true),
            Field::new("dropped_attributes_count", DataType::UInt32, true),
            Field::new("service_name", DataType::Utf8, true),
            Field::new("event_name", DataType::Utf8, true),
        ])));

        let null_strings: Vec<Option<&str>> = vec![None; n];
        let null_binaries: Vec<Option<&[u8]>> = vec![None; n];
        let null_u32: Vec<Option<u32>> = vec![None; n];
        let null_i32: Vec<Option<i32>> = vec![None; n];
        let service_names: Vec<Option<&str>> = vec![Some("test-service"); n];

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(time_unix_nanos.to_vec())),
                Arc::new(UInt64Array::from(observed_time_unix_nanos.to_vec())),
                Arc::new(Int32Array::from(null_i32)),
                Arc::new(StringArray::from(null_strings.clone())),
                Arc::new(StringArray::from(vec![Some("test log"); n])),
                Arc::new(BinaryArray::from(null_binaries.clone())),
                Arc::new(BinaryArray::from(null_binaries)),
                Arc::new(UInt32Array::from(null_u32.clone())),
                Arc::new(StringArray::from(attributes_json)),
                Arc::new(StringArray::from(resource_json)),
                Arc::new(StringArray::from(scope_json)),
                Arc::new(UInt32Array::from(null_u32)),
                Arc::new(StringArray::from(service_names)),
                Arc::new(StringArray::from(null_strings)),
            ],
        )
        .unwrap()
    }

    fn attr_batch(
        resource: Vec<Option<&str>>,
        scope: Vec<Option<&str>>,
        record: Vec<Option<&str>>,
    ) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("resource_json", DataType::Utf8, true),
            Field::new("scope_json", DataType::Utf8, true),
            Field::new("attributes_json", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(resource)),
                Arc::new(StringArray::from(scope)),
                Arc::new(StringArray::from(record)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn materialized_label_columns_extract_with_precedence() {
        // Row 1's `http.method` is in both resource and record → resource wins.
        let batch = attr_batch(
            vec![
                Some(r#"{"namespace":"prod"}"#),
                Some(r#"{"namespace":"staging","http.method":"PUT"}"#),
            ],
            vec![Some(r#"{"attributes":{"scopekey":"sv"}}"#), None],
            vec![
                Some(r#"{"http.method":"GET"}"#),
                Some(r#"{"http.method":"POST"}"#),
            ],
        );
        let labels = vec![
            "namespace".to_string(),
            "http.method".to_string(),
            "scopekey".to_string(),
        ];
        let (fields, cols) = materialized_label_columns(&batch, 2, &labels).unwrap();

        assert_eq!(fields.len(), 3);
        assert!(
            fields
                .iter()
                .all(|f| f.is_nullable() && *f.data_type() == DataType::Utf8)
        );

        // Columns are looked up by name rather than position: resolution
        // order is the labels' canonical (sorted) order, not their
        // configured order (#1448), which this test must not assume.
        let val = |name: &str, i: usize| {
            let idx = fields.iter().position(|f| f.name() == name).unwrap();
            let a = cols[idx].as_any().downcast_ref::<StringArray>().unwrap();
            if a.is_null(i) {
                None
            } else {
                Some(a.value(i).to_string())
            }
        };
        // namespace: resource on both rows.
        assert_eq!(val("label_namespace", 0).as_deref(), Some("prod"));
        assert_eq!(val("label_namespace", 1).as_deref(), Some("staging"));
        // http.method: row 0 only in record (GET); row 1 resource wins over record (PUT).
        assert_eq!(val("label_http_method", 0).as_deref(), Some("GET"));
        assert_eq!(val("label_http_method", 1).as_deref(), Some("PUT"));
        // scopekey: row 0 from scope; row 1 absent.
        assert_eq!(val("label_scopekey", 0).as_deref(), Some("sv"));
        assert_eq!(val("label_scopekey", 1), None);

        // No configured labels → no extra columns.
        let (f, c) = materialized_label_columns(&batch, 2, &[]).unwrap();
        assert!(f.is_empty() && c.is_empty());
    }

    #[test]
    fn materialized_label_columns_gives_colliding_keys_distinct_columns() {
        // `http.method` and `http_method` sanitize to the same candidate
        // column name; both must be materialized in distinct columns, and
        // neither key's values may be dropped (#1448).
        let batch = attr_batch(
            vec![None],
            vec![None],
            vec![Some(r#"{"http.method":"GET","http_method":"POST"}"#)],
        );
        let labels = vec!["http.method".to_string(), "http_method".to_string()];
        let (fields, cols) = materialized_label_columns(&batch, 1, &labels).unwrap();

        let names: Vec<String> = fields.iter().map(|f| f.name().clone()).collect();
        assert_eq!(names.len(), 2, "expected one column per key, got {names:?}");
        assert!(names.contains(&"label_http_method".to_string()));
        assert!(names.contains(&"label_http_method_2".to_string()));

        let val = |name: &str| {
            let idx = names.iter().position(|n| n == name).unwrap();
            cols[idx]
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0)
                .to_string()
        };
        assert_eq!(val("label_http_method"), "GET");
        assert_eq!(val("label_http_method_2"), "POST");
    }

    #[test]
    fn materialized_label_columns_handles_a_three_way_collision() {
        // Three distinct keys sanitizing to the same candidate name each
        // get their own column; none of their values are dropped (#1448).
        let batch = attr_batch(
            vec![None],
            vec![None],
            vec![Some(
                r#"{"http.method":"a","http_method":"b","http-method":"c"}"#,
            )],
        );
        let labels = vec![
            "http_method".to_string(),
            "http.method".to_string(),
            "http-method".to_string(),
        ];
        let (fields, cols) = materialized_label_columns(&batch, 1, &labels).unwrap();

        let names: Vec<String> = fields.iter().map(|f| f.name().clone()).collect();
        assert_eq!(names.len(), 3, "expected one column per key, got {names:?}");
        let val = |name: &str| {
            let idx = names.iter().position(|n| n == name).unwrap();
            cols[idx]
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0)
                .to_string()
        };
        // Canonical order is sorted by key string ('-' < '.' < '_' in
        // ASCII), so `http-method` claims the unsuffixed candidate.
        assert_eq!(val("label_http_method"), "c");
        assert_eq!(val("label_http_method_2"), "a");
        assert_eq!(val("label_http_method_3"), "b");
    }

    #[test]
    fn schema_creation_and_writer_resolution_agree_on_the_same_configured_list() {
        // The literal #1448 acceptance criterion: schema creation
        // (`ResolvedSchema::to_iceberg_schema_with_labels`, backing table
        // creation) and the write path's resolver
        // (`resolve_label_columns_fresh`, which `materialized_label_columns`
        // calls) must assign the same physical column to the same
        // configured *key* for the same list -- not merely the same *set*
        // of column names to some permutation of keys, which a
        // same-columns-different-keys bug would also pass.
        //
        // The writer side is fed the list in reversed order: agreement must
        // hold regardless of which order each call site happens to see the
        // same configured set in, not just when both sort it identically.
        use common::iceberg::evolution::{origin_key_of, resolve_label_columns_fresh};
        use common::schema::schema_parser::ResolvedField;

        let labels = vec![
            "namespace".to_string(),
            "http.method".to_string(),
            "http_method".to_string(),
        ];
        let reversed: Vec<String> = labels.iter().rev().cloned().collect();

        let base = ResolvedSchema {
            version: "test-only".to_string(),
            description: "fixture".to_string(),
            fields: vec![ResolvedField {
                name: "timestamp".to_string(),
                field_type: "timestamp_ns".to_string(),
                required: true,
                computed: None,
                physical_only: false,
                field_id: 1,
            }],
            partition_by: vec![],
        };
        let schema = base.to_iceberg_schema_with_labels(&labels).unwrap();
        let schema_mapping: std::collections::HashMap<String, String> = schema
            .fields()
            .iter()
            .filter_map(|f| {
                origin_key_of(f.doc.as_deref()).map(|key| (key.to_string(), f.name.clone()))
            })
            .collect();

        let writer_mapping: std::collections::HashMap<String, String> =
            resolve_label_columns_fresh(&reversed).into_iter().collect();

        assert_eq!(
            schema_mapping, writer_mapping,
            "schema creation and the writer must resolve the same configured key to \
             the same physical column, regardless of each side's input order"
        );

        // And `materialized_label_columns` -- the writer's actual call
        // site -- really does use this mapping, not a different one that
        // happens to produce the same column *names*.
        let batch = attr_batch(
            vec![None],
            vec![None],
            vec![Some(
                r#"{"namespace":"n","http.method":"a","http_method":"b"}"#,
            )],
        );
        let (fields, cols) = materialized_label_columns(&batch, 1, &reversed).unwrap();
        for (key, expected_value) in [
            ("namespace", "n"),
            ("http.method", "a"),
            ("http_method", "b"),
        ] {
            let column = &writer_mapping[key];
            let idx = fields.iter().position(|f| f.name() == column).unwrap();
            let value = cols[idx]
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0);
            assert_eq!(
                value, expected_value,
                "key {key} routed to the wrong column"
            );
        }
    }

    #[test]
    fn log_transform_resource_identity_groups_records_by_resource() {
        let ts: u64 = 1_700_000_000_000_000_000;
        let batch = make_log_flight_batch_with_attrs(
            &[ts, ts, ts],
            &[ts, ts, ts],
            vec![
                Some(r#"{"service.name":"checkout"}"#),
                Some(r#"{"service.name":"checkout"}"#),
                Some(r#"{"service.name":"billing"}"#),
            ],
            vec![None, None, None],
            vec![None, None, None],
        );

        let result = transform_logs_v1_to_iceberg(batch, &[]).unwrap();
        let identities = result
            .column(result.schema().index_of("resource_identity").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            identities.value(0),
            identities.value(1),
            "first two records share the checkout resource"
        );
        assert_ne!(
            identities.value(0),
            identities.value(2),
            "third record has a different resource"
        );
        assert_eq!(
            identities.value(0),
            resource_identity_from_json(r#"{"service.name":"checkout"}"#).unwrap()
        );
    }

    #[test]
    fn log_transform_null_resource_json_yields_null_resource_identity() {
        let ts: u64 = 1_700_000_000_000_000_000;
        let batch =
            make_log_flight_batch_with_attrs(&[ts], &[ts], vec![None], vec![None], vec![None]);

        let result = transform_logs_v1_to_iceberg(batch, &[]).unwrap();
        let identities = result
            .column(result.schema().index_of("resource_identity").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(identities.is_null(0));
    }

    #[test]
    fn log_transform_uses_time_unix_nano_when_nonzero() {
        let real_ts: u64 = 1_700_000_000_000_000_000;
        let observed_ts: u64 = 1_700_000_001_000_000_000;
        let batch = make_log_flight_batch(&[real_ts], &[observed_ts]);

        let result = transform_logs_v1_to_iceberg(batch, &[]).unwrap();
        let ts_col = result
            .column_by_name("timestamp")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();

        assert_eq!(ts_col.value(0), real_ts as i64);
    }

    #[test]
    fn log_transform_falls_back_to_observed_when_time_is_zero() {
        let observed_ts: u64 = 1_700_000_001_000_000_000;
        let batch = make_log_flight_batch(&[0], &[observed_ts]);

        let result = transform_logs_v1_to_iceberg(batch, &[]).unwrap();
        let ts_col = result
            .column_by_name("timestamp")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();

        assert_eq!(
            ts_col.value(0),
            observed_ts as i64,
            "should fall back to observed_time_unix_nano when time_unix_nano is 0"
        );
    }

    #[test]
    fn log_transform_date_day_uses_fallback_timestamp() {
        let observed_ts: u64 = 1_700_000_001_000_000_000;
        let batch = make_log_flight_batch(&[0], &[observed_ts]);

        let result = transform_logs_v1_to_iceberg(batch, &[]).unwrap();
        let date_col = result
            .column_by_name("date_day")
            .unwrap()
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();

        let epoch_1970 = 0_i32;
        assert_ne!(
            date_col.value(0),
            epoch_1970,
            "date_day should not be 1970-01-01 when observed_time_unix_nano is set"
        );
    }

    #[test]
    fn log_transform_mixed_timestamps() {
        let real_ts: u64 = 1_700_000_000_000_000_000;
        let observed_ts: u64 = 1_700_000_002_000_000_000;

        let batch = make_log_flight_batch(
            &[real_ts, 0, real_ts],
            &[observed_ts, observed_ts, observed_ts],
        );

        let result = transform_logs_v1_to_iceberg(batch, &[]).unwrap();
        let ts_col = result
            .column_by_name("timestamp")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();

        assert_eq!(ts_col.value(0), real_ts as i64, "row 0: use time_unix_nano");
        assert_eq!(
            ts_col.value(1),
            observed_ts as i64,
            "row 1: fall back to observed"
        );
        assert_eq!(ts_col.value(2), real_ts as i64, "row 2: use time_unix_nano");
    }

    #[test]
    fn log_transform_preserves_event_name_and_dropped_attributes_count() {
        use common::flight::conversion::otlp_logs_to_arrow;
        use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
        use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
        use opentelemetry_proto::tonic::resource::v1::Resource;

        let record = LogRecord {
            time_unix_nano: 1_700_000_000_000_000_000,
            event_name: "x".to_string(),
            dropped_attributes_count: 3,
            ..Default::default()
        };
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource::default()),
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![record],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let wire_batch = otlp_logs_to_arrow(&request).expect("conversion should succeed");
        let result =
            transform_logs_v1_to_iceberg(wire_batch, &[]).expect("transform should succeed");

        let event_name = result
            .column_by_name("event_name")
            .expect("event_name column should be present")
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(event_name.value(0), "x");

        let dropped_attributes_count = result
            .column_by_name("dropped_attributes_count")
            .expect("dropped_attributes_count column should be present")
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(dropped_attributes_count.value(0), 3);
    }

    fn metrics_v1_batch_mixed_types() -> RecordBatch {
        use datafusion::arrow::array::{BooleanArray, Int32Array};

        // (name, metric_type, data_json, description, unit)
        let rows: [(&str, &str, &str, &str, &str); 5] = [
            (
                "requests.gauge",
                "gauge",
                r#"[{"time_unix_nano":1,"value":1.5,"attributes":{"host":"a"}},{"time_unix_nano":2,"value":1.7,"attributes":{"host":"a"}}]"#,
                "gauge desc",
                "1",
            ),
            (
                "requests.sum",
                "sum",
                r#"[{"time_unix_nano":1,"value":2.5,"attributes":{"host":"a"}}]"#,
                "sum desc",
                "ms",
            ),
            (
                "requests.duration",
                "histogram",
                r#"[{"time_unix_nano":1,"count":3,"sum":6.0,"min":1.0,"max":3.0,"bucket_counts":[1,2,0],"explicit_bounds":[1.0,"+Inf"],"attributes":{"host":"a"},"exemplars":[{"time_unix_nano":15,"value":2.0,"trace_id":"0102030405060708090a0b0c0d0e0f10","span_id":"0102030405060708","filtered_attributes":{"scope":"dbg"}}]}]"#,
                "histogram desc",
                "ms",
            ),
            (
                "requests.duration.exp",
                "exponential_histogram",
                r#"[{"time_unix_nano":1,"count":3,"sum":6.0,"scale":2,"zero_count":1,"positive":{"offset":0,"bucket_counts":[1,2]},"negative":{"offset":1,"bucket_counts":[0,1]},"attributes":{"host":"a"}}]"#,
                "exp desc",
                "ms",
            ),
            (
                "requests.summary",
                "summary",
                r#"[{"time_unix_nano":1,"count":5,"sum":10.0,"quantile_values":[{"quantile":0.5,"value":2.0},{"quantile":0.9,"value":4.0}],"attributes":{"host":"a"}}]"#,
                "summary desc",
                "1",
            ),
        ];
        let n = rows.len();
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("description", DataType::Utf8, true),
            Field::new("unit", DataType::Utf8, true),
            Field::new("start_time_unix_nano", DataType::UInt64, true),
            Field::new("time_unix_nano", DataType::UInt64, false),
            Field::new("attributes_json", DataType::Utf8, true),
            Field::new("resource_json", DataType::Utf8, true),
            Field::new("scope_json", DataType::Utf8, true),
            Field::new("metric_type", DataType::Utf8, false),
            Field::new("data_json", DataType::Utf8, false),
            Field::new("aggregation_temporality", DataType::Int32, true),
            Field::new("is_monotonic", DataType::Boolean, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.3).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.4).collect::<Vec<_>>(),
                )),
                Arc::new(UInt64Array::from(vec![Some(0u64); n])),
                Arc::new(UInt64Array::from(vec![0u64; n])),
                Arc::new(StringArray::from(vec![Some("{}"); n])),
                Arc::new(StringArray::from(vec![
                    Some(r#"{"service.name":"svc"}"#);
                    n
                ])),
                Arc::new(StringArray::from(vec![
                    Some(r#"{"name":"hostmetrics"}"#);
                    n
                ])),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )),
                Arc::new(Int32Array::from(vec![Some(2); n])),
                Arc::new(BooleanArray::from(vec![Some(true); n])),
            ],
        )
        .unwrap()
    }

    /// Every row of `batch` whose `metric_type` column equals `want`.
    fn wide_rows(batch: &RecordBatch, want: &str) -> Vec<usize> {
        let types = get_typed_column::<StringArray>(batch, "metric_type").unwrap();
        (0..batch.num_rows())
            .filter(|&row| types.value(row) == want)
            .collect()
    }

    fn wide_row(batch: &RecordBatch, want: &str) -> usize {
        *wide_rows(batch, want)
            .first()
            .unwrap_or_else(|| panic!("no row for metric_type {want}"))
    }

    /// `batch`'s `name` list column at `row`, as a native `Vec`.
    fn list_at<T: ArrowPrimitiveType>(
        batch: &RecordBatch,
        name: &str,
        row: usize,
    ) -> Vec<T::Native> {
        let list = get_typed_column::<ListArray>(batch, name)
            .unwrap()
            .value(row);
        list.as_any()
            .downcast_ref::<datafusion::arrow::array::PrimitiveArray<T>>()
            .unwrap()
            .values()
            .to_vec()
    }

    #[test]
    fn transform_metrics_to_wide_fans_mixed_types_into_one_row_each() {
        let batch = metrics_v1_batch_mixed_types();
        let wide = transform_metrics_to_wide(batch, &[]).expect("transform");
        assert_eq!(
            wide.num_rows(),
            6,
            "2 gauge points + 1 each of 4 other types"
        );

        let gauge_rows = wide_rows(&wide, "gauge");
        assert_eq!(gauge_rows.len(), 2);
        let series_ids = get_typed_column::<StringArray>(&wide, "series_id").unwrap();
        assert_eq!(
            series_ids.value(gauge_rows[0]),
            series_ids.value(gauge_rows[1])
        );

        let temporality = get_typed_column::<Int32Array>(&wide, "aggregation_temporality").unwrap();
        let monotonic = get_typed_column::<BooleanArray>(&wide, "is_monotonic").unwrap();
        for (metric_type, want_temporality, want_monotonic) in [
            ("gauge", None, None),
            ("sum", Some(2), Some(true)),
            ("histogram", Some(2), None),
            ("exponential_histogram", Some(2), None),
            ("summary", None, None),
        ] {
            let row = wide_row(&wide, metric_type);
            assert_eq!(
                int32_value(temporality, row),
                want_temporality,
                "{metric_type} temporality"
            );
            assert_eq!(
                bool_value(monotonic, row),
                want_monotonic,
                "{metric_type} monotonic"
            );
        }

        let values = get_typed_column::<Float64Array>(&wide, "value").unwrap();
        assert_eq!(values.value(wide_row(&wide, "gauge")), 1.5);
        assert_eq!(values.value(wide_row(&wide, "sum")), 2.5);

        let descriptions = get_typed_column::<StringArray>(&wide, "metric_description").unwrap();
        let units = get_typed_column::<StringArray>(&wide, "metric_unit").unwrap();
        let sum_row = wide_row(&wide, "sum");
        assert_eq!(descriptions.value(sum_row), "sum desc");
        assert_eq!(units.value(sum_row), "ms");

        let row = wide_row(&wide, "histogram");
        assert_eq!(
            list_at::<Float64Type>(&wide, "explicit_bounds", row),
            [1.0, f64::INFINITY]
        );
        assert_eq!(list_at::<Int64Type>(&wide, "bucket_counts", row), [1, 2, 0]);

        // exponential_histogram: scale plus positive/negative bucket lists.
        let row = wide_row(&wide, "exponential_histogram");
        assert_eq!(
            get_typed_column::<Int32Array>(&wide, "scale")
                .unwrap()
                .value(row),
            2
        );
        assert_eq!(
            list_at::<Int64Type>(&wide, "positive_bucket_counts", row),
            [1, 2]
        );

        // summary: count/sum plus parallel quantile lists.
        let row = wide_row(&wide, "summary");
        assert_eq!(
            get_typed_column::<Int64Array>(&wide, "count")
                .unwrap()
                .value(row),
            5
        );
        assert_eq!(list_at::<Float64Type>(&wide, "quantiles", row), [0.5, 0.9]);
        assert_eq!(
            list_at::<Float64Type>(&wide, "quantile_values", row),
            [2.0, 4.0]
        );
    }

    #[test]
    fn transform_metric_exemplars_extracts_one_row_per_exemplar_with_the_owning_series_id() {
        let batch = metrics_v1_batch_mixed_types();
        let wide = transform_metrics_to_wide(batch.clone(), &[]).expect("wide transform");
        let exemplars = transform_metric_exemplars(batch).expect("exemplars transform");
        assert_eq!(exemplars.num_rows(), 1);

        assert_eq!(
            get_typed_column::<StringArray>(&exemplars, "trace_id")
                .unwrap()
                .value(0),
            "0102030405060708090a0b0c0d0e0f10"
        );
        assert_eq!(
            get_typed_column::<StringArray>(&exemplars, "span_id")
                .unwrap()
                .value(0),
            "0102030405060708"
        );

        let histogram_row = wide_row(&wide, "histogram");
        assert_eq!(
            get_typed_column::<StringArray>(&exemplars, "series_id")
                .unwrap()
                .value(0),
            get_typed_column::<StringArray>(&wide, "series_id")
                .unwrap()
                .value(histogram_row)
        );
    }

    #[test]
    fn transform_metric_exemplars_with_no_exemplars_is_zero_rows() {
        let batch = metrics_v1_batch(Some(r#"{"service.name":"x"}"#));
        let exemplars = transform_metric_exemplars(batch).expect("transform");
        assert_eq!(exemplars.num_rows(), 0);
    }
}

/// Every non-computed field `schemas.toml` declares for a table must have a
/// real read/write path in this module's transform for that table --
/// `unified-table-schema`'s `table-schema-consistency` capability. Computed
/// fields (`date_day`/`hour`/traces' `timestamp`) are populated by a fixed
/// recipe keyed on the field's `computed` tag rather than a 1:1 source
/// column, so they're excluded here and covered by their own dedicated
/// tests instead.
///
/// `transform_trace_v1_to_v2`/`transform_logs_v1_to_iceberg`/
/// `transform_profiles_v1_to_iceberg` already self-check this at runtime
/// (an unhandled field hits an `Unknown field in ... schema` error, since
/// each iterates its schema's own field list) -- these tests give that
/// property a fast, explicit, named failure instead of relying on it
/// surfacing through some other test. The five metrics tables have no such
/// runtime check: `transform_metrics_*_v1_to_iceberg` builds its output
/// columns positionally against its own hand-written
/// `create_metrics_*_arrow_schema()`, entirely independent of
/// `SCHEMA_DEFINITIONS` -- a field added to `schemas.toml` there would
/// silently never be populated until the resulting Iceberg write bounced
/// off a missing-required-column error, or worse, went unnoticed if the
/// new field was nullable. These tests catch that at PR/CI time instead.
#[cfg(test)]
mod schema_consistency {
    use super::*;
    use std::collections::HashSet;

    fn assert_covers_non_computed_fields(table: &str, resolved: &ResolvedSchema, touched: &[&str]) {
        let declared: HashSet<&str> = resolved
            .fields
            .iter()
            .filter(|f| f.computed.is_none())
            .map(|f| f.name.as_str())
            .collect();
        let touched: HashSet<&str> = touched.iter().copied().collect();
        assert_eq!(
            declared, touched,
            "{table}: schemas.toml's non-computed fields and this module's \
             known touched-field set have diverged -- a field was added to \
             or removed from schemas.toml without updating the matching \
             transform function (or vice versa)"
        );
    }

    /// The "touched" set for a metrics transform, straight from its own
    /// `create_metrics_*_arrow_schema()` rather than a hand-typed list --
    /// the schema literal and the test can no longer drift apart. `date_day`
    /// and `hour` are computed (see the module doc comment above), so they
    /// carry no `schemas.toml` entry and are excluded here the same way the
    /// traces/logs/profiles lists already exclude their computed fields.
    fn metrics_arrow_touched_fields(schema: &Schema) -> Vec<&str> {
        schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .filter(|name| *name != "date_day" && *name != "hour")
            .collect()
    }

    #[test]
    fn traces_transform_covers_every_non_computed_physical_v4_field() {
        let resolved = SCHEMA_DEFINITIONS
            .resolve_trace_schema("physical-v4")
            .unwrap();
        assert_covers_non_computed_fields(
            "traces",
            &resolved,
            &[
                "trace_id",
                "span_id",
                "parent_span_id",
                "service_name",
                "span_kind",
                "status_code",
                "status_message",
                "is_root",
                "span_kind_number",
                "status_code_number",
                "dropped_attributes_count",
                "dropped_events_count",
                "dropped_links_count",
                "start_time_unix_nano",
                "end_time_unix_nano",
                "events",
                "links",
                "span_name",
                "duration_nanos",
                "span_attributes",
                "resource_attributes",
                "resource_identity",
                "trace_state",
                "resource_schema_url",
                "scope_name",
                "scope_version",
                "scope_schema_url",
                "scope_attributes",
            ],
        );
    }

    #[test]
    fn logs_transform_covers_every_non_computed_physical_v3_field() {
        let resolved = SCHEMA_DEFINITIONS
            .resolve_log_schema("physical-v3")
            .unwrap();
        assert_covers_non_computed_fields(
            "logs",
            &resolved,
            &[
                "timestamp",
                "observed_timestamp",
                "trace_id",
                "span_id",
                "trace_flags",
                "severity_text",
                "severity_number",
                "service_name",
                "body",
                "resource_schema_url",
                "resource_attributes",
                "resource_identity",
                "scope_schema_url",
                "scope_name",
                "scope_version",
                "scope_attributes",
                "log_attributes",
                "event_name",
                "dropped_attributes_count",
            ],
        );
    }

    #[test]
    fn profiles_transform_covers_every_non_computed_physical_v2_field() {
        let resolved = SCHEMA_DEFINITIONS
            .resolve_table_schema(&SCHEMA_DEFINITIONS.profiles, "physical-v2")
            .unwrap();
        assert_covers_non_computed_fields(
            "profiles",
            &resolved,
            &[
                "profile_id",
                "timestamp",
                "duration_nano",
                "sample_type",
                "sample_unit",
                "period_type",
                "period_unit",
                "period",
                "service_name",
                "stacktraces_json",
                "samples_json",
                "resource_attributes",
                "scope_attributes",
                "profile_attributes",
                "trace_id",
                "span_id",
                "resource_identity",
            ],
        );
    }

    #[test]
    fn metrics_gauge_transform_covers_every_non_computed_physical_v2_field() {
        let resolved = SCHEMA_DEFINITIONS
            .resolve_table_schema(&SCHEMA_DEFINITIONS.metrics_gauge, "physical-v2")
            .unwrap();
        let schema = create_metrics_gauge_arrow_schema();
        let touched = metrics_arrow_touched_fields(&schema);
        assert_covers_non_computed_fields("metrics_gauge", &resolved, &touched);
    }

    #[test]
    fn metrics_sum_transform_covers_every_non_computed_physical_v2_field() {
        let resolved = SCHEMA_DEFINITIONS
            .resolve_table_schema(&SCHEMA_DEFINITIONS.metrics_sum, "physical-v2")
            .unwrap();
        let schema = create_metrics_sum_arrow_schema();
        let touched = metrics_arrow_touched_fields(&schema);
        assert_covers_non_computed_fields("metrics_sum", &resolved, &touched);
    }

    #[test]
    fn metrics_histogram_transform_covers_every_non_computed_physical_v2_field() {
        let resolved = SCHEMA_DEFINITIONS
            .resolve_table_schema(&SCHEMA_DEFINITIONS.metrics_histogram, "physical-v2")
            .unwrap();
        let schema = create_metrics_histogram_arrow_schema();
        let touched = metrics_arrow_touched_fields(&schema);
        assert_covers_non_computed_fields("metrics_histogram", &resolved, &touched);
    }

    #[test]
    fn metrics_exponential_histogram_transform_covers_every_non_computed_physical_v2_field() {
        let resolved = SCHEMA_DEFINITIONS
            .resolve_table_schema(
                &SCHEMA_DEFINITIONS.metrics_exponential_histogram,
                "physical-v2",
            )
            .unwrap();
        let schema = create_metrics_exponential_histogram_arrow_schema();
        let touched = metrics_arrow_touched_fields(&schema);
        assert_covers_non_computed_fields("metrics_exponential_histogram", &resolved, &touched);
    }

    #[test]
    fn metrics_summary_transform_covers_every_non_computed_physical_v2_field() {
        let resolved = SCHEMA_DEFINITIONS
            .resolve_table_schema(&SCHEMA_DEFINITIONS.metrics_summary, "physical-v2")
            .unwrap();
        let schema = create_metrics_summary_arrow_schema();
        let touched = metrics_arrow_touched_fields(&schema);
        assert_covers_non_computed_fields("metrics_summary", &resolved, &touched);
    }

    #[test]
    #[should_panic(expected = "diverged")]
    fn metrics_gauge_transform_flags_an_arrow_field_missing_from_schemas_toml() {
        // Simulate a column added to create_metrics_gauge_arrow_schema()
        // without a matching schemas.toml entry -- the scenario this
        // derivation exists to catch.
        let resolved = SCHEMA_DEFINITIONS
            .resolve_table_schema(&SCHEMA_DEFINITIONS.metrics_gauge, "physical-v2")
            .unwrap();
        let mut fields: Vec<Field> = create_metrics_gauge_arrow_schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        fields.push(Field::new("undeclared_field", DataType::Utf8, true));
        let drifted = Schema::new(fields);
        let touched = metrics_arrow_touched_fields(&drifted);
        assert_covers_non_computed_fields("metrics_gauge", &resolved, &touched);
    }
}
