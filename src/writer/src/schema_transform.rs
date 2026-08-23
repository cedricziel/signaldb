use anyhow::{Result, anyhow};
use chrono::{DateTime, Datelike, Timelike};
use common::flight::conversion::UNKNOWN_SERVICE_NAME;
use common::schema::schema_parser::ResolvedSchema;
use common::schema::{ATTR_TOKENS_COLUMN, SCHEMA_DEFINITIONS, materialized_column_name};
use datafusion::arrow::{
    array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Float64Array, Int32Array,
        Int64Array, ListArray, StringArray, StructArray, TimestampNanosecondArray, UInt32Array,
        UInt64Array,
    },
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use std::sync::Arc;

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

    Ok(FlightMetadata {
        schema_version,
        signal_type,
        target_table,
        tenant_id,
        dataset_id,
        traceparent,
        tracestate,
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

/// Resolves the compiled plan for the current traces schema version.
fn build_trace_v1_to_v2_plan() -> Result<TraceV1ToV2Plan> {
    let v2_schema =
        SCHEMA_DEFINITIONS.resolve_trace_schema(SCHEMA_DEFINITIONS.current_trace_version())?;
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
/// non-null). Duplicate labels collapse to a single column.
fn label_columns_from_maps(
    resource: &[Option<AttrMap>],
    scope: &[Option<AttrMap>],
    record: &[Option<AttrMap>],
    num_rows: usize,
    labels: &[String],
) -> (Vec<Field>, Vec<ArrayRef>) {
    let mut fields = Vec::new();
    let mut columns: Vec<ArrayRef> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for label in labels {
        let name = materialized_column_name(label);
        if !seen.insert(name.clone()) {
            continue; // duplicate label → single column
        }
        let values: Vec<Option<String>> = (0..num_rows)
            .map(|i| {
                for src in [resource.get(i), scope.get(i), record.get(i)] {
                    if let Some(Some(map)) = src
                        && let Some(v) = map.get(label)
                        && let Some(s) = attr_value_to_string(v)
                    {
                        return Some(s);
                    }
                }
                None
            })
            .collect();
        fields.push(Field::new(&name, DataType::Utf8, true));
        columns.push(Arc::new(StringArray::from(values)));
    }
    (fields, columns)
}

/// Parse a JSON-string column into per-row attribute objects the way the
/// attribute *columns* are extracted: prefer the nested `attributes` object
/// when present, else treat the root object as the attributes (matching the
/// `resource_attributes` column's fallback).
fn parse_attr_objects_with_root_fallback(
    batch: &RecordBatch,
    column: &str,
) -> Result<Vec<Option<AttrMap>>> {
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
            match root.get("attributes") {
                Some(serde_json::Value::Object(m)) => Some(m.clone()),
                _ => match root {
                    serde_json::Value::Object(m) => Some(m),
                    _ => None,
                },
            }
        })
        .collect())
}

/// Build the derived `attr_tokens` `List<Utf8>` column: one `key=value`
/// token per attribute across resource, scope, and record scopes,
/// deduplicated per row. Rows always get a (possibly empty) list, never
/// null, so an ANDed containment predicate cannot null out rows the
/// attribute-column predicate matched.
///
/// The sources mirror the attribute-column extractions exactly — the
/// querier ANDs `array_has(attr_tokens, 'key=value')` onto predicates over
/// `log_attributes`/`resource_attributes`, so the tokens must cover at
/// least everything those columns contain.
fn attr_tokens_column(batch: &RecordBatch, num_rows: usize) -> Result<(Field, ArrayRef)> {
    use datafusion::arrow::array::{ListBuilder, StringBuilder};

    let resource = parse_attr_objects_with_root_fallback(batch, "resource_json")?;
    let scope = parse_attr_objects(batch, "scope_json", Some("attributes"))?;
    let record = parse_attr_objects(batch, "attributes_json", None)?;

    let mut builder = ListBuilder::new(StringBuilder::new());
    for i in 0..num_rows {
        let mut seen = std::collections::HashSet::new();
        for src in [resource.get(i), scope.get(i), record.get(i)] {
            let Some(Some(map)) = src else { continue };
            for (key, value) in map {
                if let Some(s) = attr_value_to_string(value) {
                    let token = format!("{key}={s}");
                    if !seen.contains(token.as_str()) {
                        builder.values().append_value(&token);
                        seen.insert(token);
                    }
                }
            }
        }
        builder.append(true);
    }
    let array = builder.finish();
    let field = Field::new(ATTR_TOKENS_COLUMN, array.data_type().clone(), true);
    Ok((field, Arc::new(array)))
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
    let v1_schema = SCHEMA_DEFINITIONS.resolve_log_schema("physical-v1")?;
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

    // Derived `key=value` token column for bloom-filtered containment
    // checks; also dropped by coercion on tables that predate it.
    let (token_field, token_column) = attr_tokens_column(&batch, num_rows)?;
    let out_schema = extend_schema_with_labels(
        out_schema,
        vec![token_field],
        &mut new_columns,
        vec![token_column],
    );

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
    ]))
}

fn create_metrics_sum_arrow_schema() -> Arc<Schema> {
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
        };
    };

    let serde_json::Value::Object(obj) = parsed else {
        return ResourceContext {
            service_name: unknown(),
            resource_schema_url: None,
            resource_attributes: Some(resource_json.to_string()),
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

pub fn transform_metrics_gauge_v1_to_iceberg(
    batch: RecordBatch,
    labels: &[String],
) -> Result<RecordBatch> {
    let output_schema = create_metrics_gauge_arrow_schema();

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
    let mut values: Vec<Option<f64>> = Vec::new();
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
            values.push(Some(point_value(&point)));
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
        Arc::new(Float64Array::from(values)),
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
    ];
    let out_schema =
        extend_schema_with_labels(output_schema, label_fields, &mut columns, label_columns);
    RecordBatch::try_new(out_schema, columns).map_err(|e| {
        anyhow!(
            "Failed to create transformed metrics_gauge RecordBatch: {}",
            e
        )
    })
}

pub fn transform_metrics_sum_v1_to_iceberg(
    batch: RecordBatch,
    labels: &[String],
) -> Result<RecordBatch> {
    let output_schema = create_metrics_sum_arrow_schema();

    let name_array = get_typed_column::<StringArray>(&batch, "name")?;
    let description_array = get_typed_column::<StringArray>(&batch, "description")?;
    let unit_array = get_typed_column::<StringArray>(&batch, "unit")?;
    let resource_json_array = get_typed_column::<StringArray>(&batch, "resource_json")?;
    let scope_json_array = get_typed_column::<StringArray>(&batch, "scope_json")?;
    let data_json_array = get_typed_column::<StringArray>(&batch, "data_json")?;
    let aggregation_temporality_array =
        get_typed_column::<Int32Array>(&batch, "aggregation_temporality")?;
    let is_monotonic_array = get_typed_column::<BooleanArray>(&batch, "is_monotonic")?;

    let mut timestamps: Vec<Option<i64>> = Vec::new();
    let mut start_timestamps: Vec<Option<i64>> = Vec::new();
    let mut service_names: Vec<Option<String>> = Vec::new();
    let mut metric_names: Vec<Option<String>> = Vec::new();
    let mut metric_descriptions: Vec<Option<String>> = Vec::new();
    let mut metric_units: Vec<Option<String>> = Vec::new();
    let mut values: Vec<Option<f64>> = Vec::new();
    let mut flags: Vec<Option<i32>> = Vec::new();
    let mut aggregation_temporalities: Vec<Option<i32>> = Vec::new();
    let mut is_monotonics: Vec<Option<bool>> = Vec::new();
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

    for row in 0..batch.num_rows() {
        let metric_name = string_value(name_array, row);
        let metric_description = string_value(description_array, row);
        let metric_unit = string_value(unit_array, row);
        let aggregation_temporality = int32_value(aggregation_temporality_array, row);
        let is_monotonic = bool_value(is_monotonic_array, row);

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
            values.push(Some(point_value(&point)));
            flags.push(json_to_i32(point.get("flags")));
            aggregation_temporalities.push(aggregation_temporality);
            is_monotonics.push(is_monotonic);
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
        Arc::new(Float64Array::from(values)),
        Arc::new(Int32Array::from(flags)),
        Arc::new(Int32Array::from(aggregation_temporalities)),
        Arc::new(BooleanArray::from(is_monotonics)),
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
    ];
    let out_schema =
        extend_schema_with_labels(output_schema, label_fields, &mut columns, label_columns);
    RecordBatch::try_new(out_schema, columns).map_err(|e| {
        anyhow!(
            "Failed to create transformed metrics_sum RecordBatch: {}",
            e
        )
    })
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
        (Some("metrics"), Some("metrics_gauge")) => {
            transform_metrics_gauge_v1_to_iceberg(batch, &m.metrics)
        }
        (Some("metrics"), Some("metrics_sum")) => {
            transform_metrics_sum_v1_to_iceberg(batch, &m.metrics)
        }
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
    fn transform_trace_v1_to_v2_produces_the_complete_expected_physical_v3_batch() {
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
            "full physical-v3 schema (names, order, types, nullability) must match exactly"
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

    /// The v1 wire format spells NaN/±Inf out as strings (see
    /// `common::flight::conversion::f64_to_json`); the transforms must read
    /// them back into the non-nullable Float64 columns instead of leaving a
    /// null that makes `RecordBatch::try_new` reject the whole batch — the
    /// failure that pinned hive's metrics WAL (#1061). A point with no value
    /// at all is stored as NaN for the same reason: one bad point must never
    /// discard the batch.
    #[test]
    fn non_finite_and_missing_metric_values_do_not_poison_the_batch() {
        use datafusion::arrow::array::Float64Array;
        fn values_of(batch: &RecordBatch, col: &str) -> Vec<f64> {
            batch
                .column_by_name(col)
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .iter()
                .map(|v| v.expect("no nulls in a non-nullable value column"))
                .collect()
        }
        let points = r#"[
            {"time_unix_nano":1700000001000000000,"start_time_unix_nano":1700000000000000000,"value":"NaN","attributes":{}},
            {"time_unix_nano":1700000002000000000,"start_time_unix_nano":1700000000000000000,"value":"+Inf","attributes":{}},
            {"time_unix_nano":1700000003000000000,"start_time_unix_nano":1700000000000000000,"value":"-Inf","attributes":{}},
            {"time_unix_nano":1700000004000000000,"start_time_unix_nano":1700000000000000000,"value":null,"attributes":{}},
            {"time_unix_nano":1700000005000000000,"start_time_unix_nano":1700000000000000000,"attributes":{}},
            {"time_unix_nano":1700000006000000000,"start_time_unix_nano":1700000000000000000,"value":2.5,"attributes":{}}
        ]"#;
        let batch = metrics_v1_batch_with_points(Some(r#"{"service.name":"x"}"#), points);
        let gauge = transform_metrics_gauge_v1_to_iceberg(batch.clone(), &[]).expect("gauge");
        let v = values_of(&gauge, "value");
        assert_eq!(v.len(), 6);
        assert!(v[0].is_nan());
        assert_eq!(v[1], f64::INFINITY);
        assert_eq!(v[2], f64::NEG_INFINITY);
        assert!(v[3].is_nan(), "null value → NaN, not a rejected batch");
        assert!(v[4].is_nan(), "absent value → NaN, not a rejected batch");
        assert_eq!(v[5], 2.5);
        let sum = transform_metrics_sum_v1_to_iceberg(batch, &[]).expect("sum");
        let v = values_of(&sum, "value");
        assert!(v[0].is_nan() && v[3].is_nan() && v[4].is_nan());
        assert_eq!(v[1], f64::INFINITY);
    }

    /// OTLP allows a resource without `service.name` (an OTel Collector
    /// hostmetrics pipeline without a resource processor is the classic
    /// case), and the traces path already stores such producers as
    /// `unknown`. Metrics must not be dead-lettered for it: the transform
    /// fills the non-nullable `service_name` column with the same fallback.
    #[test]
    fn metrics_without_service_name_get_the_unknown_service_fallback() {
        for resource_json in [
            Some(r#"{"host.name":"hive","os.type":"linux"}"#),
            Some(
                r#"{"attributes":{"host.name":"hive"},"schema_url":"https://opentelemetry.io/schemas/1.43.0"}"#,
            ),
            Some("{}"),
            None,
        ] {
            let gauge = transform_metrics_gauge_v1_to_iceberg(metrics_v1_batch(resource_json), &[])
                .unwrap_or_else(|e| panic!("gauge with resource {resource_json:?}: {e}"));
            let names = gauge
                .column_by_name("service_name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(
                names.value(0),
                common::flight::conversion::UNKNOWN_SERVICE_NAME
            );
            assert!(
                !gauge
                    .schema()
                    .field_with_name("service_name")
                    .unwrap()
                    .is_nullable()
            );

            let sum = transform_metrics_sum_v1_to_iceberg(metrics_v1_batch(resource_json), &[])
                .unwrap_or_else(|e| panic!("sum with resource {resource_json:?}: {e}"));
            let names = sum
                .column_by_name("service_name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(
                names.value(0),
                common::flight::conversion::UNKNOWN_SERVICE_NAME
            );
        }

        // A present service.name still wins.
        let gauge = transform_metrics_gauge_v1_to_iceberg(
            metrics_v1_batch(Some(r#"{"service.name":"checkout"}"#)),
            &[],
        )
        .unwrap();
        let names = gauge
            .column_by_name("service_name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "checkout");
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
        assert_eq!(fields[0].name(), "label_namespace");
        assert!(
            fields
                .iter()
                .all(|f| f.is_nullable() && *f.data_type() == DataType::Utf8)
        );

        let val = |c: &ArrayRef, i: usize| {
            let a = c.as_any().downcast_ref::<StringArray>().unwrap();
            if a.is_null(i) {
                None
            } else {
                Some(a.value(i).to_string())
            }
        };
        // namespace: resource on both rows.
        assert_eq!(val(&cols[0], 0).as_deref(), Some("prod"));
        assert_eq!(val(&cols[0], 1).as_deref(), Some("staging"));
        // http.method: row 0 only in record (GET); row 1 resource wins over record (PUT).
        assert_eq!(val(&cols[1], 0).as_deref(), Some("GET"));
        assert_eq!(val(&cols[1], 1).as_deref(), Some("PUT"));
        // scopekey: row 0 from scope; row 1 absent.
        assert_eq!(val(&cols[2], 0).as_deref(), Some("sv"));
        assert_eq!(val(&cols[2], 1), None);

        // No configured labels → no extra columns.
        let (f, c) = materialized_label_columns(&batch, 2, &[]).unwrap();
        assert!(f.is_empty() && c.is_empty());
    }

    /// One row's tokens, sorted for stable comparison.
    fn tokens_of(batch: &RecordBatch, row: usize) -> Vec<String> {
        let list = batch
            .column_by_name("attr_tokens")
            .expect("attr_tokens column present")
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("attr_tokens is a ListArray");
        assert!(!list.is_null(row), "attr_tokens must never be null");
        let values = list.value(row);
        let strings = values.as_any().downcast_ref::<StringArray>().unwrap();
        let mut out: Vec<String> = (0..strings.len())
            .map(|i| strings.value(i).to_string())
            .collect();
        out.sort();
        out
    }

    #[test]
    fn log_transform_emits_attr_tokens_from_all_scopes() {
        let ts: u64 = 1_700_000_000_000_000_000;
        let batch = make_log_flight_batch_with_attrs(
            &[ts, ts],
            &[ts, ts],
            vec![
                // Flat root object (legacy shape).
                Some(r#"{"namespace":"prod"}"#),
                // Nested shape: tokens must come from `attributes`, not the
                // envelope keys — mirroring the resource_attributes column.
                Some(r#"{"attributes":{"env":"staging"},"schema_url":"http://x"}"#),
            ],
            vec![Some(r#"{"attributes":{"lib":"otel"}}"#), None],
            vec![
                // Non-string values serialize like the attribute columns do.
                Some(r#"{"http.method":"GET","retries":2}"#),
                Some(r#"{"http.method":"POST"}"#),
            ],
        );

        let result = transform_logs_v1_to_iceberg(batch, &[]).unwrap();
        assert_eq!(
            tokens_of(&result, 0),
            vec!["http.method=GET", "lib=otel", "namespace=prod", "retries=2"],
        );
        assert_eq!(
            tokens_of(&result, 1),
            vec!["env=staging", "http.method=POST"]
        );
    }

    #[test]
    fn log_transform_attr_tokens_dedupes_and_defaults_to_empty() {
        let ts: u64 = 1_700_000_000_000_000_000;
        let batch = make_log_flight_batch_with_attrs(
            &[ts, ts],
            &[ts, ts],
            vec![Some(r#"{"env":"prod"}"#), None],
            vec![None, None],
            // Row 0 repeats env=prod in the record scope → single token.
            vec![Some(r#"{"env":"prod"}"#), None],
        );

        let result = transform_logs_v1_to_iceberg(batch, &[]).unwrap();
        assert_eq!(tokens_of(&result, 0), vec!["env=prod"]);
        // No attributes anywhere → empty list, not null.
        assert_eq!(tokens_of(&result, 1), Vec::<String>::new());
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
    fn traces_transform_covers_every_non_computed_physical_v3_field() {
        let resolved = SCHEMA_DEFINITIONS
            .resolve_trace_schema(SCHEMA_DEFINITIONS.current_trace_version())
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
    fn logs_transform_covers_every_non_computed_physical_v1_field() {
        let resolved = SCHEMA_DEFINITIONS
            .resolve_log_schema(&SCHEMA_DEFINITIONS.metadata.current_log_version)
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
                "scope_schema_url",
                "scope_name",
                "scope_version",
                "scope_attributes",
                "log_attributes",
            ],
        );
    }

    #[test]
    fn profiles_transform_covers_every_non_computed_physical_v1_field() {
        let resolved = SCHEMA_DEFINITIONS
            .resolve_table_schema(&SCHEMA_DEFINITIONS.profiles, "physical-v1")
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
            ],
        );
    }

    #[test]
    fn metrics_gauge_transform_covers_every_non_computed_physical_v1_field() {
        let resolved = SCHEMA_DEFINITIONS
            .resolve_table_schema(&SCHEMA_DEFINITIONS.metrics_gauge, "physical-v1")
            .unwrap();
        let schema = create_metrics_gauge_arrow_schema();
        let touched = metrics_arrow_touched_fields(&schema);
        assert_covers_non_computed_fields("metrics_gauge", &resolved, &touched);
    }

    #[test]
    fn metrics_sum_transform_covers_every_non_computed_physical_v1_field() {
        let resolved = SCHEMA_DEFINITIONS
            .resolve_table_schema(&SCHEMA_DEFINITIONS.metrics_sum, "physical-v1")
            .unwrap();
        let schema = create_metrics_sum_arrow_schema();
        let touched = metrics_arrow_touched_fields(&schema);
        assert_covers_non_computed_fields("metrics_sum", &resolved, &touched);
    }

    #[test]
    fn metrics_histogram_transform_covers_every_non_computed_physical_v1_field() {
        let resolved = SCHEMA_DEFINITIONS
            .resolve_table_schema(&SCHEMA_DEFINITIONS.metrics_histogram, "physical-v1")
            .unwrap();
        let schema = create_metrics_histogram_arrow_schema();
        let touched = metrics_arrow_touched_fields(&schema);
        assert_covers_non_computed_fields("metrics_histogram", &resolved, &touched);
    }

    #[test]
    fn metrics_exponential_histogram_transform_covers_every_non_computed_physical_v1_field() {
        let resolved = SCHEMA_DEFINITIONS
            .resolve_table_schema(
                &SCHEMA_DEFINITIONS.metrics_exponential_histogram,
                "physical-v1",
            )
            .unwrap();
        let schema = create_metrics_exponential_histogram_arrow_schema();
        let touched = metrics_arrow_touched_fields(&schema);
        assert_covers_non_computed_fields("metrics_exponential_histogram", &resolved, &touched);
    }

    #[test]
    fn metrics_summary_transform_covers_every_non_computed_physical_v1_field() {
        let resolved = SCHEMA_DEFINITIONS
            .resolve_table_schema(&SCHEMA_DEFINITIONS.metrics_summary, "physical-v1")
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
            .resolve_table_schema(&SCHEMA_DEFINITIONS.metrics_gauge, "physical-v1")
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
