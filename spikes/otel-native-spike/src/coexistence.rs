//! Task 0.2 — typed-layout viability + field-id promotion evolution.
//!
//! SignalDB is switching to the typed attribute layout in one breaking step
//! (no legacy `Map<String,String>` coexistence, no migration read-path). What
//! still has to be de-risked against the pinned `datafusion_iceberg` provider
//! (fork rev `98b8f88`, DataFusion 54.1) is:
//!
//! 1. **Typed layout viability.** A table whose attributes live in per-type
//!    maps — `attributes_str: Map<String,Utf8>`, `attributes_int:
//!    Map<String,Int64>`, `attributes_double: Map<String,Float64>`,
//!    `attributes_bool: Map<String,Boolean>` — plus a binary residue column
//!    `attributes_residue: Map<String,Binary>` (CBOR-encoded off-type
//!    occurrences). Do those columns round-trip through the fork's Arrow
//!    conversion, is map access reachable from SQL and/or expr level, does the
//!    Binary residue survive, and how do typed-map predicates behave.
//!
//! 2. **Field-id evolution for PROMOTION under one scan** (design D4/D5,
//!    task 6.2). On that typed table, add a promoted primitive column
//!    `attr_http_response_status_code: Int64` via Iceberg field-id evolution
//!    (`AddSchema` + `SetCurrentSchema`, the same commit path
//!    `common::iceberg::evolution` uses). Data files written BEFORE promotion
//!    lack the column; files written AFTER carry it. One DataFusion scan must
//!    read both — old files null-fill the new column, and (crucially) the
//!    provider must project by **field-id**, not name/position, so a pre-
//!    promotion file's existing typed columns still read correctly after the
//!    column count changed. Demotion (dropping the column) is probed too.
//!
//! Residue encoding choice: `Map<String,Binary>` (not a single `Binary`
//! column). The residue is a per-attribute tail — a handful of keys whose
//! value on a given row did not match the column's home type — so keying it by
//! attribute name preserves *which* key was off-type and admits several at
//! once, while a lone `Binary` blob would need its own internal framing. The
//! value bytes are CBOR (compact, self-describing, no schema registry).
//!
//! Throwaway evidence: every probe records PASS/FAIL with a note instead of
//! aborting, so one broken capability still lets the rest run.

use std::sync::Arc;

use anyhow::{Context, Result};
use datafusion::arrow::array::{
    Array, ArrayRef, BinaryBuilder, BooleanBuilder, Float64Builder, Int64Array, Int64Builder,
    MapBuilder, MapFieldNames, RecordBatch, StringArray, StringBuilder, TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, SchemaRef as ArrowSchemaRef};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::SessionContext;
use iceberg_rust::catalog::Catalog;
use iceberg_rust::catalog::commit::{CommitTable, TableUpdate};
use iceberg_rust::catalog::create::CreateTableBuilder;
use iceberg_rust::catalog::identifier::Identifier;
use iceberg_rust::catalog::namespace::Namespace;
use iceberg_rust::catalog::tabular::Tabular;
use iceberg_rust::spec::partition::PartitionSpec;
use iceberg_rust::spec::schema::Schema;
use iceberg_rust::spec::types::{MapType, PrimitiveType, StructField, StructType, Type};
use iceberg_rust::table::Table;

/// The promoted column added by field-id evolution.
const PROMOTED: &str = "attr_http_response_status_code";

/// One probe outcome. `pass` drives the demo's PASS/FAIL print; `note`
/// carries the observed behavior for the findings doc.
pub struct Probe {
    pub name: &'static str,
    pub pass: bool,
    pub note: String,
}

impl Probe {
    fn ok(name: &'static str, note: impl Into<String>) -> Self {
        Self {
            name,
            pass: true,
            note: note.into(),
        }
    }
    fn fail(name: &'static str, note: impl Into<String>) -> Self {
        Self {
            name,
            pass: false,
            note: note.into(),
        }
    }
}

/// The whole spike result: the probes plus a rendered scan table for eyeballing.
pub struct Report {
    pub probes: Vec<Probe>,
    pub rendered_scan: String,
}

// ---------------------------------------------------------------------------
// Schema construction
// ---------------------------------------------------------------------------

fn required(id: i32, name: &str, prim: PrimitiveType) -> StructField {
    StructField {
        id,
        name: name.to_string(),
        required: true,
        field_type: Type::Primitive(prim),
        doc: None,
        initial_default: None,
        write_default: None,
    }
}

fn optional(id: i32, name: &str, prim: PrimitiveType) -> StructField {
    StructField {
        id,
        name: name.to_string(),
        required: false,
        field_type: Type::Primitive(prim),
        doc: None,
        initial_default: None,
        write_default: None,
    }
}

/// An optional `Map<String, value>` field. `field_id` is the map field's own
/// id; the nested key/value ids follow it (`field_id+1`, `field_id+2`), which
/// is exactly how `common`'s `mapify_attr_fields`/`schema_parser` allocate.
fn optional_map(field_id: i32, name: &str, value: PrimitiveType) -> StructField {
    StructField {
        id: field_id,
        name: name.to_string(),
        required: false,
        field_type: Type::Map(MapType {
            key_id: field_id + 1,
            key: Box::new(Type::Primitive(PrimitiveType::String)),
            value_id: field_id + 2,
            value_required: false,
            value: Box::new(Type::Primitive(value)),
        }),
        doc: None,
        initial_default: None,
        write_default: None,
    }
}

/// The typed attribute layout SignalDB is moving to: id/timestamp subset plus
/// per-type attribute maps and a binary residue map. Each map consumes three
/// field ids (field + key + value), allocated contiguously so nested ids never
/// collide (the `common` convention). `last_column_id` ends at 18.
fn typed_schema() -> Schema {
    let fields = vec![
        required(1, "timestamp", PrimitiveType::Timestamp),
        required(2, "service_name", PrimitiveType::String),
        optional(3, "span_name", PrimitiveType::String),
        optional_map(4, "attributes_str", PrimitiveType::String), // 4,5,6
        optional_map(7, "attributes_int", PrimitiveType::Long),   // 7,8,9
        optional_map(10, "attributes_double", PrimitiveType::Double), // 10,11,12
        optional_map(13, "attributes_bool", PrimitiveType::Boolean), // 13,14,15
        optional_map(16, "attributes_residue", PrimitiveType::Binary), // 16,17,18
    ];
    Schema::from_struct_type(StructType::new(fields), 0, None)
}

/// The highest field id anywhere in the schema tree (top-level + nested
/// key/value/element ids). Copied from `common::iceberg::evolution::max_field_id`
/// so the spike allocates promotion ids the same way production does.
fn max_field_id(schema: &Schema) -> i32 {
    fn walk(t: &Type, max: &mut i32) {
        match t {
            Type::Primitive(_) => {}
            Type::Struct(fields) => {
                for f in fields.iter() {
                    *max = (*max).max(f.id);
                    walk(&f.field_type, max);
                }
            }
            Type::List(l) => {
                *max = (*max).max(l.element_id);
                walk(&l.element, max);
            }
            Type::Map(m) => {
                *max = (*max).max(m.key_id).max(m.value_id);
                walk(&m.key, max);
                walk(&m.value, max);
            }
        }
    }
    let mut max = 0;
    for f in schema.fields().iter() {
        max = max.max(f.id);
        walk(&f.field_type, &mut max);
    }
    max
}

/// Promotion via Iceberg field-id evolution: add ONE optional primitive
/// `Int64` column through the same `AddSchema` + `SetCurrentSchema` commit
/// `common::iceberg::evolution::add_label_columns` uses. Returns the assigned
/// field id (which must continue past the existing tree, not restart).
async fn evolve_add_promoted(
    catalog: Arc<dyn Catalog>,
    identifier: &Identifier,
    column: &str,
) -> Result<i32> {
    let table = load_table(catalog.clone(), identifier).await?;
    let current = table
        .current_schema()
        .map_err(|e| anyhow::anyhow!("current schema: {e}"))?;
    let metadata = table.metadata();

    let mut fields: Vec<StructField> = current.fields().iter().cloned().collect();
    let new_id = max_field_id(current).max(metadata.last_column_id) + 1;
    fields.push(optional(new_id, column, PrimitiveType::Long));

    let new_schema_id = next_schema_id(metadata, current);
    let evolved = Schema::from_struct_type(StructType::new(fields), new_schema_id, None);

    catalog
        .clone()
        .update_table(CommitTable {
            identifier: identifier.clone(),
            requirements: vec![],
            updates: vec![
                TableUpdate::AddSchema {
                    schema: evolved,
                    last_column_id: Some(new_id),
                },
                TableUpdate::SetCurrentSchema {
                    schema_id: new_schema_id,
                },
            ],
        })
        .await
        .map_err(|e| anyhow::anyhow!("promotion commit: {e}"))?;

    // Post-commit verification (SQL catalog CAS can silently lose a race).
    let reloaded = load_table(catalog, identifier).await?;
    let verified = reloaded
        .current_schema()
        .map_err(|e| anyhow::anyhow!("current schema after promotion: {e}"))?;
    anyhow::ensure!(
        verified.fields().iter().any(|f| f.name == column),
        "promotion did not take effect: {column} missing"
    );
    Ok(new_id)
}

/// Demotion: drop the promoted column via `AddSchema` (pruned) +
/// `SetCurrentSchema`, mirroring `evolution::remove_label_columns`. The dropped
/// field id is NOT reused (`last_column_id` untouched). Files still on disk
/// that physically carry the column are simply no longer projected.
async fn evolve_drop_promoted(
    catalog: Arc<dyn Catalog>,
    identifier: &Identifier,
    column: &str,
) -> Result<()> {
    let table = load_table(catalog.clone(), identifier).await?;
    let current = table
        .current_schema()
        .map_err(|e| anyhow::anyhow!("current schema: {e}"))?;
    let metadata = table.metadata();

    let fields: Vec<StructField> = current
        .fields()
        .iter()
        .filter(|f| f.name != column)
        .cloned()
        .collect();
    let new_schema_id = next_schema_id(metadata, current);
    let pruned = Schema::from_struct_type(StructType::new(fields), new_schema_id, None);

    catalog
        .clone()
        .update_table(CommitTable {
            identifier: identifier.clone(),
            requirements: vec![],
            updates: vec![
                // last_column_id: None — never hand the dropped id out again.
                TableUpdate::AddSchema {
                    schema: pruned,
                    last_column_id: None,
                },
                TableUpdate::SetCurrentSchema {
                    schema_id: new_schema_id,
                },
            ],
        })
        .await
        .map_err(|e| anyhow::anyhow!("demotion commit: {e}"))?;

    let reloaded = load_table(catalog, identifier).await?;
    let verified = reloaded
        .current_schema()
        .map_err(|e| anyhow::anyhow!("current schema after demotion: {e}"))?;
    anyhow::ensure!(
        !verified.fields().iter().any(|f| f.name == column),
        "demotion did not take effect: {column} still present"
    );
    Ok(())
}

fn next_schema_id(
    metadata: &iceberg_rust::spec::table_metadata::TableMetadata,
    current: &Schema,
) -> i32 {
    metadata
        .schemas
        .keys()
        .max()
        .copied()
        .unwrap_or(*current.schema_id())
        + 1
}

async fn load_table(catalog: Arc<dyn Catalog>, identifier: &Identifier) -> Result<Table> {
    match catalog
        .load_tabular(identifier)
        .await
        .map_err(|e| anyhow::anyhow!("load {identifier}: {e}"))?
    {
        Tabular::Table(t) => Ok(t),
        _ => Err(anyhow::anyhow!("expected table, got view: {identifier}")),
    }
}

// ---------------------------------------------------------------------------
// Array / batch construction
// ---------------------------------------------------------------------------

/// Derive `(entry, key, value)` field names from a target Arrow `Map` field so
/// the arrays we build match the provider's expected map layout exactly
/// (mirrors `writer::storage::iceberg::json_strings_to_map_array`).
fn map_names(field: &Field) -> MapFieldNames {
    if let DataType::Map(entry, _) = field.data_type()
        && let DataType::Struct(kv) = entry.data_type()
    {
        return MapFieldNames {
            entry: entry.name().clone(),
            key: kv[0].name().clone(),
            value: kv[1].name().clone(),
        };
    }
    MapFieldNames {
        entry: "key_value".into(),
        key: "key".into(),
        value: "value".into(),
    }
}

/// Cast a freshly-built array onto the target field's exact datatype when the
/// builder's layout differs (nullability/name nuances), matching the writer.
fn fit(built: ArrayRef, field: &Field) -> Result<ArrayRef> {
    if built.data_type() == field.data_type() {
        Ok(built)
    } else {
        datafusion::arrow::compute::cast(&built, field.data_type())
            .with_context(|| format!("cast {} to {:?}", field.name(), field.data_type()))
    }
}

fn field_of<'a>(schema: &'a ArrowSchemaRef, name: &str) -> &'a Field {
    schema.field_with_name(name).expect("target field present")
}

fn str_map(
    schema: &ArrowSchemaRef,
    name: &str,
    rows: &[Option<Vec<(&str, &str)>>],
) -> Result<ArrayRef> {
    let field = field_of(schema, name);
    let mut b = MapBuilder::new(
        Some(map_names(field)),
        StringBuilder::new(),
        StringBuilder::new(),
    );
    for row in rows {
        match row {
            Some(kvs) => {
                for (k, v) in kvs {
                    b.keys().append_value(k);
                    b.values().append_value(v);
                }
                b.append(true)?;
            }
            None => b.append(false)?,
        }
    }
    fit(Arc::new(b.finish()), field)
}

fn int_map(
    schema: &ArrowSchemaRef,
    name: &str,
    rows: &[Option<Vec<(&str, i64)>>],
) -> Result<ArrayRef> {
    let field = field_of(schema, name);
    let mut b = MapBuilder::new(
        Some(map_names(field)),
        StringBuilder::new(),
        Int64Builder::new(),
    );
    for row in rows {
        match row {
            Some(kvs) => {
                for (k, v) in kvs {
                    b.keys().append_value(k);
                    b.values().append_value(*v);
                }
                b.append(true)?;
            }
            None => b.append(false)?,
        }
    }
    fit(Arc::new(b.finish()), field)
}

fn double_map(
    schema: &ArrowSchemaRef,
    name: &str,
    rows: &[Option<Vec<(&str, f64)>>],
) -> Result<ArrayRef> {
    let field = field_of(schema, name);
    let mut b = MapBuilder::new(
        Some(map_names(field)),
        StringBuilder::new(),
        Float64Builder::new(),
    );
    for row in rows {
        match row {
            Some(kvs) => {
                for (k, v) in kvs {
                    b.keys().append_value(k);
                    b.values().append_value(*v);
                }
                b.append(true)?;
            }
            None => b.append(false)?,
        }
    }
    fit(Arc::new(b.finish()), field)
}

fn bool_map(
    schema: &ArrowSchemaRef,
    name: &str,
    rows: &[Option<Vec<(&str, bool)>>],
) -> Result<ArrayRef> {
    let field = field_of(schema, name);
    let mut b = MapBuilder::new(
        Some(map_names(field)),
        StringBuilder::new(),
        BooleanBuilder::new(),
    );
    for row in rows {
        match row {
            Some(kvs) => {
                for (k, v) in kvs {
                    b.keys().append_value(k);
                    b.values().append_value(*v);
                }
                b.append(true)?;
            }
            None => b.append(false)?,
        }
    }
    fit(Arc::new(b.finish()), field)
}

/// Build a `Map<String,Binary>` residue array (CBOR-encoded off-type values).
fn binary_map(
    schema: &ArrowSchemaRef,
    name: &str,
    rows: &[Option<Vec<(&str, Vec<u8>)>>],
) -> Result<ArrayRef> {
    let field = field_of(schema, name);
    let mut b = MapBuilder::new(
        Some(map_names(field)),
        StringBuilder::new(),
        BinaryBuilder::new(),
    );
    for row in rows {
        match row {
            Some(kvs) => {
                for (k, v) in kvs {
                    b.keys().append_value(k);
                    b.values().append_value(v);
                }
                b.append(true)?;
            }
            None => b.append(false)?,
        }
    }
    fit(Arc::new(b.finish()), field)
}

/// The Arrow schema the provider reads against — derived from an Iceberg
/// schema's fields (carries `PARQUET:field_id` metadata that drives field-id
/// projection). Identical to what `write_parquet_partitioned` derives, so
/// batches built against it write cleanly.
fn arrow_schema_of(schema: &Schema) -> Result<ArrowSchemaRef> {
    let arrow: datafusion::arrow::datatypes::Schema = schema
        .fields()
        .try_into()
        .map_err(|e: iceberg_rust::spec::error::Error| anyhow::anyhow!("iceberg->arrow: {e}"))?;
    Ok(Arc::new(arrow))
}

/// CBOR-encode an off-type residue value `{ "type": ..., "value": ... }`.
fn cbor(type_tag: &str, value: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    let doc = serde_json::json!({ "type": type_tag, "value": value });
    ciborium::into_writer(&doc, &mut buf).expect("cbor encode");
    buf
}

fn ts(values: &[i64]) -> ArrayRef {
    Arc::new(TimestampMicrosecondArray::from(values.to_vec()))
}

fn utf8(values: &[Option<&str>]) -> ArrayRef {
    Arc::new(StringArray::from(values.to_vec()))
}

fn i64opt(values: &[Option<i64>]) -> ArrayRef {
    Arc::new(Int64Array::from(values.to_vec()))
}

/// Write a batch and commit it as one Iceberg snapshot, mirroring
/// `writer::storage::iceberg`'s append path.
async fn write_generation(table: &mut Table, batch: RecordBatch) -> Result<()> {
    let stream = futures::stream::iter(vec![Ok(batch)]);
    let files = iceberg_rust::arrow::write::write_parquet_partitioned(table, stream, None)
        .await
        .map_err(|e| anyhow::anyhow!("write_parquet_partitioned: {e}"))?;
    anyhow::ensure!(!files.is_empty(), "no data files produced");
    table
        .new_transaction(None)
        .append_data(files)
        .commit()
        .await
        .map_err(|e| anyhow::anyhow!("commit append: {e}"))?;
    Ok(())
}

/// Register a fresh `DataFusionTable` over the current catalog state as `events`.
async fn fresh_ctx(catalog: Arc<dyn Catalog>, identifier: &Identifier) -> Result<SessionContext> {
    let table = load_table(catalog, identifier).await?;
    let provider = Arc::new(datafusion_iceberg::DataFusionTable::new(
        Tabular::Table(table),
        None,
        None,
        None,
    ));
    let ctx = SessionContext::new();
    ctx.register_table("events", provider)
        .context("register_table")?;
    Ok(ctx)
}

// ---------------------------------------------------------------------------
// Orchestration
// ---------------------------------------------------------------------------

/// Run the full spike against a temp SQLite catalog + local-filesystem object
/// store. Returns a [`Report`] of probe outcomes.
pub async fn run() -> Result<Report> {
    let mut probes = Vec::new();

    // --- catalog + object store (tempfile sqlite + local filesystem) ---------
    // SPIKE_KEEP_TMP=1 leaks the dir and prints its path, for post-mortem
    // inspection of the written parquet.
    let tmp = tempfile::tempdir().context("tempdir")?;
    let root = tmp.path().to_path_buf();
    if std::env::var("SPIKE_KEEP_TMP").is_ok() {
        eprintln!("SPIKE_KEEP_TMP: files kept at {}", root.display());
        std::mem::forget(tmp);
    }
    let data_dir = root.join("data");
    std::fs::create_dir_all(&data_dir)?;
    let catalog_db = root.join("catalog.db");

    let config = common::config::Configuration {
        schema: common::config::SchemaConfig {
            catalog_type: "sql".to_string(),
            catalog_uri: format!("sqlite://{}", catalog_db.display()),
            default_schemas: Default::default(),
            materialized_labels: Default::default(),
        },
        storage: common::config::StorageConfig {
            dsn: format!("file://{}", data_dir.display()),
        },
        ..Default::default()
    };
    let catalog = common::iceberg::create_catalog_with_config(&config)
        .await
        .context("create catalog")?;

    // --- create the table directly with the TYPED layout --------------------
    let namespace = Namespace::try_new(&["spike".to_string(), "coexistence".to_string()])
        .map_err(|e| anyhow::anyhow!("namespace: {e}"))?;
    let _ = catalog.clone().create_namespace(&namespace, None).await;
    let identifier = Identifier::new(&["spike".to_string(), "coexistence".to_string()], "events");

    let create = CreateTableBuilder::default()
        .with_name("events".to_string())
        .with_schema(typed_schema())
        .with_partition_spec(PartitionSpec::default())
        .with_location("spike/coexistence/events".to_string())
        .create()
        .map_err(|e| anyhow::anyhow!("create builder: {e}"))?;
    catalog
        .clone()
        .create_table(identifier.clone(), create)
        .await
        .map_err(|e| anyhow::anyhow!("create_table: {e}"))?;

    // --- generation 1: typed layout, PRE-promotion --------------------------
    // status_code lives in attributes_int as a native Int64. One row also
    // carries an off-type occurrence (the string "OK") in the residue map.
    let mut table = load_table(catalog.clone(), &identifier).await?;
    let typed_arrow = arrow_schema_of(table.current_schema().unwrap())?;
    let gen1 = RecordBatch::try_new(
        typed_arrow.clone(),
        vec![
            ts(&[1_000_000, 2_000_000]),
            utf8(&[Some("gen1-svc"), Some("gen1-svc")]),
            utf8(&[Some("GET /a"), Some("GET /b")]),
            str_map(
                &typed_arrow,
                "attributes_str",
                &[
                    Some(vec![("http.route", "/a")]),
                    Some(vec![("http.route", "/b")]),
                ],
            )?,
            int_map(
                &typed_arrow,
                "attributes_int",
                &[
                    Some(vec![("http.response.status_code", 200)]),
                    Some(vec![("http.response.status_code", 500)]),
                ],
            )?,
            double_map(
                &typed_arrow,
                "attributes_double",
                &[
                    Some(vec![("duration_ms", 12.5)]),
                    Some(vec![("duration_ms", 3.0)]),
                ],
            )?,
            bool_map(
                &typed_arrow,
                "attributes_bool",
                &[
                    Some(vec![("cache.hit", true)]),
                    Some(vec![("cache.hit", false)]),
                ],
            )?,
            binary_map(
                &typed_arrow,
                "attributes_residue",
                &[
                    None,
                    Some(vec![("http.response.status_code", cbor("string", "OK"))]),
                ],
            )?,
        ],
    )
    .context("build gen1 batch")?;
    write_generation(&mut table, gen1).await?;
    probes.push(Probe::ok(
        "typed_layout_write",
        "per-type maps + Map<String,Binary> residue written & committed (gen-1, pre-promotion)",
    ));

    // --- typed-layout read probes (one scan, gen-1 only so far) -------------
    {
        let ctx = fresh_ctx(catalog.clone(), &identifier).await?;
        probe_typed_roundtrip(&ctx, &mut probes).await;
        probe_map_access(&ctx, &mut probes).await;
        probe_typed_predicate(&ctx, &mut probes).await;
        probe_residue(&ctx, &mut probes).await;
    }

    // --- PROMOTION via field-id evolution -----------------------------------
    let typed_max = max_field_id(table.current_schema().unwrap());
    let promoted_id = match evolve_add_promoted(catalog.clone(), &identifier, PROMOTED).await {
        Ok(id) => id,
        Err(e) => {
            probes.push(Probe::fail(
                "promotion_evolution",
                format!("AddSchema/SetCurrentSchema failed: {e}"),
            ));
            let ctx = fresh_ctx(catalog.clone(), &identifier).await?;
            let rendered = render_scan(&ctx).await;
            return Ok(Report {
                probes,
                rendered_scan: rendered,
            });
        }
    };
    let id_ok = promoted_id > typed_max;
    probes.push(Probe {
        name: "promotion_evolution",
        pass: id_ok,
        note: format!(
            "AddSchema+SetCurrentSchema added `{PROMOTED}: Int64`; typed tree max id={typed_max}, \
             assigned field id={promoted_id} ({}) — id continues past the tree (evolution, not create-time max+1)",
            if id_ok { "OK" } else { "MISPLACED" }
        ),
    });

    // --- generation 2: typed layout, POST-promotion -------------------------
    // Carries the promoted column populated AND the typed maps. Written after
    // the schema change, so these parquet files physically contain field id 19.
    let mut table = load_table(catalog.clone(), &identifier).await?;
    let evolved_arrow = arrow_schema_of(table.current_schema().unwrap())?;
    let gen2 = RecordBatch::try_new(
        evolved_arrow.clone(),
        vec![
            ts(&[3_000_000, 4_000_000]),
            utf8(&[Some("gen2-svc"), Some("gen2-svc")]),
            utf8(&[Some("GET /c"), Some("POST /d")]),
            str_map(
                &evolved_arrow,
                "attributes_str",
                &[
                    Some(vec![("http.route", "/c")]),
                    Some(vec![("http.route", "/d")]),
                ],
            )?,
            int_map(
                &evolved_arrow,
                "attributes_int",
                &[
                    Some(vec![("http.response.status_code", 200)]),
                    Some(vec![("http.response.status_code", 404)]),
                ],
            )?,
            double_map(
                &evolved_arrow,
                "attributes_double",
                &[
                    Some(vec![("duration_ms", 7.0)]),
                    Some(vec![("duration_ms", 9.0)]),
                ],
            )?,
            bool_map(
                &evolved_arrow,
                "attributes_bool",
                &[
                    Some(vec![("cache.hit", true)]),
                    Some(vec![("cache.hit", true)]),
                ],
            )?,
            binary_map(&evolved_arrow, "attributes_residue", &[None, None])?,
            // the promoted column, populated for gen-2 rows
            i64opt(&[Some(200), Some(404)]),
        ],
    )
    .context("build gen2 batch")?;
    write_generation(&mut table, gen2).await?;
    probes.push(Probe::ok(
        "post_promotion_write",
        "typed gen-2 files written carrying the promoted Int64 column",
    ));

    // --- ONE scan over BOTH generations -------------------------------------
    let ctx = fresh_ctx(catalog.clone(), &identifier).await?;
    let rendered_scan = render_scan(&ctx).await;
    probe_promotion_null_fill(&ctx, &mut probes).await;
    probe_field_id_not_positional(&ctx, &mut probes).await;
    probe_promoted_predicate(&ctx, &mut probes).await;

    // --- DEMOTION via field-id evolution ------------------------------------
    match evolve_drop_promoted(catalog.clone(), &identifier, PROMOTED).await {
        Ok(()) => {
            // Re-scan: the column is gone from the schema, but both file
            // generations (including gen-2 files that physically carry it) must
            // still read without error.
            let ctx = fresh_ctx(catalog.clone(), &identifier).await?;
            match ctx
                .sql("SELECT service_name, attributes_int FROM events")
                .await
            {
                Ok(df) => match df.collect().await {
                    Ok(batches) => {
                        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                        let col_gone = ctx
                            .sql(&format!("SELECT {PROMOTED} FROM events"))
                            .await
                            .is_err();
                        probes.push(Probe {
                            name: "demotion_evolution",
                            pass: rows == 4 && col_gone,
                            note: format!(
                                "dropped `{PROMOTED}` via AddSchema(pruned)+SetCurrentSchema; \
                                 re-scan returned {rows} rows across both generations, promoted \
                                 column no longer projectable ({}) — files carrying the field \
                                 read fine without it",
                                if col_gone { "gone" } else { "STILL PRESENT" }
                            ),
                        });
                    }
                    Err(e) => probes.push(Probe::fail(
                        "demotion_evolution",
                        format!("post-demotion scan collect: {e}"),
                    )),
                },
                Err(e) => probes.push(Probe::fail(
                    "demotion_evolution",
                    format!("post-demotion scan plan: {e}"),
                )),
            }
        }
        Err(e) => probes.push(Probe::fail(
            "demotion_evolution",
            format!("drop-column commit failed: {e}"),
        )),
    }

    Ok(Report {
        probes,
        rendered_scan,
    })
}

async fn render_scan(ctx: &SessionContext) -> String {
    match ctx
        .sql("SELECT service_name, span_name, attributes_int, attr_http_response_status_code, attributes_residue FROM events ORDER BY timestamp")
        .await
    {
        Ok(df) => match df.collect().await {
            Ok(b) => pretty_format_batches(&b).map(|d| d.to_string()).unwrap_or_default(),
            Err(e) => format!("<scan collect failed: {e}>"),
        },
        Err(e) => format!("<scan plan failed: {e}>"),
    }
}

/// Try each candidate SQL, return the first that both plans and collects.
async fn first_working<'a>(
    ctx: &SessionContext,
    candidates: &[&'a str],
) -> std::result::Result<(&'a str, Vec<RecordBatch>), String> {
    let mut errs = String::new();
    for sql in candidates {
        match ctx.sql(sql).await {
            Ok(df) => match df.collect().await {
                Ok(b) => return Ok((sql, b)),
                Err(e) => errs.push_str(&format!("\n  [{sql}] collect: {e}")),
            },
            Err(e) => errs.push_str(&format!("\n  [{sql}] plan: {e}")),
        }
    }
    Err(errs)
}

/// Do all four typed maps survive the provider's Arrow conversion in a plain
/// scan (columns come back as Map arrays, no error)?
async fn probe_typed_roundtrip(ctx: &SessionContext, probes: &mut Vec<Probe>) {
    let sql = "SELECT attributes_str, attributes_int, attributes_double, attributes_bool, attributes_residue FROM events";
    match first_working(ctx, &[sql]).await {
        Ok((_, batches)) => {
            let schema = batches.first().map(|b| b.schema());
            let all_maps = schema
                .as_ref()
                .map(|s| {
                    [
                        "attributes_str",
                        "attributes_int",
                        "attributes_double",
                        "attributes_bool",
                        "attributes_residue",
                    ]
                    .iter()
                    .all(|n| {
                        matches!(
                            s.field_with_name(n).map(|f| f.data_type()),
                            Ok(DataType::Map(_, _))
                        )
                    })
                })
                .unwrap_or(false);
            probes.push(Probe {
                name: "typed_maps_roundtrip",
                pass: all_maps,
                note: format!(
                    "all four per-type maps + binary residue returned as Arrow Map columns \
                     through the fork ({})",
                    if all_maps {
                        "types preserved"
                    } else {
                        "TYPE LOST"
                    }
                ),
            });
        }
        Err(e) => probes.push(Probe::fail(
            "typed_maps_roundtrip",
            format!("scan of typed maps failed:{e}"),
        )),
    }
}

/// Map element access — SQL vs expr level. DataFusion map support has been shaky.
async fn probe_map_access(ctx: &SessionContext, probes: &mut Vec<Probe>) {
    let candidates = [
        "SELECT attributes_int['http.response.status_code'] AS v FROM events ORDER BY timestamp",
        "SELECT map_extract(attributes_int, 'http.response.status_code') AS v FROM events ORDER BY timestamp",
        "SELECT element_at(attributes_int, 'http.response.status_code') AS v FROM events ORDER BY timestamp",
    ];
    match first_working(ctx, &candidates).await {
        Ok((sql, batches)) => {
            let rendered = pretty_format_batches(&batches)
                .map(|d| d.to_string())
                .unwrap_or_default();
            // Assert the typed values actually came back, not merely that the
            // query planned and collected (review follow-up on PR #960).
            let pass = rendered.contains("200") && rendered.contains("500");
            probes.push(Probe {
                name: "map_element_access",
                pass,
                note: if pass {
                    format!("working syntax: `{sql}`\n{rendered}")
                } else {
                    format!("query ran but expected values 200/500 missing: `{sql}`\n{rendered}")
                },
            });
        }
        Err(e) => probes.push(Probe::fail(
            "map_element_access",
            format!("no map element-access syntax worked (map may be opaque at SQL level):{e}"),
        )),
    }
}

async fn probe_typed_predicate(ctx: &SessionContext, probes: &mut Vec<Probe>) {
    let candidates = [
        "SELECT COUNT(*) AS n FROM events WHERE attributes_int['http.response.status_code'] = 200",
        "SELECT COUNT(*) AS n FROM events WHERE element_at(attributes_int, 'http.response.status_code') = 200",
        "SELECT COUNT(*) AS n FROM events WHERE map_extract(attributes_int, 'http.response.status_code')[1] = 200",
    ];
    match first_working(ctx, &candidates).await {
        Ok((sql, batches)) => {
            let rendered = pretty_format_batches(&batches)
                .map(|d| d.to_string())
                .unwrap_or_default();
            let pass = rendered.contains(" 1 ");
            probes.push(Probe {
                name: "typed_map_predicate",
                pass,
                note: format!(
                    "filter on typed map worked: `{sql}` → {}",
                    rendered.replace('\n', " ")
                ),
            });
        }
        Err(e) => probes.push(Probe::fail(
            "typed_map_predicate",
            format!("predicate on typed map column failed:{e}"),
        )),
    }
}

async fn probe_residue(ctx: &SessionContext, probes: &mut Vec<Probe>) {
    let candidates = [
        "SELECT attributes_residue['http.response.status_code'] AS v FROM events WHERE attributes_residue IS NOT NULL",
        "SELECT element_at(attributes_residue, 'http.response.status_code') AS v FROM events WHERE attributes_residue IS NOT NULL",
        "SELECT map_extract(attributes_residue, 'http.response.status_code')[1] AS v FROM events WHERE attributes_residue IS NOT NULL",
    ];
    match first_working(ctx, &candidates).await {
        Ok((sql, batches)) => {
            let mut decoded = None;
            let mut seen_type = String::new();
            for b in &batches {
                let col = b.column(0);
                seen_type = col.data_type().to_string();
                // DataFusion may hand back any binary flavor from map access.
                let bytes_at = |i: usize| -> Option<&[u8]> {
                    use datafusion::arrow::array::{
                        BinaryArray, BinaryViewArray, LargeBinaryArray,
                    };
                    let a = col.as_any();
                    if let Some(c) = a.downcast_ref::<BinaryArray>() {
                        (!c.is_null(i)).then(|| c.value(i))
                    } else if let Some(c) = a.downcast_ref::<LargeBinaryArray>() {
                        (!c.is_null(i)).then(|| c.value(i))
                    } else if let Some(c) = a.downcast_ref::<BinaryViewArray>() {
                        (!c.is_null(i)).then(|| c.value(i))
                    } else {
                        None
                    }
                };
                for i in 0..col.len() {
                    if let Some(bytes) = bytes_at(i) {
                        let v: serde_json::Value =
                            ciborium::from_reader(bytes).unwrap_or(serde_json::Value::Null);
                        decoded = Some(v);
                    }
                }
            }
            match decoded {
                Some(v) if v.get("value").and_then(|x| x.as_str()) == Some("OK") => {
                    probes.push(Probe {
                        name: "residue_cbor_roundtrip",
                        pass: true,
                        note: format!("`{sql}` → Binary residue CBOR-decoded to {v}"),
                    })
                }
                other => probes.push(Probe::fail(
                    "residue_cbor_roundtrip",
                    format!(
                        "residue read but CBOR decode unexpected: {other:?} (column type: {seen_type})"
                    ),
                )),
            }
        }
        Err(e) => probes.push(Probe::fail(
            "residue_cbor_roundtrip",
            format!("could not read Binary-valued residue map:{e}"),
        )),
    }
}

/// The load-bearing promotion proof: one scan over gen-1 (pre-promotion, no
/// column) + gen-2 (post-promotion) files. gen-1 rows null-fill the promoted
/// column; gen-2 rows serve it.
async fn probe_promotion_null_fill(ctx: &SessionContext, probes: &mut Vec<Probe>) {
    let sql = format!(
        "SELECT \
           SUM(CASE WHEN service_name='gen1-svc' AND {PROMOTED} IS NULL THEN 1 ELSE 0 END) AS gen1_null, \
           SUM(CASE WHEN service_name='gen2-svc' AND {PROMOTED} IS NOT NULL THEN 1 ELSE 0 END) AS gen2_set \
         FROM events"
    );
    match first_working(ctx, &[sql.as_str()]).await {
        Ok((_, batches)) => {
            let rendered = pretty_format_batches(&batches)
                .map(|d| d.to_string())
                .unwrap_or_default();
            // Both counts should be 2 (2 gen-1 rows null, 2 gen-2 rows set).
            let pass = rendered.matches(" 2 ").count() >= 2;
            probes.push(Probe {
                name: "promotion_single_scan_null_fill",
                pass,
                note: format!(
                    "one scan over pre- and post-promotion files: gen-1 rows null-fill `{PROMOTED}`, \
                     gen-2 rows serve it, no error:\n{rendered}"
                ),
            });
        }
        Err(e) => probes.push(Probe::fail(
            "promotion_single_scan_null_fill",
            format!("scan failed:{e}"),
        )),
    }
}

/// Prove projection is by FIELD-ID, not name/position: after promotion changed
/// the column count, a pre-promotion file's existing `attributes_int` must
/// still read its original values (200/500) — a positional reader would shift.
async fn probe_field_id_not_positional(ctx: &SessionContext, probes: &mut Vec<Probe>) {
    let candidates = [
        "SELECT attributes_int['http.response.status_code'] AS sc FROM events WHERE service_name='gen1-svc' ORDER BY timestamp",
        "SELECT map_extract(attributes_int, 'http.response.status_code')[1] AS sc FROM events WHERE service_name='gen1-svc' ORDER BY timestamp",
        "SELECT element_at(attributes_int, 'http.response.status_code') AS sc FROM events WHERE service_name='gen1-svc' ORDER BY timestamp",
    ];
    match first_working(ctx, &candidates).await {
        Ok((sql, batches)) => {
            let rendered = pretty_format_batches(&batches)
                .map(|d| d.to_string())
                .unwrap_or_default();
            // gen-1 values are unchanged 200 & 500 despite the added column.
            let pass = rendered.contains("200") && rendered.contains("500");
            probes.push(Probe {
                name: "field_id_projection_not_positional",
                pass,
                note: format!(
                    "pre-promotion file's attributes_int still reads 200/500 after the column \
                     count changed → provider maps parquet by field-id, not name/position. \
                     `{sql}` → {}",
                    rendered.replace('\n', " ")
                ),
            });
        }
        Err(e) => probes.push(Probe::fail(
            "field_id_projection_not_positional",
            format!("could not read gen-1 typed map post-promotion:{e}"),
        )),
    }
}

/// Filter/predicate directly on the promoted primitive column across both gens.
async fn probe_promoted_predicate(ctx: &SessionContext, probes: &mut Vec<Probe>) {
    let sql = format!("SELECT COUNT(*) AS n FROM events WHERE {PROMOTED} = 200");
    match first_working(ctx, &[sql.as_str()]).await {
        Ok((_, batches)) => {
            let rendered = pretty_format_batches(&batches)
                .map(|d| d.to_string())
                .unwrap_or_default();
            // Only one gen-2 row has 200 in the promoted column (gen-1 rows are NULL).
            let pass = rendered.contains(" 1 ");
            probes.push(Probe {
                name: "promoted_column_predicate",
                pass,
                note: format!(
                    "predicate on the promoted primitive column across both generations \
                     (gen-1 NULL, gen-2 populated) → {}",
                    rendered.replace('\n', " ")
                ),
            });
        }
        Err(e) => probes.push(Probe::fail(
            "promoted_column_predicate",
            format!("promoted-column filter failed:{e}"),
        )),
    }
}
