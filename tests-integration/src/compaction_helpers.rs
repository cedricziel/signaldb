//! Helpers for tests that drive compaction directly.
//!
//! Compaction is partition-scoped (issue #933): a [`CompactionCandidate`]
//! names one `timestamp_hour` partition — hours since the Unix epoch — which
//! the executor rewrites and commits as a delta. Tests that build candidates
//! by hand therefore need the real partition value rather than a placeholder,
//! and it must come from the manifests, since the generator's timestamp range
//! determines which hours the writer actually produced files for.

use anyhow::{Context, Result};
use common::catalog::Catalog;
use common::catalog_manager::CatalogManager;
use common::schema::logical::AttributeLevel;
use common::schema::type_authority::CanonicalType;
use compactor::ManifestReader;
use datafusion::arrow::array::{Array, MapArray, RecordBatch, StructArray};
use datafusion::arrow::datatypes::{DataType, Field};
use iceberg_rust::catalog::identifier::Identifier;
use iceberg_rust::catalog::tabular::Tabular;
use iceberg_rust::spec::partition::{
    PartitionField, PartitionSpec, PartitionSpecBuilder, Transform,
};
use iceberg_rust::spec::types::{MapType, PrimitiveType, StructField, Type};
use iceberg_rust::table::Table;
use std::collections::HashMap;
use std::sync::Arc;

/// Milliseconds in one hour partition.
pub const MILLIS_PER_HOUR: i64 = 60 * 60 * 1000;

/// Start of the hour `hours_ago` hours before now, in epoch millis.
///
/// Aligning a generator's `base_timestamp` to an hour boundary keeps its rows
/// inside exactly one Iceberg `timestamp_hour` partition; an unaligned range
/// straddles two, which splits the files a test expects to compact together.
pub fn aligned_hour_start(hours_ago: i64) -> i64 {
    (chrono::Utc::now().timestamp_millis() / MILLIS_PER_HOUR - hours_ago) * MILLIS_PER_HOUR
}

/// Load a table with fresh metadata.
pub async fn load_table(
    catalog_manager: &Arc<CatalogManager>,
    tenant_id: &str,
    dataset_id: &str,
    table_name: &str,
) -> Result<Table> {
    let identifier = catalog_manager.build_table_identifier(tenant_id, dataset_id, table_name);
    match catalog_manager
        .catalog()
        .load_tabular(&identifier)
        .await
        .with_context(|| format!("Failed to load table {identifier}"))?
    {
        iceberg_rust::catalog::tabular::Tabular::Table(table) => Ok(table),
        _ => anyhow::bail!("Expected a table at {identifier}"),
    }
}

/// Load a table with fresh metadata, already knowing its [`Identifier`] --
/// for a caller that built one directly rather than from
/// tenant/dataset/table strings (e.g. to reuse it for writing too).
pub async fn load_table_by_identifier(
    catalog_manager: &CatalogManager,
    identifier: &Identifier,
) -> Result<Table> {
    match catalog_manager
        .catalog()
        .load_tabular(identifier)
        .await
        .with_context(|| format!("Failed to load table {identifier}"))?
    {
        Tabular::Table(table) => Ok(table),
        _ => anyhow::bail!("Expected a table at {identifier}"),
    }
}

/// A single-column typed-layout `Map<Utf8, value>` field, e.g. one of a
/// container's four typed homes (`log_attributes_str`, `..._int`, ...).
/// Consumes `id` for the map field itself and `id + 1`/`id + 2` for its
/// nested key/value ids.
pub fn map_field(id: i32, name: &str, value: PrimitiveType) -> StructField {
    StructField {
        id,
        name: name.to_string(),
        required: false,
        field_type: Type::Map(MapType {
            key_id: id + 1,
            key: Box::new(Type::Primitive(PrimitiveType::String)),
            value_id: id + 2,
            value_required: false,
            value: Box::new(Type::Primitive(value)),
        }),
        doc: None,
        initial_default: None,
        write_default: None,
    }
}

/// A single optional `String` field, e.g. `service_name`.
pub fn string_field(id: i32, name: &str) -> StructField {
    StructField {
        id,
        name: name.to_string(),
        required: false,
        field_type: Type::Primitive(PrimitiveType::String),
        doc: None,
        initial_default: None,
        write_default: None,
    }
}

/// A single optional `Binary` field, e.g. a typed-layout container's
/// `_residue` column.
pub fn residue_field(id: i32, name: &str) -> StructField {
    StructField {
        id,
        name: name.to_string(),
        required: false,
        field_type: Type::Primitive(PrimitiveType::Binary),
        doc: None,
        initial_default: None,
        write_default: None,
    }
}

/// The four typed homes plus residue of one typed-layout attribute
/// container (`resource_attributes`, `log_attributes`, ...), starting at
/// `id` (consuming `id..id+13`): `_str`, `_int`, `_double`, `_bool` maps
/// (each a map field plus its key/value ids) and `_residue`.
pub fn typed_container_fields(id: i32, container: &str) -> Vec<StructField> {
    vec![
        map_field(id, &format!("{container}_str"), PrimitiveType::String),
        map_field(id + 3, &format!("{container}_int"), PrimitiveType::Long),
        map_field(
            id + 6,
            &format!("{container}_double"),
            PrimitiveType::Double,
        ),
        map_field(id + 9, &format!("{container}_bool"), PrimitiveType::Boolean),
        residue_field(id + 12, &format!("{container}_residue")),
    ]
}

/// Hour-partition spec on `timestamp`, matching what every production
/// signal table uses (`common::iceberg::schemas`). Compaction is
/// partition-scoped (issue #933), so a test table must be partitioned the
/// way real tables are — an unpartitioned table has no `timestamp_hour`
/// value for the planner or executor to scope a job to.
pub fn hour_partition_spec() -> PartitionSpec {
    PartitionSpecBuilder::default()
        .with_spec_id(0)
        // Iceberg convention: partition field_id = 1000 + source field id.
        .with_partition_field(PartitionField::new(
            1,
            1001,
            "timestamp_hour",
            Transform::Hour,
        ))
        .build()
        .expect("hour partition spec should build")
}

/// Re-types a [`datafusion::arrow::array::builder::MapBuilder`]-built map
/// onto `target_field`'s declared type -- same offsets, nulls, and
/// key/value arrays, but the key/value child fields' `PARQUET:field_id`
/// metadata (absent from a builder-constructed map, required by the
/// Iceberg-derived arrow schema `write_parquet_partitioned` writes
/// against).
pub fn retype_map(map: MapArray, target_field: &Field) -> Result<MapArray> {
    let DataType::Map(entries_field, ordered) = target_field.data_type().clone() else {
        anyhow::bail!("{} is not a Map field", target_field.name());
    };
    let DataType::Struct(kv_fields) = entries_field.data_type().clone() else {
        anyhow::bail!("{} entries are not a Struct", target_field.name());
    };
    let entries = StructArray::new(
        kv_fields,
        vec![map.keys().clone(), map.values().clone()],
        None,
    );
    Ok(MapArray::try_new(
        entries_field,
        map.offsets().clone(),
        entries,
        map.nulls().cloned(),
        ordered,
    )?)
}

/// Record a key's canonical type and per-level query demand together --
/// the two catalog seeds every typed per-level attribute promotion test
/// needs for a `(level, key)` to become an eligible promotion candidate.
#[allow(clippy::too_many_arguments)]
pub async fn seed_typed_attribute_demand(
    service_catalog: &Catalog,
    tenant_id: &str,
    dataset_id: &str,
    signal: &str,
    level: AttributeLevel,
    key: &str,
    canonical: CanonicalType,
    query_hits: i64,
) -> Result<()> {
    service_catalog
        .override_attribute_type(
            tenant_id,
            dataset_id,
            &common::schema::logical::LogicalFieldId {
                source: signal.to_string(),
                level: Some(level),
                name: key.to_string(),
            },
            canonical,
        )
        .await?;
    service_catalog
        .add_attribute_level_query_hits(
            tenant_id,
            dataset_id,
            signal,
            level,
            key,
            query_hits,
            chrono::Utc::now(),
        )
        .await?;
    Ok(())
}

/// The `timestamp_hour` partition holding the most live data files.
///
/// Use this to build a compaction candidate for whichever partition the test's
/// generated data actually landed in.
pub async fn busiest_partition(
    catalog_manager: &Arc<CatalogManager>,
    tenant_id: &str,
    dataset_id: &str,
    table_name: &str,
) -> Result<i64> {
    let table = load_table(catalog_manager, tenant_id, dataset_id, table_name).await?;

    let mut counts: HashMap<i64, usize> = HashMap::new();
    for file in ManifestReader::new()
        .get_snapshot_files(&table)
        .await
        .context("Failed to read live files from manifests")?
    {
        if let Some(partition) = file.partition_hours {
            *counts.entry(partition).or_default() += 1;
        }
    }

    counts
        .into_iter()
        .max_by_key(|(_, count)| *count)
        .map(|(partition, _)| partition)
        .context("No partitioned data files found in table")
}

/// The sort order id each of the current snapshot's data files attests, keyed
/// by file path.
///
/// `None` means the file claims no ordering — either it was written before
/// the table declared one, or by a producer that could not guarantee the
/// order. The query engine reads such a file as unsorted, so this is the
/// observable form of the honesty invariant: a test can check exactly which
/// files claim to be ordered.
///
/// A `SessionContext` with `table` registered under `name`.
///
/// `split_file_groups_by_statistics` is the one execution option ordering
/// tests need to vary: it decides whether the scan regroups attested files by
/// their statistics before claiming an ordering.
pub fn context_for(
    name: &str,
    table: Table,
    split_file_groups_by_statistics: bool,
) -> Result<datafusion::prelude::SessionContext> {
    use datafusion::prelude::{SessionConfig, SessionContext};

    let mut config = SessionConfig::new().with_target_partitions(4);
    config
        .options_mut()
        .execution
        .split_file_groups_by_statistics = split_file_groups_by_statistics;
    let ctx = SessionContext::new_with_config(config);
    ctx.register_table(
        name,
        Arc::new(datafusion_iceberg::DataFusionTable::from(table)),
    )
    .context("Failed to register the table with DataFusion")?;
    Ok(ctx)
}

/// The `(timestamp, trace_id)` pairs of every row of `batches`, in batch
/// order — the traces table's declared sort key, as it is compared.
///
/// Scanning without an `ORDER BY` and reading the pairs out in scan order is
/// how ordering tests observe the physical layout: for a single data file it
/// is the order the rows are stored in.
pub fn trace_sort_keys(batches: &[RecordBatch]) -> Result<Vec<(i64, String)>> {
    use datafusion::arrow::array::{StringArray, TimestampMicrosecondArray};

    let mut keys = Vec::new();
    for batch in batches {
        let timestamps = batch
            .column_by_name("timestamp")
            .context("no timestamp column")?
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .context("timestamp is not a microsecond timestamp column")?;
        let trace_ids = batch
            .column_by_name("trace_id")
            .context("no trace_id column")?
            .as_any()
            .downcast_ref::<StringArray>()
            .context("trace_id is not a string column")?;
        keys.extend(
            (0..batch.num_rows()).map(|i| (timestamps.value(i), trace_ids.value(i).to_string())),
        );
    }
    Ok(keys)
}

/// Scoped to the current snapshot, so files a compaction has already replaced
/// do not count against what the table holds now.
pub async fn attested_sort_order_ids(table: &Table) -> Result<HashMap<String, Option<i32>>> {
    Ok(ManifestReader::new()
        .get_snapshot_files(table)
        .await
        .context("Failed to read live files from manifests")?
        .into_iter()
        .map(|file| (file.file_path, file.sort_order_id))
        .collect())
}
