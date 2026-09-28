//! Rewrite-coupled typed per-level attribute promotion (otel-native-schema
//! layer 6, D4/D5)
//!
//! End-to-end test of the active (non-dry-run) typed promotion path: a
//! typed-layout table with a resource-level `String` key and a record-level
//! `Int64` key, both with recorded query demand, runs through a compaction,
//! which must evolve the schema (add `attr_resource_<key>` and
//! `attr_record_<key>` via AddSchema + SetCurrentSchema), backfill both
//! columns for the pre-existing rows from their level's typed home, and
//! leave the data queryable and idempotent across a second compaction. With
//! `dry_run = true` the same setup must change nothing.

use anyhow::Result;
use common::catalog_manager::CatalogManager;
use common::iceberg::evolution;
use common::schema::logical::AttributeLevel;
use common::schema::type_authority::CanonicalType;
use compactor::executor::{CompactionExecutor, CompactionStatus, ExecutorConfig};
use compactor::metrics::CompactionMetrics;
use compactor::planner::{CompactionCandidate, PartitionStats};
use datafusion::arrow::array::{
    ArrayRef, Int64Builder, MapBuilder, MapFieldNames, RecordBatch, StringArray, StringBuilder,
    TimestampMicrosecondArray, new_null_array,
};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::prelude::SessionContext;
use futures::stream;
use iceberg_rust::arrow::write::write_parquet_partitioned;
use iceberg_rust::catalog::create::CreateTableBuilder;
use iceberg_rust::catalog::identifier::Identifier;
use iceberg_rust::catalog::tabular::Tabular;
use iceberg_rust::spec::schema::Schema as IcebergSchema;
use iceberg_rust::spec::types::{PrimitiveType, StructField, StructType, Type};
use iceberg_rust::table::Table;
use std::sync::Arc;
use tests_integration::compaction_helpers::{
    busiest_partition, hour_partition_spec, load_table_by_identifier, retype_map,
    seed_typed_attribute_demand, string_field, typed_container_fields,
};

const TENANT: &str = "t1";
const DATASET: &str = "d1";
const TABLE: &str = "logs";

/// A typed-layout logs table: the real sort columns plus a full typed
/// `resource_attributes` container (resource level) and a full typed
/// `log_attributes` container (record level).
fn table_schema() -> IcebergSchema {
    let timestamp = StructField {
        id: 1,
        name: "timestamp".to_string(),
        required: true,
        field_type: Type::Primitive(PrimitiveType::Timestamp),
        doc: None,
        initial_default: None,
        write_default: None,
    };
    let mut fields = vec![
        timestamp,
        string_field(2, "service_name"),
        string_field(3, "severity_text"),
        string_field(4, "body"),
    ];
    fields.extend(typed_container_fields(5, "resource_attributes"));
    fields.extend(typed_container_fields(18, "log_attributes"));
    IcebergSchema::from_struct_type(StructType::new(fields), 0, None)
}

/// One test row: (timestamp, service, body, resource-level `region`,
/// record-level `retries`).
type TestRow<'a> = (i64, &'a str, &'a str, Option<&'a str>, Option<i64>);

/// Write one data file with the given rows: `region` lives in
/// `resource_attributes_str`, `retries` in `log_attributes_int`. Every home
/// the fixture doesn't populate with real data (including any promoted
/// attribute column the current schema has evolved since table creation)
/// is filled with an all-null array of the right shape -- exactly what an
/// un-reloaded writer would produce.
async fn write_file(
    catalog_manager: &CatalogManager,
    identifier: &Identifier,
    rows: &[TestRow<'_>],
) -> Result<()> {
    let mut table = load_table_by_identifier(catalog_manager, identifier).await?;
    let arrow_schema: SchemaRef = Arc::new(
        table
            .current_schema()?
            .fields()
            .try_into()
            .map_err(|e: iceberg_rust::spec::error::Error| anyhow::anyhow!("to arrow: {e}"))?,
    );

    let map_field_names = |column: &str| -> Result<MapFieldNames> {
        let field = arrow_schema.field_with_name(column)?;
        let DataType::Map(entry_field, _) = field.data_type() else {
            anyhow::bail!("{column} should convert to an Arrow Map");
        };
        let DataType::Struct(kv_fields) = entry_field.data_type() else {
            anyhow::bail!("{column} map entries should be a struct");
        };
        Ok(MapFieldNames {
            entry: entry_field.name().clone(),
            key: kv_fields[0].name().clone(),
            value: kv_fields[1].name().clone(),
        })
    };

    let mut region = MapBuilder::new(
        Some(map_field_names("resource_attributes_str")?),
        StringBuilder::new(),
        StringBuilder::new(),
    );
    let mut retries = MapBuilder::new(
        Some(map_field_names("log_attributes_int")?),
        StringBuilder::new(),
        Int64Builder::new(),
    );
    let mut timestamps = Vec::new();
    let mut services = Vec::new();
    let mut bodies = Vec::new();
    for (ts, service, body, region_value, retries_value) in rows {
        timestamps.push(*ts);
        services.push(Some(*service));
        bodies.push(Some(*body));
        // A zero-entry (but non-null) map row currently reads back with the
        // whole map column empty across every row of the batch (upstream
        // iceberg-rust quirk, already documented and worked around the same
        // way in attr_promotion_rewrite.rs) -- a constant sentinel key keeps
        // every row's map non-empty even when `region`/`retries` is absent.
        region.keys().append_value("_sentinel");
        region.values().append_value("x");
        if let Some(v) = region_value {
            region.keys().append_value("region");
            region.values().append_value(v);
        }
        region.append(true)?;
        retries.keys().append_value("_sentinel");
        retries.values().append_value(0);
        if let Some(v) = retries_value {
            retries.keys().append_value("retries");
            retries.values().append_value(*v);
        }
        retries.append(true)?;
    }
    // Re-typed onto the schema's own field: a `MapBuilder`-built array's
    // key/value fields carry no `PARQUET:field_id` metadata, which the
    // Iceberg-derived `arrow_schema` requires.
    let region = retype_map(
        region.finish(),
        arrow_schema.field_with_name("resource_attributes_str")?,
    )?;
    let retries = retype_map(
        retries.finish(),
        arrow_schema.field_with_name("log_attributes_int")?,
    )?;
    let ts = TimestampMicrosecondArray::from(timestamps);
    let len = rows.len();

    // The writer maps this batch's columns onto the table's schema
    // positionally, not by name, so every field of `arrow_schema` -- in its
    // exact order -- needs a column here: `region`/`retries` where this
    // fixture populates them, `timestamp`/`service_name`/`body` from the
    // row data, and everything else (unpopulated typed homes,
    // `severity_text`, both residues, and any typed promoted attribute
    // column added since this table was created) as an all-null array of
    // the right shape -- exactly what an un-reloaded writer would produce.
    let mut region = Some(region);
    let mut retries = Some(retries);
    let mut services = Some(services);
    let mut bodies = Some(bodies);
    let mut ts = Some(ts);
    let columns: Vec<ArrayRef> = arrow_schema
        .fields()
        .iter()
        .map(|field| match field.name().as_str() {
            "timestamp" => Arc::new(ts.take().unwrap()) as ArrayRef,
            "service_name" => Arc::new(StringArray::from(services.take().unwrap())) as ArrayRef,
            "body" => Arc::new(StringArray::from(bodies.take().unwrap())) as ArrayRef,
            "resource_attributes_str" => Arc::new(region.take().unwrap()) as ArrayRef,
            "log_attributes_int" => Arc::new(retries.take().unwrap()) as ArrayRef,
            _ => new_null_array(field.data_type(), len),
        })
        .collect();
    let batch = RecordBatch::try_new(Arc::clone(&arrow_schema), columns)?;

    let files = write_parquet_partitioned(&table, stream::iter(vec![Ok(batch)]), None).await?;
    table
        .new_transaction(None)
        .append_data(files)
        .commit()
        .await?;
    Ok(())
}

/// Build the environment: an in-memory catalog with typed promotion
/// configured (streak 1 so a single cycle promotes), a typed-layout logs
/// table with two small files, resource-level `region` (3 of 4 rows) and
/// record-level `retries` (2 of 4 rows), and query demand recorded for both
/// at their own level.
async fn setup(
    dry_run: bool,
) -> Result<(
    Arc<CatalogManager>,
    Arc<common::catalog::Catalog>,
    Identifier,
)> {
    let mut config = common::testing::TestConfigBuilder::new()
        .in_memory()
        .with_tenant(TENANT, DATASET)
        .build();
    config.compactor.attr_promotion = common::config::AttrPromotionConfig {
        enabled: true,
        dry_run,
        max_labels_per_table: 8,
        min_presence: 0.01,
        min_query_hits: 1,
        promote_streak: 1,
        max_promotions_per_cycle: 4,
        demote_after_idle: std::time::Duration::from_secs(7 * 24 * 3600),
    };
    let catalog_manager = Arc::new(CatalogManager::new(config).await?);

    let namespace = catalog_manager.build_namespace(TENANT, DATASET)?;
    catalog_manager
        .catalog()
        .create_namespace(&namespace, None)
        .await?;
    let identifier = catalog_manager.build_table_identifier(TENANT, DATASET, TABLE);
    let create = CreateTableBuilder::default()
        .with_name(TABLE.to_string())
        .with_schema(table_schema())
        .with_partition_spec(hour_partition_spec())
        .with_location(catalog_manager.build_table_location(TENANT, DATASET, TABLE))
        .create()
        .map_err(|e| anyhow::anyhow!("create table build: {e}"))?;
    catalog_manager
        .catalog()
        .create_table(identifier.clone(), create)
        .await?;

    write_file(
        &catalog_manager,
        &identifier,
        &[
            (1_000_000, "api", "hello prod", Some("us-east"), Some(3)),
            (2_000_000, "api", "hello staging", Some("us-west"), None),
        ],
    )
    .await?;
    write_file(
        &catalog_manager,
        &identifier,
        &[
            (3_000_000, "web", "no region here", None, Some(7)),
            (4_000_000, "web", "hello prod again", Some("us-east"), None),
        ],
    )
    .await?;

    let service_catalog = Arc::new(common::catalog::Catalog::new_in_memory().await?);
    seed_typed_attribute_demand(
        &service_catalog,
        TENANT,
        DATASET,
        "logs",
        AttributeLevel::Resource,
        "region",
        CanonicalType::String,
        10,
    )
    .await?;
    seed_typed_attribute_demand(
        &service_catalog,
        TENANT,
        DATASET,
        "logs",
        AttributeLevel::Record,
        "retries",
        CanonicalType::Int64,
        10,
    )
    .await?;

    Ok((catalog_manager, service_catalog, identifier))
}

async fn run_compaction(
    catalog_manager: Arc<CatalogManager>,
    service_catalog: Arc<common::catalog::Catalog>,
) -> Result<()> {
    let partition = busiest_partition(&catalog_manager, TENANT, DATASET, TABLE).await?;
    let executor = CompactionExecutor::new(
        catalog_manager,
        ExecutorConfig::default(),
        CompactionMetrics::new(),
    )
    .with_service_catalog(service_catalog);
    let candidate = CompactionCandidate {
        tenant_id: TENANT.to_string(),
        dataset_id: DATASET.to_string(),
        table_name: TABLE.to_string(),
        partition_id: partition.to_string(),
        stats: PartitionStats {
            file_count: 2,
            total_size_bytes: 4096,
            avg_file_size_bytes: 2048,
        },
    };
    let result = executor.execute_candidate(candidate).await?;
    anyhow::ensure!(
        result.status == CompactionStatus::Success,
        "compaction must succeed: {:?}",
        result.error
    );
    Ok(())
}

async fn count_rows(ctx: &SessionContext, sql: &str) -> Result<usize> {
    let rows = ctx.sql(sql).await?.collect().await?;
    Ok(rows.iter().map(|b| b.num_rows()).sum())
}

fn register_table(table: Table) -> Result<SessionContext> {
    let provider = Arc::new(datafusion_iceberg::DataFusionTable::new(
        Tabular::Table(table),
        None,
        None,
        None,
    )) as Arc<dyn datafusion::datasource::TableProvider>;
    let ctx = SessionContext::new();
    ctx.register_table("logs", provider)?;
    Ok(ctx)
}

#[tokio::test]
async fn active_promotion_evolves_schema_and_backfills_typed_columns() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let (catalog_manager, service_catalog, identifier) = setup(false).await?;

    run_compaction(catalog_manager.clone(), service_catalog.clone()).await?;

    let table = load_table_by_identifier(&catalog_manager, &identifier).await?;
    let schema = table.current_schema()?;
    let resource_field = schema
        .fields()
        .iter()
        .find(|f| f.name == "attr_resource_region")
        .expect("resource-level column must exist in the current schema");
    assert_eq!(
        resource_field.field_type,
        Type::Primitive(PrimitiveType::String)
    );
    assert_eq!(
        evolution::promoted_attr_origin(resource_field.doc.as_deref()),
        Some((AttributeLevel::Resource, "region"))
    );
    let record_field = schema
        .fields()
        .iter()
        .find(|f| f.name == "attr_record_retries")
        .expect("record-level column must exist in the current schema");
    assert_eq!(
        record_field.field_type,
        Type::Primitive(PrimitiveType::Long)
    );
    assert_eq!(
        evolution::promoted_attr_origin(record_field.doc.as_deref()),
        Some((AttributeLevel::Record, "retries"))
    );

    let ctx = register_table(table)?;
    assert_eq!(count_rows(&ctx, "SELECT body FROM logs").await?, 4);
    assert_eq!(
        count_rows(
            &ctx,
            "SELECT body FROM logs WHERE attr_resource_region = 'us-east'"
        )
        .await?,
        2,
        "pre-existing rows must be backfilled from the resource-level typed home"
    );
    assert_eq!(
        count_rows(
            &ctx,
            "SELECT body FROM logs WHERE attr_resource_region IS NULL"
        )
        .await?,
        1,
        "a row without the key stays null"
    );
    assert_eq!(
        count_rows(&ctx, "SELECT body FROM logs WHERE attr_record_retries = 3").await?,
        1,
        "pre-existing rows must be backfilled from the record-level typed home"
    );
    assert_eq!(
        count_rows(&ctx, "SELECT body FROM logs WHERE attr_record_retries = 7").await?,
        1
    );
    assert_eq!(
        count_rows(
            &ctx,
            "SELECT body FROM logs WHERE attr_record_retries IS NULL"
        )
        .await?,
        2,
        "rows without the key stay null"
    );

    // A file appended after promotion whose writer hasn't backfilled the
    // promoted columns yet (they carry only nulls for the new keys) must
    // still scan without error, with those keys null.
    write_file(
        &catalog_manager,
        &identifier,
        &[(5_000_000, "late", "arrived after promotion", None, None)],
    )
    .await?;
    let table = load_table_by_identifier(&catalog_manager, &identifier).await?;
    let ctx = register_table(table)?;
    assert_eq!(count_rows(&ctx, "SELECT body FROM logs").await?, 5);
    assert_eq!(
        count_rows(
            &ctx,
            "SELECT body FROM logs WHERE body = 'arrived after promotion' AND attr_resource_region IS NULL"
        )
        .await?,
        1
    );

    // A second compaction is idempotent: no further schema evolution, same
    // row count, values unchanged.
    let schema_id_before = load_table_by_identifier(&catalog_manager, &identifier)
        .await?
        .metadata()
        .current_schema_id;
    run_compaction(catalog_manager.clone(), service_catalog).await?;
    let table = load_table_by_identifier(&catalog_manager, &identifier).await?;
    assert_eq!(table.metadata().current_schema_id, schema_id_before);
    let ctx = register_table(table)?;
    assert_eq!(count_rows(&ctx, "SELECT body FROM logs").await?, 5);
    assert_eq!(
        count_rows(
            &ctx,
            "SELECT body FROM logs WHERE attr_resource_region = 'us-east'"
        )
        .await?,
        2
    );

    Ok(())
}

#[tokio::test]
async fn dry_run_typed_promotion_changes_nothing() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let (catalog_manager, service_catalog, identifier) = setup(true).await?;

    run_compaction(catalog_manager.clone(), service_catalog).await?;

    let table = load_table_by_identifier(&catalog_manager, &identifier).await?;
    let schema = table.current_schema()?;
    assert!(
        !schema.fields().iter().any(|f| f.name.starts_with("attr_")),
        "dry run must not evolve the schema"
    );
    assert_eq!(table.metadata().schemas.len(), 1);

    let ctx = register_table(table)?;
    assert_eq!(count_rows(&ctx, "SELECT body FROM logs").await?, 4);

    Ok(())
}
