//! Rewrite-coupled attribute auto-promotion (epic #737, #734)
//!
//! End-to-end test of the active (non-dry-run) promotion path: a table
//! with map-typed attributes and query demand for one key runs through a
//! compaction, which must evolve the schema (add the `label_<key>`
//! column via AddSchema + SetCurrentSchema), backfill the column for the
//! pre-existing rows during the rewrite, and leave the data queryable.
//! With `dry_run = true` the same setup must change nothing.

use anyhow::Result;
use common::catalog_manager::CatalogManager;
use compactor::executor::{CompactionExecutor, CompactionStatus, ExecutorConfig};
use compactor::metrics::CompactionMetrics;
use compactor::planner::{CompactionCandidate, PartitionStats};
use datafusion::arrow::array::{
    Array as _, MapBuilder, MapFieldNames, RecordBatch, StringArray, StringBuilder,
    TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef, TimeUnit};
use datafusion::prelude::SessionContext;
use futures::stream;
use iceberg_rust::arrow::write::write_parquet_partitioned;
use iceberg_rust::catalog::create::CreateTableBuilder;
use iceberg_rust::catalog::identifier::Identifier;
use iceberg_rust::catalog::tabular::Tabular;
use iceberg_rust::spec::partition::{
    PartitionField, PartitionSpec, PartitionSpecBuilder, Transform,
};
use iceberg_rust::spec::schema::Schema as IcebergSchema;
use iceberg_rust::spec::types::{MapType, PrimitiveType, StructField, StructType, Type};
use iceberg_rust::table::Table;
use std::sync::Arc;
use tests_integration::compaction_helpers::busiest_partition;

const TENANT: &str = "t1";
const DATASET: &str = "d1";
const TABLE: &str = "logs";

fn string_field(id: i32, name: &str) -> StructField {
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

/// A small logs-shaped table: the real sort columns (timestamp,
/// service_name, severity_text) plus a map-typed `log_attributes` column
/// whose nested key/value ids (6, 7) are allocated after the top-level
/// ids, as the production schema parser does.
/// Hour-partition spec on `timestamp`, matching what every production signal
/// table uses (`common::iceberg::schemas`). Compaction is partition-scoped
/// (issue #933), so a test table must be partitioned the way real tables are —
/// an unpartitioned table has no `timestamp_hour` value for the planner or
/// executor to scope a job to.
fn hour_partition_spec() -> PartitionSpec {
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
    let attributes = StructField {
        id: 5,
        name: "log_attributes".to_string(),
        required: false,
        field_type: Type::Map(MapType {
            key_id: 6,
            key: Box::new(Type::Primitive(PrimitiveType::String)),
            value_id: 7,
            value_required: false,
            value: Box::new(Type::Primitive(PrimitiveType::String)),
        }),
        doc: None,
        initial_default: None,
        write_default: None,
    };
    IcebergSchema::from_struct_type(
        StructType::new(vec![
            timestamp,
            string_field(2, "service_name"),
            string_field(3, "severity_text"),
            string_field(4, "body"),
            attributes,
        ]),
        0,
        None,
    )
}

async fn load_table(catalog_manager: &CatalogManager, identifier: &Identifier) -> Result<Table> {
    match catalog_manager.catalog().load_tabular(identifier).await? {
        Tabular::Table(table) => Ok(table),
        _ => anyhow::bail!("expected a table"),
    }
}

/// One test row: (timestamp, service, body, attributes as key/value pairs).
type TestRow<'a> = (i64, &'a str, &'a str, &'a [(&'a str, &'a str)]);

/// Write one data file with the given rows.
async fn write_file(
    catalog_manager: &CatalogManager,
    identifier: &Identifier,
    rows: &[TestRow<'_>],
) -> Result<()> {
    let mut table = load_table(catalog_manager, identifier).await?;

    // Derive the Arrow schema from the table so the map entry/key/value
    // field names line up with what the table declares.
    let arrow_schema: SchemaRef = Arc::new(
        table
            .current_schema()?
            .fields()
            .try_into()
            .map_err(|e: iceberg_rust::spec::error::Error| anyhow::anyhow!("to arrow: {e}"))?,
    );
    let attr_field = arrow_schema.field_with_name("log_attributes")?;
    let DataType::Map(entry_field, _) = attr_field.data_type() else {
        anyhow::bail!("log_attributes should convert to an Arrow Map");
    };
    let DataType::Struct(kv_fields) = entry_field.data_type() else {
        anyhow::bail!("map entries should be a struct");
    };
    let field_names = MapFieldNames {
        entry: entry_field.name().clone(),
        key: kv_fields[0].name().clone(),
        value: kv_fields[1].name().clone(),
    };

    let mut attrs = MapBuilder::new(
        Some(field_names),
        StringBuilder::new(),
        StringBuilder::new(),
    );
    let mut timestamps = Vec::new();
    let mut services = Vec::new();
    let mut severities = Vec::new();
    let mut bodies = Vec::new();
    for (ts, service, body, kvs) in rows {
        timestamps.push(*ts);
        services.push(Some(*service));
        severities.push(Some("INFO"));
        bodies.push(Some(*body));
        for (k, v) in *kvs {
            attrs.keys().append_value(k);
            attrs.values().append_value(v);
        }
        attrs.append(true)?;
    }
    let attrs = attrs.finish();
    let ts = TimestampMicrosecondArray::from(timestamps);

    let batch_schema = Arc::new(ArrowSchema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("service_name", DataType::Utf8, true),
        Field::new("severity_text", DataType::Utf8, true),
        Field::new("body", DataType::Utf8, true),
        Field::new("log_attributes", attrs.data_type().clone(), true),
    ]));
    let batch = RecordBatch::try_new(
        batch_schema,
        vec![
            Arc::new(ts),
            Arc::new(StringArray::from(services)),
            Arc::new(StringArray::from(severities)),
            Arc::new(StringArray::from(bodies)),
            Arc::new(attrs),
        ],
    )?;

    let files = write_parquet_partitioned(&table, stream::iter(vec![Ok(batch)]), None).await?;
    table
        .new_transaction(None)
        .append_data(files)
        .commit()
        .await?;
    Ok(())
}

/// Build the environment: an in-memory catalog with the promotion pass
/// configured (streak 1 so a single cycle promotes), a logs table with
/// two small files whose rows carry `env`/`pod` attributes, and query
/// demand recorded for `env` only.
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

    // Two files -> the executor has something to compact. `env` appears
    // in 3 of 4 rows; `pod` in 2 (and has no query demand). Every row
    // carries at least one attribute: a Parquet file written through
    // `write_parquet_partitioned` with an empty-map row currently reads
    // back with the whole map column empty (upstream iceberg-rust quirk,
    // unrelated to promotion), which would skew the presence stats.
    write_file(
        &catalog_manager,
        &identifier,
        &[
            (
                1_000_000,
                "api",
                "hello prod",
                &[("env", "prod"), ("pod", "api-1")],
            ),
            (2_000_000, "api", "hello staging", &[("env", "staging")]),
        ],
    )
    .await?;
    write_file(
        &catalog_manager,
        &identifier,
        &[
            (3_000_000, "web", "no env here", &[("pod", "web-1")]),
            (4_000_000, "web", "hello prod again", &[("env", "prod")]),
        ],
    )
    .await?;

    // Query demand for `env` only. The promotion pass reads stats keyed
    // by the identifier's namespace slugs (equal to the ids here).
    let service_catalog = Arc::new(common::catalog::Catalog::new_in_memory().await?);
    service_catalog
        .add_attribute_query_hits(TENANT, DATASET, "logs", "env", 10)
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

/// A rewrite that evolves the schema must still read the snapshot its
/// caller pinned, not a freshly loaded one.
///
/// The promotion pass commits AddSchema/SetCurrentSchema, after which the
/// rewrite reloads the table to write under the new schema. If it also
/// *read* from that reload it would pick up any snapshot committed since
/// — including a late write into this very partition — and rewrite rows
/// the delta commit does not remove. The row-parity check turns that into
/// an abort on every cycle that evolves the schema.
#[tokio::test]
async fn schema_evolution_does_not_sweep_in_a_late_write() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let (catalog_manager, service_catalog, identifier) = setup(false).await?;
    let partition = busiest_partition(&catalog_manager, TENANT, DATASET, TABLE).await?;

    // Pin the table the way the executor does, *before* the late write.
    let pinned = load_table(&catalog_manager, &identifier).await?;

    // A late write lands in the same hour partition after pinning.
    write_file(
        &catalog_manager,
        &identifier,
        &[(
            5_000_000,
            "late",
            "arrived after pinning",
            &[("env", "prod")],
        )],
    )
    .await?;

    let mut rewriter = compactor::rewriter::ParquetRewriter::new(catalog_manager.clone());
    rewriter.set_service_catalog(service_catalog);

    let outcome = rewriter
        .rewrite_partition(&pinned, partition, 128 * 1024 * 1024)
        .await?
        .expect("partition has data to rewrite");

    // The schema did evolve — otherwise this test would pass for the
    // wrong reason (no reload, so no way to read the wrong snapshot).
    let reloaded = load_table(&catalog_manager, &identifier).await?;
    assert!(
        reloaded
            .current_schema()?
            .fields()
            .iter()
            .any(|f| f.name == "label_env"),
        "fixture must actually evolve the schema"
    );

    assert_eq!(
        outcome.rows_written, 4,
        "the rewrite must cover only the 4 rows in the pinned snapshot; \
         reading the reloaded table would sweep in the late 5th row, which \
         the delta commit does not remove"
    );

    Ok(())
}

#[tokio::test]
async fn active_promotion_evolves_schema_and_backfills_on_rewrite() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let (catalog_manager, service_catalog, identifier) = setup(false).await?;

    run_compaction(catalog_manager.clone(), service_catalog).await?;

    // Schema evolved: `label_env` exists (and only it — `pod` had no
    // query demand and must not be promoted).
    let table = load_table(&catalog_manager, &identifier).await?;
    let schema = table.current_schema()?;
    assert!(
        schema.fields().iter().any(|f| f.name == "label_env"),
        "promoted column must exist in the current schema"
    );
    assert!(
        !schema.fields().iter().any(|f| f.name == "label_pod"),
        "unqueried key must not be promoted"
    );
    assert_eq!(table.metadata().current_schema_id, 1);
    assert_eq!(table.metadata().schemas.len(), 2);

    // The rewrite backfilled the column for the pre-existing rows and the
    // data is queryable through the promoted column.
    let provider = Arc::new(datafusion_iceberg::DataFusionTable::new(
        Tabular::Table(table),
        None,
        None,
        None,
    )) as Arc<dyn datafusion::datasource::TableProvider>;
    let ctx = SessionContext::new();
    ctx.register_table("logs", provider)?;

    assert_eq!(count_rows(&ctx, "SELECT body FROM logs").await?, 4);
    assert_eq!(
        count_rows(&ctx, "SELECT body FROM logs WHERE label_env = 'prod'").await?,
        2,
        "pre-existing rows must be backfilled from the attributes map"
    );
    assert_eq!(
        count_rows(&ctx, "SELECT body FROM logs WHERE label_env = 'staging'").await?,
        1
    );
    assert_eq!(
        count_rows(&ctx, "SELECT body FROM logs WHERE label_env IS NULL").await?,
        1,
        "rows without the attribute stay null"
    );
    // The source attributes are still intact after the rewrite.
    assert_eq!(
        count_rows(
            &ctx,
            "SELECT body FROM logs WHERE log_attributes['env'] = 'prod'"
        )
        .await?,
        2
    );

    Ok(())
}

#[tokio::test]
async fn dry_run_promotion_changes_nothing() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let (catalog_manager, service_catalog, identifier) = setup(true).await?;

    run_compaction(catalog_manager.clone(), service_catalog).await?;

    // Schema untouched: no label columns, no extra schema version.
    let table = load_table(&catalog_manager, &identifier).await?;
    let schema = table.current_schema()?;
    assert!(
        !schema.fields().iter().any(|f| f.name.starts_with("label_")),
        "dry run must not evolve the schema"
    );
    assert_eq!(table.metadata().schemas.len(), 1);

    // Compaction itself still worked and preserved the data.
    let provider = Arc::new(datafusion_iceberg::DataFusionTable::new(
        Tabular::Table(table),
        None,
        None,
        None,
    )) as Arc<dyn datafusion::datasource::TableProvider>;
    let ctx = SessionContext::new();
    ctx.register_table("logs", provider)?;
    assert_eq!(count_rows(&ctx, "SELECT body FROM logs").await?, 4);

    Ok(())
}
