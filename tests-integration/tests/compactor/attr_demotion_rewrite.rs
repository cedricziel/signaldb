//! Rewrite-coupled attribute demotion (epic #737, #734 P3)
//!
//! End-to-end test of the active (non-dry-run) demotion path: a table
//! that already carries a materialized `label_env` column but has zero
//! recorded query demand for `env` runs through a compaction, which must
//! drop the column via schema evolution (AddSchema + SetCurrentSchema)
//! before the rewrite so the new files stop carrying it — while the
//! attribute values stay queryable through the map-typed attributes
//! column. A pinned `[schema.materialized_labels]` entry must never be
//! demoted, and with `dry_run = true` the same setup must change nothing.

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

/// A logs-shaped table that already carries a materialized `label_env`
/// column (id 8, after the map's nested key/value ids 6 and 7), as a
/// previous auto-promotion would have left it.
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
            StructField {
                id: 8,
                name: "label_env".to_string(),
                required: false,
                field_type: Type::Primitive(PrimitiveType::String),
                // Origin-key doc, matching exactly what `add_label_columns`
                // stamps on a real auto-promoted column -- this is the
                // authoritative key->column record `remove_label_columns`
                // (#814) looks up by, not the field name.
                doc: Some(common::iceberg::evolution::label_doc("env")),
                initial_default: None,
                write_default: None,
            },
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

/// One test row: (timestamp, service, body, attributes, label_env value).
type TestRow<'a> = (
    i64,
    &'a str,
    &'a str,
    &'a [(&'a str, &'a str)],
    Option<&'a str>,
);

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
    let mut labels = Vec::new();
    for (ts, service, body, kvs, label_env) in rows {
        timestamps.push(*ts);
        services.push(Some(*service));
        severities.push(Some("INFO"));
        bodies.push(Some(*body));
        labels.push(*label_env);
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
        Field::new("label_env", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        batch_schema,
        vec![
            Arc::new(ts),
            Arc::new(StringArray::from(services)),
            Arc::new(StringArray::from(severities)),
            Arc::new(StringArray::from(bodies)),
            Arc::new(attrs),
            Arc::new(StringArray::from(labels)),
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
/// configured, a logs table that already carries `label_env`, two small
/// files whose rows have `env`/`pod` attributes plus materialized
/// `label_env` values — and NO query demand recorded for anything, so
/// `env` is a demotion candidate. `pinned` puts `env` on the logs
/// `[schema.materialized_labels]` allowlist.
async fn setup(
    dry_run: bool,
    pinned: bool,
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
    if pinned {
        config.schema.materialized_labels.logs = vec!["env".to_string()];
    }
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

    // Two files -> the executor has something to compact. Every row
    // carries at least one attribute (see the promotion test for the
    // empty-map read-back quirk). `env` appears in 3 of 4 rows and its
    // label column is populated.
    write_file(
        &catalog_manager,
        &identifier,
        &[
            (
                1_000_000,
                "api",
                "hello prod",
                &[("env", "prod"), ("pod", "api-1")],
                Some("prod"),
            ),
            (
                2_000_000,
                "api",
                "hello staging",
                &[("env", "staging")],
                Some("staging"),
            ),
        ],
    )
    .await?;
    write_file(
        &catalog_manager,
        &identifier,
        &[
            (3_000_000, "web", "no env here", &[("pod", "web-1")], None),
            (
                4_000_000,
                "web",
                "hello prod again",
                &[("env", "prod")],
                Some("prod"),
            ),
        ],
    )
    .await?;

    // No query demand for any key: the scan-side stats persisted during
    // the compaction give `env` query_hits = 0, making the materialized
    // column a demotion candidate.
    let service_catalog = Arc::new(common::catalog::Catalog::new_in_memory().await?);

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

fn register(table: Table, ctx: &SessionContext) -> Result<()> {
    let provider = Arc::new(datafusion_iceberg::DataFusionTable::new(
        Tabular::Table(table),
        None,
        None,
        None,
    )) as Arc<dyn datafusion::datasource::TableProvider>;
    ctx.register_table("logs", provider)?;
    Ok(())
}

#[tokio::test]
async fn active_demotion_drops_unqueried_column_and_keeps_data_queryable() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let (catalog_manager, service_catalog, identifier) = setup(false, false).await?;

    run_compaction(catalog_manager.clone(), service_catalog).await?;

    // Schema pruned: `label_env` is gone, the base columns survive.
    let table = load_table(&catalog_manager, &identifier).await?;
    let schema = table.current_schema()?;
    assert!(
        !schema.fields().iter().any(|f| f.name == "label_env"),
        "unqueried materialized column must be demoted"
    );
    assert!(
        schema.fields().iter().any(|f| f.name == "log_attributes"),
        "attributes map must survive the demotion"
    );
    assert_eq!(table.metadata().current_schema_id, 1);
    assert_eq!(table.metadata().schemas.len(), 2);

    // The rewritten files no longer carry the column, and the data is
    // still fully queryable through the attributes map — demotion loses
    // nothing.
    let ctx = SessionContext::new();
    register(table, &ctx)?;
    assert_eq!(count_rows(&ctx, "SELECT body FROM logs").await?, 4);
    assert_eq!(
        count_rows(
            &ctx,
            "SELECT body FROM logs WHERE log_attributes['env'] = 'prod'"
        )
        .await?,
        2,
        "attribute values must stay queryable via the map after demotion"
    );
    assert_eq!(
        count_rows(
            &ctx,
            "SELECT body FROM logs WHERE log_attributes['env'] = 'staging'"
        )
        .await?,
        1
    );
    assert!(
        ctx.sql("SELECT label_env FROM logs").await.is_err(),
        "the demoted column must not be projectable any more"
    );

    Ok(())
}

#[tokio::test]
async fn pinned_label_is_never_demoted_even_with_zero_demand() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let (catalog_manager, service_catalog, identifier) = setup(false, true).await?;

    run_compaction(catalog_manager.clone(), service_catalog).await?;

    // `env` is pinned via [schema.materialized_labels]: no demotion, no
    // schema churn.
    let table = load_table(&catalog_manager, &identifier).await?;
    let schema = table.current_schema()?;
    assert!(
        schema.fields().iter().any(|f| f.name == "label_env"),
        "pinned label must never be demoted"
    );
    assert_eq!(table.metadata().schemas.len(), 1, "no schema was committed");

    // Still queryable through the materialized column.
    let ctx = SessionContext::new();
    register(table, &ctx)?;
    assert_eq!(count_rows(&ctx, "SELECT body FROM logs").await?, 4);
    assert_eq!(
        count_rows(&ctx, "SELECT body FROM logs WHERE label_env = 'prod'").await?,
        2
    );

    Ok(())
}

#[tokio::test]
async fn dry_run_demotion_changes_nothing() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let (catalog_manager, service_catalog, identifier) = setup(true, false).await?;

    run_compaction(catalog_manager.clone(), service_catalog).await?;

    // Schema untouched: the column survives and no schema was committed.
    let table = load_table(&catalog_manager, &identifier).await?;
    let schema = table.current_schema()?;
    assert!(
        schema.fields().iter().any(|f| f.name == "label_env"),
        "dry run must not demote"
    );
    assert_eq!(table.metadata().schemas.len(), 1);

    // Compaction itself still worked and preserved the data, column
    // included.
    let ctx = SessionContext::new();
    register(table, &ctx)?;
    assert_eq!(count_rows(&ctx, "SELECT body FROM logs").await?, 4);
    assert_eq!(
        count_rows(&ctx, "SELECT body FROM logs WHERE label_env = 'prod'").await?,
        2
    );

    Ok(())
}
