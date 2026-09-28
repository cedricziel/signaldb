//! Rewrite-coupled typed per-level attribute demotion (otel-native-schema
//! layer 6, D4: budgeted LRU demotion).
//!
//! A promoted `attr_record_<key>` column is a redundant typed COPY of the
//! key's home in the typed map -- the map keeps every value, so dropping
//! the column loses nothing and the querier falls back to the map. These
//! tests exercise the active (non-dry-run) demotion path end to end: an
//! idle column is folded back on the next compaction, an over-budget
//! table drops its least-recently-queried promoted column first, and
//! `dry_run = true` changes nothing.

use anyhow::Result;
use common::catalog_manager::CatalogManager;
use common::iceberg::evolution;
use common::schema::logical::AttributeLevel;
use common::schema::type_authority::CanonicalType;
use compactor::executor::{CompactionExecutor, CompactionStatus, ExecutorConfig};
use compactor::metrics::CompactionMetrics;
use compactor::planner::{CompactionCandidate, PartitionStats};
use datafusion::arrow::array::{
    Array, ArrayRef, Int64Array, Int64Builder, MapBuilder, MapFieldNames, RecordBatch, StringArray,
    StringBuilder, TimestampMicrosecondArray, new_null_array,
};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::functions::core::expr_fn::get_field;
use datafusion::logical_expr::col;
use datafusion::prelude::SessionContext;
use futures::stream;
use iceberg_rust::arrow::write::write_parquet_partitioned;
use iceberg_rust::catalog::create::CreateTableBuilder;
use iceberg_rust::catalog::identifier::Identifier;
use iceberg_rust::catalog::tabular::Tabular;
use iceberg_rust::spec::schema::Schema as IcebergSchema;
use iceberg_rust::spec::types::{PrimitiveType, StructField, StructType, Type};
use iceberg_rust::table::Table;
use std::collections::HashMap;
use std::sync::Arc;
use tests_integration::compaction_helpers::{
    busiest_partition, hour_partition_spec, load_table_by_identifier, retype_map, string_field,
    typed_container_fields,
};

const TENANT: &str = "t1";
const DATASET: &str = "d1";
const TABLE: &str = "logs";

/// A typed-layout logs table: the real sort columns plus a full typed
/// `log_attributes` container (record level) -- the only level these
/// tests promote a column at.
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
    fields.extend(typed_container_fields(5, "log_attributes"));
    IcebergSchema::from_struct_type(StructType::new(fields), 0, None)
}

/// One test row: (timestamp, body, `retries`, `attempts`) -- both
/// record-level `Int64` keys, homed in `log_attributes_int`.
type TestRow<'a> = (i64, &'a str, Option<i64>, Option<i64>);

/// Write one data file with the given rows. Every home the fixture
/// doesn't populate (including any promoted attribute column the current
/// schema has evolved since table creation) is filled with an all-null
/// array of the right shape -- exactly what an un-reloaded writer would
/// produce.
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

    let field = arrow_schema.field_with_name("log_attributes_int")?;
    let DataType::Map(entry_field, _) = field.data_type() else {
        anyhow::bail!("log_attributes_int should convert to an Arrow Map");
    };
    let DataType::Struct(kv_fields) = entry_field.data_type() else {
        anyhow::bail!("log_attributes_int entries should be a struct");
    };
    let map_field_names = MapFieldNames {
        entry: entry_field.name().clone(),
        key: kv_fields[0].name().clone(),
        value: kv_fields[1].name().clone(),
    };

    let mut ints = MapBuilder::new(
        Some(map_field_names),
        StringBuilder::new(),
        Int64Builder::new(),
    );
    let mut timestamps = Vec::new();
    let mut bodies = Vec::new();
    for (ts, body, retries, attempts) in rows {
        timestamps.push(*ts);
        bodies.push(Some(*body));
        // A zero-entry (but non-null) map row currently reads back with the
        // whole map column empty across every row of the batch (upstream
        // iceberg-rust quirk) -- a constant sentinel key keeps every row's
        // map non-empty even when neither key is present.
        ints.keys().append_value("_sentinel");
        ints.values().append_value(0);
        if let Some(v) = retries {
            ints.keys().append_value("retries");
            ints.values().append_value(*v);
        }
        if let Some(v) = attempts {
            ints.keys().append_value("attempts");
            ints.values().append_value(*v);
        }
        ints.append(true)?;
    }
    // Re-typed onto the schema's own field: a `MapBuilder`-built array's
    // key/value fields carry no `PARQUET:field_id` metadata, which the
    // Iceberg-derived `arrow_schema` requires.
    let ints = retype_map(
        ints.finish(),
        arrow_schema.field_with_name("log_attributes_int")?,
    )?;
    let ts = TimestampMicrosecondArray::from(timestamps);
    let len = rows.len();

    let mut ints = Some(ints);
    let mut bodies = Some(bodies);
    let mut ts = Some(ts);
    let columns: Vec<ArrayRef> = arrow_schema
        .fields()
        .iter()
        .map(|field| match field.name().as_str() {
            "timestamp" => Arc::new(ts.take().unwrap()) as ArrayRef,
            "body" => Arc::new(StringArray::from(bodies.take().unwrap())) as ArrayRef,
            "log_attributes_int" => Arc::new(ints.take().unwrap()) as ArrayRef,
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

fn attr_config(dry_run: bool, max_labels_per_table: usize) -> common::config::AttrPromotionConfig {
    common::config::AttrPromotionConfig {
        enabled: true,
        dry_run,
        max_labels_per_table,
        demote_after_idle: std::time::Duration::from_secs(7 * 24 * 3600),
        ..Default::default()
    }
}

/// A fresh in-memory catalog with a typed-layout logs table (three rows:
/// `retries` 3/7/null, `attempts` always null) and no promoted columns yet.
async fn setup(
    dry_run: bool,
    max_labels_per_table: usize,
) -> Result<(
    Arc<CatalogManager>,
    Arc<common::catalog::Catalog>,
    Identifier,
)> {
    let mut config = common::testing::TestConfigBuilder::new()
        .in_memory()
        .with_tenant(TENANT, DATASET)
        .build();
    config.compactor.attr_promotion = attr_config(dry_run, max_labels_per_table);
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
            (1_000_000, "r3", Some(3), None),
            (2_000_000, "r7", Some(7), None),
        ],
    )
    .await?;
    write_file(
        &catalog_manager,
        &identifier,
        &[(3_000_000, "rnull", None, None)],
    )
    .await?;

    let service_catalog = Arc::new(common::catalog::Catalog::new_in_memory().await?);
    Ok((catalog_manager, service_catalog, identifier))
}

/// Promote `keys` (already-known canonical type `Int64`) directly, without
/// going through a compaction cycle -- these tests exercise demotion, not
/// promotion, and only need an already-promoted column to demote.
async fn promote_columns(
    catalog_manager: &CatalogManager,
    identifier: &Identifier,
    keys: &[&str],
) -> Result<()> {
    let attrs: Vec<(AttributeLevel, String, CanonicalType)> = keys
        .iter()
        .map(|k| (AttributeLevel::Record, k.to_string(), CanonicalType::Int64))
        .collect();
    evolution::add_promoted_attr_columns(catalog_manager.catalog(), identifier, &attrs).await?;
    Ok(())
}

/// Seed one (level, key)'s query demand with an exact `last_queried_at`:
/// the very first hit for a key is a plain insert (no prior row to take
/// the later-of with), so this pins the timestamp precisely rather than
/// only ever moving it forward.
async fn seed_query_demand(
    service_catalog: &common::catalog::Catalog,
    key: &str,
    queried_at: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    service_catalog
        .add_attribute_level_query_hits(
            TENANT,
            DATASET,
            "logs",
            AttributeLevel::Record,
            key,
            5,
            queried_at,
        )
        .await?;
    Ok(())
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
            total_size_bytes: 2048,
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

/// `retries`'s value per `body`, read from its typed-map home
/// (`log_attributes_int`) via `get_field` -- independent of whether a
/// promoted `attr_record_retries` column exists, so the same call proves
/// the map is queryable both before and after demotion.
async fn retries_by_body(table: Table) -> Result<HashMap<String, Option<i64>>> {
    let ctx = register_table(table)?;
    let batches = ctx
        .table("logs")
        .await?
        .select(vec![
            col("body"),
            get_field(col("log_attributes_int"), "retries").alias("retries"),
        ])?
        .collect()
        .await?;
    let mut result = HashMap::new();
    for batch in &batches {
        let bodies = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let retries = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            let value = (!retries.is_null(i)).then(|| retries.value(i));
            result.insert(bodies.value(i).to_string(), value);
        }
    }
    Ok(result)
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
}

#[tokio::test]
async fn idle_demotion_folds_back_into_the_typed_map() -> Result<()> {
    init_tracing();
    let (catalog_manager, service_catalog, identifier) = setup(false, 8).await?;
    promote_columns(&catalog_manager, &identifier, &["retries"]).await?;
    seed_query_demand(
        &service_catalog,
        "retries",
        chrono::Utc::now() - chrono::Duration::days(30),
    )
    .await?;

    let table = load_table_by_identifier(&catalog_manager, &identifier).await?;
    let before = retries_by_body(table).await?;

    run_compaction(catalog_manager.clone(), service_catalog).await?;

    let table = load_table_by_identifier(&catalog_manager, &identifier).await?;
    let schema = table.current_schema()?;
    assert!(
        !schema
            .fields()
            .iter()
            .any(|f| f.name == "attr_record_retries"),
        "an idle promoted column must be dropped from the current schema"
    );

    let after = retries_by_body(table).await?;
    assert_eq!(
        before, after,
        "the typed map must still hold every value after demotion"
    );
    assert_eq!(after.get("r3"), Some(&Some(3)));
    assert_eq!(after.get("r7"), Some(&Some(7)));
    assert_eq!(after.get("rnull"), Some(&None));

    Ok(())
}

#[tokio::test]
async fn over_budget_demotes_the_less_recently_queried_column_first() -> Result<()> {
    init_tracing();
    let (catalog_manager, service_catalog, identifier) = setup(false, 1).await?;
    promote_columns(&catalog_manager, &identifier, &["retries", "attempts"]).await?;
    // Both recently queried (well inside the 7-day idle window), so only
    // the budget of 1 forces a demotion -- the older of the two.
    seed_query_demand(
        &service_catalog,
        "retries",
        chrono::Utc::now() - chrono::Duration::minutes(1),
    )
    .await?;
    seed_query_demand(
        &service_catalog,
        "attempts",
        chrono::Utc::now() - chrono::Duration::hours(1),
    )
    .await?;

    run_compaction(catalog_manager.clone(), service_catalog).await?;

    let table = load_table_by_identifier(&catalog_manager, &identifier).await?;
    let schema = table.current_schema()?;
    assert!(
        schema
            .fields()
            .iter()
            .any(|f| f.name == "attr_record_retries"),
        "the more recently queried column must survive within budget"
    );
    assert!(
        !schema
            .fields()
            .iter()
            .any(|f| f.name == "attr_record_attempts"),
        "the less recently queried column must be demoted to stay within budget"
    );

    Ok(())
}

#[tokio::test]
async fn dry_run_demotion_changes_nothing() -> Result<()> {
    init_tracing();
    let (catalog_manager, service_catalog, identifier) = setup(true, 8).await?;
    promote_columns(&catalog_manager, &identifier, &["retries"]).await?;
    seed_query_demand(
        &service_catalog,
        "retries",
        chrono::Utc::now() - chrono::Duration::days(30),
    )
    .await?;

    run_compaction(catalog_manager.clone(), service_catalog).await?;

    let table = load_table_by_identifier(&catalog_manager, &identifier).await?;
    let schema = table.current_schema()?;
    assert!(
        schema
            .fields()
            .iter()
            .any(|f| f.name == "attr_record_retries"),
        "dry run must not drop the promoted column"
    );
    assert_eq!(
        table.metadata().schemas.len(),
        2,
        "no additional schema commit"
    );

    Ok(())
}
