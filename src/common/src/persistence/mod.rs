use std::sync::Arc;

use arrow_array::RecordBatch;
use datafusion::{
    datasource::{file_format::parquet::ParquetFormat, listing::ListingOptions},
    error::DataFusionError,
    parquet::{
        arrow::{async_writer::ParquetObjectWriter, AsyncArrowWriter},
        file::properties::{WriterProperties, WriterVersion},
    },
    prelude::SessionContext,
};
use object_store::{local::LocalFileSystem, path::Path, ObjectStore};

use crate::config::CONFIG;

pub async fn create_session_context() -> Result<SessionContext, DataFusionError> {
    let storage_url = CONFIG
        .get()
        .unwrap()
        .default_storage_url()
        .parse()
        .expect("Failed to parse storage URL");

    let storage_prefix = CONFIG.get().unwrap().default_storage_prefix();

    let object_store = LocalFileSystem::new_with_prefix(storage_prefix.clone()).expect(
        format!(
            "Failed to create local filesystem with prefix {}",
            storage_prefix
        )
        .as_str(),
    );

    let ctx = SessionContext::new();
    ctx.runtime_env()
        .register_object_store(&storage_url, Arc::new(object_store));

    let file_format = ParquetFormat::default().with_enable_pruning(true);
    let listing_options =
        ListingOptions::new(Arc::new(file_format)).with_file_extension(".parquet");

    ctx.register_listing_table(
        "traces",
        format!("{}/ds/traces/", storage_url),
        listing_options,
        None,
        None,
    )
    .await?;

    Ok(ctx)
}

pub async fn write_batch_to_object_store(
    object_store: Arc<dyn ObjectStore>,
    path: &str,
    batch: RecordBatch,
) -> Result<(), Box<dyn std::error::Error>> {
    let path = object_store::path::Path::from(path);

    let props = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .build();

    let schema = batch.schema();

    let object_store_writer = ParquetObjectWriter::new(object_store.clone(), Path::from(path));

    let mut arrow_writer = AsyncArrowWriter::try_new(object_store_writer, schema, Some(props))
        .expect("Error creating parquet writer");

    arrow_writer.write(&batch).await?;
    arrow_writer.close().await?;

    Ok(())
}
