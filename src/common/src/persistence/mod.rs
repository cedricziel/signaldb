use std::sync::Arc;

use datafusion::{
    datasource::{file_format::parquet::ParquetFormat, listing::ListingOptions},
    error::DataFusionError,
    execution::object_store::ObjectStoreUrl,
    prelude::SessionContext,
};
use object_store::local::LocalFileSystem;

use crate::config::CONFIG;

pub async fn create_df_session() -> Result<SessionContext, DataFusionError> {
    let config = CONFIG.get().expect("Failed to load configuration");
    let storage_url = config.default_storage_url();
    let storage_prefix = config.default_storage_prefix();

    let file_format = ParquetFormat::default().with_enable_pruning(true);
    let listing_options = ListingOptions::new(Arc::new(file_format))
        .with_table_partition_cols(vec![])
        .with_file_extension(".parquet")
        .with_collect_stat(true);

    // if storage_prefix is an absolute path, use as is, if it's a relative path,
    // get the current working directory and append the relative path.
    let storage_prefix = if storage_prefix.starts_with("/") {
        storage_prefix
    } else {
        let cwd = std::env::current_dir().expect("Failed to get current working directory");
        cwd.join(storage_prefix).to_string_lossy().to_string()
    };

    let object_store_url = ObjectStoreUrl::parse(storage_url.clone()).unwrap();
    let object_store = LocalFileSystem::new_with_prefix(storage_prefix.clone()).expect(
        format!(
            "Failed to create object store for {}",
            storage_prefix.as_str()
        )
        .as_str(),
    );

    let ctx = SessionContext::new();
    ctx.register_object_store(object_store_url.as_ref(), Arc::new(object_store));
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
