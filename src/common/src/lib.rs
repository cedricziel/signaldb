pub mod attr_demand;
pub mod attrs;
pub mod auth;
pub mod bootstrap;
pub mod catalog;
pub mod catalog_manager;
pub mod cli;
pub mod config;
pub mod datafusion_runtime;
pub mod discovery;
pub mod endpoints;
pub mod error;
pub mod flight;
pub mod iceberg;
pub mod model;
pub mod parquet_metadata_cache;
pub mod profile;
/// The query IR lives in its own leaf crate (`serde` only) so a document can be
/// built and validated without linking the query engine. Re-exported here
/// because `common::query_ir::…` is how the rest of the workspace addresses it.
pub use query_ir;
pub mod ratelimit;
pub mod schema;
pub mod schema_registry;
pub mod self_monitoring;
pub mod service_bootstrap;
pub mod storage;
pub mod storage_usage;
pub mod tenant_api;
pub mod wal;

#[cfg(any(test, feature = "testing"))]
pub mod testing;

pub use catalog_manager::{CatalogManager, ResolvedDataset, ResolvedTenant};
