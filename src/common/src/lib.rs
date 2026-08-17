pub mod attr_demand;
pub mod attrs;
pub mod auth;
pub mod bootstrap;
pub mod catalog;
pub mod catalog_manager;
pub mod cli;
pub mod config;
pub mod datafusion_runtime;
pub mod dataset;
pub mod error;
pub mod flight;
pub mod iceberg;
pub mod model;
pub mod parquet_metadata_cache;
pub mod profile;
pub mod query_ir;
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
