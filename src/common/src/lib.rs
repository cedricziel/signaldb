pub mod auth;
pub mod catalog;
pub mod catalog_manager;
pub mod cli;
pub mod config;
pub mod dataset;
pub mod flight;
pub mod iceberg;
pub mod model;
pub mod schema;
pub mod self_monitoring;
pub mod service_bootstrap;
pub mod storage;
pub mod tenant_api;
pub mod wal;

#[cfg(any(test, feature = "testing"))]
pub mod testing;

pub use catalog_manager::CatalogManager;
