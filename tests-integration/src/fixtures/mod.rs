//! Test fixtures for integration testing
//!
//! This module provides reusable test contexts for catalog, storage,
//! and complete retention testing scenarios.

mod catalog_context;
mod retention_context;
mod storage_context;

pub use catalog_context::CatalogTestContext;
pub use retention_context::{
    DataGeneratorConfig, PartitionGranularity, PartitionInfo, RetentionTestContext, TableInfo,
};
pub use storage_context::StorageTestContext;
