//! Test utilities for SignalDB.
//!
//! This module provides reusable test utilities for creating test configurations,
//! setting up in-memory stores, and other common test operations.
//!
//! # Feature Flag
//!
//! This module is only available when the `testing` feature is enabled or during tests:
//!
//! ```toml
//! [dependencies]
//! common = { path = "../common", features = ["testing"] }
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use common::testing::TestConfigBuilder;
//!
//! let config = TestConfigBuilder::new()
//!     .in_memory()
//!     .with_tenant("acme", "prod")
//!     .build();
//! ```

mod config_builder;
mod flush;
mod otel_capture;
mod temp_catalog;

pub use config_builder::TestConfigBuilder;
pub use flush::flush_storage_writers;
pub use otel_capture::OtelExportProbe;
pub use temp_catalog::TempCatalog;
