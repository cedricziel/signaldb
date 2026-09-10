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
#[cfg(any(test, feature = "testing-containers"))]
mod containers;
mod flush;
mod otel_capture;
mod otlp_fixtures;
mod temp_catalog;
mod tracing_fallback;

pub use config_builder::TestConfigBuilder;
#[cfg(any(test, feature = "testing-containers"))]
pub use containers::start_container_with_retry;
pub use flush::flush_storage_writers;
pub use otel_capture::OtelExportProbe;
pub use otlp_fixtures::{
    sample_logs_request, sample_metrics_request, sample_trace_request, string_attr,
};
pub use temp_catalog::TempCatalog;
pub use tracing_fallback::install_global_tracing_fallback;
