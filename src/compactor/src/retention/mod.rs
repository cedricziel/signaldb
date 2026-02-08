//! Retention enforcement system for SignalDB Compactor Phase 3.
//!
//! This module provides the configuration and policy resolution logic for
//! time-based retention of observability data. It supports a three-tier
//! override hierarchy (Global → Tenant → Dataset) with per-signal-type
//! retention policies.
//!
//! ## Architecture
//!
//! - `config`: Configuration structures with serde support for TOML/env vars
//! - `policy`: Policy resolution logic with override hierarchy
//!
//! ## Usage
//!
//! ```no_run
//! use compactor::retention::{RetentionConfig, RetentionPolicyResolver, SignalType};
//!
//! let config = RetentionConfig::default();
//! let resolver = RetentionPolicyResolver::new(config)?;
//!
//! let cutoff = resolver.compute_cutoff(
//!     "tenant-id",
//!     "dataset-id",
//!     SignalType::Traces,
//! )?;
//!
//! println!("Retention cutoff: {}", cutoff.cutoff_timestamp);
//! # Ok::<(), anyhow::Error>(())
//! ```

pub mod config;
pub mod policy;

// Re-export commonly used types
pub use config::{
    DatasetRetentionConfig, RetentionConfig, RetentionPolicySource, SignalType,
    TenantRetentionConfig,
};
pub use policy::{RetentionCutoff, RetentionPolicyResolver};
