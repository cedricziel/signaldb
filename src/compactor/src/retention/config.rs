//! Retention configuration structures. The types live in
//! [`common::retention`] so the router resolves the same policy; this module
//! re-exports them for the compactor.

pub use common::retention::{
    DatasetRetentionConfig, ResolvedRetention, RetentionConfig, RetentionConfigError,
    RetentionOverride, RetentionPolicySource, SignalType, TenantRetentionConfig,
};
