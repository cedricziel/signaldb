//! Retention policy resolution logic with override hierarchy.

use super::config::{
    RetentionConfig, RetentionConfigError, RetentionOverride, RetentionPolicySource, SignalType,
};
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use std::time::Duration;
use thiserror::Error;

/// Computed retention cutoff for a specific tenant/dataset/signal combination.
#[derive(Debug, Clone)]
pub struct RetentionCutoff {
    /// Tenant ID.
    pub tenant_id: String,

    /// Dataset ID.
    pub dataset_id: String,

    /// Signal type.
    pub signal_type: SignalType,

    /// Cutoff timestamp (data older than this should be dropped).
    pub cutoff_timestamp: DateTime<Utc>,

    /// The retention period used (after applying overrides).
    pub retention_period: Duration,

    /// Source of the retention period (for auditing).
    pub source: RetentionPolicySource,
}

impl RetentionCutoff {
    /// Check if a partition timestamp is expired.
    pub fn is_expired(&self, partition_timestamp: DateTime<Utc>) -> bool {
        partition_timestamp < self.cutoff_timestamp
    }

    /// Human-readable representation for logging.
    pub fn display(&self) -> String {
        format!(
            "tenant={}, dataset={}, signal={}, cutoff={}, period={}, source={:?}",
            self.tenant_id,
            self.dataset_id,
            self.signal_type,
            self.cutoff_timestamp.to_rfc3339(),
            humantime::format_duration(self.retention_period),
            self.source
        )
    }
}

/// Resolves retention policies with override hierarchy.
///
/// Resolution order:
/// 1. Dataset-specific override (if exists)
/// 2. Tenant-specific override (if exists)
/// 3. Global default
#[derive(Debug)]
pub struct RetentionPolicyResolver {
    config: RetentionConfig,
    tz: Tz,
}

impl RetentionPolicyResolver {
    /// Create a new resolver from configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The timezone string cannot be parsed
    /// - The configuration validation fails
    pub fn new(config: RetentionConfig) -> Result<Self, RetentionPolicyError> {
        // Validate configuration
        config.validate().map_err(|e| {
            RetentionPolicyError::InvalidConfiguration(format!(
                "Configuration validation failed: {e}"
            ))
        })?;

        // Parse timezone from config
        let tz =
            config
                .timezone
                .parse::<Tz>()
                .map_err(|e| RetentionPolicyError::InvalidTimezone {
                    timezone: config.timezone.clone(),
                    message: e.to_string(),
                })?;

        Ok(Self { config, tz })
    }

    /// Compute retention cutoff for a specific context.
    ///
    /// Resolution order:
    /// 1. Dataset-specific override (if exists)
    /// 2. Tenant-specific override (if exists)
    /// 3. Global default
    ///
    /// The grace period is added to the retention period for safety.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Retention period overflow occurs
    /// - Cutoff timestamp underflow occurs
    pub fn compute_cutoff(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        signal_type: SignalType,
    ) -> Result<RetentionCutoff, RetentionPolicyError> {
        // Get the current time in UTC (internal computations always in UTC)
        let now = Utc::now();

        // Resolve retention period with override hierarchy
        let (retention_period, source) =
            self.resolve_retention_period(tenant_id, dataset_id, signal_type);

        // Apply grace period for safety
        let effective_retention = retention_period
            .checked_add(self.config.grace_period)
            .ok_or(RetentionPolicyError::RetentionPeriodOverflow {
                signal_type,
                retention_period,
                grace_period: self.config.grace_period,
            })?;

        // Compute cutoff timestamp
        let cutoff_timestamp =
            now.checked_sub_signed(chrono::Duration::from_std(effective_retention).map_err(
                |e| RetentionPolicyError::DurationConversionError {
                    duration: effective_retention,
                    message: e.to_string(),
                },
            )?)
            .ok_or_else(|| RetentionPolicyError::CutoffUnderflow {
                tenant_id: tenant_id.to_string(),
                dataset_id: dataset_id.to_string(),
                signal_type,
                effective_retention,
            })?;

        Ok(RetentionCutoff {
            tenant_id: tenant_id.to_string(),
            dataset_id: dataset_id.to_string(),
            signal_type,
            cutoff_timestamp,
            retention_period,
            source,
        })
    }

    /// Resolve retention period with override hierarchy.
    ///
    /// Returns the resolved retention period and its source.
    fn resolve_retention_period(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        signal_type: SignalType,
    ) -> (Duration, RetentionPolicySource) {
        // Check dataset-level override
        if let Some(tenant_config) = self.config.tenant_overrides.get(tenant_id) {
            if let Some(dataset_config) = tenant_config.dataset_overrides.get(dataset_id)
                && let Some(period) = self.get_signal_retention(dataset_config, signal_type)
            {
                return (period, RetentionPolicySource::Dataset);
            }

            // Check tenant-level override
            if let Some(period) = self.get_signal_retention(tenant_config, signal_type) {
                return (period, RetentionPolicySource::Tenant);
            }
        }

        // Fall back to global default
        let period = match signal_type {
            SignalType::Traces => self.config.traces,
            SignalType::Logs => self.config.logs,
            SignalType::Metrics => self.config.metrics,
        };

        (period, RetentionPolicySource::Global)
    }

    /// Extract signal-specific retention from config.
    fn get_signal_retention(
        &self,
        config: &impl RetentionOverride,
        signal_type: SignalType,
    ) -> Option<Duration> {
        match signal_type {
            SignalType::Traces => config.traces(),
            SignalType::Logs => config.logs(),
            SignalType::Metrics => config.metrics(),
        }
    }

    /// Get the configured timezone (for logging/display purposes).
    pub fn timezone(&self) -> &Tz {
        &self.tz
    }
}

/// Errors that can occur during retention policy resolution.
#[derive(Error, Debug)]
pub enum RetentionPolicyError {
    /// Configuration validation failed.
    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),

    /// Invalid timezone specified.
    #[error("Invalid timezone '{timezone}': {message}")]
    InvalidTimezone { timezone: String, message: String },

    /// Retention period overflow when adding grace period.
    #[error(
        "Retention period overflow for {signal_type}: {retention_period:?} + grace period {grace_period:?}"
    )]
    RetentionPeriodOverflow {
        signal_type: SignalType,
        retention_period: Duration,
        grace_period: Duration,
    },

    /// Cutoff timestamp underflow.
    #[error(
        "Cutoff timestamp underflow for {tenant_id}/{dataset_id}/{signal_type} with effective retention {effective_retention:?}"
    )]
    CutoffUnderflow {
        tenant_id: String,
        dataset_id: String,
        signal_type: SignalType,
        effective_retention: Duration,
    },

    /// Duration conversion error.
    #[error("Duration conversion error for {duration:?}: {message}")]
    DurationConversionError { duration: Duration, message: String },
}

// Convert from config error to policy error
impl From<RetentionConfigError> for RetentionPolicyError {
    fn from(err: RetentionConfigError) -> Self {
        RetentionPolicyError::InvalidConfiguration(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn create_default_config() -> RetentionConfig {
        RetentionConfig::default()
    }

    #[test]
    fn test_retention_cutoff_global_default() {
        let config = create_default_config();
        let resolver = RetentionPolicyResolver::new(config).unwrap();

        let cutoff = resolver
            .compute_cutoff("tenant1", "dataset1", SignalType::Traces)
            .unwrap();

        assert_eq!(cutoff.tenant_id, "tenant1");
        assert_eq!(cutoff.dataset_id, "dataset1");
        assert_eq!(cutoff.signal_type, SignalType::Traces);
        assert_eq!(cutoff.source, RetentionPolicySource::Global);
        assert_eq!(cutoff.retention_period, Duration::from_secs(7 * 24 * 3600));

        // Cutoff should be approximately 7 days + 1 hour ago (with grace period)
        let expected_cutoff = Utc::now() - chrono::Duration::seconds((7 * 24 + 1) * 3600);
        let diff = (cutoff.cutoff_timestamp - expected_cutoff)
            .num_seconds()
            .abs();
        assert!(diff < 2, "Cutoff should be approximately 7d 1h ago");
    }

    #[test]
    fn test_retention_cutoff_tenant_override() {
        let mut config = create_default_config();
        config.tenant_overrides.insert(
            "acme".to_string(),
            super::super::config::TenantRetentionConfig {
                traces: Some(Duration::from_secs(14 * 24 * 3600)),
                logs: None,
                metrics: None,
                dataset_overrides: HashMap::new(),
            },
        );

        let resolver = RetentionPolicyResolver::new(config).unwrap();
        let cutoff = resolver
            .compute_cutoff("acme", "prod", SignalType::Traces)
            .unwrap();

        assert_eq!(cutoff.source, RetentionPolicySource::Tenant);
        assert_eq!(cutoff.retention_period, Duration::from_secs(14 * 24 * 3600));
    }

    #[test]
    fn test_retention_cutoff_dataset_override() {
        let mut config = create_default_config();
        let mut tenant_config = super::super::config::TenantRetentionConfig {
            traces: Some(Duration::from_secs(14 * 24 * 3600)),
            logs: None,
            metrics: None,
            dataset_overrides: HashMap::new(),
        };
        tenant_config.dataset_overrides.insert(
            "production".to_string(),
            super::super::config::DatasetRetentionConfig {
                traces: Some(Duration::from_secs(30 * 24 * 3600)),
                logs: None,
                metrics: None,
            },
        );
        config
            .tenant_overrides
            .insert("acme".to_string(), tenant_config);

        let resolver = RetentionPolicyResolver::new(config).unwrap();
        let cutoff = resolver
            .compute_cutoff("acme", "production", SignalType::Traces)
            .unwrap();

        assert_eq!(cutoff.source, RetentionPolicySource::Dataset);
        assert_eq!(cutoff.retention_period, Duration::from_secs(30 * 24 * 3600));
    }

    #[test]
    fn test_override_hierarchy_precedence() {
        let mut config = create_default_config();

        // Global: 7 days
        // Tenant: 14 days
        // Dataset: 30 days

        let mut tenant_config = super::super::config::TenantRetentionConfig {
            traces: Some(Duration::from_secs(14 * 24 * 3600)),
            logs: None,
            metrics: None,
            dataset_overrides: HashMap::new(),
        };

        tenant_config.dataset_overrides.insert(
            "production".to_string(),
            super::super::config::DatasetRetentionConfig {
                traces: Some(Duration::from_secs(30 * 24 * 3600)),
                logs: None,
                metrics: None,
            },
        );

        config
            .tenant_overrides
            .insert("acme".to_string(), tenant_config);

        let resolver = RetentionPolicyResolver::new(config).unwrap();

        // Dataset override should win
        let cutoff_prod = resolver
            .compute_cutoff("acme", "production", SignalType::Traces)
            .unwrap();
        assert_eq!(cutoff_prod.source, RetentionPolicySource::Dataset);
        assert_eq!(
            cutoff_prod.retention_period,
            Duration::from_secs(30 * 24 * 3600)
        );

        // Tenant override should be used for non-production
        let cutoff_staging = resolver
            .compute_cutoff("acme", "staging", SignalType::Traces)
            .unwrap();
        assert_eq!(cutoff_staging.source, RetentionPolicySource::Tenant);
        assert_eq!(
            cutoff_staging.retention_period,
            Duration::from_secs(14 * 24 * 3600)
        );

        // Global default for other tenants
        let cutoff_other = resolver
            .compute_cutoff("other", "prod", SignalType::Traces)
            .unwrap();
        assert_eq!(cutoff_other.source, RetentionPolicySource::Global);
        assert_eq!(
            cutoff_other.retention_period,
            Duration::from_secs(7 * 24 * 3600)
        );
    }

    #[test]
    fn test_different_signal_types() {
        let config = create_default_config();
        let resolver = RetentionPolicyResolver::new(config).unwrap();

        let traces_cutoff = resolver
            .compute_cutoff("tenant1", "dataset1", SignalType::Traces)
            .unwrap();
        assert_eq!(
            traces_cutoff.retention_period,
            Duration::from_secs(7 * 24 * 3600)
        );

        let logs_cutoff = resolver
            .compute_cutoff("tenant1", "dataset1", SignalType::Logs)
            .unwrap();
        assert_eq!(
            logs_cutoff.retention_period,
            Duration::from_secs(30 * 24 * 3600)
        );

        let metrics_cutoff = resolver
            .compute_cutoff("tenant1", "dataset1", SignalType::Metrics)
            .unwrap();
        assert_eq!(
            metrics_cutoff.retention_period,
            Duration::from_secs(90 * 24 * 3600)
        );
    }

    #[test]
    fn test_invalid_timezone_config() {
        let mut config = create_default_config();
        config.timezone = "Invalid/Timezone".to_string();

        let result = RetentionPolicyResolver::new(config);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            RetentionPolicyError::InvalidTimezone { .. }
        ));
    }

    #[test]
    fn test_is_expired() {
        let config = create_default_config();
        let resolver = RetentionPolicyResolver::new(config).unwrap();

        let cutoff = resolver
            .compute_cutoff("tenant1", "dataset1", SignalType::Traces)
            .unwrap();

        // Timestamp older than cutoff should be expired
        let old_timestamp = cutoff.cutoff_timestamp - chrono::Duration::days(1);
        assert!(cutoff.is_expired(old_timestamp));

        // Timestamp newer than cutoff should not be expired
        let new_timestamp = cutoff.cutoff_timestamp + chrono::Duration::days(1);
        assert!(!cutoff.is_expired(new_timestamp));

        // Timestamp exactly at cutoff should not be expired
        assert!(!cutoff.is_expired(cutoff.cutoff_timestamp));
    }

    #[test]
    fn test_grace_period_application() {
        let config = RetentionConfig {
            traces: Duration::from_secs(7 * 24 * 3600),
            grace_period: Duration::from_secs(3600),
            ..Default::default()
        };

        let resolver = RetentionPolicyResolver::new(config).unwrap();
        let cutoff = resolver
            .compute_cutoff("tenant1", "dataset1", SignalType::Traces)
            .unwrap();

        // Cutoff should be 7 days + 1 hour ago
        let expected_cutoff = Utc::now() - chrono::Duration::seconds((7 * 24 + 1) * 3600);
        let diff = (cutoff.cutoff_timestamp - expected_cutoff)
            .num_seconds()
            .abs();
        assert!(diff < 2, "Cutoff should include grace period");
    }
}
