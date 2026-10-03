//! Retention policy configuration and period resolution.
//!
//! Lives in `common` so the compactor (which enforces retention) and the
//! router (which reports it on query responses) resolve the same policy.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::time::Duration;
use thiserror::Error;

/// Retention policy configuration with support for tenant and dataset overrides.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RetentionConfig {
    /// Enable retention enforcement (enabled by default).
    ///
    /// Env: SIGNALDB__COMPACTOR__RETENTION__ENABLED
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    /// Interval between retention checks.
    ///
    /// Env: SIGNALDB__COMPACTOR__RETENTION__RETENTION_CHECK_INTERVAL
    #[serde(with = "humantime_serde", default = "default_retention_check_interval")]
    pub retention_check_interval: Duration,

    /// Default retention period for traces.
    ///
    /// Env: SIGNALDB__COMPACTOR__RETENTION__TRACES
    #[serde(with = "humantime_serde", default = "default_signal_retention")]
    pub traces: Duration,

    /// Default retention period for logs.
    ///
    /// Env: SIGNALDB__COMPACTOR__RETENTION__LOGS
    #[serde(with = "humantime_serde", default = "default_signal_retention")]
    pub logs: Duration,

    /// Default retention period for metrics.
    ///
    /// Env: SIGNALDB__COMPACTOR__RETENTION__METRICS
    #[serde(with = "humantime_serde", default = "default_signal_retention")]
    pub metrics: Duration,

    /// Default retention period for profiles.
    ///
    /// Env: SIGNALDB__COMPACTOR__RETENTION__PROFILES
    #[serde(with = "humantime_serde", default = "default_signal_retention")]
    pub profiles: Duration,

    /// Tenant-specific retention overrides.
    #[serde(default)]
    pub tenant_overrides: HashMap<String, TenantRetentionConfig>,

    /// Grace period before dropping expired partitions (safety margin).
    ///
    /// Env: SIGNALDB__COMPACTOR__RETENTION__GRACE_PERIOD
    #[serde(with = "humantime_serde", default = "default_grace_period")]
    pub grace_period: Duration,

    /// Timezone for retention calculations (defaults to UTC).
    ///
    /// Env: SIGNALDB__COMPACTOR__RETENTION__TIMEZONE
    #[serde(default = "default_timezone")]
    pub timezone: String,

    /// Dry-run mode: log actions without executing them.
    ///
    /// Env: SIGNALDB__COMPACTOR__RETENTION__DRY_RUN
    #[serde(default = "default_dry_run")]
    pub dry_run: bool,

    /// Number of snapshots to keep before expiring older ones.
    ///
    /// Env: SIGNALDB__COMPACTOR__RETENTION__SNAPSHOTS_TO_KEEP
    #[serde(default)]
    pub snapshots_to_keep: Option<usize>,
}

fn default_retention_check_interval() -> Duration {
    Duration::from_secs(3600)
}

fn default_signal_retention() -> Duration {
    Duration::from_secs(30 * 24 * 3600)
}

fn default_grace_period() -> Duration {
    Duration::from_secs(3600) // 1 hour grace period
}

fn default_timezone() -> String {
    "UTC".to_string()
}

fn default_enabled() -> bool {
    true // Retention enforcement is on by default
}

fn default_dry_run() -> bool {
    false // Retention enforces (deletes expired data) by default
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            retention_check_interval: default_retention_check_interval(),
            traces: default_signal_retention(),
            logs: default_signal_retention(),
            metrics: default_signal_retention(),
            profiles: default_signal_retention(),
            tenant_overrides: HashMap::new(),
            grace_period: default_grace_period(),
            timezone: default_timezone(),
            dry_run: default_dry_run(),
            snapshots_to_keep: Some(10), // Keep 10 snapshots by default
        }
    }
}

impl RetentionConfig {
    /// Validate the retention configuration.
    ///
    /// Checks:
    /// - All retention periods are positive (non-zero)
    /// - Grace period is positive
    /// - Tenant overrides are valid
    pub fn validate(&self) -> Result<(), RetentionConfigError> {
        let zero = Duration::from_secs(0);

        if self.traces <= zero {
            return Err(RetentionConfigError::InvalidRetentionPeriod {
                signal_type: SignalType::Traces,
                duration: self.traces,
            });
        }
        if self.logs <= zero {
            return Err(RetentionConfigError::InvalidRetentionPeriod {
                signal_type: SignalType::Logs,
                duration: self.logs,
            });
        }
        if self.metrics <= zero {
            return Err(RetentionConfigError::InvalidRetentionPeriod {
                signal_type: SignalType::Metrics,
                duration: self.metrics,
            });
        }
        if self.profiles <= zero {
            return Err(RetentionConfigError::InvalidRetentionPeriod {
                signal_type: SignalType::Profiles,
                duration: self.profiles,
            });
        }

        if self.grace_period < zero {
            return Err(RetentionConfigError::InvalidGracePeriod(self.grace_period));
        }

        // Validate overrides
        for (tenant_id, tenant_config) in &self.tenant_overrides {
            tenant_config
                .validate()
                .map_err(|e| RetentionConfigError::InvalidTenantOverride {
                    tenant_id: tenant_id.clone(),
                    source: Box::new(e),
                })?;
        }

        Ok(())
    }
}

/// Tenant-specific retention configuration with optional dataset overrides.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TenantRetentionConfig {
    /// Override default retention for traces.
    #[serde(
        default,
        with = "humantime_serde::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub traces: Option<Duration>,

    /// Override default retention for logs.
    #[serde(
        default,
        with = "humantime_serde::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub logs: Option<Duration>,

    /// Override default retention for metrics.
    #[serde(
        default,
        with = "humantime_serde::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub metrics: Option<Duration>,

    /// Override default retention for profiles.
    #[serde(
        default,
        with = "humantime_serde::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub profiles: Option<Duration>,

    /// Dataset-specific retention overrides.
    #[serde(default)]
    pub dataset_overrides: HashMap<String, DatasetRetentionConfig>,
}

impl TenantRetentionConfig {
    /// Validate tenant retention configuration.
    pub fn validate(&self) -> Result<(), RetentionConfigError> {
        let zero = Duration::from_secs(0);

        if let Some(duration) = self.traces
            && duration <= zero
        {
            return Err(RetentionConfigError::InvalidRetentionPeriod {
                signal_type: SignalType::Traces,
                duration,
            });
        }

        if let Some(duration) = self.logs
            && duration <= zero
        {
            return Err(RetentionConfigError::InvalidRetentionPeriod {
                signal_type: SignalType::Logs,
                duration,
            });
        }

        if let Some(duration) = self.metrics
            && duration <= zero
        {
            return Err(RetentionConfigError::InvalidRetentionPeriod {
                signal_type: SignalType::Metrics,
                duration,
            });
        }

        if let Some(duration) = self.profiles
            && duration <= zero
        {
            return Err(RetentionConfigError::InvalidRetentionPeriod {
                signal_type: SignalType::Profiles,
                duration,
            });
        }

        // Validate dataset overrides
        for (dataset_id, dataset_config) in &self.dataset_overrides {
            dataset_config.validate().map_err(|e| {
                RetentionConfigError::InvalidDatasetOverride {
                    dataset_id: dataset_id.clone(),
                    source: Box::new(e),
                }
            })?;
        }

        Ok(())
    }
}

/// Dataset-specific retention configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DatasetRetentionConfig {
    /// Override retention for traces in this dataset.
    #[serde(
        default,
        with = "humantime_serde::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub traces: Option<Duration>,

    /// Override retention for logs in this dataset.
    #[serde(
        default,
        with = "humantime_serde::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub logs: Option<Duration>,

    /// Override retention for metrics in this dataset.
    #[serde(
        default,
        with = "humantime_serde::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub metrics: Option<Duration>,

    /// Override retention for profiles in this dataset.
    #[serde(
        default,
        with = "humantime_serde::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub profiles: Option<Duration>,
}

impl DatasetRetentionConfig {
    /// Validate dataset retention configuration.
    pub fn validate(&self) -> Result<(), RetentionConfigError> {
        let zero = Duration::from_secs(0);

        if let Some(duration) = self.traces
            && duration <= zero
        {
            return Err(RetentionConfigError::InvalidRetentionPeriod {
                signal_type: SignalType::Traces,
                duration,
            });
        }

        if let Some(duration) = self.logs
            && duration <= zero
        {
            return Err(RetentionConfigError::InvalidRetentionPeriod {
                signal_type: SignalType::Logs,
                duration,
            });
        }

        if let Some(duration) = self.metrics
            && duration <= zero
        {
            return Err(RetentionConfigError::InvalidRetentionPeriod {
                signal_type: SignalType::Metrics,
                duration,
            });
        }

        if let Some(duration) = self.profiles
            && duration <= zero
        {
            return Err(RetentionConfigError::InvalidRetentionPeriod {
                signal_type: SignalType::Profiles,
                duration,
            });
        }

        Ok(())
    }
}

/// Signal type enumeration for retention policies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SignalType {
    /// Traces (distributed tracing data).
    Traces,
    /// Logs (application logs).
    Logs,
    /// Metrics (time series metrics).
    Metrics,
    /// Profiles (continuous profiling data).
    Profiles,
}

impl SignalType {
    /// Parse signal type from table name.
    ///
    /// This is the single predicate deciding whether a catalog table is a
    /// signal table the lifecycle owns — retention, snapshot expiration, and
    /// orphan cleanup all classify through it, so a table it rejects gets no
    /// lifecycle management at all (#1014).
    ///
    /// Classifies through [`crate::catalog::attribute_stats_signal`], which
    /// the attribute statistics are keyed by, so the two never drift.
    pub fn from_table_name(table_name: &str) -> Result<Self, RetentionConfigError> {
        match crate::catalog::attribute_stats_signal(table_name) {
            "traces" => Ok(SignalType::Traces),
            "logs" => Ok(SignalType::Logs),
            "metrics" => Ok(SignalType::Metrics),
            "profiles" => Ok(SignalType::Profiles),
            _ => Err(RetentionConfigError::UnknownSignalType(
                table_name.to_string(),
            )),
        }
    }

    /// Get the table name for this signal type.
    pub fn table_name(&self) -> &'static str {
        match self {
            SignalType::Traces => "traces",
            SignalType::Logs => "logs",
            SignalType::Metrics => "metrics",
            SignalType::Profiles => "profiles",
        }
    }
}

impl fmt::Display for SignalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.table_name())
    }
}

/// Source of a retention policy decision for auditing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RetentionPolicySource {
    /// Global default from configuration.
    Global,
    /// Tenant-level override.
    Tenant,
    /// Dataset-level override.
    Dataset,
}

/// Trait for types that can override retention periods.
pub trait RetentionOverride {
    /// Get traces retention override if present.
    fn traces(&self) -> Option<Duration>;
    /// Get logs retention override if present.
    fn logs(&self) -> Option<Duration>;
    /// Get metrics retention override if present.
    fn metrics(&self) -> Option<Duration>;
    /// Get profiles retention override if present.
    fn profiles(&self) -> Option<Duration>;
}

impl RetentionOverride for TenantRetentionConfig {
    fn traces(&self) -> Option<Duration> {
        self.traces
    }

    fn logs(&self) -> Option<Duration> {
        self.logs
    }

    fn metrics(&self) -> Option<Duration> {
        self.metrics
    }

    fn profiles(&self) -> Option<Duration> {
        self.profiles
    }
}

impl RetentionOverride for DatasetRetentionConfig {
    fn traces(&self) -> Option<Duration> {
        self.traces
    }

    fn logs(&self) -> Option<Duration> {
        self.logs
    }

    fn metrics(&self) -> Option<Duration> {
        self.metrics
    }

    fn profiles(&self) -> Option<Duration> {
        self.profiles
    }
}

/// The retention window resolved for one tenant, dataset and signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedRetention {
    /// Retention period after applying overrides.
    pub period: Duration,
    /// Which setting supplied the period.
    pub source: RetentionPolicySource,
}

impl RetentionConfig {
    /// Resolve the retention period and its source for a tenant, dataset and
    /// signal. Order: dataset override, tenant override, global default.
    pub fn resolve_period(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        signal_type: SignalType,
    ) -> ResolvedRetention {
        if let Some(tenant_config) = self.tenant_overrides.get(tenant_id) {
            if let Some(dataset_config) = tenant_config.dataset_overrides.get(dataset_id)
                && let Some(period) = signal_retention(dataset_config, signal_type)
            {
                return ResolvedRetention {
                    period,
                    source: RetentionPolicySource::Dataset,
                };
            }
            if let Some(period) = signal_retention(tenant_config, signal_type) {
                return ResolvedRetention {
                    period,
                    source: RetentionPolicySource::Tenant,
                };
            }
        }
        let period = match signal_type {
            SignalType::Traces => self.traces,
            SignalType::Logs => self.logs,
            SignalType::Metrics => self.metrics,
            SignalType::Profiles => self.profiles,
        };
        ResolvedRetention {
            period,
            source: RetentionPolicySource::Global,
        }
    }
}

fn signal_retention(config: &impl RetentionOverride, signal_type: SignalType) -> Option<Duration> {
    match signal_type {
        SignalType::Traces => config.traces(),
        SignalType::Logs => config.logs(),
        SignalType::Metrics => config.metrics(),
        SignalType::Profiles => config.profiles(),
    }
}

impl RetentionPolicySource {
    /// Lowercase name used in API responses and messages.
    pub fn as_str(&self) -> &'static str {
        match self {
            RetentionPolicySource::Global => "global",
            RetentionPolicySource::Tenant => "tenant",
            RetentionPolicySource::Dataset => "dataset",
        }
    }
}

/// Errors that can occur during retention configuration validation.
#[derive(Error, Debug)]
pub enum RetentionConfigError {
    /// Invalid retention period (must be positive).
    #[error("Invalid retention period for {signal_type}: {duration:?} must be positive")]
    InvalidRetentionPeriod {
        signal_type: SignalType,
        duration: Duration,
    },

    /// Invalid grace period (must be positive).
    #[error("Invalid grace period: {0:?} must be positive")]
    InvalidGracePeriod(Duration),

    /// Unknown signal type in table name.
    #[error("Unknown signal type: {0}")]
    UnknownSignalType(String),

    /// Invalid tenant override configuration.
    #[error("Invalid retention configuration for tenant '{tenant_id}': {source}")]
    InvalidTenantOverride {
        tenant_id: String,
        source: Box<RetentionConfigError>,
    },

    /// Invalid dataset override configuration.
    #[error("Invalid retention configuration for dataset '{dataset_id}': {source}")]
    InvalidDatasetOverride {
        dataset_id: String,
        source: Box<RetentionConfigError>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config_is_valid() {
        let config = RetentionConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_zero_retention_is_invalid() {
        let config = RetentionConfig {
            traces: Duration::from_secs(0),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_signal_type_from_table_name() {
        assert_eq!(
            SignalType::from_table_name("traces").unwrap(),
            SignalType::Traces
        );
        assert_eq!(
            SignalType::from_table_name("logs").unwrap(),
            SignalType::Logs
        );
        assert_eq!(
            SignalType::from_table_name("metrics").unwrap(),
            SignalType::Metrics
        );
        assert_eq!(
            SignalType::from_table_name("profiles").unwrap(),
            SignalType::Profiles
        );
        assert!(SignalType::from_table_name("invalid").is_err());
    }

    // otel-native-schema layer 7 (D10) cutover prep: `metric_exemplars`
    // doesn't start with `metrics_`, so it needs its own arm.
    #[test]
    fn test_signal_type_from_table_name_recognizes_metric_exemplars() {
        assert_eq!(
            SignalType::from_table_name("metric_exemplars").unwrap(),
            SignalType::Metrics
        );
    }

    /// Every table the schema registry can create must map to a retention
    /// signal type, or the lifecycle silently skips it (#1014: `profiles`
    /// accumulated 7,305 snapshots because it matched no arm here). Adding a
    /// `TableSchema` variant without a `SignalType` fails this test.
    #[test]
    fn every_registry_table_maps_to_a_signal_type() {
        for schema in crate::iceberg::schemas::TableSchema::all() {
            let table_name = schema.table_name();
            assert!(
                SignalType::from_table_name(table_name).is_ok(),
                "table {table_name} has no retention signal type — the \
                 lifecycle would never enumerate it"
            );
        }
    }

    #[test]
    fn test_signal_type_from_metrics_subtypes() {
        // Test that metrics subtypes are recognized
        assert_eq!(
            SignalType::from_table_name("metrics_gauge").unwrap(),
            SignalType::Metrics
        );
        assert_eq!(
            SignalType::from_table_name("metrics_counter").unwrap(),
            SignalType::Metrics
        );
        assert_eq!(
            SignalType::from_table_name("metrics_histogram").unwrap(),
            SignalType::Metrics
        );
        assert_eq!(
            SignalType::from_table_name("metrics_summary").unwrap(),
            SignalType::Metrics
        );

        // Case insensitive
        assert_eq!(
            SignalType::from_table_name("METRICS_GAUGE").unwrap(),
            SignalType::Metrics
        );
    }

    #[test]
    fn test_signal_type_table_name() {
        assert_eq!(SignalType::Traces.table_name(), "traces");
        assert_eq!(SignalType::Logs.table_name(), "logs");
        assert_eq!(SignalType::Metrics.table_name(), "metrics");
        assert_eq!(SignalType::Profiles.table_name(), "profiles");
    }

    #[test]
    fn test_tenant_override_validation() {
        let mut tenant_config = TenantRetentionConfig {
            traces: Some(Duration::from_secs(14 * 24 * 3600)),
            logs: None,
            metrics: None,
            profiles: None,
            dataset_overrides: HashMap::new(),
        };

        assert!(tenant_config.validate().is_ok());

        tenant_config.traces = Some(Duration::from_secs(0));
        assert!(tenant_config.validate().is_err());
    }

    #[test]
    fn test_dataset_override_validation() {
        let mut dataset_config = DatasetRetentionConfig {
            traces: Some(Duration::from_secs(30 * 24 * 3600)),
            logs: None,
            metrics: None,
            profiles: None,
        };

        assert!(dataset_config.validate().is_ok());

        dataset_config.logs = Some(Duration::from_secs(0));
        assert!(dataset_config.validate().is_err());
    }

    #[test]
    fn resolve_period_uses_global_without_overrides() {
        let config = RetentionConfig::default();
        let r = config.resolve_period("t", "d", SignalType::Traces);
        assert_eq!(r.period, Duration::from_secs(30 * 24 * 3600));
        assert_eq!(r.source, RetentionPolicySource::Global);
    }

    fn config_with_overrides() -> RetentionConfig {
        let mut config = RetentionConfig::default();
        let mut tenant = TenantRetentionConfig {
            traces: Some(Duration::from_secs(7 * 24 * 3600)),
            logs: None,
            metrics: None,
            profiles: None,
            dataset_overrides: HashMap::new(),
        };
        tenant.dataset_overrides.insert(
            "ds".to_string(),
            DatasetRetentionConfig {
                traces: Some(Duration::from_secs(24 * 3600)),
                logs: None,
                metrics: None,
                profiles: None,
            },
        );
        config.tenant_overrides.insert("acme".to_string(), tenant);
        config
    }

    #[test]
    fn resolve_period_prefers_tenant_override() {
        let config = config_with_overrides();
        let r = config.resolve_period("acme", "other", SignalType::Traces);
        assert_eq!(r.period, Duration::from_secs(7 * 24 * 3600));
        assert_eq!(r.source, RetentionPolicySource::Tenant);
        // Signals the tenant does not override stay global.
        let r = config.resolve_period("acme", "other", SignalType::Logs);
        assert_eq!(r.source, RetentionPolicySource::Global);
        let r = config.resolve_period("someone-else", "ds", SignalType::Traces);
        assert_eq!(r.source, RetentionPolicySource::Global);
    }

    #[test]
    fn resolve_period_prefers_dataset_override() {
        let config = config_with_overrides();
        let r = config.resolve_period("acme", "ds", SignalType::Traces);
        assert_eq!(r.period, Duration::from_secs(24 * 3600));
        assert_eq!(r.source, RetentionPolicySource::Dataset);
        // Dataset has no logs override and tenant has none: global.
        let r = config.resolve_period("acme", "ds", SignalType::Logs);
        assert_eq!(r.source, RetentionPolicySource::Global);
    }

    #[test]
    fn tenant_overrides_parse_from_toml_shape() {
        let json = serde_json::json!({
            "tenant_overrides": {
                "acme": { "traces": "14d", "dataset_overrides": { "ds": { "logs": "2d" } } }
            }
        });
        let config: RetentionConfig = serde_json::from_value(json).expect("parses");
        assert_eq!(
            config.resolve_period("acme", "ds", SignalType::Logs).source,
            RetentionPolicySource::Dataset
        );
        assert_eq!(
            config
                .resolve_period("acme", "x", SignalType::Traces)
                .period,
            Duration::from_secs(14 * 24 * 3600)
        );
    }

    /// A malformed override now fails config load instead of being skipped
    /// with a warning, so a typo cannot silently fall back to the global
    /// period.
    #[test]
    fn a_malformed_tenant_override_fails_to_parse() {
        let json = serde_json::json!({
            "tenant_overrides": { "acme": { "traces": "fortnight" } }
        });
        assert!(serde_json::from_value::<RetentionConfig>(json).is_err());
    }
}
