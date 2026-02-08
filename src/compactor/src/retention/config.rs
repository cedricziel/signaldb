//! Retention configuration structures for SignalDB Compactor Phase 3.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::time::Duration;
use thiserror::Error;

/// Retention policy configuration with support for tenant and dataset overrides.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RetentionConfig {
    /// Enable retention enforcement.
    ///
    /// Env: SIGNALDB__COMPACTOR__RETENTION__ENABLED
    #[serde(default)]
    pub enabled: bool,

    /// Interval between retention checks.
    ///
    /// Env: SIGNALDB__COMPACTOR__RETENTION__RETENTION_CHECK_INTERVAL
    #[serde(with = "humantime_serde")]
    pub retention_check_interval: Duration,

    /// Default retention period for traces.
    ///
    /// Env: SIGNALDB__COMPACTOR__RETENTION__TRACES
    #[serde(with = "humantime_serde")]
    pub traces: Duration,

    /// Default retention period for logs.
    ///
    /// Env: SIGNALDB__COMPACTOR__RETENTION__LOGS
    #[serde(with = "humantime_serde")]
    pub logs: Duration,

    /// Default retention period for metrics.
    ///
    /// Env: SIGNALDB__COMPACTOR__RETENTION__METRICS
    #[serde(with = "humantime_serde")]
    pub metrics: Duration,

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

fn default_grace_period() -> Duration {
    Duration::from_secs(3600) // 1 hour grace period
}

fn default_timezone() -> String {
    "UTC".to_string()
}

fn default_dry_run() -> bool {
    true // Dry-run enabled by default for safety
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            enabled: false,                                      // Disabled by default for safety
            retention_check_interval: Duration::from_secs(3600), // 1 hour
            traces: Duration::from_secs(7 * 24 * 3600),          // 7 days
            logs: Duration::from_secs(30 * 24 * 3600),           // 30 days
            metrics: Duration::from_secs(90 * 24 * 3600),        // 90 days
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

        if self.grace_period <= zero {
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
    #[serde(with = "humantime_serde", skip_serializing_if = "Option::is_none")]
    pub traces: Option<Duration>,

    /// Override default retention for logs.
    #[serde(with = "humantime_serde", skip_serializing_if = "Option::is_none")]
    pub logs: Option<Duration>,

    /// Override default retention for metrics.
    #[serde(with = "humantime_serde", skip_serializing_if = "Option::is_none")]
    pub metrics: Option<Duration>,

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
    #[serde(with = "humantime_serde", skip_serializing_if = "Option::is_none")]
    pub traces: Option<Duration>,

    /// Override retention for logs in this dataset.
    #[serde(with = "humantime_serde", skip_serializing_if = "Option::is_none")]
    pub logs: Option<Duration>,

    /// Override retention for metrics in this dataset.
    #[serde(with = "humantime_serde", skip_serializing_if = "Option::is_none")]
    pub metrics: Option<Duration>,
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
}

impl SignalType {
    /// Parse signal type from table name.
    pub fn from_table_name(table_name: &str) -> Result<Self, RetentionConfigError> {
        match table_name.to_lowercase().as_str() {
            "traces" => Ok(SignalType::Traces),
            "logs" => Ok(SignalType::Logs),
            "metrics" => Ok(SignalType::Metrics),
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
        }
    }
}

impl fmt::Display for SignalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.table_name())
    }
}

/// Source of a retention policy decision for auditing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
}

/// Errors that can occur during retention configuration validation.
#[derive(Error, Debug)]
pub enum RetentionConfigError {
    /// Invalid timezone specified.
    #[error("Invalid timezone '{timezone}': {message}")]
    InvalidTimezone { timezone: String, message: String },

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
        assert!(SignalType::from_table_name("invalid").is_err());
    }

    #[test]
    fn test_signal_type_table_name() {
        assert_eq!(SignalType::Traces.table_name(), "traces");
        assert_eq!(SignalType::Logs.table_name(), "logs");
        assert_eq!(SignalType::Metrics.table_name(), "metrics");
    }

    #[test]
    fn test_tenant_override_validation() {
        let mut tenant_config = TenantRetentionConfig {
            traces: Some(Duration::from_secs(14 * 24 * 3600)),
            logs: None,
            metrics: None,
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
        };

        assert!(dataset_config.validate().is_ok());

        dataset_config.logs = Some(Duration::from_secs(0));
        assert!(dataset_config.validate().is_err());
    }
}
