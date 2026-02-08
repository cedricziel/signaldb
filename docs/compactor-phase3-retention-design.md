# Phase 3: Retention Cutoff Computation System Design

## Overview

This document details the design for the retention cutoff computation system in Phase 3 of the Compactor Service. This system determines which partitions should be dropped based on time-based retention policies configured per signal type, with support for tenant and dataset-level overrides.

## Architecture

### 1. Data Structures

#### RetentionConfig

The main configuration structure that extends the existing `CompactorConfig`:

```rust
/// Retention policy configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RetentionConfig {
    /// Enable retention enforcement
    /// Env: SIGNALDB__COMPACTOR__RETENTION__ENABLED
    #[serde(default)]
    pub enabled: bool,

    /// Interval between retention checks
    /// Env: SIGNALDB__COMPACTOR__RETENTION__RETENTION_CHECK_INTERVAL
    #[serde(with = "humantime_serde")]
    pub retention_check_interval: Duration,

    /// Default retention period for traces
    /// Env: SIGNALDB__COMPACTOR__RETENTION__TRACES
    #[serde(with = "humantime_serde")]
    pub traces: Duration,

    /// Default retention period for logs
    /// Env: SIGNALDB__COMPACTOR__RETENTION__LOGS
    #[serde(with = "humantime_serde")]
    pub logs: Duration,

    /// Default retention period for metrics
    /// Env: SIGNALDB__COMPACTOR__RETENTION__METRICS
    #[serde(with = "humantime_serde")]
    pub metrics: Duration,

    /// Tenant-specific retention overrides
    #[serde(default)]
    pub tenant_overrides: HashMap<String, TenantRetentionConfig>,

    /// Grace period before dropping expired partitions (safety margin)
    /// Env: SIGNALDB__COMPACTOR__RETENTION__GRACE_PERIOD
    #[serde(with = "humantime_serde", default = "default_grace_period")]
    pub grace_period: Duration,

    /// Timezone for retention calculations (defaults to UTC)
    /// Env: SIGNALDB__COMPACTOR__RETENTION__TIMEZONE
    #[serde(default = "default_timezone")]
    pub timezone: String,
}

fn default_grace_period() -> Duration {
    Duration::from_secs(3600) // 1 hour grace period
}

fn default_timezone() -> String {
    "UTC".to_string()
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            retention_check_interval: Duration::from_secs(3600), // 1 hour
            traces: Duration::from_secs(7 * 24 * 3600),  // 7 days
            logs: Duration::from_secs(30 * 24 * 3600),   // 30 days
            metrics: Duration::from_secs(90 * 24 * 3600), // 90 days
            tenant_overrides: HashMap::new(),
            grace_period: default_grace_period(),
            timezone: default_timezone(),
        }
    }
}
```

#### TenantRetentionConfig

Tenant-specific retention overrides:

```rust
/// Tenant-specific retention configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TenantRetentionConfig {
    /// Override default retention for traces
    #[serde(with = "humantime_serde", skip_serializing_if = "Option::is_none")]
    pub traces: Option<Duration>,

    /// Override default retention for logs
    #[serde(with = "humantime_serde", skip_serializing_if = "Option::is_none")]
    pub logs: Option<Duration>,

    /// Override default retention for metrics
    #[serde(with = "humantime_serde", skip_serializing_if = "Option::is_none")]
    pub metrics: Option<Duration>,

    /// Dataset-specific retention overrides
    #[serde(default)]
    pub dataset_overrides: HashMap<String, DatasetRetentionConfig>,
}

/// Dataset-specific retention configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DatasetRetentionConfig {
    /// Override retention for traces in this dataset
    #[serde(with = "humantime_serde", skip_serializing_if = "Option::is_none")]
    pub traces: Option<Duration>,

    /// Override retention for logs in this dataset
    #[serde(with = "humantime_serde", skip_serializing_if = "Option::is_none")]
    pub logs: Option<Duration>,

    /// Override retention for metrics in this dataset
    #[serde(with = "humantime_serde", skip_serializing_if = "Option::is_none")]
    pub metrics: Option<Duration>,
}
```

#### SignalType Enum

Strongly-typed signal types:

```rust
/// Signal type enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SignalType {
    Traces,
    Logs,
    Metrics,
}

impl SignalType {
    /// Parse signal type from table name
    pub fn from_table_name(table_name: &str) -> Result<Self> {
        match table_name.to_lowercase().as_str() {
            "traces" => Ok(SignalType::Traces),
            "logs" => Ok(SignalType::Logs),
            "metrics" => Ok(SignalType::Metrics),
            _ => Err(anyhow::anyhow!("Unknown signal type: {}", table_name)),
        }
    }

    /// Get the table name for this signal type
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
```

#### RetentionCutoff

The computed retention cutoff for a specific context:

```rust
/// Computed retention cutoff for a tenant/dataset/signal combination
#[derive(Debug, Clone)]
pub struct RetentionCutoff {
    /// Tenant ID
    pub tenant_id: String,

    /// Dataset ID
    pub dataset_id: String,

    /// Signal type
    pub signal_type: SignalType,

    /// Cutoff timestamp (data older than this should be dropped)
    pub cutoff_timestamp: DateTime<Utc>,

    /// The retention period used (after applying overrides)
    pub retention_period: Duration,

    /// Source of the retention period (for auditing)
    pub source: RetentionPolicySource,
}

/// Source of a retention policy decision
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetentionPolicySource {
    /// Global default from config
    Global,
    /// Tenant-level override
    Tenant,
    /// Dataset-level override
    Dataset,
}

impl RetentionCutoff {
    /// Check if a partition timestamp is expired
    pub fn is_expired(&self, partition_timestamp: DateTime<Utc>) -> bool {
        partition_timestamp < self.cutoff_timestamp
    }

    /// Human-readable representation
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
```

### 2. Retention Policy Resolver

The core component that computes retention cutoffs:

```rust
/// Resolves retention policies with override hierarchy
pub struct RetentionPolicyResolver {
    config: RetentionConfig,
    tz: Tz, // chrono_tz timezone
}

impl RetentionPolicyResolver {
    /// Create a new resolver from configuration
    pub fn new(config: RetentionConfig) -> Result<Self> {
        // Parse timezone from config
        let tz = config.timezone.parse::<Tz>()
            .context("Invalid timezone configuration")?;

        Ok(Self { config, tz })
    }

    /// Compute retention cutoff for a specific context
    ///
    /// Resolution order:
    /// 1. Dataset-specific override (if exists)
    /// 2. Tenant-specific override (if exists)
    /// 3. Global default
    pub fn compute_cutoff(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        signal_type: SignalType,
    ) -> Result<RetentionCutoff> {
        // Get the current time in the configured timezone
        let now = Utc::now().with_timezone(&self.tz);

        // Resolve retention period with override hierarchy
        let (retention_period, source) = self.resolve_retention_period(
            tenant_id,
            dataset_id,
            signal_type,
        );

        // Apply grace period for safety
        let effective_retention = retention_period
            .checked_add(self.config.grace_period)
            .context("Retention period overflow")?;

        // Compute cutoff timestamp
        let cutoff_timestamp = now
            .checked_sub_signed(chrono::Duration::from_std(effective_retention)?)
            .context("Cutoff timestamp underflow")?
            .with_timezone(&Utc);

        Ok(RetentionCutoff {
            tenant_id: tenant_id.to_string(),
            dataset_id: dataset_id.to_string(),
            signal_type,
            cutoff_timestamp,
            retention_period,
            source,
        })
    }

    /// Resolve retention period with override hierarchy
    fn resolve_retention_period(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        signal_type: SignalType,
    ) -> (Duration, RetentionPolicySource) {
        // Check dataset-level override
        if let Some(tenant_config) = self.config.tenant_overrides.get(tenant_id) {
            if let Some(dataset_config) = tenant_config.dataset_overrides.get(dataset_id) {
                if let Some(period) = self.get_signal_retention(dataset_config, signal_type) {
                    return (period, RetentionPolicySource::Dataset);
                }
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

    /// Extract signal-specific retention from config
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
}

/// Trait for types that can override retention periods
trait RetentionOverride {
    fn traces(&self) -> Option<Duration>;
    fn logs(&self) -> Option<Duration>;
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
```

### 3. Integration with Compactor

Update `CompactorConfig` to include retention:

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompactorConfig {
    // ... existing fields ...

    /// Retention policy configuration
    #[serde(default)]
    pub retention: RetentionConfig,
}
```

### 4. Partition Timestamp Extraction

Utility to extract timestamps from Iceberg partitions:

```rust
/// Extract timestamp from partition values
pub fn extract_partition_timestamp(
    partition_values: &HashMap<String, String>,
) -> Result<DateTime<Utc>> {
    // SignalDB uses hour-based partitioning with "hour" field
    let hour_value = partition_values
        .get("hour")
        .context("Partition missing 'hour' field")?;

    // Parse hour partition format: "YYYY-MM-DD-HH"
    let naive_dt = NaiveDateTime::parse_from_str(
        &format!("{hour_value}:00:00"),
        "%Y-%m-%d-%H:%M:%S",
    )
    .with_context(|| format!("Invalid hour partition format: {}", hour_value))?;

    Ok(DateTime::<Utc>::from_naive_utc_and_offset(naive_dt, Utc))
}
```

## Configuration Schema

### Global Default Configuration

```toml
[compactor.retention]
enabled = true
retention_check_interval = "1h"
traces = "7d"
logs = "30d"
metrics = "90d"
grace_period = "1h"
timezone = "UTC"
```

### Tenant-Level Overrides

```toml
[compactor.retention.tenant_overrides.acme]
traces = "14d"  # Acme keeps traces for 14 days instead of 7
logs = "60d"    # Acme keeps logs for 60 days instead of 30
# metrics not specified, uses global default of 90d

[compactor.retention.tenant_overrides.beta]
traces = "3d"   # Beta keeps traces for only 3 days
```

### Dataset-Level Overrides

```toml
[compactor.retention.tenant_overrides.acme.dataset_overrides.production]
traces = "30d"   # Production traces kept for 30 days
logs = "90d"     # Production logs kept for 90 days

[compactor.retention.tenant_overrides.acme.dataset_overrides.staging]
traces = "2d"    # Staging traces kept for only 2 days
logs = "7d"      # Staging logs kept for only 7 days
```

### Environment Variable Overrides

```bash
# Global settings
export SIGNALDB__COMPACTOR__RETENTION__ENABLED=true
export SIGNALDB__COMPACTOR__RETENTION__RETENTION_CHECK_INTERVAL=2h
export SIGNALDB__COMPACTOR__RETENTION__TRACES=14d
export SIGNALDB__COMPACTOR__RETENTION__LOGS=60d
export SIGNALDB__COMPACTOR__RETENTION__METRICS=180d
export SIGNALDB__COMPACTOR__RETENTION__GRACE_PERIOD=2h
export SIGNALDB__COMPACTOR__RETENTION__TIMEZONE=America/New_York
```

## Error Handling

### Edge Cases and Error Scenarios

#### 1. Invalid Timezone Configuration

```rust
pub enum RetentionError {
    #[error("Invalid timezone '{0}': {1}")]
    InvalidTimezone(String, String),

    #[error("Retention period overflow for {signal_type}")]
    PeriodOverflow { signal_type: SignalType },

    #[error("Cutoff timestamp underflow for {tenant}/{dataset}/{signal_type}")]
    CutoffUnderflow {
        tenant: String,
        dataset: String,
        signal_type: SignalType,
    },

    #[error("Invalid partition timestamp format: {0}")]
    InvalidPartitionFormat(String),

    #[error("Missing partition field: {0}")]
    MissingPartitionField(String),
}
```

**Handling Strategy**: Fail fast at startup if timezone is invalid. Log error and use UTC as fallback for runtime errors.

```rust
impl RetentionPolicyResolver {
    pub fn new(config: RetentionConfig) -> Result<Self> {
        let tz = config.timezone.parse::<Tz>().map_err(|e| {
            RetentionError::InvalidTimezone(
                config.timezone.clone(),
                e.to_string(),
            )
        })?;

        Ok(Self { config, tz })
    }
}
```

#### 2. Misconfigured Retention Periods

**Scenario**: Retention period set to 0 or negative duration.

**Handling Strategy**: Validate at configuration load time:

```rust
impl RetentionConfig {
    pub fn validate(&self) -> Result<()> {
        let zero = Duration::from_secs(0);

        if self.traces <= zero {
            return Err(anyhow::anyhow!("Traces retention must be positive"));
        }
        if self.logs <= zero {
            return Err(anyhow::anyhow!("Logs retention must be positive"));
        }
        if self.metrics <= zero {
            return Err(anyhow::anyhow!("Metrics retention must be positive"));
        }

        // Validate overrides
        for (tenant_id, tenant_config) in &self.tenant_overrides {
            tenant_config.validate()
                .with_context(|| format!("Invalid retention for tenant '{}'", tenant_id))?;
        }

        Ok(())
    }
}
```

#### 3. Partition Timestamp Parsing Failures

**Scenario**: Partition has malformed timestamp or missing "hour" field.

**Handling Strategy**: Log warning and skip partition (fail safe - don't delete if uncertain):

```rust
pub fn identify_expired_partitions(
    &self,
    cutoff: &RetentionCutoff,
    partitions: Vec<PartitionInfo>,
) -> Vec<PartitionInfo> {
    partitions
        .into_iter()
        .filter_map(|partition| {
            match extract_partition_timestamp(&partition.values) {
                Ok(timestamp) if cutoff.is_expired(timestamp) => Some(partition),
                Ok(_) => None, // Not expired
                Err(e) => {
                    tracing::warn!(
                        tenant_id = %cutoff.tenant_id,
                        dataset_id = %cutoff.dataset_id,
                        signal = %cutoff.signal_type,
                        partition = ?partition.values,
                        error = %e,
                        "Failed to parse partition timestamp, skipping retention check"
                    );
                    None // Skip on error - fail safe
                }
            }
        })
        .collect()
}
```

#### 4. Daylight Saving Time and Timezone Transitions

**Scenario**: Hour partition during DST transition may be ambiguous or non-existent.

**Handling Strategy**: Use UTC for all internal storage and computation, only apply timezone for display/logging:

```rust
impl RetentionPolicyResolver {
    fn compute_cutoff(&self, ...) -> Result<RetentionCutoff> {
        // Always work in UTC internally
        let now_utc = Utc::now();

        // Apply retention period in UTC
        let cutoff_timestamp = now_utc
            .checked_sub_signed(chrono::Duration::from_std(effective_retention)?)
            .context("Cutoff timestamp underflow")?;

        // Timezone only affects logging/display
        tracing::info!(
            "Computed retention cutoff in {}: {}",
            self.tz,
            cutoff_timestamp.with_timezone(&self.tz)
        );

        Ok(RetentionCutoff {
            cutoff_timestamp, // Stored in UTC
            // ...
        })
    }
}
```

#### 5. Clock Skew and Time Travel

**Scenario**: System clock set incorrectly, or partitions with future timestamps.

**Handling Strategy**: Implement sanity checks:

```rust
pub fn validate_partition_timestamp(timestamp: DateTime<Utc>) -> Result<()> {
    let now = Utc::now();
    let max_future = now + chrono::Duration::hours(24);

    if timestamp > max_future {
        return Err(anyhow::anyhow!(
            "Partition timestamp {} is more than 24 hours in the future",
            timestamp.to_rfc3339()
        ));
    }

    Ok(())
}
```

#### 6. Configuration Hot Reload Failures

**Scenario**: Config updated with invalid values while service is running.

**Handling Strategy**: Validate before applying, keep old config on failure:

```rust
impl CompactorService {
    pub async fn reload_config(&mut self, new_config: RetentionConfig) -> Result<()> {
        // Validate before applying
        new_config.validate()
            .context("Invalid retention configuration")?;

        // Test timezone parsing
        let _tz = new_config.timezone.parse::<Tz>()
            .context("Invalid timezone in new configuration")?;

        // Apply atomically
        self.retention_resolver = RetentionPolicyResolver::new(new_config)?;

        tracing::info!("Successfully reloaded retention configuration");
        Ok(())
    }
}
```

## Time-Based Partitioning Integration

SignalDB uses hour-based partitioning with the partition field "hour" in format `YYYY-MM-DD-HH`:

```rust
/// Partition information from Iceberg table
#[derive(Debug, Clone)]
pub struct PartitionInfo {
    /// Partition values (e.g., {"hour": "2024-01-15-14"})
    pub values: HashMap<String, String>,

    /// List of data files in this partition
    pub files: Vec<DataFileInfo>,
}

impl PartitionInfo {
    /// Get the partition timestamp
    pub fn timestamp(&self) -> Result<DateTime<Utc>> {
        extract_partition_timestamp(&self.values)
    }

    /// Get human-readable partition identifier
    pub fn identifier(&self) -> String {
        self.values
            .get("hour")
            .map(|h| h.to_string())
            .unwrap_or_else(|| format!("{:?}", self.values))
    }
}
```

## Observability and Metrics

### Logging

```rust
// At startup
tracing::info!(
    retention_enabled = config.retention.enabled,
    check_interval = ?config.retention.retention_check_interval,
    traces_retention = ?config.retention.traces,
    logs_retention = ?config.retention.logs,
    metrics_retention = ?config.retention.metrics,
    timezone = %config.retention.timezone,
    grace_period = ?config.retention.grace_period,
    "Retention policy resolver initialized"
);

// For each cutoff computation
tracing::debug!(
    tenant_id = %cutoff.tenant_id,
    dataset_id = %cutoff.dataset_id,
    signal = %cutoff.signal_type,
    cutoff = %cutoff.cutoff_timestamp.to_rfc3339(),
    retention_period = ?cutoff.retention_period,
    source = ?cutoff.source,
    "Computed retention cutoff"
);

// When identifying expired partitions
tracing::info!(
    tenant_id = %tenant_id,
    dataset_id = %dataset_id,
    signal = %signal_type,
    expired_count = expired_partitions.len(),
    total_count = all_partitions.len(),
    oldest_expired = ?oldest_timestamp,
    "Identified expired partitions for retention enforcement"
);
```

### Metrics

```rust
pub struct RetentionMetrics {
    /// Number of retention cutoff computations
    pub cutoffs_computed: Counter,

    /// Retention cutoff timestamp by tenant/dataset/signal (gauge)
    pub cutoff_timestamp_seconds: GaugeVec,

    /// Effective retention period by tenant/dataset/signal (gauge)
    pub retention_period_seconds: GaugeVec,

    /// Number of expired partitions identified
    pub expired_partitions: HistogramVec,

    /// Retention policy resolution errors
    pub resolution_errors: CounterVec,
}
```

## Testing Strategy

### Unit Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retention_cutoff_global_default() {
        let config = RetentionConfig::default();
        let resolver = RetentionPolicyResolver::new(config).unwrap();

        let cutoff = resolver.compute_cutoff("tenant1", "dataset1", SignalType::Traces).unwrap();

        assert_eq!(cutoff.source, RetentionPolicySource::Global);
        assert_eq!(cutoff.retention_period, Duration::from_secs(7 * 24 * 3600));
    }

    #[test]
    fn test_retention_cutoff_tenant_override() {
        let mut config = RetentionConfig::default();
        config.tenant_overrides.insert(
            "acme".to_string(),
            TenantRetentionConfig {
                traces: Some(Duration::from_secs(14 * 24 * 3600)),
                logs: None,
                metrics: None,
                dataset_overrides: HashMap::new(),
            },
        );

        let resolver = RetentionPolicyResolver::new(config).unwrap();
        let cutoff = resolver.compute_cutoff("acme", "prod", SignalType::Traces).unwrap();

        assert_eq!(cutoff.source, RetentionPolicySource::Tenant);
        assert_eq!(cutoff.retention_period, Duration::from_secs(14 * 24 * 3600));
    }

    #[test]
    fn test_retention_cutoff_dataset_override() {
        let mut config = RetentionConfig::default();
        let mut tenant_config = TenantRetentionConfig {
            traces: Some(Duration::from_secs(14 * 24 * 3600)),
            logs: None,
            metrics: None,
            dataset_overrides: HashMap::new(),
        };
        tenant_config.dataset_overrides.insert(
            "production".to_string(),
            DatasetRetentionConfig {
                traces: Some(Duration::from_secs(30 * 24 * 3600)),
                logs: None,
                metrics: None,
            },
        );
        config.tenant_overrides.insert("acme".to_string(), tenant_config);

        let resolver = RetentionPolicyResolver::new(config).unwrap();
        let cutoff = resolver.compute_cutoff("acme", "production", SignalType::Traces).unwrap();

        assert_eq!(cutoff.source, RetentionPolicySource::Dataset);
        assert_eq!(cutoff.retention_period, Duration::from_secs(30 * 24 * 3600));
    }

    #[test]
    fn test_partition_timestamp_extraction() {
        let mut partition_values = HashMap::new();
        partition_values.insert("hour".to_string(), "2024-01-15-14".to_string());

        let timestamp = extract_partition_timestamp(&partition_values).unwrap();

        assert_eq!(timestamp.year(), 2024);
        assert_eq!(timestamp.month(), 1);
        assert_eq!(timestamp.day(), 15);
        assert_eq!(timestamp.hour(), 14);
    }

    #[test]
    fn test_invalid_timezone_config() {
        let mut config = RetentionConfig::default();
        config.timezone = "Invalid/Timezone".to_string();

        let result = RetentionPolicyResolver::new(config);
        assert!(result.is_err());
    }

    #[test]
    fn test_grace_period_application() {
        let config = RetentionConfig {
            traces: Duration::from_secs(7 * 24 * 3600),
            grace_period: Duration::from_secs(3600),
            ..Default::default()
        };

        let resolver = RetentionPolicyResolver::new(config).unwrap();
        let cutoff = resolver.compute_cutoff("tenant1", "dataset1", SignalType::Traces).unwrap();

        // Cutoff should be 7 days + 1 hour ago
        let expected_cutoff = Utc::now() - chrono::Duration::seconds((7 * 24 + 1) * 3600);
        let diff = (cutoff.cutoff_timestamp - expected_cutoff).num_seconds().abs();
        assert!(diff < 2, "Cutoff should be approximately 7d 1h ago");
    }
}
```

### Integration Tests

```rust
#[tokio::test]
async fn test_retention_enforcement_end_to_end() {
    // Setup: Create catalog with expired and current partitions
    let catalog_manager = create_test_catalog_manager().await;

    // Create table with hourly partitions
    // - Partitions 30 days old (should be expired for traces)
    // - Partitions 1 day old (should be kept)

    let config = RetentionConfig {
        traces: Duration::from_secs(7 * 24 * 3600),
        ..Default::default()
    };

    let resolver = RetentionPolicyResolver::new(config).unwrap();
    let cutoff = resolver.compute_cutoff("test-tenant", "test-dataset", SignalType::Traces).unwrap();

    // Get partitions and identify expired ones
    let all_partitions = get_table_partitions(&catalog_manager, "traces").await.unwrap();
    let expired = identify_expired_partitions(&cutoff, all_partitions);

    assert_eq!(expired.len(), 24 * 23); // 23 days of hourly partitions should be expired
}
```

## Dependencies

### New Crate Dependencies

```toml
[dependencies]
chrono = { workspace = true }
chrono-tz = "0.10"  # Timezone support
humantime = "2.1"   # Duration parsing/formatting
humantime-serde = "1.1"  # Serde integration for Duration
```

## Implementation Phases

### Phase 3a: Foundation (Week 1)
- Add `RetentionConfig` and related data structures
- Implement `RetentionPolicyResolver`
- Add configuration parsing and validation
- Unit tests for policy resolution

### Phase 3b: Integration (Week 2)
- Integrate with `CompactorService`
- Implement partition timestamp extraction
- Add retention check scheduling
- Error handling and observability

### Phase 3c: Testing & Validation (Week 3)
- Comprehensive integration tests
- Performance testing with large partition counts
- Documentation and examples
- Production readiness review

## Migration Path

### Backward Compatibility

The retention feature is opt-in and disabled by default for existing deployments:

```rust
impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            enabled: false,  // Disabled by default
            // ... other defaults
        }
    }
}
```

Users must explicitly enable retention in their configuration:

```toml
[compactor.retention]
enabled = true
```

### Configuration Migration

For users upgrading from Phase 2, no configuration changes are required. To enable retention:

1. Add `[compactor.retention]` section to config
2. Set `enabled = true`
3. Configure retention periods per signal type
4. Optionally add tenant/dataset overrides

## Security and Safety Considerations

1. **Fail-Safe Default**: If retention cannot be computed (invalid config, parsing errors), default to NOT dropping data
2. **Grace Period**: Always apply grace period to prevent premature deletion due to clock skew
3. **Audit Logging**: Log all retention policy decisions and partition drops with full context
4. **Dry-Run Mode**: Support dry-run mode to preview retention enforcement without actually dropping data
5. **Configuration Validation**: Validate all retention configs at startup and reject invalid configurations

## Summary

This design provides a flexible, hierarchical retention policy system with:

- **Three-tier override hierarchy**: Global → Tenant → Dataset
- **Per-signal-type retention**: Different policies for traces, logs, and metrics
- **Time-zone aware**: Support for non-UTC timezones with DST handling
- **Fail-safe error handling**: Prefer keeping data over incorrect deletion
- **Observable and auditable**: Comprehensive logging and metrics
- **Backward compatible**: Opt-in feature, disabled by default
- **Integration-ready**: Designed to work with existing Iceberg hour partitioning

The system is designed to integrate cleanly with the existing compactor architecture and Iceberg table structure, providing a solid foundation for Phase 3 retention enforcement.
