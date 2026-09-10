//! Partition operations for Iceberg tables
//!
//! Provides functionality to list, analyze, and drop partitions
//! for retention enforcement.

use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDateTime, Utc};
use futures::StreamExt;
use iceberg_rust::spec::manifest::{DataFile, Status};
use iceberg_rust::spec::values::Value;
use iceberg_rust::table::Table;
use std::collections::HashMap;
use tracing::warn;

/// Name of the hour partition field used by all SignalDB signal tables
/// (Iceberg Hour transform on the `timestamp` column).
pub const TIMESTAMP_HOUR_FIELD: &str = "timestamp_hour";

/// Extract the hour partition value (hours since Unix epoch) for a data file.
///
/// The authoritative source is the typed partition struct recorded in the
/// manifest entry (`DataFile::partition`): the Hour transform stores an
/// integer, so this is layout-independent and survives any change to file
/// locations (object-storage hash prefixes, location providers, fork bumps).
///
/// Parsing the `timestamp_hour=<N>` file-path component is kept only as a
/// fallback for manifest entries that genuinely lack a usable partition
/// value (absent field or null value, e.g. legacy manifests); the fallback
/// logs a warning because it depends on the storage layout.
///
/// Returns `None` when neither source yields a value — callers must treat
/// such files as unclassifiable and keep them (safe default).
pub fn data_file_partition_hours(data_file: &DataFile) -> Option<i64> {
    match data_file.partition().get(TIMESTAMP_HOUR_FIELD) {
        Some(Some(Value::Int(hours))) => return Some(i64::from(*hours)),
        Some(Some(Value::LongInt(hours))) => return Some(*hours),
        Some(Some(unexpected)) => {
            warn!(
                file.path = %data_file.file_path(),
                signaldb.job.partition_value = ?unexpected,
                "Manifest partition value for timestamp_hour has an unexpected type; \
                 falling back to file-path parsing"
            );
        }
        // Field absent or value null: fall through to the path fallback.
        _ => {}
    }

    let recovered = data_file
        .file_path()
        .split('/')
        .find(|component| component.starts_with("timestamp_hour="))
        .and_then(|component| component.strip_prefix("timestamp_hour="))
        .and_then(|hours| hours.parse::<i64>().ok());

    if recovered.is_some() {
        warn!(
            file.path = %data_file.file_path(),
            "Manifest entry has no usable timestamp_hour partition value; \
             recovered it from the file path (layout-dependent fallback)"
        );
    }

    recovered
}

/// Information about a table partition
#[derive(Debug, Clone)]
pub struct PartitionInfo {
    /// Partition specification (e.g., {"hour": "2024-01-15-10"})
    pub partition_values: HashMap<String, String>,
    /// Partition timestamp (extracted from hour field)
    pub timestamp: DateTime<Utc>,
    /// Number of data files in this partition (if available)
    pub file_count: Option<usize>,
    /// Total size of data files in bytes (if available)
    pub total_size_bytes: Option<u64>,
}

impl PartitionInfo {
    /// Check if partition is older than the given cutoff timestamp
    pub fn is_older_than(&self, cutoff: &DateTime<Utc>) -> bool {
        self.timestamp < *cutoff
    }

    /// Get partition hour value (e.g., "2024-01-15-10")
    pub fn get_hour_value(&self) -> Option<&str> {
        self.partition_values.get("hour").map(|s| s.as_str())
    }
}

/// Manages Iceberg table partition operations
pub struct PartitionManager;

impl PartitionManager {
    /// Create a new partition manager
    pub fn new() -> Self {
        Self
    }

    /// Parse partition hour value to DateTime
    ///
    /// Expected format: "YYYY-MM-DD-HH" (e.g., "2024-01-15-10")
    pub fn parse_partition_hour(&self, hour_value: &str) -> Result<DateTime<Utc>> {
        // Expected format: YYYY-MM-DD-HH
        let parts: Vec<&str> = hour_value.split('-').collect();

        if parts.len() != 4 {
            return Err(anyhow::anyhow!(
                "Invalid partition hour format '{}': expected YYYY-MM-DD-HH",
                hour_value
            ));
        }

        let year: i32 = parts[0]
            .parse()
            .with_context(|| format!("Invalid year in partition '{}'", hour_value))?;

        let month: u32 = parts[1]
            .parse()
            .with_context(|| format!("Invalid month in partition '{}'", hour_value))?;

        let day: u32 = parts[2]
            .parse()
            .with_context(|| format!("Invalid day in partition '{}'", hour_value))?;

        let hour: u32 = parts[3]
            .parse()
            .with_context(|| format!("Invalid hour in partition '{}'", hour_value))?;

        // Validate ranges
        if !(1..=12).contains(&month) {
            return Err(anyhow::anyhow!(
                "Invalid month {} in partition '{}'",
                month,
                hour_value
            ));
        }

        if !(1..=31).contains(&day) {
            return Err(anyhow::anyhow!(
                "Invalid day {} in partition '{}'",
                day,
                hour_value
            ));
        }

        if hour >= 24 {
            return Err(anyhow::anyhow!(
                "Invalid hour {} in partition '{}'",
                hour,
                hour_value
            ));
        }

        // Build DateTime
        let naive_dt = NaiveDateTime::new(
            chrono::NaiveDate::from_ymd_opt(year, month, day)
                .ok_or_else(|| anyhow::anyhow!("Invalid date: {}-{}-{}", year, month, day))?,
            chrono::NaiveTime::from_hms_opt(hour, 0, 0)
                .ok_or_else(|| anyhow::anyhow!("Invalid hour: {}", hour))?,
        );

        Ok(DateTime::from_naive_utc_and_offset(naive_dt, Utc))
    }

    /// Validate partition hour format
    pub fn validate_partition_hour(&self, hour_value: &str) -> Result<()> {
        self.parse_partition_hour(hour_value)?;
        Ok(())
    }

    /// List all partitions for a table by scanning its current snapshot manifests.
    ///
    /// Reads manifest files from the current snapshot, groups non-deleted data
    /// files by the `timestamp_hour` partition value recorded in each manifest
    /// entry (integer hours since Unix epoch, produced by the Iceberg Hour
    /// transform), and returns one `PartitionInfo` per unique partition value
    /// found. Files whose partition value cannot be determined (see
    /// [`data_file_partition_hours`]) are excluded from the listing with a
    /// warning — they never enter retention decisions.
    pub async fn list_partitions(&self, table: &Table) -> Result<Vec<PartitionInfo>> {
        let all_manifests = table
            .manifests(None, None)
            .await
            .context("Failed to read manifest list")?;

        if all_manifests.is_empty() {
            return Ok(vec![]);
        }

        let file_iter = table
            .datafiles(&all_manifests, None, (None, None))
            .await
            .context("Failed to read data files from manifests")?;

        // Accumulate (file_count, total_bytes) per partition hour (integer hours since epoch).
        let mut by_hour: HashMap<i64, (usize, u64)> = HashMap::new();

        let mut file_iter = std::pin::pin!(file_iter);
        while let Some(result) = file_iter.next().await {
            let (_, entry) = result.context("Failed to read manifest entry")?;
            if *entry.status() == Status::Deleted {
                continue;
            }
            let data_file = entry.data_file();
            let file_size = *data_file.file_size_in_bytes() as u64;

            match data_file_partition_hours(data_file) {
                Some(hours) => {
                    let slot = by_hour.entry(hours).or_insert((0, 0));
                    slot.0 += 1;
                    slot.1 += file_size;
                }
                None => {
                    warn!(
                        file.path = %data_file.file_path(),
                        "Data file has no recoverable timestamp_hour partition value; \
                         excluding it from the partition listing"
                    );
                }
            }
        }

        let partitions = by_hour
            .into_iter()
            .filter_map(|(hours_since_epoch, (file_count, total_bytes))| {
                // Convert integer hours-since-epoch to DateTime<Utc>
                let secs = hours_since_epoch.checked_mul(3600)?;
                let timestamp = DateTime::from_timestamp(secs, 0)?;

                // Store ISO format in "hour" key for display/logging compat
                let iso_hour = timestamp.format("%Y-%m-%d-%H").to_string();
                let mut partition_values = HashMap::new();
                partition_values.insert("hour".to_string(), iso_hour);
                partition_values.insert(
                    TIMESTAMP_HOUR_FIELD.to_string(),
                    hours_since_epoch.to_string(),
                );

                Some(PartitionInfo {
                    partition_values,
                    timestamp,
                    file_count: Some(file_count),
                    total_size_bytes: if total_bytes > 0 {
                        Some(total_bytes)
                    } else {
                        None
                    },
                })
            })
            .collect();

        Ok(partitions)
    }

    /// Filter partitions older than cutoff
    pub fn filter_partitions_older_than(
        &self,
        partitions: Vec<PartitionInfo>,
        cutoff: &DateTime<Utc>,
    ) -> Vec<PartitionInfo> {
        partitions
            .into_iter()
            .filter(|p| p.is_older_than(cutoff))
            .collect()
    }
}

impl Default for PartitionManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Test-only helpers for constructing manifest-entry data files, shared
/// with the retention enforcer's unit tests.
#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use iceberg_rust::spec::manifest::{Content, FileFormat};
    use iceberg_rust::spec::values::Struct;

    /// Build a minimal manifest-entry data file with the given partition
    /// struct and file path.
    pub(crate) fn test_data_file(partition: Struct, file_path: &str) -> DataFile {
        DataFile::builder()
            .with_content(Content::Data)
            .with_file_path(file_path.to_string())
            .with_file_format(FileFormat::Parquet)
            .with_partition(partition)
            .with_record_count(1)
            .with_file_size_in_bytes(1024)
            .with_column_sizes(None)
            .with_value_counts(None)
            .with_null_value_counts(None)
            .with_nan_value_counts(None)
            .with_distinct_counts(None)
            .with_lower_bounds(None)
            .with_upper_bounds(None)
            .build()
            .expect("test data file must build")
    }

    /// Partition struct with a single `timestamp_hour` field.
    pub(crate) fn hour_partition(hours: i32) -> Struct {
        Struct::from_iter([(TIMESTAMP_HOUR_FIELD.to_string(), Some(Value::Int(hours)))])
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::{hour_partition, test_data_file};
    use super::*;
    use chrono::Datelike;
    use chrono::Timelike;
    use iceberg_rust::spec::values::Struct;

    #[test]
    fn partition_hours_come_from_manifest_entry_not_file_path() {
        // Path deliberately has NO timestamp_hour= component: any layout
        // (hash prefixes, custom location providers) must still classify.
        let data_file = test_data_file(
            hour_partition(473_364),
            "s3://bucket/3af9c2/data/00000-0-abc.parquet",
        );

        assert_eq!(data_file_partition_hours(&data_file), Some(473_364));
    }

    #[test]
    fn partition_hours_prefer_manifest_value_over_conflicting_path() {
        let data_file = test_data_file(
            hour_partition(473_364),
            "s3://bucket/data/timestamp_hour=999999/00000-0-abc.parquet",
        );

        assert_eq!(data_file_partition_hours(&data_file), Some(473_364));
    }

    #[test]
    fn partition_hours_accept_long_int_manifest_value() {
        let partition = Struct::from_iter([(
            TIMESTAMP_HOUR_FIELD.to_string(),
            Some(Value::LongInt(473_364)),
        )]);
        let data_file = test_data_file(partition, "s3://bucket/data/00000-0-abc.parquet");

        assert_eq!(data_file_partition_hours(&data_file), Some(473_364));
    }

    #[test]
    fn partition_hours_fall_back_to_path_when_manifest_value_is_null() {
        let partition = Struct::from_iter([(TIMESTAMP_HOUR_FIELD.to_string(), None)]);
        let data_file = test_data_file(
            partition,
            "s3://bucket/data/timestamp_hour=473364/00000-0-abc.parquet",
        );

        assert_eq!(data_file_partition_hours(&data_file), Some(473_364));
    }

    #[test]
    fn partition_hours_fall_back_to_path_when_partition_struct_is_empty() {
        let partition = Struct::from_iter(std::iter::empty::<(String, Option<Value>)>());
        let data_file = test_data_file(
            partition,
            "s3://bucket/data/timestamp_hour=473364/00000-0-abc.parquet",
        );

        assert_eq!(data_file_partition_hours(&data_file), Some(473_364));
    }

    #[test]
    fn partition_hours_none_when_manifest_and_path_both_lack_value() {
        let partition = Struct::from_iter(std::iter::empty::<(String, Option<Value>)>());
        let data_file = test_data_file(partition, "s3://bucket/data/00000-0-abc.parquet");

        assert_eq!(data_file_partition_hours(&data_file), None);
    }

    #[test]
    fn test_parse_partition_hour_valid() {
        let manager = PartitionManager::new();

        // Valid partition hour
        let result = manager.parse_partition_hour("2024-01-15-10");
        assert!(result.is_ok());

        let dt = result.unwrap();
        assert_eq!(dt.year(), 2024);
        assert_eq!(dt.month(), 1);
        assert_eq!(dt.day(), 15);
        assert_eq!(dt.hour(), 10);
    }

    #[test]
    fn test_parse_partition_hour_invalid_format() {
        let manager = PartitionManager::new();

        // Invalid formats
        let invalid_cases = vec![
            "2024-01-15",          // Missing hour
            "2024-01-15-10-30",    // Extra field
            "2024-13-15-10",       // Invalid month
            "2024-01-32-10",       // Invalid day
            "2024-01-15-24",       // Invalid hour
            "not-a-date",          // Complete garbage
            "2024-01-15-10-extra", // Extra data
        ];

        for case in invalid_cases {
            let result = manager.parse_partition_hour(case);
            assert!(result.is_err(), "Should fail for: {}", case);
        }
    }

    #[test]
    fn test_validate_partition_hour() {
        let manager = PartitionManager::new();

        assert!(manager.validate_partition_hour("2024-01-15-10").is_ok());
        assert!(manager.validate_partition_hour("2024-13-15-10").is_err());
    }

    #[test]
    fn test_partition_info_is_older_than() {
        let mut partition_values = HashMap::new();
        partition_values.insert("hour".to_string(), "2024-01-15-10".to_string());

        let partition = PartitionInfo {
            partition_values,
            timestamp: DateTime::parse_from_rfc3339("2024-01-15T10:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            file_count: Some(5),
            total_size_bytes: Some(1024 * 1024),
        };

        let cutoff_newer = DateTime::parse_from_rfc3339("2024-01-15T11:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert!(partition.is_older_than(&cutoff_newer));

        let cutoff_older = DateTime::parse_from_rfc3339("2024-01-15T09:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert!(!partition.is_older_than(&cutoff_older));
    }

    #[test]
    fn test_filter_partitions_older_than() {
        let manager = PartitionManager::new();

        let partitions = vec![
            PartitionInfo {
                partition_values: {
                    let mut map = HashMap::new();
                    map.insert("hour".to_string(), "2024-01-15-08".to_string());
                    map
                },
                timestamp: DateTime::parse_from_rfc3339("2024-01-15T08:00:00Z")
                    .unwrap()
                    .with_timezone(&Utc),
                file_count: None,
                total_size_bytes: None,
            },
            PartitionInfo {
                partition_values: {
                    let mut map = HashMap::new();
                    map.insert("hour".to_string(), "2024-01-15-10".to_string());
                    map
                },
                timestamp: DateTime::parse_from_rfc3339("2024-01-15T10:00:00Z")
                    .unwrap()
                    .with_timezone(&Utc),
                file_count: None,
                total_size_bytes: None,
            },
            PartitionInfo {
                partition_values: {
                    let mut map = HashMap::new();
                    map.insert("hour".to_string(), "2024-01-15-12".to_string());
                    map
                },
                timestamp: DateTime::parse_from_rfc3339("2024-01-15T12:00:00Z")
                    .unwrap()
                    .with_timezone(&Utc),
                file_count: None,
                total_size_bytes: None,
            },
        ];

        let cutoff = DateTime::parse_from_rfc3339("2024-01-15T10:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        let old_partitions = manager.filter_partitions_older_than(partitions, &cutoff);

        assert_eq!(old_partitions.len(), 1);
        assert_eq!(old_partitions[0].get_hour_value(), Some("2024-01-15-08"));
    }
}
