//! Partition operations for Iceberg tables
//!
//! Provides functionality to list, analyze, and drop partitions
//! for retention enforcement.

use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDateTime, Utc};
use iceberg_rust::table::Table;
use std::collections::HashMap;

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

    /// List all partitions for a table
    ///
    /// Note: This is a placeholder. Full implementation would require
    /// scanning table metadata or using DataFusion to query partition info.
    pub async fn list_partitions(&self, _table: &Table) -> Result<Vec<PartitionInfo>> {
        // SAFETY: Explicitly error instead of returning empty list.
        // An empty partition list would cause retention enforcement to silently no-op,
        // potentially leaving old data indefinitely and violating retention policies.
        Err(anyhow::anyhow!(
            "list_partitions not implemented — manifest scanning required"
        ))
    }

    /// Generate SQL to drop a partition
    ///
    /// Returns the ALTER TABLE statement to drop the specified partition.
    ///
    /// # Arguments
    /// * `table_name` - Fully qualified table name (e.g., "tenant.dataset.traces")
    /// * `hour_value` - Partition hour value (e.g., "2024-01-15-10")
    pub fn generate_partition_drop_sql(
        &self,
        table_name: &str,
        hour_value: &str,
    ) -> Result<String> {
        // Validate the hour format first
        self.validate_partition_hour(hour_value)?;

        // SAFETY: Validate table_name to prevent SQL injection
        // Only allow alphanumeric characters, underscores, and dots for qualified names
        if !table_name
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '.')
        {
            return Err(anyhow::anyhow!(
                "Invalid table name '{}': must contain only alphanumeric characters, underscores, and dots",
                table_name
            ));
        }

        // Generate the DROP PARTITION statement
        // Note: Syntax may vary by query engine (DataFusion, Spark, etc.)
        let sql = format!(
            "ALTER TABLE {} DROP PARTITION (hour = '{}')",
            table_name, hour_value
        );

        Ok(sql)
    }

    /// Generate SQL to drop multiple partitions
    ///
    /// Returns a vector of ALTER TABLE statements, one per partition.
    pub fn generate_partition_drop_sql_batch(
        &self,
        table_name: &str,
        hour_values: &[String],
    ) -> Result<Vec<String>> {
        hour_values
            .iter()
            .map(|hour| self.generate_partition_drop_sql(table_name, hour))
            .collect()
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Datelike;
    use chrono::Timelike;

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
    fn test_generate_partition_drop_sql() {
        let manager = PartitionManager::new();

        let sql = manager
            .generate_partition_drop_sql("tenant.dataset.traces", "2024-01-15-10")
            .unwrap();

        assert!(sql.contains("ALTER TABLE"));
        assert!(sql.contains("DROP PARTITION"));
        assert!(sql.contains("hour = '2024-01-15-10'"));
    }

    #[test]
    fn test_generate_partition_drop_sql_batch() {
        let manager = PartitionManager::new();

        let hours = vec![
            "2024-01-15-10".to_string(),
            "2024-01-15-11".to_string(),
            "2024-01-15-12".to_string(),
        ];

        let sqls = manager
            .generate_partition_drop_sql_batch("tenant.dataset.traces", &hours)
            .unwrap();

        assert_eq!(sqls.len(), 3);
        for sql in sqls {
            assert!(sql.contains("ALTER TABLE"));
            assert!(sql.contains("DROP PARTITION"));
        }
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
