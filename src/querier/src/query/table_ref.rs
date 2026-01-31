//! Shared table reference utility for multi-tenant queries.
//!
//! This module provides a unified way to build fully qualified table references
//! for multi-tenant queries across all SignalDB query services.
//!
//! The table reference format follows the pattern:
//! ```text
//! {tenant_slug}.{dataset_slug}.{table_name}
//! └────────────┘ └────────────┘ └──────────┘
//!    catalog       schema         table
//!   (tenant)      (dataset)
//! ```

use datafusion::common::TableReference;
use thiserror::Error;

/// Errors that can occur when building table references.
#[derive(Error, Debug)]
pub enum TableRefError {
    /// Invalid slug (tenant, dataset, or table name) - contains unsafe characters.
    #[error("Invalid slug '{0}': must contain only alphanumeric, underscore, or hyphen characters")]
    InvalidSlug(String),
    /// Empty slug not allowed.
    #[error("Empty slug not allowed")]
    EmptySlug,
}

/// Validates a slug (tenant, dataset, or table name) for safe use in table references.
///
/// # Validation Rules
/// - Must not be empty
/// - Must contain only alphanumeric characters, underscores, or hyphens
///
/// # Security
/// This validation prevents SQL injection attacks by rejecting any characters
/// that could be used to escape or manipulate SQL queries.
fn validate_slug(slug: &str) -> Result<(), TableRefError> {
    if slug.is_empty() {
        return Err(TableRefError::EmptySlug);
    }
    if !slug
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
    {
        return Err(TableRefError::InvalidSlug(slug.to_string()));
    }
    Ok(())
}

/// Builds a fully qualified table reference for multi-tenant queries.
///
/// Creates a DataFusion `TableReference` with:
/// - Catalog = `tenant_slug` (tenant isolation)
/// - Schema = `dataset_slug` (dataset isolation)
/// - Table = `table_name`
///
/// # Arguments
/// * `tenant_slug` - The URL-friendly slug for the tenant (e.g., "acme", "my_corp")
/// * `dataset_slug` - The URL-friendly slug for the dataset (e.g., "prod", "staging")
/// * `table_name` - The name of the table (e.g., "traces", "metrics_gauge")
///
/// # Returns
/// A `TableReference::Full` that can be used with DataFusion queries.
///
/// # Errors
/// Returns `TableRefError` if any of the slugs contain invalid characters
/// or are empty.
///
/// # Security
/// All inputs are validated to contain only alphanumeric characters, underscores,
/// and hyphens to prevent SQL injection attacks.
///
/// # Example
/// ```ignore
/// let table_ref = build_table_reference("acme", "prod", "traces")?;
/// // Results in: acme.prod.traces
/// ```
pub fn build_table_reference(
    tenant_slug: &str,
    dataset_slug: &str,
    table_name: &str,
) -> Result<TableReference, TableRefError> {
    validate_slug(tenant_slug)?;
    validate_slug(dataset_slug)?;
    validate_slug(table_name)?;

    Ok(TableReference::full(
        tenant_slug.to_string(),
        dataset_slug.to_string(),
        table_name.to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_table_reference_valid() {
        let table_ref = build_table_reference("acme", "prod", "traces").unwrap();
        match table_ref {
            TableReference::Full {
                catalog,
                schema,
                table,
            } => {
                assert_eq!(catalog.as_ref(), "acme");
                assert_eq!(schema.as_ref(), "prod");
                assert_eq!(table.as_ref(), "traces");
            }
            _ => panic!("Expected TableReference::Full"),
        }
    }

    #[test]
    fn test_build_table_reference_with_underscores() {
        let table_ref = build_table_reference("my_corp", "data_set", "my_table").unwrap();
        match table_ref {
            TableReference::Full {
                catalog,
                schema,
                table,
            } => {
                assert_eq!(catalog.as_ref(), "my_corp");
                assert_eq!(schema.as_ref(), "data_set");
                assert_eq!(table.as_ref(), "my_table");
            }
            _ => panic!("Expected TableReference::Full"),
        }
    }

    #[test]
    fn test_build_table_reference_with_hyphens() {
        let table_ref = build_table_reference("my-corp", "data-set", "my-table").unwrap();
        match table_ref {
            TableReference::Full {
                catalog,
                schema,
                table,
            } => {
                assert_eq!(catalog.as_ref(), "my-corp");
                assert_eq!(schema.as_ref(), "data-set");
                assert_eq!(table.as_ref(), "my-table");
            }
            _ => panic!("Expected TableReference::Full"),
        }
    }

    #[test]
    fn test_build_table_reference_metrics_tables() {
        // Test various metrics table names
        for table_name in [
            "metrics_gauge",
            "metrics_counter",
            "metrics_histogram",
            "metrics_summary",
        ] {
            let table_ref = build_table_reference("tenant", "dataset", table_name).unwrap();
            match table_ref {
                TableReference::Full { table, .. } => {
                    assert_eq!(table.as_ref(), table_name);
                }
                _ => panic!("Expected TableReference::Full"),
            }
        }
    }

    #[test]
    fn test_build_table_reference_empty_tenant_slug() {
        let result = build_table_reference("", "prod", "traces");
        assert!(matches!(result, Err(TableRefError::EmptySlug)));
    }

    #[test]
    fn test_build_table_reference_empty_dataset_slug() {
        let result = build_table_reference("acme", "", "traces");
        assert!(matches!(result, Err(TableRefError::EmptySlug)));
    }

    #[test]
    fn test_build_table_reference_empty_table_name() {
        let result = build_table_reference("acme", "prod", "");
        assert!(matches!(result, Err(TableRefError::EmptySlug)));
    }

    #[test]
    fn test_build_table_reference_invalid_chars_dot() {
        // Dots could be used for SQL injection
        let result = build_table_reference("acme.evil", "prod", "traces");
        assert!(matches!(result, Err(TableRefError::InvalidSlug(_))));
    }

    #[test]
    fn test_build_table_reference_invalid_chars_semicolon() {
        // Semicolons could terminate SQL statements
        let result = build_table_reference("acme; DROP TABLE", "prod", "traces");
        assert!(matches!(result, Err(TableRefError::InvalidSlug(_))));
    }

    #[test]
    fn test_build_table_reference_invalid_chars_quote() {
        // Quotes could escape strings
        let result = build_table_reference("acme'", "prod", "traces");
        assert!(matches!(result, Err(TableRefError::InvalidSlug(_))));
    }

    #[test]
    fn test_build_table_reference_invalid_chars_special() {
        // Various special characters that shouldn't be allowed
        for invalid_char in [
            '@', '#', '$', '%', '^', '&', '*', '(', ')', '+', '=', '{', '}', '[', ']', '|', '\\',
            '/', '?', '<', '>', ',', ' ',
        ] {
            let slug = format!("acme{invalid_char}corp");
            let result = build_table_reference(&slug, "prod", "traces");
            assert!(
                matches!(result, Err(TableRefError::InvalidSlug(_))),
                "Expected InvalidSlug error for character '{invalid_char}'"
            );
        }
    }

    #[test]
    fn test_validate_slug_alphanumeric() {
        assert!(validate_slug("abc123").is_ok());
        assert!(validate_slug("ABC123").is_ok());
        assert!(validate_slug("a1b2c3").is_ok());
    }

    #[test]
    fn test_validate_slug_with_underscore() {
        assert!(validate_slug("my_slug").is_ok());
        assert!(validate_slug("_leading").is_ok());
        assert!(validate_slug("trailing_").is_ok());
    }

    #[test]
    fn test_validate_slug_with_hyphen() {
        assert!(validate_slug("my-slug").is_ok());
        assert!(validate_slug("-leading").is_ok());
        assert!(validate_slug("trailing-").is_ok());
    }
}
