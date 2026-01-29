//! Tenant and Dataset ID validation utilities
//!
//! This module provides reusable validation functions for tenant and dataset
//! identifiers used throughout SignalDB. Consistent validation ensures security
//! and prevents issues with path traversal, filesystem naming, and SQL injection.

use super::AuthError;

/// Maximum length for tenant and dataset IDs
pub const MAX_ID_LENGTH: usize = 64;

/// Validation error types for tenant/dataset IDs
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationError {
    /// ID is empty after trimming whitespace
    Empty,
    /// ID exceeds maximum length
    TooLong { max: usize, actual: usize },
    /// ID contains invalid characters
    InvalidCharacters { invalid: Vec<char> },
    /// ID contains path traversal patterns
    PathTraversal,
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValidationError::Empty => write!(f, "ID cannot be empty"),
            ValidationError::TooLong { max, actual } => {
                write!(f, "ID exceeds maximum length ({actual} > {max})")
            }
            ValidationError::InvalidCharacters { invalid } => {
                let chars: String = invalid.iter().collect();
                write!(f, "ID contains invalid characters: '{chars}'")
            }
            ValidationError::PathTraversal => {
                write!(f, "ID contains path traversal patterns")
            }
        }
    }
}

impl std::error::Error for ValidationError {}

impl From<ValidationError> for AuthError {
    fn from(err: ValidationError) -> Self {
        AuthError::bad_request(err.to_string())
    }
}

/// Check if a character is valid for tenant/dataset IDs
///
/// Valid characters are:
/// - Lowercase letters: a-z
/// - Uppercase letters: A-Z
/// - Digits: 0-9
/// - Hyphen: -
/// - Underscore: _
fn is_valid_id_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '-' || c == '_'
}

/// Validate a tenant or dataset ID
///
/// Validation rules:
/// - Trims leading/trailing whitespace
/// - Rejects empty strings
/// - Allows only: a-z, A-Z, 0-9, -, _
/// - Max length: 64 characters (after trimming)
/// - Rejects path traversal patterns (.., /, \)
///
/// Returns the trimmed, validated ID on success.
///
/// # Examples
///
/// ```
/// use common::auth::validation::validate_id;
///
/// // Valid IDs
/// assert!(validate_id("acme").is_ok());
/// assert!(validate_id("my-tenant").is_ok());
/// assert!(validate_id("Tenant_123").is_ok());
///
/// // Invalid IDs
/// assert!(validate_id("").is_err());           // Empty
/// assert!(validate_id("../evil").is_err());    // Path traversal
/// assert!(validate_id("tenant/sub").is_err()); // Contains /
/// ```
pub fn validate_id(id: &str) -> Result<String, ValidationError> {
    // Trim whitespace
    let trimmed = id.trim();

    // Check for empty
    if trimmed.is_empty() {
        return Err(ValidationError::Empty);
    }

    // Check length
    if trimmed.len() > MAX_ID_LENGTH {
        return Err(ValidationError::TooLong {
            max: MAX_ID_LENGTH,
            actual: trimmed.len(),
        });
    }

    // Check for path traversal patterns
    if trimmed.contains("..") || trimmed.contains('/') || trimmed.contains('\\') {
        return Err(ValidationError::PathTraversal);
    }

    // Check for invalid characters
    let invalid_chars: Vec<char> = trimmed.chars().filter(|c| !is_valid_id_char(*c)).collect();

    if !invalid_chars.is_empty() {
        return Err(ValidationError::InvalidCharacters {
            invalid: invalid_chars,
        });
    }

    Ok(trimmed.to_string())
}

/// Validate a tenant ID
///
/// Wrapper around `validate_id` with tenant-specific error messages.
pub fn validate_tenant_id(tenant_id: &str) -> Result<String, AuthError> {
    validate_id(tenant_id).map_err(|e| AuthError::bad_request(format!("Invalid tenant ID: {e}")))
}

/// Validate a dataset ID
///
/// Wrapper around `validate_id` with dataset-specific error messages.
pub fn validate_dataset_id(dataset_id: &str) -> Result<String, AuthError> {
    validate_id(dataset_id).map_err(|e| AuthError::bad_request(format!("Invalid dataset ID: {e}")))
}

/// Sanitize an ID for use in file paths
///
/// Converts to lowercase and replaces any remaining problematic characters.
/// This is useful for generating consistent slug-based paths.
///
/// Note: This should only be called after validation passes.
pub fn sanitize_for_path(id: &str) -> String {
    id.trim().to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_id_valid_inputs() {
        // Simple alphanumeric
        assert_eq!(validate_id("acme").unwrap(), "acme");
        assert_eq!(validate_id("tenant123").unwrap(), "tenant123");

        // With hyphens and underscores
        assert_eq!(validate_id("my-tenant").unwrap(), "my-tenant");
        assert_eq!(validate_id("my_tenant").unwrap(), "my_tenant");
        assert_eq!(validate_id("my-tenant_123").unwrap(), "my-tenant_123");

        // Mixed case
        assert_eq!(validate_id("AcmeCorp").unwrap(), "AcmeCorp");
        assert_eq!(validate_id("ACME").unwrap(), "ACME");
    }

    #[test]
    fn test_validate_id_trims_whitespace() {
        assert_eq!(validate_id("  acme  ").unwrap(), "acme");
        assert_eq!(validate_id("\tacme\n").unwrap(), "acme");
        assert_eq!(validate_id("   my-tenant   ").unwrap(), "my-tenant");
    }

    #[test]
    fn test_validate_id_empty_string() {
        assert_eq!(validate_id(""), Err(ValidationError::Empty));
        assert_eq!(validate_id("   "), Err(ValidationError::Empty));
        assert_eq!(validate_id("\t\n"), Err(ValidationError::Empty));
    }

    #[test]
    fn test_validate_id_too_long() {
        let long_id = "a".repeat(65);
        let result = validate_id(&long_id);
        assert!(matches!(
            result,
            Err(ValidationError::TooLong {
                max: 64,
                actual: 65
            })
        ));

        // Exactly 64 should be fine
        let max_id = "a".repeat(64);
        assert!(validate_id(&max_id).is_ok());
    }

    #[test]
    fn test_validate_id_path_traversal() {
        assert_eq!(validate_id(".."), Err(ValidationError::PathTraversal));
        assert_eq!(validate_id("../evil"), Err(ValidationError::PathTraversal));
        assert_eq!(
            validate_id("foo/../bar"),
            Err(ValidationError::PathTraversal)
        );
        assert_eq!(
            validate_id("tenant/sub"),
            Err(ValidationError::PathTraversal)
        );
        assert_eq!(
            validate_id("tenant\\sub"),
            Err(ValidationError::PathTraversal)
        );
    }

    #[test]
    fn test_validate_id_invalid_characters() {
        // Special characters
        let result = validate_id("acme@corp");
        assert!(matches!(
            result,
            Err(ValidationError::InvalidCharacters { .. })
        ));

        let result = validate_id("tenant#1");
        assert!(matches!(
            result,
            Err(ValidationError::InvalidCharacters { .. })
        ));

        let result = validate_id("my tenant");
        assert!(matches!(
            result,
            Err(ValidationError::InvalidCharacters { .. })
        ));

        let result = validate_id("tenant.name");
        assert!(matches!(
            result,
            Err(ValidationError::InvalidCharacters { .. })
        ));

        // Unicode characters
        let result = validate_id("tenantñ");
        assert!(matches!(
            result,
            Err(ValidationError::InvalidCharacters { .. })
        ));
    }

    #[test]
    fn test_validate_tenant_id() {
        // Valid
        assert!(validate_tenant_id("acme").is_ok());
        assert!(validate_tenant_id("  acme  ").is_ok());

        // Invalid - check error message format
        let err = validate_tenant_id("").unwrap_err();
        assert_eq!(err.status_code, 400);
        assert!(err.message.contains("Invalid tenant ID"));
        assert!(err.message.contains("empty"));
    }

    #[test]
    fn test_validate_dataset_id() {
        // Valid
        assert!(validate_dataset_id("production").is_ok());
        assert!(validate_dataset_id("  staging  ").is_ok());

        // Invalid - check error message format
        let err = validate_dataset_id("../etc").unwrap_err();
        assert_eq!(err.status_code, 400);
        assert!(err.message.contains("Invalid dataset ID"));
        assert!(err.message.contains("path traversal"));
    }

    #[test]
    fn test_sanitize_for_path() {
        assert_eq!(sanitize_for_path("ACME"), "acme");
        assert_eq!(sanitize_for_path("  MyTenant  "), "mytenant");
        assert_eq!(sanitize_for_path("My-Tenant_123"), "my-tenant_123");
    }

    #[test]
    fn test_validation_error_display() {
        assert_eq!(ValidationError::Empty.to_string(), "ID cannot be empty");

        assert_eq!(
            ValidationError::TooLong {
                max: 64,
                actual: 100
            }
            .to_string(),
            "ID exceeds maximum length (100 > 64)"
        );

        assert_eq!(
            ValidationError::InvalidCharacters {
                invalid: vec!['@', '#']
            }
            .to_string(),
            "ID contains invalid characters: '@#'"
        );

        assert_eq!(
            ValidationError::PathTraversal.to_string(),
            "ID contains path traversal patterns"
        );
    }

    #[test]
    fn test_validation_error_to_auth_error() {
        let validation_err = ValidationError::Empty;
        let auth_err: AuthError = validation_err.into();
        assert_eq!(auth_err.status_code, 400);
        assert!(auth_err.message.contains("empty"));
    }
}
