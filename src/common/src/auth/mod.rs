//! Authentication module for multi-tenancy support
//!
//! This module provides types and utilities for tenant-based authentication
//! using API keys and header-based tenant/dataset identification.

/// Tenant context extracted from authenticated request
#[derive(Debug, Clone)]
pub struct TenantContext {
    /// Unique tenant identifier
    pub tenant_id: String,
    /// Dataset identifier (resolved from header or default)
    pub dataset_id: String,
    /// Optional API key name for logging/audit
    pub api_key_name: Option<String>,
    /// Source of the tenant configuration (config file or database)
    pub source: TenantSource,
}

impl TenantContext {
    /// Create a new TenantContext
    pub fn new(
        tenant_id: String,
        dataset_id: String,
        api_key_name: Option<String>,
        source: TenantSource,
    ) -> Self {
        Self {
            tenant_id,
            dataset_id,
            api_key_name,
            source,
        }
    }
}

/// Source of tenant configuration
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TenantSource {
    /// Tenant defined in configuration file
    Config,
    /// Tenant created dynamically via API
    Database,
}

impl std::fmt::Display for TenantSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TenantSource::Config => write!(f, "config"),
            TenantSource::Database => write!(f, "database"),
        }
    }
}

/// Authentication error with HTTP status code
#[derive(Debug, Clone)]
pub struct AuthError {
    /// HTTP status code (400, 401, 403)
    pub status_code: u16,
    /// Error message for client
    pub message: String,
}

impl AuthError {
    /// Create a 400 Bad Request error (missing required headers)
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status_code: 400,
            message: message.into(),
        }
    }

    /// Create a 401 Unauthorized error (missing/invalid API key)
    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            status_code: 401,
            message: message.into(),
        }
    }

    /// Create a 403 Forbidden error (valid key but wrong tenant)
    pub fn forbidden(message: impl Into<String>) -> Self {
        Self {
            status_code: 403,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.status_code, self.message)
    }
}

impl std::error::Error for AuthError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tenant_context_creation() {
        let ctx = TenantContext::new(
            "acme".to_string(),
            "production".to_string(),
            Some("prod-key".to_string()),
            TenantSource::Config,
        );

        assert_eq!(ctx.tenant_id, "acme");
        assert_eq!(ctx.dataset_id, "production");
        assert_eq!(ctx.api_key_name, Some("prod-key".to_string()));
        assert_eq!(ctx.source, TenantSource::Config);
    }

    #[test]
    fn test_tenant_source_display() {
        assert_eq!(TenantSource::Config.to_string(), "config");
        assert_eq!(TenantSource::Database.to_string(), "database");
    }

    #[test]
    fn test_auth_error_constructors() {
        let bad_request = AuthError::bad_request("Missing X-Tenant-ID header");
        assert_eq!(bad_request.status_code, 400);
        assert_eq!(bad_request.message, "Missing X-Tenant-ID header");

        let unauthorized = AuthError::unauthorized("Invalid API key");
        assert_eq!(unauthorized.status_code, 401);
        assert_eq!(unauthorized.message, "Invalid API key");

        let forbidden = AuthError::forbidden("Tenant mismatch");
        assert_eq!(forbidden.status_code, 403);
        assert_eq!(forbidden.message, "Tenant mismatch");
    }

    #[test]
    fn test_auth_error_display() {
        let error = AuthError::unauthorized("Invalid API key");
        assert_eq!(error.to_string(), "401: Invalid API key");
    }
}
