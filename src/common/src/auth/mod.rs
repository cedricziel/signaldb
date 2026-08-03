//! Authentication module for multi-tenancy support
//!
//! This module provides types and utilities for tenant-based authentication
//! using API keys and header-based tenant/dataset identification.

mod authenticator;
mod middleware;
pub mod password;
pub mod session;
pub mod validation;

pub use authenticator::Authenticator;
pub use middleware::{TenantContextExtractor, admin_auth_middleware, auth_middleware};
pub use password::{
    PasswordError, SESSION_TOKEN_PREFIX, generate_session_token, hash_password, hash_session_token,
    verify_password,
};
pub use session::{SESSION_COOKIE, session_token_from_headers};
pub use validation::{ValidationError, validate_dataset_id, validate_id, validate_tenant_id};

/// Per-signal read scopes granted over the query surface (e.g. the MCP read
/// tools). The mirror of the acceptor's ingest write scopes; a token or key
/// carrying `<signal>:read` may read that signal (see
/// [`TenantContext::can_read`]).
pub const READ_SCOPES: [&str; 3] = ["traces:read", "logs:read", "metrics:read"];

/// Human principal resolved from a server-side browser session.
#[derive(Debug, Clone)]
pub struct UserContext {
    pub user_id: String,
    pub email: String,
    pub is_instance_admin: bool,
    pub session_id: String,
}

/// Tenant context extracted from authenticated request
#[derive(Debug, Clone)]
pub struct TenantContext {
    /// Unique tenant identifier
    pub tenant_id: String,
    /// Dataset identifier (resolved from header or default)
    pub dataset_id: String,
    /// URL-friendly tenant slug for Iceberg namespace paths
    pub tenant_slug: String,
    /// URL-friendly dataset slug for Iceberg namespace paths
    pub dataset_slug: String,
    /// Optional API key name for logging/audit
    pub api_key_name: Option<String>,
    /// Explicit API-key scopes. `None` denotes a legacy unrestricted key or
    /// a human session; `Some` is always enforced.
    pub api_key_scopes: Option<Vec<String>>,
    /// Dataset restriction carried by a database-backed API key.
    pub api_key_dataset_id: Option<String>,
    /// Human user ID when the request was authenticated with a user session.
    pub user_id: Option<String>,
    /// Tenant role when the request was authenticated with a user session.
    pub role: Option<crate::catalog::MembershipRole>,
    /// Whether the authenticated human user is an instance administrator.
    pub is_instance_admin: bool,
    /// Server-side session ID, used to revoke the current browser session.
    pub session_id: Option<String>,
    /// Source of the tenant configuration (config file or database)
    pub source: TenantSource,
}

impl TenantContext {
    /// Create a new TenantContext
    pub fn new(
        tenant_id: String,
        dataset_id: String,
        tenant_slug: String,
        dataset_slug: String,
        api_key_name: Option<String>,
        source: TenantSource,
    ) -> Self {
        Self {
            tenant_id,
            dataset_id,
            tenant_slug,
            dataset_slug,
            api_key_name,
            api_key_scopes: None,
            api_key_dataset_id: None,
            user_id: None,
            role: None,
            is_instance_admin: false,
            session_id: None,
            source,
        }
    }

    /// Attach authorization restrictions from a database-backed API key.
    pub fn with_api_key_restrictions(
        mut self,
        scopes: Option<Vec<String>>,
        dataset_id: Option<String>,
    ) -> Self {
        self.api_key_scopes = scopes;
        self.api_key_dataset_id = dataset_id;
        self
    }

    /// Attach the human principal that produced this tenant context.
    pub fn with_user(
        mut self,
        user_id: String,
        role: crate::catalog::MembershipRole,
        is_instance_admin: bool,
        session_id: String,
    ) -> Self {
        self.user_id = Some(user_id);
        self.role = Some(role);
        self.is_instance_admin = is_instance_admin;
        self.session_id = Some(session_id);
        self
    }

    /// Whether the principal may administer this tenant.
    pub fn can_manage_tenant(&self) -> bool {
        self.is_instance_admin
            || self.role == Some(crate::catalog::MembershipRole::Admin)
            || self.user_id.is_none()
    }

    /// Whether the principal may write data in this tenant.
    pub fn can_write(&self) -> bool {
        self.user_id.is_none()
            || self.is_instance_admin
            || matches!(
                self.role,
                Some(
                    crate::catalog::MembershipRole::Admin | crate::catalog::MembershipRole::Member
                )
            )
    }

    /// Whether this principal may ingest a particular telemetry signal.
    pub fn can_ingest(&self, signal: &str) -> bool {
        if !self.can_write() {
            return false;
        }
        let required = format!("{signal}:write");
        self.api_key_scopes
            .as_ref()
            .is_none_or(|scopes| scopes.iter().any(|scope| scope == &required))
    }

    /// Whether this principal may read a particular telemetry signal.
    ///
    /// Every membership role (including `Viewer`) may read; a legacy key with
    /// no explicit scopes is unrestricted. When scopes are present, the
    /// matching `<signal>:read` scope is required — write scopes do not grant
    /// read. This mirrors [`can_ingest`](Self::can_ingest) on the query side.
    pub fn can_read(&self, signal: &str) -> bool {
        let required = format!("{signal}:read");
        self.api_key_scopes
            .as_ref()
            .is_none_or(|scopes| scopes.iter().any(|scope| scope == &required))
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

#[cfg(test)]
mod scoped_authorization_tests {
    use super::*;

    fn context(scopes: Option<Vec<String>>) -> TenantContext {
        TenantContext::new(
            "acme".into(),
            "production".into(),
            "acme".into(),
            "production".into(),
            Some("collector".into()),
            TenantSource::Database,
        )
        .with_api_key_restrictions(scopes, Some("production".into()))
    }

    #[test]
    fn every_ingestion_signal_requires_its_matching_write_scope() {
        for signal in ["metrics", "logs", "traces", "profiles"] {
            let allowed = context(Some(vec![format!("{signal}:write")]));
            assert!(allowed.can_ingest(signal));
            for other in ["metrics", "logs", "traces", "profiles"] {
                if other != signal {
                    assert!(!allowed.can_ingest(other));
                }
            }
        }
    }

    #[test]
    fn legacy_unscoped_keys_remain_unrestricted() {
        let legacy = context(None);
        for signal in ["metrics", "logs", "traces", "profiles"] {
            assert!(legacy.can_ingest(signal));
        }
    }

    #[test]
    fn viewer_sessions_are_read_only() {
        let viewer = context(None).with_user(
            "user-1".into(),
            crate::catalog::MembershipRole::Viewer,
            false,
            "session-1".into(),
        );
        assert!(!viewer.can_write());
        assert!(!viewer.can_ingest("metrics"));
        assert!(!viewer.can_manage_tenant());
    }

    #[test]
    fn every_read_signal_requires_its_matching_read_scope() {
        for signal in ["metrics", "logs", "traces"] {
            let allowed = context(Some(vec![format!("{signal}:read")]));
            assert!(allowed.can_read(signal));
            for other in ["metrics", "logs", "traces"] {
                if other != signal {
                    assert!(!allowed.can_read(other));
                }
            }
        }
    }

    #[test]
    fn legacy_unscoped_keys_may_read_any_signal() {
        let legacy = context(None);
        for signal in ["metrics", "logs", "traces"] {
            assert!(legacy.can_read(signal));
        }
    }

    #[test]
    fn viewer_sessions_may_still_read() {
        let viewer = context(None).with_user(
            "user-1".into(),
            crate::catalog::MembershipRole::Viewer,
            false,
            "session-1".into(),
        );
        for signal in ["metrics", "logs", "traces"] {
            assert!(viewer.can_read(signal));
        }
    }

    #[test]
    fn write_scopes_do_not_grant_read() {
        let writer_only = context(Some(vec!["traces:write".into()]));
        assert!(!writer_only.can_read("traces"));
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
            "acme".to_string(),
            "production".to_string(),
            Some("prod-key".to_string()),
            TenantSource::Config,
        );

        assert_eq!(ctx.tenant_id, "acme");
        assert_eq!(ctx.dataset_id, "production");
        assert_eq!(ctx.tenant_slug, "acme");
        assert_eq!(ctx.dataset_slug, "production");
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
