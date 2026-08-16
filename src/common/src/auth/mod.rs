//! Authentication module for multi-tenancy support
//!
//! This module provides types and utilities for tenant-based authentication
//! using API keys and header-based tenant/dataset identification.

mod authenticator;
mod middleware;
pub mod oauth;
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
pub use validation::{
    ValidationError, validate_dataset_id, validate_id, validate_scopes, validate_tenant_id,
};

/// Scope granting read access to the schema registry (registries, attribute /
/// entity / metric lookups). A read scope: OAuth grants it by default.
pub const SCHEMA_READ_SCOPE: &str = "schema:read";

/// Scope granting mutation of custom schema registries. Never OAuth-grantable.
pub const SCHEMA_WRITE_SCOPE: &str = "schema:write";

/// Both schema-registry scopes.
pub const SCHEMA_SCOPES: [&str; 2] = [SCHEMA_READ_SCOPE, SCHEMA_WRITE_SCOPE];

/// Per-signal ingest scopes enforced by the acceptor; a key carrying
/// `<signal>:write` may ingest that signal (see [`TenantContext::can_ingest`]).
pub const INGEST_SCOPES: [&str; 4] = [
    "metrics:write",
    "logs:write",
    "traces:write",
    "profiles:write",
];

/// Read scopes granted over the query surface (e.g. the MCP read tools) and
/// grantable through OAuth consent. A token or key carrying `<signal>:read`
/// may read that signal (see [`TenantContext::can_read`]); `schema:read`
/// covers the schema registry.
pub const READ_SCOPES: [&str; 5] = [
    "traces:read",
    "logs:read",
    "metrics:read",
    "profiles:read",
    SCHEMA_READ_SCOPE,
];

/// Scope granting an API key self-management of the tenant it belongs to
/// (datasets, API keys, memberships, schema view via the management API).
/// Never OAuth-grantable, and — unlike every other scope — **explicit only**:
/// a legacy key without scopes does NOT gain management (see
/// [`TenantContext::can_manage_via_key`]).
pub const TENANT_MANAGE_SCOPE: &str = "tenant:manage";

/// The complete API-key scope vocabulary: `INGEST_SCOPES ∪ READ_SCOPES ∪
/// SCHEMA_SCOPES ∪ {TENANT_MANAGE_SCOPE}`. Every key-management surface
/// (admin API, management API, CLI, MCP, UI) accepts exactly these; see
/// [`validate_scopes`].
pub const API_KEY_SCOPES: [&str; 11] = [
    "metrics:write",
    "logs:write",
    "traces:write",
    "profiles:write",
    "traces:read",
    "logs:read",
    "metrics:read",
    "profiles:read",
    SCHEMA_READ_SCOPE,
    SCHEMA_WRITE_SCOPE,
    TENANT_MANAGE_SCOPE,
];

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
    ///
    /// `session_id` is the browser session that can revoke this context;
    /// `None` for principals with no revocable browser session (e.g. an OAuth
    /// access token, whose revocation is the token row itself).
    pub fn with_user(
        mut self,
        user_id: String,
        role: crate::catalog::MembershipRole,
        is_instance_admin: bool,
        session_id: Option<String>,
    ) -> Self {
        self.user_id = Some(user_id);
        self.role = Some(role);
        self.is_instance_admin = is_instance_admin;
        self.session_id = session_id;
        self
    }

    /// Whether the principal may administer this tenant.
    pub fn can_manage_tenant(&self) -> bool {
        self.is_instance_admin
            || self.role == Some(crate::catalog::MembershipRole::Admin)
            || self.user_id.is_none()
    }

    /// Whether this principal is an API key explicitly scoped with
    /// [`TENANT_MANAGE_SCOPE`], allowing it to call the tenant management API
    /// for its own tenant.
    ///
    /// This is deliberately NOT `has_scope_or_unrestricted`: a legacy key
    /// without explicit scopes is unrestricted for ingest, read, and schema
    /// access, but does **not** gain tenant management. Those keys were
    /// minted before management existed for keys; widening them silently
    /// would be a security surprise. Human sessions never qualify here — they
    /// are authorized through membership roles ([`can_manage_tenant`]).
    ///
    /// [`can_manage_tenant`]: Self::can_manage_tenant
    pub fn can_manage_via_key(&self) -> bool {
        self.user_id.is_none()
            && self
                .api_key_scopes
                .as_ref()
                .is_some_and(|scopes| scopes.iter().any(|scope| scope == TENANT_MANAGE_SCOPE))
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

    /// Whether this principal may read the schema registry.
    ///
    /// Any membership role may read; a legacy key with no explicit scopes is
    /// unrestricted; explicit scopes must contain [`SCHEMA_READ_SCOPE`].
    pub fn can_read_schema(&self) -> bool {
        self.has_scope_or_unrestricted(SCHEMA_READ_SCOPE)
    }

    /// Whether this principal may create, replace, validate, or delete custom
    /// schema registries.
    ///
    /// Sessions need tenant Admin or instance-admin; keys follow the same
    /// shape as [`can_ingest`](Self::can_ingest) with [`SCHEMA_WRITE_SCOPE`].
    pub fn can_write_schema(&self) -> bool {
        self.can_manage_tenant() && self.has_scope_or_unrestricted(SCHEMA_WRITE_SCOPE)
    }

    fn has_scope_or_unrestricted(&self, required: &str) -> bool {
        self.api_key_scopes
            .as_ref()
            .is_none_or(|scopes| scopes.iter().any(|scope| scope == required))
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
    fn metrics_write_scope_allows_only_metrics_ingestion() {
        let allowed = context(Some(vec!["metrics:write".into()]));
        assert!(allowed.can_ingest("metrics"));
        assert!(!allowed.can_ingest("logs"));
        assert!(!allowed.can_ingest("traces"));
        assert!(!allowed.can_ingest("profiles"));
    }

    #[test]
    fn logs_write_scope_allows_only_logs_ingestion() {
        let allowed = context(Some(vec!["logs:write".into()]));
        assert!(!allowed.can_ingest("metrics"));
        assert!(allowed.can_ingest("logs"));
        assert!(!allowed.can_ingest("traces"));
        assert!(!allowed.can_ingest("profiles"));
    }

    #[test]
    fn traces_write_scope_allows_only_traces_ingestion() {
        let allowed = context(Some(vec!["traces:write".into()]));
        assert!(!allowed.can_ingest("metrics"));
        assert!(!allowed.can_ingest("logs"));
        assert!(allowed.can_ingest("traces"));
        assert!(!allowed.can_ingest("profiles"));
    }

    #[test]
    fn profiles_write_scope_allows_only_profiles_ingestion() {
        let allowed = context(Some(vec!["profiles:write".into()]));
        assert!(!allowed.can_ingest("metrics"));
        assert!(!allowed.can_ingest("logs"));
        assert!(!allowed.can_ingest("traces"));
        assert!(allowed.can_ingest("profiles"));
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
            Some("session-1".into()),
        );
        assert!(!viewer.can_write());
        assert!(!viewer.can_ingest("metrics"));
        assert!(!viewer.can_manage_tenant());
    }

    #[test]
    fn metrics_read_scope_allows_only_metrics_reads() {
        let allowed = context(Some(vec!["metrics:read".into()]));
        assert!(allowed.can_read("metrics"));
        assert!(!allowed.can_read("logs"));
        assert!(!allowed.can_read("traces"));
        assert!(!allowed.can_read("profiles"));
    }

    #[test]
    fn logs_read_scope_allows_only_logs_reads() {
        let allowed = context(Some(vec!["logs:read".into()]));
        assert!(!allowed.can_read("metrics"));
        assert!(allowed.can_read("logs"));
        assert!(!allowed.can_read("traces"));
        assert!(!allowed.can_read("profiles"));
    }

    #[test]
    fn traces_read_scope_allows_only_traces_reads() {
        let allowed = context(Some(vec!["traces:read".into()]));
        assert!(!allowed.can_read("metrics"));
        assert!(!allowed.can_read("logs"));
        assert!(allowed.can_read("traces"));
        assert!(!allowed.can_read("profiles"));
    }

    #[test]
    fn profiles_read_scope_allows_only_profiles_reads() {
        let allowed = context(Some(vec!["profiles:read".into()]));
        assert!(!allowed.can_read("metrics"));
        assert!(!allowed.can_read("logs"));
        assert!(!allowed.can_read("traces"));
        assert!(allowed.can_read("profiles"));
    }

    #[test]
    fn legacy_unscoped_keys_may_read_any_signal() {
        let legacy = context(None);
        for signal in ["metrics", "logs", "traces", "profiles"] {
            assert!(legacy.can_read(signal));
        }
    }

    #[test]
    fn viewer_sessions_may_still_read() {
        let viewer = context(None).with_user(
            "user-1".into(),
            crate::catalog::MembershipRole::Viewer,
            false,
            Some("session-1".into()),
        );
        for signal in ["metrics", "logs", "traces", "profiles"] {
            assert!(viewer.can_read(signal));
        }
    }

    #[test]
    fn write_scopes_do_not_grant_read() {
        let writer_only = context(Some(vec!["traces:write".into()]));
        assert!(!writer_only.can_read("traces"));
    }

    #[test]
    fn schema_read_scope_allows_only_schema_reads() {
        let reader = context(Some(vec![SCHEMA_READ_SCOPE.into()]));
        assert!(reader.can_read_schema());
        assert!(!reader.can_write_schema());
        assert!(!reader.can_read("traces"));
    }

    #[test]
    fn schema_write_scope_allows_writes_but_not_reads() {
        let writer = context(Some(vec![SCHEMA_WRITE_SCOPE.into()]));
        assert!(writer.can_write_schema());
        assert!(!writer.can_read_schema());
    }

    #[test]
    fn ingest_only_key_cannot_touch_schema() {
        let ingest = context(Some(vec!["traces:write".into()]));
        assert!(!ingest.can_read_schema());
        assert!(!ingest.can_write_schema());
    }

    #[test]
    fn legacy_unscoped_keys_have_full_schema_access() {
        let legacy = context(None);
        assert!(legacy.can_read_schema());
        assert!(legacy.can_write_schema());
    }

    #[test]
    fn sessions_read_schema_with_any_role_and_write_only_as_admin() {
        use crate::catalog::MembershipRole;
        for (role, may_write) in [
            (MembershipRole::Viewer, false),
            (MembershipRole::Member, false),
            (MembershipRole::Admin, true),
        ] {
            let session =
                context(None).with_user("user-1".into(), role, false, Some("session-1".into()));
            assert!(session.can_read_schema(), "{role:?} must read schema");
            assert_eq!(session.can_write_schema(), may_write, "{role:?} write");
        }
        let instance_admin = context(None).with_user(
            "root".into(),
            MembershipRole::Viewer,
            true,
            Some("session-2".into()),
        );
        assert!(instance_admin.can_read_schema());
        assert!(instance_admin.can_write_schema());
    }

    #[test]
    fn schema_read_is_a_read_scope_but_schema_write_is_not() {
        assert!(READ_SCOPES.contains(&SCHEMA_READ_SCOPE));
        assert!(!READ_SCOPES.contains(&SCHEMA_WRITE_SCOPE));
        assert_eq!(SCHEMA_SCOPES, [SCHEMA_READ_SCOPE, SCHEMA_WRITE_SCOPE]);
    }

    #[test]
    fn tenant_manage_is_a_key_scope_but_never_oauth_grantable() {
        assert!(API_KEY_SCOPES.contains(&TENANT_MANAGE_SCOPE));
        assert!(!READ_SCOPES.contains(&TENANT_MANAGE_SCOPE));
        assert!(!INGEST_SCOPES.contains(&TENANT_MANAGE_SCOPE));
        assert!(!SCHEMA_SCOPES.contains(&TENANT_MANAGE_SCOPE));
        assert_eq!(validate_scopes(&[TENANT_MANAGE_SCOPE.to_string()]), Ok(()));
    }

    #[test]
    fn can_manage_via_key_requires_the_explicit_scope() {
        use crate::catalog::MembershipRole;
        let scoped = context(Some(vec![
            "traces:write".into(),
            TENANT_MANAGE_SCOPE.into(),
        ]));
        assert!(scoped.can_manage_via_key());

        let ingest_only = context(Some(vec!["traces:write".into()]));
        assert!(!ingest_only.can_manage_via_key());

        // The one deliberate exception to "unscoped legacy keys are
        // unrestricted": management is opt-in.
        let legacy = context(None);
        assert!(!legacy.can_manage_via_key());

        let admin_session = context(None).with_user(
            "user-1".into(),
            MembershipRole::Admin,
            true,
            Some("session-1".into()),
        );
        assert!(admin_session.can_manage_tenant());
        assert!(!admin_session.can_manage_via_key());
    }

    #[test]
    fn api_key_scopes_is_the_union_of_ingest_read_and_schema() {
        for scope in INGEST_SCOPES
            .iter()
            .chain(READ_SCOPES.iter())
            .chain(SCHEMA_SCOPES.iter())
            .chain(std::iter::once(&TENANT_MANAGE_SCOPE))
        {
            assert!(API_KEY_SCOPES.contains(scope), "{scope} missing");
        }
        for scope in API_KEY_SCOPES {
            assert!(
                INGEST_SCOPES.contains(&scope)
                    || READ_SCOPES.contains(&scope)
                    || SCHEMA_SCOPES.contains(&scope)
                    || scope == TENANT_MANAGE_SCOPE,
                "{scope} is not in any family"
            );
        }
        let unique: std::collections::BTreeSet<&str> = API_KEY_SCOPES.into_iter().collect();
        assert_eq!(unique.len(), API_KEY_SCOPES.len(), "duplicate scope");
    }

    #[test]
    fn validate_scopes_accepts_every_known_scope() {
        let all: Vec<String> = API_KEY_SCOPES.iter().map(|s| s.to_string()).collect();
        assert_eq!(validate_scopes(&all), Ok(()));
        assert_eq!(validate_scopes(&["schema:read".to_string()]), Ok(()));
    }

    #[test]
    fn validate_scopes_rejects_empty_and_names_unknown_scope() {
        assert_eq!(validate_scopes(&[]), Err(ValidationError::NoScopes));
        assert_eq!(
            validate_scopes(&["traces:write".to_string(), "schema:admin".to_string()]),
            Err(ValidationError::UnknownScope {
                scope: "schema:admin".to_string()
            })
        );
        let message = ValidationError::UnknownScope {
            scope: "schema:admin".to_string(),
        }
        .to_string();
        assert!(message.contains("schema:admin"), "{message}");
        assert!(
            ValidationError::NoScopes
                .to_string()
                .contains("at least one scope"),
        );
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
