//! Shared tenant-resource authorization, called explicitly at the top of
//! every handler that needs it — never via a path-keyed layer, so privilege
//! never depends on where a route happens to be mounted (issue #1561
//! follow-up: every scope-prefixed path, `/manage` and `/manage/admin`
//! alike, is gone; every resource lives at one regular path such as
//! `/api/v1/tenants/{id}/api-keys`).
//!
//! Two credentials reach these handlers with no resolved [`TenantContext`]
//! at all (`ctx: None`): a request carrying the break-glass
//! `[auth].admin_api_key` bearer. `create_router`'s `auth_layer` (see
//! `lib.rs`) lets such a request past the normal per-tenant `X-Tenant-ID`
//! requirement — matched by the specific routes it applies to, not a path
//! prefix, and regardless of whether `X-Tenant-ID` is *also* present (MCP
//! forwards both) — so it reaches here with `ctx: None` unless the caller
//! also supplied a valid tenant credential. The functions below are what
//! actually decides whether the bearer is in fact that configured key.

use crate::RouterAppState;
use crate::endpoints::management::{authorize_tenant, error};
use axum::{
    http::{HeaderMap, StatusCode, header::AUTHORIZATION},
    response::Response,
};
use common::auth::{Authenticator, TenantContext};

/// Whether `headers` carries a bearer token matching the configured
/// break-glass `admin_api_key`. `false` when no key is configured.
fn admin_key_matches(state: &RouterAppState, headers: &HeaderMap) -> bool {
    let bearer = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    match (state.config().auth.admin_api_key.as_deref(), bearer) {
        (Some(expected), Some(token)) => {
            Authenticator::hash_api_key(token) == Authenticator::hash_api_key(expected)
        }
        _ => false,
    }
}

/// Authorizes an instance-wide operation (tenant create/update/delete,
/// listing/creating users): an instance-admin tenant credential, or the
/// break-glass admin key with no tenant at all.
///
/// Returns the status and message as data, not a rendered [`Response`], so
/// callers that declare `signaldb_api::ApiError` (`error` + `message`) as
/// their OpenAPI response body — the tenant/user handlers in `tenants.rs` —
/// can render it in that shape instead of the bare `{"error"}` body
/// [`management::error`] produces, which would otherwise mismatch the
/// documented schema on a 401/403.
pub(crate) fn require_instance_admin_or_admin_key(
    state: &RouterAppState,
    headers: &HeaderMap,
    ctx: Option<&TenantContext>,
) -> Result<(), (StatusCode, &'static str)> {
    if let Some(ctx) = ctx {
        return if ctx.is_instance_admin {
            Ok(())
        } else {
            Err((StatusCode::FORBIDDEN, "Instance administrator required"))
        };
    }
    if admin_key_matches(state, headers) {
        return Ok(());
    }
    Err((
        StatusCode::UNAUTHORIZED,
        "Missing administrator credentials",
    ))
}

/// Authorizes an operation scoped to one tenant (datasets, API keys,
/// memberships, GitHub installations, the tenant resource itself): a tenant
/// credential authorized for `tenant_id` (instance admin, or
/// `tenant:manage`/admin role for that specific tenant — see
/// [`authorize_tenant`]), or the break-glass admin key with no tenant, which
/// may act on any tenant that exists. On the admin-key path the tenant is
/// looked up and a `404` returned if it doesn't exist, rather than letting
/// the caller silently operate on nothing.
pub(crate) async fn authorize_tenant_or_admin_key(
    state: &RouterAppState,
    headers: &HeaderMap,
    ctx: Option<&TenantContext>,
    tenant_id: &str,
) -> Result<(), Box<Response>> {
    match ctx {
        Some(ctx) => authorize_tenant(ctx, tenant_id)
            .map_err(|(status, message)| Box::new(error(status, message))),
        None => {
            if !admin_key_matches(state, headers) {
                return Err(Box::new(error(
                    StatusCode::UNAUTHORIZED,
                    "Missing administrator credentials",
                )));
            }
            match state.catalog().get_tenant(tenant_id).await {
                Ok(Some(_)) => Ok(()),
                Ok(None) => Err(Box::new(error(
                    StatusCode::NOT_FOUND,
                    format!("Tenant '{tenant_id}' not found"),
                ))),
                Err(catalog_error) => {
                    tracing::error!(error = %catalog_error, tenant_id, "tenant existence check failed");
                    Err(Box::new(error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "Unable to verify tenant",
                    )))
                }
            }
        }
    }
}
