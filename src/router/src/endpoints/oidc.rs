//! # OIDC SSO login endpoints (change: oidc-login)
//!
//! `GET /ui/session/oidc/start` and `GET /ui/session/oidc/callback` — the
//! two-endpoint authorization-code + PKCE flow beside the password session
//! endpoints (design decision 2). Both are unauthenticated and 404 when
//! `[auth.oidc]` is absent.
//!
//! The callback reuses the exact session-issuance path
//! `POST /ui/session` uses (`common::auth::{generate_session_token,
//! hash_session_token}` + `Catalog::create_user_session`, `signaldb_session`
//! cookie with the same attributes/lifetime) — no separate session
//! mechanism.
//!
//! Group-claim → membership mapping (`sync_oidc_memberships`, tasks 3.2/3.3)
//! runs in the callback right after identity resolution succeeds and before
//! session issuance: computed once per login from the token's groups ×
//! `[auth.oidc].group_mappings`, and skipped entirely (no catalog write at
//! all) when no `group_claim`/`group_mappings` are configured.

use axum::{
    Router,
    body::Body,
    extract::{Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use chrono::{Duration as ChronoDuration, Utc};
use common::auth::{generate_session_token, hash_session_token, session_cookie_header};
use common::catalog::{MembershipRole, UserRecord};
use common::config::OidcConfig;
use serde::Deserialize;
use serde_json::json;

use crate::RouterState;
use crate::oidc::{
    self, PENDING_COOKIE_NAME, PENDING_COOKIE_PATH, PENDING_COOKIE_TTL_SECS, VerifiedIdentity,
};

/// Routes mounted at the router root, beside `/ui/session`.
pub fn router<S: RouterState>() -> Router<S> {
    Router::new()
        .route("/ui/session/oidc/start", get(start::<S>))
        .route("/ui/session/oidc/callback", get(callback::<S>))
}

/// GET /ui/session/oidc/start
///
/// 302s to the IdP's authorization endpoint with a fresh PKCE challenge,
/// `state`, and `nonce`, and sets the signed pending-login cookie carrying
/// what the callback needs to complete the exchange. 404 when OIDC isn't
/// configured; 503 naming the issuer while discovery hasn't (yet) succeeded.
pub async fn start<S: RouterState>(State(state): State<S>, headers: HeaderMap) -> Response {
    let Some(runtime) = state.oidc() else {
        return error_response(StatusCode::NOT_FOUND, "OIDC is not configured");
    };
    let Some(provider) = runtime.provider().await else {
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &format!(
                "OIDC provider '{}' is currently unavailable",
                runtime.issuer_url
            ),
        );
    };

    let redirect_uri = match runtime.config.redirect_url.clone() {
        Some(url) => url,
        None => match derive_callback_url(&headers) {
            Some(url) => url,
            None => {
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Unable to determine the OIDC callback URL from the request",
                );
            }
        },
    };

    let authorization = match provider.begin_authorization(&redirect_uri) {
        Ok(authorization) => authorization,
        Err(error) => {
            tracing::error!(
                issuer = %runtime.issuer_url,
                error = %error,
                "Failed to build the OIDC authorization request"
            );
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Unable to start SSO login",
            );
        }
    };

    let cookie_value = match oidc::sign_pending_login(
        &runtime.config.client_secret,
        authorization.state.secret(),
        authorization.nonce.secret(),
        authorization.pkce_verifier.secret(),
        &redirect_uri,
    ) {
        Ok(value) => value,
        Err(error) => {
            tracing::error!(error = %error, "Failed to sign the OIDC pending-login cookie");
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Unable to start SSO login",
            );
        }
    };
    let pending_cookie = format!(
        "{PENDING_COOKIE_NAME}={cookie_value}; HttpOnly; Secure; SameSite=Lax; \
         Path={PENDING_COOKIE_PATH}; Max-Age={PENDING_COOKIE_TTL_SECS}"
    );

    match axum::http::Response::builder()
        .status(StatusCode::FOUND)
        .header(header::LOCATION, authorization.authorization_url.as_str())
        .header(header::SET_COOKIE, pending_cookie)
        .body(Body::empty())
    {
        Ok(response) => response,
        Err(error) => {
            tracing::error!(error = %error, "Failed to build the OIDC start response");
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Unable to start SSO login",
            )
        }
    }
}

/// Query parameters the IdP's redirect carries back to the callback.
#[derive(Debug, Deserialize)]
pub struct CallbackParams {
    #[serde(default)]
    code: Option<String>,
    #[serde(default)]
    state: Option<String>,
    /// Present when the user denied consent or the IdP otherwise failed the
    /// request before ever issuing a code (RFC 6749 §4.1.2.1).
    #[serde(default)]
    error: Option<String>,
}

/// GET /ui/session/oidc/callback
///
/// Reads state/nonce/PKCE-verifier from the pending-login cookie, exchanges
/// the code, validates the ID token, resolves the identity, and issues the
/// standard session on success. Every failure — missing/invalid pending
/// cookie, state mismatch, a bad nonce/signature/expiry, an unverified email
/// on the link path, an allowlist refusal, or a disabled user — collapses
/// into the same generic redirect with no session created and no disclosure
/// of which check failed.
pub async fn callback<S: RouterState>(
    State(state): State<S>,
    Query(params): Query<CallbackParams>,
    headers: HeaderMap,
) -> Response {
    let Some(runtime) = state.oidc() else {
        return error_response(StatusCode::NOT_FOUND, "OIDC is not configured");
    };

    let clear_pending_cookie = format!(
        "{PENDING_COOKIE_NAME}=; HttpOnly; Secure; SameSite=Lax; Path={PENDING_COOKIE_PATH}; \
         Max-Age=0"
    );

    let Some(provider) = runtime.provider().await else {
        return reject(&clear_pending_cookie, "provider_unavailable");
    };
    let Some(http_client) = runtime.http_client() else {
        return reject(&clear_pending_cookie, "http_client_unavailable");
    };
    let Some(pending_cookie_value) = pending_cookie_from_headers(&headers) else {
        return reject(&clear_pending_cookie, "missing_pending_cookie");
    };
    let Some(pending) =
        oidc::verify_pending_login(&runtime.config.client_secret, &pending_cookie_value)
    else {
        return reject(&clear_pending_cookie, "invalid_pending_cookie");
    };
    if params.error.is_some() {
        return reject(&clear_pending_cookie, "provider_denied");
    }
    let Some(returned_state) = params.state.as_deref() else {
        return reject(&clear_pending_cookie, "missing_state");
    };
    if returned_state != pending.state {
        return reject(&clear_pending_cookie, "state_mismatch");
    }
    let Some(code) = params.code else {
        return reject(&clear_pending_cookie, "missing_code");
    };

    let identity = match provider
        .exchange_and_verify(
            code,
            &pending.redirect_uri,
            pending.pkce_verifier,
            &pending.nonce,
            http_client,
            runtime.config.group_claim.as_deref(),
        )
        .await
    {
        Ok(identity) => identity,
        Err(error) => {
            tracing::warn!(
                issuer = %runtime.issuer_url,
                error = %error,
                "OIDC token exchange or ID-token verification failed"
            );
            return reject(&clear_pending_cookie, "exchange_failed");
        }
    };

    let user = match resolve_identity(&state, &runtime.issuer_url, &identity, &runtime.config).await
    {
        Ok(Some(user)) => user,
        Ok(None) => return reject(&clear_pending_cookie, "identity_refused"),
        Err(error) => {
            tracing::error!(error = %error, "OIDC identity resolution failed");
            return reject(&clear_pending_cookie, "identity_resolution_error");
        }
    };
    if user.disabled_at.is_some() {
        return reject(&clear_pending_cookie, "disabled_user");
    }

    // Group-claim -> membership mapping (tasks 3.2/3.3, design decision 6):
    // only touches `granted_by = 'oidc_mapping'` rows, and only when mapping
    // is actually configured — "no mapping, no membership changes" (spec).
    // Never grants or revokes `is_instance_admin`.
    if runtime.config.group_claim.is_some() && !runtime.config.group_mappings.is_empty() {
        let mut desired: Vec<(String, MembershipRole)> = runtime
            .config
            .group_mappings
            .iter()
            .filter(|mapping| identity.groups.iter().any(|g| g == &mapping.group))
            .map(|mapping| (mapping.tenant.clone(), mapping.role))
            .collect();

        // A mapping rule can name a tenant that doesn't exist yet (not
        // provisioned, or a typo): syncing it would trip a foreign-key
        // violation and reject the whole login. One bad rule must not block
        // an otherwise-valid identity, so unknown tenants are dropped here
        // (with a warning naming each one) before the sync runs; a real
        // sync failure below (e.g. the database being down) still rejects
        // the login.
        if !desired.is_empty() {
            match state.catalog().list_tenants().await {
                Ok(tenants) => {
                    let known: std::collections::HashSet<&str> =
                        tenants.iter().map(|t| t.id.as_str()).collect();
                    desired.retain(|(tenant_id, _)| {
                        let exists = known.contains(tenant_id.as_str());
                        if !exists {
                            tracing::warn!(
                                user_id = %user.id,
                                tenant_id = %tenant_id,
                                "OIDC group mapping names a tenant that does not exist; skipping"
                            );
                        }
                        exists
                    });
                }
                Err(error) => {
                    tracing::error!(user_id = %user.id, error = %error, "OIDC group-mapping tenant lookup failed");
                    return reject(&clear_pending_cookie, "membership_sync_error");
                }
            }
        }

        if let Err(error) = state
            .catalog()
            .sync_oidc_memberships(&user.id, &desired)
            .await
        {
            tracing::error!(user_id = %user.id, error = %error, "OIDC group-mapping membership sync failed");
            return reject(&clear_pending_cookie, "membership_sync_error");
        }
    }

    let token = generate_session_token();
    let token_hash = hash_session_token(&token);
    let expires_at = Utc::now() + ChronoDuration::hours(12);
    if let Err(error) = state
        .catalog()
        .create_user_session(&user.id, &token_hash, expires_at)
        .await
    {
        tracing::error!(user_id = %user.id, error = %error, "OIDC session persistence failed");
        return reject(&clear_pending_cookie, "session_persistence_error");
    }

    tracing::info!(user_id = %user.id, issuer = %runtime.issuer_url, "OIDC SSO login succeeded");

    let session_cookie = session_cookie_header(&token);
    match axum::http::Response::builder()
        .status(StatusCode::FOUND)
        .header(header::LOCATION, "/")
        .header(header::SET_COOKIE, clear_pending_cookie)
        .header(header::SET_COOKIE, session_cookie)
        .body(Body::empty())
    {
        Ok(response) => response,
        Err(error) => {
            tracing::error!(error = %error, "Failed to build the OIDC callback response");
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Unable to complete SSO login",
            )
        }
    }
}

/// Every callback rejection lands here: clear the pending cookie and bounce
/// to the login page with a generic, non-disclosing failure marker.
/// `reason` is logged by the caller (never sent to the client).
fn reject(clear_pending_cookie: &str, reason: &str) -> Response {
    tracing::warn!(reason, "OIDC callback rejected");
    match axum::http::Response::builder()
        .status(StatusCode::FOUND)
        .header(header::LOCATION, "/?sso_error=1")
        .header(header::SET_COOKIE, clear_pending_cookie)
        .body(Body::empty())
    {
        Ok(response) => response,
        Err(error) => {
            tracing::error!(error = %error, "Failed to build the OIDC rejection response");
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Unable to complete SSO login",
            )
        }
    }
}

/// Identity resolution (design decision 3): `(issuer, subject)` exact match,
/// then verified-email link, then JIT creation (allowlist permitting).
/// `Ok(None)` means "refuse without creating anything" (allowlist refusal or
/// no email to provision from) — the caller maps that to the same generic
/// rejection as every other failure.
async fn resolve_identity<S: RouterState>(
    state: &S,
    issuer_url: &str,
    identity: &VerifiedIdentity,
    config: &OidcConfig,
) -> anyhow::Result<Option<UserRecord>> {
    let catalog = state.catalog();

    if let Some(user) = catalog
        .find_user_by_oidc_identity(issuer_url, &identity.subject)
        .await?
    {
        return Ok(Some(user));
    }

    // `allowed_email_domains` intentionally gates only the JIT-creation
    // branch below, not this link branch: an already-existing user whose
    // email falls outside the allowlist can still link and log in via SSO
    // once matched by verified email (see
    // `callback_links_existing_user_outside_allowlist_via_verified_email`).
    // TODO(group 5, task 5.2): call this nuance out explicitly in the IdP
    // setup guide alongside the allowlist docs.
    if identity.email_verified
        && let Some(email) = &identity.email
        && let Some(user) = catalog.get_user_by_email(email).await?
    {
        // Don't mutate a disabled account on a refused login: skip the link
        // and let the caller's existing `disabled_at` check fire the
        // standard refusal with no write performed.
        if user.disabled_at.is_none() {
            catalog
                .link_oidc_identity(&user.id, issuer_url, &identity.subject)
                .await?;
        }
        return Ok(Some(user));
    }

    // No match: JIT-provision, allowlist permitting. Provisioning needs an
    // email (the catalog's `users.email` is a mandatory, unique column);
    // an identity asserting none can't be provisioned.
    let Some(email) = &identity.email else {
        return Ok(None);
    };
    if let Some(allowed_domains) = &config.allowed_email_domains {
        let domain = email.rsplit_once('@').map(|(_, domain)| domain);
        let allowed = domain.is_some_and(|domain| {
            allowed_domains
                .iter()
                .any(|d| d.eq_ignore_ascii_case(domain))
        });
        if !allowed {
            return Ok(None);
        }
    }

    let user = catalog
        .create_oidc_user(
            email,
            identity.name.as_deref(),
            issuer_url,
            &identity.subject,
        )
        .await?;
    Ok(Some(user))
}

/// Derive the callback URL from the request's origin (Risks section): reads
/// `X-Forwarded-{Host,Proto}` first (the reverse-proxy case the setup docs
/// lead with), falling back to the `Host` header and an `https` scheme.
fn derive_callback_url(headers: &HeaderMap) -> Option<String> {
    let host = headers
        .get("x-forwarded-host")
        .or_else(|| headers.get(header::HOST))
        .and_then(|v| v.to_str().ok())?;
    let scheme = headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("https");
    Some(format!("{scheme}://{host}/ui/session/oidc/callback"))
}

/// Extract the raw (still-signed) pending-login cookie value, mirroring
/// [`common::auth::session::session_token_from_headers`] for a different
/// cookie name.
fn pending_cookie_from_headers(headers: &HeaderMap) -> Option<String> {
    for header in headers.get_all(header::COOKIE) {
        let Ok(cookies) = header.to_str() else {
            continue;
        };
        for pair in cookies.split(';') {
            let Some((name, value)) = pair.split_once('=') else {
                continue;
            };
            if name.trim() == PENDING_COOKIE_NAME {
                return Some(value.trim().to_string());
            }
        }
    }
    None
}

fn error_response(status: StatusCode, message: &str) -> Response {
    (status, axum::Json(json!({ "error": message }))).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RouterAppState, create_router};
    use axum::body::Body;
    use axum::http::{Request, header};
    use common::catalog::{Catalog, GrantSource};
    use common::config::{ApiKeyConfig, AuthConfig, Configuration, DatasetConfig, TenantConfig};
    use openidconnect::core::{
        CoreIdToken, CoreIdTokenClaims, CoreJwsSigningAlgorithm, CoreRsaPrivateSigningKey,
    };
    use openidconnect::{
        Audience, EmptyAdditionalClaims, EndUserEmail, EndUserName, IssuerUrl, JsonWebKeyId,
        LocalizedClaim, Nonce as OidcNonce, PrivateSigningKey, StandardClaims, SubjectIdentifier,
    };
    use std::time::Duration;
    use tower::ServiceExt;
    use url::Url;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Two distinct 2048-bit RSA test keys (never used outside this test
    /// module): `KEY_1`/`kid1` backs the "steady state" JWKS; `KEY_2`/`kid2`
    /// is introduced mid-test to exercise JWKS rotation (task 2.7).
    const KEY_1_PEM: &str = "-----BEGIN RSA PRIVATE KEY-----\n\
MIIEowIBAAKCAQEA2hr1r+agnYPWbuB22SEW2dxySMgCU478EClnq1zCXAmUO07K\n\
J2rrKUnTrJfVAYL2UC8LNPXWxD2nUwoV1B0IHbv9z00FwopADzWRhJ0miKa2iIhn\n\
p4Wr0oLfqtUEBNfzABROP6BqVhx4897Tk0/0YCKOcm/YhmTDAdjPs5n7LEkVUsHH\n\
248whn/nSjPuzVua3tsFVePUj41xIQ3Jp91choxQKZQ+W6G9tiW05KvawsWsASIp\n\
F2Xf1eYjvzVAXFXZBViYaZuQE7POcFgETCkW56mKs+jesP4Jk9uIy9kMG7W08BsR\n\
s2o+yt/N19zUGnHOIHddEIDILtC0xFPhKnSr7wIDAQABAoIBACT4bu8LM+yfXWjc\n\
CalSj5IMaR7nMGXDlfyTWCxXA+cgBI1tFJ1L5WLRTd0yu3uPHHuJDehDR81p+gP8\n\
cKjM5wRLSoGqN7C7SJKLbQhCrzZ4s/Y+0Ps3kGGVp55Ij27WwC0iRAVLBttj5ijI\n\
e6q74dut3+GMDfLx/5A/lS2Hi2j9Biyz/oAS254TE5RanR+9NdC8nuSYgzC2m4Xn\n\
PRvvXzLh9zt/TGSp4ewDfdu3vNeeLkWZu5+gFeggEytuCzYW7qb7MrDiRAaajx1L\n\
5dOMmZEC0HrgvO6WbhvYR+zbP2GTURamXkg5yTHcd6VJ99IcUualKItV4CJhc99q\n\
qneQ/1kCgYEA+uMJ1MKshhZR16m7F+zLS2PRERsz8ZpX0lXD5TH8aF2PiO/sn4kx\n\
caGKyvIPzKatTpLsDF6WxZnHsBeaLnsoSQLpd2wxLd0WYU370HX3m2qwjz4a9WvV\n\
T7EwubnzytFrDL5e+C9StBfsNdFz+RtkCReGrRQRhHr/fLpPxAHIbysCgYEA3ozj\n\
i3z1eiOEsV2L9Ib1n7MG1Bmzgmt0bw6y7ayZGSnb+8F3Umhhz9PdK12o+FkAPXMw\n\
WI9V+A4fb5ZXMHq+DoZvjRy73uLtZZQ432RMU4+tBEUs+UFk/oFXgjydppMG0nVc\n\
B3tJlaFUk3Hf2qYZYj6QJnIiuSdZJt/DaHCDtE0CgYAQoSvtplm5KZGTMfTsyQ1Q\n\
mfUppRv0T76yemzZrE6GvGzfLsgIaxeT6JlCinjxn7qtEqAC0eI8hsztpyLZIeCx\n\
tjezasB3wcfR+1FNqAQZkSDS3dL7oYIqHhyUsvat4uOtnJC+8qQQu+U0TAXb5Szk\n\
TWLn5gSjO2Pj6JWQ8G1QsQKBgCmkhUNpi2liUgb6MSD9S+KpFiMD+CH0R2IshvCQ\n\
NkmOGpqeFdy2qW7A/waJTP/Db5cQAcDgeT6kLd+sav4oSX3gS+lEsia/oZo7RPUQ\n\
CHQuUzqUUxkE0ogI3b4B56Huqv0gdqrzt84m1POOAEwifmdyu3hmLPcmVLlAb4kQ\n\
XKSVAoGBAIOp3FPsDdLBq0iPEUXLcGZpm5lXaiphfUVt6eihCP4orEF3lYWg10MM\n\
vK9NUakzbzYgvOftGADyw8Ravm8xNpXGImZ8D+377BRqDmpNFPMAqVswcSHSv8+q\n\
kX3qS//vqFcQ7Dv32ABDvQBLI7SMPssfgF+3ueRCP+//2B3WINnl\n\
-----END RSA PRIVATE KEY-----\n";

    const KEY_2_PEM: &str = "-----BEGIN RSA PRIVATE KEY-----\n\
MIIEpAIBAAKCAQEAv0TpwSYOifrNxU5482SlhEQAuwczPXFlRGlXrszaLT+fsd2g\n\
qrwYbGkR24u/JprpuhPWPHhNYA8fF3TesThipthZQf1/y97MgIifNnXQccIEe4Da\n\
Tmy+g2uhxpnZtTvdaPTOVdZk81SoQ+sfywdCmgpHnr/ii9SLe/pqk5OwVQ0Ms3uk\n\
RiXTbiXEBhfpnrBa9QWvg2uv/C8T9IrY0Hn8B4qRvdddRRTOyBQki6Y3/mii5MTC\n\
nZ6Zzbb9U0ukBbauyxKPoQm21PUPsMIhO8YfzEiLcN67kGJs2PCMQK76xcJWDjUG\n\
PgyfPiMvnwcUZ78h+t14y7I+oGvBM/69E2qEJwIDAQABAoIBADK/KfXhDwJ01JuA\n\
0n5hObj6AeedZW7r4x5fhLAQEeLneotKbhJejWxWBDLnxPONLm38TV8F3CGcYXdr\n\
vpnuh94UkLn9dCa3GjMQDI36fC9ydpX9/e3I1FMx/14d+7EmDPyH1ybXFzmoQGyk\n\
uW64omcUSyRHbOgEoG1oZ1Z4XNRzh2y5f/fWd1H4DB7rGggoLDGOmHW0pyJ49iRL\n\
eATzE8lwaz0RSEmadXtVIzd31yvuEjW2LjUSlxjxJgBLp5m7kVmWzZ9ErRlZ/ErW\n\
zVfcAgJ2W9oqMvSFkdp1PmLmwhNO9YWJRf5oKPvTdh52/b3Kt+OV4BGeIfdOmMmN\n\
fKrMAsECgYEA/QyUm71OoSYYuVEyujhtbjPIGdfR4+STla7sNp+AvOxqlnqZGz+9\n\
6S8AlxlirqG7dp3gVafB3qucWNWRl6PqE9SCdc9CnSfRixG2M+9659HR6d73OT7v\n\
I/FizmTZD10ES9On5lD6dEAgnkP5/P3UbeSQGgnIZb+57bGeqGMcQfkCgYEAwX/n\n\
JBNvRA9oLLO6l8P71ocD13KE0dFKNuCY+bKMK0+YDPePcuq9NE8hHm1y2KIIL+5u\n\
leaZAO0ZL5IjOsSSbzCgmhzjmSVZYRkGCP8gYJOxXtaotu1EyN/pPwFGCLTfmICm\n\
RzYQeBS8SC84QXNVJ2vJSES8vh6y4oY6iNFkfx8CgYEAvwvrerVvEt1XVzpFiTCm\n\
9cekcFZvwZXGEwFQl2DO6lO3mUqQI3F5aP6yfGGPDndOuBwzzZdtGDcKQEtls03u\n\
oPAVWuqSyWHRoyRJImbQrQrNZ7uNfSvVkpZg2aBr/FxmrPTsU0eZ/4CW38ZEi8v7\n\
wMgKP45ZPPCGaD1soobJQ8kCgYAtNwf55Bix3K/JEk5dvEwSuwXoyDfFF5Sx8hWM\n\
823RiQ3MqeR5Q+TmXj8s7wJRILutB5gLqxDBqKWj+hAFYX2eQcyldm6wkIusJr7E\n\
R2l3Z8ugj6Ro6lSSY9ALCu8kha9h4V35ceazSJUegPoyQRL63TLxki5QdrfyOs9e\n\
vR8wnQKBgQCZ0hvC/TVnGpZ+lFmmon3ULmOSKQLQ+p7hDvKKfQkixJkunyFrAkTE\n\
efOUKSArza4TYRd9nJATpxivso3JUGq8v1BAamO5mHcldvuPqsRh9ofnPbLtKgmH\n\
bjkNcKEJskeng2DMpy0SXaOVUOc6sU5cxc7F6vtWUDblAWQmMg0Y/g==\n\
-----END RSA PRIVATE KEY-----\n";

    fn signing_key(pem: &str, kid: &str) -> CoreRsaPrivateSigningKey {
        CoreRsaPrivateSigningKey::from_pem(pem, Some(JsonWebKeyId::new(kid.to_string())))
            .expect("test RSA key parses")
    }

    fn jwk_json(pem: &str, kid: &str) -> serde_json::Value {
        let key = signing_key(pem, kid);
        serde_json::to_value(key.as_verification_key()).expect("JWK serializes")
    }

    fn oidc_config(issuer_url: String, redirect_url: Option<String>) -> OidcConfig {
        OidcConfig {
            issuer_url,
            client_id: "test-client".to_string(),
            client_secret: "test-client-secret".to_string(),
            redirect_url,
            ..OidcConfig::default()
        }
    }

    async fn mount_discovery_and_jwks(server: &MockServer, keys: Vec<serde_json::Value>) {
        let discovery = json!({
            "issuer": server.uri(),
            "authorization_endpoint": format!("{}/authorize", server.uri()),
            "token_endpoint": format!("{}/token", server.uri()),
            "jwks_uri": format!("{}/jwks", server.uri()),
            "response_types_supported": ["code"],
            "subject_types_supported": ["public"],
            "id_token_signing_alg_values_supported": ["RS256"],
        });
        Mock::given(method("GET"))
            .and(path("/.well-known/openid-configuration"))
            .respond_with(ResponseTemplate::new(200).set_body_json(discovery))
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .and(path("/jwks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "keys": keys })))
            // Capped so a later `mount()` (rotation tests) can serve a
            // different keyset once this one's budget is used up by the
            // discovery-time fetch.
            .up_to_n_times(1)
            .mount(server)
            .await;
    }

    #[allow(clippy::too_many_arguments)]
    fn sign_id_token(
        pem: &str,
        kid: &str,
        issuer: &str,
        audience: &str,
        subject: &str,
        nonce: &str,
        email: Option<&str>,
        email_verified: Option<bool>,
        name: Option<&str>,
        expires_in: chrono::Duration,
    ) -> String {
        let key = signing_key(pem, kid);
        let mut claims = CoreIdTokenClaims::new(
            IssuerUrl::new(issuer.to_string()).expect("issuer parses"),
            vec![Audience::new(audience.to_string())],
            Utc::now() + expires_in,
            Utc::now(),
            StandardClaims::new(SubjectIdentifier::new(subject.to_string())),
            EmptyAdditionalClaims {},
        )
        .set_nonce(Some(OidcNonce::new(nonce.to_string())));
        if let Some(email) = email {
            claims = claims.set_email(Some(EndUserEmail::new(email.to_string())));
        }
        if let Some(verified) = email_verified {
            claims = claims.set_email_verified(Some(verified));
        }
        if let Some(name) = name {
            let mut localized = LocalizedClaim::new();
            localized.insert(None, EndUserName::new(name.to_string()));
            claims = claims.set_name(Some(localized));
        }
        CoreIdToken::new(
            claims,
            &key,
            CoreJwsSigningAlgorithm::RsaSsaPkcs1V15Sha256,
            None,
            None,
        )
        .expect("ID token signs")
        .to_string()
    }

    /// Like [`sign_id_token`] but also sets an additional claim named
    /// `group_claim_name` carrying `groups` as a JSON string array — the
    /// shape task 3.2's mapping tests need to drive the callback's
    /// group-claim extraction (`crate::oidc::extract_groups`).
    #[allow(clippy::too_many_arguments)]
    fn sign_id_token_with_groups(
        pem: &str,
        kid: &str,
        issuer: &str,
        audience: &str,
        subject: &str,
        nonce: &str,
        email: &str,
        group_claim_name: &str,
        groups: &[&str],
    ) -> String {
        use crate::oidc::{GroupAwareClaims, GroupsIdToken, GroupsIdTokenClaims};

        let key = signing_key(pem, kid);
        let mut additional = std::collections::HashMap::new();
        additional.insert(
            group_claim_name.to_string(),
            json!(groups.iter().collect::<Vec<_>>()),
        );
        let claims = GroupsIdTokenClaims::new(
            IssuerUrl::new(issuer.to_string()).expect("issuer parses"),
            vec![Audience::new(audience.to_string())],
            Utc::now() + chrono::Duration::minutes(5),
            Utc::now(),
            StandardClaims::new(SubjectIdentifier::new(subject.to_string())),
            GroupAwareClaims(additional),
        )
        .set_nonce(Some(OidcNonce::new(nonce.to_string())))
        .set_email(Some(EndUserEmail::new(email.to_string())))
        .set_email_verified(Some(true));
        GroupsIdToken::new(
            claims,
            &key,
            CoreJwsSigningAlgorithm::RsaSsaPkcs1V15Sha256,
            None,
            None,
        )
        .expect("ID token signs")
        .to_string()
    }

    async fn mount_token_response(server: &MockServer, id_token: &str) {
        Mock::given(method("POST"))
            .and(path("/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "access_token": "test-access-token",
                "token_type": "Bearer",
                "id_token": id_token,
            })))
            .mount(server)
            .await;
    }

    fn tenant(id: &str, key: &str) -> TenantConfig {
        TenantConfig {
            id: id.to_string(),
            slug: id.to_string(),
            name: format!("{id} Inc"),
            default_dataset: Some("default".to_string()),
            datasets: vec![DatasetConfig {
                id: "default".to_string(),
                slug: "default".to_string(),
                is_default: true,
                storage: None,
            }],
            api_keys: vec![ApiKeyConfig {
                key: key.to_string(),
                name: Some("test".to_string()),
            }],
            schema_config: None,
            limits: None,
        }
    }

    async fn test_state(oidc: Option<OidcConfig>) -> RouterAppState {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let config = Configuration {
            auth: AuthConfig {
                tenants: vec![tenant("acme", "acme-key")],
                oidc,
                ..Default::default()
            },
            ..Default::default()
        };
        catalog.sync_config_tenants(&config.auth).await.unwrap();
        RouterAppState::new(catalog, config)
    }

    async fn wait_for_ready(state: &RouterAppState) {
        let runtime = state.oidc().expect("oidc configured for this test");
        for _ in 0..500 {
            if runtime.provider().await.is_some() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("OIDC provider did not become available in time");
    }

    fn query_param(url: &Url, name: &str) -> String {
        url.query_pairs()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.into_owned())
            .unwrap_or_else(|| panic!("query param {name} present in {url}"))
    }

    /// The Set-Cookie value minus attributes, e.g. `signaldb_oidc_pending=abc`.
    fn cookie_pair(set_cookie: &str) -> &str {
        set_cookie.split(';').next().expect("cookie name=value")
    }

    #[tokio::test]
    async fn start_returns_404_when_oidc_unconfigured() {
        let state = test_state(None).await;
        let app = create_router(state);
        let request = Request::builder()
            .uri("/ui/session/oidc/start")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn start_returns_503_naming_issuer_while_unavailable() {
        // No mocks registered: discovery can never succeed, so the runtime
        // stays `unavailable` for the whole test - no race to worry about.
        let server = MockServer::start().await;
        let state = test_state(Some(oidc_config(server.uri(), None))).await;
        let app = create_router(state);
        let request = Request::builder()
            .uri("/ui/session/oidc/start")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(body["error"].as_str().unwrap().contains(&server.uri()));
    }

    #[tokio::test]
    async fn start_redirects_with_state_nonce_pkce_and_sets_lax_pending_cookie() {
        let server = MockServer::start().await;
        mount_discovery_and_jwks(&server, vec![jwk_json(KEY_1_PEM, "kid1")]).await;
        let redirect_url = "https://signaldb.example.com/ui/session/oidc/callback".to_string();
        let state = test_state(Some(oidc_config(server.uri(), Some(redirect_url.clone())))).await;
        wait_for_ready(&state).await;
        let app = create_router(state);

        let request = Request::builder()
            .uri("/ui/session/oidc/start")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::FOUND);

        let location = response
            .headers()
            .get(header::LOCATION)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let url = Url::parse(&location).unwrap();
        assert!(location.starts_with(&format!("{}/authorize", server.uri())));
        assert_eq!(query_param(&url, "response_type"), "code");
        assert_eq!(query_param(&url, "client_id"), "test-client");
        assert_eq!(query_param(&url, "redirect_uri"), redirect_url);
        assert_eq!(query_param(&url, "code_challenge_method"), "S256");
        assert!(!query_param(&url, "state").is_empty());
        assert!(!query_param(&url, "nonce").is_empty());
        assert!(!query_param(&url, "code_challenge").is_empty());

        let set_cookie = response
            .headers()
            .get(header::SET_COOKIE)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(cookie_pair(set_cookie).starts_with("signaldb_oidc_pending="));
        assert!(set_cookie.contains("HttpOnly"));
        assert!(set_cookie.contains("Secure"));
        assert!(set_cookie.contains("SameSite=Lax"));
        assert!(!set_cookie.contains("SameSite=Strict"));
        assert!(set_cookie.contains("Path=/ui/session/oidc"));
        assert!(set_cookie.contains("Max-Age=300"));
    }

    /// Drives a real `/start` request to capture the actual, randomly
    /// generated state/nonce/PKCE and the pending cookie the callback needs
    /// — the callback tests below can't invent these values themselves.
    struct StartedLogin {
        state: String,
        nonce: String,
        pending_cookie: String,
    }

    async fn drive_start(app: &axum::Router) -> StartedLogin {
        let request = Request::builder()
            .uri("/ui/session/oidc/start")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::FOUND);
        let location = response
            .headers()
            .get(header::LOCATION)
            .unwrap()
            .to_str()
            .unwrap();
        let url = Url::parse(location).unwrap();
        let pending_cookie = response
            .headers()
            .get(header::SET_COOKIE)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        StartedLogin {
            state: query_param(&url, "state"),
            nonce: query_param(&url, "nonce"),
            pending_cookie: cookie_pair(&pending_cookie).to_string(),
        }
    }

    async fn drive_callback(
        app: &axum::Router,
        code: Option<&str>,
        state: Option<&str>,
        pending_cookie: Option<&str>,
    ) -> axum::response::Response {
        let mut uri = "/ui/session/oidc/callback".to_string();
        let mut params = vec![];
        if let Some(code) = code {
            params.push(format!("code={code}"));
        }
        if let Some(state) = state {
            params.push(format!("state={state}"));
        }
        if !params.is_empty() {
            uri.push('?');
            uri.push_str(&params.join("&"));
        }
        let mut builder = Request::builder().uri(uri);
        if let Some(cookie) = pending_cookie {
            builder = builder.header(header::COOKIE, cookie);
        }
        app.clone()
            .oneshot(builder.body(Body::empty()).unwrap())
            .await
            .unwrap()
    }

    fn assert_generic_rejection_without_session(response: &axum::response::Response) {
        assert_eq!(response.status(), StatusCode::FOUND);
        assert_eq!(
            response
                .headers()
                .get(header::LOCATION)
                .unwrap()
                .to_str()
                .unwrap(),
            "/?sso_error=1"
        );
        // A rejection may clear the pending cookie but must never set a
        // session cookie.
        for value in response.headers().get_all(header::SET_COOKIE) {
            assert!(!value.to_str().unwrap().starts_with("signaldb_session="));
        }
    }

    const REDIRECT_URL: &str = "https://signaldb.example.com/ui/session/oidc/callback";

    #[tokio::test]
    async fn callback_links_verified_email_to_existing_user_and_issues_session() {
        let server = MockServer::start().await;
        mount_discovery_and_jwks(&server, vec![jwk_json(KEY_1_PEM, "kid1")]).await;
        let state = test_state(Some(oidc_config(
            server.uri(),
            Some(REDIRECT_URL.to_string()),
        )))
        .await;
        wait_for_ready(&state).await;

        let catalog = state.catalog().clone();
        let password_hash = common::auth::hash_password("correct horse battery staple").unwrap();
        let user = catalog
            .create_user(
                "alice@example.com",
                Some("Alice"),
                Some(&password_hash),
                false,
            )
            .await
            .unwrap();
        catalog
            .upsert_tenant_membership(&user.id, "acme", MembershipRole::Viewer)
            .await
            .unwrap();

        let app = create_router(state);
        let login = drive_start(&app).await;
        let id_token = sign_id_token(
            KEY_1_PEM,
            "kid1",
            &server.uri(),
            "test-client",
            "idp-subject-1",
            &login.nonce,
            Some("alice@example.com"),
            Some(true),
            Some("Alice"),
            chrono::Duration::minutes(5),
        );
        mount_token_response(&server, &id_token).await;

        let response = drive_callback(
            &app,
            Some("test-code"),
            Some(&login.state),
            Some(&login.pending_cookie),
        )
        .await;

        assert_eq!(response.status(), StatusCode::FOUND);
        assert_eq!(
            response
                .headers()
                .get(header::LOCATION)
                .unwrap()
                .to_str()
                .unwrap(),
            "/"
        );
        let session_cookie = response
            .headers()
            .get_all(header::SET_COOKIE)
            .iter()
            .map(|v| v.to_str().unwrap().to_string())
            .find(|v| v.starts_with("signaldb_session="))
            .expect("session cookie set");

        // The identity is linked, not duplicated: whoami over the new
        // session names the same account the password login would.
        let cookie = cookie_pair(&session_cookie).to_string();
        let request = Request::builder()
            .uri("/api/v1/whoami")
            .header(header::COOKIE, &cookie)
            .header("x-tenant-id", "acme")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["user"]["email"], "alice@example.com");
        assert_eq!(body["user_id"], user.id);

        let linked = catalog.get_user(&user.id).await.unwrap().unwrap();
        assert_eq!(linked.oidc_issuer.as_deref(), Some(server.uri().as_str()));
        assert_eq!(linked.oidc_subject.as_deref(), Some("idp-subject-1"));
        assert_eq!(
            linked.password_hash.as_deref(),
            Some(password_hash.as_str())
        );
    }

    #[tokio::test]
    async fn callback_jit_creates_a_new_user_with_no_password() {
        let server = MockServer::start().await;
        mount_discovery_and_jwks(&server, vec![jwk_json(KEY_1_PEM, "kid1")]).await;
        let state = test_state(Some(oidc_config(
            server.uri(),
            Some(REDIRECT_URL.to_string()),
        )))
        .await;
        wait_for_ready(&state).await;
        let catalog = state.catalog().clone();
        let app = create_router(state);

        let login = drive_start(&app).await;
        let id_token = sign_id_token(
            KEY_1_PEM,
            "kid1",
            &server.uri(),
            "test-client",
            "idp-subject-new",
            &login.nonce,
            Some("newperson@example.com"),
            Some(true),
            Some("New Person"),
            chrono::Duration::minutes(5),
        );
        mount_token_response(&server, &id_token).await;

        let response = drive_callback(
            &app,
            Some("test-code"),
            Some(&login.state),
            Some(&login.pending_cookie),
        )
        .await;
        assert_eq!(response.status(), StatusCode::FOUND);
        assert!(
            response
                .headers()
                .get_all(header::SET_COOKIE)
                .iter()
                .any(|v| v.to_str().unwrap().starts_with("signaldb_session="))
        );

        let created = catalog
            .find_user_by_oidc_identity(&server.uri(), "idp-subject-new")
            .await
            .unwrap()
            .expect("JIT-created user exists");
        assert_eq!(created.email, "newperson@example.com");
        assert_eq!(created.display_name.as_deref(), Some("New Person"));
        assert!(created.password_hash.is_none());
        assert!(!created.is_instance_admin);
    }

    #[tokio::test]
    async fn callback_rejects_missing_pending_cookie() {
        let server = MockServer::start().await;
        mount_discovery_and_jwks(&server, vec![jwk_json(KEY_1_PEM, "kid1")]).await;
        let state = test_state(Some(oidc_config(
            server.uri(),
            Some(REDIRECT_URL.to_string()),
        )))
        .await;
        wait_for_ready(&state).await;
        let app = create_router(state);

        let response = drive_callback(&app, Some("code"), Some("some-state"), None).await;
        assert_generic_rejection_without_session(&response);
    }

    #[tokio::test]
    async fn callback_rejects_state_mismatch() {
        let server = MockServer::start().await;
        mount_discovery_and_jwks(&server, vec![jwk_json(KEY_1_PEM, "kid1")]).await;
        let state = test_state(Some(oidc_config(
            server.uri(),
            Some(REDIRECT_URL.to_string()),
        )))
        .await;
        wait_for_ready(&state).await;
        let app = create_router(state);

        let login = drive_start(&app).await;
        let response = drive_callback(
            &app,
            Some("code"),
            Some("not-the-real-state"),
            Some(&login.pending_cookie),
        )
        .await;
        assert_generic_rejection_without_session(&response);
    }

    #[tokio::test]
    async fn callback_rejects_bad_nonce() {
        let server = MockServer::start().await;
        mount_discovery_and_jwks(&server, vec![jwk_json(KEY_1_PEM, "kid1")]).await;
        let state = test_state(Some(oidc_config(
            server.uri(),
            Some(REDIRECT_URL.to_string()),
        )))
        .await;
        wait_for_ready(&state).await;
        let app = create_router(state);

        let login = drive_start(&app).await;
        // Signed with a nonce that does NOT match the one `/start` minted.
        let id_token = sign_id_token(
            KEY_1_PEM,
            "kid1",
            &server.uri(),
            "test-client",
            "idp-subject-1",
            "wrong-nonce",
            Some("bob@example.com"),
            Some(true),
            None,
            chrono::Duration::minutes(5),
        );
        mount_token_response(&server, &id_token).await;

        let response = drive_callback(
            &app,
            Some("code"),
            Some(&login.state),
            Some(&login.pending_cookie),
        )
        .await;
        assert_generic_rejection_without_session(&response);
    }

    #[tokio::test]
    async fn callback_rejects_bad_signature() {
        let server = MockServer::start().await;
        // JWKS advertises only `kid1`'s (KEY_1) public key.
        mount_discovery_and_jwks(&server, vec![jwk_json(KEY_1_PEM, "kid1")]).await;
        let state = test_state(Some(oidc_config(
            server.uri(),
            Some(REDIRECT_URL.to_string()),
        )))
        .await;
        wait_for_ready(&state).await;
        let app = create_router(state);

        let login = drive_start(&app).await;
        // Signed with KEY_2 but the header claims `kid1` — the verifier
        // finds `kid1`'s (KEY_1) public key and the signature fails to
        // verify against it, a different failure than an unknown `kid`.
        let id_token = sign_id_token(
            KEY_2_PEM,
            "kid1",
            &server.uri(),
            "test-client",
            "idp-subject-1",
            &login.nonce,
            Some("bob@example.com"),
            Some(true),
            None,
            chrono::Duration::minutes(5),
        );
        mount_token_response(&server, &id_token).await;

        let response = drive_callback(
            &app,
            Some("code"),
            Some(&login.state),
            Some(&login.pending_cookie),
        )
        .await;
        assert_generic_rejection_without_session(&response);
    }

    #[tokio::test]
    async fn callback_rejects_expired_token() {
        let server = MockServer::start().await;
        mount_discovery_and_jwks(&server, vec![jwk_json(KEY_1_PEM, "kid1")]).await;
        let state = test_state(Some(oidc_config(
            server.uri(),
            Some(REDIRECT_URL.to_string()),
        )))
        .await;
        wait_for_ready(&state).await;
        let app = create_router(state);

        let login = drive_start(&app).await;
        let id_token = sign_id_token(
            KEY_1_PEM,
            "kid1",
            &server.uri(),
            "test-client",
            "idp-subject-1",
            &login.nonce,
            Some("bob@example.com"),
            Some(true),
            None,
            // Well past the crate's ~5 minute clock-skew leeway.
            chrono::Duration::minutes(-30),
        );
        mount_token_response(&server, &id_token).await;

        let response = drive_callback(
            &app,
            Some("code"),
            Some(&login.state),
            Some(&login.pending_cookie),
        )
        .await;
        assert_generic_rejection_without_session(&response);
    }

    #[tokio::test]
    async fn callback_unverified_email_does_not_link_and_allowlist_refuses_jit() {
        let server = MockServer::start().await;
        mount_discovery_and_jwks(&server, vec![jwk_json(KEY_1_PEM, "kid1")]).await;
        let mut config = oidc_config(server.uri(), Some(REDIRECT_URL.to_string()));
        config.allowed_email_domains = Some(vec!["nowhere.example".to_string()]);
        let state = test_state(Some(config)).await;
        wait_for_ready(&state).await;

        let catalog = state.catalog().clone();
        let password_hash = common::auth::hash_password("correct horse battery staple").unwrap();
        let user = catalog
            .create_user(
                "alice@example.com",
                Some("Alice"),
                Some(&password_hash),
                false,
            )
            .await
            .unwrap();

        let app = create_router(state);
        let login = drive_start(&app).await;
        let id_token = sign_id_token(
            KEY_1_PEM,
            "kid1",
            &server.uri(),
            "test-client",
            "idp-subject-unverified",
            &login.nonce,
            // Same email as the existing user, but NOT verified: must be
            // treated as no match rather than linked (design decision 3).
            Some("alice@example.com"),
            Some(false),
            None,
            chrono::Duration::minutes(5),
        );
        mount_token_response(&server, &id_token).await;

        let response = drive_callback(
            &app,
            Some("code"),
            Some(&login.state),
            Some(&login.pending_cookie),
        )
        .await;
        assert_generic_rejection_without_session(&response);

        let unchanged = catalog.get_user(&user.id).await.unwrap().unwrap();
        assert!(unchanged.oidc_issuer.is_none(), "must not have been linked");
        assert!(
            catalog
                .find_user_by_oidc_identity(&server.uri(), "idp-subject-unverified")
                .await
                .unwrap()
                .is_none(),
            "must not have JIT-created a user either"
        );
    }

    #[tokio::test]
    async fn callback_allowlist_refuses_outside_identity_and_creates_no_user() {
        let server = MockServer::start().await;
        mount_discovery_and_jwks(&server, vec![jwk_json(KEY_1_PEM, "kid1")]).await;
        let mut config = oidc_config(server.uri(), Some(REDIRECT_URL.to_string()));
        config.allowed_email_domains = Some(vec!["example.com".to_string()]);
        let state = test_state(Some(config)).await;
        wait_for_ready(&state).await;
        let catalog = state.catalog().clone();
        let users_before = catalog.list_users().await.unwrap().len();

        let app = create_router(state);
        let login = drive_start(&app).await;
        let id_token = sign_id_token(
            KEY_1_PEM,
            "kid1",
            &server.uri(),
            "test-client",
            "idp-subject-mallory",
            &login.nonce,
            Some("mallory@evil.test"),
            Some(true),
            None,
            chrono::Duration::minutes(5),
        );
        mount_token_response(&server, &id_token).await;

        let response = drive_callback(
            &app,
            Some("code"),
            Some(&login.state),
            Some(&login.pending_cookie),
        )
        .await;
        assert_generic_rejection_without_session(&response);
        assert_eq!(catalog.list_users().await.unwrap().len(), users_before);
    }

    #[tokio::test]
    async fn callback_refuses_disabled_user() {
        let server = MockServer::start().await;
        mount_discovery_and_jwks(&server, vec![jwk_json(KEY_1_PEM, "kid1")]).await;
        let state = test_state(Some(oidc_config(
            server.uri(),
            Some(REDIRECT_URL.to_string()),
        )))
        .await;
        wait_for_ready(&state).await;
        let catalog = state.catalog().clone();
        let user = catalog
            .create_oidc_user(
                "disabled@example.com",
                Some("Disabled"),
                &server.uri(),
                "idp-subject-disabled",
            )
            .await
            .unwrap();
        catalog.set_user_disabled(&user.id, true).await.unwrap();

        let app = create_router(state);
        let login = drive_start(&app).await;
        let id_token = sign_id_token(
            KEY_1_PEM,
            "kid1",
            &server.uri(),
            "test-client",
            "idp-subject-disabled",
            &login.nonce,
            Some("disabled@example.com"),
            Some(true),
            None,
            chrono::Duration::minutes(5),
        );
        mount_token_response(&server, &id_token).await;

        let response = drive_callback(
            &app,
            Some("code"),
            Some(&login.state),
            Some(&login.pending_cookie),
        )
        .await;
        assert_generic_rejection_without_session(&response);
    }

    #[tokio::test]
    async fn callback_disabled_user_matched_by_email_is_refused_and_not_linked() {
        // Distinct from `callback_refuses_disabled_user` above: that test's
        // identity already matches by `(issuer, subject)` (the first
        // resolution branch). This one is a disabled *local* user with no
        // existing OIDC identity, matched only via the verified-email link
        // branch — `resolve_identity` must not call `link_oidc_identity`
        // before the caller's `disabled_at` check has a chance to refuse.
        let server = MockServer::start().await;
        mount_discovery_and_jwks(&server, vec![jwk_json(KEY_1_PEM, "kid1")]).await;
        let state = test_state(Some(oidc_config(
            server.uri(),
            Some(REDIRECT_URL.to_string()),
        )))
        .await;
        wait_for_ready(&state).await;
        let catalog = state.catalog().clone();
        let password_hash = common::auth::hash_password("correct horse battery staple").unwrap();
        let user = catalog
            .create_user(
                "disabled-local@example.com",
                Some("Disabled Local"),
                Some(&password_hash),
                false,
            )
            .await
            .unwrap();
        catalog.set_user_disabled(&user.id, true).await.unwrap();

        let app = create_router(state);
        let login = drive_start(&app).await;
        let id_token = sign_id_token(
            KEY_1_PEM,
            "kid1",
            &server.uri(),
            "test-client",
            "idp-subject-disabled-local",
            &login.nonce,
            Some("disabled-local@example.com"),
            Some(true),
            None,
            chrono::Duration::minutes(5),
        );
        mount_token_response(&server, &id_token).await;

        let response = drive_callback(
            &app,
            Some("code"),
            Some(&login.state),
            Some(&login.pending_cookie),
        )
        .await;
        assert_generic_rejection_without_session(&response);

        let unchanged = catalog.get_user(&user.id).await.unwrap().unwrap();
        assert!(
            unchanged.oidc_issuer.is_none(),
            "disabled user must not have been linked"
        );
        assert!(
            catalog
                .find_user_by_oidc_identity(&server.uri(), "idp-subject-disabled-local")
                .await
                .unwrap()
                .is_none(),
            "no identity should have been linked for a disabled user"
        );
    }

    #[tokio::test]
    async fn callback_rejects_audience_mismatch() {
        // Pins that `openidconnect`'s `IdTokenVerifier` enforces `aud`: an ID
        // token signed for a different client must be rejected generically,
        // just like a bad nonce or signature.
        let server = MockServer::start().await;
        mount_discovery_and_jwks(&server, vec![jwk_json(KEY_1_PEM, "kid1")]).await;
        let state = test_state(Some(oidc_config(
            server.uri(),
            Some(REDIRECT_URL.to_string()),
        )))
        .await;
        wait_for_ready(&state).await;
        let app = create_router(state);

        let login = drive_start(&app).await;
        let id_token = sign_id_token(
            KEY_1_PEM,
            "kid1",
            &server.uri(),
            // Configured client_id is "test-client"; this token is minted
            // for a different audience entirely.
            "some-other-client",
            "idp-subject-1",
            &login.nonce,
            Some("bob@example.com"),
            Some(true),
            None,
            chrono::Duration::minutes(5),
        );
        mount_token_response(&server, &id_token).await;

        let response = drive_callback(
            &app,
            Some("code"),
            Some(&login.state),
            Some(&login.pending_cookie),
        )
        .await;
        assert_generic_rejection_without_session(&response);
    }

    #[tokio::test]
    async fn callback_rejects_code_replay() {
        // Defense-in-depth pin: this relying party relies on the IdP
        // refusing to redeem an authorization code twice. Model that by
        // capping the token-endpoint mock at one successful redemption; the
        // second callback submission (same pending cookie, same code) must
        // be rejected generically with no second session minted.
        let server = MockServer::start().await;
        mount_discovery_and_jwks(&server, vec![jwk_json(KEY_1_PEM, "kid1")]).await;
        let state = test_state(Some(oidc_config(
            server.uri(),
            Some(REDIRECT_URL.to_string()),
        )))
        .await;
        wait_for_ready(&state).await;
        let app = create_router(state);

        let login = drive_start(&app).await;
        let id_token = sign_id_token(
            KEY_1_PEM,
            "kid1",
            &server.uri(),
            "test-client",
            "idp-subject-replay",
            &login.nonce,
            Some("replay@example.com"),
            Some(true),
            None,
            chrono::Duration::minutes(5),
        );
        Mock::given(method("POST"))
            .and(path("/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "access_token": "test-access-token",
                "token_type": "Bearer",
                "id_token": id_token,
            })))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/token"))
            .respond_with(ResponseTemplate::new(400).set_body_json(json!({
                "error": "invalid_grant",
            })))
            .mount(&server)
            .await;

        let first = drive_callback(
            &app,
            Some("test-code"),
            Some(&login.state),
            Some(&login.pending_cookie),
        )
        .await;
        assert_eq!(first.status(), StatusCode::FOUND);
        assert!(
            first
                .headers()
                .get_all(header::SET_COOKIE)
                .iter()
                .any(|v| v.to_str().unwrap().starts_with("signaldb_session=")),
            "first redemption should succeed"
        );

        let second = drive_callback(
            &app,
            Some("test-code"),
            Some(&login.state),
            Some(&login.pending_cookie),
        )
        .await;
        assert_generic_rejection_without_session(&second);
    }

    #[tokio::test]
    async fn callback_links_existing_user_outside_allowlist_via_verified_email() {
        // Design intent (not a bug): `allowed_email_domains` gates only JIT
        // creation, not the verified-email link path. An already-existing
        // user whose email falls outside the allowlist can still link and
        // log in via SSO once matched. This nuance must be called out in the
        // Group 5 IdP setup guide.
        let server = MockServer::start().await;
        mount_discovery_and_jwks(&server, vec![jwk_json(KEY_1_PEM, "kid1")]).await;
        let mut config = oidc_config(server.uri(), Some(REDIRECT_URL.to_string()));
        config.allowed_email_domains = Some(vec!["allowed.example".to_string()]);
        let state = test_state(Some(config)).await;
        wait_for_ready(&state).await;

        let catalog = state.catalog().clone();
        let password_hash = common::auth::hash_password("correct horse battery staple").unwrap();
        let user = catalog
            .create_user(
                "carol@outside.example",
                Some("Carol"),
                Some(&password_hash),
                false,
            )
            .await
            .unwrap();

        let app = create_router(state);
        let login = drive_start(&app).await;
        let id_token = sign_id_token(
            KEY_1_PEM,
            "kid1",
            &server.uri(),
            "test-client",
            "idp-subject-carol",
            &login.nonce,
            Some("carol@outside.example"),
            Some(true),
            None,
            chrono::Duration::minutes(5),
        );
        mount_token_response(&server, &id_token).await;

        let response = drive_callback(
            &app,
            Some("code"),
            Some(&login.state),
            Some(&login.pending_cookie),
        )
        .await;
        assert_eq!(response.status(), StatusCode::FOUND);
        assert!(
            response
                .headers()
                .get_all(header::SET_COOKIE)
                .iter()
                .any(|v| v.to_str().unwrap().starts_with("signaldb_session=")),
            "an existing user outside the allowlist must still link and log in"
        );

        let linked = catalog.get_user(&user.id).await.unwrap().unwrap();
        assert_eq!(linked.oidc_issuer.as_deref(), Some(server.uri().as_str()));
        assert_eq!(linked.oidc_subject.as_deref(), Some("idp-subject-carol"));
    }

    #[tokio::test]
    async fn callback_recovers_from_jwks_rotation_on_unknown_kid() {
        let server = MockServer::start().await;
        mount_discovery_and_jwks(&server, vec![jwk_json(KEY_1_PEM, "kid1")]).await;
        let state = test_state(Some(oidc_config(
            server.uri(),
            Some(REDIRECT_URL.to_string()),
        )))
        .await;
        wait_for_ready(&state).await;
        let app = create_router(state);

        let login = drive_start(&app).await;
        // Signed with a second key/kid that the JWKS served at discovery
        // time never advertised.
        let id_token = sign_id_token(
            KEY_2_PEM,
            "kid2",
            &server.uri(),
            "test-client",
            "idp-subject-rotated",
            &login.nonce,
            Some("rotated@example.com"),
            Some(true),
            None,
            chrono::Duration::minutes(5),
        );
        mount_token_response(&server, &id_token).await;

        // The cached JWKS (kid1 only) rejects the first verification
        // attempt with `NoMatchingKey`; a refetch of `/jwks` (now serving
        // both keys) must succeed on retry.
        Mock::given(method("GET"))
            .and(path("/jwks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "keys": [jwk_json(KEY_1_PEM, "kid1"), jwk_json(KEY_2_PEM, "kid2")],
            })))
            .mount(&server)
            .await;

        let response = drive_callback(
            &app,
            Some("code"),
            Some(&login.state),
            Some(&login.pending_cookie),
        )
        .await;
        assert_eq!(response.status(), StatusCode::FOUND);
        assert_eq!(
            response
                .headers()
                .get(header::LOCATION)
                .unwrap()
                .to_str()
                .unwrap(),
            "/"
        );
        assert!(
            response
                .headers()
                .get_all(header::SET_COOKIE)
                .iter()
                .any(|v| v.to_str().unwrap().starts_with("signaldb_session=")),
            "JWKS rotation should have let the login through"
        );
    }

    #[tokio::test]
    async fn invalid_oidc_config_fails_startup_naming_the_setting() {
        // Group 1 already covers `OidcConfig::validate()` exhaustively;
        // this pins that the router's actual startup path
        // (`Configuration::load_from_path`, which `cli::run` calls) still
        // fails hard on it rather than silently degrading like an
        // unreachable issuer does.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("signaldb.toml");
        std::fs::write(
            &path,
            r#"
            [auth.oidc]
            disable_password_login = true
            "#,
        )
        .unwrap();

        let error = Configuration::load_from_path(&path).unwrap_err();
        assert!(error.to_string().contains("issuer_url"));
    }

    // --- Group-claim -> membership mapping (tasks 3.2/3.3) ---

    /// Like [`test_state`] but with two tenants (`acme`, `globex`) and an
    /// `OidcConfig` that also carries `group_claim`/`group_mappings`, the
    /// shape every mapping test below needs.
    async fn test_state_with_mapping(mut config: OidcConfig) -> RouterAppState {
        config.group_claim = Some("groups".to_string());
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let full_config = Configuration {
            auth: AuthConfig {
                tenants: vec![tenant("acme", "acme-key"), tenant("globex", "globex-key")],
                oidc: Some(config),
                ..Default::default()
            },
            ..Default::default()
        };
        catalog
            .sync_config_tenants(&full_config.auth)
            .await
            .unwrap();
        RouterAppState::new(catalog, full_config)
    }

    fn group_mapping(
        group: &str,
        tenant: &str,
        role: MembershipRole,
    ) -> common::config::GroupMapping {
        common::config::GroupMapping {
            group: group.to_string(),
            tenant: tenant.to_string(),
            role,
        }
    }

    #[tokio::test]
    async fn callback_grants_mapped_membership_when_token_carries_group() {
        let server = MockServer::start().await;
        mount_discovery_and_jwks(&server, vec![jwk_json(KEY_1_PEM, "kid1")]).await;
        let mut config = oidc_config(server.uri(), Some(REDIRECT_URL.to_string()));
        config.group_mappings = vec![group_mapping(
            "observability-admins",
            "acme",
            MembershipRole::Admin,
        )];
        let state = test_state_with_mapping(config).await;
        wait_for_ready(&state).await;
        let catalog = state.catalog().clone();
        let app = create_router(state);

        let login = drive_start(&app).await;
        let id_token = sign_id_token_with_groups(
            KEY_1_PEM,
            "kid1",
            &server.uri(),
            "test-client",
            "idp-subject-mapped",
            &login.nonce,
            "mapped@example.com",
            "groups",
            &["observability-admins"],
        );
        mount_token_response(&server, &id_token).await;

        let response = drive_callback(
            &app,
            Some("code"),
            Some(&login.state),
            Some(&login.pending_cookie),
        )
        .await;
        assert_eq!(response.status(), StatusCode::FOUND);
        assert!(
            response
                .headers()
                .get_all(header::SET_COOKIE)
                .iter()
                .any(|v| v.to_str().unwrap().starts_with("signaldb_session=")),
        );

        let user = catalog
            .find_user_by_oidc_identity(&server.uri(), "idp-subject-mapped")
            .await
            .unwrap()
            .expect("JIT-created user exists");
        let membership = catalog
            .get_tenant_membership(&user.id, "acme")
            .await
            .unwrap()
            .expect("mapped membership exists");
        assert_eq!(membership.role, MembershipRole::Admin);
        assert_eq!(membership.granted_by, GrantSource::OidcMapping);
        // Mapping SHALL NOT grant the instance-admin flag (spec).
        assert!(!user.is_instance_admin);
    }

    #[tokio::test]
    async fn callback_grants_highest_role_when_two_groups_map_to_the_same_tenant() {
        // Two rules both target `acme` at different roles; a token carrying
        // both groups must not crash the login on the `(user_id, tenant_id,
        // granted_by)` primary key. The user ends up with the higher role.
        let server = MockServer::start().await;
        mount_discovery_and_jwks(&server, vec![jwk_json(KEY_1_PEM, "kid1")]).await;
        let mut config = oidc_config(server.uri(), Some(REDIRECT_URL.to_string()));
        config.group_mappings = vec![
            group_mapping("org-viewers", "acme", MembershipRole::Viewer),
            group_mapping("org-admins", "acme", MembershipRole::Admin),
        ];
        let state = test_state_with_mapping(config).await;
        wait_for_ready(&state).await;
        let catalog = state.catalog().clone();
        let app = create_router(state);

        let login = drive_start(&app).await;
        let id_token = sign_id_token_with_groups(
            KEY_1_PEM,
            "kid1",
            &server.uri(),
            "test-client",
            "idp-subject-double-mapped",
            &login.nonce,
            "double-mapped@example.com",
            "groups",
            &["org-viewers", "org-admins"],
        );
        mount_token_response(&server, &id_token).await;

        let response = drive_callback(
            &app,
            Some("code"),
            Some(&login.state),
            Some(&login.pending_cookie),
        )
        .await;
        assert_eq!(
            response.status(),
            StatusCode::FOUND,
            "login must succeed despite two mappings targeting the same tenant"
        );

        let user = catalog
            .find_user_by_oidc_identity(&server.uri(), "idp-subject-double-mapped")
            .await
            .unwrap()
            .expect("JIT-created user exists");
        let membership = catalog
            .get_tenant_membership(&user.id, "acme")
            .await
            .unwrap()
            .expect("mapped membership exists");
        assert_eq!(membership.role, MembershipRole::Admin);
    }

    #[tokio::test]
    async fn callback_skips_mapping_to_a_nonexistent_tenant_and_still_succeeds() {
        // One rule maps to a tenant that was never created (typo or not yet
        // provisioned), another maps to a real tenant. The token carries
        // both groups: login must succeed, the valid mapping must apply,
        // and the unknown-tenant mapping must be silently skipped rather
        // than failing the whole sync (an FK violation would otherwise
        // reject the login for every user in that group).
        let server = MockServer::start().await;
        mount_discovery_and_jwks(&server, vec![jwk_json(KEY_1_PEM, "kid1")]).await;
        let mut config = oidc_config(server.uri(), Some(REDIRECT_URL.to_string()));
        config.group_mappings = vec![
            group_mapping("ghost-team", "no-such-tenant", MembershipRole::Admin),
            group_mapping("observability-admins", "acme", MembershipRole::Viewer),
        ];
        let state = test_state_with_mapping(config).await;
        wait_for_ready(&state).await;
        let catalog = state.catalog().clone();
        let app = create_router(state);

        let login = drive_start(&app).await;
        let id_token = sign_id_token_with_groups(
            KEY_1_PEM,
            "kid1",
            &server.uri(),
            "test-client",
            "idp-subject-unknown-tenant",
            &login.nonce,
            "unknown-tenant@example.com",
            "groups",
            &["ghost-team", "observability-admins"],
        );
        mount_token_response(&server, &id_token).await;

        let response = drive_callback(
            &app,
            Some("code"),
            Some(&login.state),
            Some(&login.pending_cookie),
        )
        .await;
        assert_eq!(
            response.status(),
            StatusCode::FOUND,
            "a mapping to an unknown tenant must not reject the login"
        );

        let user = catalog
            .find_user_by_oidc_identity(&server.uri(), "idp-subject-unknown-tenant")
            .await
            .unwrap()
            .expect("JIT-created user exists");
        let acme_membership = catalog
            .get_tenant_membership(&user.id, "acme")
            .await
            .unwrap()
            .expect("the valid mapping was still applied");
        assert_eq!(acme_membership.role, MembershipRole::Viewer);
        assert!(
            catalog
                .get_tenant_membership(&user.id, "no-such-tenant")
                .await
                .unwrap()
                .is_none(),
            "the unknown-tenant mapping must not produce a membership row"
        );
    }

    #[tokio::test]
    async fn callback_lost_group_removes_only_mapped_row_and_leaves_local_membership() {
        let server = MockServer::start().await;
        mount_discovery_and_jwks(&server, vec![jwk_json(KEY_1_PEM, "kid1")]).await;
        let mut config = oidc_config(server.uri(), Some(REDIRECT_URL.to_string()));
        config.group_mappings = vec![group_mapping(
            "observability-admins",
            "globex",
            MembershipRole::Admin,
        )];
        let state = test_state_with_mapping(config).await;
        wait_for_ready(&state).await;
        let catalog = state.catalog().clone();

        // A prior login already granted the mapped membership in `globex`,
        // and an admin has separately, locally granted this user `viewer`
        // in `acme`.
        let user = catalog
            .create_oidc_user(
                "loses-group@example.com",
                Some("Loses Group"),
                &server.uri(),
                "idp-subject-loses-group",
            )
            .await
            .unwrap();
        catalog
            .sync_oidc_memberships(&user.id, &[("globex".to_string(), MembershipRole::Admin)])
            .await
            .unwrap();
        catalog
            .upsert_tenant_membership(&user.id, "acme", MembershipRole::Viewer)
            .await
            .unwrap();

        let app = create_router(state);
        let login = drive_start(&app).await;
        // This login's token no longer carries `observability-admins`.
        let id_token = sign_id_token_with_groups(
            KEY_1_PEM,
            "kid1",
            &server.uri(),
            "test-client",
            "idp-subject-loses-group",
            &login.nonce,
            "loses-group@example.com",
            "groups",
            &[],
        );
        mount_token_response(&server, &id_token).await;

        let response = drive_callback(
            &app,
            Some("code"),
            Some(&login.state),
            Some(&login.pending_cookie),
        )
        .await;
        assert_eq!(response.status(), StatusCode::FOUND);

        assert!(
            catalog
                .get_tenant_membership(&user.id, "globex")
                .await
                .unwrap()
                .is_none(),
            "the mapped membership must be removed once the group disappears"
        );
        let local = catalog
            .get_tenant_membership(&user.id, "acme")
            .await
            .unwrap()
            .expect("the locally granted membership must survive");
        assert_eq!(local.role, MembershipRole::Viewer);
        assert_eq!(local.granted_by, GrantSource::Local);
    }

    #[tokio::test]
    async fn callback_without_mapping_config_makes_no_membership_writes() {
        let server = MockServer::start().await;
        mount_discovery_and_jwks(&server, vec![jwk_json(KEY_1_PEM, "kid1")]).await;
        // No `group_mappings` configured at all: `group_claim` alone must
        // not be enough to trigger a sync (spec: "no mapping, no membership
        // changes").
        let config = oidc_config(server.uri(), Some(REDIRECT_URL.to_string()));
        let state = test_state_with_mapping(config).await;
        wait_for_ready(&state).await;
        let catalog = state.catalog().clone();

        let user = catalog
            .create_oidc_user(
                "no-mapping@example.com",
                Some("No Mapping"),
                &server.uri(),
                "idp-subject-no-mapping",
            )
            .await
            .unwrap();
        catalog
            .upsert_tenant_membership(&user.id, "acme", MembershipRole::Viewer)
            .await
            .unwrap();
        let before = catalog.list_memberships_for_user(&user.id).await.unwrap();

        let app = create_router(state);
        let login = drive_start(&app).await;
        let id_token = sign_id_token_with_groups(
            KEY_1_PEM,
            "kid1",
            &server.uri(),
            "test-client",
            "idp-subject-no-mapping",
            &login.nonce,
            "no-mapping@example.com",
            "groups",
            &["observability-admins"],
        );
        mount_token_response(&server, &id_token).await;

        let response = drive_callback(
            &app,
            Some("code"),
            Some(&login.state),
            Some(&login.pending_cookie),
        )
        .await;
        assert_eq!(response.status(), StatusCode::FOUND);

        let after = catalog.list_memberships_for_user(&user.id).await.unwrap();
        assert_eq!(
            before.len(),
            after.len(),
            "memberships after login must be exactly the memberships before"
        );
        assert_eq!(after[0].role, MembershipRole::Viewer);
        assert_eq!(after[0].granted_by, GrantSource::Local);
    }
}
