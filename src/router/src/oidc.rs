//! # OIDC relying-party runtime (change: oidc-login)
//!
//! Single-provider state attached to the router: startup discovery with a
//! background retry loop (design decision 10 — fail hard on bad config,
//! degrade on an unreachable issuer), the signed pending-login cookie, and
//! ID-token verification with JWKS rotation. The HTTP surface this backs
//! (`GET /ui/session/oidc/{start,callback}`) lives in
//! `crate::endpoints::oidc`.
//!
//! Every outbound call to the IdP (discovery, JWKS, token exchange) is a
//! boundary surface and goes through [`TracedHttpClient`], the single call
//! site that opens an HTTP client span via
//! `common::self_monitoring::spans::http_client_span`.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use hmac::{Hmac, KeyInit, Mac};
use openidconnect::core::{
    CoreAuthenticationFlow, CoreClient, CoreIdToken, CoreIdTokenClaims, CoreIdTokenVerifier,
    CoreJsonWebKeySet, CoreProviderMetadata,
};
use openidconnect::{
    AsyncHttpClient, AuthorizationCode, ClaimsVerificationError, ClientId, ClientSecret, CsrfToken,
    EndpointMaybeSet, EndpointNotSet, EndpointSet, HttpRequest, HttpResponse, IdTokenVerifier,
    IssuerUrl, JsonWebKeySet, JsonWebKeySetUrl, Nonce, PkceCodeChallenge, PkceCodeVerifier,
    RedirectUrl, Scope, SignatureVerificationError, TokenResponse,
};
use sha2::Sha256;
use tokio::sync::RwLock;
use tracing::Instrument;

use common::config::OidcConfig;
use common::self_monitoring::spans::{
    http_client_span, record_http_client_result, record_span_error,
};

type HmacSha256 = Hmac<Sha256>;

/// Concrete client type produced by [`CoreClient::from_provider_metadata`]:
/// discovery always resolves the authorization endpoint, and resolves the
/// token endpoint "maybe" — the type only promises what discovery
/// guarantees, though the OIDC spec requires it in practice.
type DiscoveredClient = CoreClient<
    EndpointSet,
    EndpointNotSet,
    EndpointNotSet,
    EndpointNotSet,
    EndpointMaybeSet,
    EndpointMaybeSet,
>;

/// Name of the short-lived, HMAC-signed, stateless cookie carrying the
/// pending login's state/nonce/PKCE-verifier (design decision 2).
pub const PENDING_COOKIE_NAME: &str = "signaldb_oidc_pending";
/// Narrow path the pending cookie is scoped to, matching the two endpoints
/// that read or set it.
pub const PENDING_COOKIE_PATH: &str = "/ui/session/oidc";
/// `Max-Age` of the pending cookie, and the independent server-side TTL the
/// signed payload's `issued_at` is checked against.
pub const PENDING_COOKIE_TTL_SECS: i64 = 300;

/// Domain-separation label for deriving the pending-cookie HMAC key from the
/// configured OIDC client secret. `[auth.oidc]` has no dedicated signing
/// secret (Group 1 didn't add one); the client secret is the only
/// confidential value already guaranteed present, shared across router
/// replicas, and scoped to this login mechanism, so it is used as HKDF-like
/// input rather than minting a new persisted secret for a 5-minute artifact.
const PENDING_COOKIE_KEY_LABEL: &[u8] = b"signaldb-oidc-pending-login-v1";

fn derive_pending_cookie_key(client_secret: &str) -> [u8; 32] {
    // HMAC-SHA256 accepts a key of any length, so this construction is
    // genuinely infallible; an `Err` here would mean the `hmac` crate itself
    // is broken, which no caller can meaningfully recover from.
    let mut mac = match HmacSha256::new_from_slice(PENDING_COOKIE_KEY_LABEL) {
        Ok(mac) => mac,
        Err(error) => panic!("HMAC-SHA256 key construction is infallible: {error}"),
    };
    mac.update(client_secret.as_bytes());
    mac.finalize().into_bytes().into()
}

/// The pending login's contents, carried signed and base64url-encoded in the
/// [`PENDING_COOKIE_NAME`] cookie value as `{payload}.{hmac_tag}`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct PendingLoginPayload {
    state: String,
    nonce: String,
    pkce_verifier: String,
    redirect_uri: String,
    issued_at: i64,
}

/// A pending login recovered from a verified cookie, typed for direct use
/// against `openidconnect`.
pub struct PendingLogin {
    pub state: String,
    pub nonce: Nonce,
    pub pkce_verifier: PkceCodeVerifier,
    pub redirect_uri: String,
}

/// Sign a pending login into the cookie value: `state`/`nonce` are passed as
/// plain strings (the caller holds the `CsrfToken`/`Nonce` it minted) so this
/// module doesn't need to know their internal representation.
///
/// Reachable from the start handler on every request, so failures (in
/// practice, never expected — `PendingLoginPayload` is a struct of plain
/// strings/an i64, and HMAC-SHA256 accepts a key of any length) are returned
/// rather than panicking the request.
pub fn sign_pending_login(
    client_secret: &str,
    state: &str,
    nonce: &str,
    pkce_verifier: &str,
    redirect_uri: &str,
) -> anyhow::Result<String> {
    let payload = PendingLoginPayload {
        state: state.to_string(),
        nonce: nonce.to_string(),
        pkce_verifier: pkce_verifier.to_string(),
        redirect_uri: redirect_uri.to_string(),
        issued_at: chrono::Utc::now().timestamp(),
    };
    let json = serde_json::to_vec(&payload)
        .map_err(|e| anyhow::anyhow!("failed to serialize OIDC pending-login payload: {e}"))?;
    let encoded_payload = URL_SAFE_NO_PAD.encode(json);
    let key = derive_pending_cookie_key(client_secret);
    let mut mac = HmacSha256::new_from_slice(&key)
        .map_err(|e| anyhow::anyhow!("failed to construct pending-login HMAC: {e}"))?;
    mac.update(encoded_payload.as_bytes());
    let tag = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
    Ok(format!("{encoded_payload}.{tag}"))
}

/// Verify and decode a pending-login cookie value: checks the HMAC tag
/// (constant-time) and the independent server-side TTL before trusting the
/// payload. `None` on any failure — the caller collapses every failure mode
/// into one generic response (never discloses which check failed).
pub fn verify_pending_login(client_secret: &str, cookie_value: &str) -> Option<PendingLogin> {
    let (encoded_payload, encoded_tag) = cookie_value.split_once('.')?;
    let key = derive_pending_cookie_key(client_secret);
    let mut mac = HmacSha256::new_from_slice(&key).ok()?;
    mac.update(encoded_payload.as_bytes());
    let tag = URL_SAFE_NO_PAD.decode(encoded_tag).ok()?;
    mac.verify_slice(&tag).ok()?;

    let json = URL_SAFE_NO_PAD.decode(encoded_payload).ok()?;
    let payload: PendingLoginPayload = serde_json::from_slice(&json).ok()?;
    let age = chrono::Utc::now().timestamp() - payload.issued_at;
    if !(0..=PENDING_COOKIE_TTL_SECS).contains(&age) {
        return None;
    }
    Some(PendingLogin {
        state: payload.state,
        nonce: Nonce::new(payload.nonce),
        pkce_verifier: PkceCodeVerifier::new(payload.pkce_verifier),
        redirect_uri: payload.redirect_uri,
    })
}

/// Wraps `openidconnect::reqwest::Client` so every outbound call the OIDC RP
/// flow makes (discovery, JWKS, token exchange) opens a boundary span
/// through [`http_client_span`] — the only outbound HTTP calls this module
/// makes all go through one `call()`, so this is the single wrapper needed.
pub struct TracedHttpClient(openidconnect::reqwest::Client);

impl TracedHttpClient {
    pub fn new() -> Result<Self, openidconnect::reqwest::Error> {
        let client = openidconnect::reqwest::ClientBuilder::new()
            // Following redirects on an IdP response opens the client up to
            // SSRF (the crate's own docs warn about this).
            .redirect(openidconnect::reqwest::redirect::Policy::none())
            .build()?;
        Ok(Self(client))
    }
}

impl<'c> AsyncHttpClient<'c> for TracedHttpClient {
    type Error = <openidconnect::reqwest::Client as AsyncHttpClient<'c>>::Error;
    type Future =
        Pin<Box<dyn Future<Output = Result<HttpResponse, Self::Error>> + Send + Sync + 'c>>;

    fn call(&'c self, request: HttpRequest) -> Self::Future {
        let method = request.method().to_string();
        let url = request.uri().to_string();
        Box::pin(async move {
            let span = http_client_span(&method, &url);
            let result = self.0.call(request).instrument(span.clone()).await;
            match &result {
                Ok(response) => record_http_client_result(&span, response.status().as_u16()),
                Err(_) => record_span_error(&span, "transport_error"),
            }
            result
        })
    }
}

/// A discovered, ready-to-use provider: the OAuth2/OIDC client built from
/// discovery, plus a JWKS cache kept independent of the client so a rotation
/// (task 2.7 — unknown `kid`) can refresh it without rebuilding the client.
pub struct DiscoveredProvider {
    client: DiscoveredClient,
    issuer: IssuerUrl,
    client_id: ClientId,
    client_secret: ClientSecret,
    jwks_uri: JsonWebKeySetUrl,
    jwks: RwLock<CoreJsonWebKeySet>,
}

/// The verified identity a callback resolves to, extracted from the ID
/// token's claims immediately after verification (never held as a borrow
/// into the token, so the verifier and JWKS lock don't need to outlive it).
pub struct VerifiedIdentity {
    pub subject: String,
    pub email: Option<String>,
    pub email_verified: bool,
    pub name: Option<String>,
}

/// The parts of a fresh authorization request the start endpoint needs:
/// where to redirect the browser, and what to sign into the pending cookie.
pub struct AuthorizationStart {
    pub authorization_url: url::Url,
    pub state: CsrfToken,
    pub nonce: Nonce,
    pub pkce_verifier: PkceCodeVerifier,
}

/// Why an ID-token exchange/verification failed. Logged server-side only —
/// every variant maps to the same generic client-facing failure (spec: "SHALL
/// be rejected ... without revealing which check failed").
#[derive(Debug, thiserror::Error)]
pub enum ExchangeError {
    #[error("invalid redirect_uri")]
    InvalidRedirectUri,
    #[error("token endpoint not configured by discovery")]
    NotConfigured,
    #[error("token exchange failed: {0}")]
    TokenRequest(String),
    #[error("token response carried no ID token")]
    NoIdToken,
    #[error("ID token claims verification failed: {0}")]
    Claims(String),
    #[error("JWKS refetch failed: {0}")]
    JwksRefetch(String),
}

impl DiscoveredProvider {
    async fn discover(config: &OidcConfig, http_client: &TracedHttpClient) -> anyhow::Result<Self> {
        let issuer = IssuerUrl::new(config.issuer_url.clone())
            .map_err(|e| anyhow::anyhow!("invalid issuer_url: {e}"))?;
        let metadata = CoreProviderMetadata::discover_async(issuer.clone(), http_client)
            .await
            .map_err(|e| anyhow::anyhow!("OIDC discovery failed: {e}"))?;
        let jwks_uri = metadata.jwks_uri().clone();
        let jwks = metadata.jwks().clone();
        let client_id = ClientId::new(config.client_id.clone());
        let client_secret = ClientSecret::new(config.client_secret.clone());
        let client = CoreClient::from_provider_metadata(
            metadata,
            client_id.clone(),
            Some(client_secret.clone()),
        );
        Ok(Self {
            client,
            issuer,
            client_id,
            client_secret,
            jwks_uri,
            jwks: RwLock::new(jwks),
        })
    }

    /// Begin an authorization-code + PKCE request (task 2.3/2.4): generates
    /// state, nonce, and a fresh PKCE pair, and binds the redirect URI the
    /// caller derived from the request (or the config override) into both
    /// the authorization URL and the value the callback must reuse for the
    /// token exchange.
    pub fn begin_authorization(&self, redirect_uri: &str) -> anyhow::Result<AuthorizationStart> {
        let redirect_url = RedirectUrl::new(redirect_uri.to_string())
            .map_err(|e| anyhow::anyhow!("invalid redirect_uri: {e}"))?;
        let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();
        let (authorization_url, state, nonce) = self
            .client
            .clone()
            .set_redirect_uri(redirect_url)
            .authorize_url(
                CoreAuthenticationFlow::AuthorizationCode,
                CsrfToken::new_random,
                Nonce::new_random,
            )
            .add_scope(Scope::new("email".to_string()))
            .add_scope(Scope::new("profile".to_string()))
            .set_pkce_challenge(pkce_challenge)
            .url();
        Ok(AuthorizationStart {
            authorization_url,
            state,
            nonce,
            pkce_verifier,
        })
    }

    /// Exchange the authorization code and verify the returned ID token
    /// (task 2.5/2.6): signature against the cached JWKS (refetching once on
    /// an unknown `kid`, task 2.7), issuer, audience, expiry, and nonce —
    /// `openidconnect` enforces all of these; the default ~5 minute clock
    /// leeway is accepted as-is (design Risks section).
    pub async fn exchange_and_verify(
        &self,
        code: String,
        redirect_uri: &str,
        pkce_verifier: PkceCodeVerifier,
        nonce: &Nonce,
        http_client: &TracedHttpClient,
    ) -> Result<VerifiedIdentity, ExchangeError> {
        let redirect_url = RedirectUrl::new(redirect_uri.to_string())
            .map_err(|_| ExchangeError::InvalidRedirectUri)?;
        let client = self.client.clone().set_redirect_uri(redirect_url);
        let request = client
            .exchange_code(AuthorizationCode::new(code))
            .map_err(|_| ExchangeError::NotConfigured)?
            .set_pkce_verifier(pkce_verifier);
        let token_response = request
            .request_async(http_client)
            .await
            .map_err(|e| ExchangeError::TokenRequest(e.to_string()))?;
        let id_token = token_response.id_token().ok_or(ExchangeError::NoIdToken)?;
        self.verify(id_token, nonce, http_client).await
    }

    async fn verify(
        &self,
        id_token: &CoreIdToken,
        nonce: &Nonce,
        http_client: &TracedHttpClient,
    ) -> Result<VerifiedIdentity, ExchangeError> {
        let verifier = self.build_verifier().await;
        match id_token.claims(&verifier, nonce) {
            Ok(claims) => return Ok(extract_identity(claims)),
            Err(ClaimsVerificationError::SignatureVerification(
                SignatureVerificationError::NoMatchingKey,
            )) => {
                // Key rotation mid-flight (task 2.7): refetch JWKS once and
                // retry with the fresh set before giving up.
            }
            Err(e) => return Err(ExchangeError::Claims(e.to_string())),
        }

        let fresh = JsonWebKeySet::fetch_async(&self.jwks_uri, http_client)
            .await
            .map_err(|e| ExchangeError::JwksRefetch(e.to_string()))?;
        *self.jwks.write().await = fresh;
        let verifier = self.build_verifier().await;
        id_token
            .claims(&verifier, nonce)
            .map(extract_identity)
            .map_err(|e| ExchangeError::Claims(e.to_string()))
    }

    async fn build_verifier(&self) -> CoreIdTokenVerifier<'static> {
        let jwks = self.jwks.read().await.clone();
        IdTokenVerifier::new_confidential_client(
            self.client_id.clone(),
            self.client_secret.clone(),
            self.issuer.clone(),
            jwks,
        )
    }
}

fn extract_identity(claims: &CoreIdTokenClaims) -> VerifiedIdentity {
    VerifiedIdentity {
        subject: claims.subject().as_str().to_string(),
        email: claims.email().map(|e| e.as_str().to_string()),
        email_verified: claims.email_verified().unwrap_or(false),
        name: claims
            .name()
            .and_then(|n| n.get(None))
            .map(|n| n.as_str().to_string()),
    }
}

/// Whether SSO can be offered right now.
enum ProviderStatus {
    Available(Arc<DiscoveredProvider>),
    Unavailable,
}

/// Runtime OIDC state attached to the router (change: oidc-login).
/// `RouterAppState` holds `Option<Arc<OidcRuntime>>`: `None` when
/// `[auth.oidc]` is absent, so the endpoints 404 and no background task
/// runs at all.
pub struct OidcRuntime {
    pub issuer_url: String,
    pub display_name: String,
    pub config: OidcConfig,
    status: Arc<RwLock<ProviderStatus>>,
    http_client: Option<Arc<TracedHttpClient>>,
}

/// Derive the cosmetic default display name (issuer host) when
/// `[auth.oidc].display_name` is unset (design Open Questions).
fn issuer_host(issuer_url: &str) -> String {
    url::Url::parse(issuer_url)
        .ok()
        .and_then(|u| u.host_str().map(str::to_string))
        .unwrap_or_else(|| issuer_url.to_string())
}

impl OidcRuntime {
    /// Spawn the runtime with production backoff (starts at 1s, caps at 5
    /// minutes — design decision 10). Discovery itself runs entirely in the
    /// background, so this never blocks startup: the instance is up whether
    /// or not the issuer answers.
    pub fn spawn(config: OidcConfig) -> Arc<Self> {
        Self::spawn_with_backoff(config, Duration::from_secs(1), Duration::from_secs(300))
    }

    /// Test seam: a short backoff so "discovery recovers without a restart"
    /// tests don't wait minutes.
    pub fn spawn_with_backoff(
        config: OidcConfig,
        initial_delay: Duration,
        max_delay: Duration,
    ) -> Arc<Self> {
        let issuer_url = config.issuer_url.clone();
        let display_name = config
            .display_name
            .clone()
            .unwrap_or_else(|| issuer_host(&issuer_url));
        let status = Arc::new(RwLock::new(ProviderStatus::Unavailable));

        let http_client = match TracedHttpClient::new() {
            Ok(client) => Some(Arc::new(client)),
            Err(error) => {
                tracing::error!(
                    issuer = %issuer_url,
                    error = %error,
                    "OIDC HTTP client construction failed; SSO stays unavailable"
                );
                None
            }
        };

        let runtime = Arc::new(Self {
            issuer_url: issuer_url.clone(),
            display_name,
            config: config.clone(),
            status: status.clone(),
            http_client: http_client.clone(),
        });

        if let Some(http_client) = http_client {
            tokio::spawn(async move {
                let mut delay = initial_delay;
                loop {
                    match DiscoveredProvider::discover(&config, &http_client).await {
                        Ok(provider) => {
                            tracing::info!(issuer = %issuer_url, "OIDC discovery succeeded; SSO is available");
                            *status.write().await = ProviderStatus::Available(Arc::new(provider));
                            return;
                        }
                        Err(error) => {
                            tracing::error!(
                                issuer = %issuer_url,
                                error = %error,
                                "OIDC discovery failed; instance continues without SSO, retrying in background"
                            );
                            tokio::time::sleep(delay).await;
                            delay = std::cmp::min(delay.saturating_mul(2), max_delay);
                        }
                    }
                }
            });
        }

        runtime
    }

    /// The discovered provider, or `None` while discovery hasn't (yet)
    /// succeeded.
    pub async fn provider(&self) -> Option<Arc<DiscoveredProvider>> {
        match &*self.status.read().await {
            ProviderStatus::Available(provider) => Some(provider.clone()),
            ProviderStatus::Unavailable => None,
        }
    }

    pub fn http_client(&self) -> Option<&Arc<TracedHttpClient>> {
        self.http_client.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn test_config(issuer_url: String) -> OidcConfig {
        OidcConfig {
            issuer_url,
            client_id: "test-client".to_string(),
            client_secret: "test-secret".to_string(),
            ..OidcConfig::default()
        }
    }

    async fn mount_valid_discovery(server: &MockServer) {
        let discovery = serde_json::json!({
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
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "keys": [],
            })))
            .mount(server)
            .await;
    }

    async fn wait_for<F: Fn() -> bool>(condition: F, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        while tokio::time::Instant::now() < deadline {
            if condition() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        false
    }

    #[tokio::test]
    async fn discovery_success_marks_the_provider_available() {
        let server = MockServer::start().await;
        mount_valid_discovery(&server).await;

        let runtime = OidcRuntime::spawn(test_config(server.uri()));
        let mut available = false;
        for _ in 0..200 {
            if runtime.provider().await.is_some() {
                available = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(available, "discovery should have succeeded");
    }

    #[tokio::test]
    async fn unreachable_issuer_leaves_the_provider_unavailable() {
        // No mocks mounted: every discovery attempt 404s forever, so the
        // instance starts (and stays) without SSO instead of failing to boot.
        let server = MockServer::start().await;
        let runtime = OidcRuntime::spawn(test_config(server.uri()));
        assert!(runtime.provider().await.is_none());
        // Give the background task a moment to make (and fail) an attempt;
        // it must still report unavailable rather than panic or hang.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(runtime.provider().await.is_none());
    }

    #[tokio::test]
    async fn background_retry_flips_to_available_without_a_restart() {
        // Starts unreachable (no mocks registered yet) so the first attempt
        // fails; registering the discovery mocks afterward simulates the
        // issuer coming back up, which a background retry must pick up
        // without anyone restarting the process.
        let server = MockServer::start().await;
        let runtime = OidcRuntime::spawn_with_backoff(
            test_config(server.uri()),
            Duration::from_millis(10),
            Duration::from_millis(50),
        );
        assert!(runtime.provider().await.is_none());

        mount_valid_discovery(&server).await;

        let recovered = wait_for(
            || {
                // `provider()` is async; poll via try_read to keep this
                // closure sync for `wait_for`.
                matches!(
                    runtime
                        .status
                        .try_read()
                        .map(|s| matches!(*s, ProviderStatus::Available(_))),
                    Ok(true)
                )
            },
            Duration::from_secs(2),
        )
        .await;
        assert!(recovered, "background retry should have recovered");
    }

    #[test]
    fn pending_login_round_trips_through_sign_and_verify() {
        let cookie = sign_pending_login(
            "s3cret",
            "state-1",
            "nonce-1",
            "verifier-1",
            "https://signaldb.example.com/ui/session/oidc/callback",
        )
        .unwrap();
        let pending = verify_pending_login("s3cret", &cookie).expect("valid cookie verifies");
        assert_eq!(pending.state, "state-1");
        assert_eq!(pending.nonce.secret(), "nonce-1");
        assert_eq!(pending.pkce_verifier.secret(), "verifier-1");
        assert_eq!(
            pending.redirect_uri,
            "https://signaldb.example.com/ui/session/oidc/callback"
        );
    }

    #[test]
    fn pending_login_rejects_wrong_key() {
        let cookie = sign_pending_login("s3cret", "s", "n", "v", "https://example.com/cb").unwrap();
        assert!(verify_pending_login("different-secret", &cookie).is_none());
    }

    #[test]
    fn pending_login_rejects_tampered_payload() {
        let cookie = sign_pending_login("s3cret", "s", "n", "v", "https://example.com/cb").unwrap();
        let (payload, tag) = cookie.split_once('.').unwrap();
        let mut bytes = URL_SAFE_NO_PAD.decode(payload).unwrap();
        // Flip a byte in the encoded JSON payload without recomputing the tag.
        bytes[0] ^= 0xFF;
        let tampered = format!("{}.{}", URL_SAFE_NO_PAD.encode(bytes), tag);
        assert!(verify_pending_login("s3cret", &tampered).is_none());
    }

    #[test]
    fn pending_login_rejects_malformed_cookie() {
        assert!(verify_pending_login("s3cret", "not-a-valid-cookie").is_none());
        assert!(verify_pending_login("s3cret", "").is_none());
    }

    #[test]
    fn pending_login_rejects_expired_payload() {
        let payload = PendingLoginPayload {
            state: "s".to_string(),
            nonce: "n".to_string(),
            pkce_verifier: "v".to_string(),
            redirect_uri: "https://example.com/cb".to_string(),
            issued_at: chrono::Utc::now().timestamp() - (PENDING_COOKIE_TTL_SECS + 30),
        };
        let json = serde_json::to_vec(&payload).unwrap();
        let encoded_payload = URL_SAFE_NO_PAD.encode(json);
        let key = derive_pending_cookie_key("s3cret");
        let mut mac = HmacSha256::new_from_slice(&key).unwrap();
        mac.update(encoded_payload.as_bytes());
        let tag = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
        let cookie = format!("{encoded_payload}.{tag}");

        assert!(verify_pending_login("s3cret", &cookie).is_none());
    }

    #[test]
    fn issuer_host_extracts_hostname() {
        assert_eq!(
            issuer_host("https://idp.example.com/realms/signaldb"),
            "idp.example.com"
        );
    }
}
