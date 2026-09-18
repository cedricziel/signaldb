//! Outbound GitHub client (change: `github-app-source-context`).
//!
//! This module is the SignalDB-side of the GitHub App integration: it mints
//! GitHub App JWTs, exchanges them for short-lived installation access
//! tokens (with an in-process cache), and exchanges an OAuth-on-install
//! `code` for a user-to-server token used only to verify that a callback's
//! `installation_id` actually belongs to the authorizing GitHub user. Every
//! outbound call this module makes is a boundary surface and is traced via
//! [`common::self_monitoring::spans::http_client_span`] — see
//! [`GitHubApp::send`] for the single call site.
//!
//! No token this module mints or exchanges is ever written to durable
//! storage: installation tokens live only in the in-process
//! [`GitHubApp::tokens`] cache (keyed by installation id, evicted on expiry
//! or [`GitHubApp::forget_installation`]), and a [`UserToken`] lives only for
//! the duration of a single link-completion callback.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::Deserialize;
use tokio::sync::Mutex;
use tracing::Instrument;

use common::config::GitHubAppConfig;
use common::self_monitoring::spans::{
    http_client_span, record_http_client_result, record_span_error,
};

/// A page size GitHub accepts for any paginated list endpoint used here.
const PER_PAGE: u32 = 100;
/// A hard stop on pagination so a misbehaving mock or an unbounded GitHub
/// response can never spin this client forever.
const MAX_PAGES: u32 = 20;
/// Reuse a cached installation token while it still has at least this much
/// life left; otherwise mint a fresh one.
const TOKEN_REFRESH_MARGIN: chrono::Duration = chrono::Duration::minutes(5);
/// Clock-skew leeway subtracted from the app JWT's `iat`.
const JWT_CLOCK_SKEW: i64 = 60;
/// The app JWT's lifetime; GitHub caps this at 10 minutes.
const JWT_LIFETIME_SECS: i64 = 9 * 60;

/// Everything that can go wrong talking to GitHub.
#[derive(Debug, thiserror::Error)]
pub enum GitHubError {
    /// The `[github]` configuration itself is unusable (e.g. a private key
    /// that doesn't parse as PEM). GitHub's reachability is a separate,
    /// request-time concern — this variant is only for configuration that
    /// could never work.
    #[error("GitHub App configuration is invalid: {0}")]
    Config(String),
    /// The HTTP request itself failed (DNS, TLS, connection reset, timeout)
    /// before a response was received.
    #[error("GitHub request failed: {0}")]
    Transport(#[from] reqwest::Error),
    /// GitHub answered with a non-2xx status.
    #[error("GitHub {endpoint} answered {status}: {message}")]
    Status {
        /// The HTTP status code.
        status: u16,
        /// The endpoint that was called, for diagnosis.
        endpoint: String,
        /// The first ~200 characters of the response body's `message`
        /// field, or of the raw body when it carries no such field.
        message: String,
    },
    /// The response body didn't parse the way this client expects.
    #[error("GitHub response was malformed: {0}")]
    Malformed(String),
    /// Signing the GitHub App JWT failed.
    #[error("failed to sign the GitHub App JWT: {0}")]
    Jwt(#[from] jsonwebtoken::errors::Error),
}

/// One installation as GitHub's `GET /user/installations` or
/// `GET /app/installations/{id}` reports it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct InstallationSummary {
    /// The installation id.
    pub id: i64,
    /// The GitHub App id this installation belongs to.
    pub app_id: i64,
    /// The org or user account the installation was created on.
    pub account: InstallationAccount,
    /// Permission name -> level (`"read"`, `"write"`, `"admin"`) granted to
    /// the installation.
    #[serde(default)]
    pub permissions: BTreeMap<String, String>,
    /// `"all"` or `"selected"`, per GitHub's API.
    #[serde(default)]
    pub repository_selection: Option<String>,
}

/// The org or user account an installation lives on.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct InstallationAccount {
    /// The account's login (org or user name).
    pub login: String,
    /// The account's numeric id.
    pub id: i64,
    /// `"Organization"` or `"User"`.
    #[serde(rename = "type")]
    pub kind: String,
}

/// A GitHub user, as `GET /user` reports it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct GitHubUser {
    /// The user's login.
    pub login: String,
    /// The user's numeric id.
    pub id: i64,
}

/// A user-to-server token produced by exchanging an OAuth-on-install `code`.
/// It exists only to verify installation ownership during a single
/// link-completion callback and is never persisted. `Debug` redacts the
/// secret value so it can't leak via incidental logging.
pub struct UserToken(String);

impl UserToken {
    /// The bearer token value, for use in an `Authorization` header.
    pub fn secret(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for UserToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("UserToken").field(&"[redacted]").finish()
    }
}

/// A cached installation access token plus its declared expiry.
struct CachedInstallationToken {
    token: String,
    expires_at: DateTime<Utc>,
}

/// The GitHub App client: mints app JWTs, mints and caches installation
/// tokens, and exchanges OAuth-on-install codes for user tokens.
pub struct GitHubApp {
    config: GitHubAppConfig,
    http: reqwest::Client,
    signing_key: jsonwebtoken::EncodingKey,
    tokens: Mutex<HashMap<i64, CachedInstallationToken>>,
}

impl std::fmt::Debug for GitHubApp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GitHubApp")
            .field("app_id", &self.config.app_id)
            .field("app_slug", &self.config.app_slug)
            .field("api_url", &self.config.api_url)
            .field("web_url", &self.config.web_url)
            .finish()
    }
}

/// The response shape of `POST /app/installations/{id}/access_tokens`.
#[derive(Deserialize)]
struct AccessTokenResponse {
    token: String,
    expires_at: DateTime<Utc>,
}

/// The response shape of `POST {web}/login/oauth/access_token` requested
/// with `Accept: application/json`, covering both the success and the
/// error-carrying-200 shapes GitHub uses on this endpoint.
#[derive(Deserialize)]
struct OAuthTokenResponse {
    #[serde(default)]
    access_token: Option<String>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    error_description: Option<String>,
}

/// A page of `GET /user/installations`.
#[derive(Deserialize)]
struct InstallationsPage {
    installations: Vec<InstallationSummary>,
}

/// One repository entry as `GET /installation/repositories` reports it.
#[derive(Deserialize)]
struct RepositoryEntry {
    full_name: String,
}

/// A page of `GET /installation/repositories`.
#[derive(Deserialize)]
struct RepositoriesPage {
    repositories: Vec<RepositoryEntry>,
}

/// The subset of GitHub's error body this client extracts a message from.
#[derive(Deserialize)]
struct ErrorBody {
    #[serde(default)]
    message: Option<String>,
}

impl GitHubApp {
    /// Build a client from a validated `[github]` configuration. Fails hard
    /// on an unreadable or unparsable private key — bad configuration stops
    /// startup; GitHub's own reachability is a request-time concern this
    /// constructor never touches.
    pub fn new(config: GitHubAppConfig) -> Result<Self, GitHubError> {
        let pem = config.private_key_pem().map_err(GitHubError::Config)?;
        let signing_key = jsonwebtoken::EncodingKey::from_rsa_pem(pem.as_bytes())
            .map_err(|e| GitHubError::Config(format!("invalid GitHub App private key: {e}")))?;
        // Headers every GitHub REST call carries; a request that sets its
        // own `Accept` (the OAuth token endpoint wants `application/json`)
        // overrides the default.
        let mut default_headers = reqwest::header::HeaderMap::new();
        default_headers.insert(
            reqwest::header::ACCEPT,
            reqwest::header::HeaderValue::from_static("application/vnd.github+json"),
        );
        default_headers.insert(
            reqwest::header::HeaderName::from_static("x-github-api-version"),
            reqwest::header::HeaderValue::from_static("2022-11-28"),
        );
        let http = reqwest::Client::builder()
            .default_headers(default_headers)
            .user_agent("signaldb")
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(15))
            .build()
            .map_err(|e| GitHubError::Config(format!("failed to build HTTP client: {e}")))?;
        Ok(Self {
            config,
            http,
            signing_key,
            tokens: Mutex::new(HashMap::new()),
        })
    }

    /// The configuration this client was built from.
    pub fn config(&self) -> &GitHubAppConfig {
        &self.config
    }

    /// Sign an RS256 app-level JWT: `iss` is the app id (GitHub also accepts
    /// the client id here), `iat` is backdated 60s for clock skew, and `exp`
    /// is 9 minutes out (GitHub caps the lifetime at 10).
    fn app_jwt(&self) -> Result<String, GitHubError> {
        #[derive(serde::Serialize)]
        struct Claims {
            iss: String,
            iat: i64,
            exp: i64,
        }
        let now = Utc::now().timestamp();
        let claims = Claims {
            iss: self.config.app_id.to_string(),
            iat: now - JWT_CLOCK_SKEW,
            exp: now + JWT_LIFETIME_SECS,
        };
        let header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
        let token = jsonwebtoken::encode(&header, &claims, &self.signing_key)?;
        Ok(token)
    }

    /// Send a request behind a client boundary span and turn a non-2xx
    /// response into [`GitHubError::Status`]. The single outbound call site
    /// this module funnels every request through.
    async fn send(
        &self,
        request: reqwest::RequestBuilder,
        method: &str,
        url: &str,
    ) -> Result<reqwest::Response, GitHubError> {
        let request = request.build()?;
        let span = http_client_span(method, url);
        let result = self.http.execute(request).instrument(span.clone()).await;
        let response = match result {
            Ok(response) => {
                record_http_client_result(&span, response.status().as_u16());
                response
            }
            Err(e) => {
                record_span_error(&span, "transport_error");
                return Err(GitHubError::Transport(e));
            }
        };

        if response.status().is_success() {
            return Ok(response);
        }
        let status = response.status().as_u16();
        let body = response.text().await.unwrap_or_default();
        let message = serde_json::from_str::<ErrorBody>(&body)
            .ok()
            .and_then(|b| b.message)
            .unwrap_or(body);
        let message: String = message.chars().take(200).collect();
        Err(GitHubError::Status {
            status,
            endpoint: url.to_string(),
            message,
        })
    }

    /// [`Self::send`] plus JSON decoding of the body, mapping a body that
    /// does not match `T` to [`GitHubError::Malformed`].
    async fn send_json<T: serde::de::DeserializeOwned>(
        &self,
        request: reqwest::RequestBuilder,
        method: &str,
        url: &str,
    ) -> Result<T, GitHubError> {
        self.send(request, method, url)
            .await?
            .json()
            .await
            .map_err(|e| GitHubError::Malformed(e.to_string()))
    }

    /// Walk a paginated GitHub list endpoint (`per_page`/`page` query
    /// parameters) with `bearer`, extracting each page's items via
    /// `items`, until a short page or [`MAX_PAGES`].
    async fn paginate<P: serde::de::DeserializeOwned, T>(
        &self,
        path: &str,
        bearer: &str,
        items: fn(P) -> Vec<T>,
    ) -> Result<Vec<T>, GitHubError> {
        let mut all = Vec::new();
        for page in 1..=MAX_PAGES {
            let url = format!(
                "{}{path}?per_page={PER_PAGE}&page={page}",
                self.config.api_base()
            );
            let request = self.http.get(&url).bearer_auth(bearer);
            let page_items = items(self.send_json::<P>(request, "GET", &url).await?);
            let count = page_items.len();
            all.extend(page_items);
            if count < PER_PAGE as usize {
                break;
            }
        }
        Ok(all)
    }

    /// Mint or reuse a cached installation access token. A cached token is
    /// reused while it has more than [`TOKEN_REFRESH_MARGIN`] left; otherwise
    /// a fresh one is minted via `POST /app/installations/{id}/access_tokens`
    /// and cached. Concurrent calls for the same installation may double
    /// mint; that's acceptable (GitHub allows several live tokens).
    pub async fn installation_token(&self, installation_id: i64) -> Result<String, GitHubError> {
        {
            let cache = self.tokens.lock().await;
            if let Some(cached) = cache.get(&installation_id)
                && cached.expires_at - Utc::now() > TOKEN_REFRESH_MARGIN
            {
                return Ok(cached.token.clone());
            }
        }

        let jwt = self.app_jwt()?;
        let url = format!(
            "{}/app/installations/{installation_id}/access_tokens",
            self.config.api_base()
        );
        let request = self.http.post(&url).bearer_auth(&jwt);
        let parsed: AccessTokenResponse = self.send_json(request, "POST", &url).await?;

        let mut cache = self.tokens.lock().await;
        cache.insert(
            installation_id,
            CachedInstallationToken {
                token: parsed.token.clone(),
                expires_at: parsed.expires_at,
            },
        );
        Ok(parsed.token)
    }

    /// Drop a cached installation token, so a removed installation link is
    /// never served a stale cached token again (spec: removal takes effect
    /// immediately).
    pub async fn forget_installation(&self, installation_id: i64) {
        self.tokens.lock().await.remove(&installation_id);
    }

    /// `GET /app/installations/{id}` with the app JWT. A 404 (installation
    /// revoked or never existed) surfaces as `GitHubError::Status { status:
    /// 404, .. }`.
    pub async fn installation(
        &self,
        installation_id: i64,
    ) -> Result<InstallationSummary, GitHubError> {
        let jwt = self.app_jwt()?;
        let url = format!(
            "{}/app/installations/{installation_id}",
            self.config.api_base()
        );
        let request = self.http.get(&url).bearer_auth(&jwt);
        self.send_json(request, "GET", &url).await
    }

    /// `GET /installation/repositories`, paginated with the installation's
    /// access token, returning the sorted, deduplicated set of covered
    /// repository full names.
    pub async fn installation_repositories(
        &self,
        installation_id: i64,
    ) -> Result<Vec<String>, GitHubError> {
        let token = self.installation_token(installation_id).await?;
        let names: std::collections::BTreeSet<String> = self
            .paginate(
                "/installation/repositories",
                &token,
                |page: RepositoriesPage| {
                    page.repositories.into_iter().map(|r| r.full_name).collect()
                },
            )
            .await?
            .into_iter()
            .collect();
        Ok(names.into_iter().collect())
    }

    /// Exchange an OAuth-on-install callback `code` for a user-to-server
    /// token. GitHub answers this endpoint with HTTP 200 even on failure,
    /// carrying an `error`/`error_description` pair instead of a token; that
    /// shape maps to `GitHubError::Status { status: 400, .. }`.
    pub async fn exchange_user_code(&self, code: &str) -> Result<UserToken, GitHubError> {
        let url = format!("{}/login/oauth/access_token", self.config.web_base());
        let form = [
            ("client_id", self.config.client_id.as_str()),
            ("client_secret", self.config.client_secret.as_str()),
            ("code", code),
        ];
        let request = self
            .http
            .post(&url)
            .header("Accept", "application/json")
            .form(&form);
        let parsed: OAuthTokenResponse = self.send_json(request, "POST", &url).await?;
        if let Some(token) = parsed.access_token {
            return Ok(UserToken(token));
        }
        let message = parsed
            .error_description
            .or(parsed.error)
            .unwrap_or_else(|| "GitHub did not return an access token".to_string());
        Err(GitHubError::Status {
            status: 400,
            endpoint: url,
            message,
        })
    }

    /// `GET /user` with the user token.
    pub async fn user(&self, token: &UserToken) -> Result<GitHubUser, GitHubError> {
        let url = format!("{}/user", self.config.api_base());
        let request = self.http.get(&url).bearer_auth(token.secret());
        self.send_json(request, "GET", &url).await
    }

    /// `GET /user/installations`, paginated with the user token, returning
    /// the `installations` array across all pages.
    pub async fn user_installations(
        &self,
        token: &UserToken,
    ) -> Result<Vec<InstallationSummary>, GitHubError> {
        self.paginate(
            "/user/installations",
            token.secret(),
            |page: InstallationsPage| page.installations,
        )
        .await
    }
}

/// Permission names carried at a write-capable level (`write` or `admin`).
/// The linking flow refuses an installation with any such permission (spec:
/// only `contents:read` and `metadata:read` are expected).
pub fn write_permissions(permissions: &BTreeMap<String, String>) -> Vec<String> {
    permissions
        .iter()
        .filter(|(_, level)| level.as_str() == "write" || level.as_str() == "admin")
        .map(|(name, _)| name.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsa::RsaPrivateKey;
    use rsa::pkcs1::DecodeRsaPrivateKey;
    use rsa::pkcs8::{EncodePublicKey, LineEnding};
    use serde_json::json;
    use wiremock::matchers::{body_string_contains, header, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use common::testing::GITHUB_TEST_PEM as TEST_PEM;

    const GARBAGE_PEM: &str =
        "-----BEGIN RSA PRIVATE KEY-----\nnot-a-key\n-----END RSA PRIVATE KEY-----";

    /// The public key matching [`TEST_PEM`], PEM-encoded, for verifying
    /// signed JWTs in tests.
    fn test_public_key_pem() -> String {
        let private = RsaPrivateKey::from_pkcs1_pem(TEST_PEM).expect("valid test PEM");
        private
            .to_public_key()
            .to_public_key_pem(LineEnding::LF)
            .expect("public key encodes to PEM")
    }

    fn test_config(server: &MockServer) -> GitHubAppConfig {
        GitHubAppConfig {
            app_id: 12345,
            app_slug: "test-app".to_string(),
            private_key: TEST_PEM.to_string(),
            client_id: "test-client-id".to_string(),
            client_secret: "test-client-secret".to_string(),
            api_url: server.uri(),
            web_url: server.uri(),
            ..GitHubAppConfig::default()
        }
    }

    fn decode_jwt(token: &str, app_id: u64) -> jsonwebtoken::TokenData<serde_json::Value> {
        let key = jsonwebtoken::DecodingKey::from_rsa_pem(test_public_key_pem().as_bytes())
            .expect("valid public key");
        let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::RS256);
        validation.set_issuer(&[app_id.to_string()]);
        jsonwebtoken::decode::<serde_json::Value>(token, &key, &validation)
            .expect("JWT verifies against the test key")
    }

    #[tokio::test]
    async fn app_jwt_signs_rs256_with_expected_claims() {
        let server = MockServer::start().await;
        let app = GitHubApp::new(test_config(&server)).expect("valid config");

        let before = Utc::now().timestamp();
        let token = app.app_jwt().expect("signs");
        let data = decode_jwt(&token, app.config().app_id);

        assert_eq!(data.claims["iss"], "12345");
        let iat = data.claims["iat"].as_i64().expect("iat is a number");
        let exp = data.claims["exp"].as_i64().expect("exp is a number");
        assert!((before - JWT_CLOCK_SKEW - 2..=before - JWT_CLOCK_SKEW + 2).contains(&iat));
        assert!((before + JWT_LIFETIME_SECS - 2..=before + JWT_LIFETIME_SECS + 2).contains(&exp));
    }

    #[test]
    fn new_fails_with_config_error_on_garbage_pem() {
        let config = GitHubAppConfig {
            app_id: 1,
            app_slug: "x".to_string(),
            private_key: GARBAGE_PEM.to_string(),
            client_id: "id".to_string(),
            client_secret: "secret".to_string(),
            ..GitHubAppConfig::default()
        };
        let result = GitHubApp::new(config);
        assert!(matches!(result, Err(GitHubError::Config(_))));
    }

    #[tokio::test]
    async fn installation_token_mints_via_mocked_endpoint() {
        let server = MockServer::start().await;
        let app = GitHubApp::new(test_config(&server)).expect("valid config");

        Mock::given(method("POST"))
            .and(path("/app/installations/42/access_tokens"))
            .respond_with(|req: &wiremock::Request| {
                let auth = req
                    .headers
                    .get("Authorization")
                    .expect("Authorization header present")
                    .to_str()
                    .expect("valid header value");
                assert!(auth.starts_with("Bearer "));
                ResponseTemplate::new(201).set_body_json(json!({
                    "token": "ghs_minted_token",
                    "expires_at": (Utc::now() + chrono::Duration::hours(1)).to_rfc3339(),
                }))
            })
            .expect(1)
            .mount(&server)
            .await;

        let token = app.installation_token(42).await.expect("mints a token");
        assert_eq!(token, "ghs_minted_token");
    }

    #[tokio::test]
    async fn installation_token_is_cached_when_far_from_expiry() {
        let server = MockServer::start().await;
        let app = GitHubApp::new(test_config(&server)).expect("valid config");

        Mock::given(method("POST"))
            .and(path("/app/installations/42/access_tokens"))
            .respond_with(ResponseTemplate::new(201).set_body_json(json!({
                "token": "ghs_long_lived",
                "expires_at": (Utc::now() + chrono::Duration::hours(1)).to_rfc3339(),
            })))
            .expect(1)
            .mount(&server)
            .await;

        let first = app.installation_token(42).await.expect("mints");
        let second = app.installation_token(42).await.expect("reuses cache");
        assert_eq!(first, second);
    }

    #[tokio::test]
    async fn installation_token_is_reminted_when_close_to_expiry() {
        let server = MockServer::start().await;
        let app = GitHubApp::new(test_config(&server)).expect("valid config");

        Mock::given(method("POST"))
            .and(path("/app/installations/42/access_tokens"))
            .respond_with(ResponseTemplate::new(201).set_body_json(json!({
                "token": "ghs_short_lived",
                "expires_at": (Utc::now() + chrono::Duration::minutes(4)).to_rfc3339(),
            })))
            .expect(2)
            .mount(&server)
            .await;

        app.installation_token(42).await.expect("mints");
        app.installation_token(42).await.expect("re-mints");
    }

    #[tokio::test]
    async fn forget_installation_forces_a_remint() {
        let server = MockServer::start().await;
        let app = GitHubApp::new(test_config(&server)).expect("valid config");

        Mock::given(method("POST"))
            .and(path("/app/installations/42/access_tokens"))
            .respond_with(ResponseTemplate::new(201).set_body_json(json!({
                "token": "ghs_token",
                "expires_at": (Utc::now() + chrono::Duration::hours(1)).to_rfc3339(),
            })))
            .expect(2)
            .mount(&server)
            .await;

        app.installation_token(42).await.expect("mints");
        app.forget_installation(42).await;
        app.installation_token(42)
            .await
            .expect("re-mints after forget");
    }

    fn repo_page(names: &[&str]) -> serde_json::Value {
        json!({
            "repositories": names
                .iter()
                .map(|n| json!({ "full_name": n }))
                .collect::<Vec<_>>(),
        })
    }

    #[tokio::test]
    async fn installation_repositories_paginates_sorts_and_dedupes() {
        let server = MockServer::start().await;
        let app = GitHubApp::new(test_config(&server)).expect("valid config");

        Mock::given(method("POST"))
            .and(path("/app/installations/42/access_tokens"))
            .respond_with(ResponseTemplate::new(201).set_body_json(json!({
                "token": "ghs_repo_token",
                "expires_at": (Utc::now() + chrono::Duration::hours(1)).to_rfc3339(),
            })))
            .mount(&server)
            .await;

        let page1_names: Vec<String> = (0..100).map(|i| format!("org/repo-{i:03}")).collect();
        let page1_refs: Vec<&str> = page1_names.iter().map(String::as_str).collect();

        Mock::given(method("GET"))
            .and(path("/installation/repositories"))
            .and(query_param("page", "1"))
            .and(header("Authorization", "Bearer ghs_repo_token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(repo_page(&page1_refs)))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/installation/repositories"))
            .and(query_param("page", "2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(repo_page(&[
                "org/repo-a",
                "org/repo-b",
                "org/repo-a",
            ])))
            .expect(1)
            .mount(&server)
            .await;

        let repos = app.installation_repositories(42).await.expect("paginates");
        assert_eq!(repos.len(), 102);
        assert!(repos.windows(2).all(|w| w[0] <= w[1]));
        assert_eq!(repos.iter().filter(|r| *r == "org/repo-a").count(), 1);
    }

    #[tokio::test]
    async fn exchange_user_code_succeeds() {
        let server = MockServer::start().await;
        let app = GitHubApp::new(test_config(&server)).expect("valid config");

        Mock::given(method("POST"))
            .and(path("/login/oauth/access_token"))
            .and(header("Accept", "application/json"))
            .and(body_string_contains("client_id=test-client-id"))
            .and(body_string_contains("client_secret=test-client-secret"))
            .and(body_string_contains("code=the-code"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "access_token": "ghu_user_token",
                "token_type": "bearer",
            })))
            .expect(1)
            .mount(&server)
            .await;

        let token = app.exchange_user_code("the-code").await.expect("exchanges");
        assert_eq!(token.secret(), "ghu_user_token");
    }

    #[tokio::test]
    async fn exchange_user_code_maps_200_error_body_to_status_400() {
        let server = MockServer::start().await;
        let app = GitHubApp::new(test_config(&server)).expect("valid config");

        Mock::given(method("POST"))
            .and(path("/login/oauth/access_token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "error": "bad_verification_code",
                "error_description": "The code passed is incorrect or expired.",
            })))
            .mount(&server)
            .await;

        let err = app
            .exchange_user_code("bad-code")
            .await
            .expect_err("maps to an error");
        match err {
            GitHubError::Status {
                status, message, ..
            } => {
                assert_eq!(status, 400);
                assert_eq!(message, "The code passed is incorrect or expired.");
            }
            other => panic!("expected Status error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn user_installations_returns_summaries_and_write_permissions_works() {
        let server = MockServer::start().await;
        let app = GitHubApp::new(test_config(&server)).expect("valid config");

        Mock::given(method("GET"))
            .and(path("/user/installations"))
            .and(query_param("page", "1"))
            .and(header("Authorization", "Bearer ghu_token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "installations": [
                    {
                        "id": 7,
                        "app_id": 12345,
                        "account": { "login": "octo-org", "id": 99, "type": "Organization" },
                        "permissions": { "contents": "read", "metadata": "read" },
                        "repository_selection": "selected",
                    }
                ],
            })))
            .mount(&server)
            .await;

        let token = UserToken("ghu_token".to_string());
        let installations = app
            .user_installations(&token)
            .await
            .expect("returns summaries");
        assert_eq!(installations.len(), 1);
        assert_eq!(installations[0].account.login, "octo-org");
        assert_eq!(
            installations[0].permissions.get("contents"),
            Some(&"read".to_string())
        );

        let read_only: BTreeMap<String, String> = [
            ("contents".to_string(), "read".to_string()),
            ("metadata".to_string(), "read".to_string()),
        ]
        .into_iter()
        .collect();
        assert!(write_permissions(&read_only).is_empty());

        let write: BTreeMap<String, String> = [("contents".to_string(), "write".to_string())]
            .into_iter()
            .collect();
        assert_eq!(write_permissions(&write), vec!["contents".to_string()]);

        let admin: BTreeMap<String, String> = [("administration".to_string(), "admin".to_string())]
            .into_iter()
            .collect();
        assert_eq!(
            write_permissions(&admin),
            vec!["administration".to_string()]
        );
    }

    #[tokio::test]
    async fn non_2xx_with_json_message_maps_to_status_error() {
        let server = MockServer::start().await;
        let app = GitHubApp::new(test_config(&server)).expect("valid config");

        Mock::given(method("GET"))
            .and(path("/app/installations/99"))
            .respond_with(ResponseTemplate::new(404).set_body_json(json!({
                "message": "Not Found",
            })))
            .mount(&server)
            .await;

        let err = app.installation(99).await.expect_err("404 maps to error");
        match err {
            GitHubError::Status {
                status, message, ..
            } => {
                assert_eq!(status, 404);
                assert_eq!(message, "Not Found");
            }
            other => panic!("expected Status error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn transport_failure_maps_to_transport_error() {
        // Bind then immediately drop a plain `std::net::TcpListener` to get
        // a port nothing listens on. `std::net`'s `Drop` closes the socket
        // synchronously (unlike dropping a `MockServer`, whose shutdown runs
        // asynchronously and can race a request made right after), so the
        // connection attempt below is guaranteed to be refused.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("binds an ephemeral port");
        let port = listener
            .local_addr()
            .expect("listener has a local addr")
            .port();
        drop(listener);

        let base_server = MockServer::start().await;
        let mut config = test_config(&base_server);
        config.api_url = format!("http://127.0.0.1:{port}");
        let app = GitHubApp::new(config).expect("valid config");

        let err = app.installation(1).await.expect_err("connection refused");
        assert!(matches!(err, GitHubError::Transport(_)));
    }
}
