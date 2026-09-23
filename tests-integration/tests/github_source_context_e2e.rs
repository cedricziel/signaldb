//! End-to-end GitHub App source-context flow (change:
//! `github-app-source-context`, spec task 8.1).
//!
//! `src/router/src/endpoints/github.rs` and
//! `src/router/src/endpoints/source_context.rs` already unit-test each half
//! of this in isolation: the App-install/callback dance against a mocked
//! GitHub API, and the source-context lookup/cache against a catalog seeded
//! with installations directly (bypassing the callback). This test instead
//! drives one router instance through the *whole* flow in order — mint a
//! state token, link an installation via the real browser-redirect callback,
//! fetch source context for a repo the installation covers (twice, to prove
//! the cache), remove the installation, and confirm a subsequent fetch
//! reports unavailable — over the real (in-memory SQLite) catalog rather
//! than a seeded one. It is SDK-free throughout: every step, including the
//! install callback, is a plain HTTP request against `router::create_router`
//! driven with `tower::ServiceExt::oneshot`, matching
//! `tests-integration/tests/tenant_manage_clients.rs` and
//! `tests-integration/tests/oidc_e2e.rs`.

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use base64::Engine;
use chrono::Utc;
use common::auth::hash_password;
use common::catalog::{Catalog, MembershipRole};
use common::config::{ApiKeyConfig, AuthConfig, Configuration, DatasetConfig, TenantConfig};
use common::testing::github_test_config;
use router::{RouterAppState, create_router};
use serde_json::{Value, json};
use tower::ServiceExt;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const TENANT: &str = "acme";
const LEGACY_KEY: &str = "acme-legacy-key";
const ADMIN_EMAIL: &str = "admin@example.com";
const ADMIN_PASSWORD: &str = "correct horse battery staple";
const INSTALLATION_ID: i64 = 777;
const APP_ID: u64 = 4242;
const REPO: &str = "octo-org/api";
const FILE_PATH: &str = "src/handler.rs";

/// A 20-line file (`line1`..`line20`), so a `line: 10, context_lines: 2`
/// request resolves to a deterministic, checkable window.
fn file_text() -> String {
    (1..=20).map(|n| format!("line{n}\n")).collect()
}

fn tenant_config() -> TenantConfig {
    TenantConfig {
        id: TENANT.to_string(),
        slug: TENANT.to_string(),
        name: "Acme Inc".to_string(),
        default_dataset: Some("production".to_string()),
        datasets: vec![DatasetConfig {
            id: "production".to_string(),
            slug: "production".to_string(),
            is_default: true,
            storage: None,
        }],
        api_keys: vec![ApiKeyConfig {
            key: LEGACY_KEY.to_string(),
            name: Some("legacy".to_string()),
        }],
        schema_config: None,
        limits: None,
    }
}

fn github_config(server: &MockServer) -> common::config::GitHubAppConfig {
    github_test_config(&server.uri())
}

/// Mounts every GitHub API mock the flow touches: user-token exchange,
/// `/user`, the authorizing user's installations (one, id 777), an
/// installation access-token mint, the installation's covered repositories,
/// and the Contents API file this test reads.
async fn mount_github_api(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/login/oauth/access_token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "access_token": "gho_x" })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/user"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({ "login": "octocat", "id": 1 })),
        )
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/user/installations"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "installations": [{
                "id": INSTALLATION_ID,
                "app_id": APP_ID,
                "account": { "login": "octo-org", "id": 9, "type": "Organization" },
                "permissions": { "contents": "read", "metadata": "read" },
            }],
        })))
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path(format!(
            "/app/installations/{INSTALLATION_ID}/access_tokens"
        )))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({
            "token": "ghs_x",
            "expires_at": (Utc::now() + chrono::Duration::hours(1)).to_rfc3339(),
        })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/installation/repositories"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "repositories": [{ "full_name": REPO }],
        })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/repos/{REPO}/contents/{FILE_PATH}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "type": "file",
            "encoding": "base64",
            "content": base64::engine::general_purpose::STANDARD.encode(file_text()),
            "sha": "sha-abc",
            "html_url": format!("https://github.com/{REPO}/blob/main/{FILE_PATH}"),
        })))
        // Exactly one contents fetch: the second identical source-context
        // request below must be served from the snippet cache.
        .expect(1)
        .mount(server)
        .await;
}

/// Builds the router with tenant `acme` (a legacy API key, unused here but
/// matching the shape every other test in this crate builds), `[github]`
/// pointed at `server`, and an admin user who is a tenant Admin of `acme`.
async fn build_app(server: &MockServer) -> axum::Router {
    let config = Configuration {
        auth: AuthConfig {
            tenants: vec![tenant_config()],
            ..Default::default()
        },
        github: Some(github_config(server)),
        ..Default::default()
    };
    let catalog = Catalog::new("sqlite::memory:").await.unwrap();
    catalog.sync_config_tenants(&config.auth).await.unwrap();
    let password_hash = hash_password(ADMIN_PASSWORD).unwrap();
    let admin = catalog
        .create_user(ADMIN_EMAIL, Some("Admin"), Some(&password_hash), false)
        .await
        .unwrap();
    catalog
        .upsert_tenant_membership(&admin.id, TENANT, MembershipRole::Admin)
        .await
        .unwrap();
    create_router(RouterAppState::new(catalog, config))
}

/// Logs in as the admin and returns the `signaldb_session=...` cookie.
async fn login(app: &axum::Router) -> String {
    let request = Request::builder()
        .method("POST")
        .uri("/ui/session")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({ "email": ADMIN_EMAIL, "password": ADMIN_PASSWORD, "tenant": TENANT })
                .to_string(),
        ))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let set_cookie = response
        .headers()
        .get(header::SET_COOKIE)
        .expect("Set-Cookie present")
        .to_str()
        .unwrap();
    set_cookie.split(';').next().unwrap().to_string()
}

/// A cookie-authenticated JSON request against `app`, returning the decoded
/// status and body.
async fn call(
    app: &axum::Router,
    method_: axum::http::Method,
    uri: &str,
    cookie: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method(method_)
        .uri(uri)
        .header(header::COOKIE, cookie)
        .header("x-tenant-id", TENANT);
    let request_body = match body {
        Some(body) => {
            builder = builder.header("content-type", "application/json");
            Body::from(body.to_string())
        }
        None => Body::empty(),
    };
    let response = app
        .clone()
        .oneshot(builder.body(request_body).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, json)
}

/// Extracts the `state=` query value from an `install_url`.
fn state_from_install_url(install_url: &str) -> String {
    url::Url::parse(install_url)
        .unwrap()
        .query_pairs()
        .find(|(k, _)| k == "state")
        .map(|(_, v)| v.into_owned())
        .expect("install_url carries a state parameter")
}

fn location(response: &axum::response::Response) -> String {
    response
        .headers()
        .get(header::LOCATION)
        .expect("Location header present")
        .to_str()
        .unwrap()
        .to_string()
}

fn source_context_request(repository: Option<&str>) -> Value {
    let mut body = json!({
        "path": FILE_PATH,
        "ref": "main",
        "line": 10,
        "context_lines": 2,
    });
    if let Some(repository) = repository {
        body["repository"] = json!(repository);
    }
    body
}

#[tokio::test]
async fn github_app_install_link_source_context_and_removal_e2e() {
    let server = MockServer::start().await;
    mount_github_api(&server).await;
    let app = build_app(&server).await;
    let cookie = login(&app).await;

    // 1. Issue a state token.
    let (status, body) = call(
        &app,
        axum::http::Method::POST,
        "/api/v1/tenants/acme/github-installations/link",
        &cookie,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let state_token = state_from_install_url(body["install_url"].as_str().unwrap());

    // 2. Complete the link via the browser-redirect callback.
    let callback_request = Request::builder()
        .method("GET")
        .uri(format!(
            "/ui/github/callback?code=abc&installation_id={INSTALLATION_ID}&setup_action=install&state={state_token}"
        ))
        .header(header::COOKIE, &cookie)
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(callback_request).await.unwrap();
    assert_eq!(response.status(), StatusCode::FOUND);
    assert_eq!(
        location(&response),
        format!("/integrations/github?github=linked&installation_id={INSTALLATION_ID}")
    );

    // 3. Fetch source context for a covered repo.
    let (status, body) = call(
        &app,
        axum::http::Method::POST,
        "/api/v1/tenants/acme/source-context",
        &cookie,
        Some(source_context_request(Some(REPO))),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["status"], "available", "{body}");
    let snippet = &body["snippet"];
    assert_eq!(snippet["start_line"], 8);
    assert_eq!(
        snippet["lines"],
        json!(["line8", "line9", "line10", "line11", "line12"])
    );
    assert_eq!(
        snippet["html_url"],
        format!("https://github.com/{REPO}/blob/main/{FILE_PATH}#L10")
    );

    // Repeat the identical request: still available, and the contents mock
    // (mounted with `.expect(1)`) must not be hit again — proving the cache.
    // `MockServer` verifies `.expect(...)` counts when it is dropped at the
    // end of this test.
    let (status, body) = call(
        &app,
        axum::http::Method::POST,
        "/api/v1/tenants/acme/source-context",
        &cookie,
        Some(source_context_request(Some(REPO))),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["status"], "available", "{body}");

    // A path-only request (no `repository`) resolves the same file via the
    // installation's covered-repositories list.
    let (status, body) = call(
        &app,
        axum::http::Method::POST,
        "/api/v1/tenants/acme/source-context",
        &cookie,
        Some(source_context_request(None)),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["status"], "available", "{body}");
    assert_eq!(body["snippet"]["repository"], REPO);

    // 4. Remove the installation.
    let (status, _) = call(
        &app,
        axum::http::Method::DELETE,
        &format!("/api/v1/tenants/acme/github-installations/{INSTALLATION_ID}"),
        &cookie,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // A subsequent fetch reports unavailable.
    let (status, body) = call(
        &app,
        axum::http::Method::POST,
        "/api/v1/tenants/acme/source-context",
        &cookie,
        Some(source_context_request(Some(REPO))),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["status"], "unavailable", "{body}");
    assert_eq!(body["reason"], "no_installation", "{body}");

    let (status, body) = call(
        &app,
        axum::http::Method::GET,
        "/api/v1/tenants/acme/github-installations",
        &cookie,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body["installations"].as_array().unwrap().is_empty(),
        "{body}"
    );
}
