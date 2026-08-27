//! End-to-end OAuth 2.1 connector flow against the router (change:
//! mcp-oauth-dcr).
//!
//! Drives the full sequence a Claude/OpenAI connector performs, through the
//! real `create_router` app: dynamic client registration → consent decision
//! (authenticated by a browser session) → PKCE token exchange → a
//! token-authenticated request whose tenant and scopes come from the token.
//! Then it asserts the isolation and lifecycle guarantees: a revoked token
//! stops working, and a token minted for another resource is rejected.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use common::auth::oauth::hash_oauth_token;
use common::auth::{generate_session_token, hash_session_token};
use common::catalog::{Catalog, MembershipRole};
use common::config::{Configuration, OAuthConfig};
use router::{RouterAppState, create_router};
use tower::ServiceExt;

const RESOURCE: &str = "http://localhost:3000/mcp";
// RFC 7636 Appendix B PKCE pair.
const PKCE_VERIFIER: &str = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
const PKCE_CHALLENGE: &str = "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM";

fn oauth_config() -> OAuthConfig {
    OAuthConfig {
        enabled: true,
        issuer_url: Some("http://localhost:3000".to_string()),
        resource_url: Some(RESOURCE.to_string()),
        ..Default::default()
    }
}

async fn json(res: axum::response::Response) -> serde_json::Value {
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

/// A router app plus the catalog behind it (a clone sharing the in-memory DB),
/// with tenant `acme` (dataset `production`), a member user, and a live browser
/// session. Returns (app, catalog, session_cookie).
async fn app_with_session() -> (axum::Router, Catalog, String) {
    let catalog = Catalog::new("sqlite::memory:").await.unwrap();
    catalog
        .upsert_tenant("acme", "Acme", Some("production"), "database")
        .await
        .unwrap();
    catalog.create_dataset("acme", "production").await.unwrap();
    let user = catalog
        .create_user("human@example.com", None, Some("phc"), false)
        .await
        .unwrap();
    catalog
        .upsert_tenant_membership(&user.id, "acme", MembershipRole::Member)
        .await
        .unwrap();
    let cookie = generate_session_token();
    catalog
        .create_user_session(
            &user.id,
            &hash_session_token(&cookie),
            chrono::Utc::now() + chrono::Duration::hours(1),
        )
        .await
        .unwrap();

    let mut config = Configuration::default();
    config.mcp.oauth = oauth_config();
    let app = create_router(RouterAppState::new(catalog.clone(), config));
    (app, catalog, cookie)
}

#[tokio::test]
async fn full_connector_flow_registers_consents_and_authenticates() {
    let (app, _catalog, cookie) = app_with_session().await;

    // 1. Dynamic Client Registration (RFC 7591).
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/oauth/register")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"redirect_uris":["https://claude.ai/cb"],"client_name":"Claude"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);
    let client_id = json(res).await["client_id"].as_str().unwrap().to_string();

    // 2. Consent decision (authenticated by the session cookie) → auth code.
    let body = format!(
        r#"{{"client_id":"{client_id}","redirect_uri":"https://claude.ai/cb","code_challenge":"{PKCE_CHALLENGE}","scope":"traces:read","resource":"{RESOURCE}","tenant":"acme","approved":true}}"#
    );
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/oauth/authorize/decision")
                .header("content-type", "application/json")
                .header("cookie", format!("signaldb_session={cookie}"))
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let redirect = json(res).await["redirect"].as_str().unwrap().to_string();
    let code = url::Url::parse(&redirect)
        .unwrap()
        .query_pairs()
        .find(|(k, _)| k == "code")
        .map(|(_, v)| v.into_owned())
        .expect("redirect carries a code");

    // 3. Token exchange with the PKCE verifier.
    let form = format!(
        "grant_type=authorization_code&code={code}&code_verifier={PKCE_VERIFIER}\
         &redirect_uri=https%3A%2F%2Fclaude.ai%2Fcb&client_id={client_id}"
    );
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/oauth/token")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(form))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let tokens = json(res).await;
    assert_eq!(tokens["token_type"], "Bearer");
    assert_eq!(tokens["scope"], "traces:read");
    let access = tokens["access_token"].as_str().unwrap().to_string();

    // 4. A token-authenticated trace query — no X-Tenant-ID. It must clear
    // authentication and the traces:read gate (the tenant comes from the
    // token). With no querier registered the handler cannot execute, so the
    // response is a downstream error, NOT an auth rejection.
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/tempo/api/search?limit=1")
                .header("authorization", format!("Bearer {access}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_ne!(
        res.status(),
        StatusCode::UNAUTHORIZED,
        "token must authenticate"
    );
    assert_ne!(
        res.status(),
        StatusCode::FORBIDDEN,
        "traces:read must be granted"
    );

    // 4b. The same token lacks metrics:read → the metrics surface is denied.
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/prometheus/api/v1/query?query=up")
                .header("authorization", format!("Bearer {access}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn revoked_token_stops_working() {
    let (app, catalog, _cookie) = app_with_session().await;
    // Mint an access token directly for the member user.
    let raw = common::auth::oauth::generate_oauth_token(common::auth::oauth::TokenKind::Access);
    let user = catalog
        .get_user_by_email("human@example.com")
        .await
        .unwrap()
        .unwrap();
    catalog
        .create_access_token(
            &hash_oauth_token(&raw),
            "client-1",
            &user.id,
            "acme",
            &["traces:read".to_string()],
            Some(RESOURCE),
            chrono::Utc::now() + chrono::Duration::hours(1),
        )
        .await
        .unwrap();

    // Works before revocation (past auth).
    let status = |app: axum::Router, tok: String| async move {
        app.oneshot(
            Request::builder()
                .uri("/tempo/api/search?limit=1")
                .header("authorization", format!("Bearer {tok}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
    };
    assert_ne!(
        status(app.clone(), raw.clone()).await,
        StatusCode::UNAUTHORIZED
    );

    // Revoke (row delete) → the next request is unauthenticated.
    catalog
        .revoke_access_token(&hash_oauth_token(&raw))
        .await
        .unwrap();
    assert_eq!(status(app, raw).await, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn token_for_another_resource_is_rejected() {
    let (app, catalog, _cookie) = app_with_session().await;
    let raw = common::auth::oauth::generate_oauth_token(common::auth::oauth::TokenKind::Access);
    let user = catalog
        .get_user_by_email("human@example.com")
        .await
        .unwrap()
        .unwrap();
    // Audience is a different resource than this deployment serves.
    catalog
        .create_access_token(
            &hash_oauth_token(&raw),
            "client-1",
            &user.id,
            "acme",
            &["traces:read".to_string()],
            Some("https://other.example.com/mcp"),
            chrono::Utc::now() + chrono::Duration::hours(1),
        )
        .await
        .unwrap();

    let res = app
        .oneshot(
            Request::builder()
                .uri("/tempo/api/search?limit=1")
                .header("authorization", format!("Bearer {raw}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}
