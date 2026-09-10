//! End-to-end coverage for the multi-dataset-key-restriction change (task
//! 7.1): a dataset-set restriction on an API key or an OAuth grant is
//! enforced everywhere the design promises — the data-plane query path (D3),
//! the ambiguous-default rejection (D4), the management API regardless of
//! how the principal got its role (D9), the tenant self-service listings
//! (D10), and the shared `Authenticator` on every HTTP surface, not only the
//! MCP tool wrapper. Follows the harness conventions of
//! `oauth_connector_flow.rs` (full DCR flow against a real `create_router`
//! app) and `tenant_manage_clients.rs` (the `McpHttpClient` used to drive MCP
//! tools over the Streamable HTTP transport).

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use common::auth::oauth::hash_oauth_token;
use common::auth::{Authenticator, generate_session_token, hash_session_token};
use common::catalog::{Catalog, MembershipRole};
use common::config::{Configuration, OAuthConfig};
use futures::StreamExt;
use mcp_server::{McpAppState, mcp_http_router};
use router::{RouterAppState, create_router};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::net::TcpListener;
use tower::ServiceExt;
use url::Url;

const TENANT: &str = "acme";
const PRODUCTION: &str = "production";
const STAGING: &str = "staging";
const RESOURCE: &str = "http://localhost:3000/mcp";
// RFC 7636 Appendix B PKCE pair (canonical test vector, reused across
// `oauth_connector_flow.rs` and `oauth.rs`'s own tests).
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

/// A fresh in-memory catalog with tenant `acme` and two datasets
/// (`production`, `staging`), and a router app over it with OAuth enabled and
/// the mixed-version dataset-restriction rollout gate already open — every
/// scenario here issues a genuinely restricted credential (API key or OAuth
/// grant), which the gate would otherwise refuse to create (D2).
async fn router_with_tenant() -> (axum::Router, Catalog) {
    let catalog = Catalog::new("sqlite::memory:").await.unwrap();
    catalog
        .upsert_tenant(TENANT, "Acme", Some(PRODUCTION), "database")
        .await
        .unwrap();
    catalog.create_dataset(TENANT, PRODUCTION).await.unwrap();
    catalog.create_dataset(TENANT, STAGING).await.unwrap();
    let mut config = Configuration::default();
    config.mcp.oauth = oauth_config();
    config.auth.dataset_restriction_rollout_complete = true;
    let app = create_router(RouterAppState::new(catalog.clone(), config));
    (app, catalog)
}

/// Create a scoped, optionally dataset-restricted API key directly against
/// the catalog (the layer `common::catalog::DatasetRestrictionUpdate`/D2b
/// tests already cover in isolation; this drives it end to end through the
/// router instead).
async fn api_key(catalog: &Catalog, secret: &str, dataset_ids: Option<&[&str]>, scopes: &[&str]) {
    let scopes: Vec<String> = scopes.iter().map(|s| s.to_string()).collect();
    let dataset_ids: Option<Vec<String>> =
        dataset_ids.map(|ids| ids.iter().map(|s| s.to_string()).collect());
    catalog
        .upsert_scoped_api_key(
            TENANT,
            &Authenticator::hash_api_key(secret),
            Some(secret),
            dataset_ids.as_deref(),
            Some(&scopes),
            None,
        )
        .await
        .unwrap();
}

/// A single-signal Query IR document reading `traces` — enough to clear the
/// scope gate (`traces:read`) and reach dataset resolution; no querier is
/// registered in these tests, so a resolved request surfaces as a downstream
/// error, not an auth rejection (matching `oauth_connector_flow.rs`'s own
/// convention for the same reason).
fn ir_traces_body() -> Body {
    Body::from(
        json!({
            "irVersion": 1,
            "from": "traces",
            "range": { "from": "now-1h", "to": "now" },
            "result": "rows",
            "pipeline": []
        })
        .to_string(),
    )
}

/// POST `/api/v1/query` authenticated by an API key (`X-Tenant-ID` + Bearer).
async fn query_with_api_key(app: &axum::Router, key: &str, dataset: Option<&str>) -> StatusCode {
    let mut req = Request::builder()
        .method("POST")
        .uri("/api/v1/query")
        .header("authorization", format!("Bearer {key}"))
        .header("x-tenant-id", TENANT)
        .header("content-type", "application/json");
    if let Some(dataset) = dataset {
        req = req.header("x-dataset-id", dataset);
    }
    app.clone()
        .oneshot(req.body(ir_traces_body()).unwrap())
        .await
        .unwrap()
        .status()
}

/// POST `/api/v1/query` authenticated by an OAuth access token (its tenant
/// comes from the token, never a header).
async fn query_with_oauth_token(
    app: &axum::Router,
    token: &str,
    dataset: Option<&str>,
) -> StatusCode {
    let mut req = Request::builder()
        .method("POST")
        .uri("/api/v1/query")
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json");
    if let Some(dataset) = dataset {
        req = req.header("x-dataset-id", dataset);
    }
    app.clone()
        .oneshot(req.body(ir_traces_body()).unwrap())
        .await
        .unwrap()
        .status()
}

/// Parse a response body as JSON, `Value::Null` if it isn't (e.g. a
/// bodyless response), so callers can print it in an assertion message
/// without an extra layer of `Option`/`Result` handling.
async fn json_body(res: axum::response::Response) -> Value {
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap_or(Value::Null)
}

/// GET `/api/v1/manage/tenants/acme/datasets` with a bearer credential —
/// shared by the two D9 scenarios (a scope-carrying API key and a
/// role-carrying OAuth session), which differ only in whether an
/// `X-Tenant-ID` header is needed (an OAuth token's tenant comes from the
/// token itself, so it never sends one).
async fn manage_list_datasets(
    app: &axum::Router,
    bearer: &str,
    tenant_header: Option<&str>,
) -> axum::response::Response {
    let mut req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/manage/tenants/acme/datasets")
        .header("authorization", format!("Bearer {bearer}"));
    if let Some(tenant) = tenant_header {
        req = req.header("x-tenant-id", tenant);
    }
    app.clone()
        .oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap()
}

// ---- Scenario 1 (D3/D4 base case): a single-dataset restriction ----

#[tokio::test]
async fn api_key_restricted_to_one_dataset_reaches_it_and_is_refused_for_others() {
    let (app, catalog) = router_with_tenant().await;
    api_key(
        &catalog,
        "sk-prod-only",
        Some(&[PRODUCTION]),
        &["traces:read"],
    )
    .await;

    let allowed = query_with_api_key(&app, "sk-prod-only", Some(PRODUCTION)).await;
    assert_ne!(allowed, StatusCode::UNAUTHORIZED, "must authenticate");
    assert_ne!(
        allowed,
        StatusCode::FORBIDDEN,
        "production is within the restriction"
    );

    let denied = query_with_api_key(&app, "sk-prod-only", Some(STAGING)).await;
    assert_eq!(
        denied,
        StatusCode::FORBIDDEN,
        "staging is outside the restriction"
    );
}

// ---- Scenario 3 (no regression): unrestricted credentials reach every
// dataset — both a pre-migration-shaped legacy key (no scopes, no dataset
// restriction of any kind) and a newly created key that simply omits
// `dataset_ids`. ----

#[tokio::test]
async fn unrestricted_keys_reach_every_dataset() {
    let (app, catalog) = router_with_tenant().await;

    // Legacy-shaped: created via the pre-scopes-and-datasets `upsert_api_key`
    // path, which still exists and still yields a fully unrestricted key.
    catalog
        .upsert_api_key(TENANT, &Authenticator::hash_api_key("sk-legacy"), None)
        .await
        .unwrap();
    // Newly created under current code, explicit scopes, `dataset_ids: None`.
    api_key(&catalog, "sk-new-unrestricted", None, &["traces:read"]).await;

    for key in ["sk-legacy", "sk-new-unrestricted"] {
        for dataset in [PRODUCTION, STAGING] {
            let status = query_with_api_key(&app, key, Some(dataset)).await;
            assert_ne!(status, StatusCode::UNAUTHORIZED, "{key}/{dataset}");
            assert_ne!(
                status,
                StatusCode::FORBIDDEN,
                "{key} must reach {dataset}: unrestricted keys see the whole tenant"
            );
        }
        // No header at all: falls through to the tenant default, same as
        // today's only behavior.
        let status = query_with_api_key(&app, key, None).await;
        assert_ne!(status, StatusCode::FORBIDDEN, "{key} with no header");
    }
}

// ---- Scenario 4 (D4): a multi-element restriction with no explicit
// dataset is a rejection, never a silent default. ----

#[tokio::test]
async fn multi_dataset_key_without_explicit_header_is_rejected_not_defaulted() {
    let (app, catalog) = router_with_tenant().await;
    api_key(
        &catalog,
        "sk-multi",
        Some(&[PRODUCTION, STAGING]),
        &["traces:read"],
    )
    .await;

    // No X-Dataset-ID: ambiguous, rejected with 400 naming the ambiguity —
    // never silently resolved to the tenant default (production) even though
    // it is a member of the restriction.
    let res = query_with_api_key(&app, "sk-multi", None).await;
    assert_eq!(
        res,
        StatusCode::BAD_REQUEST,
        "an unheaded request against a multi-element restriction must be rejected, not defaulted"
    );

    // Both named datasets remain reachable once the caller is explicit.
    for dataset in [PRODUCTION, STAGING] {
        let status = query_with_api_key(&app, "sk-multi", Some(dataset)).await;
        assert_ne!(status, StatusCode::FORBIDDEN, "{dataset} is in the set");
        assert_ne!(status, StatusCode::BAD_REQUEST, "{dataset} is in the set");
    }
    // A dataset outside the two-element set is still refused.
    let outside = query_with_api_key(&app, "sk-multi", Some("nonexistent")).await;
    assert_eq!(outside, StatusCode::FORBIDDEN);
}

// ---- Scenario 5 (D9, scope-carrying case): a key carrying `tenant:manage`
// and a dataset restriction is refused by the management API end to end. ----

#[tokio::test]
async fn dataset_restricted_manage_key_is_refused_by_management_api() {
    let (app, catalog) = router_with_tenant().await;
    api_key(
        &catalog,
        "sk-restricted-manage",
        Some(&[PRODUCTION]),
        &[
            "traces:read",
            "traces:write",
            common::auth::TENANT_MANAGE_SCOPE,
        ],
    )
    .await;

    let res = manage_list_datasets(&app, "sk-restricted-manage", Some(TENANT)).await;
    assert_eq!(
        res.status(),
        StatusCode::FORBIDDEN,
        "a dataset-restricted tenant:manage key must still be refused: {:?}",
        json_body(res).await
    );

    // The narrower, data-plane access the key actually has is unaffected.
    let query_status = query_with_api_key(&app, "sk-restricted-manage", Some(PRODUCTION)).await;
    assert_ne!(
        query_status,
        StatusCode::FORBIDDEN,
        "D9 narrows only the management API, not the key's own dataset access"
    );
}

// ---- OAuth: full DCR flow helpers, shared by scenarios 2, 6, and 9. ----

/// A router app plus the catalog behind it, with tenant `acme` (datasets
/// `production`/`staging`), a live browser session for `email`, and that
/// user's membership `role` in the tenant. Returns (app, catalog, cookie).
async fn app_with_session(email: &str, role: MembershipRole) -> (axum::Router, Catalog, String) {
    let (app, catalog) = router_with_tenant().await;
    let user = catalog
        .create_user(email, None, "phc", false)
        .await
        .unwrap();
    catalog
        .upsert_tenant_membership(&user.id, TENANT, role)
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
    (app, catalog, cookie)
}

/// Register a client via DCR, then drive a consent decision for `tenant`
/// (optionally dataset-restricted) authenticated by `cookie`, and exchange
/// the resulting code for a token pair. Returns (access_token, refresh_token,
/// client_id).
async fn issue_oauth_tokens(
    app: &axum::Router,
    cookie: &str,
    dataset_ids: Option<&[&str]>,
) -> (String, String, String) {
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
    assert_eq!(res.status(), StatusCode::CREATED, "DCR registration");
    let client_id = json_body(res).await["client_id"]
        .as_str()
        .unwrap()
        .to_string();

    let mut decision = json!({
        "client_id": client_id,
        "redirect_uri": "https://claude.ai/cb",
        "code_challenge": PKCE_CHALLENGE,
        "scope": "traces:read",
        "tenant": TENANT,
        "approved": true,
    });
    if let Some(ids) = dataset_ids {
        decision["dataset_ids"] = json!(ids);
    }
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/oauth/authorize/decision")
                .header("content-type", "application/json")
                .header("cookie", format!("signaldb_session={cookie}"))
                .body(Body::from(decision.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        StatusCode::OK,
        "consent decision: {:?}",
        json_body(res).await
    );
    let redirect = json_body(res).await["redirect"]
        .as_str()
        .unwrap()
        .to_string();
    let code = Url::parse(&redirect)
        .unwrap()
        .query_pairs()
        .find(|(k, _)| k == "code")
        .map(|(_, v)| v.into_owned())
        .expect("redirect carries a code");

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
    assert_eq!(
        res.status(),
        StatusCode::OK,
        "token exchange: {:?}",
        json_body(res).await
    );
    let tokens = json_body(res).await;
    (
        tokens["access_token"].as_str().unwrap().to_string(),
        tokens["refresh_token"].as_str().unwrap().to_string(),
        client_id,
    )
}

// ---- Scenario 2 (D6): an OAuth token issued with a dataset restriction
// through the full DCR flow, and a refresh that preserves it after the
// original access token is discarded. ----

#[tokio::test]
async fn oauth_token_with_dataset_restriction_via_dcr_flow_and_refresh_preserves_it() {
    let (app, catalog, cookie) =
        app_with_session("member@example.com", MembershipRole::Member).await;
    let (access, refresh, client_id) = issue_oauth_tokens(&app, &cookie, Some(&[PRODUCTION])).await;

    let allowed = query_with_oauth_token(&app, &access, Some(PRODUCTION)).await;
    assert_ne!(allowed, StatusCode::UNAUTHORIZED);
    assert_ne!(allowed, StatusCode::FORBIDDEN, "production is granted");
    let denied = query_with_oauth_token(&app, &access, Some(STAGING)).await;
    assert_eq!(denied, StatusCode::FORBIDDEN, "staging was never granted");

    // Discard the original access token entirely (revoke it) before
    // refreshing — D6 requires the refresh to read the restriction from the
    // *refresh token's own row*, not from the access token it was issued
    // alongside, which by now is gone.
    catalog
        .revoke_access_token(&hash_oauth_token(&access))
        .await
        .unwrap();
    assert_eq!(
        query_with_oauth_token(&app, &access, Some(PRODUCTION)).await,
        StatusCode::UNAUTHORIZED,
        "the discarded access token must no longer authenticate"
    );

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/oauth/token")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(format!(
                    "grant_type=refresh_token&refresh_token={refresh}&client_id={client_id}"
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        StatusCode::OK,
        "refresh: {:?}",
        json_body(res).await
    );
    let tokens = json_body(res).await;
    let new_access = tokens["access_token"].as_str().unwrap().to_string();

    let allowed = query_with_oauth_token(&app, &new_access, Some(PRODUCTION)).await;
    assert_ne!(
        allowed,
        StatusCode::FORBIDDEN,
        "the refreshed token still carries the production restriction"
    );
    let denied = query_with_oauth_token(&app, &new_access, Some(STAGING)).await;
    assert_eq!(
        denied,
        StatusCode::FORBIDDEN,
        "the refreshed token must not have widened to the whole tenant"
    );
}

// ---- Scenario 6 (D9, role-carrying case): a tenant-admin OAuth session
// authorized with a non-empty dataset restriction is refused by the
// management API — the same door D9 closes for a scope-carrying API key. ----

#[tokio::test]
async fn oauth_tenant_admin_session_with_dataset_restriction_is_refused_by_management_api() {
    let (app, _catalog, cookie) =
        app_with_session("admin@example.com", MembershipRole::Admin).await;
    let (access, _refresh, _client_id) =
        issue_oauth_tokens(&app, &cookie, Some(&[PRODUCTION])).await;

    // Sanity: this session's role alone would normally clear `can_manage`
    // (tenant-admin) — the refusal below must come from D9's dataset-
    // restriction check, not from a missing role.
    let res = manage_list_datasets(&app, &access, None).await;
    assert_eq!(
        res.status(),
        StatusCode::FORBIDDEN,
        "a dataset-restricted tenant-admin session must still be refused: {:?}",
        json_body(res).await
    );

    // Its own data-plane grant still works.
    let query_status = query_with_oauth_token(&app, &access, Some(PRODUCTION)).await;
    assert_ne!(query_status, StatusCode::FORBIDDEN);
}

// ---- Scenario 9: an OAuth access token restricted to `[production]`,
// presented directly (bearer token, no MCP involved) against a
// Tempo-compatible HTTP endpoint, is refused for `staging` — proving the
// restriction is enforced in the shared `Authenticator`, not only through
// the MCP tool wrapper. ----

#[tokio::test]
async fn oauth_token_restricted_dataset_refused_on_compat_endpoint_directly() {
    let (app, _catalog, cookie) =
        app_with_session("member2@example.com", MembershipRole::Member).await;
    let (access, _refresh, _client_id) =
        issue_oauth_tokens(&app, &cookie, Some(&[PRODUCTION])).await;

    let allowed = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/tempo/api/search?limit=1")
                .header("authorization", format!("Bearer {access}"))
                .header("x-dataset-id", PRODUCTION)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_ne!(allowed.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(allowed.status(), StatusCode::FORBIDDEN);

    let denied = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/tempo/api/search?limit=1")
                .header("authorization", format!("Bearer {access}"))
                .header("x-dataset-id", STAGING)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        denied.status(),
        StatusCode::FORBIDDEN,
        "a Tempo-compatible endpoint must enforce the token's dataset restriction too"
    );
}

// ---- Scenario 8 (D10, whoami): a credential restricted to `[production]`
// excludes `staging` from its `GET /api/v1/whoami` dataset list. ----

#[tokio::test]
async fn whoami_excludes_restricted_dataset() {
    let (app, catalog) = router_with_tenant().await;
    api_key(
        &catalog,
        "sk-whoami-restricted",
        Some(&[PRODUCTION]),
        &["traces:read"],
    )
    .await;

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/whoami")
                .header("authorization", "Bearer sk-whoami-restricted")
                .header("x-tenant-id", TENANT)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = json_body(res).await;
    let names: Vec<&str> = body["datasets"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["id"].as_str().unwrap())
        .collect();
    assert!(names.contains(&PRODUCTION), "{names:?}");
    assert!(
        !names.contains(&STAGING),
        "a restricted credential must not see staging in whoami: {names:?}"
    );
    assert_eq!(body["dataset_ids"], json!([PRODUCTION]));
}

// ---- Scenario 7 (D10, MCP): `discover_datasets` never lists a dataset
// outside the restriction, and `tenant_list_tables` for the same restricted
// credential neither exposes nor accepts `staging`. ----

/// Minimal Streamable HTTP MCP client over the in-process `mcp_http_router`,
/// forwarding a chosen API key (mirrors `tenant_manage_clients.rs`).
struct McpHttpClient {
    app: axum::Router,
    api_key: String,
    session_id: Option<String>,
}

impl McpHttpClient {
    async fn connect(router_url: &str, api_key: &str) -> Self {
        let app = mcp_http_router(McpAppState::new(router_url.to_string()), &[]);
        let mut client = Self {
            app,
            api_key: api_key.to_string(),
            session_id: None,
        };
        let init = client
            .post(json!({
                "jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": {
                    "protocolVersion": "2025-03-26",
                    "capabilities": {},
                    "clientInfo": {"name": "tests-integration", "version": "0"}
                }
            }))
            .await;
        assert!(
            init["result"]["serverInfo"].is_object(),
            "initialize response: {init}"
        );
        let res = client
            .app
            .clone()
            .oneshot(
                client.request(json!({"jsonrpc": "2.0", "method": "notifications/initialized"})),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::ACCEPTED, "notification accepted");
        client
    }

    fn request(&self, body: Value) -> Request<Body> {
        let mut req = Request::builder()
            .method("POST")
            .uri("/mcp")
            .header("host", "localhost")
            .header("authorization", format!("Bearer {}", self.api_key))
            .header("x-tenant-id", TENANT)
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream");
        if let Some(id) = &self.session_id {
            req = req.header("mcp-session-id", id);
        }
        req.body(Body::from(body.to_string())).unwrap()
    }

    async fn post(&mut self, body: Value) -> Value {
        let res = self.app.clone().oneshot(self.request(body)).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK, "MCP POST accepted");
        if let Some(id) = res
            .headers()
            .get("mcp-session-id")
            .and_then(|v| v.to_str().ok())
        {
            self.session_id = Some(id.to_string());
        }
        let is_sse = res
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|ct| ct.contains("text/event-stream"));
        let mut stream = res.into_body().into_data_stream();
        let mut buffer = String::new();
        let read = async {
            while let Some(chunk) = stream.next().await {
                buffer.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
                if let Some(message) = extract_message(&buffer, is_sse) {
                    return message;
                }
            }
            extract_message(&buffer, is_sse)
                .unwrap_or_else(|| panic!("no JSON-RPC message in response body: {buffer:?}"))
        };
        tokio::time::timeout(Duration::from_secs(30), read)
            .await
            .expect("MCP response within 30s")
    }

    async fn call_tool(&mut self, name: &str, arguments: Value) -> Value {
        self.post(json!({
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": {"name": name, "arguments": arguments}
        }))
        .await
    }
}

fn extract_message(buffer: &str, is_sse: bool) -> Option<Value> {
    if !is_sse {
        return serde_json::from_str(buffer).ok();
    }
    buffer
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .filter_map(|data| serde_json::from_str::<Value>(data.trim()).ok())
        .find(|message| message.get("result").is_some() || message.get("error").is_some())
}

fn tool_text(response: &Value) -> String {
    response["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_default()
        .to_string()
}

/// Spawn a real listener serving `create_router(RouterAppState::new(catalog,
/// config))` — the MCP server proxies real HTTP calls to it, so (unlike the
/// other scenarios here) it needs an actual bound address.
async fn spawn_router(catalog: Catalog, config: Configuration) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = create_router(RouterAppState::new(catalog, config));
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    for attempt in 0..50 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            break;
        }
        assert!(attempt < 49, "router never became reachable");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    format!("http://{addr}")
}

#[tokio::test]
async fn discover_datasets_and_tenant_list_tables_hide_restricted_dataset() {
    // `TenantApi::list_tables` lists every dataset the tenant is known to
    // have — including one with nothing provisioned yet, with an empty
    // table vector rather than being omitted (`schema/mod.rs`'s
    // `list_tables_for_tenant` doc) — so both `discover_datasets` and
    // `tenant_list_tables` have real dataset names to filter without this
    // test needing to provision any actual signal tables.
    let catalog = Catalog::new("sqlite::memory:").await.unwrap();
    catalog
        .upsert_tenant(TENANT, "Acme", Some(PRODUCTION), "database")
        .await
        .unwrap();
    catalog.create_dataset(TENANT, PRODUCTION).await.unwrap();
    catalog.create_dataset(TENANT, STAGING).await.unwrap();
    api_key(
        &catalog,
        "sk-mcp-restricted",
        Some(&[PRODUCTION]),
        &["traces:read"],
    )
    .await;
    let router_url = spawn_router(catalog, Configuration::default()).await;

    let mut client = McpHttpClient::connect(&router_url, "sk-mcp-restricted").await;

    let discovered = client.call_tool("discover_datasets", json!({})).await;
    assert!(discovered.get("error").is_none(), "{discovered}");
    let markdown = tool_text(&discovered);
    assert!(markdown.contains(PRODUCTION), "{markdown}");
    assert!(
        !markdown.contains(STAGING),
        "discover_datasets must never list a dataset outside the restriction: {markdown}"
    );

    let tables = client
        .call_tool("tenant_list_tables", json!({"tenant_id": TENANT}))
        .await;
    assert!(tables.get("error").is_none(), "{tables}");
    let tables: Value = serde_json::from_str(&tool_text(&tables)).expect("JSON result");
    let dataset_names: Vec<&str> = tables["datasets"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["dataset"].as_str().unwrap())
        .collect();
    assert!(dataset_names.contains(&PRODUCTION), "{dataset_names:?}");
    assert!(
        !dataset_names.contains(&STAGING),
        "tenant_list_tables must not expose staging: {dataset_names:?}"
    );
    // Every listed table's own `dataset` field is likewise never `staging` —
    // the tool takes no dataset argument at all, so there is no way to ask
    // it for staging in the first place (D10 covers both fields it filters).
    for table in tables["tables"].as_array().unwrap() {
        if let Some(dataset) = table["dataset"].as_str() {
            assert_ne!(dataset, STAGING, "{table:?}");
        }
    }
}
