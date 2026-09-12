//! End-to-end multi-tenant OAuth connector flow, across both the router and
//! the MCP server (design: mcp-multi-tenant-oauth-grants, task 7.1).
//!
//! Drives the full sequence a Claude/OpenAI connector performs against a real
//! `router::create_router` app — dynamic client registration, a consent
//! decision granting two tenants in one shot with independently different
//! dataset restrictions, and a PKCE token exchange — then presents the
//! resulting access token to a real `mcp_server::mcp_http_router` (backed by
//! the router over an actual TCP socket, as `mcp_auth_middleware`'s own
//! `POST /oauth/introspect` call and every downstream tool call are real
//! outbound HTTP requests, not in-process calls) and drives MCP tool calls
//! through it: one per granted tenant (each via the tool's own `tenant_id`
//! argument, since an OAuth request carries no `X-Tenant-ID` of its own), and
//! one naming a tenant the user belongs to but never granted.

use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use common::auth::{generate_session_token, hash_session_token};
use common::catalog::{Catalog, MembershipRole};
use common::config::{Configuration, OAuthConfig};
use futures::StreamExt;
use mcp_server::{McpAppState, mcp_http_router};
use router::{RouterAppState, create_router};
use serde_json::json;
use tokio::net::TcpListener;
use tower::ServiceExt;

const RESOURCE: &str = "http://localhost:3100/mcp";
// RFC 7636 Appendix B PKCE pair.
const PKCE_VERIFIER: &str = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
const PKCE_CHALLENGE: &str = "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM";

fn oauth_config() -> OAuthConfig {
    OAuthConfig {
        enabled: true,
        issuer_url: Some("http://localhost:3100".to_string()),
        resource_url: Some(RESOURCE.to_string()),
        ..Default::default()
    }
}

async fn json_body(res: axum::response::Response) -> serde_json::Value {
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

/// A router app plus its catalog, with three real tenants the same user is a
/// member of: `acme` (datasets `production`, `staging`), `globex` (dataset
/// `default`), and `initech` (dataset `default`, deliberately never
/// consented to below). Returns (app, catalog, cookie) — the browser session
/// authenticating the consent decision.
async fn app_catalog_and_session() -> (axum::Router, Catalog, String) {
    let catalog = Catalog::new("sqlite::memory:").await.unwrap();
    for (tenant, datasets) in [
        ("acme", &["production", "staging"][..]),
        ("globex", &["default"][..]),
        ("initech", &["default"][..]),
    ] {
        catalog
            .upsert_tenant(tenant, tenant, Some(datasets[0]), "database")
            .await
            .unwrap();
        for dataset in datasets {
            catalog.create_dataset(tenant, dataset).await.unwrap();
        }
    }
    let user = catalog
        .create_user("multi@example.com", None, Some("phc"), false)
        .await
        .unwrap();
    for tenant in ["acme", "globex", "initech"] {
        catalog
            .upsert_tenant_membership(&user.id, tenant, MembershipRole::Member)
            .await
            .unwrap();
    }
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
    config.auth.dataset_restriction_rollout_complete = true;
    let app = create_router(RouterAppState::new(catalog.clone(), config));
    (app, catalog, cookie)
}

/// Spawn a clone of `app` on a real TCP listener and return its base URL.
/// Needed because the MCP server's own auth middleware and every downstream
/// tool call are real outbound HTTP requests to the router, not in-process
/// `oneshot` calls.
async fn spawn_on_tcp(app: axum::Router) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind router");
    let addr = listener.local_addr().expect("router address");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("router serves");
    });
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    format!("http://{addr}")
}

fn mcp_request(
    session_id: Option<&str>,
    access_token: &str,
    body: serde_json::Value,
) -> Request<Body> {
    // Deliberately no `X-Tenant-ID`: an OAuth-authenticated MCP request never
    // carries one (design D4) — the MCP server derives the tenant for each
    // call from the tool's own arguments instead.
    let mut builder = Request::builder()
        .method("POST")
        .uri("/mcp")
        .header("host", "localhost")
        .header("authorization", format!("Bearer {access_token}"))
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream");
    if let Some(session_id) = session_id {
        builder = builder.header("mcp-session-id", session_id);
    }
    builder
        .body(Body::from(body.to_string()))
        .expect("build MCP request")
}

/// Read a Streamable HTTP response (JSON or SSE) until the JSON-RPC message
/// with `id` arrives.
async fn read_jsonrpc_response(response: axum::response::Response, id: u64) -> serde_json::Value {
    let mut stream = response.into_body().into_data_stream();
    let mut buffered = String::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let chunk = tokio::time::timeout_at(deadline, stream.next())
            .await
            .expect("response arrives before the deadline");
        let Some(chunk) = chunk else {
            panic!("response stream ended without a reply for id {id}: {buffered}");
        };
        let chunk = chunk.expect("read response chunk");
        buffered.push_str(&String::from_utf8_lossy(&chunk));
        for line in buffered.lines() {
            let candidate = line.strip_prefix("data:").map(str::trim).unwrap_or(line);
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(candidate)
                && value.get("id").and_then(|v| v.as_u64()) == Some(id)
            {
                return value;
            }
        }
    }
}

/// Open an MCP session authenticated by `access_token`, returning its
/// session id.
async fn open_mcp_session(mcp_app: &axum::Router, access_token: &str) -> String {
    let init = mcp_request(
        None,
        access_token,
        json!({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": "2025-03-26", "capabilities": {},
                       "clientInfo": {"name": "multi-tenant-oauth-flow-test", "version": "0"}}
        }),
    );
    let response = mcp_app
        .clone()
        .oneshot(init)
        .await
        .expect("initialize responds");
    assert_eq!(response.status(), StatusCode::OK, "initialize");
    let session_id = response
        .headers()
        .get("mcp-session-id")
        .and_then(|v| v.to_str().ok())
        .expect("initialize assigns a session id")
        .to_string();
    let _ = read_jsonrpc_response(response, 1).await;

    let initialized = mcp_request(
        Some(&session_id),
        access_token,
        json!({"jsonrpc": "2.0", "method": "notifications/initialized"}),
    );
    let response = mcp_app
        .clone()
        .oneshot(initialized)
        .await
        .expect("initialized responds");
    assert_eq!(response.status(), StatusCode::ACCEPTED, "initialized");
    session_id
}

/// Call a tool in an already-open session and return its JSON-RPC reply.
async fn call_tool(
    mcp_app: &axum::Router,
    session_id: &str,
    access_token: &str,
    id: u64,
    name: &str,
    arguments: serde_json::Value,
) -> serde_json::Value {
    let call = mcp_request(
        Some(session_id),
        access_token,
        json!({
            "jsonrpc": "2.0", "id": id, "method": "tools/call",
            "params": {"name": name, "arguments": arguments}
        }),
    );
    let response = mcp_app
        .clone()
        .oneshot(call)
        .await
        .expect("tools/call responds");
    assert_eq!(response.status(), StatusCode::OK, "tools/call HTTP status");
    read_jsonrpc_response(response, id).await
}

/// Drive DCR, one consent decision, and the PKCE token exchange against
/// `app`, returning the resulting access token. `tenant_grants_json` is the
/// consent decision's `tenant_grants` array literal (e.g.
/// `r#"[{"tenant_id":"acme"}]"#`), authenticated by `cookie`'s browser
/// session.
async fn register_consent_and_exchange(
    app: &axum::Router,
    cookie: &str,
    tenant_grants_json: &str,
) -> String {
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
    let client_id = json_body(res).await["client_id"]
        .as_str()
        .unwrap()
        .to_string();

    // 2. One consent decision granting the requested tenant set.
    let decision_body = format!(
        r#"{{"client_id":"{client_id}","redirect_uri":"https://claude.ai/cb","code_challenge":"{PKCE_CHALLENGE}","scope":"traces:read","resource":"{RESOURCE}","tenant_grants":{tenant_grants_json},"approved":true}}"#
    );
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/oauth/authorize/decision")
                .header("content-type", "application/json")
                .header("cookie", format!("signaldb_session={cookie}"))
                .body(Body::from(decision_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let redirect = json_body(res).await["redirect"]
        .as_str()
        .unwrap()
        .to_string();
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
    json_body(res).await["access_token"]
        .as_str()
        .unwrap()
        .to_string()
}

/// The dataset names reported for a successful `tenant_list_tables` reply.
fn reported_datasets(reply: &serde_json::Value) -> Vec<String> {
    let text = reply["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_else(|| panic!("tool reply carries no text content: {reply}"));
    let body: serde_json::Value = serde_json::from_str(text).expect("tool text is JSON");
    body["datasets"]
        .as_array()
        .unwrap_or_else(|| panic!("no `datasets` array: {body}"))
        .iter()
        .map(|d| d["dataset"].as_str().unwrap().to_string())
        .collect()
}

/// Task 7.1: register a client, consent to two tenants with different
/// dataset restrictions in one decision, exchange the code, call a tool
/// against each granted tenant through the MCP server using that one access
/// token, and confirm a third tenant — one the user belongs to but never
/// granted — is refused.
#[tokio::test(flavor = "multi_thread")]
async fn multi_tenant_oauth_grant_reaches_each_granted_tenant_and_refuses_a_third() {
    let (app, _catalog, cookie) = app_catalog_and_session().await;
    let router_base_url = spawn_on_tcp(app.clone()).await;

    // One consent decision granting both `acme` (restricted to `production`,
    // not `staging`) and `globex` (unrestricted) — genuinely different
    // restrictions, not two unrestricted grants. `initech` is a real
    // membership the user simply does not check.
    let access_token = register_consent_and_exchange(
        &app,
        &cookie,
        r#"[{"tenant_id":"acme","dataset_ids":["production"]},{"tenant_id":"globex"}]"#,
    )
    .await;

    // Present the one access token to a real MCP server backed by the
    // spawned router, and drive `tenant_list_tables` once per granted
    // tenant via its own `tenant_id` argument.
    let mcp_state = McpAppState::new(router_base_url).with_router_timeout(Duration::from_secs(10));
    let mcp_app = mcp_http_router(mcp_state, &[]);
    let session_id = open_mcp_session(&mcp_app, &access_token).await;

    let acme_reply = call_tool(
        &mcp_app,
        &session_id,
        &access_token,
        2,
        "tenant_list_tables",
        json!({"tenant_id": "acme"}),
    )
    .await;
    assert!(
        acme_reply.get("error").is_none(),
        "acme call must succeed: {acme_reply}"
    );
    assert_eq!(
        reported_datasets(&acme_reply),
        vec!["production".to_string()],
        "acme's own restriction (production only, not staging) must be reflected: {acme_reply}"
    );

    let globex_reply = call_tool(
        &mcp_app,
        &session_id,
        &access_token,
        3,
        "tenant_list_tables",
        json!({"tenant_id": "globex"}),
    )
    .await;
    assert!(
        globex_reply.get("error").is_none(),
        "globex call must succeed: {globex_reply}"
    );
    assert_eq!(
        reported_datasets(&globex_reply),
        vec!["default".to_string()],
        "globex's own (unrestricted) dataset set must be reflected: {globex_reply}"
    );

    // `initech`: a tenant the user is a member of, but never checked in
    // the consent decision above — outside this token's own grant set, not
    // outside the user's memberships. Rejected by the MCP server's own
    // `check_tenant_scope` before any router call, per design D4 ("the
    // router is still the sole authority... a bug in the MCP server's local
    // check can therefore only narrow access, never widen it").
    let initech_reply = call_tool(
        &mcp_app,
        &session_id,
        &access_token,
        4,
        "tenant_list_tables",
        json!({"tenant_id": "initech"}),
    )
    .await;
    assert!(
        initech_reply.get("result").is_none(),
        "initech must be refused: {initech_reply}"
    );
    let message = initech_reply["error"]["message"]
        .as_str()
        .unwrap_or_default();
    assert!(
        message.contains("granted tenants"),
        "expected a check_tenant_scope-shaped rejection naming the credential's granted \
         tenants (client-side, before any router call), got: {initech_reply}"
    );
}

/// Task 7.2: deleting one tenant from a two-tenant grant leaves the other
/// tenant's access on that same token unaffected — the regression test for
/// the FK-cascade hazard the `tenant_grants` JSON-column design (D2) exists
/// to rule out. An earlier draft of this change kept the legacy `tenant_id`
/// column's `ON DELETE CASCADE` FK on the token row itself, mirrored with
/// "the grant's first tenant" — deleting that one tenant cascade-deleted the
/// *entire* token (access token, refresh token, or auth code), silently
/// revoking every other tenant sharing the same grant. `tenant_grants` has no
/// DB-level FK at all, so a tenant's deletion can never cascade through it.
#[tokio::test(flavor = "multi_thread")]
async fn deleting_one_granted_tenant_leaves_the_others_reachable() {
    let (app, catalog, cookie) = app_catalog_and_session().await;
    let router_base_url = spawn_on_tcp(app.clone()).await;

    // Both tenants unrestricted: this test is about survival of the grant
    // itself across a tenant deletion, not dataset filtering (already
    // covered above).
    let access_token = register_consent_and_exchange(
        &app,
        &cookie,
        r#"[{"tenant_id":"acme"},{"tenant_id":"globex"}]"#,
    )
    .await;

    let mcp_state = McpAppState::new(router_base_url).with_router_timeout(Duration::from_secs(10));
    let mcp_app = mcp_http_router(mcp_state, &[]);
    let session_id = open_mcp_session(&mcp_app, &access_token).await;

    // Sanity: both tenants are reachable on this token before any deletion.
    let acme_before = call_tool(
        &mcp_app,
        &session_id,
        &access_token,
        2,
        "tenant_list_tables",
        json!({"tenant_id": "acme"}),
    )
    .await;
    assert!(
        acme_before.get("error").is_none(),
        "acme must be reachable before deletion: {acme_before}"
    );
    let globex_before = call_tool(
        &mcp_app,
        &session_id,
        &access_token,
        3,
        "tenant_list_tables",
        json!({"tenant_id": "globex"}),
    )
    .await;
    assert!(
        globex_before.get("error").is_none(),
        "globex must be reachable before deletion: {globex_before}"
    );

    // Delete `acme` through the catalog's real tenant-deletion path — the
    // same one the router's `DELETE /api/v1/admin/tenants/{tenant_id}`
    // handler calls into — not a raw SQL DELETE.
    let deleted = catalog
        .delete_tenant("acme")
        .await
        .expect("delete_tenant succeeds");
    assert!(deleted, "acme must have existed to delete");

    // globex's own access, from the same token and the same grant, is
    // unaffected by acme's deletion.
    let globex_after = call_tool(
        &mcp_app,
        &session_id,
        &access_token,
        4,
        "tenant_list_tables",
        json!({"tenant_id": "globex"}),
    )
    .await;
    assert!(
        globex_after.get("error").is_none(),
        "globex must remain reachable after acme is deleted: {globex_after}"
    );
    assert_eq!(
        reported_datasets(&globex_after),
        vec!["default".to_string()],
        "globex's dataset listing must be unaffected: {globex_after}"
    );

    // acme's own grant entry is now unresolvable at the router (D3: the same
    // outcome as a selector outside the grant set) — but the token as a
    // whole is not bricked, unlike the rejected FK-cascade design. This is a
    // router-side resolution failure (the MCP server's own `check_tenant_scope`
    // still lets `acme` through: introspection still reports it, since
    // `tenant_grants` is untouched by the deletion), not the client-side
    // rejection `initech` gets above — proof the deletion did not simply
    // shrink the MCP server's local view of the grant set.
    let acme_after = call_tool(
        &mcp_app,
        &session_id,
        &access_token,
        5,
        "tenant_list_tables",
        json!({"tenant_id": "acme"}),
    )
    .await;
    assert!(
        acme_after.get("result").is_none(),
        "acme must fail to resolve once its tenant is deleted: {acme_after}"
    );
    let message = acme_after["error"]["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("access denied"),
        "expected the router's own resolution failure (403 access denied), not a \
         check_tenant_scope-shaped rejection: {acme_after}"
    );
}
