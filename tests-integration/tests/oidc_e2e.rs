//! End-to-end OIDC/SSO login against a real IdP container (change:
//! oidc-login, task 4.5).
//!
//! `src/router/src/endpoints/oidc.rs`'s unit tests already exercise the
//! callback logic exhaustively against a *simulated* IdP (wiremock serving
//! hand-signed JWTs). This test instead drives the full authorization-code +
//! PKCE dance against a real, running OpenID Connect provider — a Keycloak
//! testcontainer — to catch anything the simulation can't: a real discovery
//! document, a real login form, real state/nonce/PKCE enforcement performed
//! by the IdP itself, and a `groups` claim whose presence actually depends on
//! how the IdP is configured to include it.
//!
//! ## Why Keycloak, not Dex
//!
//! This workspace's pinned `testcontainers-modules` version ships a
//! first-party `dex` module, which would have been the simpler choice (no
//! hand-rolled `GenericImage`, no realm-import JSON). It was rejected: Dex's
//! server hard-gates the `groups` ID-token claim behind an explicit `groups`
//! OAuth scope requested by the relying party — confirmed against Dex's own
//! `server/server_test.go`, which always adds `"groups"` to
//! `requestedScopes` whenever it expects the claim back. SignalDB's relying
//! party (`router::oidc::DiscoveredProvider::begin_authorization`) only ever
//! requests `openid email profile`; adding `groups` would be a production
//! behavior change outside this task's scope. Dex can therefore never
//! exercise the group-mapping assertion below, with or without a testcontainer.
//!
//! Keycloak's client-scope model has no such coupling: a group-membership
//! protocol mapper attached to a realm's *default* (not "optional") client
//! scope is folded into every token issued to that client regardless of the
//! `scope` the RP requests — exactly the shape a real deployment would rely
//! on to make `[auth.oidc].group_claim` work against SignalDB's fixed scope
//! set. There is no first-party `testcontainers-modules` Keycloak image, so
//! this hand-rolls a `GenericImage` plus a realm-import JSON instead.
//!
//! ## Docker gating
//!
//! No other testcontainers-backed suite in this workspace gates itself with
//! `#[ignore]` or a runtime Docker probe (see
//! `src/common/tests/catalog_integration.rs`, `tests-integration/src/
//! test_helpers.rs`'s `MinioTestContext`): Docker availability is treated as
//! a property of the CI runner/job, not something a test checks itself, and
//! `.github/workflows/ci.yml`'s main test job (`cargo test --workspace`) and
//! `test-matrix.yml`'s database job both run on GitHub-hosted runners that
//! have a Docker daemon. This test matches that idiom exactly — no `#[ignore]`,
//! no env-var probe — rather than inventing a new gating mechanism.
//!
//! ## Caveat
//!
//! This module was written in a sandbox with no Docker daemon. A later pass
//! reclaimed disk (`cargo clean`), enabled `testcontainers-modules`'
//! `http_wait` feature and `reqwest`'s `form` feature (both required by this
//! test and previously off; see `Cargo.toml`), and got a clean
//! `cargo test -p tests-integration --test oidc_e2e --no-run`. It has **not**
//! been run against a live container: this sandbox still has no Docker
//! daemon. The Keycloak login-form scraping and the realm-import JSON shape
//! are implemented against documented/source-verified Keycloak behavior (see
//! the inline comments, each backed by a specific Keycloak source file or a
//! sibling realm-export fixture from the Keycloak test suite) but have not
//! been *run* against a live container. Treat the first real run against a
//! live container as this test's remaining acceptance bar, and expect to
//! need small fixups (a query-param name, an extra redirect hop) before it
//! succeeds.

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use common::catalog::{Catalog, GrantSource, MembershipRole};
use common::config::{
    ApiKeyConfig, AuthConfig, Configuration, DatasetConfig, GroupMapping, OAuthConfig, OidcConfig,
    TenantConfig,
};
use router::{RouterAppState, RouterState, create_router};
use std::time::Duration;
use testcontainers_modules::testcontainers::core::wait::HttpWaitStrategy;
use testcontainers_modules::testcontainers::core::{ContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{GenericImage, ImageExt};
use tower::ServiceExt;
use url::Url;

const REALM: &str = "signaldb-e2e";
const CLIENT_ID: &str = "signaldb-e2e";
const CLIENT_SECRET: &str = "signaldb-e2e-secret";
/// Never dialed: the router under test is driven in-process via `oneshot`
/// (see `drive_start`/the callback call below), so this only needs to be a
/// registered, syntactically valid redirect URI both sides agree on. `.invalid`
/// is the RFC 2606 TLD reserved for names that are guaranteed never to
/// resolve.
const REDIRECT_URL: &str = "https://signaldb.invalid/ui/session/oidc/callback";
const USER_EMAIL: &str = "sso.tester@example.com";
const USER_PASSWORD: &str = "correct horse battery staple";
const MAPPED_GROUP: &str = "observability-admins";
const MAPPED_TENANT: &str = "acme";
// RFC 7636 Appendix B PKCE pair, reused from tests/oauth_connector_flow.rs;
// only the challenge is needed since this test stops at consent, not token
// exchange (see the module doc's MCP-OAuth-over-SSO section).
const MCP_PKCE_CHALLENGE: &str = "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM";
const MCP_RESOURCE: &str = "http://localhost:3000/mcp";

/// The realm-import JSON Keycloak's `--import-realm` flag loads at startup.
///
/// Shape verified against Keycloak's own test fixture
/// (`testsuite/integration-arquillian/.../testrealm.json`, top-level
/// `groups`/per-user `groups` arrays) and against
/// `GroupMembershipMapper.java`'s `PROVIDER_ID = "oidc-group-membership-mapper"`
/// and its `full.path` config key.
fn realm_import_json() -> Vec<u8> {
    serde_json::json!({
        "realm": REALM,
        "enabled": true,
        "sslRequired": "none",
        "groups": [{ "name": MAPPED_GROUP }],
        "users": [{
            "username": "sso-tester",
            "email": USER_EMAIL,
            "enabled": true,
            "emailVerified": true,
            "credentials": [{
                "type": "password",
                "value": USER_PASSWORD,
                "temporary": false,
            }],
            "groups": [format!("/{MAPPED_GROUP}")],
        }],
        "clientScopes": [{
            // A *default* client scope (named below in the client's
            // `defaultClientScopes`) is included in every token issued to
            // that client regardless of the OAuth `scope` parameter it
            // requests — see the module doc's "Why Keycloak, not Dex"
            // section.
            "name": "signaldb-groups",
            "protocol": "openid-connect",
            "protocolMappers": [{
                "name": "groups",
                "protocol": "openid-connect",
                "protocolMapper": "oidc-group-membership-mapper",
                "config": {
                    "claim.name": "groups",
                    // Bare group names (no leading `/path`), matching how
                    // `[auth.oidc].group_mappings[].group` is written.
                    "full.path": "false",
                    "id.token.claim": "true",
                    "access.token.claim": "true",
                    "userinfo.token.claim": "true",
                },
            }],
        }],
        "clients": [{
            "clientId": CLIENT_ID,
            "secret": CLIENT_SECRET,
            "enabled": true,
            "publicClient": false,
            "standardFlowEnabled": true,
            "directAccessGrantsEnabled": false,
            "consentRequired": false,
            "protocol": "openid-connect",
            "redirectUris": [REDIRECT_URL],
            "defaultClientScopes": ["openid", "profile", "email", "signaldb-groups"],
        }],
    })
    .to_string()
    .into_bytes()
}

/// Starts a Keycloak container with the realm above already imported.
/// Waits on the realm's own discovery document (not just the base HTTP
/// port) so the returned container is actually ready to serve `/authorize`.
async fn start_keycloak() -> testcontainers_modules::testcontainers::ContainerAsync<GenericImage> {
    let image = GenericImage::new("quay.io/keycloak/keycloak", "26.0")
        .with_exposed_port(ContainerPort::Tcp(8080))
        .with_wait_for(WaitFor::http(
            HttpWaitStrategy::new(format!("/realms/{REALM}/.well-known/openid-configuration"))
                .with_port(ContainerPort::Tcp(8080))
                .with_expected_status_code(200u16),
        ))
        // Both env-var pairs are set since Keycloak's bootstrap-admin
        // variable name changed across major versions; the unused pair is
        // harmless. Not otherwise used by this test (no admin-console
        // calls).
        .with_env_var("KEYCLOAK_ADMIN", "admin")
        .with_env_var("KEYCLOAK_ADMIN_PASSWORD", "admin")
        .with_env_var("KC_BOOTSTRAP_ADMIN_USERNAME", "admin")
        .with_env_var("KC_BOOTSTRAP_ADMIN_PASSWORD", "admin")
        .with_copy_to("/opt/keycloak/data/import/realm.json", realm_import_json())
        .with_cmd(["start-dev", "--import-realm"]);
    image.start().await.expect("Keycloak container starts")
}

fn tenant_config() -> TenantConfig {
    TenantConfig {
        id: MAPPED_TENANT.to_string(),
        slug: MAPPED_TENANT.to_string(),
        name: "Acme Inc".to_string(),
        default_dataset: Some("default".to_string()),
        datasets: vec![DatasetConfig {
            id: "default".to_string(),
            slug: "default".to_string(),
            is_default: true,
            storage: None,
        }],
        api_keys: vec![ApiKeyConfig {
            key: "acme-key".to_string(),
            name: Some("test".to_string()),
        }],
        schema_config: None,
        limits: None,
    }
}

/// A `RouterAppState` configured for SSO against the given issuer (the live
/// Keycloak container's discovery URL), with a `group_mappings` rule that
/// grants `MAPPED_GROUP` -> `MAPPED_TENANT` at `MembershipRole::Member`, and
/// MCP OAuth enabled so the consent step can be driven the same way
/// `tests/oauth_connector_flow.rs` drives it over a password session.
async fn router_state_with_oidc(issuer_url: String) -> RouterAppState {
    let oidc = OidcConfig {
        issuer_url,
        client_id: CLIENT_ID.to_string(),
        client_secret: CLIENT_SECRET.to_string(),
        redirect_url: Some(REDIRECT_URL.to_string()),
        group_claim: Some("groups".to_string()),
        group_mappings: vec![GroupMapping {
            group: MAPPED_GROUP.to_string(),
            tenant: MAPPED_TENANT.to_string(),
            role: MembershipRole::Member,
        }],
        ..OidcConfig::default()
    };
    let catalog = Catalog::new("sqlite::memory:")
        .await
        .expect("in-memory catalog opens");
    let mut config = Configuration {
        auth: AuthConfig {
            tenants: vec![tenant_config()],
            oidc: Some(oidc),
            ..Default::default()
        },
        ..Default::default()
    };
    config.mcp.oauth = OAuthConfig {
        enabled: true,
        issuer_url: Some("http://localhost:3000".to_string()),
        resource_url: Some(MCP_RESOURCE.to_string()),
        ..Default::default()
    };
    // Config-defined tenants live in `config.auth.tenants`, not the catalog;
    // the callback's group-mapping step only trusts tenants the *catalog*
    // knows about (`state.catalog().list_tenants()`), so this sync is what
    // makes `MAPPED_TENANT` pass that check (mirrors `test_state_with_mapping`
    // in `src/router/src/endpoints/oidc.rs`'s unit tests).
    catalog
        .sync_config_tenants(&config.auth)
        .await
        .expect("config tenants sync into the catalog");
    RouterAppState::new(catalog, config)
}

/// Polls the OIDC runtime's discovery status until the real Keycloak
/// container answers, or panics after a generous timeout.
async fn wait_for_oidc_ready(state: &RouterAppState) {
    let runtime = state.oidc().expect("oidc is configured for this test");
    for _ in 0..600 {
        if runtime.provider().await.is_some() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("OIDC discovery against the Keycloak container did not succeed in time");
}

struct StartedLogin {
    authorization_url: String,
    pending_cookie: String,
}

/// Drives `GET /ui/session/oidc/start` through the real router (in-process,
/// via `oneshot` — no TCP listener needed since nothing else needs to dial
/// back into this app; only the IdP call below goes over the network).
async fn drive_start(app: &axum::Router) -> StartedLogin {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/ui/session/oidc/start")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::FOUND,
        "start should redirect to the IdP"
    );
    let authorization_url = response
        .headers()
        .get(header::LOCATION)
        .expect("start sets a Location header")
        .to_str()
        .expect("Location header is ASCII")
        .to_string();
    let pending_cookie = response
        .headers()
        .get(header::SET_COOKIE)
        .expect("start sets the pending-login cookie")
        .to_str()
        .expect("Set-Cookie header is ASCII")
        .split(';')
        .next()
        .expect("cookie has a name=value pair")
        .to_string();
    StartedLogin {
        authorization_url,
        pending_cookie,
    }
}

/// Extracts the `action` attribute of Keycloak's `<form id="kc-form-login">`
/// (`themes/.../base/login/login.ftl`: `<form id="kc-form-login" ...
/// action="${url.loginAction}" method="post">`, fields named `username` and
/// `password`). Deliberately not a full HTML parser: the login page's only
/// unpredictable, request-scoped piece is this one URL (it carries a
/// one-time `session_code`/`execution`/`tab_id` set), so a bounded string
/// search is simpler than pulling in an HTML-parsing dependency for it.
fn extract_login_form_action(html: &str) -> String {
    let form_start = html
        .find(r#"id="kc-form-login""#)
        .expect("login page contains the kc-form-login form");
    let after_form = &html[form_start..];
    let action_key = "action=\"";
    let action_start = after_form
        .find(action_key)
        .map(|offset| form_start + offset + action_key.len())
        .expect("kc-form-login has an action attribute");
    let action_end = html[action_start..]
        .find('"')
        .map(|offset| action_start + offset)
        .expect("action attribute value is terminated");
    html[action_start..action_end].replace("&amp;", "&")
}

/// Cookies accumulate name=value pairs across requests, mimicking (just
/// enough of) a browser's cookie jar without needing reqwest's `cookies`
/// feature — pulling that in forces a wide dependency-graph recompile for a
/// feature nothing else in this crate needs (Keycloak's login flow sets an
/// `AUTH_SESSION_ID`/`KC_RESTART` pair on the GET that the following POST
/// must echo back).
#[derive(Default)]
struct CookieJar(std::collections::HashMap<String, String>);

impl CookieJar {
    fn record(&mut self, response: &reqwest::Response) {
        for value in response.headers().get_all(reqwest::header::SET_COOKIE) {
            let Ok(value) = value.to_str() else { continue };
            if let Some(pair) = value.split(';').next()
                && let Some((name, value)) = pair.split_once('=')
            {
                self.0
                    .insert(name.trim().to_string(), value.trim().to_string());
            }
        }
    }

    fn header(&self) -> String {
        self.0
            .iter()
            .map(|(name, value)| format!("{name}={value}"))
            .collect::<Vec<_>>()
            .join("; ")
    }
}

/// Acts as the browser: visits the authorization URL the router just
/// produced, submits the Keycloak login form for the seeded test user, and
/// returns the `(code, state)` pair carried by the final redirect toward
/// `REDIRECT_URL`. Redirects are followed manually (never automatically)
/// because the final hop targets `REDIRECT_URL`, which is never actually
/// reachable (see its doc comment) — only its query string matters.
async fn perform_keycloak_login(authorization_url: &str) -> (String, String) {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("reqwest client builds");
    let mut cookies = CookieJar::default();

    let login_page = client
        .get(authorization_url)
        .send()
        .await
        .expect("GET the authorization endpoint");
    assert_eq!(
        login_page.status(),
        reqwest::StatusCode::OK,
        "expected the Keycloak login page (no other identity providers are configured on this realm)"
    );
    cookies.record(&login_page);
    let html = login_page.text().await.expect("login page body reads");
    let action = extract_login_form_action(&html);

    let submitted = client
        .post(&action)
        .header(reqwest::header::COOKIE, cookies.header())
        .form(&[("username", USER_EMAIL), ("password", USER_PASSWORD)])
        .send()
        .await
        .expect("POST the login form");
    assert_eq!(
        submitted.status(),
        reqwest::StatusCode::FOUND,
        "a correct login should redirect toward the client's redirect_uri"
    );
    cookies.record(&submitted);
    let mut location = submitted
        .headers()
        .get(reqwest::header::LOCATION)
        .expect("login redirect has a Location header")
        .to_str()
        .expect("Location header is ASCII")
        .to_string();

    // `consentRequired` is `false` on the test client (see
    // `realm_import_json`), so this should already be `REDIRECT_URL`.
    // Following a few more same-origin hops defensively costs nothing and
    // survives a future realm default reintroducing an intermediate step.
    for _ in 0..3 {
        if location.starts_with(REDIRECT_URL) || !location.contains("/realms/") {
            break;
        }
        let hop = client
            .get(&location)
            .header(reqwest::header::COOKIE, cookies.header())
            .send()
            .await
            .expect("follow an intermediate Keycloak redirect");
        cookies.record(&hop);
        location = hop
            .headers()
            .get(reqwest::header::LOCATION)
            .expect("intermediate redirect has a Location header")
            .to_str()
            .expect("Location header is ASCII")
            .to_string();
    }

    let redirect_url = Url::parse(&location).expect("final redirect target parses as a URL");
    let code = redirect_url
        .query_pairs()
        .find(|(key, _)| key == "code")
        .map(|(_, value)| value.into_owned())
        .expect("final redirect carries an authorization code");
    let state = redirect_url
        .query_pairs()
        .find(|(key, _)| key == "state")
        .map(|(_, value)| value.into_owned())
        .expect("final redirect carries the echoed state");
    (code, state)
}

#[tokio::test]
async fn sso_login_jit_provisions_user_and_grants_mapped_membership() {
    let keycloak = start_keycloak().await;
    let port = keycloak
        .get_host_port_ipv4(8080)
        .await
        .expect("Keycloak's mapped host port");
    // Keycloak's default ("request") hostname provider derives the `iss`
    // claim and every discovery-document URL from whatever Host header the
    // client used to reach it, so this issuer URL is self-consistent with no
    // circular "the container needs to know its own host port before it
    // starts" problem (unlike Dex, whose issuer is a static, pre-baked
    // config value).
    let issuer_url = format!("http://127.0.0.1:{port}/realms/{REALM}");

    let state = router_state_with_oidc(issuer_url.clone()).await;
    wait_for_oidc_ready(&state).await;
    let catalog = state.catalog().clone();
    let app = create_router(state);

    let login = drive_start(&app).await;
    let (code, idp_state) = perform_keycloak_login(&login.authorization_url).await;

    let query = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("code", &code)
        .append_pair("state", &idp_state)
        .finish();
    let callback_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/ui/session/oidc/callback?{query}"))
                .header(header::COOKIE, &login.pending_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        callback_response.status(),
        StatusCode::FOUND,
        "a valid callback should redirect on success, not bounce to /?sso_error=1"
    );
    let session_cookie = callback_response
        .headers()
        .get_all(header::SET_COOKIE)
        .iter()
        .map(|value| value.to_str().unwrap())
        .find(|value| value.starts_with("signaldb_session="))
        .map(|value| value.split(';').next().unwrap().to_string())
        .expect("a successful SSO login sets a signaldb_session cookie");

    // Assertion 1: SSO login succeeds and `whoami` names the user.
    let whoami_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/whoami")
                .header(header::COOKIE, &session_cookie)
                .header("x-tenant-id", MAPPED_TENANT)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(whoami_response.status(), StatusCode::OK);
    let whoami_body = axum::body::to_bytes(whoami_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let whoami_body: serde_json::Value = serde_json::from_slice(&whoami_body).unwrap();
    assert_eq!(whoami_body["user"]["email"], USER_EMAIL);

    // Assertion 2: the user was JIT-provisioned — exists afterward, with no
    // password and a linked OIDC identity.
    let user = catalog
        .get_user_by_email(USER_EMAIL)
        .await
        .unwrap()
        .expect("SSO login JIT-provisions the user by verified email");
    assert!(
        user.password_hash.is_none(),
        "an SSO-only, JIT-provisioned user carries no password"
    );
    assert_eq!(user.oidc_issuer.as_deref(), Some(issuer_url.as_str()));

    // Assertion 3: the mapped membership was granted at the mapped role,
    // sourced from the group mapping (not a local grant).
    let membership = catalog
        .get_tenant_membership(&user.id, MAPPED_TENANT)
        .await
        .unwrap()
        .expect("group_mappings granted a membership in the mapped tenant");
    assert_eq!(membership.role, MembershipRole::Member);
    assert_eq!(membership.granted_by, GrantSource::OidcMapping);

    // Assertion 4: MCP OAuth consent proceeds over the SSO-issued session,
    // exactly as `tests/oauth_connector_flow.rs` drives it over a password
    // session — dynamic client registration, then a consent decision
    // authenticated solely by `session_cookie`.
    let register_response = app
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
    assert_eq!(register_response.status(), StatusCode::CREATED);
    let register_body = axum::body::to_bytes(register_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let register_body: serde_json::Value = serde_json::from_slice(&register_body).unwrap();
    let mcp_client_id = register_body["client_id"].as_str().unwrap().to_string();

    let decision_body = serde_json::json!({
        "client_id": mcp_client_id,
        "redirect_uri": "https://claude.ai/cb",
        "code_challenge": MCP_PKCE_CHALLENGE,
        "scope": "traces:read",
        "resource": MCP_RESOURCE,
        "tenant": MAPPED_TENANT,
        "approved": true,
    })
    .to_string();
    let decision_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/oauth/authorize/decision")
                .header("content-type", "application/json")
                .header(header::COOKIE, &session_cookie)
                .body(Body::from(decision_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        decision_response.status(),
        StatusCode::OK,
        "MCP OAuth consent must proceed over an SSO-issued session"
    );
    let decision_body = axum::body::to_bytes(decision_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let decision_body: serde_json::Value = serde_json::from_slice(&decision_body).unwrap();
    assert!(
        decision_body["redirect"]
            .as_str()
            .unwrap()
            .contains("code="),
        "consent decision should hand back an authorization code"
    );
}
