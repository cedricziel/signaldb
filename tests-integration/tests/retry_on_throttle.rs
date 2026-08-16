//! Retry-on-throttle across the client surfaces (openspec change
//! `sdk-retry-on-throttle`; specs `client-retry-on-throttle`,
//! `client-surface-parity`, `cli-command-surface`).
//!
//! End-to-end: a real router with `max_query_requests_per_sec = 1,
//! burst_seconds = 1` throttles the second of two back-to-back requests. The
//! SDK's default policy absorbs that (three sequential requests all succeed);
//! `RetryPolicy::disabled()` sees the `429` with its `Retry-After`; the CLI
//! goes through the same client and so succeeds three times in a row, while
//! `--no-retry` fails fast on the throttled call with the throttled exit
//! code's error.
//!
//! Parity: the CLI and MCP crates contain no bare `reqwest` client
//! construction (their own source-scan tests enforce it; this asserts those
//! tests exist and the crates reach the SDK builder), and the UI's generated
//! client installs `retryingFetch`.

use std::path::Path;
use std::time::Duration;

use clap::Parser;
use common::auth::Authenticator;
use common::catalog::Catalog;
use common::config::{
    ApiKeyConfig, AuthConfig, Configuration, DatasetConfig, TenantConfig, TenantLimits,
};
use router::{RouterAppState, create_router};
use signaldb_sdk::retry::retry_after;
use signaldb_sdk::{ClientBuilder, Error, RetryPolicy};
use tokio::net::TcpListener;

const TENANT: &str = "acme";
const KEY: &str = "sk-acme-throttle";

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
            key: KEY.to_string(),
            name: Some("throttle".to_string()),
        }],
        schema_config: None,
        limits: None,
    }
}

/// A router whose query surface allows one request per second with a
/// one-second burst: the first request passes, an immediate second is
/// throttled with `Retry-After`.
async fn serve_throttling_router() -> String {
    let catalog = Catalog::new("sqlite::memory:").await.unwrap();
    let config = Configuration {
        auth: AuthConfig {
            default_limits: TenantLimits {
                max_query_requests_per_sec: Some(1),
                burst_seconds: 1.0,
                ..Default::default()
            },
            tenants: vec![tenant_config()],
            ..Default::default()
        },
        ..Default::default()
    };
    catalog.sync_config_tenants(&config.auth).await.unwrap();
    catalog
        .upsert_scoped_api_key(
            TENANT,
            &Authenticator::hash_api_key(KEY),
            Some("throttle"),
            None,
            Some(&["schema:read".to_string()]),
            None,
        )
        .await
        .unwrap();
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

fn client(base_url: &str, policy: RetryPolicy) -> signaldb_sdk::Client {
    ClientBuilder::new(base_url)
        .bearer(KEY)
        .tenant(TENANT)
        .retry(policy)
        .build()
        .expect("client builds")
}

#[tokio::test]
async fn sdk_default_policy_absorbs_router_throttling() {
    let base_url = serve_throttling_router().await;
    let sdk = client(&base_url, RetryPolicy::default());
    for i in 0..3 {
        let resp = sdk
            .schema_list_registries()
            .send()
            .await
            .unwrap_or_else(|e| panic!("request {i} failed through the default policy: {e}"));
        assert!(!resp.into_inner().registries.is_empty());
    }
}

#[tokio::test]
async fn sdk_disabled_policy_sees_the_429_with_retry_after() {
    let base_url = serve_throttling_router().await;
    let sdk = client(&base_url, RetryPolicy::disabled());
    sdk.schema_list_registries()
        .send()
        .await
        .expect("first request is within the burst");
    let err = sdk
        .schema_list_registries()
        .send()
        .await
        .expect_err("second back-to-back request is throttled");
    assert_eq!(err.status().map(|s| s.as_u16()), Some(429));
    let wait = retry_after(&err).expect("429 carries Retry-After");
    assert!(
        wait >= Duration::from_secs(1),
        "server-stated wait: {wait:?}"
    );
    assert!(
        matches!(err, Error::ErrorResponse(_) | Error::UnexpectedResponse(_)),
        "a response-shaped error"
    );
    assert!(signaldb_sdk::retry::throttle_of(&err).is_some());
}

fn cli(base_url: &str, extra: &[&str]) -> signaldb_cli::commands::Cli {
    let mut args = vec!["signaldb-cli"];
    args.extend_from_slice(extra);
    args.extend_from_slice(&[
        "schema",
        "registry",
        "list",
        "--url",
        base_url,
        "--api-key",
        KEY,
        "--tenant-id",
        TENANT,
    ]);
    signaldb_cli::commands::Cli::try_parse_from(args).expect("parses")
}

#[tokio::test]
async fn cli_retries_router_throttling_and_no_retry_fails_fast() {
    let base_url = serve_throttling_router().await;
    // Three back-to-back invocations all succeed: the SDK policy absorbs the
    // throttled ones.
    for i in 0..3 {
        cli(&base_url, &[])
            .run()
            .await
            .unwrap_or_else(|e| panic!("invocation {i} failed: {e:#}"));
    }
    // Wait for the bucket to refill, then fail fast: the first call passes,
    // the immediate second is a throttled error the binary maps to exit 4.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    cli(&base_url, &["--no-retry"])
        .run()
        .await
        .expect("first call within the burst");
    let err = cli(&base_url, &["--no-retry"])
        .run()
        .await
        .expect_err("second immediate call is throttled");
    let throttled = signaldb_cli::retry::throttled(&err).expect("main maps this to EXIT_THROTTLED");
    assert!(throttled.retry_after.is_some());
    assert_eq!(signaldb_cli::retry::EXIT_THROTTLED, 4);
    // Leave the process-wide switch as we found it for other tests.
    signaldb_cli::retry::set_no_retry(false);
}

// ---- parity ----------------------------------------------------------------

fn repo_root() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("tests-integration lives in the workspace root")
        .to_path_buf()
}

fn scan_for_bare_clients(dir: &Path, hits: &mut Vec<String>) {
    for entry in std::fs::read_dir(dir).expect("readable dir") {
        let path = entry.expect("entry").path();
        if path.is_dir() {
            scan_for_bare_clients(&path, hits);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            let source = std::fs::read_to_string(&path).expect("readable file");
            for (n, line) in source.lines().enumerate() {
                let code = line.split("//").next().unwrap_or(line);
                if [
                    "reqwest::Client::builder()",
                    "reqwest::ClientBuilder::new()",
                    "reqwest::Client::new()",
                ]
                .iter()
                .any(|p| code.contains(p))
                {
                    hits.push(format!("{}:{}", path.display(), n + 1));
                }
            }
        }
    }
}

/// `client-surface-parity` — "Throttling is retried on every surface": the
/// CLI and MCP crates build clients only through the SDK builder (which
/// installs the retry policy) and the UI's generated client is configured
/// with `retryingFetch`.
#[test]
fn every_surface_routes_requests_through_the_retrying_client() {
    let root = repo_root();
    for (krate, builder_site) in [
        ("src/signaldb-cli", "src/retry.rs"),
        ("src/mcp-server", "src/lib.rs"),
    ] {
        let mut hits = Vec::new();
        scan_for_bare_clients(&root.join(krate).join("src"), &mut hits);
        assert!(
            hits.is_empty(),
            "{krate} constructs a bare reqwest client (must use signaldb_sdk::ClientBuilder):\n{}",
            hits.join("\n")
        );
        let site = std::fs::read_to_string(root.join(krate).join(builder_site)).unwrap();
        assert!(
            site.contains("ClientBuilder::new("),
            "{krate}/{builder_site} must obtain its client from signaldb_sdk::ClientBuilder"
        );
        assert!(
            root.join(krate)
                .join("tests/client_construction.rs")
                .exists(),
            "{krate} must carry its own client-construction source-scan test"
        );
    }

    let client_ts = std::fs::read_to_string(root.join("src/ui/src/api/client.ts")).unwrap();
    assert!(
        client_ts.contains("fetch: retryingFetch"),
        "src/ui/src/api/client.ts must install retryingFetch on the generated client"
    );
    for legacy in ["session.ts", "tempo.ts", "prom.ts", "pyroscope.ts"] {
        let source = std::fs::read_to_string(root.join("src/ui/src/api").join(legacy)).unwrap();
        assert!(
            !source.contains("await fetch("),
            "src/ui/src/api/{legacy} must call retryingFetch, not raw fetch"
        );
    }
}
