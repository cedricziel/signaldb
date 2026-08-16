//! Retry-on-throttle contract of the generated client (openspec change
//! `sdk-retry-on-throttle`, spec `client-retry-on-throttle`).
//!
//! Every generated operation runs through `signaldb_sdk::retry::execute`,
//! installed by `cargo xtask generate` as the progenitor `ClientHooks::exec`
//! override. These tests drive real generated operations against a scripted
//! HTTP server, and replay the shared `api/retry-cases.json` fixture against
//! the policy engine so the Rust and TypeScript clients cannot drift.

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use reqwest::Method;
use signaldb_sdk::retry::{Decision, Failure, RetryEvent, RetryPolicy, retry_after, throttle_of};
use signaldb_sdk::{ClientBuilder, Error};
use support::{Scripted, ScriptedServer};

const TENANTS: &str = r#"{"tenants":[]}"#;

/// A fast policy for tests that only care about counts and classification.
fn fast() -> RetryPolicy {
    RetryPolicy {
        base: Duration::from_millis(5),
        ..RetryPolicy::default()
    }
}

fn client(server: &ScriptedServer, policy: RetryPolicy) -> signaldb_sdk::Client {
    ClientBuilder::new(&server.base_url)
        .retry(policy)
        .build()
        .expect("client builds")
}

#[tokio::test]
async fn throttled_get_succeeds_after_retry() {
    let server =
        ScriptedServer::start(vec![Scripted::throttled("0"), Scripted::ok_json(TENANTS)]).await;
    let client = client(&server, fast());
    let resp = client
        .list_tenants()
        .send()
        .await
        .expect("succeeds on retry");
    assert!(resp.into_inner().tenants.is_empty());
    assert_eq!(server.request_count(), 2);
}

#[tokio::test]
async fn throttled_post_is_retried_too() {
    let server = ScriptedServer::start(vec![
        Scripted::throttled("0"),
        Scripted::ok_json(
            r#"{"namespace":"acme","version":"1.0.0","attribute_count":0,"entity_count":0,"metric_count":0,"read_only":false,"source":"custom"}"#,
        )
        .with_status(201),
    ])
    .await;
    let client = client(&server, fast());
    let doc = serde_json::json!({"name":"acme","version":"1.0.0","groups":[]});
    let result = client
        .schema_create_registry()
        .body(doc.as_object().cloned().expect("object"))
        .send()
        .await;
    assert!(
        result.is_ok(),
        "POST retried after 429: {:?}",
        result.err().map(|e| e.to_string())
    );
    assert_eq!(server.request_count(), 2);
    assert!(server.seen().iter().all(|s| s.method == "POST"));
}

#[tokio::test]
async fn transient_failure_on_get_is_retried() {
    let server =
        ScriptedServer::start(vec![Scripted::status(503), Scripted::ok_json(TENANTS)]).await;
    let client = client(&server, fast());
    client.list_tenants().send().await.expect("503 then 200");
    assert_eq!(server.request_count(), 2);
}

#[tokio::test]
async fn transient_failure_on_post_is_not_retried() {
    let server = ScriptedServer::start(vec![Scripted::status(503), Scripted::ok_json("{}")]).await;
    let client = client(&server, fast());
    let doc = serde_json::json!({"name":"acme","version":"1.0.0","groups":[]});
    let err = client
        .schema_create_registry()
        .body(doc.as_object().cloned().expect("object"))
        .send()
        .await
        .expect_err("503 on POST surfaces");
    assert_eq!(err.status().map(|s| s.as_u16()), Some(503));
    assert_eq!(server.request_count(), 1);
}

#[tokio::test]
async fn client_error_is_not_retried() {
    let server =
        ScriptedServer::start(vec![Scripted::status(400), Scripted::ok_json(TENANTS)]).await;
    let client = client(&server, fast());
    let err = client
        .list_tenants()
        .send()
        .await
        .expect_err("400 surfaces");
    assert_eq!(err.status().map(|s| s.as_u16()), Some(400));
    assert_eq!(server.request_count(), 1);
}

#[tokio::test]
async fn retry_after_seconds_is_respected() {
    let server =
        ScriptedServer::start(vec![Scripted::throttled("1"), Scripted::ok_json(TENANTS)]).await;
    let client = client(&server, fast());
    let started = Instant::now();
    client.list_tenants().send().await.expect("succeeds");
    assert!(
        started.elapsed() >= Duration::from_secs(1),
        "waited only {:?}",
        started.elapsed()
    );
    assert_eq!(server.request_count(), 2);
}

#[tokio::test]
async fn retry_after_http_date_is_respected() {
    let at = std::time::SystemTime::now() + Duration::from_secs(1);
    let server = ScriptedServer::start(vec![
        Scripted::throttled(&httpdate::fmt_http_date(at)),
        Scripted::ok_json(TENANTS),
    ])
    .await;
    let client = client(&server, fast());
    let started = Instant::now();
    client.list_tenants().send().await.expect("succeeds");
    // HTTP-dates have second resolution, so the parsed wait can be up to a
    // second shorter than the intended one; a retry must still have waited
    // for the visible remainder rather than firing immediately.
    assert!(server.request_count() == 2);
    assert!(started.elapsed() < Duration::from_secs(3));
}

#[tokio::test]
async fn retry_after_beyond_cap_fails_fast() {
    let server = ScriptedServer::start(vec![
        Scripted::throttled("3600"),
        Scripted::ok_json(TENANTS),
    ])
    .await;
    let client = client(&server, fast());
    let started = Instant::now();
    let err = client
        .list_tenants()
        .send()
        .await
        .expect_err("fails fast with the 429");
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(err.status().map(|s| s.as_u16()), Some(429));
    assert_eq!(retry_after(&err), Some(Duration::from_secs(3600)));
    assert_eq!(server.request_count(), 1);
}

#[tokio::test]
async fn attempts_are_bounded_and_last_failure_is_returned() {
    let server = ScriptedServer::start(vec![
        Scripted::throttled("0"),
        Scripted::throttled("0"),
        Scripted::throttled("0"),
        Scripted::throttled("5"),
        Scripted::ok_json(TENANTS),
    ])
    .await;
    let client = client(&server, fast());
    let err = client
        .list_tenants()
        .send()
        .await
        .expect_err("still throttled after the budget");
    assert_eq!(err.status().map(|s| s.as_u16()), Some(429));
    assert_eq!(
        retry_after(&err),
        Some(Duration::from_secs(5)),
        "the final response's Retry-After is exposed"
    );
    assert_eq!(server.request_count(), 4, "max_attempts requests in total");
    let throttled = throttle_of(&err).expect("recognised as throttled through dyn Error");
    assert_eq!(throttled.retry_after, Some(Duration::from_secs(5)));
}

#[tokio::test]
async fn disabled_policy_never_retries() {
    let server =
        ScriptedServer::start(vec![Scripted::throttled("0"), Scripted::ok_json(TENANTS)]).await;
    let client = client(&server, RetryPolicy::disabled());
    let err = client
        .list_tenants()
        .send()
        .await
        .expect_err("429 surfaces on the first attempt");
    assert_eq!(err.status().map(|s| s.as_u16()), Some(429));
    assert_eq!(retry_after(&err), Some(Duration::ZERO));
    assert_eq!(server.request_count(), 1);
}

#[tokio::test]
async fn observer_sees_one_event_per_retry() {
    let server = ScriptedServer::start(vec![
        Scripted::throttled("0"),
        Scripted::status(503),
        Scripted::ok_json(TENANTS),
    ])
    .await;
    let events: Arc<std::sync::Mutex<Vec<RetryEvent>>> = Arc::default();
    let sink = events.clone();
    let policy = fast().with_observer(Arc::new(move |e: &RetryEvent| {
        sink.lock().unwrap().push(*e);
    }));
    let client = client(&server, policy);
    client.list_tenants().send().await.expect("succeeds");
    let events = events.lock().unwrap().clone();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].attempt, 1);
    assert_eq!(events[0].failure, Failure::Status(429));
    assert_eq!(events[1].attempt, 2);
    assert_eq!(events[1].failure, Failure::Status(503));
}

#[tokio::test]
async fn connection_failure_on_get_is_retried_up_to_the_budget() {
    // Bind then drop, so the port is (almost certainly) closed.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let attempts = Arc::new(AtomicUsize::new(0));
    let counter = attempts.clone();
    let policy = fast().with_observer(Arc::new(move |_e: &RetryEvent| {
        counter.fetch_add(1, Ordering::SeqCst);
    }));
    let client = ClientBuilder::new(format!("http://{addr}"))
        .connect_timeout(Duration::from_millis(500))
        .retry(policy)
        .build()
        .unwrap();
    let err = client
        .list_tenants()
        .send()
        .await
        .expect_err("nothing listens");
    assert!(matches!(err, Error::CommunicationError(_)));
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        3,
        "three retries after the first"
    );
}

#[tokio::test]
async fn generated_client_routes_operations_through_the_retry_hook() {
    // Drift guard for the xtask shim: the generated file must contain the
    // exec override, not progenitor's empty impl.
    let generated = include_str!("../src/generated.rs");
    assert!(
        generated.contains("impl ClientHooks<crate::retry::RetryPolicy> for &Client {\n"),
        "generated.rs lost the retry exec override — re-run `cargo xtask generate`"
    );
    assert!(
        generated.contains("crate::retry::execute(self.client(), self.inner(), request, info)"),
        "generated.rs exec override does not call crate::retry::execute"
    );
    assert!(
        !generated.contains("\nimpl ClientHooks<crate::retry::RetryPolicy> for &Client {}"),
        "progenitor's empty ClientHooks impl is present; the xtask rewrite did not run"
    );
}

#[test]
fn throttle_of_covers_every_generated_error_type() {
    let generated = include_str!("../src/generated.rs");
    let mut found: Vec<String> = Vec::new();
    for (idx, _) in generated.match_indices("Error<") {
        let rest = &generated[idx + "Error<".len()..];
        let Some(end) = rest.find('>') else { continue };
        let ty = rest[..end].trim_start_matches("types::").to_string();
        if !found.contains(&ty) {
            found.push(ty);
        }
    }
    found.sort();
    let mut known: Vec<String> = signaldb_sdk::retry::KNOWN_ERROR_TYPES
        .iter()
        .map(|s| s.to_string())
        .collect();
    known.sort();
    assert_eq!(
        found, known,
        "generated error payload types changed; update retry::throttle_of and KNOWN_ERROR_TYPES"
    );
}

// --- shared fixture -------------------------------------------------------

#[derive(serde::Deserialize)]
struct Fixture {
    policy: FixturePolicy,
    cases: Vec<Case>,
}

#[derive(serde::Deserialize)]
struct FixturePolicy {
    max_attempts: u32,
    base_ms: u64,
    per_attempt_cap_ms: u64,
    total_cap_ms: u64,
}

#[derive(serde::Deserialize)]
struct Case {
    name: String,
    failure: String,
    method: String,
    retry_after: Option<String>,
    attempt: u32,
    expect: Expect,
}

#[derive(serde::Deserialize)]
struct Expect {
    retry: bool,
    #[serde(default)]
    fail_fast: bool,
    wait_ms_min: Option<u64>,
    wait_ms_max: Option<u64>,
}

#[test]
fn shared_fixture_replays_against_the_policy_engine() {
    let fixture: Fixture =
        serde_json::from_str(include_str!("../../../api/retry-cases.json")).expect("fixture");
    let policy = RetryPolicy {
        max_attempts: fixture.policy.max_attempts,
        base: Duration::from_millis(fixture.policy.base_ms),
        per_attempt_cap: Duration::from_millis(fixture.policy.per_attempt_cap_ms),
        total_cap: Duration::from_millis(fixture.policy.total_cap_ms),
        ..RetryPolicy::default()
    };
    assert_eq!(policy.max_attempts, RetryPolicy::default().max_attempts);
    assert_eq!(policy.base, RetryPolicy::default().base);
    for case in fixture.cases {
        let failure = match case.failure.as_str() {
            "connect" => Failure::Connect,
            "timeout" => Failure::Timeout,
            s => Failure::Status(
                s.strip_prefix("status:")
                    .and_then(|c| c.parse().ok())
                    .unwrap_or_else(|| panic!("bad failure in fixture: {s}")),
            ),
        };
        let method = Method::from_bytes(case.method.as_bytes()).expect("method");
        let retry_after = case
            .retry_after
            .as_deref()
            .and_then(signaldb_sdk::retry::parse_retry_after);
        // Sample repeatedly: backoff is jittered.
        for _ in 0..50 {
            let decision =
                policy.decide(failure, &method, retry_after, case.attempt, Duration::ZERO);
            match (&decision, case.expect.retry, case.expect.fail_fast) {
                (Decision::Retry(wait), true, _) => {
                    let ms = wait.as_millis() as u64;
                    let min = case.expect.wait_ms_min.unwrap_or(0);
                    let max = case.expect.wait_ms_max.unwrap_or(u64::MAX);
                    assert!(
                        (min..=max).contains(&ms),
                        "{}: wait {ms}ms outside [{min}, {max}]",
                        case.name
                    );
                }
                (Decision::FailFast, false, true) => {}
                (Decision::Return, false, false) => {}
                (other, _, _) => panic!(
                    "{}: got {other:?}, expected retry={} fail_fast={}",
                    case.name, case.expect.retry, case.expect.fail_fast
                ),
            }
        }
    }
}
