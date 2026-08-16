//! Throttled commands retry through the SDK policy, then fail distinctly
//! (`cli-command-surface`: "Throttled commands retry, then exit distinctly").
//!
//! One test function: the fail-fast switch is process-wide, so the phases run
//! sequentially rather than as parallel tests racing on it.

use std::time::Duration;

use clap::Parser;
use signaldb_cli::commands::Cli;
use signaldb_cli::retry::{EXIT_THROTTLED, throttled, throttled_message};

fn cli(server_url: &str, extra: &[&str]) -> Cli {
    let mut args = vec![
        "signaldb-cli",
        "--url",
        server_url,
        "--admin-key",
        "sk-admin",
    ];
    args.extend_from_slice(extra);
    args.extend_from_slice(&["admin", "tenant", "list"]);
    Cli::try_parse_from(args).expect("parses")
}

#[tokio::test]
async fn throttled_command_reports_the_wait_and_the_throttled_exit_code() {
    assert_eq!(EXIT_THROTTLED, 4);

    // Phase 1: retries exhausted. Every attempt is throttled with a zero
    // wait, so the default policy sends its full budget of four requests and
    // the final error still names the server-stated wait.
    let mut server = mockito::Server::new_async().await;
    let always = server
        .mock("GET", "/api/v1/admin/tenants")
        .with_status(429)
        .with_header("retry-after", "0")
        .with_header("content-type", "application/json")
        .with_body(r#"{"error":"rate_limited","message":"slow down"}"#)
        .expect(4)
        .create_async()
        .await;
    let err = cli(&server.url(), &[])
        .run()
        .await
        .expect_err("still throttled after the budget");
    always.assert_async().await;
    let info = throttled(&err).expect("recognised as throttled");
    assert_eq!(info.retry_after, Some(Duration::ZERO));
    assert_eq!(
        throttled_message(&info),
        "rate limited; server asked to retry in 0s"
    );

    // Phase 2: `--no-retry` fails fast on the first 429 and reports the
    // server's wait verbatim.
    let mut server = mockito::Server::new_async().await;
    let once = server
        .mock("GET", "/api/v1/admin/tenants")
        .with_status(429)
        .with_header("retry-after", "5")
        .expect(1)
        .create_async()
        .await;
    let started = std::time::Instant::now();
    let err = cli(&server.url(), &["--no-retry"])
        .run()
        .await
        .expect_err("throttled");
    once.assert_async().await;
    assert!(started.elapsed() < Duration::from_secs(1), "no wait");
    let info = throttled(&err).expect("recognised as throttled");
    assert_eq!(
        throttled_message(&info),
        "rate limited; server asked to retry in 5s"
    );

    // Phase 3: a non-throttling failure is not classified as throttled, and
    // the switch does not stick across invocations that don't pass it.
    let mut server = mockito::Server::new_async().await;
    let _forbidden = server
        .mock("GET", "/api/v1/admin/tenants")
        .with_status(403)
        .create_async()
        .await;
    let err = cli(&server.url(), &[]).run().await.expect_err("403");
    assert!(throttled(&err).is_none());
    assert!(!signaldb_cli::retry::no_retry());
}
