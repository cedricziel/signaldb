//! # SignalDB signal producer
//!
//! Generates realistic OpenTelemetry traces, logs and metrics for two synthetic
//! microservice estates — a **rideshare** platform and an **online shop** — and
//! ships them to an OTLP endpoint (SignalDB's acceptor by default).
//!
//! The data is designed to be *correlatable*: distributed traces cross service
//! boundaries with a shared `trace_id`, logs are emitted inside the active span
//! so they join to the trace, histogram measurements carry trace exemplars, and
//! infrastructure metrics share the same `service.*` / `k8s.*` / `cloud.*`
//! resource attributes as the spans they accompany. Every attribute follows the
//! OpenTelemetry semantic conventions.
//!
//! See [`topology`] for the estates and services, [`fleet`] for the per-service
//! SDK pipelines, [`scenarios`] for the traffic patterns, and [`infra`] for the
//! host/Kubernetes metric pass.

mod emit;
mod fleet;
mod infra;
mod scenarios;
mod topology;

use std::time::Duration;

use anyhow::Context as _;
use clap::Parser;

use crate::fleet::Fleet;

#[derive(Debug, Parser)]
#[command(author, version, about = "SignalDB realistic OTLP signal producer")]
struct Cli {
    /// OTLP/gRPC endpoint to export to.
    #[arg(long, default_value = "http://localhost:4317")]
    endpoint: String,

    /// Which estate(s) to generate traffic for: `rideshare`, `shop`, or `all`.
    #[arg(long, default_value = "all")]
    estate: String,

    /// Seconds between generation ticks.
    #[arg(long, default_value_t = 5)]
    interval: u64,

    /// Number of ticks to run (0 = run until Ctrl+C).
    #[arg(long, default_value_t = 0)]
    count: u64,

    /// Distributed traces to emit per estate per tick.
    #[arg(long, default_value_t = 4)]
    traces_per_tick: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_ansi(false)
        .compact()
        .init();

    let cli = Cli::parse();
    let estates = topology::estates(&cli.estate);
    if estates.is_empty() {
        anyhow::bail!(
            "no estates matched selector '{}'. expected: rideshare, shop, all",
            cli.estate
        );
    }

    let namespaces: Vec<&str> = estates.iter().map(|e| e.namespace).collect();
    eprintln!(
        "starting signal producer: endpoint={} estates=[{}] interval={}s count={} traces_per_tick={}",
        cli.endpoint,
        namespaces.join(", "),
        cli.interval,
        cli.count,
        cli.traces_per_tick,
    );

    let fleet = Fleet::build(&cli.endpoint, &estates)
        .context("failed to build service telemetry pipelines")?;

    let interval = Duration::from_secs(cli.interval.max(1));
    run_loop(
        &fleet,
        &namespaces,
        cli.count,
        cli.traces_per_tick,
        interval,
    )
    .await;

    fleet.shutdown();
    eprintln!("signal producer stopped");
    Ok(())
}

/// Main generation loop: each tick emits `traces_per_tick` scenarios per estate
/// plus one infrastructure-metric sample per service.
async fn run_loop(
    fleet: &Fleet,
    namespaces: &[&str],
    count: u64,
    traces_per_tick: u64,
    interval: Duration,
) {
    let mut rng = rand::rng();
    let mut tick = 0_u64;

    loop {
        if count > 0 && tick >= count {
            eprintln!("completed requested {count} ticks");
            break;
        }
        tick += 1;

        for namespace in namespaces {
            for _ in 0..traces_per_tick.max(1) {
                scenarios::run(fleet, &mut rng, namespace);
            }
        }
        infra::sample(fleet, &mut rng);

        eprintln!(
            "tick {tick}: emitted {} traces across {} estate(s)",
            traces_per_tick.max(1) * namespaces.len() as u64,
            namespaces.len()
        );

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                eprintln!("received Ctrl+C, shutting down gracefully");
                break;
            }
            _ = tokio::time::sleep(interval) => {}
        }
    }
}
