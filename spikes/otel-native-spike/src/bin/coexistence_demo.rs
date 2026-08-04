//! Task 0.2 demo — typed-layout + promotion-evolution viability scan.
//!
//! Runs the spike in `otel_native_spike::coexistence` and prints a PASS/FAIL
//! line per probe, followed by the single-scan output over pre- and
//! post-promotion file generations (both typed layout — the legacy
//! `Map<String,String>` coexistence scope was dropped for the one-shot
//! cutover). Exit code is non-zero if any probe failed.

use anyhow::Result;
use otel_native_spike::coexistence;

#[tokio::main]
async fn main() -> Result<()> {
    let report = coexistence::run().await?;

    println!("\n=== dual-layout coexistence probes ===\n");
    let mut failed = 0;
    for p in &report.probes {
        let tag = if p.pass { "PASS" } else { "FAIL" };
        if !p.pass {
            failed += 1;
        }
        println!(
            "[{tag}] {}\n       {}\n",
            p.name,
            p.note.replace('\n', "\n       ")
        );
    }

    if !report.rendered_scan.is_empty() {
        println!("=== single DataFusion scan over gen-1 + gen-2 files ===");
        println!("{}", report.rendered_scan);
    }

    println!(
        "\n=== {}/{} probes passed ===",
        report.probes.len() - failed,
        report.probes.len()
    );

    if failed > 0 {
        std::process::exit(1);
    }
    Ok(())
}
