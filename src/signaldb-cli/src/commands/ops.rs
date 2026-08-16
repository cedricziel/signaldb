//! The `ops` command: operational control (compaction) via the SDK.
//!
//! `signaldb ops compact run|status|dry-run` proxies to the router's
//! `/api/v1/ops/*` endpoints (admin-authenticated), which forward to the
//! compactor. Only compaction control exists today; retention/snapshot/orphan
//! control is a follow-up (see the `operational-control-api` spec).

use clap::Subcommand;
use signaldb_sdk::Client;

#[derive(Subcommand)]
pub enum OpsAction {
    /// Compaction control
    Compact {
        #[command(subcommand)]
        action: CompactAction,
    },
}

#[derive(Subcommand)]
pub enum CompactAction {
    /// Trigger a compaction pass now
    Run,
    /// Show active compaction leases and metrics
    Status,
    /// Plan compaction candidates without executing (read-only preview)
    DryRun,
}

impl OpsAction {
    pub async fn run(self, client: &Client) -> anyhow::Result<()> {
        let result = match self {
            OpsAction::Compact { action } => match action {
                CompactAction::Run => client.ops_compact().send().await.map(|r| r.into_inner()),
                CompactAction::Status => client
                    .ops_compact_status()
                    .send()
                    .await
                    .map(|r| r.into_inner()),
                CompactAction::DryRun => client
                    .ops_compact_dry_run()
                    .send()
                    .await
                    .map(|r| r.into_inner()),
            },
        };
        let value = result.map_err(|e| anyhow::Error::new(e).context("ops request failed"))?;
        crate::commands::print_json(&value)?;
        Ok(())
    }
}
