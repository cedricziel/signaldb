//! `signaldb wal dead-letter list|replay|purge`: the operator-facing tool
//! for a WAL's `dead-letter/` directory (#1494). Run on the node that owns
//! the WAL directory (`--wal-dir` is the *base* directory — the same value
//! as `[wal].wal_dir` / `ACCEPTOR_WAL_DIR` / `WRITER_WAL_DIR`).
//!
//! The parsing/orchestration lives here; the actual directory scanning,
//! sweeping, and replay logic is `common::wal::dead_letter`, shared with the
//! acceptor retry consumer and the writer drain loop's own reconciliation.
//!
//! `replay` opens a live [`Wal`] for the target tenant/dataset/signal and
//! re-appends each intact, decodable payload through the same
//! [`Wal::append`] path ingest uses, then removes it from dead-letter. The
//! WAL's owning service (acceptor or writer) is expected to still be
//! running — that is the point, so its normal retry consumer / drain loop
//! picks the replayed entry up. Running replay against a directory whose
//! segments a live process is *also* actively rotating is not something the
//! WAL format coordinates between two processes today, so prefer running
//! replay during a lull in traffic for that tenant, or briefly stop the
//! owning service first if the WAL is under heavy write load.

use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::{Args as ClapArgs, Parser, Subcommand};
use common::wal::dead_letter::{self, DeadLetterKind};
use common::wal::{Wal, WalConfig, WalOperation};

#[derive(Parser, Debug)]
pub struct Args {
    #[command(subcommand)]
    pub command: WalCommand,
}

#[derive(Subcommand, Debug)]
pub enum WalCommand {
    /// Inspect, replay, or purge a WAL's dead-letter directory
    #[command(subcommand)]
    DeadLetter(DeadLetterCommand),
}

#[derive(Subcommand, Debug)]
pub enum DeadLetterCommand {
    /// Print dead-lettered entry/byte counts, by kind
    List(DeadLetterArgs),
    /// Re-append intact, decodable payloads to the live WAL and remove them
    /// from dead-letter
    Replay(DeadLetterArgs),
    /// Delete dead-letter entries
    Purge(DeadLetterArgs),
}

#[derive(ClapArgs, Debug)]
pub struct DeadLetterArgs {
    /// Base WAL directory (e.g. `/data/wal/acceptor`) — the same value as
    /// `[wal].wal_dir` / `ACCEPTOR_WAL_DIR` / `WRITER_WAL_DIR`
    #[arg(long)]
    pub wal_dir: PathBuf,
    #[arg(long)]
    pub tenant: String,
    #[arg(long)]
    pub dataset: String,
    /// One of `traces`, `logs`, `metrics`, `profiles`
    #[arg(long)]
    pub signal: String,
    /// Restrict to `rejected` or `unreadable`; every entry if omitted
    #[arg(long)]
    pub kind: Option<String>,
    /// Report what would happen without touching disk or the live WAL
    #[arg(long)]
    pub dry_run: bool,
}

impl DeadLetterArgs {
    fn dead_letter_dir(&self) -> PathBuf {
        self.wal_dir
            .join(&self.tenant)
            .join(&self.dataset)
            .join(&self.signal)
            .join("dead-letter")
    }

    fn kind_filter(&self) -> Result<Option<DeadLetterKind>> {
        match self.kind.as_deref() {
            None => Ok(None),
            Some("rejected") => Ok(Some(DeadLetterKind::Rejected)),
            Some("unreadable") => Ok(Some(DeadLetterKind::Unreadable)),
            Some(other) => bail!("--kind must be \"rejected\" or \"unreadable\", got {other:?}"),
        }
    }
}

pub async fn run(args: Args) -> Result<()> {
    match args.command {
        WalCommand::DeadLetter(DeadLetterCommand::List(args)) => list(&args).await,
        WalCommand::DeadLetter(DeadLetterCommand::Replay(args)) => replay(&args).await,
        WalCommand::DeadLetter(DeadLetterCommand::Purge(args)) => purge(&args).await,
    }
}

async fn list(args: &DeadLetterArgs) -> Result<()> {
    let dir = args.dead_letter_dir();
    let entries = dead_letter::list_entries(&dir)
        .await
        .with_context(|| format!("Failed to list {}", dir.display()))?;
    let stats = dead_letter::DeadLetterStats::from_entries(&entries);

    println!("{}", dir.display());
    println!(
        "  rejected:   {} entries, {} bytes",
        stats.rejected_entries, stats.rejected_bytes
    );
    println!(
        "  unreadable: {} entries, {} bytes",
        stats.unreadable_entries, stats.unreadable_bytes
    );
    Ok(())
}

async fn replay(args: &DeadLetterArgs) -> Result<()> {
    let dir = args.dead_letter_dir();
    let kind = args.kind_filter()?;
    let operation = WalOperation::from_signal(&args.signal).with_context(|| {
        format!(
            "--signal must be one of traces, logs, metrics, profiles, got {:?}",
            args.signal
        )
    })?;

    let mut config = WalConfig::with_defaults(args.wal_dir.clone()).for_tenant_dataset(
        &args.tenant,
        &args.dataset,
        &args.signal,
    );
    // This process never starts the background flush timer and exits as
    // soon as the replay pass is done, so an entry sitting in the in-memory
    // buffer past that point is silently lost rather than merely delayed.
    // `append` flushes synchronously, inline, once the buffer reaches this
    // cap (see `Wal::append`) — 1 makes every single replayed entry durable
    // before `dead_letter::replay` deletes its dead-letter pair.
    config.max_buffer_entries = 1;
    let wal = Wal::new(config).await.with_context(|| {
        format!(
            "Failed to open the live WAL for {}/{}/{} under {}",
            args.tenant,
            args.dataset,
            args.signal,
            args.wal_dir.display()
        )
    })?;

    let result = dead_letter::replay(&dir, &wal, operation, kind, args.dry_run)
        .await
        .with_context(|| format!("Failed to replay {}", dir.display()))?;

    let verb = if args.dry_run {
        "Would replay"
    } else {
        "Replayed"
    };
    println!(
        "{verb} {} entries; {} left in place (no payload or undecodable)",
        result.replayed, result.failed
    );
    Ok(())
}

async fn purge(args: &DeadLetterArgs) -> Result<()> {
    let dir = args.dead_letter_dir();
    let kind = args.kind_filter()?;
    let result = dead_letter::purge(&dir, kind, args.dry_run)
        .await
        .with_context(|| format!("Failed to purge {}", dir.display()))?;

    let verb = if args.dry_run {
        "Would delete"
    } else {
        "Deleted"
    };
    println!(
        "{verb} {} entries, {} bytes",
        result.deleted_entries, result.freed_bytes
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `Args::try_parse_from` treats the first element as the program name
    /// (consumed, not parsed as a real argument) exactly like `std::env::args`
    /// would, so callers pass a placeholder ahead of the real arguments.
    fn parse(args: &[&str]) -> Args {
        let with_program_name: Vec<&str> = std::iter::once("signaldb-wal")
            .chain(args.iter().copied())
            .collect();
        Args::try_parse_from(with_program_name).unwrap_or_else(|e| panic!("parse {args:?}: {e}"))
    }

    #[test]
    fn dead_letter_list_parses_required_flags() {
        let args = parse(&[
            "dead-letter",
            "list",
            "--wal-dir",
            "/data/wal/acceptor",
            "--tenant",
            "acme",
            "--dataset",
            "production",
            "--signal",
            "metrics",
        ]);
        let WalCommand::DeadLetter(DeadLetterCommand::List(dl)) = args.command else {
            panic!("expected DeadLetter(List), got {args:?}");
        };
        assert_eq!(dl.wal_dir, PathBuf::from("/data/wal/acceptor"));
        assert_eq!(dl.tenant, "acme");
        assert_eq!(dl.dataset, "production");
        assert_eq!(dl.signal, "metrics");
        assert_eq!(dl.kind, None);
        assert!(!dl.dry_run);
    }

    #[test]
    fn dead_letter_replay_parses_kind_and_dry_run() {
        let args = parse(&[
            "dead-letter",
            "replay",
            "--wal-dir",
            "/data/wal/acceptor",
            "--tenant",
            "acme",
            "--dataset",
            "production",
            "--signal",
            "metrics",
            "--kind",
            "rejected",
            "--dry-run",
        ]);
        let WalCommand::DeadLetter(DeadLetterCommand::Replay(dl)) = args.command else {
            panic!("expected DeadLetter(Replay), got {args:?}");
        };
        assert_eq!(dl.kind.as_deref(), Some("rejected"));
        assert!(dl.dry_run);
    }

    #[test]
    fn dead_letter_dir_joins_tenant_dataset_signal() {
        let args = DeadLetterArgs {
            wal_dir: PathBuf::from("/data/wal/acceptor"),
            tenant: "acme".to_string(),
            dataset: "production".to_string(),
            signal: "metrics".to_string(),
            kind: None,
            dry_run: false,
        };
        assert_eq!(
            args.dead_letter_dir(),
            PathBuf::from("/data/wal/acceptor/acme/production/metrics/dead-letter")
        );
    }

    #[test]
    fn kind_filter_rejects_an_unknown_value() {
        let args = DeadLetterArgs {
            wal_dir: PathBuf::from("/data/wal/acceptor"),
            tenant: "acme".to_string(),
            dataset: "production".to_string(),
            signal: "metrics".to_string(),
            kind: Some("bogus".to_string()),
            dry_run: false,
        };
        assert!(args.kind_filter().is_err());
    }

    #[tokio::test]
    async fn replay_durably_persists_before_the_process_would_exit() {
        // Regression guard: this CLI process never starts the WAL's
        // background flush timer and exits right after `replay` returns, so
        // a replayed entry sitting only in the in-memory buffer at that
        // point would be silently lost rather than merely delayed. Assert
        // durability the way an operator would notice its absence: close
        // this `Wal` and reopen a fresh one at the same directory, and the
        // entry must still be there, pending.
        use common::wal::{WalOperation, bytes_to_record_batch, record_batch_to_bytes};
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let wal_dir = tempfile::TempDir::new().unwrap();
        let dead_letter_args = DeadLetterArgs {
            wal_dir: wal_dir.path().to_path_buf(),
            tenant: "acme".to_string(),
            dataset: "production".to_string(),
            signal: "metrics".to_string(),
            kind: None,
            dry_run: false,
        };
        let dl_dir = dead_letter_args.dead_letter_dir();
        std::fs::create_dir_all(&dl_dir).unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64]))]).unwrap();
        let payload = record_batch_to_bytes(&batch).unwrap();
        std::fs::write(dl_dir.join("a.bin"), &payload).unwrap();
        std::fs::write(dl_dir.join("a.rejected.json"), b"{}").unwrap();

        replay(&dead_letter_args).await.unwrap();

        assert!(
            !dl_dir.join("a.bin").exists(),
            "a successfully replayed pair must be removed from dead-letter"
        );

        // Simulates the next process (the acceptor retry consumer or the
        // writer drain loop) opening this WAL after the CLI has exited.
        let config = WalConfig::with_defaults(wal_dir.path().to_path_buf()).for_tenant_dataset(
            &dead_letter_args.tenant,
            &dead_letter_args.dataset,
            &dead_letter_args.signal,
        );
        let reopened = Wal::new(config).await.unwrap();
        let pending = reopened.get_unprocessed_entries().await.unwrap();
        assert_eq!(
            pending.len(),
            1,
            "the replayed entry must be durable on disk, not lost with the CLI process"
        );
        let restored =
            bytes_to_record_batch(&reopened.read_entry_data(&pending[0]).await.unwrap()).unwrap();
        assert_eq!(restored.num_rows(), 1);
        assert!(matches!(pending[0].operation, WalOperation::WriteMetrics));
    }
}
