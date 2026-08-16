//! # Retry policy and throttling exit for the CLI
//!
//! The CLI inherits the SDK's retry-on-throttle policy through
//! [`client_builder`], the single place every command obtains its HTTP client
//! (see `client-surface-parity`). This module owns:
//!
//! - the process-wide fail-fast switch (`--no-retry` / `SIGNALDB_NO_RETRY`),
//! - the interactive per-retry stderr note (only when stderr is a terminal),
//! - recognising a throttled-after-retries failure in the final error so
//!   `main` can print the server-stated wait and exit with
//!   [`EXIT_THROTTLED`].

use std::io::IsTerminal;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use signaldb_sdk::retry::{Failure, RetryEvent, Throttled, throttle_of};
use signaldb_sdk::{ClientBuilder, RetryPolicy};

/// Exit code when a command is still throttled after the retry budget (or
/// immediately, with `--no-retry`). Distinct from generic failure (`1`) and
/// clap usage errors (`2`) so scripts can back off and re-run.
pub const EXIT_THROTTLED: i32 = 4;

/// Environment switch equivalent to `--no-retry`; truthy values are `1`,
/// `true`, `yes`, `on` (case-insensitive).
pub const NO_RETRY_ENV: &str = "SIGNALDB_NO_RETRY";

static NO_RETRY: AtomicBool = AtomicBool::new(false);

/// Disable (or re-enable) retry for every client built afterwards.
pub fn set_no_retry(disabled: bool) {
    NO_RETRY.store(disabled, Ordering::SeqCst);
}

/// Whether retry is currently disabled.
pub fn no_retry() -> bool {
    NO_RETRY.load(Ordering::SeqCst)
}

/// Whether `value` (from [`NO_RETRY_ENV`]) asks for fail-fast.
pub fn env_value_disables_retry(value: Option<&str>) -> bool {
    matches!(
        value.map(|v| v.trim().to_ascii_lowercase()).as_deref(),
        Some("1" | "true" | "yes" | "on")
    )
}

/// Apply [`NO_RETRY_ENV`] to the process-wide switch from the current
/// process environment.
///
/// `main` calls this before `clap_complete::CompleteEnv::complete()` runs:
/// dynamic shell completion builds its own client (see
/// `commands::completions::tenant_id_completer`) and exits the process
/// before `Cli::run` ever gets a chance to apply the switch, so without this
/// early call `SIGNALDB_NO_RETRY=1` would be silently ignored during
/// completion. `Cli::run` calls it again (there ORed with `--no-retry`,
/// which isn't parsed yet at this point) — both calls are idempotent.
pub fn init_no_retry_from_env() {
    set_no_retry(env_value_disables_retry(
        std::env::var(NO_RETRY_ENV).ok().as_deref(),
    ));
}

/// The retry policy for the current invocation: disabled under
/// `--no-retry`, otherwise the SDK default with an interactive stderr note
/// per retry when stderr is a terminal.
pub fn policy() -> RetryPolicy {
    if no_retry() {
        return RetryPolicy::disabled();
    }
    let policy = RetryPolicy::default();
    if std::io::stderr().is_terminal() {
        policy.with_observer(Arc::new(|event: &RetryEvent| {
            eprintln!("{}", retry_note(event));
        }))
    } else {
        policy
    }
}

/// The one-line interactive note written per retry.
pub fn retry_note(event: &RetryEvent) -> String {
    let cause = match event.failure {
        Failure::Status(429) => "rate limited".to_string(),
        Failure::Status(s) => format!("HTTP {s}"),
        Failure::Connect => "connection failed".to_string(),
        Failure::Timeout => "request timed out".to_string(),
    };
    format!(
        "{cause}; retrying in {} (attempt {})",
        human_duration(event.wait),
        event.attempt + 1
    )
}

/// An SDK client builder rooted at `url` with the invocation's retry policy
/// applied. Every command adds its own credentials/headers and calls
/// `.build()`.
pub fn client_builder(url: &str) -> ClientBuilder {
    ClientBuilder::new(url).retry(policy())
}

/// If `err` (an `anyhow` chain) ends in a throttled SDK error, the
/// throttling detail — used by `main` to print the wait and exit with
/// [`EXIT_THROTTLED`].
pub fn throttled(err: &anyhow::Error) -> Option<Throttled> {
    err.chain().find_map(throttle_of)
}

/// The stderr diagnostic for a throttled-after-retries failure.
pub fn throttled_message(throttled: &Throttled) -> String {
    match throttled.retry_after {
        Some(wait) => format!(
            "rate limited; server asked to retry in {}",
            human_duration(wait)
        ),
        None => "rate limited; retry shortly".to_string(),
    }
}

fn human_duration(d: Duration) -> String {
    let ms = d.as_millis();
    if ms == 0 {
        "0s".to_string()
    } else if ms < 1000 {
        format!("{ms}ms")
    } else if ms.is_multiple_of(1000) {
        format!("{}s", ms / 1000)
    } else {
        format!("{:.1}s", d.as_secs_f64())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_values_that_disable_retry() {
        assert!(env_value_disables_retry(Some("1")));
        assert!(env_value_disables_retry(Some("TRUE")));
        assert!(env_value_disables_retry(Some(" yes ")));
        assert!(!env_value_disables_retry(Some("0")));
        assert!(!env_value_disables_retry(Some("")));
        assert!(!env_value_disables_retry(None));
    }

    #[test]
    fn throttled_message_names_the_server_wait() {
        assert_eq!(
            throttled_message(&Throttled {
                retry_after: Some(Duration::from_secs(5))
            }),
            "rate limited; server asked to retry in 5s"
        );
        assert_eq!(
            throttled_message(&Throttled { retry_after: None }),
            "rate limited; retry shortly"
        );
    }

    #[test]
    fn retry_note_names_the_wait_and_next_attempt() {
        let note = retry_note(&RetryEvent {
            attempt: 1,
            wait: Duration::from_millis(1500),
            failure: Failure::Status(429),
        });
        assert_eq!(note, "rate limited; retrying in 1.5s (attempt 2)");
    }

    #[test]
    fn no_retry_switch_disables_the_policy() {
        set_no_retry(true);
        assert!(!policy().is_enabled());
        set_no_retry(false);
        assert!(policy().is_enabled());
    }

    /// `main` calls `init_no_retry_from_env` before dynamic shell completion
    /// runs, so `SIGNALDB_NO_RETRY` reaches the completer's client even
    /// though `Cli::run` (which used to be the only caller of this switch)
    /// hasn't executed yet. Mutates the same process-wide switch as
    /// `no_retry_switch_disables_the_policy`, so it restores both the env
    /// var and the switch on every exit path to avoid leaking state into
    /// whichever test runs next.
    #[test]
    fn init_no_retry_from_env_applies_the_env_var_switch() {
        // SAFETY: no other thread reads/writes this process's env in a way
        // that races with this test; std::env mutation just requires the
        // caller to reason about that themselves.
        unsafe {
            std::env::set_var(NO_RETRY_ENV, "1");
        }
        init_no_retry_from_env();
        let enabled_after_set = no_retry();

        unsafe {
            std::env::remove_var(NO_RETRY_ENV);
        }
        init_no_retry_from_env();
        let enabled_after_unset = no_retry();

        assert!(
            enabled_after_set,
            "SIGNALDB_NO_RETRY=1 should disable retry"
        );
        assert!(
            !enabled_after_unset,
            "unset SIGNALDB_NO_RETRY should leave retry enabled"
        );
    }
}
