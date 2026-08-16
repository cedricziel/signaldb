//! # Shell Completions
//!
//! Static completion script generation (`signaldb-cli completions <shell>`)
//! and dynamic tenant-ID completion for arguments that take a tenant.
//!
//! Dynamic completion is powered by `clap_complete`'s `unstable-dynamic`
//! engine: registering `source <(COMPLETE=<shell> signaldb-cli)` installs a
//! hook that calls back into the binary with `COMPLETE=<shell>` set, and
//! [`tenant_id_completer`] fetches candidates from the admin API the same
//! way `tenant list` does. Lookups fail silently (empty candidate list) so
//! completion never prints errors or hangs the shell.

use std::ffi::OsStr;
use std::io::Write;
use std::time::Duration;

use clap::CommandFactory;
use clap_complete::Shell;
use clap_complete::engine::CompletionCandidate;

/// Maximum time to spend fetching tenant IDs during shell completion.
const COMPLETION_TIMEOUT: Duration = Duration::from_secs(2);

/// Generate a static completion script for `shell` on stdout.
pub fn generate(shell: Shell) -> anyhow::Result<()> {
    let mut stdout = std::io::stdout();
    write_completion_script(shell, &mut stdout)?;
    Ok(())
}

/// Write the completion script for `shell` into `buf`.
fn write_completion_script(shell: Shell, buf: &mut dyn Write) -> std::io::Result<()> {
    let mut cmd = super::Cli::command();
    let bin_name = cmd.get_name().to_string();
    clap_complete::generate(shell, &mut cmd, bin_name, buf);
    buf.flush()
}

/// Dynamic completer for tenant-ID arguments.
///
/// Reads the router URL and admin key from `SIGNALDB_URL` /
/// `SIGNALDB_ADMIN_KEY` (the same environment variables the CLI itself
/// honors) and lists tenants via the admin API. Returns no candidates on
/// any error so shell completion stays silent when the backend is
/// unreachable or unauthenticated.
pub fn tenant_id_completer(current: &OsStr) -> Vec<CompletionCandidate> {
    let Some(prefix) = current.to_str() else {
        return Vec::new();
    };
    let url = std::env::var("SIGNALDB_URL").unwrap_or_else(|_| "http://localhost:3000".to_string());
    let admin_key = std::env::var("SIGNALDB_ADMIN_KEY").ok();

    fetch_tenants(&url, admin_key.as_deref(), COMPLETION_TIMEOUT)
        .into_iter()
        .filter(|(id, _)| id.starts_with(prefix))
        .map(|(id, name)| CompletionCandidate::new(id).help(Some(name.into())))
        .collect()
}

/// Fetch `(id, name)` pairs for all tenants from the admin API.
///
/// Never errors and never blocks longer than `timeout`: any failure
/// (unreachable endpoint, bad credentials, malformed response) yields an
/// empty list.
fn fetch_tenants(url: &str, admin_key: Option<&str>, timeout: Duration) -> Vec<(String, String)> {
    let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    else {
        return Vec::new();
    };

    runtime.block_on(async {
        let fetch = async {
            // Generated SDK URLs are absolute; base is the router root.
            let mut builder = crate::retry::client_builder(url).timeout(timeout);
            if let Some(key) = admin_key {
                builder = builder.bearer(key);
            }
            let client = builder.build().ok()?;
            let resp = client.list_tenants().send().await.ok()?;
            Some(
                resp.into_inner()
                    .tenants
                    .into_iter()
                    .map(|t| (t.id, t.name))
                    .collect::<Vec<_>>(),
            )
        };
        match tokio::time::timeout(timeout, fetch).await {
            Ok(Some(tenants)) => tenants,
            _ => Vec::new(),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completion_script_is_nonempty_for_every_shell() {
        for shell in [
            Shell::Bash,
            Shell::Zsh,
            Shell::Fish,
            Shell::Elvish,
            Shell::PowerShell,
        ] {
            let mut buf = Vec::new();
            write_completion_script(shell, &mut buf)
                .unwrap_or_else(|e| panic!("writing {shell} script failed: {e}"));
            assert!(!buf.is_empty(), "empty completion script for {shell}");
        }
    }

    #[test]
    fn fetch_tenants_returns_empty_when_endpoint_unreachable() {
        // Port 1 is reserved and refuses connections immediately.
        let tenants = fetch_tenants("http://127.0.0.1:1", None, Duration::from_secs(2));
        assert!(tenants.is_empty());
    }

    #[test]
    fn fetch_tenants_returns_ids_and_names_from_admin_api() {
        let mut server = mockito::Server::new();
        let mock = server
            .mock("GET", "/api/v1/admin/tenants")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"tenants":[
                    {"id":"acme","name":"Acme Corporation","source":"config",
                     "created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z"},
                    {"id":"globex","name":"Globex","source":"database",
                     "created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z"}
                ]}"#,
            )
            .create();

        let tenants = fetch_tenants(&server.url(), Some("test-key"), Duration::from_secs(2));

        mock.assert();
        assert_eq!(
            tenants,
            vec![
                ("acme".to_string(), "Acme Corporation".to_string()),
                ("globex".to_string(), "Globex".to_string()),
            ]
        );
    }

    #[test]
    fn fetch_tenants_returns_empty_for_malformed_response() {
        let mut server = mockito::Server::new();
        server
            .mock("GET", "/api/v1/admin/tenants")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"unexpected":true}"#)
            .create();

        let tenants = fetch_tenants(&server.url(), None, Duration::from_secs(2));
        assert!(tenants.is_empty());
    }
}
