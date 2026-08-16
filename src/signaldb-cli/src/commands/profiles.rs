//! The `profiles` command: the Pyroscope-compatible profile query surface
//! (`types`, `labels`, `label-values`, `render`, `diff`, `by-trace`), via
//! `signaldb-sdk`. Kept outside `query` because Pyroscope has no single
//! query-language flag — the selector and time ranges are per-verb
//! parameters, not a shared `--promql`-style flag (see the
//! `cli-command-surface` capability).
//!
//! - `signaldb-cli profiles types [--from --until]`
//! - `signaldb-cli profiles labels [--from --until]`
//! - `signaldb-cli profiles label-values <label> [--from --until]`
//! - `signaldb-cli profiles render <selector> [--from --until]`
//! - `signaldb-cli profiles diff <selector> [--left-from --left-until --right-from --right-until]`
//! - `signaldb-cli profiles by-trace <trace_id>`
//!
//! `--from`/`--until` (and the diff variants) accept the same forms
//! Pyroscope does: unix seconds, unix milliseconds, or `now[-<N><s|m|h|d>]`.
//! Every subcommand authenticates with a tenant API key (`--api-key` /
//! `--tenant-id`, or the `SIGNALDB_*` environment) and prints the native
//! Pyroscope JSON response unchanged.

use clap::{Args, Subcommand};

use super::discover::ConnectArgs;
use super::query::print_json_response;

#[derive(Subcommand)]
pub enum ProfilesAction {
    /// List profile types with data in the window
    Types(RangeArgs),
    /// List profile label names
    Labels(RangeArgs),
    /// List the known values for one profile label
    LabelValues(LabelValuesArgs),
    /// Render a flame graph for a selector and time range
    Render(RenderArgs),
    /// Render a differential flame graph between two ranges
    Diff(DiffArgs),
    /// List profiles correlated with a trace
    ByTrace(ByTraceArgs),
}

/// Shared `--from`/`--until` window used by `types` and `labels`.
#[derive(Args)]
pub struct RangeArgs {
    /// Range start: unix seconds, unix milliseconds, or `now[-<N><s|m|h|d>]`
    #[arg(long)]
    from: Option<String>,
    /// Range end, same forms as `--from`
    #[arg(long)]
    until: Option<String>,
    #[command(flatten)]
    connect: ConnectArgs,
}

#[derive(Args)]
pub struct LabelValuesArgs {
    /// Label name to list values for
    label: String,
    /// Range start: unix seconds, unix milliseconds, or `now[-<N><s|m|h|d>]`
    #[arg(long)]
    from: Option<String>,
    /// Range end, same forms as `--from`
    #[arg(long)]
    until: Option<String>,
    #[command(flatten)]
    connect: ConnectArgs,
}

#[derive(Args)]
pub struct RenderArgs {
    /// Pyroscope query, e.g. `process_cpu:cpu:nanoseconds{service_name="checkout"}`
    selector: String,
    /// Range start: unix seconds, unix milliseconds, or `now[-<N><s|m|h|d>]`
    #[arg(long)]
    from: Option<String>,
    /// Range end, same forms as `--from`
    #[arg(long)]
    until: Option<String>,
    #[command(flatten)]
    connect: ConnectArgs,
}

#[derive(Args)]
pub struct DiffArgs {
    /// Pyroscope query shared by both ranges
    selector: String,
    /// Baseline range start
    #[arg(long)]
    left_from: Option<String>,
    /// Baseline range end
    #[arg(long)]
    left_until: Option<String>,
    /// Comparison range start
    #[arg(long)]
    right_from: Option<String>,
    /// Comparison range end
    #[arg(long)]
    right_until: Option<String>,
    #[command(flatten)]
    connect: ConnectArgs,
}

#[derive(Args)]
pub struct ByTraceArgs {
    /// Trace ID to fetch correlated profiles for
    trace_id: String,
    #[command(flatten)]
    connect: ConnectArgs,
}

impl ProfilesAction {
    pub async fn run(self) -> anyhow::Result<()> {
        match self {
            ProfilesAction::Types(args) => {
                let client = args.connect.build_client()?;
                let mut req = client.pyroscope_profile_types();
                if let Some(v) = &args.from {
                    req = req.from(v);
                }
                if let Some(v) = &args.until {
                    req = req.until(v);
                }
                let v = req.send().await;
                print_json_response(v.map(|r| r.into_inner()), "profiles types")
            }
            ProfilesAction::Labels(args) => {
                let client = args.connect.build_client()?;
                let mut req = client.pyroscope_label_names();
                if let Some(v) = &args.from {
                    req = req.from(v);
                }
                if let Some(v) = &args.until {
                    req = req.until(v);
                }
                let v = req.send().await;
                print_json_response(v.map(|r| r.into_inner()), "profiles labels")
            }
            ProfilesAction::LabelValues(args) => {
                let client = args.connect.build_client()?;
                let mut req = client.pyroscope_label_values().label(&args.label);
                if let Some(v) = &args.from {
                    req = req.from(v);
                }
                if let Some(v) = &args.until {
                    req = req.until(v);
                }
                let v = req.send().await;
                print_json_response(v.map(|r| r.into_inner()), "profiles label-values")
            }
            ProfilesAction::Render(args) => {
                let client = args.connect.build_client()?;
                let mut req = client.pyroscope_render().query(&args.selector);
                if let Some(v) = &args.from {
                    req = req.from(v);
                }
                if let Some(v) = &args.until {
                    req = req.until(v);
                }
                let v = req.send().await;
                print_json_response(v.map(|r| r.into_inner()), "profiles render")
            }
            ProfilesAction::Diff(args) => {
                let client = args.connect.build_client()?;
                let mut req = client.pyroscope_render_diff().query(&args.selector);
                if let Some(v) = &args.left_from {
                    req = req.left_from(v);
                }
                if let Some(v) = &args.left_until {
                    req = req.left_until(v);
                }
                if let Some(v) = &args.right_from {
                    req = req.right_from(v);
                }
                if let Some(v) = &args.right_until {
                    req = req.right_until(v);
                }
                let v = req.send().await;
                print_json_response(v.map(|r| r.into_inner()), "profiles diff")
            }
            ProfilesAction::ByTrace(args) => {
                let client = args.connect.build_client()?;
                let v = client
                    .profiles_by_trace()
                    .trace_id(&args.trace_id)
                    .send()
                    .await;
                print_json_response(v.map(|r| r.into_inner()), "profiles by-trace")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct TestCli {
        #[command(subcommand)]
        action: ProfilesAction,
    }

    #[test]
    fn types_parses_with_optional_range() {
        let cli = TestCli::try_parse_from(["profiles", "types"]).expect("parses");
        assert!(matches!(cli.action, ProfilesAction::Types(_)));

        let cli =
            TestCli::try_parse_from(["profiles", "types", "--from", "now-1h", "--until", "now"])
                .expect("parses");
        let ProfilesAction::Types(args) = cli.action else {
            panic!("expected Types");
        };
        assert_eq!(args.from.as_deref(), Some("now-1h"));
        assert_eq!(args.until.as_deref(), Some("now"));
    }

    #[test]
    fn labels_parses() {
        let cli = TestCli::try_parse_from(["profiles", "labels"]).expect("parses");
        assert!(matches!(cli.action, ProfilesAction::Labels(_)));
    }

    #[test]
    fn label_values_requires_the_label_positional() {
        assert!(TestCli::try_parse_from(["profiles", "label-values"]).is_err());
        let cli =
            TestCli::try_parse_from(["profiles", "label-values", "service_name"]).expect("parses");
        let ProfilesAction::LabelValues(args) = cli.action else {
            panic!("expected LabelValues");
        };
        assert_eq!(args.label, "service_name");
    }

    #[test]
    fn render_requires_the_selector_positional() {
        assert!(TestCli::try_parse_from(["profiles", "render"]).is_err());
        let cli = TestCli::try_parse_from([
            "profiles",
            "render",
            "process_cpu:cpu:nanoseconds{service_name=\"checkout\"}",
            "--from",
            "now-1h",
        ])
        .expect("parses");
        let ProfilesAction::Render(args) = cli.action else {
            panic!("expected Render");
        };
        assert_eq!(
            args.selector,
            "process_cpu:cpu:nanoseconds{service_name=\"checkout\"}"
        );
        assert_eq!(args.from.as_deref(), Some("now-1h"));
    }

    #[test]
    fn diff_accepts_both_ranges() {
        let cli = TestCli::try_parse_from([
            "profiles",
            "diff",
            "cpu",
            "--left-from",
            "now-2h",
            "--left-until",
            "now-1h",
            "--right-from",
            "now-1h",
            "--right-until",
            "now",
        ])
        .expect("parses");
        let ProfilesAction::Diff(args) = cli.action else {
            panic!("expected Diff");
        };
        assert_eq!(args.selector, "cpu");
        assert_eq!(args.left_from.as_deref(), Some("now-2h"));
        assert_eq!(args.left_until.as_deref(), Some("now-1h"));
        assert_eq!(args.right_from.as_deref(), Some("now-1h"));
        assert_eq!(args.right_until.as_deref(), Some("now"));
    }

    #[test]
    fn by_trace_requires_the_trace_id_positional() {
        assert!(TestCli::try_parse_from(["profiles", "by-trace"]).is_err());
        let cli =
            TestCli::try_parse_from(["profiles", "by-trace", "0123456789abcdef"]).expect("parses");
        let ProfilesAction::ByTrace(args) = cli.action else {
            panic!("expected ByTrace");
        };
        assert_eq!(args.trace_id, "0123456789abcdef");
    }

    #[tokio::test]
    async fn types_dispatches_via_sdk() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/pyroscope/profile-types")
            .match_header("authorization", "Bearer sk-test")
            .match_header("x-tenant-id", "acme")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"[{"ID":"cpu:cpu:nanoseconds","name":"cpu","sampleType":"cpu","sampleUnit":"nanoseconds"}]"#)
            .create_async()
            .await;

        let args = RangeArgs {
            from: None,
            until: None,
            connect: ConnectArgs {
                url: server.url(),
                api_key: Some("sk-test".to_string()),
                tenant_id: Some("acme".to_string()),
                dataset_id: None,
            },
        };
        ProfilesAction::Types(args)
            .run()
            .await
            .expect("profiles types succeeds");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn by_trace_dispatches_via_sdk() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/api/profiles/trace/abc123")
            .match_header("authorization", "Bearer sk-test")
            .match_header("x-tenant-id", "acme")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body("[]")
            .create_async()
            .await;

        let args = ByTraceArgs {
            trace_id: "abc123".to_string(),
            connect: ConnectArgs {
                url: server.url(),
                api_key: Some("sk-test".to_string()),
                tenant_id: Some("acme".to_string()),
                dataset_id: None,
            },
        };
        ProfilesAction::ByTrace(args)
            .run()
            .await
            .expect("profiles by-trace succeeds");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn render_against_unreachable_endpoint_errors() {
        let args = RenderArgs {
            selector: "cpu".to_string(),
            from: None,
            until: None,
            connect: ConnectArgs {
                url: "http://127.0.0.1:1".to_string(),
                api_key: None,
                tenant_id: None,
                dataset_id: None,
            },
        };
        assert!(ProfilesAction::Render(args).run().await.is_err());
    }
}
