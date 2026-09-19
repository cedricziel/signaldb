//! The `processors` command group and `admin processors`: tenant OTTL
//! processor CRUD, validation, and dry-run testing via `signaldb-sdk`
//! (openspec change `tenant-ottl-processors`, D6). Mirrors the router's
//! `/api/v1/processors*` API (`src/router/src/endpoints/processors.rs`).
//!
//! - `signaldb-cli processors list|get|validate|test` — reads, `processors:read`
//! - `signaldb-cli admin processors create|replace|delete` — mutations,
//!   `processors:write` (tenant-admin key/session)
//!
//! Every subcommand authenticates with a tenant API key (`--api-key` /
//! `--tenant-id`, or the `SIGNALDB_*` environment), like `schema`.

use std::path::{Path, PathBuf};

use anyhow::Context;
use clap::{Args, Subcommand};

use super::discover::ConnectArgs;
use super::query::print_json_response;

/// `signaldb-cli processors <verb>` — read-only lookup, validation, and
/// dry-run testing.
#[derive(Subcommand)]
pub enum ProcessorsAction {
    /// List this tenant's processors
    List(ConnectArgs),
    /// Fetch one processor by name
    Get {
        /// Processor name
        #[arg(long)]
        name: String,
        #[command(flatten)]
        connect: ConnectArgs,
    },
    /// Compile-check a program without storing it
    Validate(ValidateArgs),
    /// Apply processor(s) against an inline OTLP/JSON payload; never writes
    Test(TestArgs),
}

/// `signaldb-cli admin processors <verb>` — processor management.
#[derive(Subcommand)]
pub enum AdminProcessorsAction {
    /// Create a processor
    Create(CreateArgs),
    /// Replace a processor's definition (`--name`, or the file/flags' own
    /// `name`, must match an existing row)
    Replace(CreateArgs),
    /// Delete a processor
    Delete {
        /// Processor name
        #[arg(long)]
        name: String,
        #[command(flatten)]
        connect: ConnectArgs,
    },
}

/// A program given either as a file (JSON/YAML: `{"signal": ..., "statements": [...]}`)
/// or as `--signal`/repeatable `--statement` flags.
#[derive(Args)]
pub struct ValidateArgs {
    /// Signal the statements apply to (traces, logs, metrics); required
    /// unless `--file` is given
    #[arg(long)]
    signal: Option<String>,
    /// One OTTL statement; repeat for multiple. Required unless `--file` is
    /// given
    #[arg(long = "statement")]
    statements: Vec<String>,
    /// Read `{"signal": ..., "statements": [...]}` from a file (JSON or YAML)
    /// instead of `--signal`/`--statement`
    #[arg(short = 'f', long, value_name = "PATH")]
    file: Option<PathBuf>,
    #[command(flatten)]
    connect: ConnectArgs,
}

/// Flags shared by `create` and `replace`.
#[derive(Args)]
pub struct CreateArgs {
    /// Read the full processor spec from a file (JSON or YAML) instead of
    /// the flags below
    #[arg(short = 'f', long, value_name = "PATH")]
    file: Option<PathBuf>,
    /// Processor name (required with flag input; `create` only)
    #[arg(long)]
    name: Option<String>,
    /// Signal the statements apply to (traces, logs, metrics)
    #[arg(long)]
    signal: Option<String>,
    /// Dataset name this processor applies to (omit for tenant-wide)
    #[arg(long)]
    dataset: Option<String>,
    /// One OTTL statement; repeat for multiple
    #[arg(long = "statement")]
    statements: Vec<String>,
    /// Evaluation priority (lower runs first; server default 100)
    #[arg(long)]
    priority: Option<i32>,
    /// Error handling: ignore, silent, or propagate (server default ignore)
    #[arg(long)]
    error_mode: Option<String>,
    /// Human-readable description
    #[arg(long)]
    description: Option<String>,
    /// Create the processor disabled
    #[arg(long)]
    disabled: bool,
    #[command(flatten)]
    connect: ConnectArgs,
}

#[derive(Args)]
pub struct TestArgs {
    /// Signal the payload carries (traces, logs, metrics)
    #[arg(long)]
    signal: String,
    /// Dataset to resolve stored processors for (defaults to the
    /// credential's dataset); ignored when `--file` supplies inline
    /// processors
    #[arg(long)]
    dataset: Option<String>,
    /// Inline processor spec(s) to apply instead of the tenant's stored ones
    /// (JSON or YAML: one spec object, or a list of spec objects)
    #[arg(short = 'f', long, value_name = "PATH")]
    file: Option<PathBuf>,
    /// OTLP/JSON export request to transform
    #[arg(long, value_name = "PATH")]
    payload: PathBuf,
    #[command(flatten)]
    connect: ConnectArgs,
}

/// A processor spec as the JSON object the API takes.
type Document = serde_json::Map<String, serde_json::Value>;

fn read_document(path: &Path) -> anyhow::Result<serde_json::Value> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    parse_document(&text, path)
}

fn parse_document(text: &str, path: &Path) -> anyhow::Result<serde_json::Value> {
    let is_json = path
        .extension()
        .and_then(|e| e.to_str())
        .is_some_and(|e| e.eq_ignore_ascii_case("json"));
    if is_json {
        serde_json::from_str(text).with_context(|| format!("{} is not valid JSON", path.display()))
    } else {
        serde_norway::from_str(text)
            .with_context(|| format!("{} is not valid YAML", path.display()))
    }
}

fn spec_object(value: serde_json::Value, path: &Path) -> anyhow::Result<Document> {
    match value {
        serde_json::Value::Object(map) => Ok(map),
        _ => anyhow::bail!(
            "{} must contain a processor spec object (name, signal, statements)",
            path.display()
        ),
    }
}

fn parse_processor_spec(map: Document) -> anyhow::Result<signaldb_sdk::types::ProcessorSpec> {
    Ok(serde_json::from_value(serde_json::Value::Object(map))?)
}

impl ProcessorsAction {
    pub async fn run(self) -> anyhow::Result<()> {
        match self {
            ProcessorsAction::List(connect) => {
                let client = connect.build_client()?;
                let result = client
                    .processors_list()
                    .send()
                    .await
                    .map(|r| r.into_inner());
                print_json_response(result, "processors list")
            }
            ProcessorsAction::Get { name, connect } => {
                let client = connect.build_client()?;
                let result = client
                    .processors_get()
                    .name(name)
                    .send()
                    .await
                    .map(|r| r.into_inner());
                print_json_response(result, "processors get")
            }
            ProcessorsAction::Validate(args) => run_validate(args).await,
            ProcessorsAction::Test(args) => run_test(args).await,
        }
    }
}

async fn run_validate(args: ValidateArgs) -> anyhow::Result<()> {
    let (signal, statements) = match &args.file {
        Some(path) => {
            let value = read_document(path)?;
            let req: ValidateFile = serde_json::from_value(value)
                .with_context(|| format!("{} is not a valid validate request", path.display()))?;
            (req.signal, req.statements)
        }
        None => {
            let signal = args
                .signal
                .clone()
                .ok_or_else(|| anyhow::anyhow!("--signal is required without --file"))?;
            if args.statements.is_empty() {
                anyhow::bail!("at least one --statement is required without --file");
            }
            (signal, args.statements.clone())
        }
    };
    let client = args.connect.build_client()?;
    let result = client
        .processors_validate()
        .body(signaldb_sdk::types::ValidateRequest { signal, statements })
        .send()
        .await
        .map(|r| r.into_inner());
    match result {
        Ok(response) if response.errors.is_empty() => {
            println!("{}", serde_json::to_string_pretty(&response)?);
            Ok(())
        }
        Ok(response) => {
            println!("{}", serde_json::to_string_pretty(&response)?);
            anyhow::bail!("processors validate failed: statements did not compile")
        }
        Err(e) => Err(anyhow::Error::new(e).context("processors validate failed")),
    }
}

#[derive(serde::Deserialize)]
struct ValidateFile {
    signal: String,
    statements: Vec<String>,
}

async fn run_test(args: TestArgs) -> anyhow::Result<()> {
    let payload = read_document(&args.payload)
        .with_context(|| format!("failed to read payload {}", args.payload.display()))?;
    let processors = match &args.file {
        Some(path) => {
            let value = read_document(path)?;
            let specs = match value {
                serde_json::Value::Array(items) => items
                    .into_iter()
                    .map(|v| spec_object(v, path).and_then(parse_processor_spec))
                    .collect::<anyhow::Result<Vec<_>>>()?,
                other => vec![parse_processor_spec(spec_object(other, path)?)?],
            };
            Some(specs)
        }
        None => None,
    };
    let client = args.connect.build_client()?;
    let result = client
        .processors_test()
        .body(signaldb_sdk::types::TestRequest {
            signal: args.signal,
            dataset: args.dataset,
            processors,
            payload,
        })
        .send()
        .await
        .map(|r| r.into_inner());
    print_json_response(result, "processors test")
}

impl AdminProcessorsAction {
    pub async fn run(self) -> anyhow::Result<()> {
        match self {
            AdminProcessorsAction::Create(args) => {
                let spec = build_spec(args)?;
                let client = spec.connect.build_client()?;
                let result = client
                    .processors_create()
                    .body(spec.spec)
                    .send()
                    .await
                    .map(|r| r.into_inner());
                print_json_response(result, "admin processors create")
            }
            AdminProcessorsAction::Replace(args) => {
                let spec = build_spec(args)?;
                let name = spec.spec.name.clone();
                let client = spec.connect.build_client()?;
                let result = client
                    .processors_replace()
                    .name(name)
                    .body(spec.spec)
                    .send()
                    .await
                    .map(|r| r.into_inner());
                print_json_response(result, "admin processors replace")
            }
            AdminProcessorsAction::Delete { name, connect } => {
                let client = connect.build_client()?;
                client
                    .processors_delete()
                    .name(&name)
                    .send()
                    .await
                    .map_err(|e| anyhow::Error::new(e).context("admin processors delete failed"))?;
                println!("Processor '{name}' deleted.");
                Ok(())
            }
        }
    }
}

struct BuiltSpec {
    spec: signaldb_sdk::types::ProcessorSpec,
    connect: ConnectArgs,
}

/// Builds the processor spec for `create`/`replace` from `--file` or flags.
/// `--name` fills in a `name` the file omits (a file that already names the
/// processor wins).
fn build_spec(args: CreateArgs) -> anyhow::Result<BuiltSpec> {
    let spec = match &args.file {
        Some(path) => {
            let value = read_document(path)?;
            let mut map = spec_object(value, path)?;
            if !map.contains_key("name")
                && let Some(name) = &args.name
            {
                map.insert("name".to_string(), serde_json::Value::String(name.clone()));
            }
            parse_processor_spec(map)?
        }
        None => {
            let name = args
                .name
                .clone()
                .ok_or_else(|| anyhow::anyhow!("--name is required without --file"))?;
            let signal = args
                .signal
                .clone()
                .ok_or_else(|| anyhow::anyhow!("--signal is required without --file"))?;
            if args.statements.is_empty() {
                anyhow::bail!("at least one --statement is required without --file");
            }
            signaldb_sdk::types::ProcessorSpec {
                name,
                dataset: args.dataset.clone(),
                signal,
                enabled: Some(!args.disabled),
                priority: args.priority,
                error_mode: args.error_mode.clone(),
                description: args.description.clone(),
                statements: args.statements.clone(),
            }
        }
    };
    Ok(BuiltSpec {
        spec,
        connect: args.connect,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct ProcessorsCli {
        #[command(subcommand)]
        action: ProcessorsAction,
    }

    #[derive(Parser)]
    struct AdminProcessorsCli {
        #[command(subcommand)]
        action: AdminProcessorsAction,
    }

    fn connect(url: &str) -> ConnectArgs {
        ConnectArgs {
            url: url.to_string(),
            api_key: Some("sk-test".to_string()),
            tenant_id: Some("acme".to_string()),
            dataset_id: None,
        }
    }

    fn write_temp(name: &str, contents: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("signaldb-cli-processors-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let path = dir.join(name);
        std::fs::write(&path, contents).expect("write fixture");
        path
    }

    #[test]
    fn processors_list_and_get_parse() {
        assert!(ProcessorsCli::try_parse_from(["processors", "list"]).is_ok());
        let cli = ProcessorsCli::try_parse_from(["processors", "get", "--name", "redact"])
            .expect("parses");
        let ProcessorsAction::Get { name, .. } = cli.action else {
            panic!("expected get");
        };
        assert_eq!(name, "redact");
        // `get` requires `--name`.
        assert!(ProcessorsCli::try_parse_from(["processors", "get"]).is_err());
    }

    #[test]
    fn processors_validate_parses_flags_or_file() {
        assert!(
            ProcessorsCli::try_parse_from([
                "processors",
                "validate",
                "--signal",
                "traces",
                "--statement",
                "set(attributes[\"x\"], \"y\")",
            ])
            .is_ok()
        );
        assert!(
            ProcessorsCli::try_parse_from(["processors", "validate", "-f", "prog.yaml"]).is_ok()
        );
        // Bare `validate` parses (flags are optional at the clap level; the
        // "signal or statements required" check happens in `run()`).
        assert!(ProcessorsCli::try_parse_from(["processors", "validate"]).is_ok());
    }

    #[test]
    fn processors_test_requires_signal_and_payload() {
        assert!(
            ProcessorsCli::try_parse_from([
                "processors",
                "test",
                "--signal",
                "traces",
                "--payload",
                "payload.json",
            ])
            .is_ok()
        );
        assert!(
            ProcessorsCli::try_parse_from(["processors", "test", "--signal", "traces"]).is_err()
        );
        assert!(
            ProcessorsCli::try_parse_from(["processors", "test", "--payload", "p.json"]).is_err()
        );
    }

    #[test]
    fn admin_processors_subcommands_parse() {
        assert!(
            AdminProcessorsCli::try_parse_from([
                "admin-processors",
                "create",
                "--name",
                "redact",
                "--signal",
                "traces",
                "--statement",
                "set(attributes[\"x\"], \"y\")",
            ])
            .is_ok()
        );
        assert!(
            AdminProcessorsCli::try_parse_from(["admin-processors", "create", "-f", "spec.json"])
                .is_ok()
        );
        assert!(
            AdminProcessorsCli::try_parse_from([
                "admin-processors",
                "replace",
                "--name",
                "redact",
                "-f",
                "spec.json"
            ])
            .is_ok()
        );
        assert!(
            AdminProcessorsCli::try_parse_from(["admin-processors", "delete", "--name", "redact"])
                .is_ok()
        );
        // `delete` requires `--name` at parse time; `replace` and `create`
        // accept `--name` as optional at the clap level (a file may supply
        // its own `name`) and enforce it at `run()` time instead, see
        // `create_without_file_requires_name_signal_and_statement`.
        assert!(AdminProcessorsCli::try_parse_from(["admin-processors", "delete"]).is_err());
    }

    #[test]
    fn create_without_file_requires_name_signal_and_statement() {
        let args = CreateArgs {
            file: None,
            name: None,
            signal: Some("traces".to_string()),
            dataset: None,
            statements: vec!["set(attributes[\"x\"], \"y\")".to_string()],
            priority: None,
            error_mode: None,
            description: None,
            disabled: false,
            connect: connect("http://router.invalid"),
        };
        let Err(err) = build_spec(args) else {
            panic!("missing --name must fail");
        };
        assert!(err.to_string().contains("--name"), "{err}");
    }

    #[test]
    fn create_without_file_builds_spec_from_flags() {
        let args = CreateArgs {
            file: None,
            name: Some("redact".to_string()),
            signal: Some("traces".to_string()),
            dataset: Some("prod".to_string()),
            statements: vec!["set(attributes[\"x\"], \"y\")".to_string()],
            priority: Some(50),
            error_mode: Some("propagate".to_string()),
            description: Some("redacts x".to_string()),
            disabled: true,
            connect: connect("http://router.invalid"),
        };
        let built = build_spec(args).expect("builds");
        assert_eq!(built.spec.name, "redact");
        assert_eq!(built.spec.dataset.as_deref(), Some("prod"));
        assert_eq!(built.spec.enabled, Some(false));
        assert_eq!(built.spec.priority, Some(50));
    }

    #[test]
    fn create_from_file_parses_json_and_yaml() {
        let json = write_temp(
            "spec.json",
            r#"{"name":"redact","signal":"traces","statements":["set(attributes[\"x\"], \"y\")"]}"#,
        );
        let args = CreateArgs {
            file: Some(json),
            name: None,
            signal: None,
            dataset: None,
            statements: vec![],
            priority: None,
            error_mode: None,
            description: None,
            disabled: false,
            connect: connect("http://router.invalid"),
        };
        let built = build_spec(args).expect("builds from json");
        assert_eq!(built.spec.name, "redact");

        let yaml = write_temp(
            "spec.yaml",
            "name: redact\nsignal: traces\nstatements:\n  - 'set(attributes[\"x\"], \"y\")'\n",
        );
        let args = CreateArgs {
            file: Some(yaml),
            name: None,
            signal: None,
            dataset: None,
            statements: vec![],
            priority: None,
            error_mode: None,
            description: None,
            disabled: false,
            connect: connect("http://router.invalid"),
        };
        let built = build_spec(args).expect("builds from yaml");
        assert_eq!(built.spec.name, "redact");
    }

    #[test]
    fn replace_from_file_fills_in_name_when_missing() {
        let file = write_temp(
            "spec-noname.json",
            r#"{"signal":"traces","statements":["set(attributes[\"x\"], \"y\")"]}"#,
        );
        let args = CreateArgs {
            file: Some(file),
            name: Some("redact".to_string()),
            signal: None,
            dataset: None,
            statements: vec![],
            priority: None,
            error_mode: None,
            description: None,
            disabled: false,
            connect: connect("http://router.invalid"),
        };
        let built = build_spec(args).expect("builds");
        assert_eq!(built.spec.name, "redact");
    }

    #[tokio::test]
    async fn processors_list_resolves_via_sdk() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/api/v1/processors")
            .match_header("authorization", "Bearer sk-test")
            .match_header("x-tenant-id", "acme")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"processors":[]}"#)
            .create_async()
            .await;

        ProcessorsAction::List(connect(&server.url()))
            .run()
            .await
            .expect("processors list succeeds");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn validate_reports_positional_errors_as_a_failure() {
        let mut server = mockito::Server::new_async().await;
        let _mock = server
            .mock("POST", "/api/v1/processors:validate")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"errors":[{"statement":0,"message":"bad token"}]}"#)
            .create_async()
            .await;

        let err = ProcessorsAction::Validate(ValidateArgs {
            signal: Some("traces".to_string()),
            statements: vec!["not a valid statement (".to_string()],
            file: None,
            connect: connect(&server.url()),
        })
        .run()
        .await
        .expect_err("a 200 with compile errors must still fail the command");
        assert!(err.to_string().contains("did not compile"), "{err}");
    }

    #[tokio::test]
    async fn validate_succeeds_when_there_are_no_errors() {
        let mut server = mockito::Server::new_async().await;
        let _mock = server
            .mock("POST", "/api/v1/processors:validate")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"errors":[]}"#)
            .create_async()
            .await;

        ProcessorsAction::Validate(ValidateArgs {
            signal: Some("traces".to_string()),
            statements: vec!["set(attributes[\"x\"], \"y\")".to_string()],
            file: None,
            connect: connect(&server.url()),
        })
        .run()
        .await
        .expect("no errors succeeds");
    }

    #[tokio::test]
    async fn admin_processors_create_surfaces_a_422_as_an_error() {
        let mut server = mockito::Server::new_async().await;
        let _mock = server
            .mock("POST", "/api/v1/processors")
            .with_status(422)
            .with_header("content-type", "application/json")
            .with_body(r#"{"error":"processor failed to compile","errors":[]}"#)
            .create_async()
            .await;

        let args = CreateArgs {
            file: None,
            name: Some("redact".to_string()),
            signal: Some("traces".to_string()),
            dataset: None,
            statements: vec!["not a valid statement (".to_string()],
            priority: None,
            error_mode: None,
            description: None,
            disabled: false,
            connect: connect(&server.url()),
        };
        let err = AdminProcessorsAction::Create(args)
            .run()
            .await
            .expect_err("a 422 must fail the command");
        assert!(
            err.to_string().contains("admin processors create failed"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn admin_processors_delete_hits_the_processor_path() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("DELETE", "/api/v1/processors/redact")
            .with_status(204)
            .create_async()
            .await;

        AdminProcessorsAction::Delete {
            name: "redact".to_string(),
            connect: connect(&server.url()),
        }
        .run()
        .await
        .expect("admin processors delete succeeds");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_endpoint_sends_inline_processors_from_file() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/api/v1/processors:test")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"payload":{},"statements":[]}"#)
            .create_async()
            .await;

        let payload = write_temp("payload.json", r#"{"resourceSpans":[]}"#);
        let spec_file = write_temp(
            "inline-spec.json",
            r#"{"name":"inline","signal":"traces","statements":["set(attributes[\"x\"], \"y\")"]}"#,
        );

        ProcessorsAction::Test(TestArgs {
            signal: "traces".to_string(),
            dataset: None,
            file: Some(spec_file),
            payload,
            connect: connect(&server.url()),
        })
        .run()
        .await
        .expect("processors test succeeds");
        mock.assert_async().await;
    }
}
