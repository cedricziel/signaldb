//! The `eval-sets` command group and `admin eval-sets`: agent eval sets via
//! `signaldb-sdk` (openspec change `agent-offline-evals`). Mirrors the
//! router's `/api/v1/eval-sets*` API (`src/router/src/endpoints/eval_sets.rs`).
//!
//! - `signaldb-cli eval-sets list|get|export` — reads, `evals:read`
//! - `signaldb-cli admin eval-sets create|replace|delete|append` — mutations,
//!   `evals:write`
//!
//! A set belongs to the credential's tenant and dataset (`--dataset-id`, or
//! the tenant's default dataset).

use std::fmt::Write as _;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::Context;
use clap::Subcommand;
use signaldb_sdk::types::{
    AppendCasesOutcome, EvalCase, EvalCaseSource, EvalSetListResponse, EvalSetResponse, EvalSetSpec,
};

use super::OutputArgs;
use super::discover::ConnectArgs;
use super::query::print_json_response;

/// `signaldb-cli eval-sets <verb>` — read eval sets.
#[derive(Subcommand)]
pub enum EvalSetsAction {
    /// List the eval sets in the dataset (without their cases)
    List(OutputArgs),
    /// Show one eval set with its cases
    Get {
        /// Eval set name
        name: String,
        #[command(flatten)]
        output: OutputArgs,
    },
    /// Print a set's cases as JSONL (one case object per line) on stdout
    Export {
        /// Eval set name
        name: String,
        #[command(flatten)]
        connect: ConnectArgs,
    },
}

/// `signaldb-cli admin eval-sets <verb>` — eval set management.
#[derive(Subcommand)]
pub enum AdminEvalSetsAction {
    /// Create an eval set from a YAML or JSON file (`{"name", "agent",
    /// "description", "cases": [...]}`)
    Create {
        /// YAML or JSON file holding the eval set (`.json` is read as JSON,
        /// anything else as YAML)
        #[arg(short = 'f', long, value_name = "PATH")]
        file: PathBuf,
        #[command(flatten)]
        connect: ConnectArgs,
    },
    /// Replace an eval set's agent, description and every case (never
    /// creates)
    Replace {
        /// Eval set name
        name: String,
        /// YAML or JSON file holding the eval set; a missing `name` is filled
        /// in from the argument, a different one is rejected
        #[arg(short = 'f', long, value_name = "PATH")]
        file: PathBuf,
        #[command(flatten)]
        connect: ConnectArgs,
    },
    /// Delete an eval set and its cases
    Delete {
        /// Eval set name
        name: String,
        #[command(flatten)]
        connect: ConnectArgs,
    },
    /// Append cases to an eval set, skipping ids it already holds
    Append {
        /// Eval set name
        name: String,
        /// Cases as JSONL (one case object per line) or as a JSON
        /// `{"cases": [...]}` object
        #[arg(short = 'f', long, value_name = "PATH")]
        file: PathBuf,
        #[command(flatten)]
        connect: ConnectArgs,
    },
}

impl EvalSetsAction {
    pub async fn run(self) -> anyhow::Result<()> {
        match self {
            EvalSetsAction::List(OutputArgs { connect, json }) => {
                let v = connect
                    .build_client()?
                    .list_eval_sets()
                    .send()
                    .await
                    .map_err(|e| anyhow::Error::new(e).context("eval-sets list failed"))?
                    .into_inner();
                if json {
                    super::print_json(&v)?;
                } else {
                    println!("{}", format_eval_set_list(&v));
                }
                Ok(())
            }
            EvalSetsAction::Get {
                name,
                output: OutputArgs { connect, json },
            } => {
                let v = get_set(&connect, &name, "eval-sets get").await?;
                if json {
                    super::print_json(&v)?;
                } else {
                    println!("{}", format_eval_set(&v));
                }
                Ok(())
            }
            EvalSetsAction::Export { name, connect } => {
                let v = get_set(&connect, &name, "eval-sets export").await?;
                let stdout = std::io::stdout();
                match write_jsonl(&v.cases, std::io::BufWriter::new(stdout.lock())) {
                    // `| head` closed the pipe: the reader has what it wants.
                    Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => Ok(()),
                    other => other.context("failed to write the cases to stdout"),
                }
            }
        }
    }
}

async fn get_set(connect: &ConnectArgs, name: &str, what: &str) -> anyhow::Result<EvalSetResponse> {
    Ok(connect
        .build_client()?
        .get_eval_set()
        .name(name)
        .send()
        .await
        .map_err(|e| anyhow::Error::new(e).context(format!("{what} failed")))?
        .into_inner())
}

impl AdminEvalSetsAction {
    pub async fn run(self) -> anyhow::Result<()> {
        match self {
            AdminEvalSetsAction::Create { file, connect } => {
                let spec = read_spec(&file, None)?;
                let result = connect
                    .build_client()?
                    .create_eval_set()
                    .body(spec)
                    .send()
                    .await
                    .map(|r| r.into_inner());
                print_json_response(result, "admin eval-sets create")
            }
            AdminEvalSetsAction::Replace {
                name,
                file,
                connect,
            } => {
                let spec = read_spec(&file, Some(&name))?;
                let result = connect
                    .build_client()?
                    .replace_eval_set()
                    .name(&name)
                    .body(spec)
                    .send()
                    .await
                    .map(|r| r.into_inner());
                print_json_response(result, "admin eval-sets replace")
            }
            AdminEvalSetsAction::Delete { name, connect } => {
                connect
                    .build_client()?
                    .delete_eval_set()
                    .name(&name)
                    .send()
                    .await
                    .map_err(|e| anyhow::Error::new(e).context("admin eval-sets delete failed"))?;
                println!("Eval set '{name}' deleted.");
                Ok(())
            }
            AdminEvalSetsAction::Append {
                name,
                file,
                connect,
            } => {
                let cases = read_cases(&file)?;
                let outcome = connect
                    .build_client()?
                    .append_eval_cases()
                    .name(&name)
                    .body(signaldb_sdk::types::AppendEvalCasesRequest { cases })
                    .send()
                    .await
                    .map_err(|e| anyhow::Error::new(e).context("admin eval-sets append failed"))?
                    .into_inner();
                println!("{}", format_append_outcome(&name, &outcome));
                Ok(())
            }
        }
    }
}

/// Reads an eval set spec from a YAML or JSON file. With `path_name`
/// (replace), a spec without a `name` takes it, and a spec naming a
/// different set is rejected before any request is sent.
fn read_spec(path: &Path, path_name: Option<&str>) -> anyhow::Result<EvalSetSpec> {
    parse_spec(super::read_json_object(path, "eval set")?, path, path_name)
}

fn parse_spec(
    mut map: serde_json::Map<String, serde_json::Value>,
    path: &Path,
    path_name: Option<&str>,
) -> anyhow::Result<EvalSetSpec> {
    if let Some(n) = path_name {
        map.entry("name").or_insert(n.into());
    }
    let spec: EvalSetSpec = serde_json::from_value(serde_json::Value::Object(map))
        .with_context(|| format!("{} is not a valid eval set", path.display()))?;
    if let Some(expected) = path_name
        && spec.name != expected
    {
        anyhow::bail!(
            "{} names eval set '{}', but '{expected}' is being replaced",
            path.display(),
            spec.name
        );
    }
    Ok(spec)
}

fn read_cases(path: &Path) -> anyhow::Result<Vec<EvalCase>> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    parse_cases(&text, path)
}

/// A JSON `{"cases": [...]}` document; `cases` is optional so a one-line
/// JSONL file (a lone case object) falls through to the JSONL parse.
#[derive(serde::Deserialize)]
struct CasesDoc {
    cases: Option<Vec<EvalCase>>,
}

/// Parses cases from a JSON `{"cases": [...]}` object or, failing that, from
/// JSONL: one case object per non-blank line.
fn parse_cases(text: &str, path: &Path) -> anyhow::Result<Vec<EvalCase>> {
    match serde_json::from_str::<CasesDoc>(text) {
        Ok(CasesDoc { cases: Some(cases) }) => return Ok(cases),
        // Well-formed JSON object whose `cases` does not hold eval cases.
        Err(e) if e.is_data() && text.trim_start().starts_with('{') => {
            return Err(anyhow::Error::new(e)
                .context(format!("{} holds an invalid `cases` array", path.display())));
        }
        _ => {}
    }
    let cases = text
        .lines()
        .enumerate()
        .filter(|(_, line)| !line.trim().is_empty())
        .map(|(i, line)| {
            serde_json::from_str::<EvalCase>(line)
                .with_context(|| format!("{}:{}: not a valid eval case", path.display(), i + 1))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    if cases.is_empty() {
        anyhow::bail!("{} holds no cases", path.display());
    }
    Ok(cases)
}

/// Writes one case object per line.
fn write_jsonl(cases: &[EvalCase], mut w: impl Write) -> std::io::Result<()> {
    for case in cases {
        serde_json::to_writer(&mut w, case)?;
        w.write_all(b"\n")?;
    }
    w.flush()
}

fn format_timestamp(t: &chrono::DateTime<chrono::Utc>) -> String {
    t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

/// Render `NAME  AGENT  CASES  UPDATED  DESCRIPTION` rows.
fn format_eval_set_list(v: &EvalSetListResponse) -> String {
    let rows: Vec<(String, String, String, String, String)> = v
        .items
        .iter()
        .map(|s| {
            (
                s.name.clone(),
                s.agent.clone(),
                s.case_count.to_string(),
                format_timestamp(&s.updated_at),
                s.description.clone().unwrap_or_default(),
            )
        })
        .collect();
    super::format_table(
        ["NAME", "AGENT", "CASES", "UPDATED", "DESCRIPTION"],
        &rows,
        "No eval sets.",
    )
}

/// Keeps a table cell on one line and at most `max` characters.
fn one_line(s: &str, max: usize) -> String {
    let flat = s.split_whitespace().collect::<Vec<_>>().join(" ");
    if flat.chars().count() <= max {
        flat
    } else {
        let cut: String = flat.chars().take(max.saturating_sub(1)).collect();
        format!("{cut}…")
    }
}

fn format_source(source: Option<&EvalCaseSource>) -> String {
    match source {
        Some(EvalCaseSource::Trace(id)) => format!("trace:{id}"),
        Some(EvalCaseSource::Upload) => "upload".to_string(),
        Some(EvalCaseSource::HandWritten) | None => "hand_written".to_string(),
    }
}

/// A header block for the set followed by `ID  SOURCE  EXPECTED TOOLS  TAGS
/// INPUT` rows, one per case in order.
fn format_eval_set(v: &EvalSetResponse) -> String {
    let mut out = format!(
        "Name:        {}\nAgent:       {}\nDataset:     {}\nCases:       {}\nUpdated:     {}\n",
        v.name,
        v.agent,
        v.dataset,
        v.case_count,
        format_timestamp(&v.updated_at)
    );
    if let Some(description) = &v.description {
        let _ = writeln!(out, "Description: {description}");
    }
    out.push('\n');
    let rows: Vec<(String, String, String, String, String)> = v
        .cases
        .iter()
        .map(|c| {
            (
                c.id.clone(),
                format_source(c.source.as_ref()),
                c.expected_tools.join(", "),
                c.tags.join(", "),
                one_line(&c.input, 80),
            )
        })
        .collect();
    out.push_str(&super::format_table(
        ["ID", "SOURCE", "EXPECTED TOOLS", "TAGS", "INPUT"],
        &rows,
        "No cases.",
    ));
    out
}

fn format_append_outcome(name: &str, o: &AppendCasesOutcome) -> String {
    let mut out = format!(
        "Appended to '{name}': {} added, {} already present.",
        o.added, o.already_present
    );
    if !o.already_present_ids.is_empty() {
        let _ = write!(
            out,
            "\nAlready present: {}",
            o.already_present_ids.join(", ")
        );
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::test_support::{connect, write_temp};

    const SET_BODY: &str = r#"{
        "name": "refunds", "agent": "support-triage", "description": "March misses",
        "case_count": 2, "dataset": "production", "tenant_id": "acme",
        "created_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-02T00:00:00Z",
        "cases": [
            {"id": "edge-01", "input": "Refund order 1182", "expected_tools": ["lookup_order"],
             "source": {"kind": "trace", "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736"}},
            {"id": "edge-02", "input": "Cancel my\nsubscription"}
        ],
        "_links": {"self": {"href": "/api/v1/eval-sets/refunds"}}
    }"#;

    fn object(json: &str) -> serde_json::Map<String, serde_json::Value> {
        serde_json::from_str(json).expect("fixture is a JSON object")
    }

    #[test]
    fn replace_spec_fills_in_a_missing_name_and_rejects_a_different_one() {
        let path = Path::new("set.json");
        let spec =
            parse_spec(object(r#"{"agent": "a"}"#), path, Some("refunds")).expect("fills name");
        assert_eq!(spec.name, "refunds");

        let err = parse_spec(
            object(r#"{"name": "other", "agent": "a"}"#),
            path,
            Some("refunds"),
        )
        .expect_err("a different name must fail");
        assert!(err.to_string().contains("'other'"), "{err}");

        let err =
            parse_spec(object(r#"{"agent": "a"}"#), path, None).expect_err("create needs a name");
        assert!(err.to_string().contains("not a valid eval set"), "{err}");
    }

    #[test]
    fn cases_parse_from_jsonl_or_a_cases_object() {
        let path = Path::new("cases.jsonl");
        let jsonl = "{\"id\": \"a\", \"input\": \"x\"}\n\n{\"id\": \"b\", \"input\": \"y\", \"source\": {\"kind\": \"upload\"}}\n";
        let cases = parse_cases(jsonl, path).expect("jsonl");
        assert_eq!(
            cases.iter().map(|c| c.id.as_str()).collect::<Vec<_>>(),
            ["a", "b"]
        );

        let object = "{\"cases\": [\n  {\"id\": \"c\", \"input\": \"z\"}\n]}";
        let cases = parse_cases(object, path).expect("cases object");
        assert_eq!(cases[0].id, "c");

        let err = parse_cases("{\"id\": \"a\", \"input\": \"x\"}\n{\"id\": \"b\"}\n", path)
            .expect_err("missing input");
        assert!(err.to_string().contains("cases.jsonl:2"), "{err}");
        let err = parse_cases(r#"{"cases": [{"id": "c"}]}"#, path).expect_err("bad cases");
        assert!(err.to_string().contains("invalid `cases`"), "{err}");
        assert!(parse_cases("\n  \n", path).is_err());
    }

    #[test]
    fn export_writes_one_case_per_line_that_parses_back() {
        let set: EvalSetResponse = serde_json::from_str(SET_BODY).expect("fixture parses");
        let mut out = Vec::new();
        write_jsonl(&set.cases, &mut out).expect("serializes");
        let jsonl = String::from_utf8(out).expect("utf-8");
        assert_eq!(jsonl.lines().count(), 2);
        let back = parse_cases(&jsonl, Path::new("export.jsonl")).expect("round-trips");
        assert_eq!(back[0].id, "edge-01");
        assert_eq!(back[1].input, "Cancel my\nsubscription");
        assert!(matches!(back[0].source, Some(EvalCaseSource::Trace(_))));
    }

    #[test]
    fn human_output_lists_sets_and_cases() {
        let list: EvalSetListResponse = serde_json::from_str(
            r#"{"items": [{"name": "refunds", "agent": "support-triage", "case_count": 2,
                "created_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-02T00:00:00Z",
                "_links": {"self": {"href": "/api/v1/eval-sets/refunds"}}}],
               "_links": {"self": {"href": "/api/v1/eval-sets"}}}"#,
        )
        .expect("list parses");
        let table = format_eval_set_list(&list);
        assert!(table.starts_with("NAME"), "{table}");
        assert!(table.contains("refunds") && table.contains("support-triage"));

        let set: EvalSetResponse = serde_json::from_str(SET_BODY).expect("set parses");
        let out = format_eval_set(&set);
        assert!(out.contains("Agent:       support-triage"), "{out}");
        assert!(out.contains("Description: March misses"), "{out}");
        assert!(
            out.contains("trace:4bf92f3577b34da6a3ce929d0e0e4736"),
            "{out}"
        );
        assert!(out.contains("Cancel my subscription"), "{out}");

        let outcome = AppendCasesOutcome {
            added: 1,
            added_ids: vec!["edge-41".to_string()],
            already_present: 1,
            already_present_ids: vec!["edge-40".to_string()],
        };
        let out = format_append_outcome("refunds", &outcome);
        assert!(out.contains("1 added, 1 already present"), "{out}");
        assert!(out.contains("Already present: edge-40"), "{out}");
    }

    #[tokio::test]
    async fn list_sends_the_tenant_and_dataset_headers() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/api/v1/eval-sets")
            .match_header("authorization", "Bearer sk-test")
            .match_header("x-tenant-id", "acme")
            .match_header("x-dataset-id", "production")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"items": [], "_links": {"self": {"href": "/api/v1/eval-sets"}}}"#)
            .create_async()
            .await;

        EvalSetsAction::List(OutputArgs {
            connect: connect(&server.url()),
            json: false,
        })
        .run()
        .await
        .expect("eval-sets list succeeds");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn create_posts_the_file_body() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/api/v1/eval-sets")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "name": "refunds", "agent": "support-triage",
                "cases": [{"id": "edge-01", "input": "Refund order 1182"}]
            })))
            .with_status(201)
            .with_header("content-type", "application/json")
            .with_body(SET_BODY)
            .create_async()
            .await;

        let file = write_temp(
            "create.yaml",
            "name: refunds\nagent: support-triage\ncases:\n  - id: edge-01\n    input: Refund order 1182\n",
        );
        AdminEvalSetsAction::Create {
            file: file.path().to_path_buf(),
            connect: connect(&server.url()),
        }
        .run()
        .await
        .expect("admin eval-sets create succeeds");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn append_posts_jsonl_cases() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/api/v1/eval-sets/refunds/cases")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "cases": [{"id": "edge-40", "input": "a"}, {"id": "edge-41", "input": "b"}]
            })))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"added": 1, "already_present": 1, "added_ids": ["edge-41"],
                    "already_present_ids": ["edge-40"]}"#,
            )
            .create_async()
            .await;

        let file = write_temp(
            "append.jsonl",
            "{\"id\": \"edge-40\", \"input\": \"a\"}\n{\"id\": \"edge-41\", \"input\": \"b\"}\n",
        );
        AdminEvalSetsAction::Append {
            name: "refunds".to_string(),
            file: file.path().to_path_buf(),
            connect: connect(&server.url()),
        }
        .run()
        .await
        .expect("admin eval-sets append succeeds");
        mock.assert_async().await;
    }
}
