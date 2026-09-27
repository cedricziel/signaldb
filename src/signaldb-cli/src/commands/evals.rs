//! The `evals` command group (openspec change `agent-offline-evals`, task
//! 8.1b): `signaldb-cli evals upload` sends a JSONL/CSV results file as one
//! offline run through `POST /api/v1/evals/results` (`evals:write`), prints
//! the run's per-evaluator summary and a link to it in the UI, and — as a CI
//! gate — exits non-zero when a `--fail-if` condition holds for the run.
//!
//! `--compare-to latest:<version>` resolves to the newest other run of the
//! same agent, version and eval set through a Query IR read
//! (`POST /api/v1/query`, `logs:read`).

use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::Context;
use clap::Subcommand;
use signaldb_sdk::types::{
    EvalResultsFormat, EvalResultsUploadResponse, QueryIrRequest, QueryRange,
};

use super::discover::ConnectArgs;

/// `signaldb-cli evals <verb>` — offline evaluation results.
#[derive(Subcommand)]
pub enum EvalsAction {
    /// Upload a JSONL or CSV results file as one offline run; with
    /// `--fail-if`, exit non-zero when a condition holds for the run (CI gate)
    Upload(UploadArgs),
}

/// File format of `evals upload`.
#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileFormat {
    Csv,
    Jsonl,
}

#[derive(clap::Args)]
pub struct UploadArgs {
    /// Results file: JSONL (one object per line) or CSV with a header row.
    /// Columns: case_id, name (required); score, label, explanation,
    /// trace_id, span_id, evaluator, error, trial
    file: PathBuf,
    /// The agent the run evaluated (`gen_ai.agent.name`)
    #[arg(long)]
    agent: String,
    /// The agent version under test (`gen_ai.agent.version`)
    #[arg(long)]
    version: String,
    /// The eval set the run replayed
    #[arg(long)]
    set: String,
    /// Run id [default: a generated UUID, printed before the upload]. Retrying
    /// an upload with the same file and run id is safe: a copy that already
    /// landed is not written twice
    #[arg(long)]
    run_id: Option<String>,
    /// File format [default: from the extension: .csv, or .jsonl/.ndjson]
    #[arg(long, value_enum)]
    format: Option<FileFormat>,
    /// Baseline to link the comparison against: a run id, or
    /// `latest:<version>` for the newest other run of this agent and eval set
    /// at that version
    #[arg(long, value_name = "RUN_ID|latest:VERSION")]
    compare_to: Option<String>,
    /// Fail (exit non-zero) when this holds for the uploaded run:
    /// `<evaluator>.mean|pass_rate <op> <number>`, op one of
    /// `< <= > >= == !=` (repeatable), e.g. "Correctness.pass_rate < 0.9"
    #[arg(long = "fail-if", value_name = "EXPR")]
    fail_if: Vec<String>,
    /// Print the upload response as JSON instead of the summary
    #[arg(long)]
    json: bool,
    #[command(flatten)]
    connect: ConnectArgs,
}

/// The run statistic a `--fail-if` condition reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Metric {
    Mean,
    PassRate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    Lt,
    Le,
    Gt,
    Ge,
    Eq,
    Ne,
}

impl Op {
    /// Longest first, so `<=` is not read as `<`.
    const TOKENS: [(&'static str, Op); 6] = [
        ("<=", Op::Le),
        (">=", Op::Ge),
        ("==", Op::Eq),
        ("!=", Op::Ne),
        ("<", Op::Lt),
        (">", Op::Gt),
    ];

    fn holds(self, value: f64, threshold: f64) -> bool {
        match self {
            Op::Lt => value < threshold,
            Op::Le => value <= threshold,
            Op::Gt => value > threshold,
            Op::Ge => value >= threshold,
            Op::Eq => value == threshold,
            Op::Ne => value != threshold,
        }
    }
}

/// One parsed `--fail-if` expression.
#[derive(Debug, Clone, PartialEq)]
struct FailCondition {
    expr: String,
    evaluator: String,
    metric: Metric,
    op: Op,
    threshold: f64,
}

impl fmt::Display for FailCondition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.expr)
    }
}

impl FromStr for FailCondition {
    type Err = anyhow::Error;

    fn from_str(expr: &str) -> anyhow::Result<Self> {
        let bad = |why: &str| {
            anyhow::anyhow!(
                "--fail-if `{expr}`: {why} (expected `<evaluator>.mean|pass_rate <op> <number>`)"
            )
        };
        let at = expr
            .find(['<', '>', '=', '!'])
            .ok_or_else(|| bad("no comparison operator"))?;
        let (lhs, rest) = expr.split_at(at);
        let (token, op) = Op::TOKENS
            .iter()
            .find(|(token, _)| rest.starts_with(token))
            .ok_or_else(|| bad("unknown operator"))?;
        let threshold: f64 = rest[token.len()..]
            .trim()
            .parse()
            .map_err(|_| bad("the right-hand side is not a number"))?;
        let (evaluator, metric) = lhs
            .trim()
            .rsplit_once('.')
            .ok_or_else(|| bad("the left-hand side is not `<evaluator>.<metric>`"))?;
        let metric = match metric {
            "mean" => Metric::Mean,
            "pass_rate" => Metric::PassRate,
            _ => return Err(bad("the metric must be `mean` or `pass_rate`")),
        };
        if evaluator.is_empty() {
            return Err(bad("the evaluator name is empty"));
        }
        Ok(Self {
            expr: expr.trim().to_string(),
            evaluator: evaluator.to_string(),
            metric,
            op: *op,
            threshold,
        })
    }
}

/// The conditions that hold for the run, as messages. A condition naming an
/// evaluator the run lacks, or a metric the evaluator has no value for,
/// counts as failing with its own message.
fn failing_conditions(
    conditions: &[FailCondition],
    run: &EvalResultsUploadResponse,
) -> Vec<String> {
    conditions
        .iter()
        .filter_map(|c| {
            let Some(evaluator) = run.evaluators.iter().find(|e| e.name == c.evaluator) else {
                let known: Vec<&str> = run.evaluators.iter().map(|e| e.name.as_str()).collect();
                return Some(format!(
                    "{c}: evaluator `{}` is not in this run (evaluators: {})",
                    c.evaluator,
                    known.join(", ")
                ));
            };
            let (value, what) = match c.metric {
                Metric::Mean => (evaluator.mean, "no scored results"),
                Metric::PassRate => (evaluator.pass_rate, "no results with a verdict"),
            };
            match value {
                None => Some(format!("{c}: `{}` has {what}", c.evaluator)),
                Some(v) if c.op.holds(v, c.threshold) => Some(format!("{c} (actual {v:.4})")),
                Some(_) => None,
            }
        })
        .collect()
}

fn file_format(path: &Path, explicit: Option<FileFormat>) -> anyhow::Result<EvalResultsFormat> {
    let format = match explicit {
        Some(format) => format,
        None => match path
            .extension()
            .and_then(|e| e.to_str())
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            Some("csv") => FileFormat::Csv,
            Some("jsonl" | "ndjson") => FileFormat::Jsonl,
            _ => anyhow::bail!(
                "cannot tell the format of {} from its extension; pass --format csv|jsonl",
                path.display()
            ),
        },
    };
    Ok(match format {
        FileFormat::Csv => EvalResultsFormat::Csv,
        FileFormat::Jsonl => EvalResultsFormat::Jsonl,
    })
}

/// Percent-encodes a query-string value.
fn encode(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

fn compare_url(base: &str, baseline: &str, candidate: &str) -> String {
    format!(
        "{}/evals/compare?baseline={}&candidate={}",
        base.trim_end_matches('/'),
        encode(baseline),
        encode(candidate)
    )
}

fn runs_url(base: &str) -> String {
    format!("{}/evals/runs", base.trim_end_matches('/'))
}

/// How far back `latest:<version>` looks.
const LATEST_LOOKBACK: &str = "now-30d";

/// The Query IR document behind `latest:<version>`: the last result time
/// of every other run of the agent, version and eval set.
fn latest_run_document(
    agent: &str,
    version: &str,
    set: &str,
    exclude_run: &str,
) -> anyhow::Result<QueryIrRequest> {
    let eq = |field: &str, value: &str| serde_json::json!({"where": {"field": field, "op": "eq", "value": value}});
    let pipeline = [
        eq("event_name", "gen_ai.evaluation.result"),
        eq("gen_ai.agent.name", agent),
        eq("gen_ai.agent.version", version),
        eq("signaldb.eval.set", set),
        serde_json::json!({"where": {"field": "signaldb.eval.run_id", "op": "ne", "value": exclude_run}}),
        serde_json::json!({"aggregate": {
            "by": ["signaldb.eval.run_id"],
            "aggs": [{"fn": "max", "of": "timestamp", "as": "last"}]
        }}),
    ]
    .into_iter()
    .map(|stage| match stage {
        serde_json::Value::Object(map) => Ok(map),
        _ => anyhow::bail!("pipeline stage is not an object"),
    })
    .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(QueryIrRequest {
        depth: None,
        fields: None,
        focus: None,
        from: "logs".to_string(),
        ir_version: 4,
        pipeline,
        range: QueryRange {
            from: LATEST_LOOKBACK.to_string(),
            to: "now".to_string(),
        },
        result: "table".to_string(),
        trace_id: None,
    })
}

/// The run id with the latest `last` in a [`latest_run_document`] result.
fn newest_run(response: &signaldb_sdk::types::QueryIrResponse) -> Option<String> {
    let column = |names: &[&str]| {
        response
            .columns
            .iter()
            .position(|c| names.contains(&c.name.as_str()))
    };
    let run_col = column(&["signaldb_eval_run_id", "signaldb.eval.run_id"])?;
    let last_col = column(&["last"])?;
    response
        .rows
        .iter()
        .filter_map(|row| {
            let run = row.get(run_col)?.as_str().filter(|s| !s.is_empty())?;
            let last = row.get(last_col).and_then(|v| {
                v.as_f64()
                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
            })?;
            Some((last, run))
        })
        .max_by(|a, b| a.0.total_cmp(&b.0))
        .map(|(_, run)| run.to_string())
}

impl UploadArgs {
    async fn resolve_baseline(
        &self,
        client: &signaldb_sdk::Client,
        run: &EvalResultsUploadResponse,
    ) -> anyhow::Result<Option<String>> {
        let Some(target) = &self.compare_to else {
            return Ok(None);
        };
        let Some(version) = target.strip_prefix("latest:") else {
            return Ok(Some(target.clone()));
        };
        let document = latest_run_document(&run.agent, version, &run.set, &run.run_id)?;
        let response = client
            .query_ir()
            .body(document)
            .send()
            .await
            .map_err(|e| anyhow::Error::new(e).context(format!("resolving --compare-to {target}")))?
            .into_inner();
        let baseline = newest_run(&response);
        if baseline.is_none() {
            eprintln!(
                "warning: no other run of {} {version} on {} in the last 30 days; not comparing",
                run.agent, run.set
            );
        }
        Ok(baseline)
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let conditions = self
            .fail_if
            .iter()
            .map(|expr| expr.parse::<FailCondition>())
            .collect::<anyhow::Result<Vec<_>>>()?;
        let format = file_format(&self.file, self.format)?;
        let text = tokio::fs::read_to_string(&self.file)
            .await
            .with_context(|| format!("failed to read {}", self.file.display()))?;
        // Chosen here rather than by the server so a retry after a lost
        // response can reuse it and be deduplicated.
        let run_id = match self.run_id.as_deref().map(str::trim) {
            Some(run_id) if !run_id.is_empty() => run_id.to_string(),
            _ => {
                let run_id = uuid::Uuid::new_v4().to_string();
                eprintln!("Run id: {run_id} (pass --run-id {run_id} to retry this upload safely)");
                run_id
            }
        };

        let client = self.connect.build_client()?;
        let request = client
            .upload_eval_results()
            .agent(&self.agent)
            .version(&self.version)
            .set(&self.set)
            .run_id(&run_id)
            .format(format)
            .body(text);
        let run = request
            .send()
            .await
            .map_err(|e| upload_error(e, &self.file, &run_id))?
            .into_inner();

        let baseline = self.resolve_baseline(&client, &run).await?;
        let link = match &baseline {
            Some(baseline) => compare_url(&self.connect.url, baseline, &run.run_id),
            None => runs_url(&self.connect.url),
        };
        if self.json {
            super::print_json(&run)?;
        } else {
            println!("{}", format_summary(&run));
            println!(
                "{}: {link}",
                if baseline.is_some() {
                    "Compare"
                } else {
                    "Runs"
                }
            );
        }

        let failing = failing_conditions(&conditions, &run);
        if failing.is_empty() {
            return Ok(());
        }
        for message in &failing {
            eprintln!("fail-if: {message}");
        }
        anyhow::bail!(
            "{} of {} --fail-if conditions failed for run {}",
            failing.len(),
            conditions.len(),
            run.run_id
        )
    }
}

/// An upload failure, with the server's row errors listed under it.
fn upload_error(
    err: signaldb_sdk::Error<signaldb_sdk::types::ApiErrorBody>,
    file: &Path,
    run_id: &str,
) -> anyhow::Error {
    let details = match &err {
        signaldb_sdk::Error::ErrorResponse(response) => response
            .details
            .iter()
            .flatten()
            .map(|d| {
                let location = match (d.row, &d.column) {
                    (Some(row), Some(column)) => format!("{}:{row} `{column}`", file.display()),
                    (Some(row), None) => format!("{}:{row}", file.display()),
                    (None, Some(column)) => format!("{} `{column}`", file.display()),
                    (None, None) => file.display().to_string(),
                };
                format!("\n  {location}: {}", d.reason)
            })
            .collect::<String>(),
        _ => String::new(),
    };
    // Without a response the upload may still have landed; the same run id
    // makes the retry a no-op in that case.
    let retry = match &err {
        signaldb_sdk::Error::ErrorResponse(response) if response.status().is_client_error() => {
            String::new()
        }
        _ => format!("\n  retry with --run-id {run_id}"),
    };
    anyhow::Error::new(err).context(format!("evals upload failed{details}{retry}"))
}

fn format_ratio(value: Option<f64>) -> String {
    value.map_or_else(|| "-".to_string(), |v| format!("{v:.3}"))
}

/// A header line for the run, then `EVALUATOR  RESULTS  ERRORS  MEAN
/// PASS RATE` rows.
fn format_summary(run: &EvalResultsUploadResponse) -> String {
    let rows: Vec<(String, String, String, String, String)> = run
        .evaluators
        .iter()
        .map(|e| {
            (
                e.name.clone(),
                e.results.to_string(),
                e.errors.to_string(),
                format_ratio(e.mean),
                format_ratio(e.pass_rate),
            )
        })
        .collect();
    format!(
        "Uploaded run {} ({} {} on {}): {} results, {} cases, {} span-linked, {} run-level\n\n{}\n",
        run.run_id,
        run.agent,
        run.version,
        run.set,
        run.rows,
        run.cases,
        run.span_linked,
        run.run_level,
        super::format_table(
            ["EVALUATOR", "RESULTS", "ERRORS", "MEAN", "PASS RATE"],
            &rows,
            "No evaluators.",
        )
    )
}

impl EvalsAction {
    pub async fn run(self) -> anyhow::Result<()> {
        match self {
            EvalsAction::Upload(args) => args.run().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::test_support::{connect, write_temp};

    const RESPONSE: &str = r#"{
        "run_id": "run-42", "agent": "support-triage", "version": "v1.9.0",
        "set": "triage-golden", "rows": 5, "cases": 3, "span_linked": 4, "run_level": 1,
        "evaluators": [
            {"name": "Correctness", "results": 4, "errors": 1, "mean": 0.84, "pass_rate": 0.75},
            {"name": "Tone", "results": 1, "errors": 0, "mean": null, "pass_rate": null}
        ],
        "_links": {"query": {"href": "/api/v1/query", "method": "POST"},
                   "runs": {"href": "/evals/runs"}}
    }"#;

    fn response() -> EvalResultsUploadResponse {
        serde_json::from_str(RESPONSE).expect("fixture parses")
    }

    fn condition(expr: &str) -> FailCondition {
        expr.parse().expect("valid condition")
    }

    #[test]
    fn fail_if_expressions_parse_every_operator() {
        let c = condition("ToolTrajectory.mean < 0.85");
        assert_eq!(c.evaluator, "ToolTrajectory");
        assert_eq!(c.metric, Metric::Mean);
        assert_eq!(c.op, Op::Lt);
        assert_eq!(c.threshold, 0.85);
        for (expr, op) in [
            ("A.pass_rate<=1", Op::Le),
            ("A.pass_rate >= 0.5", Op::Ge),
            ("A.mean == 1", Op::Eq),
            ("A.mean != 1", Op::Ne),
            ("A.mean > .5", Op::Gt),
        ] {
            assert_eq!(condition(expr).op, op, "{expr}");
        }
        let dotted = condition("judge@2.1.pass_rate < 0.9");
        assert_eq!(dotted.evaluator, "judge@2.1");
        assert_eq!(dotted.metric, Metric::PassRate);
    }

    #[test]
    fn malformed_fail_if_expressions_are_rejected() {
        for expr in [
            "Correctness.mean",
            "Correctness.median < 1",
            "Correctness < 1",
            ".mean < 1",
            "Correctness.mean < high",
            "Correctness.mean => 1",
        ] {
            let err = expr.parse::<FailCondition>().expect_err(expr);
            assert!(err.to_string().contains("--fail-if"), "{err}");
        }
    }

    #[test]
    fn conditions_are_checked_against_the_run_summary() {
        let run = response();
        let failing = failing_conditions(
            &[
                condition("Correctness.mean < 0.85"),
                condition("Correctness.pass_rate < 0.5"),
                condition("Missing.mean < 1"),
                condition("Tone.pass_rate < 0.5"),
            ],
            &run,
        );
        assert_eq!(failing.len(), 3, "{failing:?}");
        assert!(
            failing[0].starts_with("Correctness.mean < 0.85 (actual 0.8400)"),
            "{failing:?}"
        );
        assert!(
            failing[1].contains("`Missing` is not in this run"),
            "{failing:?}"
        );
        assert!(failing[1].contains("Correctness, Tone"), "{failing:?}");
        assert!(
            failing[2].contains("no results with a verdict"),
            "{failing:?}"
        );
    }

    #[test]
    fn the_format_comes_from_the_flag_or_the_extension() {
        let path = Path::new("results.CSV");
        assert!(matches!(
            file_format(path, None),
            Ok(EvalResultsFormat::Csv)
        ));
        assert!(matches!(
            file_format(Path::new("r.ndjson"), None),
            Ok(EvalResultsFormat::Jsonl)
        ));
        assert!(matches!(
            file_format(Path::new("r.txt"), Some(FileFormat::Jsonl)),
            Ok(EvalResultsFormat::Jsonl)
        ));
        assert!(file_format(Path::new("r.txt"), None).is_err());
    }

    #[test]
    fn links_point_at_the_ui_pages() {
        assert_eq!(
            compare_url("http://sdb:3000/", "run a/b", "run-42"),
            "http://sdb:3000/evals/compare?baseline=run+a%2Fb&candidate=run-42"
        );
        assert_eq!(runs_url("http://sdb:3000"), "http://sdb:3000/evals/runs");
        let out = format_summary(&response());
        assert!(out.contains("Uploaded run run-42"), "{out}");
        assert!(
            out.contains("Correctness") && out.contains("0.840"),
            "{out}"
        );
    }

    #[test]
    fn the_newest_other_run_wins() {
        let response: signaldb_sdk::types::QueryIrResponse = serde_json::from_value(serde_json::json!({
            "result": "table",
            "columns": [{"name": "signaldb_eval_run_id", "type": "string"}, {"name": "last", "type": "int"}],
            "rows": [["run-a", 100], ["run-b", 300], ["", 900], ["run-c", 200]],
            "window": {"start_ns": 0, "end_ns": 1}
        }))
        .expect("response parses");
        assert_eq!(newest_run(&response).as_deref(), Some("run-b"));
        let doc = serde_json::to_value(latest_run_document("a", "v1", "golden", "run-42").unwrap())
            .unwrap();
        assert_eq!(doc["pipeline"][4]["where"]["op"], "ne");
        assert_eq!(doc["pipeline"][4]["where"]["value"], "run-42");
    }

    fn upload_args(file: &Path, url: &str, extra: &[&str]) -> UploadArgs {
        #[derive(clap::Parser)]
        struct Cli {
            #[command(flatten)]
            args: UploadArgs,
        }
        let file = file.to_string_lossy().to_string();
        let argv = [
            "upload",
            file.as_str(),
            "--agent",
            "support-triage",
            "--version",
            "v1.9.0",
            "--set",
            "triage-golden",
        ]
        .into_iter()
        .chain(extra.iter().copied());
        let mut args = <Cli as clap::Parser>::try_parse_from(argv)
            .expect("flags parse")
            .args;
        args.connect = connect(url);
        args
    }

    #[tokio::test]
    async fn upload_sends_the_file_and_the_run_metadata() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/api/v1/evals/results")
            .match_query(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded("agent".into(), "support-triage".into()),
                mockito::Matcher::UrlEncoded("version".into(), "v1.9.0".into()),
                mockito::Matcher::UrlEncoded("set".into(), "triage-golden".into()),
                mockito::Matcher::UrlEncoded("format".into(), "csv".into()),
                mockito::Matcher::UrlEncoded("run_id".into(), "run-42".into()),
            ]))
            .match_header("authorization", "Bearer sk-test")
            .match_header("x-dataset-id", "production")
            .match_body("case_id,name,score\ncase-1,Correctness,0.84\n")
            .with_status(201)
            .with_header("content-type", "application/json")
            .with_body(RESPONSE)
            .create_async()
            .await;
        let file = write_temp(
            "results.csv",
            "case_id,name,score\ncase-1,Correctness,0.84\n",
        );
        upload_args(
            file.path(),
            &server.url(),
            &["--run-id", "run-42", "--fail-if", "Correctness.mean < 0.8"],
        )
        .run()
        .await
        .expect("upload succeeds and the gate passes");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn without_a_run_id_the_cli_generates_and_sends_one() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/api/v1/evals/results")
            .match_query(mockito::Matcher::Regex(
                "(^|&)run_id=[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}(&|$)"
                    .into(),
            ))
            .with_status(201)
            .with_header("content-type", "application/json")
            .with_body(RESPONSE)
            .create_async()
            .await;
        let file = write_temp(
            "results.csv",
            "case_id,name,score
a,C,1
",
        );
        upload_args(file.path(), &server.url(), &[])
            .run()
            .await
            .expect("upload succeeds");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn a_holding_condition_fails_the_command_after_the_upload() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/api/v1/evals/results")
            .match_query(mockito::Matcher::Any)
            .with_status(201)
            .with_header("content-type", "application/json")
            .with_body(RESPONSE)
            .create_async()
            .await;
        let file = write_temp(
            "results.jsonl",
            "{\"case_id\": \"a\", \"name\": \"Correctness\", \"score\": 0.84}\n",
        );
        let err = upload_args(
            file.path(),
            &server.url(),
            &["--fail-if", "Correctness.mean < 0.85"],
        )
        .run()
        .await
        .expect_err("the gate fails");
        assert!(
            err.to_string()
                .contains("1 of 1 --fail-if conditions failed"),
            "{err}"
        );
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn a_malformed_condition_fails_before_anything_is_sent() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", mockito::Matcher::Any)
            .expect(0)
            .create_async()
            .await;
        let file = write_temp("results.csv", "case_id,name\n");
        let err = upload_args(
            file.path(),
            &server.url(),
            &["--fail-if", "Correctness.p95 < 1"],
        )
        .run()
        .await
        .expect_err("bad expression");
        assert!(err.to_string().contains("--fail-if"), "{err}");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn row_errors_from_the_server_are_listed() {
        let mut server = mockito::Server::new_async().await;
        server
            .mock("POST", "/api/v1/evals/results")
            .match_query(mockito::Matcher::Any)
            .with_status(400)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"status": "error", "errorType": "bad_data",
                    "error": "invalid results file: row 2, `score`: must be a finite number",
                    "details": [{"row": 2, "column": "score", "reason": "must be a finite number"}]}"#,
            )
            .create_async()
            .await;
        let file = write_temp("results.csv", "case_id,name,score\na,C,x\n");
        let err = upload_args(file.path(), &server.url(), &[])
            .run()
            .await
            .expect_err("rejected");
        let text = err.to_string();
        assert!(
            text.contains("results.csv:2 `score`: must be a finite number"),
            "{text}"
        );
    }

    #[tokio::test]
    async fn compare_to_latest_resolves_through_the_query_ir() {
        let mut server = mockito::Server::new_async().await;
        server
            .mock("POST", "/api/v1/evals/results")
            .match_query(mockito::Matcher::Any)
            .with_status(201)
            .with_header("content-type", "application/json")
            .with_body(RESPONSE)
            .create_async()
            .await;
        let query = server
            .mock("POST", "/api/v1/query")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "from": "logs",
                "pipeline": [
                    {"where": {"field": "event_name", "op": "eq", "value": "gen_ai.evaluation.result"}},
                    {"where": {"field": "gen_ai.agent.name", "op": "eq", "value": "support-triage"}},
                    {"where": {"field": "gen_ai.agent.version", "op": "eq", "value": "v1.8.0"}},
                    {"where": {"field": "signaldb.eval.set", "op": "eq", "value": "triage-golden"}},
                    {"where": {"field": "signaldb.eval.run_id", "op": "ne", "value": "run-42"}}
                ]
            })))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"result": "table",
                    "columns": [{"name": "signaldb_eval_run_id", "type": "string"},
                                {"name": "last", "type": "int"}],
                    "rows": [["run-41", 10]], "window": {"start_ns": 0, "end_ns": 1}}"#,
            )
            .create_async()
            .await;
        let file = write_temp("results.csv", "case_id,name,score\na,C,1\n");
        let args = upload_args(
            file.path(),
            &server.url(),
            &["--compare-to", "latest:v1.8.0"],
        );
        let client = args.connect.build_client().expect("client");
        let baseline = args
            .resolve_baseline(&client, &response())
            .await
            .expect("resolves");
        assert_eq!(baseline.as_deref(), Some("run-41"));
        query.assert_async().await;
    }
}
