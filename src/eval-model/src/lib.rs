//! # The eval run/comparison model
//!
//! A leaf crate: the attribute names and pass rule an evaluator result is
//! judged by (design D3), the run and comparison semantics of the Evaluate
//! pages ([`compare`], design D4, D5), and the Query IR reads the CLI
//! (`evals runs|compare`) and MCP tools (`list_eval_runs`,
//! `compare_eval_runs`) build on them ([`runs`]) — mirroring the UI's
//! `features/evals/evalModel.ts` so every client judges a result the same
//! way. It is its own crate so `signaldb-cli` can depend on it instead of
//! on `common` and building it; `common::evals` re-exports
//! it for in-process consumers (the router, the MCP server) alongside its
//! server-only modules (the upload parser, span-event conversion), which
//! reach into OTLP proto types this crate does not depend on.

use opentelemetry_semantic_conventions::attribute;

pub mod compare;
pub mod runs;
#[cfg(any(test, feature = "testing"))]
pub mod test_util;

/// `event_name` of an evaluator result log record. The semconv crate has no
/// constant for this event name (only for the attributes on it), so it stays
/// a local literal.
pub const EVALUATION_RESULT_EVENT: &str = "gen_ai.evaluation.result";
/// The evaluator's name, e.g. `Correctness`.
///
/// The pinned `opentelemetry-semantic-conventions` crate still carries the
/// `gen_ai.*` GenAI attributes but marks them deprecated (moved to a
/// separate GenAI semconv repo upstream); the wire names are unchanged, so
/// `#[allow(deprecated)]` below just silences the migration notice, same as
/// `self_monitoring::spans`'s use of `GEN_AI_TOOL_NAME`.
#[allow(deprecated)]
pub const EVALUATION_NAME: &str = attribute::GEN_AI_EVALUATION_NAME;
/// Numeric score.
#[allow(deprecated)]
pub const EVALUATION_SCORE_VALUE: &str = attribute::GEN_AI_EVALUATION_SCORE_VALUE;
/// Verdict label, e.g. `pass` / `fail`.
#[allow(deprecated)]
pub const EVALUATION_SCORE_LABEL: &str = attribute::GEN_AI_EVALUATION_SCORE_LABEL;
/// The judge's reasoning.
#[allow(deprecated)]
pub const EVALUATION_EXPLANATION: &str = attribute::GEN_AI_EVALUATION_EXPLANATION;
/// Set when the evaluator itself failed.
pub const ERROR_TYPE: &str = attribute::ERROR_TYPE;

/// Groups results into one offline run (design D2); absent on production
/// results. The `signaldb.eval.*` names are SignalDB's own: semconv has no
/// run or eval-set layer.
pub const RUN_ID: &str = "signaldb.eval.run_id";
/// The eval set the run replayed.
pub const SET: &str = "signaldb.eval.set";
/// The case within the set: the join key between runs.
pub const CASE_ID: &str = "signaldb.eval.case_id";
/// Evaluator implementation and version, e.g. `trajectory-match@2.1.0`.
pub const EVALUATOR: &str = "signaldb.eval.evaluator";
/// Trial index for a case run several times.
pub const TRIAL: &str = "signaldb.eval.trial";

/// `gen_ai.operation.name` of a GenAI span (`invoke_agent`, `execute_tool`, `chat`).
#[allow(deprecated)]
pub const OPERATION_NAME: &str = attribute::GEN_AI_OPERATION_NAME;
/// The operation of an agent invocation span.
pub const OPERATION_INVOKE_AGENT: &str = "invoke_agent";
/// The operation of a tool call span.
pub const OPERATION_EXECUTE_TOOL: &str = "execute_tool";
/// `gen_ai.agent.name` on an agent span.
#[allow(deprecated)]
pub const AGENT_NAME: &str = attribute::GEN_AI_AGENT_NAME;
/// `gen_ai.agent.version` on an agent span or result.
#[allow(deprecated)]
pub const AGENT_VERSION: &str = attribute::GEN_AI_AGENT_VERSION;
/// `gen_ai.tool.name` on a tool call span.
#[allow(deprecated)]
pub const TOOL_NAME: &str = attribute::GEN_AI_TOOL_NAME;
/// The agent span's input messages: a JSON array of `{role, parts}`.
#[allow(deprecated)]
pub const INPUT_MESSAGES: &str = attribute::GEN_AI_INPUT_MESSAGES;
/// The agent span's output messages, same shape as [`INPUT_MESSAGES`].
#[allow(deprecated)]
pub const OUTPUT_MESSAGES: &str = attribute::GEN_AI_OUTPUT_MESSAGES;

/// A numeric score at or above this passes when no label decides.
pub const PASS_THRESHOLD: f64 = 0.5;

const PASS_LABELS: [&str; 6] = ["pass", "passed", "true", "yes", "correct", "safe"];
const FAIL_LABELS: [&str; 6] = ["fail", "failed", "false", "no", "incorrect", "unsafe"];

/// A pass/fail judgement of one evaluator result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Pass,
    Fail,
}

/// One evaluator result as the pass rule sees it.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct EvalResult<'a> {
    /// `error.type`: set when the evaluator failed to produce a judgement.
    pub error: Option<&'a str>,
    pub label: Option<&'a str>,
    pub score: Option<f64>,
}

/// The verdict a label carries on its own (case-insensitive), or `None`
/// when it is not one of the recognised pass/fail words.
pub fn verdict_of_label(label: &str) -> Option<Verdict> {
    let label = label.to_ascii_lowercase();
    if PASS_LABELS.contains(&label.as_str()) {
        Some(Verdict::Pass)
    } else if FAIL_LABELS.contains(&label.as_str()) {
        Some(Verdict::Fail)
    } else {
        None
    }
}

/// The pass rule (design D3): an evaluator error (non-empty `error.type`)
/// has no verdict; otherwise a recognised label decides; otherwise a score
/// of at least [`PASS_THRESHOLD`] passes and a lower one fails; a result
/// with neither is scored without a verdict.
pub fn verdict_of_result(result: EvalResult<'_>) -> Option<Verdict> {
    if result.error.is_some_and(|e| !e.is_empty()) {
        return None;
    }
    result.label.and_then(verdict_of_label).or_else(|| {
        result.score.map(|score| {
            if score >= PASS_THRESHOLD {
                Verdict::Pass
            } else {
                Verdict::Fail
            }
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn result<'a>(
        error: Option<&'a str>,
        label: Option<&'a str>,
        score: Option<f64>,
    ) -> EvalResult<'a> {
        EvalResult {
            error,
            label,
            score,
        }
    }

    #[test]
    fn recognised_labels_decide_case_insensitively() {
        for label in ["pass", "PASSED", "True", "yes", "correct", "Safe"] {
            assert_eq!(verdict_of_label(label), Some(Verdict::Pass), "{label}");
        }
        for label in ["fail", "Failed", "FALSE", "no", "incorrect", "UNSAFE"] {
            assert_eq!(verdict_of_label(label), Some(Verdict::Fail), "{label}");
        }
        for label in ["", "maybe", "partial", "passing"] {
            assert_eq!(verdict_of_label(label), None, "{label}");
        }
    }

    #[test]
    fn an_evaluator_error_has_no_verdict_whatever_its_label_or_score() {
        assert_eq!(
            verdict_of_result(result(Some("timeout"), Some("fail"), Some(0.0))),
            None
        );
        assert_eq!(
            verdict_of_result(result(Some("timeout"), None, Some(0.9))),
            None
        );
    }

    #[test]
    fn an_empty_error_type_is_not_an_error() {
        assert_eq!(
            verdict_of_result(result(Some(""), Some("fail"), None)),
            Some(Verdict::Fail)
        );
    }

    #[test]
    fn a_recognised_label_beats_the_score() {
        assert_eq!(
            verdict_of_result(result(None, Some("pass"), Some(0.1))),
            Some(Verdict::Pass)
        );
        assert_eq!(
            verdict_of_result(result(None, Some("Incorrect"), Some(0.9))),
            Some(Verdict::Fail)
        );
    }

    #[test]
    fn without_a_recognised_label_the_score_decides_at_the_threshold() {
        assert_eq!(
            verdict_of_result(result(None, None, Some(PASS_THRESHOLD))),
            Some(Verdict::Pass)
        );
        assert_eq!(
            verdict_of_result(result(None, Some("partial"), Some(0.49))),
            Some(Verdict::Fail)
        );
    }

    #[test]
    fn neither_label_nor_score_has_no_verdict() {
        assert_eq!(verdict_of_result(result(None, Some("partial"), None)), None);
        assert_eq!(verdict_of_result(EvalResult::default()), None);
    }

    /// One row of the shared pass-rule fixture (`testdata/eval_verdicts.json`),
    /// also loaded by the UI's `evalModel.test.ts` so the Rust and TypeScript
    /// pass rules are checked against the same cases and cannot drift apart.
    #[derive(serde::Deserialize)]
    struct FixtureCase {
        label: Option<String>,
        score: Option<f64>,
        error_type: Option<String>,
        /// `"pass"` / `"fail"` / `"error"` / `"none"`; the last two both mean
        /// no verdict (an evaluator error vs. neither a label nor a score
        /// deciding), kept distinct here only for the fixture reader's sake.
        verdict: String,
    }

    #[test]
    fn matches_the_shared_pass_rule_fixture() {
        let raw = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../testdata/eval_verdicts.json"
        ));
        let cases: Vec<FixtureCase> = serde_json::from_str(raw).expect("fixture parses");
        assert!(!cases.is_empty());
        for case in &cases {
            let expected = match case.verdict.as_str() {
                "pass" => Some(Verdict::Pass),
                "fail" => Some(Verdict::Fail),
                "error" | "none" => None,
                other => panic!("unknown fixture verdict `{other}`"),
            };
            let actual = verdict_of_result(result(
                case.error_type.as_deref(),
                case.label.as_deref(),
                case.score,
            ));
            assert_eq!(
                actual, expected,
                "label={:?} score={:?} error_type={:?}",
                case.label, case.score, case.error_type
            );
        }
    }
}
