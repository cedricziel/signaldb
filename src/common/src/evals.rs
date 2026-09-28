//! # Evaluation results: attribute names and the pass rule
//!
//! An evaluator result is a log record with `event_name =
//! gen_ai.evaluation.result` (OTel GenAI semantic conventions), linked to
//! the span it scores through its trace context (change:
//! agent-offline-evals). The attribute names, the pass rule (design D3), the
//! run/comparison model (design D4, D5) and the Query IR reads built on them
//! live in the [`eval_model`] crate, a leaf crate that exists so
//! `signaldb-cli` can depend on it instead of on (and building) `common`.
//! This module re-exports it for in-process consumers (the
//! router, the MCP server), so `common::evals::*` paths keep compiling.
//! [`upload`] and [`span_events`] are server-only: they convert OTLP proto
//! types this crate depends on but `eval_model` does not, so they stay here.

pub use eval_model::compare;
pub use eval_model::runs;
pub use eval_model::{
    AGENT_NAME, AGENT_VERSION, CASE_ID, ERROR_TYPE, EVALUATION_EXPLANATION, EVALUATION_NAME,
    EVALUATION_RESULT_EVENT, EVALUATION_SCORE_LABEL, EVALUATION_SCORE_VALUE, EVALUATOR, EvalResult,
    INPUT_MESSAGES, OPERATION_EXECUTE_TOOL, OPERATION_INVOKE_AGENT, OPERATION_NAME,
    OUTPUT_MESSAGES, PASS_THRESHOLD, RUN_ID, SET, TOOL_NAME, TRIAL, Verdict, verdict_of_label,
    verdict_of_result,
};

pub mod span_events;
pub mod upload;
