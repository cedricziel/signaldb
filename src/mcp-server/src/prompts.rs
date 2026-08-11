//! Reusable prompt templates for the MCP `prompts` capability.
//!
//! Prompts are static, compiled-in templates a client can surface directly to
//! the user (e.g. as a slash command or menu item) — distinct from the tools
//! the model calls on its own. Each renders into a single user-role message
//! that seeds an investigation using this server's own tools. Rendering is
//! pure argument substitution, no router call, so prompts stay usable even
//! before this session's router credential has been validated.

use rmcp::ErrorData;
use rmcp::model::{GetPromptResult, JsonObject, Prompt, PromptArgument, PromptMessage, Role};

/// One prompt's static shape, plus how to render it from caller-supplied
/// arguments.
struct PromptSpec {
    name: &'static str,
    title: &'static str,
    description: &'static str,
    /// `(name, description, required)` per argument, in declaration order.
    arguments: &'static [(&'static str, &'static str, bool)],
    render: fn(&JsonObject) -> Result<String, ErrorData>,
}

/// Every prompt this server serves, for `prompts/list` and `prompts/get`.
const SPECS: &[PromptSpec] = &[
    PromptSpec {
        name: "investigate_trace",
        title: "Investigate a trace",
        description: "Fetch a trace and analyze its critical path, errors, and self time.",
        arguments: &[("trace_id", "The trace ID to investigate.", true)],
        render: |args| {
            let trace_id = require_str(args, "trace_id")?;
            Ok(format!(
                "Investigate trace `{trace_id}` in SignalDB. Call `get_trace` with \
                 `trace_id: \"{trace_id}\"` to fetch it, then summarize: overall duration, the \
                 critical path (the chain of spans on the longest path), which spans have \
                 errors, and any spans with unusually high self time relative to their \
                 children. Call out anything that looks like a bug or a performance \
                 regression."
            ))
        },
    },
    PromptSpec {
        name: "find_recent_errors",
        title: "Find recent errors for a service",
        description: "Search recent traces and logs for a service's errors and summarize failure patterns.",
        arguments: &[
            ("service", "The `service.name` to search for.", true),
            (
                "minutes",
                "How many minutes back to search (default 15).",
                false,
            ),
        ],
        render: |args| {
            let service = require_str(args, "service")?;
            let minutes = optional_str(args, "minutes").unwrap_or_else(|| "15".to_string());
            Ok(format!(
                "Find recent errors for service `{service}` in the last {minutes} minutes. \
                 Call `search_traces` with a TraceQL query like \
                 `{{ .service.name = \"{service}\" && status = error }}` and a matching time \
                 range, and `search_logs` with a LogQL query like \
                 `{{service_name=\"{service}\"}} |= \"error\"`. Summarize the common failure \
                 patterns you find — which operations fail, any shared error messages, and \
                 whether failures cluster around a specific time or downstream dependency."
            ))
        },
    },
    PromptSpec {
        name: "build_promql_query",
        title: "Build a PromQL query",
        description: "Discover a metric's exact name and build a PromQL query for it.",
        arguments: &[
            (
                "metric",
                "The metric name, or a fragment of it if unsure.",
                true,
            ),
            (
                "intent",
                "What you want to compute (e.g. \"rate\", \"p99 latency\", \"error ratio\").",
                false,
            ),
        ],
        render: |args| {
            let metric = require_str(args, "metric")?;
            let intent_line = match optional_str(args, "intent") {
                Some(intent) => format!(" The goal is to compute: {intent}."),
                None => String::new(),
            };
            Ok(format!(
                "Build a PromQL query for the metric `{metric}`.{intent_line} If you are not \
                 sure of the exact metric name, call `discover_metrics` first and pick the \
                 closest match. Then call `query_metrics` with the PromQL expression and \
                 explain what the result means."
            ))
        },
    },
];

fn find(name: &str) -> Option<&'static PromptSpec> {
    SPECS.iter().find(|spec| spec.name == name)
}

/// Read a required string argument, or an `invalid_params` error naming it.
fn require_str<'a>(args: &'a JsonObject, name: &str) -> Result<&'a str, ErrorData> {
    args.get(name)
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            ErrorData::invalid_params(format!("missing required argument `{name}`"), None)
        })
}

/// Read an optional string argument.
fn optional_str(args: &JsonObject, name: &str) -> Option<String> {
    args.get(name)
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
}

/// The prompts this server exposes, for `prompts/list`.
pub fn list() -> Vec<Prompt> {
    SPECS
        .iter()
        .map(|spec| {
            let arguments = spec
                .arguments
                .iter()
                .map(|(name, description, required)| {
                    PromptArgument::new(*name)
                        .with_description(*description)
                        .with_required(*required)
                })
                .collect();
            Prompt::new(spec.name, Some(spec.description), Some(arguments)).with_title(spec.title)
        })
        .collect()
}

/// Render `name` with `arguments` into a `prompts/get` result.
///
/// Errors when `name` is not a known prompt, or when a required argument is
/// missing — both `invalid_params`, since both are caller mistakes.
pub fn get(name: &str, arguments: Option<JsonObject>) -> Result<GetPromptResult, ErrorData> {
    let spec = find(name)
        .ok_or_else(|| ErrorData::invalid_params(format!("unknown prompt `{name}`"), None))?;
    let args = arguments.unwrap_or_default();
    let text = (spec.render)(&args)?;
    Ok(
        GetPromptResult::new(vec![PromptMessage::new_text(Role::User, text)])
            .with_description(spec.description),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use rmcp::model::ContentBlock;
    use serde_json::json;

    fn args(pairs: &[(&str, &str)]) -> JsonObject {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), json!(v)))
            .collect()
    }

    fn text_of(result: &GetPromptResult) -> &str {
        let ContentBlock::Text(text) = &result.messages[0].content else {
            panic!("prompt message must be text");
        };
        &text.text
    }

    #[test]
    fn lists_three_prompts_with_required_arguments_marked() {
        let prompts = list();
        let names: Vec<_> = prompts.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(
            names,
            [
                "investigate_trace",
                "find_recent_errors",
                "build_promql_query"
            ]
        );

        let investigate = prompts
            .iter()
            .find(|p| p.name == "investigate_trace")
            .expect("investigate_trace is listed");
        let trace_id_arg = investigate
            .arguments
            .as_ref()
            .expect("has arguments")
            .iter()
            .find(|a| a.name == "trace_id")
            .expect("trace_id argument is declared");
        assert_eq!(trace_id_arg.required, Some(true));
    }

    #[test]
    fn investigate_trace_renders_the_trace_id_into_the_message() {
        let result = get("investigate_trace", Some(args(&[("trace_id", "abc123")])))
            .expect("renders with a valid argument");
        assert!(text_of(&result).contains("abc123"));
    }

    #[test]
    fn investigate_trace_requires_trace_id() {
        let error = get("investigate_trace", None).expect_err("trace_id is required");
        assert!(error.message.contains("trace_id"));
    }

    #[test]
    fn find_recent_errors_defaults_minutes_when_omitted() {
        let result = get("find_recent_errors", Some(args(&[("service", "checkout")])))
            .expect("renders with only the required argument");
        let text = text_of(&result);
        assert!(text.contains("checkout"));
        assert!(text.contains("15"));
    }

    #[test]
    fn find_recent_errors_honors_an_explicit_minutes_argument() {
        let result = get(
            "find_recent_errors",
            Some(args(&[("service", "checkout"), ("minutes", "60")])),
        )
        .expect("renders");
        assert!(text_of(&result).contains("60"));
    }

    #[test]
    fn build_promql_query_includes_intent_when_given() {
        let result = get(
            "build_promql_query",
            Some(args(&[
                ("metric", "http_requests_total"),
                ("intent", "p99 latency"),
            ])),
        )
        .expect("renders");
        let text = text_of(&result);
        assert!(text.contains("http_requests_total"));
        assert!(text.contains("p99 latency"));
    }

    #[test]
    fn unknown_prompt_name_is_rejected() {
        let error = get("no-such-prompt", None).expect_err("unknown prompt must error");
        assert!(error.message.contains("no-such-prompt"));
    }
}
