//! # Evaluation results sent as span events
//!
//! Harnesses that follow the GenAI semconv literally record a result with
//! `span.add_event("gen_ai.evaluation.result", ...)` on the span it scores.
//! The Evaluate pages read results from the `logs` table (design D1), so the
//! acceptor's trace ingest turns each such span event into one log record
//! with [`evaluation_logs_from_spans`]. The span is stored unchanged.

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::trace::v1::Span;
use opentelemetry_proto::tonic::trace::v1::span::Event;

use super::{AGENT_NAME, AGENT_VERSION, EVALUATION_RESULT_EVENT};

/// Only the low byte of a span's `flags` is the W3C trace flags; the log
/// record's `flags` carries just those.
const TRACE_FLAGS_MASK: u32 = 0xff;

/// The log records for every `gen_ai.evaluation.result` span event in
/// `request`, or `None` when there are none.
///
/// Each record takes the event's time and attributes and the span's trace
/// context, under the span's resource and scope (so `service.name` /
/// `service.version` stay the agent-identity fallback). `gen_ai.agent.name`
/// and `gen_ai.agent.version` are copied from the span when the event lacks
/// them (design D2); the event's own value wins. The output depends only on
/// `request`, so a resent trace batch yields byte-identical logs and the
/// log ingest's fingerprint dedup catches it.
///
/// Takes `request` by value and moves its resources/scopes/event attributes
/// into the output instead of cloning them, since the acceptor's trace
/// handler calls this last, once the request is no longer needed.
pub fn evaluation_logs_from_spans(
    request: ExportTraceServiceRequest,
) -> Option<ExportLogsServiceRequest> {
    let resource_logs: Vec<ResourceLogs> = request
        .resource_spans
        .into_iter()
        .filter_map(|rs| {
            let scope_logs: Vec<ScopeLogs> = rs
                .scope_spans
                .into_iter()
                .filter_map(|ss| {
                    let log_records: Vec<LogRecord> =
                        ss.spans.into_iter().flat_map(span_records).collect();
                    (!log_records.is_empty()).then_some(ScopeLogs {
                        scope: ss.scope,
                        log_records,
                        schema_url: ss.schema_url,
                    })
                })
                .collect();
            (!scope_logs.is_empty()).then_some(ResourceLogs {
                resource: rs.resource,
                scope_logs,
                schema_url: rs.schema_url,
            })
        })
        .collect();
    (!resource_logs.is_empty()).then_some(ExportLogsServiceRequest { resource_logs })
}

/// The evaluation-result log records for one span's events, consuming the
/// span's events; `trace_id`/`span_id` are still copied per record since one
/// span can carry several such events.
fn span_records(mut span: Span) -> impl Iterator<Item = LogRecord> {
    let events = std::mem::take(&mut span.events);
    events
        .into_iter()
        .filter(|event| event.name == EVALUATION_RESULT_EVENT)
        .map(move |event| log_record(&span, event))
}

fn log_record(span: &Span, event: Event) -> LogRecord {
    let mut attributes = event.attributes;
    for key in [AGENT_NAME, AGENT_VERSION] {
        if attributes.iter().any(|kv| kv.key == key) {
            continue;
        }
        if let Some(kv) = span.attributes.iter().find(|kv| kv.key == key) {
            attributes.push(kv.clone());
        }
    }
    LogRecord {
        time_unix_nano: event.time_unix_nano,
        observed_time_unix_nano: event.time_unix_nano,
        attributes,
        dropped_attributes_count: event.dropped_attributes_count,
        flags: span.flags & TRACE_FLAGS_MASK,
        trace_id: span.trace_id.clone(),
        span_id: span.span_id.clone(),
        event_name: EVALUATION_RESULT_EVENT.to_string(),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use opentelemetry_proto::tonic::common::v1::{
        AnyValue, InstrumentationScope, KeyValue, any_value,
    };
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans};

    use super::super::{EVALUATION_NAME, EVALUATION_SCORE_VALUE};
    use super::*;
    use crate::testing::string_attr as string_kv;

    fn attr<'a>(attributes: &'a [KeyValue], key: &str) -> Option<&'a any_value::Value> {
        attributes
            .iter()
            .find(|kv| kv.key == key)
            .and_then(|kv| kv.value.as_ref())
            .and_then(|v| v.value.as_ref())
    }

    fn string_attr<'a>(attributes: &'a [KeyValue], key: &str) -> Option<&'a str> {
        match attr(attributes, key) {
            Some(any_value::Value::StringValue(s)) => Some(s),
            _ => None,
        }
    }

    fn eval_event(time: u64, name: &str, score: f64) -> Event {
        Event {
            time_unix_nano: time,
            name: EVALUATION_RESULT_EVENT.to_string(),
            attributes: vec![
                string_kv(EVALUATION_NAME, name),
                KeyValue {
                    key: EVALUATION_SCORE_VALUE.to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::DoubleValue(score)),
                    }),
                    ..Default::default()
                },
            ],
            dropped_attributes_count: 0,
        }
    }

    fn span(events: Vec<Event>, attributes: Vec<KeyValue>) -> Span {
        Span {
            trace_id: vec![0xab; 16],
            span_id: vec![0xcd; 8],
            name: "invoke_agent triage".to_string(),
            start_time_unix_nano: 1_000,
            end_time_unix_nano: 9_000,
            flags: 0x0000_0301,
            attributes,
            events,
            ..Default::default()
        }
    }

    fn request(spans: Vec<Span>) -> ExportTraceServiceRequest {
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![string_kv("service.name", "triage-agent")],
                    ..Default::default()
                }),
                scope_spans: vec![ScopeSpans {
                    scope: Some(InstrumentationScope {
                        name: "harness".to_string(),
                        ..Default::default()
                    }),
                    spans,
                    schema_url: "https://opentelemetry.io/schemas/1.37.0".to_string(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    fn records(logs: &ExportLogsServiceRequest) -> Vec<&LogRecord> {
        logs.resource_logs
            .iter()
            .flat_map(|rl| &rl.scope_logs)
            .flat_map(|sl| &sl.log_records)
            .collect()
    }

    #[test]
    fn an_evaluation_event_becomes_a_record_with_the_span_trace_context_and_event_time() {
        let req = request(vec![span(
            vec![eval_event(5_000, "Correctness", 0.9)],
            vec![],
        )]);

        let expected_resource = req.resource_spans[0].resource.clone();
        let expected_scope = req.resource_spans[0].scope_spans[0].scope.clone();
        let expected_schema_url = req.resource_spans[0].scope_spans[0].schema_url.clone();

        let logs = evaluation_logs_from_spans(req).expect("one result");

        let [record] = records(&logs)[..] else {
            panic!("expected exactly one record");
        };
        assert_eq!(record.event_name, EVALUATION_RESULT_EVENT);
        assert_eq!(record.time_unix_nano, 5_000);
        assert_eq!(record.trace_id, vec![0xab; 16]);
        assert_eq!(record.span_id, vec![0xcd; 8]);
        assert_eq!(record.flags, 0x01, "only the W3C trace flags byte");
        assert_eq!(
            string_attr(&record.attributes, EVALUATION_NAME),
            Some("Correctness")
        );
        assert_eq!(
            attr(&record.attributes, EVALUATION_SCORE_VALUE),
            Some(&any_value::Value::DoubleValue(0.9))
        );

        let resource_logs = &logs.resource_logs[0];
        assert_eq!(resource_logs.resource, expected_resource);
        let scope_logs = &resource_logs.scope_logs[0];
        assert_eq!(scope_logs.scope, expected_scope);
        assert_eq!(scope_logs.schema_url, expected_schema_url);
    }

    #[test]
    fn several_events_on_one_span_become_several_records() {
        let req = request(vec![span(
            vec![
                eval_event(5_000, "Correctness", 0.9),
                eval_event(6_000, "Helpfulness", 0.4),
            ],
            vec![],
        )]);

        let logs = evaluation_logs_from_spans(req).expect("results");

        let names: Vec<_> = records(&logs)
            .iter()
            .map(|r| {
                (
                    r.time_unix_nano,
                    string_attr(&r.attributes, EVALUATION_NAME),
                )
            })
            .collect();
        assert_eq!(
            names,
            vec![(5_000, Some("Correctness")), (6_000, Some("Helpfulness"))]
        );
    }

    #[test]
    fn agent_identity_is_copied_from_the_span_unless_the_event_has_its_own() {
        let mut event = eval_event(5_000, "Correctness", 0.9);
        event.attributes.push(string_kv(AGENT_VERSION, "2.0.0"));
        let req = request(vec![span(
            vec![event],
            vec![
                string_kv(AGENT_NAME, "triage"),
                string_kv(AGENT_VERSION, "1.0.0"),
            ],
        )]);

        let logs = evaluation_logs_from_spans(req).expect("one result");

        let record = records(&logs)[0];
        assert_eq!(string_attr(&record.attributes, AGENT_NAME), Some("triage"));
        assert_eq!(
            string_attr(&record.attributes, AGENT_VERSION),
            Some("2.0.0"),
            "the event's own value wins"
        );
        assert_eq!(
            record
                .attributes
                .iter()
                .filter(|kv| kv.key == AGENT_VERSION)
                .count(),
            1
        );
    }

    #[test]
    fn spans_without_evaluation_events_yield_none() {
        let other = Event {
            name: "exception".to_string(),
            ..Default::default()
        };
        assert!(evaluation_logs_from_spans(request(vec![span(vec![], vec![])])).is_none());
        assert!(evaluation_logs_from_spans(request(vec![span(vec![other], vec![])])).is_none());
        assert!(evaluation_logs_from_spans(ExportTraceServiceRequest::default()).is_none());
    }

    #[test]
    fn other_events_and_spans_are_left_out() {
        let other = Event {
            time_unix_nano: 4_000,
            name: "gen_ai.choice".to_string(),
            ..Default::default()
        };
        let mut plain = span(vec![], vec![]);
        plain.span_id = vec![0xee; 8];
        let req = request(vec![
            plain,
            span(vec![other, eval_event(5_000, "Correctness", 0.9)], vec![]),
        ]);

        let logs = evaluation_logs_from_spans(req).expect("one result");

        let all = records(&logs);
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].time_unix_nano, 5_000);
        assert_eq!(all[0].span_id, vec![0xcd; 8]);
    }

    #[test]
    fn resources_without_results_are_dropped() {
        let mut req = request(vec![span(
            vec![eval_event(5_000, "Correctness", 0.9)],
            vec![],
        )]);
        let mut empty = request(vec![span(vec![], vec![])]).resource_spans.remove(0);
        empty.resource = None;
        req.resource_spans.insert(0, empty);

        let logs = evaluation_logs_from_spans(req).expect("one result");

        assert_eq!(logs.resource_logs.len(), 1);
        assert!(logs.resource_logs[0].resource.is_some());
    }
}
