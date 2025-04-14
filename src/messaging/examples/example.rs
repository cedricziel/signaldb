use std::collections::HashMap;

use messaging::{
    config::BackendConfig,
    messages::{
        span::{Span, SpanBatch, SpanKind, SpanStatus},
        trace::Trace,
        SimpleMessage,
    },
    Dispatcher, Message,
};

/// This example demonstrates how to use the messaging system.
///
/// It shows how to:
/// - Create a backend configuration
/// - Create a backend from the configuration
/// - Create a dispatcher
/// - Send and receive messages of different types
#[tokio::main]
async fn main() -> Result<(), String> {
    // Create a backend configuration
    let config = BackendConfig::memory(10);

    // Create a backend from the configuration
    let backend = config.create_backend().await?;

    // Create a dispatcher
    let dispatcher = Dispatcher::new(backend.as_ref().clone());

    // Send a simple message
    let simple_message = Message::SimpleMessage(SimpleMessage {
        id: "1".to_string(),
        name: "Hello, world!".to_string(),
    });
    dispatcher
        .send("simple_topic", simple_message.clone())
        .await?;

    // Send a span batch
    let span = Span {
        trace_id: "trace_id".to_string(),
        span_id: "span_id".to_string(),
        parent_span_id: "parent_span_id".to_string(),
        status: SpanStatus::Ok,
        is_root: true,
        name: "name".to_string(),
        service_name: "service_name".to_string(),
        span_kind: SpanKind::Client,
        start_time_unix_nano: 0,
        duration_nano: 1_000_000_000, // 1 second
        attributes: HashMap::new(),
        resource: HashMap::new(),
        children: vec![],
    };
    let span_batch = Message::SpanBatch(SpanBatch::new_with_spans(vec![span.clone()]));
    dispatcher.send("span_topic", span_batch.clone()).await?;

    // Send a trace
    let trace = Message::Trace(Trace::new_with_spans("trace_id", vec![span]));
    dispatcher.send("trace_topic", trace.clone()).await?;

    // Receive messages
    let mut simple_stream = dispatcher.stream("simple_topic").await;
    let mut span_stream = dispatcher.stream("span_topic").await;
    let mut trace_stream = dispatcher.stream("trace_topic").await;

    // Use tokio::select! to wait for messages from all streams
    tokio::select! {
        Some(message) = simple_stream.next() => {
            println!("Received simple message: {:?}", message);
        }
        Some(message) = span_stream.next() => {
            println!("Received span batch: {:?}", message);
        }
        Some(message) = trace_stream.next() => {
            println!("Received trace: {:?}", message);
        }
    }

    Ok(())
}
