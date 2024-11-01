use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use tonic::Request;

#[tracing::instrument()]
pub fn handle_grpc_otlp_traces(request: Request<ExportTraceServiceRequest>) {
    println!("Got a request: {:?}", request);

    request
        .get_ref()
        .resource_spans
        .iter()
        .for_each(|resource_span| {
            println!("Resource: {:?}", resource_span.resource);
            resource_span
                .scope_spans
                .iter()
                .for_each(|instrumentation_library_span| {
                    instrumentation_library_span.spans.iter().for_each(|span| {
                        println!("Span: {:?}", span);
                    });
                });
        });
}
