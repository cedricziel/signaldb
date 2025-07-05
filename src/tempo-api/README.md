# SignalDB Tempo API

The tempo-api library provides Grafana Tempo-compatible data structures and protocol definitions for SignalDB. It enables seamless integration with Grafana Tempo clients and ensures API compatibility with the Tempo ecosystem.

## Purpose & Overview

The tempo-api library serves as the compatibility layer between SignalDB and Grafana Tempo:
- Provides Rust data structures for Tempo API responses
- Includes Protocol Buffer definitions for trace data
- Ensures JSON serialization compatibility with Tempo
- Supports both v1 and v2 Tempo API versions
- Enables seamless Grafana integration

## Architecture

The library is organized into several key modules:

### Core API Types (`lib.rs`)
- **SearchQueryParams**: Query parameters for trace search
- **SearchResult**: Search response with traces and metrics
- **Trace**: Complete trace representation
- **Span**: Individual span data structure
- **Attribute**: Span attribute key-value pairs

### v2 API Extensions (`v2.rs`)
- **TagValuesResponse**: Enhanced tag values with metadata
- **TagSearchResponse**: Scoped tag search responses
- **TagSearchScope**: Tag organization by scope

### Protocol Buffers (`proto/`)
- **tempo.proto**: Main Tempo protocol definitions
- **common/v1/**: Common data structures
- **resource/v1/**: Resource attribute definitions
- **trace/v1/**: Trace and span definitions

### Generated Code (`generated/`)
- Auto-generated Rust code from Protocol Buffer definitions
- Includes all Tempo protobuf message types
- Provides serialization and deserialization support

## API Reference

### Core Data Structures

#### SearchResult
Main response structure for trace search operations:
```rust
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct SearchResult {
    pub traces: Vec<Trace>,
    pub metrics: HashMap<String, u16>,
}
```

#### Trace
Represents a complete distributed trace:
```rust
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Trace {
    pub trace_id: String,
    pub root_service_name: String,
    pub root_trace_name: String,
    pub start_time_unix_nano: String,
    pub duration_ms: u64,
    pub span_sets: Vec<SpanSet>,
}
```

#### Span
Individual span within a trace:
```rust
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Span {
    pub span_id: String,
    pub start_time_unix_nano: String,
    pub duration_nanos: String,
    pub attributes: HashMap<String, Attribute>,
}
```

### Query Parameters

#### SearchQueryParams
Parameters for trace search operations:
```rust
#[derive(Serialize, Deserialize, Debug)]
pub struct SearchQueryParams {
    pub q: Option<String>,           // TraceQL query
    pub tags: Option<String>,        // Tag filters
    pub min_duration: Option<i32>,   // Minimum duration filter
    pub max_duration: Option<i32>,   // Maximum duration filter
    pub limit: Option<i32>,          // Result limit
    pub start: Option<i32>,          // Start timestamp
    pub end: Option<i32>,            // End timestamp
    pub spss: Option<i32>,           // Spans per span set
}
```

#### TraceQueryParams
Parameters for single trace queries:
```rust
#[derive(Deserialize, Debug)]
pub struct TraceQueryParams {
    start: Option<String>,
    end: Option<String>,
}
```

## Usage Examples

### Creating Search Results
```rust
use tempo_api::{SearchResult, Trace, SpanSet, Span, Attribute, Value};
use std::collections::HashMap;

// Create a trace
let trace = Trace {
    trace_id: "2f3e0cee77ae5dc9c17ade3689eb2e54".to_string(),
    root_service_name: "user-service".to_string(),
    root_trace_name: "GET /users".to_string(),
    start_time_unix_nano: "1684778327699392724".to_string(),
    duration_ms: 150,
    span_sets: vec![SpanSet {
        spans: vec![Span {
            span_id: "563d623c76514f8e".to_string(),
            start_time_unix_nano: "1684778327735077898".to_string(),
            duration_nanos: "150000000".to_string(),
            attributes: {
                let mut attrs = HashMap::new();
                attrs.insert("http.method".to_string(), Attribute {
                    key: "http.method".to_string(),
                    value: Value::StringValue("GET".to_string()),
                });
                attrs.insert("http.status_code".to_string(), Attribute {
                    key: "http.status_code".to_string(),
                    value: Value::IntValue(200),
                });
                attrs
            },
        }],
        matched: 1,
    }],
};

// Create search result
let search_result = SearchResult {
    traces: vec![trace],
    metrics: {
        let mut metrics = HashMap::new();
        metrics.insert("total_traces".to_string(), 1);
        metrics
    },
};

// Serialize to JSON
let json = serde_json::to_string(&search_result)?;
```

### Parsing Query Parameters
```rust
use tempo_api::SearchQueryParams;

// Parse from query string
let query_params = SearchQueryParams {
    q: Some("{ duration > 100ms }".to_string()),
    tags: Some("service.name=user-service".to_string()),
    min_duration: Some(100),
    max_duration: Some(5000),
    limit: Some(50),
    start: Some(1684778327),
    end: Some(1684778427),
    spss: Some(10),
};

// Use in search logic
if let Some(ref traceql) = query_params.q {
    // Process TraceQL query
    println!("Processing TraceQL: {}", traceql);
}
```

### Working with Attributes
```rust
use tempo_api::{Attribute, Value};
use std::collections::HashMap;

// Create span attributes
let mut attributes = HashMap::new();

// String attribute
attributes.insert("service.name".to_string(), Attribute {
    key: "service.name".to_string(),
    value: Value::StringValue("user-service".to_string()),
});

// Integer attribute
attributes.insert("http.status_code".to_string(), Attribute {
    key: "http.status_code".to_string(),
    value: Value::IntValue(200),
});

// Boolean attribute
attributes.insert("error".to_string(), Attribute {
    key: "error".to_string(),
    value: Value::BoolValue(false),
});

// Double attribute
attributes.insert("response_time_ms".to_string(), Attribute {
    key: "response_time_ms".to_string(),
    value: Value::DoubleValue(125.5),
});
```

### v2 API Usage
```rust
use tempo_api::v2::{TagValuesResponse, TagWithValue, TagSearchResponse, TagSearchScope};

// Create v2 tag values response
let tag_values = TagValuesResponse {
    tag_values: vec![
        TagWithValue {
            tag: "service.name".to_string(),
            value: "user-service".to_string(),
        },
        TagWithValue {
            tag: "service.name".to_string(),
            value: "order-service".to_string(),
        },
    ],
};

// Create v2 tag search response
let tag_search = TagSearchResponse {
    scopes: vec![
        TagSearchScope {
            scope: "resource".to_string(),
            tags: vec!["service.name".to_string(), "service.version".to_string()],
        },
        TagSearchScope {
            scope: "span".to_string(),
            tags: vec!["http.method".to_string(), "http.status_code".to_string()],
        },
    ],
};
```

## Configuration

### Serde Configuration
The library uses serde for JSON serialization with custom field naming:
- `trace_id` serializes as `traceID`
- `span_id` serializes as `spanID`
- `start_time_unix_nano` serializes as `startTimeUnixNano`
- `duration_ms` serializes as `durationMs`

### Protocol Buffer Generation
Protocol buffers are generated using the `build.rs` script:
```rust
// build.rs automatically generates Rust code from .proto files
// Generated files are included in the source tree for convenience
```

## Dependencies

### Core Dependencies
- **serde**: JSON serialization and deserialization
- **serde_json**: JSON processing support

### Build Dependencies
- **prost-build**: Protocol buffer code generation
- **tonic-build**: gRPC service generation

## Testing

### Unit Tests
```bash
# Run all tempo-api tests
cargo test -p tempo-api

# Run with output
cargo test -p tempo-api -- --nocapture
```

### Serialization Tests
```rust
#[test]
fn test_search_result_serialization() {
    let search_result = SearchResult {
        traces: vec![],
        metrics: HashMap::new(),
    };
    
    let json = serde_json::to_string(&search_result).unwrap();
    let deserialized: SearchResult = serde_json::from_str(&json).unwrap();
    
    assert_eq!(search_result, deserialized);
}
```

### Compatibility Tests
```bash
# Test against real Tempo API responses
cargo test -p tempo-api -- --test compatibility
```

## Integration

### With Router
The router uses tempo-api types for HTTP responses:
```rust
use tempo_api::{SearchResult, Trace};

async fn search_traces() -> Result<axum::Json<SearchResult>, StatusCode> {
    let result = SearchResult {
        traces: query_traces().await?,
        metrics: calculate_metrics().await?,
    };
    Ok(axum::Json(result))
}
```

### With Grafana
SignalDB is compatible with Grafana Tempo datasource:
1. Add SignalDB as a Tempo datasource
2. Use SignalDB router URL as the endpoint
3. All standard Tempo features work seamlessly

### Data Conversion
Convert from internal formats to Tempo API:
```rust
use tempo_api::{Trace, Span, Attribute, Value};

fn convert_internal_trace_to_tempo(internal_trace: &InternalTrace) -> Trace {
    Trace {
        trace_id: internal_trace.id.clone(),
        root_service_name: internal_trace.service.clone(),
        root_trace_name: internal_trace.operation.clone(),
        start_time_unix_nano: internal_trace.start_time.to_string(),
        duration_ms: internal_trace.duration.as_millis() as u64,
        span_sets: convert_spans(&internal_trace.spans),
    }
}
```

## Future Enhancements

### API Compatibility
- Support for additional Tempo API versions
- Extended TraceQL support
- Advanced query parameter validation
- Response compression support

### Protocol Extensions
- Additional attribute value types
- Custom metadata support
- Extended trace relationships
- Performance metrics integration

### Grafana Integration
- Dashboard template support
- Alert rule templates
- Custom panel plugins
- Advanced visualization options

## Error Handling

### Serialization Errors
```rust
// Handle JSON serialization failures
match serde_json::to_string(&search_result) {
    Ok(json) => Ok(json),
    Err(e) => {
        log::error!("Failed to serialize search result: {}", e);
        Err(InternalError)
    }
}
```

### Validation
```rust
// Validate trace data before conversion
fn validate_trace(trace: &Trace) -> Result<(), ValidationError> {
    if trace.trace_id.is_empty() {
        return Err(ValidationError::EmptyTraceId);
    }
    if trace.span_sets.is_empty() {
        return Err(ValidationError::NoSpans);
    }
    Ok(())
}
```

## Performance Considerations

### Memory Usage
- Large traces are streamed to avoid memory pressure
- Attribute maps use efficient HashMap storage
- String interning for common attribute keys

### Serialization Performance
- Zero-copy deserialization where possible
- Efficient JSON generation with serde
- Minimal allocation during conversion

### Protocol Buffer Efficiency
- Compact binary encoding for internal communication
- Efficient parsing with prost
- Memory-mapped file support for large traces