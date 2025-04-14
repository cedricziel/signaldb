# SignalDB Messaging System

A flexible messaging system that supports multiple backends and message types.

## Features

- Multiple backend implementations:
  - In-memory backend for testing and development
  - NATS backend for production use
  - NATS JetStream backend for persistent messaging
- Support for multiple message types:
  - SimpleMessage
  - SpanBatch
  - Trace
- Easy configuration system to switch between backends
- Clean, consistent API across all backends

## Usage

### Creating a Backend

```rust
use messaging::config::BackendConfig;

// Create an in-memory backend
let config = BackendConfig::memory(10);
let backend = config.create_backend().await?;

// Create a NATS backend
let config = BackendConfig::nats("nats://localhost:4222");
let backend = config.create_backend().await?;

// Create a NATS JetStream backend
let config = BackendConfig::jetstream("nats://localhost:4222");
let backend = config.create_backend().await?;
```

### Creating a Dispatcher

```rust
use messaging::Dispatcher;

let dispatcher = Dispatcher::new(backend.as_ref().clone());
```

### Sending Messages

```rust
use messaging::{Message, messages::SimpleMessage};

// Send a simple message
let simple_message = Message::SimpleMessage(SimpleMessage {
    id: "1".to_string(),
    name: "Hello, world!".to_string(),
});
dispatcher.send("simple_topic", simple_message).await?;
```

### Receiving Messages

```rust
// Receive messages
let mut stream = dispatcher.stream("simple_topic").await;

while let Some(message) = stream.next().await {
    match message {
        Message::SimpleMessage(simple_message) => {
            println!("Received simple message: {:?}", simple_message);
        }
        Message::SpanBatch(span_batch) => {
            println!("Received span batch: {:?}", span_batch);
        }
        Message::Trace(trace) => {
            println!("Received trace: {:?}", trace);
        }
    }
}
```

### Acknowledging Messages

```rust
// Acknowledge a message
dispatcher.ack(message).await?;
```

## Example

See [example.rs](src/example.rs) for a complete example of how to use the messaging system.

## Testing

The messaging system includes comprehensive tests for all backends. To run the tests:

```bash
cargo test -p messaging
```
