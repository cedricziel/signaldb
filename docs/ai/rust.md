# Rust Best Practices

This document defines the Rust coding standards for the SignalDB project.

## Error Handling

### Library vs Application Errors

- Use `thiserror` for errors in library-style code (reusable modules, public APIs)
- Use `anyhow` for application-level errors (main functions, CLI, service entry points)

### Avoid Panics

- NEVER use `.unwrap()` or `.expect()` in production code paths
- Use `?` operator with proper error context via `anyhow::Context`
- Exception: Tests and intentional panics with `panic!` macro are acceptable

### Error Context Pattern

```rust
// Good - adds context for debugging
use anyhow::Context;

file.read_to_string(&mut buf)
    .context("Failed to read configuration file")?;

config.parse()
    .with_context(|| format!("Failed to parse config at {path}"))?;

// Bad - loses context, crashes in production
file.read_to_string(&mut buf).unwrap();
config.parse().expect("config should be valid");
```

### Result Propagation

```rust
// Preferred - propagate with context
fn load_config(path: &Path) -> anyhow::Result<Config> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read {}", path.display()))?;
    toml::from_str(&content)
        .context("Failed to parse TOML configuration")
}

// Avoid - swallows errors or panics
fn load_config(path: &Path) -> Config {
    let content = std::fs::read_to_string(path).unwrap();
    toml::from_str(&content).unwrap()
}
```

## Traits and Abstractions

### When to Extract a Trait

Extract a trait when you have a **concrete need**, not speculatively:

**Good reasons to create a trait:**

1. **Multiple implementations exist today** - You have 2+ types sharing behavior
2. **Testing requires substitution** - You need to swap a real implementation for a mock/fake
3. **External extensibility** - Users of your library need to provide implementations
4. **Decoupling compile-time dependencies** - Breaking circular dependencies between crates

**Bad reasons (avoid these):**

- "We might need this someday" - YAGNI applies strongly to traits
- "Java does it this way" - Rust idioms differ; don't create `IFoo` for every `Foo`
- Single implementation with no testing need - Just use the concrete type

### Trait Design Guidelines

```rust
// Good - trait exists because we have multiple backends
trait ObjectStore: Send + Sync {
    async fn get(&self, path: &Path) -> Result<Bytes>;
    async fn put(&self, path: &Path, data: Bytes) -> Result<()>;
}

struct S3Store { /* ... */ }
struct LocalStore { /* ... */ }
struct InMemoryStore { /* ... */ }  // For testing

// Bad - trait with single implementation, no testing benefit
trait ConfigLoader {
    fn load(&self) -> Result<Config>;
}

struct TomlConfigLoader;  // Only implementation, ever

// Better - just use a function or the struct directly
fn load_config(path: &Path) -> Result<Config> { /* ... */ }
```

### Testing-Driven Trait Extraction

The most common valid reason for traits in application code is testability:

```rust
// Before: Hard to test because it hits real database
impl UserService {
    pub async fn get_user(&self, id: UserId) -> Result<User> {
        sqlx::query_as("SELECT * FROM users WHERE id = $1")
            .bind(id)
            .fetch_one(&self.pool)
            .await
    }
}

// After: Trait allows test doubles
#[async_trait]
trait UserRepository: Send + Sync {
    async fn find_by_id(&self, id: UserId) -> Result<Option<User>>;
}

struct PostgresUserRepository { pool: PgPool }
struct InMemoryUserRepository { users: DashMap<UserId, User> }  // For tests

impl UserService {
    pub fn new(repo: Arc<dyn UserRepository>) -> Self { /* ... */ }
}
```

### Prefer Generics Over Trait Objects

When the concrete type is known at compile time, use generics:

```rust
// Preferred - monomorphized, no vtable overhead
fn process<S: Storage>(storage: &S, data: &[u8]) -> Result<()> {
    storage.write(data)
}

// Use trait objects when you need runtime polymorphism
fn process_any(storage: &dyn Storage, data: &[u8]) -> Result<()> {
    storage.write(data)
}

// Trait objects are appropriate for:
// - Heterogeneous collections: Vec<Box<dyn Handler>>
// - Plugin systems where types aren't known at compile time
// - Reducing binary size (many generic instantiations)
```

### Trait Bounds

Keep bounds minimal and add them where needed:

```rust
// Good - bounds only where required
trait Storage {
    fn write(&self, data: &[u8]) -> Result<()>;
}

// Add Send + Sync only if actually needed for async/threading
trait AsyncStorage: Send + Sync {
    async fn write(&self, data: &[u8]) -> Result<()>;
}

// Avoid over-constraining
// Bad - why does Storage need Clone + Debug?
trait Storage: Clone + Debug + Send + Sync { /* ... */ }
```

## Logging & Observability

### Use tracing over log

```rust
// Preferred - structured fields for machine parsing
tracing::info!(tenant_id = %ctx.tenant_id, dataset = %dataset, "Processing request");
tracing::error!(error = ?err, service_id = %id, "Service registration failed");

// Avoid - string interpolation breaks log aggregation
log::info!("Processing request for tenant {}", ctx.tenant_id);
```

### Span Usage for Request Tracing

```rust
// Create spans for request context
let span = tracing::info_span!("process_batch", tenant_id = %tenant, batch_size = count);
async move {
    // work happens here
}.instrument(span).await
```

### Log Level Guidelines

- `trace`: Detailed debugging (data dumps, internal state)
- `debug`: Developer information (function entry/exit, intermediate values)
- `info`: Service lifecycle (startup, shutdown, discoveries, configuration)
- `warn`: Recoverable issues (retries, fallbacks, deprecated usage)
- `error`: Unrecoverable failures requiring attention

### Emoji Policy

- CLI output: OK to use emoji for human readability (progress indicators, status)
- Logs: NO emoji (breaks log parsing, aggregation, and alerting systems)

## Testing Patterns

### Test Organization

- Unit tests: Inline `#[cfg(test)] mod tests {}` for private function testing
- Integration tests: `tests/` directory within each crate for public API testing
- Workspace integration: `tests-integration/` crate for cross-service testing

### Async Test Pattern

```rust
#[tokio::test]
async fn test_service_discovery() -> anyhow::Result<()> {
    // Arrange
    let config = TestConfig::default();
    let service = Service::new(config).await?;

    // Act
    let result = service.discover_peers().await?;

    // Assert
    assert!(!result.is_empty());
    Ok(())
}
```

### Test Fixtures and Mocking

```rust
// Feature-gate test utilities
#[cfg(any(test, feature = "testing"))]
pub mod test_utils {
    pub fn create_test_context() -> RequestContext { ... }
}

// Use in-memory implementations for isolation
let store = object_store::memory::InMemory::new();
let transport = InMemoryTransport::new();
```

### Test Naming

```rust
// Descriptive names that explain the scenario
#[test]
fn parse_duration_returns_error_for_negative_values() { ... }

#[test]
fn service_discovery_finds_all_registered_services() { ... }

// Avoid vague names
#[test]
fn test_parse() { ... }
```

## Documentation

### Module-Level Documentation Required

Every module should have `//!` documentation explaining its purpose:

```rust
//! # WAL (Write-Ahead Log)
//!
//! Provides durable storage for incoming signals before persistence.
//! Entries are written synchronously and processed asynchronously by the writer.
//!
//! ## Architecture
//!
//! - `WalManager`: Coordinates WAL operations across tenants/datasets
//! - `WalWriter`: Handles individual file writes
//! - `WalReader`: Supports replay and recovery
```

### Public API Documentation

All `pub` items should have `///` documentation with examples for complex types:

```rust
/// Creates a new Flight client connection to the specified service.
///
/// # Arguments
///
/// * `address` - The service address in `host:port` format
/// * `config` - Connection configuration including timeouts
///
/// # Errors
///
/// Returns an error if the connection cannot be established or
/// the handshake fails.
///
/// # Example
///
/// ```rust
/// let client = FlightClient::connect("localhost:50051", config).await?;
/// ```
pub async fn connect(address: &str, config: &Config) -> Result<Self> { ... }
```

## Async Code Guidelines

### Sync Primitives in Async Context

```rust
// Use tokio primitives in async code
use tokio::sync::{RwLock, Mutex, mpsc};

// DashMap for concurrent HashMap access
use dashmap::DashMap;
let cache: DashMap<String, Value> = DashMap::new();

// Avoid std primitives in async - can block the runtime
use std::sync::{RwLock, Mutex};  // blocking!
```

### Background Task Error Handling

```rust
// Always handle errors in spawned tasks
tokio::spawn(async move {
    if let Err(e) = background_process().await {
        tracing::error!(error = ?e, "Background task failed");
    }
});

// Avoid silent failures
tokio::spawn(async move {
    let _ = background_process().await;  // errors lost!
});
```

### Timeouts

```rust
// All network operations need explicit timeouts
use tokio::time::{timeout, Duration};

let result = timeout(Duration::from_secs(30), client.call())
    .await
    .context("Request timed out")?
    .context("Request failed")?;
```

### Cancellation Safety

```rust
// Use select! carefully - ensure operations are cancellation-safe
tokio::select! {
    result = operation() => handle_result(result),
    _ = shutdown_signal.recv() => {
        tracing::info!("Shutting down gracefully");
        return Ok(());
    }
}
```

## Dependencies

### Workspace-First Approach

```toml
# In workspace Cargo.toml - define version once
[workspace.dependencies]
tokio = { version = "1.43", features = ["full"] }
anyhow = "1.0"

# In member Cargo.toml - reference workspace
[dependencies]
tokio.workspace = true
anyhow.workspace = true
```

### FDAP Version Alignment

Arrow, Parquet, and DataFusion must be version-compatible. Always use DataFusion's re-exported types:

```rust
// Use DataFusion's re-exports
use datafusion::arrow::array::StringArray;
use datafusion::parquet::arrow::ArrowWriter;

// Don't import arrow/parquet directly - version mismatch risk
use arrow::array::StringArray;  // may be wrong version!
```

### Check for Unused Dependencies

```bash
cargo machete --with-metadata  # Run before every commit
```

## Dead Code and Warnings

### No `#[allow(dead_code)]` Without Justification

```rust
// If needed, explain why
#[allow(dead_code)]  // Used by integration tests via `testing` feature
fn test_helper() { ... }

// Don't suppress without reason
#[allow(dead_code)]
fn mystery_function() { ... }  // Delete it or use it!
```

### Address Warnings Immediately

- Treat warnings as errors in CI: `RUSTFLAGS="-D warnings"`
- Don't commit code with warnings
- Use `#[allow(...)]` only with documented justification
