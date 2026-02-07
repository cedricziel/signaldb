---
name: rust-patterns
description: SignalDB Rust coding patterns and conventions - error handling (thiserror/anyhow), FDAP version alignment, tracing, async code, trait design, clippy rules, dependency management, testing patterns, and documentation standards. Use when writing or reviewing Rust code for this project.
user-invocable: false
---

# SignalDB Rust Patterns & Conventions

## Edition & Toolchain

- Rust Edition **2024**, minimum **1.88.0**
- AGPL-3.0 license

## Error Handling

- **Library code** (reusable modules, public APIs): Use `thiserror`
- **Application code** (main, CLI, service entry): Use `anyhow`
- **NEVER** use `.unwrap()` or `.expect()` in production paths
- Use `?` with `anyhow::Context` for error propagation:

```rust
use anyhow::Context;
file.read_to_string(&mut buf)
    .context("Failed to read configuration file")?;
config.parse()
    .with_context(|| format!("Failed to parse config at {path}"))?;
```

## FDAP Version Alignment

Arrow, Parquet, DataFusion must be version-compatible. **Always use DataFusion re-exports**:

```rust
// CORRECT
use datafusion::arrow::array::StringArray;
use datafusion::parquet::arrow::ArrowWriter;

// WRONG
use arrow::array::StringArray;  // may be wrong version!
```

## Logging

Use `tracing`, not `log`. Structured fields for machine parsing:

```rust
tracing::info!(tenant_id = %ctx.tenant_id, dataset = %dataset, "Processing request");
tracing::error!(error = ?err, service_id = %id, "Service registration failed");
```

**No emoji in logs** (breaks parsing). Emoji OK in CLI output.

Log levels: trace (data dumps) -> debug (dev info) -> info (lifecycle) -> warn (recoverable) -> error (failures)

## Async Code

```rust
// Use tokio primitives, not std
use tokio::sync::{RwLock, Mutex, mpsc};
use dashmap::DashMap;  // For concurrent HashMap

// All network ops need timeouts
let result = timeout(Duration::from_secs(30), client.call())
    .await.context("Request timed out")?.context("Request failed")?;

// Always handle errors in spawned tasks
tokio::spawn(async move {
    if let Err(e) = background_process().await {
        tracing::error!(error = ?e, "Background task failed");
    }
});
```

## Traits

Extract traits only when you have a **concrete need**:
- Multiple implementations exist today
- Testing requires substitution (most common reason)
- External extensibility needed
- Breaking circular compile dependencies

**Don't** create traits speculatively ("we might need this someday").

Prefer generics over trait objects when type is known at compile time.

## Clippy Rules

```rust
// Direct variable interpolation
format!("Service {service_id} at {address}")
tracing::info!("Discovered {count} services")

// vec! macro
let items = vec![item1, item2, item3];

// !is_empty() over len() > 0
if !items.is_empty() { ... }

// panic! for intentional panics
panic!("Failed to initialize: {error}");
```

## Dependencies

Workspace-first approach:
```toml
# In workspace Cargo.toml
[workspace.dependencies]
tokio = { version = "1.48", features = ["full"] }

# In member Cargo.toml
[dependencies]
tokio.workspace = true
```

Run `cargo machete --with-metadata` before every commit.

## Testing

- Unit tests: inline `#[cfg(test)] mod tests {}`
- Integration tests: `tests/` directory per crate
- Cross-service tests: `tests-integration/` crate
- Feature-gate test utilities: `#[cfg(any(test, feature = "testing"))]`
- Use `InMemory` object store for test isolation

```rust
#[tokio::test]
async fn descriptive_test_name_explains_scenario() -> anyhow::Result<()> {
    // Arrange
    // Act
    // Assert
    Ok(())
}
```

## Documentation

- Every module needs `//!` docs
- All `pub` items need `///` docs
- Complex types need `# Example` sections

## Dead Code

No `#[allow(dead_code)]` without justification comment. Delete unused code.
