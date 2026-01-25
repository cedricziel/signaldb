# SignalDB Code Style & Conventions

## Rust Edition 2024
Project requires Rust 1.88.0+ minimum.

## Clippy Compliance
```rust
// Direct variable interpolation
format!("Service {service_id} at {address}")
log::info!("Discovered {count} services")

// Use vec! macro
let items = vec![item1, item2, item3];

// Prefer !is_empty() over len() > 0
if !items.is_empty() { ... }

// Use panic! for intentional panics
panic!("Failed to initialize: {error}");
```

## Commit Guidelines
- Use semantic commits (feat:, fix:, chore:, docs:, refactor:, test:)
- Run `cargo fmt` and `cargo clippy` before committing (pre-commit hooks)

## Key Patterns
- Use Arrow & Parquet types re-exported by DataFusion for version compatibility
- Flight for zero-copy inter-service communication
- ServiceBootstrap pattern for automatic service registration
- Capability-based routing (TraceIngestion, Storage, QueryExecution, Routing)

## Error Handling
- Use `anyhow` for application errors
- Use `thiserror` for library errors
- Avoid over-engineering error handling
