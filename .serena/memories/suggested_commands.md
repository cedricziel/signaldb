# SignalDB Development Commands

## Build Commands
```bash
cargo build                    # Build all workspace members
cargo build --release          # Release build with optimizations
cargo build -p grafana-plugin  # Build specific package
```

## Test Commands
```bash
cargo test                     # Run all tests
cargo test -p <package>        # Run tests for specific package
cargo test <test_name>         # Run specific test
cargo test -- --nocapture      # With output visible
RUST_LOG=debug cargo test test_name -- --nocapture  # With logging
```

## Run Services
```bash
cargo run --bin signaldb       # Monolithic mode (all services)
cargo run --bin router         # Router only (HTTP 3000 + Flight 50053)
./scripts/run-dev.sh           # Local development mode
```

## Linting & Formatting (Pre-commit)
```bash
cargo fmt                      # Format code
cargo clippy --workspace --all-targets --all-features  # Lint
cargo machete --with-metadata  # Check unused dependencies
cargo deny check               # License and security auditing
```

## Grafana Plugin
```bash
cd src/grafana-plugin && npm run build:backend  # Build Rust backend
npm run grafana:dev            # Watch and rebuild frontend
npm run grafana:build          # Production frontend build
```

## Docker
```bash
docker compose up              # Start all services
docker compose up --build      # Build images first
```

## Git
```bash
git commit  # Triggers pre-commit hooks (fmt + clippy)
```
