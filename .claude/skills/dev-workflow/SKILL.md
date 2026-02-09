---
name: dev-workflow
description: SignalDB development workflow - build, test, lint, format, run services, Docker, Grafana plugin, health checks, and semantic commits. Use when building, testing, running, or deploying SignalDB.
---

# SignalDB Development Workflow

## Build

```bash
cargo build                # Debug build
cargo build --release      # Release build
cargo build -p <package>   # Build specific crate
```

## Test

```bash
cargo test                          # All tests
cargo test -p <package>             # Specific crate (common, writer, acceptor, etc.)
cargo test <test_name>              # Specific test by name
cargo test -- --nocapture           # With stdout visible
RUST_LOG=debug cargo test <name> -- --nocapture   # With logging
cargo test -p tests-integration     # Integration tests
cargo test -p tests-integration compactor   # Compactor integration tests
cargo test -p compactor             # Compactor unit tests
```

## Pre-Commit Checks (MANDATORY before committing)

These run automatically via cargo-husky hooks, but run manually to catch issues early:

```bash
cargo fmt                                               # Format code
cargo clippy --workspace --all-targets --all-features   # Lint
cargo machete --with-metadata                           # Unused dependencies
cargo deny check                                        # License/security audit
```

## Running Services

### Monolithic Mode
```bash
cargo run --bin signaldb                              # All services in one process
./scripts/run-dev.sh                                  # With local file storage
./scripts/run-dev.sh --sqlite                         # SQLite (default, no deps)
./scripts/run-dev.sh --with-deps --postgres           # With PostgreSQL via docker compose
```

### Microservices Mode
```bash
cargo run --bin acceptor   # OTLP ingestion (:4317/:4318)
cargo run --bin router     # HTTP router (:3000) + Flight (:50053)
cargo run --bin writer     # Data persistence (Flight :50061)
cargo run --bin querier    # Query execution (Flight :50054)
```

```bash
./scripts/run-dev.sh services   # All microservices (logs to .data/logs/)
```

### Storage Locations
- WAL: `.data/wal/`
- Parquet data: `.data/storage/`
- SQLite databases: `.data/*.db`

## Docker

```bash
docker compose up          # Start PostgreSQL, Grafana, MinIO, SignalDB
docker compose up --build  # Build images first
docker compose build       # Build only
```

MinIO: Console `localhost:9001`, API `localhost:9000` (minioadmin/minioadmin)

## Grafana Plugin

```bash
npm install                     # From workspace root
npm run grafana:dev             # Watch + rebuild frontend
npm run grafana:build           # Production build
cd src/grafana-plugin && npm run build:backend   # Rust backend
```

## Health Checks

```bash
curl http://localhost:4318/health   # Acceptor
curl http://localhost:3000/health   # Router
```

## Semantic Commits

Use semantic commit messages:
- `feat:` new feature
- `fix:` bug fix
- `refactor:` code restructuring
- `docs:` documentation
- `test:` test changes
- `chore:` build/tooling changes

## Configuration

Precedence: defaults -> `signaldb.toml` -> env vars (`SIGNALDB_*`)

```bash
cp signaldb.dist.toml signaldb.toml   # Create local config
```

Key env vars for S3/MinIO:
```bash
AWS_ENDPOINT_URL=http://localhost:9000
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_REGION=us-east-1
```
