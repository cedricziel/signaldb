# SignalDB Scripts

Utility scripts for development and operations.

## Development Scripts

### `run-dev.sh` - Local Development Runner

Quick start for local development with local file storage (no Docker required).

**Basic usage:**
```bash
./scripts/run-dev.sh                    # Monolithic mode with SQLite
./scripts/run-dev.sh services           # Microservices mode
./scripts/run-dev.sh --with-deps        # Start PostgreSQL via Docker
```

**Features:**
- ✅ Zero configuration - works out of the box
- ✅ Local file storage (no MinIO needed)
- ✅ SQLite by default (no database setup)
- ✅ Automatic directory creation
- ✅ Graceful shutdown with Ctrl+C

**Storage locations:**
- Data: `.data/storage/` (Parquet files)
- WAL: `.data/wal/acceptor/`, `.data/wal/writer/`
- Logs: `.data/logs/*.log` (services mode)
- Databases: `.data/signaldb.db`, `.data/catalog.db`

**Modes:**
- **Monolithic** (default): Single process, stdout logs, easiest debugging
- **Services**: Separate processes, file logs, production-like

### `setup-dev-storage.sh` - Storage Directory Setup

Creates development storage directories for Docker Compose deployments.

```bash
./scripts/setup-dev-storage.sh
```

Creates:
- `data/writer-wal/` - Writer WAL files
- `data/acceptor-wal/` - Acceptor WAL files
- `data/writer-storage/` - Parquet data files
- `data/.gitignore` - Prevents committing data files

## Build Scripts

### `build-images.sh` - Docker Image Builder

Builds SignalDB Docker images using the multi-stage Dockerfile.

```bash
./scripts/build-images.sh               # Build all services
./scripts/build-images.sh writer        # Build specific service
```

Builds:
- `signaldb/acceptor:latest` - OTLP ingestion
- `signaldb/router:latest` - HTTP API + Flight
- `signaldb/writer:latest` - Data persistence
- `signaldb/querier:latest` - Query engine

## Test Scripts

### `test-deployment.sh` - Deployment Tester

Tests different deployment modes (monolithic vs microservices).

```bash
./scripts/test-deployment.sh
```

## Quick Reference

| Script | Purpose | When to Use |
|--------|---------|-------------|
| `run-dev.sh` | Local development | Daily development, debugging |
| `setup-dev-storage.sh` | Setup Docker volumes | Before first `docker compose up` |
| `build-images.sh` | Build Docker images | CI/CD, custom image builds |
| `test-deployment.sh` | Test deployments | Verify deployment modes work |

## Common Workflows

**Start developing (fastest):**
```bash
./scripts/run-dev.sh
```

**Start with Docker Compose:**
```bash
./scripts/setup-dev-storage.sh
docker compose up --build
```

**Build images only:**
```bash
./scripts/build-images.sh
```

**Test changes in production-like mode:**
```bash
./scripts/run-dev.sh services --with-deps --postgres
```
