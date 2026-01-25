# SignalDB Project Overview

## Purpose
SignalDB is a distributed observability signal database built on the FDAP stack (Flight, DataFusion, Arrow, Parquet). It provides cost-effective storage and querying of metrics, logs, and traces with native OTLP support and Tempo API compatibility.

## Tech Stack
- **Language**: Rust (Edition 2024, minimum 1.88.0)
- **Data processing**: Apache Arrow, DataFusion, Parquet
- **Communication**: Apache Arrow Flight (gRPC), Axum (HTTP)
- **Schema**: Apache Iceberg with SQL catalog backend
- **Async runtime**: Tokio
- **Build tool**: Cargo (workspace)

## Architecture
- **acceptor**: OTLP HTTP/gRPC ingestion (ports 4317/4318)
- **router**: HTTP router (port 3000) + Flight (port 50053) with Tempo-compatible API
- **writer**: Stateful ingestion service (the "Ingester")
- **querier**: Query execution engine
- **common**: Shared configuration, discovery, and data models
- **grafana-plugin**: Native Grafana datasource plugin

## Data Flow
- **Write Path**: Client → Acceptor (OTLP) → WAL → Writer (Flight) → Iceberg Tables (Parquet)
- **Query Path**: Client → Router (HTTP) → Querier (Flight) → DataFusion → Iceberg Tables

## Key Directories
- `src/`: All workspace member crates
- `.data/`: Local storage (WAL, Parquet, SQLite)
- `scripts/`: Development scripts
- `examples/`: Example code (including flight_client.rs)
