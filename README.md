# signaldb

A signal database based on the FDAP stack.

## Project goals

I am building this project in my free time. The goal is to build a database for observability signals (metrics/logs/traces/..).

The key traits I want this project to develop over time:

* cost-effective storage
* open standards native ingestion (OTLP, Prom, ..)
* effective querying
* robust interfacing with popular analysis tools (Grafana, Perses, ..)
* easy operation

## Configuration

signaldb can be configured using a TOML configuration file or environment variables. The configuration is loaded in the following order:

1. Default values
2. TOML configuration file (default: `signaldb.toml`)
3. Environment variables (prefixed with `SIGNALDB_`)

### Database Configuration

The database configuration controls where SignalDB stores its metadata:

```toml
[database]
dsn = "sqlite://.data/signaldb.db"  # SQLite database path (default)
```

Environment variable: `SIGNALDB_DATABASE_DSN`

### Queue Configuration

SignalDB uses an internal queue system for processing incoming data. The queue can be configured with the following options:

```toml
[queue]
dsn = "memory://"             # Queue backend DSN (default: memory://)
max_batch_size = 1000         # Maximum number of items per batch
max_batch_wait = "10s"        # Maximum time to wait before processing a non-full batch
```

Environment variables:

* `SIGNALDB_QUEUE_DSN`: Queue backend DSN
* `SIGNALDB_QUEUE_MAX_BATCH_SIZE`: Maximum batch size
* `SIGNALDB_QUEUE_MAX_BATCH_WAIT`: Maximum batch wait time (supports human-readable durations like "10s", "1m")

Currently supported queue backends:
* `memory://`: In-memory queue for single-node deployments

### Storage Configuration

SignalDB supports multiple storage backends for storing observability data:

```toml
[storage]
default = "local"  # Name of the default storage adapter to use

[storage.adapters.local]  # Configure a storage adapter named "local"
type = "filesystem"  # Storage backend type
url = "file:///data"  # Storage URL
prefix = "traces"  # Prefix for all objects in this storage
```

Environment variables:

* `SIGNALDB_STORAGE_DEFAULT`: Name of the default storage adapter
* `SIGNALDB_STORAGE_ADAPTERS_<NAME>_TYPE`: Storage type for adapter
* `SIGNALDB_STORAGE_ADAPTERS_<NAME>_URL`: Storage URL for adapter
* `SIGNALDB_STORAGE_ADAPTERS_<NAME>_PREFIX`: Storage prefix for adapter

## What is the FDAP stack?

The FDAP stack is a set of technologies that can be used to build a data acquisition and processing system.
It is composed of the following components:

* **F**light - Apache Arrow Flight
* **D**ataFusion - Apache DataFusion
* **A**rrow - Apache Arrow
* **P**arquet - Apache Parquet

<https://www.influxdata.com/glossary/fdap-stack/>

## License

AGPL-3.0
