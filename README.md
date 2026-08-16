# SignalDB

**Observability for your homelab — one binary, one data directory.**

SignalDB stores traces, logs, metrics, and continuous profiles. It ingests
OpenTelemetry natively, answers Grafana's Tempo, Loki, Prometheus, and
Pyroscope APIs, and ships a built-in Explore UI — so you get a full
observability stack from a single process, without running four separate
systems to get four signals.

It is designed to be **stupidly easy to run small** and able to **scale out
when you outgrow small**: the same binary that runs on a Raspberry Pi or a
NAS with SQLite and local disk splits into independent services backed by
PostgreSQL and S3-compatible object storage.

![SignalDB Explore UI showing live logs from two services with level colors, a volume histogram, and a fields sidebar](docs/assets/screenshots/explore-logs.png)

## Why homelabbers run it

- **One process, no dependencies.** SQLite catalog, local-disk storage,
  sensible defaults. No JVM, no Zookeeper, no sidecar zoo.
- **Bounded disk usage.** A built-in compactor enforces 30-day retention by
  default and compacts Parquet files in the background — it won't quietly
  eat your NAS.
- **A UI out of the box.** Logs, traces, and metrics are explorable at
  `/ui/` without installing Grafana. When you want Grafana, the
  Tempo/Loki/Prometheus-compatible APIs and a native plugin are there.
- **Runs on your hardware.** Multi-arch images (`amd64`/`arm64`) built on
  Alpine; cheap object storage (MinIO, S3) is optional, not required.
- **Open standards.** OTLP ingest (gRPC and HTTP), Prometheus
  `remote_write`, and Apache Parquet/Iceberg on disk — your data stays in
  open formats you can query with anything that speaks SQL over Parquet.

## Quick start

Zero config works: start SignalDB with no tenants configured and it
provisions a `default` tenant (dataset `default`) on first boot, printing a
fresh ingest API key **once** in the startup logs (`docker logs signaldb`).
Point your SDK at it with `x-tenant-id=default` and that key.

To choose your own tenant and key instead, create a minimal
`signaldb.toml` defining a tenant and an ingest API key
(authentication is always on — even at home, telemetry endpoints shouldn't
be open writes):

```toml
[database]
dsn = "sqlite:///data/signaldb.db"

[discovery]
dsn = "sqlite:///data/signaldb.db"

[schema]
catalog_type = "sql"
catalog_uri = "sqlite:///data/catalog.db"

[storage]
dsn = "file:///data/storage"

[[auth.tenants]]
id = "homelab"
slug = "homelab"
name = "Homelab"
default_dataset = "default"

[[auth.tenants.api_keys]]
key = "sk-homelab-change-me"
name = "Ingest key"

[[auth.tenants.datasets]]
id = "default"
slug = "default"
is_default = true
```

Run the monolithic image (published for `amd64` and `arm64`):

```bash
docker run -d --name signaldb \
  -p 3000:3000 -p 4317:4317 -p 4318:4318 \
  -v signaldb-data:/data \
  -v "$PWD/signaldb.toml:/data/signaldb.toml:ro" \
  ghcr.io/cedricziel/signaldb:main --config /data/signaldb.toml
```

Create a user so you can sign in to the UI:

```bash
docker exec -e SIGNALDB_USER_PASSWORD=changeme signaldb \
  signaldb-cli --config /data/signaldb.toml \
  user create you@example.com --tenant homelab
```

Point any OpenTelemetry SDK or Collector at it:

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
OTEL_EXPORTER_OTLP_HEADERS="authorization=Bearer sk-homelab-change-me,x-tenant-id=homelab"
```

Then open <http://localhost:3000/ui/> and sign in. Full guides:
[sending OTLP data](docs/users/sending-otlp.md) ·
[Explore UI](docs/users/explore-ui.md) ·
[Grafana datasource](docs/users/grafana-datasource.md).

## What works today

SignalDB is under active development.

| Signal   | Ingest                          | Query                                                      | Maturity                                                                                           |
| -------- | ------------------------------- | ---------------------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| Traces   | OTLP gRPC/HTTP                  | Tempo-compatible API, TraceQL search, Explore UI waterfall | Most mature surface                                                                                |
| Logs     | OTLP gRPC/HTTP                  | LogQL (broad function coverage), live tail in the UI       | Solid; some backend query optimizations still landing                                              |
| Metrics  | OTLP, Prometheus `remote_write` | PromQL (growing function coverage), SQL                    | Usable; full PromQL parity is tracked in [#336](https://github.com/cedricziel/signaldb/issues/336) |
| Profiles | OTLP (`v1development`)          | Pyroscope-compatible API, Grafana flame graphs             | Experimental                                                                                       |

Nightly micro-benchmark trends (OTLP decode, WAL, Iceberg append, querier
read paths, compaction) are published at
[cedricziel.github.io/signaldb/benchmarks](https://cedricziel.github.io/signaldb/benchmarks/)
— see [Benchmarking](https://cedricziel.github.io/signaldb/contributing/benchmarking/)
for what they do and do not measure. There are no published end-to-end load
numbers yet. Remaining gaps are tracked in the
[issue tracker](https://github.com/cedricziel/signaldb/issues).

## When you outgrow one box

The monolith is one deployment mode, not a ceiling. The same binary runs
each service on its own — `signaldb acceptor`, `signaldb router`,
`signaldb writer`, `signaldb querier`, `signaldb compactor` — discovering
each other through a shared catalog and communicating over Apache Arrow
Flight:

- **PostgreSQL** replaces SQLite for the catalog and service discovery
- **S3-compatible object storage** (MinIO, AWS S3) replaces local disk
- **Multi-tenancy** with per-tenant API keys, datasets, quotas, and
  rate limits — isolation is built in, not bolted on
- **WAL-based durability** on the ingest path, so acknowledged data
  survives crashes

Under the hood SignalDB is built on the
[FDAP stack](docs/architecture/fdap.md) — Apache Arrow **F**light,
**D**ataFusion, **A**rrow, and **P**arquet — with Apache Iceberg as the
table format. See the
[architecture overview](docs/architecture/overview.md).

## Documentation

Docs live at **<https://cedricziel.github.io/signaldb/>** (also browsable
in [`docs/`](docs/)):

- [Using SignalDB](docs/users/) — sending data, querying, Grafana, the UI
- [Operating SignalDB](docs/operations/) — deployment, WAL persistence,
  retention and compaction
- [Architecture](docs/architecture/) — how it works inside
- [Contributing](docs/contributing/rust.md) — coding standards

## Development

```bash
cargo build                # build everything
cargo test                 # run the test suite
./scripts/run-dev.sh       # monolithic mode with local file storage
```

Prerequisites: stable Rust (edition 2024) and the Protocol Buffers
compiler. See [CLAUDE.md](CLAUDE.md) and
[docs/contributing/rust.md](docs/contributing/rust.md) for conventions.

## License

[AGPL-3.0](LICENSE)
