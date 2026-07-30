# SignalDB Signal Producer

A development and demo utility that generates **realistic, correlatable**
OpenTelemetry traces, logs and metrics for two synthetic microservice estates
and ships them to SignalDB over OTLP/gRPC. It is meant to make a fresh SignalDB
deployment look like a real production backend within seconds.

## What it generates

Two independent estates, each modeled as a full microservice deployment on AWS
EKS with its own `service.namespace`, cloud region and Kubernetes cluster:

### `rideshare` (us-east-1)

A ride-hailing platform. Rider and driver **mobile apps** call an **edge
gateway** that fans out over gRPC to trip, matching, pricing, location, payment
and notification services, backed by **Redis** (geo/surge), **PostgreSQL** and
**Kafka**.

| Service                | Platform         | Role                        |
| ---------------------- | ---------------- | --------------------------- |
| `rider-app`            | iOS (mobile)     | Rider handset client        |
| `driver-app`           | Android (mobile) | Driver handset client       |
| `edge-gateway`         | Go pod           | Public API edge             |
| `trip-service`         | Java pod         | Trip orchestration          |
| `matching-service`     | Go pod           | Driver matching (Redis geo) |
| `location-service`     | Go pod           | Geo resolution (PostgreSQL) |
| `pricing-service`      | Rust pod         | Surge pricing (Redis cache) |
| `payment-service`      | Java pod         | Fare capture                |
| `notification-service` | Node.js pod      | Kafka consumer → FCM push   |

### `shop` (eu-west-1)

An online storefront. A **web SPA** and a **mobile app** call an edge gateway
fronting catalog, cart, checkout, inventory, payment, shipping, search and
recommendation services, backed by **Redis**, **PostgreSQL**, **Elasticsearch**
and **Kafka**, plus an external **Stripe-like** payment gateway.

| Service                  | Platform         | Role                           |
| ------------------------ | ---------------- | ------------------------------ |
| `web-storefront`         | Browser          | Web SPA client                 |
| `shop-mobile`            | Android (mobile) | Mobile client                  |
| `edge-gateway`           | Go pod           | Public API edge                |
| `checkout-service`       | Java pod         | Checkout orchestration         |
| `cart-service`           | Go pod           | Cart (Redis)                   |
| `inventory-service`      | Go pod           | Stock reservation (PostgreSQL) |
| `payment-service`        | Java pod         | External gateway charge        |
| `shipping-service`       | Rust pod         | Kafka consumer → shipment      |
| `search-service`         | Java pod         | Product search (Elasticsearch) |
| `catalog-service`        | Java pod         | Product hydration (Redis)      |
| `recommendation-service` | Python pod       | Related products               |

## Correlatability

The data is built so all three signals join up the way they would in a real
backend:

- **Distributed traces cross services.** Each estate emits full traces (e.g.
  rider handset → gateway → trip → matching → location → PostgreSQL, with a
  Kafka `trip.requested` event consumed by the notification service). Every span
  in a trace shares a `trace_id` even though it belongs to a different OTLP
  resource — because child spans are created from the parent's context, exactly
  as real cross-service propagation works.
- **Logs join to traces.** Log records are emitted inside the active span, so
  the backend can correlate them by `trace_id`/`span_id`.
- **Metrics carry trace exemplars.** RED and dependency latency histograms are
  recorded inside the span context, so histogram points reference the span that
  produced them.
- **Infra metrics share resources with spans.** Host and Kubernetes metrics
  (`system.cpu.utilization`, `system.memory.usage`, `k8s.pod.cpu.usage`, …) are
  emitted from each service's meter, so they carry the same `service.*`,
  `k8s.*`, `host.*` and `cloud.*` attributes as that service's traces and logs.

Roughly one trace in eight fails, injecting error spans, exception events,
`error`-severity logs and `error.type` metric dimensions (no drivers available,
Redis timeout, out of stock, card declined).

## OpenTelemetry semantic conventions

Everything follows the OTel semantic conventions: HTTP (`http.request.method`,
`http.route`, `http.response.status_code`), RPC (`rpc.system=grpc`,
`rpc.service`, `rpc.method`), database (`db.system`, `db.operation.name`,
`db.collection.name`), messaging (`messaging.system=kafka`,
`messaging.destination.name`, `messaging.operation.type`), plus resource-level
`service.*`, `cloud.*`, `k8s.*`, `container.*`, `host.*`, `device.*` and
`browser.*` attributes.

## Usage

```bash
# Both estates to a local acceptor (OTLP/gRPC on :4317)
cargo run --bin signal-producer

# Only the rideshare estate, faster ticks
cargo run --bin signal-producer -- --estate rideshare --interval 2

# A fixed number of ticks (useful for tests/CI), more traces per tick
cargo run --bin signal-producer -- --count 10 --traces-per-tick 8

# Point at a remote SignalDB acceptor
cargo run --bin signal-producer -- --endpoint http://my-signaldb:4317
```

### Flags

| Flag                | Default                 | Description                                 |
| ------------------- | ----------------------- | ------------------------------------------- |
| `--endpoint`        | `http://localhost:4317` | OTLP/gRPC endpoint to export to             |
| `--estate`          | `all`                   | `rideshare`, `shop`, or `all`               |
| `--interval`        | `5`                     | Seconds between generation ticks            |
| `--count`           | `0`                     | Number of ticks to run (`0` = until Ctrl+C) |
| `--traces-per-tick` | `4`                     | Distributed traces per estate per tick      |

Each tick emits `traces_per_tick` traces per estate plus one infrastructure
metric sample per backend service.

## Verifying ingestion

```bash
# Start the SignalDB stack (or a single acceptor)
cargo run --bin signaldb

# In another terminal, generate signals
cargo run --bin signal-producer -- --count 5

# Query traces via the Tempo-compatible API
curl "http://localhost:3000/api/search?limit=10"
```

## Code layout

| Module         | Responsibility                                                 |
| -------------- | -------------------------------------------------------------- |
| `topology.rs`  | Estates, services and their OTLP resource attributes           |
| `fleet.rs`     | One tracer/logger/meter SDK pipeline + instruments per service |
| `emit.rs`      | Span/log/metric helpers (simulated timing, trace-correlated)   |
| `scenarios.rs` | The distributed-trace traffic patterns per estate              |
| `infra.rs`     | Per-tick host/Kubernetes metric sampling                       |
| `main.rs`      | CLI, generation loop, flush/shutdown                           |

## Design notes

- **One SDK pipeline per service.** Each service is a distinct OTLP resource, so
  a single trace legitimately spans many resources — as it would in production.
- **Simulated timing.** Spans are laid out on a scenario-local timeline with
  realistic latencies and a per-trace jitter factor, so latency histograms have
  spread without the generator ever sleeping.
- **Best-effort shutdown.** On exit every pipeline is flushed and shut down;
  failures are logged but never strand the remaining pipelines.
