## Service Discovery via NATS

### Context
- In a microservice deployment, components (acceptor, writer, router, querier, etc.) must locate one another dynamically.
- In monolithic mode services are co‑located and discovery is not required.
- We already use NATS for messaging; we can leverage it for lightweight service discovery.

### Goals
- Decouple service endpoints behind well‑known roles.
- Support dynamic registration, unregistration, and automatic health expiration.
- Minimize additional infrastructure by reusing NATS core KV or JetStream.
- Provide client‑side caching and notifications of topology changes.

### Roles and Subjects
| Role      | KV Bucket or Subject Prefix       |
|-----------|-----------------------------------|
| acceptor  | `services.acceptor`               |
| writer    | `services.writer`                 |
| router    | `services.router`                 |
| querier   | `services.querier`                |
| ...       | ...                               |

### Registration
1. On startup, a service instance registers itself:
   - **KV store approach** (preferred):
     ```bash
     # Create bucket once (ops)
     nats kv create services.acceptor --ttl 30s
     # Upsert presence
     nats kv put services.acceptor/{instanceId} '{"host":"...","port":...}'
     ```
   - **Subject approach**:
     ```bash
     nats pub services.acceptor.register '{"id":"...","host":"...","port":...}'
     ```
2. On graceful shutdown, deregister:
   ```bash
   nats kv del services.acceptor/{instanceId}
   ```

### Health TTL / Heartbeats
- **KV**: use key TTL (e.g., 30s) and periodically refresh with `kv put`.
- **Subjects**: publish heartbeat messages on `services.acceptor.heartbeat`.

### Discovery / Lookup
- **KV watch**: apps watch `KV.watch()` on bucket to receive updates.
- **Subjects**: subscribe to `services.{role}.register` / `.deregister`.
- Provide a `lookup(role: &str) -> Vec<Instance>` API and streaming updates.

### Example API (in common/discovery)
```rust
/// Service instance metadata
struct Instance { id: String, host: String, port: u16 }

/// Register self under role
async fn register(role: &str, inst: Instance) -> anyhow::Result<()>;

/// Deregister self
async fn deregister(role: &str, id: &str) -> anyhow::Result<()>;

/// Stream changes for a role
fn watch(role: &str) -> impl Stream<Item = Vec<Instance>>;
```

### Integration
- **Monolith**: discovery disabled; direct wiring used.
- **Microservices**: enable via config (e.g., `DISCOVERY_KIND=nats`).
- Each service calls `register()` at startup; clients use `watch()` or `lookup()`.

### Operational Considerations
- Secure NATS access (mTLS, JWT).
- Tune KV bucket TTL, entry count.
- Handle NATS reconnection and backpressure.
- Consider multi‑region replication or fallback DNS.

### Next Steps
1. Prototype `common/src/discovery/nats` using NATS KV.
2. Add integration tests for registration and watch.
3. Integrate calls in acceptor, writer, router start‑up.
4. Document ENV flags and defaults.