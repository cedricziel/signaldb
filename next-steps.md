* ✅ Extract the Catalog setup into a shared helper or wrapper so microservices (acceptor/ingester, router) can each initialize themselves.
  - Created `ServiceBootstrap` helper with unified catalog registration, heartbeat, and graceful shutdown
  - Updated acceptor service to use catalog-only discovery (removed duplicate NATS implementation)
  - Added SQLite support for dev/testing alongside PostgreSQL for production

* ✅ Update remaining services (writer, router, querier) to use ServiceBootstrap pattern.
  - Updated writer service to use catalog-based discovery instead of NATS
  - Updated monolithic main.rs to register router service with catalog
  - All services now use consistent ServiceBootstrap for registration and graceful shutdown
* ✅ Replace the one‐off log calls with a background polling/watch mechanism for the router.
  - Created ServiceRegistry with background polling to keep service state updated
  - Router endpoints now use service registry for routing decisions instead of just returning mock data
  - Added proper service discovery and load balancing infrastructure
* ✅ Implement automatic SQLite database creation for configless operation.
  - Added automatic data directory creation for SQLite databases (like Prometheus)
  - Updated configuration defaults to use SQLite (.data/signaldb.db) by default
  - Enabled service discovery by default for zero-config startup
  - Added comprehensive tests for directory creation and config defaults
  - SignalDB now supports zero-config portable deployments
* ✅ Hook up real config (e.g. via Figment or env vars) for DSN, TTLs, heartbeat intervals.
  - ServiceBootstrap now uses Configuration instead of env vars
  - Added proper defaults for discovery config (SQLite, reasonable intervals)
  - SignalDB now supports completely configless operation with sensible defaults
  - Environment variables and TOML configs still work for advanced use cases
* Move towards separate binaries (acceptor vs router) each wiring the Catalog in the same pattern.
