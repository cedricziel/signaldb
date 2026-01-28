# Deploying SignalDB on Dokku

SignalDB can be deployed on [Dokku](https://dokku.com) using the monolithic Docker image from GHCR, or by building from source via git push.

## From GHCR image

```bash
# Create app
dokku apps:create signaldb

# Persistent storage
sudo mkdir -p /var/lib/dokku/data/storage/signaldb-{wal,data,db}
sudo chown -R 1000:1000 /var/lib/dokku/data/storage/signaldb-*
dokku storage:mount signaldb /var/lib/dokku/data/storage/signaldb-wal:/data/wal
dokku storage:mount signaldb /var/lib/dokku/data/storage/signaldb-data:/data/storage
dokku storage:mount signaldb /var/lib/dokku/data/storage/signaldb-db:/data/db

# Configuration
dokku config:set signaldb \
  SIGNALDB_DATABASE_DSN="sqlite:///data/db/signaldb.db" \
  SIGNALDB_DISCOVERY_DSN="sqlite:///data/db/signaldb.db" \
  SIGNALDB_STORAGE_DSN="file:///data/storage" \
  SIGNALDB_SCHEMA_CATALOG_URI="sqlite:///data/db/catalog.db" \
  ACCEPTOR_WAL_DIR="/data/wal/acceptor" \
  WRITER_WAL_DIR="/data/wal/writer" \
  RUST_LOG=info

# Ports
dokku ports:set signaldb http:4318:4318 grpc:4317:4317 grpc:50051:50051 grpc:50053:50053 http:3000:3000

# Deploy
dokku git:from-image signaldb ghcr.io/cedricziel/signaldb:<version>
```

## From source (git push)

Dokku will build using the production `Dockerfile` with the `monolithic` target:

```bash
dokku apps:create signaldb
dokku docker-options:add signaldb build "--target monolithic"
# Apply the same storage, config, and port settings as above

git remote add dokku dokku@your-server:signaldb
git push dokku main
```

## Ports

| Port | Protocol | Service |
|------|----------|---------|
| 4317 | gRPC | OTLP gRPC ingestion |
| 4318 | HTTP | OTLP HTTP ingestion |
| 50051 | gRPC | Writer Flight |
| 50053 | gRPC | Router Flight |
| 3000 | HTTP | Tempo-compatible HTTP API |

## Health checks

```bash
curl http://signaldb.your-domain:4318/health  # Acceptor
curl http://signaldb.your-domain:3000/health   # Router HTTP API
```
