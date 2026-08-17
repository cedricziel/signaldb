---
audience: operator
type: how-to
status: living
sources:
  - src/common/src/wal/**
  - src/acceptor/src/cli.rs
  - src/writer/src/cli.rs
---

# WAL Persistence Configuration

SignalDB implements Write-Ahead Logging (WAL) to provide durability guarantees for incoming observability data. This document describes WAL configuration, deployment patterns, and operational best practices.

## Overview

✅ **Production Ready**: SignalDB's WAL implementation provides comprehensive durability with crash recovery, automatic replay, and configurable persistence policies.

### WAL Architecture

```
OTLP Client → Acceptor → WAL (Disk) → Writer → Parquet Storage
     ↓           ↓         ↓          ↓           ↓
   gRPC/HTTP   Flight   fsync()   Flight   Object Store
```

### Durability Guarantees

1. **Before Acknowledgment**: All OTLP data is written to WAL before client acknowledgment
2. **Crash Recovery**: Unprocessed WAL entries are automatically replayed on service restart
3. **Entry Tracking**: WAL entries are marked as processed only after successful storage
4. **Configurable Flushing**: Supports both immediate and batched flush policies

### Service WAL Usage

Each service keeps its WAL in a per-service subdirectory of the configured base directory (`[wal].wal_dir`, default `.data/wal`):

- **Acceptor Service**: `{wal_dir}/acceptor` (default: `.data/wal/acceptor`), overridable via `ACCEPTOR_WAL_DIR`
- **Writer Service**: `{wal_dir}/writer` (default: `.data/wal/writer`), overridable via `WRITER_WAL_DIR`

⚠️ **Production Warning**: Default WAL directories use local paths that **will not persist** across container or pod restarts. Configure persistent volumes for production deployments.

## Configuration

The WAL base directory is set in the `[wal]` TOML section and applies to both services. Precedence per service:

1. Service-specific override (`ACCEPTOR_WAL_DIR` / `WRITER_WAL_DIR` env var, or `--wal-dir` CLI flag) — points at the **full** service directory
2. `[wal].wal_dir` from `signaldb.toml` (or `SIGNALDB__WAL__WAL_DIR`) with `/acceptor` or `/writer` appended
3. Built-in default `.data/wal`, i.e. `.data/wal/acceptor` and `.data/wal/writer`

### Environment Variables

| Variable                 | Default              | Description                                                 |
| ------------------------ | -------------------- | ----------------------------------------------------------- |
| `ACCEPTOR_WAL_DIR`       | `{wal_dir}/acceptor` | Full WAL directory for acceptor service (override)          |
| `WRITER_WAL_DIR`         | `{wal_dir}/writer`   | Full WAL directory for writer service (override)            |
| `SIGNALDB__WAL__WAL_DIR` | `.data/wal`          | Base WAL directory (figment; equivalent to `[wal].wal_dir`) |

There are no `[wal.acceptor]`/`[wal.writer]` TOML subsections; the per-service overrides are env/CLI only.

### TOML Configuration

The `[wal]` section in `signaldb.toml` uses a single block:

```toml
[wal]
wal_dir = ".data/wal"
max_segment_size = 67108864     # 64MB segments
max_buffer_entries = 1000       # Buffer 1000 entries
flush_interval = "30s"          # Flush every 30 seconds
max_buffer_size_bytes = 134217728  # 128MB
```

Note: segment size, buffer, and flush tuning currently ship as built-in defaults compiled into the services (64MB segments, 1000-entry buffer, 30s flush; the acceptor uses more aggressive per-signal settings for logs and metrics). The `[wal]` TOML block matches these defaults but the services do not yet read the tuning knobs from it — of the `[wal]` settings, only `wal_dir` changes runtime behavior today.

`max_segment_size` caps **both** the entry-log file and the payload data file. Because payloads dominate size (the log holds only fixed-size per-entry metadata), rotation is driven in practice by the data file crossing the cap; a segment is sealed and a new one started before either file exceeds it. This keeps individual segments small, bounds recovery cost, and keeps data-file offsets well clear of the 4 GB (2³²) range.

## Docker Compose Configuration

Configure persistent volumes for WAL directories:

```yaml
services:
  signaldb-writer:
    image: signaldb/writer:latest
    environment:
      WRITER_WAL_DIR: "/data/wal"
    volumes:
      # Persistent WAL storage to survive container restarts
      - writer-wal:/data/wal
    # ... other configuration

  signaldb-acceptor:
    image: signaldb/acceptor:latest
    environment:
      ACCEPTOR_WAL_DIR: "/data/wal"
    volumes:
      # Persistent WAL storage to survive container restarts
      - acceptor-wal:/data/wal
    # ... other configuration

volumes:
  writer-wal:
    driver: local
    driver_opts:
      type: none
      o: bind
      device: ./data/writer-wal
  acceptor-wal:
    driver: local
    driver_opts:
      type: none
      o: bind
      device: ./data/acceptor-wal
```

## Kubernetes Configuration

Use PersistentVolumeClaims to ensure WAL data survives pod restarts:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: signaldb-writer
spec:
  template:
    spec:
      containers:
        - name: writer
          image: signaldb/writer:latest
          env:
            - name: WRITER_WAL_DIR
              value: "/data/wal"
          volumeMounts:
            - name: wal-storage
              mountPath: /data/wal
      volumes:
        - name: wal-storage
          persistentVolumeClaim:
            claimName: signaldb-writer-wal
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: signaldb-writer-wal
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 10Gi
  # Configure storage class for performance requirements
  storageClassName: fast-ssd
```

## Storage Requirements

### Performance Considerations

- **WAL Storage**: Use fast storage (SSD/NVMe) for optimal write performance
- **Storage Class**: In Kubernetes, use high-performance storage classes
- **IOPS**: Ensure adequate IOPS for write-heavy workloads

### Capacity Planning

- **Writer WAL**: Size based on ingestion rate and flush frequency
  - Typical: 5-20GB for high-volume deployments
  - Formula: `max_segment_size * max_segments + buffer`
- **Acceptor WAL**: Generally smaller than writer WAL
  - Typical: 1-5GB for most deployments

### Recommended Storage Classes

| Environment | Storage Class | Performance          |
| ----------- | ------------- | -------------------- |
| Development | `standard`    | Standard disk        |
| Staging     | `fast`        | SSD                  |
| Production  | `fast-ssd`    | High-performance SSD |
| High-volume | `ultra-ssd`   | NVMe with high IOPS  |

## WAL Implementation Details

### Directory Structure

Both the acceptor and the writer keep one WAL per tenant/dataset/signal
combination, so a poisoned segment, a slow fsync, or lock contention on one
tenant's WAL cannot stall another tenant's **append** path. (The writer's
background drain still walks those WALs one at a time, so a tenant with slow
Iceberg commits delays other tenants' commits within a cycle — failures are
isolated, commit latency is not.) Each WAL directory uses a segment-based
structure:

```text
/data/wal/                          # ACCEPTOR_WAL_DIR or WRITER_WAL_DIR
└── acme/                           # Tenant
    └── production/                 # Dataset
        └── traces/                 # Signal type
            ├── wal-0000000000.log    # Segment entry records (framed + CRC)
            ├── wal-0000000000.data   # Segment payload records (framed + CRC)
            ├── wal-0000000000.index  # Processed-entry index
            ├── writer.id             # Stable identity of this WAL directory
            └── dead-letter/          # Entries retired so they stop blocking
                ├── <entry_id>.bin                        # Preserved payload
                ├── <entry_id>.rejected.json              # Why the writer refused it
                ├── <entry_id>.unreadable.json             # Marker; no bytes recoverable
                ├── <entry_id>.corrupt.bin                 # Raw bytes of a payload record
                                                           # that failed its CRC on read
                └── segment-<id>-offset-<offset>.corrupt.bin  # Raw bytes of a log
                                                               # record that failed its
                                                               # CRC / decode on replay
```

Every other marker is keyed by `<entry_id>`, because it is produced by code
that has a successfully-decoded `WalEntry` in hand. The `.corrupt.bin`
marker is the one exception: it exists precisely because the entry's id
could not be recovered (the record that would have named it is what failed
to deserialize), so it is keyed by `segment-<segment_id>-offset-<offset>` —
its physical location in the `.log` file — instead.

A writer WAL is opened lazily, on the first write for that
tenant/dataset/signal. Directories left behind by a previous run are opened at
startup as well, so entries pending from before a restart drain even if that
tenant sends no new traffic.

#### Upgrading a writer from the single-WAL layout

Writers before this change kept one WAL for every tenant, with its segments
lying directly in `{wal_dir}/writer` instead of in a
`{tenant}/{dataset}/{signal}` tree. No operator step is needed: on startup the
writer adopts any such segments as a drain-only WAL, logging

```text
WARN Adopting legacy single-directory WAL segments for draining; new writes use the per-tenant/dataset/signal tree
```

Their pending entries are processed normally: an entry that carries routing
metadata is routed by it, and one that does not falls back to `default` /
`default`, which is where a pre-upgrade writer put it. New writes always go to
the per-tenant tree, so nothing is ever added to the legacy directory.

**The drained files are not deleted automatically.** WAL segment cleanup
(`Wal::start_background_cleanup`) has no caller in any service today, so the
segments stay, are re-read into memory at every writer start, and the `WARN`
above repeats on every restart. Once the writer logs no unprocessed entries
for them — the writer's WAL backlog gauge is at zero and no
`dead-letter/` markers are being added — the files are safe to remove by hand:

```bash
# With the writer stopped, after its last shutdown flush completed cleanly:
rm -f /data/wal/writer/wal-*.log /data/wal/writer/wal-*.data /data/wal/writer/wal-*.index
```

Leave the per-tenant subdirectories (`/data/wal/writer/{tenant}/…`) alone —
only the segment files sitting _directly_ in the writer's WAL directory belong
to the legacy layout.

### Data Flow with WAL

1. **Acceptor receives OTLP data**
2. **Data written to Acceptor WAL** (durability checkpoint)
3. **Client acknowledgment sent** (data is durable)
4. **Data forwarded to Writer via Flight** (Storage capability)
5. **Writer appends to its own WAL and confirms** — the confirm does **not** wait for the Iceberg commit
6. **Writer's background loop commits to Parquet/Iceberg asynchronously**, coalescing pending entries per `(tenant, dataset, table)` per `[writer].commit_interval` / `max_uncommitted_rows` (see the Configuration reference)
7. **WAL entries marked as processed** (cleanup eligible)

Because step 6 is asynchronous, freshly-ingested data is queryable only once the
background loop commits it (bounded by `commit_interval`, default 5s). This
decouples ingest acknowledgement latency from Iceberg/catalog latency and caps
the catalog-metadata write rate. A caller needing immediate queryability forces
a commit with the Writer Flight `do_action("flush")`.

### Recovery Process

On service restart:

1. **WAL scan**: Identify unprocessed entries
2. **Automatic replay**: Reprocess unprocessed entries
3. **Resume normal operation**: Continue with new data

The recovered backlog is logged at startup
(`signaldb.wal.recovered_pending`) and seeded into
`signaldb.wal.entries_pending`, so the pending gauge counts entries carried
over from the previous process as well as ones appended by this one. Without
that seed the gauge drifts negative after every restart that recovers a
backlog, since those entries are decremented when processed but were
incremented by a process that is gone.

#### Record Framing and Checksums

Every WAL record carries a length and a CRC-32 of its payload, and every
`.log` file opens with a format header (`SDBW` magic + version). The exact
layout is documented in
[Storage Layout](../architecture/storage-layout.md#record-framing-format-v1).
Operationally this means:

- **Corruption is attributable.** A damaged record is reported against the
  entry it belongs to (`entry_id`, `tenant_id`, `dataset_id`, `signal`,
  `segment_id`, `data_offset`), never as an anonymous Arrow parse error.
- **Corruption is skippable.** One bad record never blocks the records around
  it: replay resyncs on the next `.log` record, and a bad `.data` record fails
  only its own read.
- **Legacy (pre-framing) segments keep working.** A segment without the
  format header is read on the old layout, sealed against new writes, and
  rewritten into the framed format by compaction; the service logs a
  `WAL segment uses the legacy unframed format` warning per legacy segment on
  open. No operator action is needed, and nothing converts a segment in place.
  A `.log` file whose header names a version this build does not know is
  refused at open with a clear error (a downgrade after an upgrade that
  bumped the format).

`signaldb.wal.corrupt_entries` carries a `record` attribute: `log` for a
`.log` record that failed replay, `data` for a `.data` record that failed its
integrity check on read (with a `signal` attribute).

#### Corrupted Entry Records During Replay

Replay reads a segment's `.log` file as a sequence of `[u32 length][u32
crc32][bincode-encoded WalEntry]` records after the segment header. Two
distinct failures can occur while walking that sequence, and they are handled
differently:

- **Torn tail** (short read): the length prefix or the payload it describes
  runs past the end of the file — the shape of a crash or kill mid-write.
  Framing is unrecoverable past this point, so replay stops and logs a
  `WAL segment tail truncated` warning naming the segment and byte offset.
  Every entry before the tear is preserved; the incomplete final record is
  dropped.
- **Content corruption** (framing intact, payload damaged): the length
  prefix is valid and an in-bounds byte range was read, but that range fails
  its CRC or does not deserialize as a `WalEntry` — for example a bit flip
  or partial overwrite from an OOM kill or disk fault landed inside an
  otherwise complete record. Because the framing is intact, the byte offset
  of the _next_ record is still known, so replay **skips the corrupt record
  and continues** rather than aborting: it logs a `WAL entry record corrupt
during replay` error naming the segment and offset, increments the
  `signaldb.wal.corrupt_entries{record="log"}` counter, quarantines the raw
  record bytes to
  `<wal_dir>/dead-letter/segment-<segment_id>-offset-<offset>.corrupt.bin`,
  and resumes at the next record.

  This is a genuine, permanent loss of that one entry: the `WalEntry`
  metadata record — which carries the `data_offset`/`data_size` needed to
  locate its payload in the segment's `.data` file, plus its
  `tenant_id`/`dataset_id`/`operation` — is what failed to decode, so the
  associated payload bytes are orphaned and unreachable through the normal
  read path even if they are themselves intact. The quarantined
  `.corrupt.bin` file (the raw, still-corrupted metadata record bytes) is
  the only remaining copy; there is no automatic recovery from it, only
  manual forensic inspection (`hexdump`, or diffing against a healthy
  entry's known layout) if the tenant/dataset/data range needs to be
  reconstructed by hand.

This distinction matters operationally: treating content corruption like a
torn tail (aborting replay) would silently discard every entry _after_ the
corrupt one on every single restart, and because the corrupt bytes are
persistent on disk, the service would hit the same record and fail identically
on every subsequent restart — a permanent crash loop rather than a one-time
loss of a single entry (issue #1033). A failed quarantine write (disk full,
permissions) is logged and swallowed rather than propagated, for the same
reason: it must never turn into another way to re-trigger the crash loop it
exists to prevent.

### Write Integrity

Each entry records the byte offset of its payload in the segment's `.data`
file, and reads seek to that offset. The offset is therefore authoritative:
appends **seek to the tracked offset and overwrite**, rather than relying on the
OS append mode (`O_APPEND`) to place bytes at the physical end of file. This
makes a short write self-correcting — if a payload write lands only some of its
bytes and then errors (for example under disk pressure), the offset counter is
not advanced, so the next append seeks back to the same offset and overwrites
the partial bytes. A single short write can therefore no longer shift every
subsequent entry in the segment, which previously corrupted the Arrow framing of
all following entries.

### Corrupt or Unreadable Entries

An entry can become unreadable — a truncated or partial data write, an entry
whose stored byte range runs past the data file, a `.data` record whose CRC no
longer matches its payload — or readable but undecodable (bytes that are not a
valid Arrow IPC stream). One such entry must not wedge the processing loop, so
the writer (and the acceptor's retry consumer) handle it in three steps:

1. **Bounds and integrity check first**: before handing bytes to the Arrow
   decoder, the WAL validates the entry's byte range against the actual
   data-file length and then verifies the record header (magic, length, CRC).
   A failure is a precise error naming the entry and segment, rather than an
   opaque parse error later; the damaged record's raw bytes are quarantined
   as `dead-letter/<entry_id>.corrupt.bin` and
   `signaldb.wal.corrupt_entries{record="data"}` is incremented.
2. **Attributed diagnostics**: route, read, deserialize, and dead-letter
   failures log `tenant_id`, `dataset_id`, `signal`, `data_offset`, and
   `data_size`, so a burst of failures is attributable to the affected tenant
   and signal instead of an anonymous flood.
3. **Retire without blocking neighbours**: an _unreadable_ entry (bounds or
   integrity failure) is deterministic, so it is retired on first sight with
   a `<entry_id>.unreadable.json` marker and marked processed; an
   _undecodable_ entry is retried a bounded number of times and then moved to
   `<wal_dir>/dead-letter/<entry_id>.bin` with its raw payload preserved.
   Either way the entries around it keep processing in the same cycle.

A growing dead-letter directory or a spike in these error logs indicates
corrupt WAL segments; inspect the preserved payloads and, if a whole segment
is unreadable, remove it so replay stops retrying it.

### Entries the Writer Refuses

The acceptor's retry consumer faces a different failure: an entry that reads
and deserializes cleanly, but that the writer will not accept — a batch that
cannot be shaped into its target table, such as a null in a column the table
declares non-nullable. The verdict is a property of the bytes, so it is
identical on every retry.

The consumer classifies each forward failure before deciding:

- **The writer refused the batch** (`InvalidArgument`, `FailedPrecondition`,
  `OutOfRange`, `Unimplemented`) — the entry is dead-lettered and the pass
  continues to the entries behind it.
- **Anything else**, including `Internal` and an unreachable writer, is
  treated as transient: the pass stops for this WAL and retries next cycle,
  and nothing is discarded.

The default is deliberately asymmetric. A rejection misread as transient only
costs retries; a transient failure misread as a rejection discards data the
writer would have accepted.

Retiring the entry matters as much as preserving it: until it is marked
processed it stays in the unprocessed set, every later pass walks it again,
and its segment can never be reclaimed. One refused entry blocking a pass is
enough to stop a WAL draining entirely.

Rejected payloads are intact and replayable once the underlying cause is
fixed, so they are preserved as `<entry_id>.bin` alongside an
`<entry_id>.rejected.json` marker recording the entry's identity and the
writer's reason. That marker is what distinguishes them from unparseable
payloads in the same directory — check it before replaying anything:

```bash
# Why entries were refused, newest first
cat /data/wal/*/*/*/dead-letter/*.rejected.json | jq -r '.reason'
```

A recurring reason across many entries points at a systematic conversion or
schema fault rather than isolated corruption.

## Permissions

Ensure proper file system permissions:

- **User**: Container user (typically UID 1000)
- **Permissions**: Read/write access to WAL directory
- **SELinux**: Configure appropriate labels if enabled

### Docker Example

```bash
# Create WAL directories with proper permissions
mkdir -p ./data/writer-wal ./data/acceptor-wal
chown 1000:1000 ./data/writer-wal ./data/acceptor-wal
chmod 755 ./data/writer-wal ./data/acceptor-wal
```

### Kubernetes Security Context

```yaml
spec:
  template:
    spec:
      securityContext:
        runAsUser: 1000
        runAsGroup: 1000
        fsGroup: 1000
      containers:
        - name: writer
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
```

## Performance Tuning

Segment size, buffer size, and flush interval are currently built-in per-signal defaults and are not runtime-configurable:

| Service  | Signal  | Segment size | Buffer entries | Flush interval |
| -------- | ------- | ------------ | -------------- | -------------- |
| Acceptor | Traces  | 64MB         | 1000           | 30s            |
| Acceptor | Logs    | 64MB         | 2000           | 15s            |
| Acceptor | Metrics | 128MB        | 5000           | 10s            |
| Writer   | All     | 64MB         | 1000           | 30s            |

Tuning WAL throughput today means tuning the storage underneath it (see Storage Requirements above).

## Monitoring and Alerting

### Key Metrics

- **WAL Disk Usage**: Monitor disk space consumption
- **WAL Segment Count**: Track number of segments
- **WAL Flush Latency**: Monitor write performance (`signaldb.wal.flush.duration`)
- **WAL Errors**: Alert on WAL operation failures
- **Unprocessed Entries**: Monitor processing lag (`signaldb.wal.entries_pending`)
- **Open WAL instances** (`signaldb.wal.instances`): one per tenant/dataset/signal, opened on first write and never closed. Each holds three file descriptors and a flush timer, so this gauge is the early warning for file-descriptor pressure in a deployment that keeps adding tenants
- **Skipped WALs** (`signaldb.wal.list_failures`): a WAL whose entries could not be listed is skipped for that processing cycle; a non-zero rate means some tenant's backlog is not draining

### Health Check Endpoints

The health endpoints are simple liveness probes — they do not expose WAL status:

```bash
# Acceptor liveness (OTLP HTTP port, returns "ok")
curl http://acceptor:4318/health

# Router liveness (HTTP API port, returns 200)
curl -i http://router:3000/health
```

To observe WAL state, inspect the WAL directories directly:

```bash
# Disk usage per WAL directory
du -sh /data/wal/*

# Segment count (acceptor: per tenant/dataset/signal)
find /data/wal -name 'wal-*.log' | wc -l

# Dead-lettered entries (should be empty)
find /data/wal -path '*/dead-letter/*' | wc -l
```

When `[self_monitoring]` is enabled, services also export `signaldb.wal.*` metrics (entries written/processed/pending, flush duration) via OTLP into SignalDB itself. `signaldb.wal.entries_pending` is the backlog signal: it is process-local (a restart resets it, then re-seeds it from the recovered backlog) and must never read below zero — a negative value means increments and decrements have gone out of balance and the metric cannot be trusted until that is fixed.

`signaldb.wal.corrupt_entries` counts records that failed their integrity check: `record="log"` for entry records discarded during replay (see [Corrupted Entry Records During Replay](#corrupted-entry-records-during-replay)), `record="data"` for payload records that failed their CRC on read. It is an alertable signal, not just a diagnostic: it should stay at zero, and any increase means an entry's data was permanently lost — not merely delayed or retried — with only the quarantined `segment-<id>-offset-<offset>.corrupt.bin` / `<entry_id>.corrupt.bin` file under `dead-letter/` left to inspect by hand. A nonzero rate points at disk-level corruption (OOM kill mid-write, disk fault, or similar) rather than an application bug, so treat it as a storage-health alert.

### Example Prometheus Alerts

```yaml
groups:
  - name: signaldb-wal
    rules:
      - alert: WALDiskSpaceHigh
        expr: (disk_used_bytes{mountpoint="/data/wal"} / disk_size_bytes{mountpoint="/data/wal"}) > 0.85
        labels:
          severity: warning
        annotations:
          summary: "WAL disk space usage is high"
```

(Alert on generic node/disk metrics for the WAL mount; SignalDB does not currently expose WAL metrics in Prometheus format.)

## Backup and Recovery

### Backup Strategy

1. **Stop Service**: Gracefully stop the service to flush WAL
2. **Backup WAL**: Copy WAL directory to backup location
3. **Backup Metadata**: Include checkpoint files
4. **Restart Service**: Resume normal operations

### Recovery Process

1. **Restore WAL**: Copy WAL files to correct location
2. **Set Permissions**: Ensure proper ownership and permissions
3. **Start Service**: Service will automatically recover from WAL

## Troubleshooting

### Common Issues

#### "WAL directory not writable"

```bash
# Check permissions
ls -la /data/wal/
sudo chown -R signaldb:signaldb /data/wal/
sudo chmod 755 /data/wal/
```

#### "High WAL disk usage"

```bash
# Check segment count
find /data/wal -name 'wal-*.log' | wc -l

# Check if writer is processing WAL entries
tail -f /var/log/signaldb/writer.log | grep "WAL"
```

#### "WAL segment corruption"

```bash
# Service will log corruption and skip bad segments
tail -f /var/log/signaldb/acceptor.log | grep -i "corrupt"

# Manual segment inspection (if needed)
hexdump -C /data/wal/acme/production/traces/wal-0000000000.log | head
```

### Debug Commands

```bash
# Enable WAL debug logging
export RUST_LOG=common::wal=debug

# Check WAL directory structure
tree /data/wal/

# Monitor real-time WAL activity
tail -f /var/log/signaldb/*.log | grep WAL

# Monitor WAL disk usage
watch 'du -sh /data/wal/*'
```

### Recovery Procedures

#### Disaster Recovery

1. **Stop affected services**:

   ```bash
   kubectl scale deployment signaldb-acceptor --replicas=0
   kubectl scale deployment signaldb-writer --replicas=0
   ```

2. **Restore WAL data**:

   ```bash
   tar -xzf wal-backup-20240315.tar.gz -C /data/
   ```

3. **Restart services**:

   ```bash
   kubectl scale deployment signaldb-acceptor --replicas=2
   kubectl scale deployment signaldb-writer --replicas=2
   ```

4. **Verify recovery**:
   ```bash
   # Check logs for WAL replay messages
   kubectl logs -f deployment/signaldb-acceptor | grep "WAL replay"
   ```

## Best Practices

### Production Deployments

1. **Use persistent storage** with appropriate IOPS (>3000 IOPS recommended)
2. **Monitor WAL metrics** to detect processing delays
3. **Set up automated backups** for WAL directories
4. **Test recovery procedures** regularly
5. **Size WAL storage** for 2-3x peak ingestion rates
6. **Use fast storage** (NVMe SSD) for WAL directories
7. **Separate WAL and data storage** to avoid I/O contention

### Security

1. **Encrypt WAL directories** at rest using filesystem encryption
2. **Restrict access** to WAL directories (600/700 permissions)
3. **Monitor access** to WAL files in security logs
4. **Use dedicated service accounts** for WAL access

### Operational Excellence

1. **Implement comprehensive monitoring** with Prometheus metrics
2. **Set up alerting** for WAL health and performance
3. **Document recovery procedures** and train operations team
4. **Regular disaster recovery testing** with WAL restore
5. **Capacity planning** based on ingestion patterns
6. **Performance benchmarking** under realistic loads

### Storage Sizing Guidelines

**Development**: 100MB - 1GB (default local directories)
**Staging**: 1GB - 10GB (moderate ingestion rates)
**Production**: 10GB - 100GB+ (depends on ingestion rate and retention)
**High Volume**: 100GB+ with NVMe storage for optimal performance

**Calculation Formula**:

```
WAL Size ≈ Ingestion Rate × Flush Interval × Safety Factor

Example:
- 1000 spans/sec × 100 bytes/span × 30 sec flush = 3MB/flush
- With 3x safety factor: 9MB WAL storage per flush cycle
- Daily retention: 9MB × 2880 flushes = ~25GB
```
