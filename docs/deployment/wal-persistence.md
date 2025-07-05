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

- **Acceptor Service**: Uses WAL at `ACCEPTOR_WAL_DIR` (default: `.wal/acceptor`)
- **Writer Service**: Uses WAL at `WRITER_WAL_DIR` (default: `.wal/writer`)

⚠️ **Production Warning**: Default WAL directories use local paths that **will not persist** across container or pod restarts. Configure persistent volumes for production deployments.

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ACCEPTOR_WAL_DIR` | `.wal/acceptor` | WAL directory for acceptor service |
| `WRITER_WAL_DIR` | `.wal/writer` | WAL directory for writer service |
| `WAL_MAX_SEGMENT_SIZE` | `1048576` (1MB) | Maximum size per WAL segment |
| `WAL_MAX_BUFFER_ENTRIES` | `1000` | Buffer size before forced flush |
| `WAL_FLUSH_INTERVAL_SECS` | `10` | Automatic flush interval in seconds |

### TOML Configuration

```toml
[wal]
max_segment_size = 1048576      # 1MB segments
max_buffer_entries = 1000       # Buffer 1000 entries
flush_interval_secs = 10        # Flush every 10 seconds

[wal.acceptor]
wal_dir = "/data/wal/acceptor"   # Persistent storage path

[wal.writer] 
wal_dir = "/data/wal/writer"     # Persistent storage path
```

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

| Environment | Storage Class | Performance |
|-------------|---------------|-------------|
| Development | `standard` | Standard disk |
| Staging | `fast` | SSD |
| Production | `fast-ssd` | High-performance SSD |
| High-volume | `ultra-ssd` | NVMe with high IOPS |

## WAL Implementation Details

### Directory Structure

The WAL uses a segment-based structure:

```
/data/wal/
├── segments/
│   ├── 000001.wal        # Active segment
│   ├── 000002.wal        # Completed segment
│   └── 000003.wal        # Completed segment
└── metadata.json         # WAL metadata
```

### Data Flow with WAL

1. **Acceptor receives OTLP data**
2. **Data written to Acceptor WAL** (durability checkpoint)
3. **Client acknowledgment sent** (data is durable)
4. **Data forwarded to Writer via Flight** (Storage capability)
5. **Writer processes and stores to Parquet**
6. **WAL entries marked as processed** (cleanup eligible)

### Recovery Process

On service restart:
1. **WAL scan**: Identify unprocessed entries
2. **Automatic replay**: Reprocess unprocessed entries
3. **Resume normal operation**: Continue with new data

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

### For High Throughput

```bash
# Increase buffer size to reduce flush frequency
export WAL_MAX_BUFFER_ENTRIES=10000

# Use larger segments for efficiency  
export WAL_MAX_SEGMENT_SIZE=10485760  # 10MB

# Reduce flush interval for sustained writes
export WAL_FLUSH_INTERVAL_SECS=5
```

### For Low Latency

```bash
# Immediate flushing (minimal buffering)
export WAL_MAX_BUFFER_ENTRIES=1
export WAL_FLUSH_INTERVAL_SECS=1

# Smaller segments for faster rotation
export WAL_MAX_SEGMENT_SIZE=1048576   # 1MB
```

## Monitoring and Alerting

### Key Metrics

- **WAL Disk Usage**: Monitor disk space consumption
- **WAL Segment Count**: Track number of segments
- **WAL Flush Latency**: Monitor write performance
- **WAL Errors**: Alert on WAL operation failures
- **Unprocessed Entries**: Monitor processing lag

### Health Check Endpoints

```bash
# Check WAL status
curl http://acceptor:8080/health | jq '.wal'
curl http://writer:8080/health | jq '.wal'

# Example response
{
  "wal": {
    "unprocessed_entries": 0,
    "total_segments": 3,
    "disk_usage_bytes": 2097152,
    "last_flush": "2024-03-15T10:30:00Z"
  }
}
```

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
      
  - alert: WALFlushLatencyHigh
    expr: wal_flush_duration_seconds > 1.0
    labels:
      severity: warning
    annotations:
      summary: "WAL flush latency is high"
```

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
# Check for stuck processing
curl http://writer:8080/health | jq '.wal.unprocessed_entries'

# Check segment count
ls -la /data/wal/segments/ | wc -l

# Check if writer is processing WAL entries
tail -f /var/log/signaldb/writer.log | grep "WAL entry processed"
```

#### "WAL segment corruption"
```bash
# Service will log corruption and skip bad segments
tail -f /var/log/signaldb/acceptor.log | grep "WAL corruption"

# Manual segment inspection (if needed)
hexdump -C /data/wal/segments/000001.wal | head
```

### Debug Commands

```bash
# Enable WAL debug logging
export RUST_LOG=signaldb_common::wal=debug

# Check WAL directory structure
tree /data/wal/

# Monitor real-time WAL activity
tail -f /var/log/signaldb/*.log | grep WAL

# Check WAL metadata
cat /data/wal/metadata.json | jq

# Monitor WAL processing
watch 'curl -s http://acceptor:8080/health | jq .wal'
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
- 1000 spans/sec × 100 bytes/span × 10 sec flush = 1MB/flush
- With 3x safety factor: 3MB WAL storage per flush cycle
- Daily retention: 3MB × 8640 flushes = ~25GB
```