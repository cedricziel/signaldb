# WAL Persistence Configuration

The Write-Ahead Log (WAL) is critical for data durability in SignalDB. This document describes how to configure persistent storage for the WAL to ensure data survives container or pod restarts.

## Overview

SignalDB uses WAL (Write-Ahead Log) for durability across acceptor and writer services:

- **Writer Service**: Uses WAL at `WRITER_WAL_DIR` (default: `.wal/writer`)
- **Acceptor Service**: Uses WAL at `ACCEPTOR_WAL_DIR` (default: `.wal/acceptor`)

⚠️ **Warning**: The default WAL directories are local paths that **will not persist** across container or pod restarts, potentially causing data loss.

## Environment Variables

### WRITER_WAL_DIR
- **Description**: Directory path for the writer service WAL
- **Default**: `.wal/writer`
- **Required**: No (but recommended to override for production)
- **Example**: `/data/wal/writer`

### ACCEPTOR_WAL_DIR  
- **Description**: Directory path for the acceptor service WAL
- **Default**: `.wal/acceptor`
- **Required**: No (but recommended to override for production)
- **Example**: `/data/wal/acceptor`

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

## Directory Structure

The WAL uses a segment-based structure:

```
/data/wal/
├── segment-000001.wal    # Active segment
├── segment-000002.wal    # Completed segment
├── segment-000003.wal    # Completed segment
└── .checkpoint           # Checkpoint metadata
```

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

## Monitoring and Alerting

Monitor WAL health and storage usage:

### Key Metrics

- **WAL Disk Usage**: Monitor disk space consumption
- **WAL Segment Count**: Track number of segments
- **WAL Flush Latency**: Monitor write performance
- **WAL Errors**: Alert on WAL operation failures

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

1. **Permission Denied**
   - Solution: Check directory permissions and ownership
   - Command: `ls -la /data/wal`

2. **No Space Left on Device**
   - Solution: Increase storage allocation or clean up old segments
   - Monitor: Disk usage and WAL segment count

3. **WAL Corruption**
   - Solution: Service will skip corrupted segments and log errors
   - Recovery: May require data re-ingestion for affected timeframes

### Debug Commands

```bash
# Check WAL directory contents
ls -la /data/wal/

# Monitor WAL disk usage
df -h /data/wal

# Check WAL file permissions
find /data/wal -type f -exec ls -la {} \;

# Monitor WAL flush activity
tail -f /var/log/signaldb/writer.log | grep WAL
```

## Best Practices

1. **Use Dedicated Storage**: Separate WAL from data storage for performance
2. **Monitor Disk Space**: Set up alerts for disk usage
3. **Regular Backups**: Implement automated WAL backup procedures
4. **Test Recovery**: Regularly test WAL recovery procedures
5. **Performance Tuning**: Adjust WAL configuration based on workload
6. **Security**: Ensure WAL directories are not accessible to unauthorized users