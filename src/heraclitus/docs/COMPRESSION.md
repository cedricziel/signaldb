# Kafka Message Compression in Heraclitus

## Overview

Heraclitus fully supports Kafka message compression for all compression codecs specified in the Kafka protocol. Compression is automatically handled by the `kafka-protocol` Rust crate, which provides built-in support for encoding and decoding compressed message batches.

## Supported Compression Codecs

Heraclitus supports all standard Kafka compression codecs:

| Codec | ID | Status | Feature Flag |
|-------|----|---------| ------------|
| None | 0 | ✅ Supported | (none) |
| Gzip | 1 | ✅ Supported | `gzip` |
| Snappy | 2 | ✅ Supported | `snappy` |
| Lz4 | 3 | ✅ Supported | `lz4` |
| Zstd | 4 | ✅ Supported | `zstd` |

All compression features are enabled in `src/heraclitus/Cargo.toml`:

```toml
kafka-protocol = { version = "0.16", features = [
    "messages_enums",
    "broker",
    "gzip",
    "zstd",
    "snappy",
    "lz4",
] }
```

## How Compression Works

### Producer Path (Produce Request)

When a Kafka producer sends compressed messages to Heraclitus:

1. The producer compresses the record batch using one of the supported codecs
2. Heraclitus receives the ProduceRequest with compressed records
3. The `RecordBatchDecoder::decode()` function automatically detects the compression type from the batch attributes
4. The kafka-protocol library automatically decompresses the records using the appropriate codec
5. Decompressed messages are stored in Heraclitus

**Implementation**: `src/heraclitus/src/protocol_v2/connection_handler.rs:517`

```rust
let decode_result = RecordBatchDecoder::decode(&mut records_buf);
// RecordBatchDecoder automatically handles decompression based on attributes
```

### Consumer Path (Fetch Response)

When a Kafka consumer requests messages from Heraclitus:

1. Heraclitus retrieves stored messages
2. Messages are encoded into a record batch with optional compression
3. The `RecordBatchEncoder::encode()` function automatically compresses if compression is configured
4. The kafka-protocol library handles all compression details
5. Compressed batch is sent to the consumer

**Implementation**: `src/heraclitus/src/protocol_v2/connection_handler.rs:660-672`

```rust
let compression = if self.compression_config.enable_fetch_compression {
    get_compression_from_config(&self.compression_config.algorithm)
} else {
    Compression::None
};

let encode_options = RecordEncodeOptions {
    version: 2,
    compression,
};

RecordBatchEncoder::encode(&mut buf, records.iter(), &encode_options)?;
// RecordBatchEncoder automatically handles compression based on options
```

## Configuration

Heraclitus supports configuration for fetch response compression through the `CompressionConfig` struct:

```rust
pub struct CompressionConfig {
    pub algorithm: String,              // "none", "gzip", "snappy", "lz4", "zstd"
    pub level: i32,                     // Compression level
    pub enable_producer_compression: bool,
    pub enable_fetch_compression: bool,
}
```

### Configuration Example

```toml
[compression]
algorithm = "zstd"                # Compression algorithm for fetch responses
level = 3                         # Compression level (codec-specific)
enable_producer_compression = false  # Currently not used (producers choose compression)
enable_fetch_compression = true   # Enable compression for fetch responses
```

## Technical Details

### Automatic Compression Detection

The kafka-protocol library automatically detects compression from the record batch attributes field:

- **Bits 0-2**: Compression codec (0-4)
- **Bit 3**: Timestamp type
- **Bits 4-15**: Reserved

### Compression Implementation

All compression/decompression is handled by the kafka-protocol crate's built-in support:

**Decompression** (Produce requests):
- Location: `kafka-protocol-0.16.0/src/records.rs:592-619`
- The decoder automatically matches on the compression type and applies the correct decompressor
- Supports: None, Gzip, Snappy, Lz4, Zstd

**Compression** (Fetch responses):
- Location: `kafka-protocol-0.16.0/src/records.rs:352-380`
- The encoder automatically matches on the compression type and applies the correct compressor
- Supports: None, Gzip, Snappy, Lz4, Zstd

### Zero-Copy Design

The kafka-protocol library uses a zero-copy design with the `bytes::Bytes` and `bytes::BytesMut` types, ensuring efficient compression and decompression without unnecessary data copies.

## Testing

### Unit Tests

The kafka-protocol crate includes comprehensive unit tests for all compression codecs. Since Heraclitus relies entirely on this library's compression implementation, these tests provide coverage.

### Integration Tests

Integration tests with rdkafka clients verify that compressed messages round-trip correctly:

- `tests/rdkafka/compression.rs`: Tests each compression codec with real rdkafka producers/consumers
- Tests verify that messages compressed by rdkafka can be received and decompressed by Heraclitus
- Tests verify that messages can be re-compressed by Heraclitus for fetch responses

## Performance Considerations

### Compression Trade-offs

| Codec | Speed | Ratio | Best For |
|-------|-------|-------|----------|
| None | Fastest | 1:1 | Already compressed data, low latency requirements |
| Snappy | Fast | 2-3:1 | General purpose, balanced performance |
| Lz4 | Fast | 2-3:1 | Low latency with moderate compression |
| Gzip | Medium | 3-5:1 | Better compression, moderate CPU |
| Zstd | Medium | 3-6:1 | Best compression ratio, modern codec |

### When to Use Compression

**Enable compression when:**
- Network bandwidth is limited
- Messages are highly compressible (text, JSON, logs)
- Storage costs are a concern
- Network latency > compression overhead

**Disable compression when:**
- Messages are already compressed (images, video)
- Ultra-low latency is required
- CPU resources are constrained
- Messages are very small (<1KB)

## Troubleshooting

### Common Issues

**Problem**: Compression test failures
**Possible Causes**:
- Missing cargo features (verify `kafka-protocol` features in Cargo.toml)
- Infrastructure issues (MinIO, port conflicts)
- Version mismatches between producer and Heraclitus

**Problem**: Poor compression ratios
**Solution**:
- Ensure message batching is enabled
- Increase batch size for better compression
- Choose appropriate codec for your data type

### Verification

To verify compression is working:

1. Enable debug logging: `RUST_LOG=debug`
2. Check encoded batch sizes in logs
3. Compare network traffic with/without compression
4. Verify attributes field in wire protocol captures

## References

- [Kafka Protocol Documentation](https://kafka.apache.org/protocol)
- [kafka-protocol Rust Crate](https://docs.rs/kafka-protocol/)
- [Kafka Record Batch Format](https://kafka.apache.org/documentation/#recordbatch)

## Summary

✅ Compression is **fully implemented** in Heraclitus through the kafka-protocol crate
✅ All Kafka compression codecs (None, Gzip, Snappy, Lz4, Zstd) are supported
✅ Automatic compression/decompression happens transparently
✅ No additional code changes needed for compression support

The kafka-protocol library handles all compression details, providing a robust and well-tested implementation that is fully compatible with the Kafka wire protocol.
