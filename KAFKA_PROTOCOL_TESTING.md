# Testing Kafka Protocol Implementation in Heraclitus

## Current Implementation Status

The Kafka wire protocol foundation has been implemented in Heraclitus with:

1. **Wire Protocol Handling**:
   - Frame reading/writing with 4-byte length prefix
   - Request header parsing (API key, version, correlation ID, client ID)
   - Response header writing
   - Connection handling with proper buffering

2. **Metadata API**:
   - Basic metadata request parsing
   - Response generation with broker information
   - Returns broker list (currently just the single Heraclitus instance)
   - Empty topic list (topics will be implemented with produce/fetch APIs)

## Testing the Implementation

### 1. Start Heraclitus

```bash
# Start the necessary infrastructure
docker compose up -d

# Run Heraclitus with Kafka protocol on port 9092
cargo run --bin heraclitus
```

### 2. Test with Python Script

A test script is provided to verify the Kafka protocol:

```bash
# Install kafka-python if not already installed
pip install kafka-python

# Run the test script
python test_kafka_protocol.py
```

This script will:
- Connect to Heraclitus on port 9092
- Send a metadata request
- Display discovered brokers and topics

### 3. Test with Kafka CLI Tools

You can also test with official Kafka tools:

```bash
# List topics (will show empty list)
kafka-topics.sh --bootstrap-server localhost:9092 --list

# Get metadata
kafka-metadata.sh --bootstrap-server localhost:9092
```

## Next Steps

The following APIs need to be implemented to have a functional Kafka-compatible system:

1. **Produce API**: Accept messages and forward to SignalDB writer
2. **Fetch API**: Retrieve messages from SignalDB querier
3. **Consumer Group APIs**: FindCoordinator, JoinGroup, Heartbeat, etc.
4. **Offset Management**: ListOffsets, OffsetCommit, OffsetFetch

## Architecture Notes

- Heraclitus acts as a stateless Kafka protocol adapter
- State is persisted in object storage (configured via `signaldb.toml`)
- Messages are stored in SignalDB's trace format
- Kafka topics map to SignalDB trace collections