# Kafka Protocol Specification

This document provides a comprehensive reference for the Apache Kafka wire protocol implementation in Heraclitus.

## Table of Contents

- [Overview](#overview)
- [Protocol Basics](#protocol-basics)
- [Network Layer](#network-layer)
- [Primitive Types](#primitive-types)
- [Message Format](#message-format)
- [API Keys](#api-keys)
- [Request/Response Headers](#requestresponse-headers)
- [Error Codes](#error-codes)
- [Record Batch Format](#record-batch-format)
- [Versioning](#versioning)
- [Security](#security)
- [API Reference](#api-reference)

## Overview

The Kafka protocol is a binary protocol that runs over TCP. The protocol defines a set of request/response message pairs, where each request type has a corresponding response type. All messages are size delimited and are made up of primitive types.

## Protocol Basics

### Key Characteristics

- **Binary Protocol**: All data is encoded in binary format for efficiency
- **TCP Transport**: Uses TCP for reliable, ordered delivery
- **Request/Response**: Follows a request-response pattern
- **Pipelined**: Supports request pipelining for better throughput
- **Versioned**: Each API has multiple versions for backward compatibility

### Design Principles

1. **Efficiency**: Minimal overhead in wire format
2. **Simplicity**: Straightforward encoding/decoding rules
3. **Evolution**: Support for adding new fields without breaking compatibility
4. **Performance**: Optimized for high-throughput scenarios

## Network Layer

### Connection Management

- Clients establish persistent TCP connections to brokers
- Connections can be reused for multiple requests
- Supports connection pooling for better resource utilization
- Implements keep-alive mechanisms to detect failed connections

### Message Framing

All Kafka messages follow this structure:

```
+------------------------+
| Length (4 bytes)       |
+------------------------+
| Request/Response Body  |
+------------------------+
```

The length field does not include itself and represents the size of the subsequent message body.

## Primitive Types

### Basic Types

| Type | Size | Description | Notes |
|------|------|-------------|-------|
| `BOOLEAN` | 1 byte | Boolean value | 0 = false, 1 = true |
| `INT8` | 1 byte | 8-bit signed integer | -128 to 127 |
| `INT16` | 2 bytes | 16-bit signed integer | Big-endian encoding |
| `INT32` | 4 bytes | 32-bit signed integer | Big-endian encoding |
| `INT64` | 8 bytes | 64-bit signed integer | Big-endian encoding |
| `UINT32` | 4 bytes | 32-bit unsigned integer | Big-endian encoding |
| `VARINT` | Variable | Variable-length signed int | ZigZag + VarInt encoding |
| `VARLONG` | Variable | Variable-length signed long | ZigZag + VarInt encoding |
| `UUID` | 16 bytes | 128-bit UUID | RFC 4122 format |
| `FLOAT64` | 8 bytes | 64-bit floating point | IEEE 754 double precision |

### Complex Types

| Type | Description | Encoding |
|------|-------------|----------|
| `STRING` | UTF-8 encoded string | Length prefix (INT16) + UTF-8 bytes |
| `COMPACT_STRING` | Compact UTF-8 string | UNSIGNED_VARINT length + UTF-8 bytes |
| `NULLABLE_STRING` | Optional string | Length -1 for null, otherwise STRING |
| `BYTES` | Raw byte array | Length prefix (INT32) + raw bytes |
| `COMPACT_BYTES` | Compact byte array | UNSIGNED_VARINT length + raw bytes |
| `NULLABLE_BYTES` | Optional byte array | Length -1 for null, otherwise BYTES |
| `RECORDS` | Kafka records | Version-specific record batch format |
| `ARRAY` | Array of elements | Length prefix + elements |
| `COMPACT_ARRAY` | Compact array | UNSIGNED_VARINT length + elements |

### Variable-Length Encoding

Kafka uses variable-length encoding for integers in compact message formats:

- **VarInt**: Variable-length encoding for integers
- **ZigZag**: Encoding scheme that maps signed integers to unsigned for efficient encoding of negative numbers

## Message Format

### Request Message Structure

```
RequestMessage => RequestHeader RequestBody
  RequestHeader => ApiKey ApiVersion CorrelationId ClientId
  RequestBody => (API-specific fields)
```

### Response Message Structure

```
ResponseMessage => ResponseHeader ResponseBody
  ResponseHeader => CorrelationId
  ResponseBody => (API-specific fields)
```

## API Keys

### Core APIs

| API | Key | Description |
|-----|-----|-------------|
| Produce | 0 | Send messages to topics |
| Fetch | 1 | Retrieve messages from topics |
| ListOffsets | 2 | Get offset information |
| Metadata | 3 | Get cluster metadata |
| LeaderAndIsr | 4 | Internal leader election |
| StopReplica | 5 | Internal replication control |
| UpdateMetadata | 6 | Internal metadata updates |
| ControlledShutdown | 7 | Graceful broker shutdown |
| OffsetCommit | 8 | Commit consumer offsets |
| OffsetFetch | 9 | Retrieve consumer offsets |

### Coordination APIs

| API | Key | Description |
|-----|-----|-------------|
| FindCoordinator | 10 | Find group/transaction coordinator |
| JoinGroup | 11 | Join consumer group |
| Heartbeat | 12 | Send heartbeat to coordinator |
| LeaveGroup | 13 | Leave consumer group |
| SyncGroup | 14 | Synchronize group state |
| DescribeGroups | 15 | Get group information |
| ListGroups | 16 | List all groups |
| SaslHandshake | 17 | Initiate SASL authentication |
| ApiVersions | 18 | Query supported API versions |
| CreateTopics | 19 | Create new topics |
| DeleteTopics | 20 | Delete existing topics |

### Transaction APIs

| API | Key | Description |
|-----|-----|-------------|
| InitProducerId | 22 | Initialize transactional producer |
| AddPartitionsToTxn | 24 | Add partitions to transaction |
| AddOffsetsToTxn | 25 | Add offsets to transaction |
| EndTxn | 26 | Commit/abort transaction |
| WriteTxnMarkers | 27 | Internal transaction markers |
| TxnOffsetCommit | 28 | Commit offsets within transaction |

### Admin APIs

| API | Key | Description |
|-----|-----|-------------|
| DescribeAcls | 29 | Describe access control lists |
| CreateAcls | 30 | Create access control lists |
| DeleteAcls | 31 | Delete access control lists |
| DescribeConfigs | 32 | Get configuration values |
| AlterConfigs | 33 | Update configuration values |
| AlterReplicaLogDirs | 34 | Move replica log directories |
| DescribeLogDirs | 35 | Get log directory information |
| SaslAuthenticate | 36 | Perform SASL authentication |
| CreatePartitions | 37 | Add partitions to topics |
| CreateDelegationToken | 38 | Create delegation token |
| RenewDelegationToken | 39 | Renew delegation token |
| ExpireDelegationToken | 40 | Expire delegation token |
| DescribeDelegationToken | 41 | Describe delegation tokens |
| DeleteGroups | 42 | Delete consumer groups |

## Request/Response Headers

### Request Header (v0)

```
RequestHeader => ApiKey ApiVersion CorrelationId
  ApiKey => INT16
  ApiVersion => INT16
  CorrelationId => INT32
```

### Request Header (v1)

```
RequestHeader => ApiKey ApiVersion CorrelationId ClientId
  ApiKey => INT16
  ApiVersion => INT16
  CorrelationId => INT32
  ClientId => NULLABLE_STRING
```

### Request Header (v2) - Flexible Version

```
RequestHeader => ApiKey ApiVersion CorrelationId ClientId TaggedFields
  ApiKey => INT16
  ApiVersion => INT16
  CorrelationId => INT32
  ClientId => NULLABLE_STRING
  TaggedFields => (see Tagged Fields section)
```

### Response Header (v0)

```
ResponseHeader => CorrelationId
  CorrelationId => INT32
```

### Response Header (v1) - Flexible Version

```
ResponseHeader => CorrelationId TaggedFields
  CorrelationId => INT32
  TaggedFields => (see Tagged Fields section)
```

## Error Codes

### Common Error Codes

| Code | Name | Description | Retriable |
|------|------|-------------|-----------|
| -1 | UNKNOWN_SERVER_ERROR | Unknown server error | No |
| 0 | NONE | No error | N/A |
| 1 | OFFSET_OUT_OF_RANGE | Requested offset is out of range | Yes |
| 2 | CORRUPT_MESSAGE | Message contents do not match CRC | Yes |
| 3 | UNKNOWN_TOPIC_OR_PARTITION | Topic or partition doesn't exist | Yes |
| 4 | INVALID_FETCH_SIZE | Invalid fetch size | No |
| 5 | LEADER_NOT_AVAILABLE | No leader for partition | Yes |
| 6 | NOT_LEADER_OR_FOLLOWER | Broker not leader/follower for partition | Yes |
| 7 | REQUEST_TIMED_OUT | Request exceeded timeout | Yes |
| 8 | BROKER_NOT_AVAILABLE | Broker not available | No |
| 9 | REPLICA_NOT_AVAILABLE | Replica not available | No |
| 10 | MESSAGE_TOO_LARGE | Message exceeds maximum size | No |
| 11 | STALE_CONTROLLER_EPOCH | Controller epoch is stale | No |
| 12 | OFFSET_METADATA_TOO_LARGE | Offset metadata too large | No |
| 13 | NETWORK_EXCEPTION | Network error | Yes |
| 14 | COORDINATOR_LOAD_IN_PROGRESS | Coordinator loading | Yes |
| 15 | COORDINATOR_NOT_AVAILABLE | Coordinator not available | Yes |
| 16 | NOT_COORDINATOR | Not coordinator for group | Yes |

### Authorization Error Codes

| Code | Name | Description |
|------|------|-------------|
| 29 | TOPIC_AUTHORIZATION_FAILED | Not authorized for topic operation |
| 30 | GROUP_AUTHORIZATION_FAILED | Not authorized for group operation |
| 31 | CLUSTER_AUTHORIZATION_FAILED | Not authorized for cluster operation |
| 32 | INVALID_TIMESTAMP | Invalid timestamp |
| 33 | UNSUPPORTED_SASL_MECHANISM | SASL mechanism not supported |
| 34 | ILLEGAL_SASL_STATE | Invalid SASL state |
| 35 | UNSUPPORTED_VERSION | API version not supported |

### Transaction Error Codes

| Code | Name | Description |
|------|------|-------------|
| 48 | INVALID_PRODUCER_ID_MAPPING | Producer ID mapping invalid |
| 49 | INVALID_TRANSACTION_TIMEOUT | Invalid transaction timeout |
| 50 | CONCURRENT_TRANSACTIONS | Concurrent transactions conflict |
| 51 | TRANSACTION_COORDINATOR_FENCED | Transaction coordinator fenced |
| 52 | TRANSACTIONAL_ID_AUTHORIZATION_FAILED | Transactional ID authorization failed |
| 53 | SECURITY_DISABLED | Security features disabled |
| 54 | OPERATION_NOT_ATTEMPTED | Operation not attempted |

## Record Batch Format

### Record Batch Structure (v2)

```
RecordBatch =>
  BaseOffset => INT64
  BatchLength => INT32
  PartitionLeaderEpoch => INT32
  Magic => INT8 (current magic value is 2)
  CRC => UINT32
  Attributes => INT16
    bit 0~2: compression type
      0: no compression
      1: gzip
      2: snappy
      3: lz4
      4: zstd
    bit 3: timestampType
      0: CreateTime
      1: LogAppendTime
    bit 4: isTransactional
    bit 5: isControlBatch
    bit 6: hasDeleteHorizonMs (for compaction)
    bit 7~15: unused
  LastOffsetDelta => INT32
  BaseTimestamp => INT64
  MaxTimestamp => INT64
  ProducerId => INT64
  ProducerEpoch => INT16
  BaseSequence => INT32
  Records => [Record]
```

### Record Structure (v2)

```
Record =>
  Length => VARINT
  Attributes => INT8
    bit 0~7: unused
  TimestampDelta => VARLONG
  OffsetDelta => VARINT
  KeyLen => VARINT
  Key => BYTES
  ValueLen => VARINT
  Value => BYTES
  Headers => [Header]
  
Header =>
  HeaderKeyLen => VARINT
  HeaderKey => STRING
  HeaderValueLen => VARINT
  HeaderValue => BYTES
```

## Versioning

### API Version Negotiation

1. Client sends ApiVersions request (v0) to discover supported versions
2. Broker responds with supported API versions
3. Client selects highest mutually supported version for each API
4. Subsequent requests use negotiated versions

### Version Compatibility Rules

- **Forward Compatibility**: New fields added at end with defaults
- **Backward Compatibility**: Old fields remain, marked deprecated
- **Breaking Changes**: Require new API version
- **Tagged Fields**: Allow optional fields in flexible versions

### Flexible Versions

APIs with flexible versions support:
- Compact encoding for better efficiency
- Tagged fields for forward compatibility
- More efficient string and array encoding

## Security

### Authentication Mechanisms

#### SASL/PLAIN
- Simple username/password authentication
- Should only be used with SSL/TLS

#### SASL/SCRAM
- Salted Challenge Response Authentication Mechanism
- Supports SCRAM-SHA-256 and SCRAM-SHA-512
- More secure than PLAIN

#### SASL/GSSAPI (Kerberos)
- Enterprise authentication via Kerberos
- Supports delegation tokens

#### SSL/TLS
- Transport layer security
- Mutual TLS for client authentication
- Certificate-based authentication

### Authorization

- ACL-based authorization
- Operations: Read, Write, Create, Delete, Alter, Describe, ClusterAction
- Resources: Topic, Group, Cluster, TransactionalId, DelegationToken

## API Reference

### Produce API (Key: 0)

Sends messages to Kafka topics.

#### Request Schema (v9)

```
ProduceRequest => TransactionalId Acks TimeoutMs TopicData
  TransactionalId => NULLABLE_STRING
  Acks => INT16
  TimeoutMs => INT32
  TopicData => [TopicProduceData]
    
TopicProduceData => Name PartitionData
  Name => STRING
  PartitionData => [PartitionProduceData]
    
PartitionProduceData => Index Records
  Index => INT32
  Records => RECORDS
```

#### Response Schema (v9)

```
ProduceResponse => Responses ThrottleTimeMs
  Responses => [TopicProduceResponse]
  ThrottleTimeMs => INT32
  
TopicProduceResponse => Name PartitionResponses
  Name => STRING
  PartitionResponses => [PartitionProduceResponse]
  
PartitionProduceResponse => Index ErrorCode BaseOffset LogAppendTimeMs LogStartOffset
  Index => INT32
  ErrorCode => INT16
  BaseOffset => INT64
  LogAppendTimeMs => INT64
  LogStartOffset => INT64
```

### Fetch API (Key: 1)

Retrieves messages from Kafka topics.

#### Request Schema (v13)

```
FetchRequest => ReplicaId MaxWaitMs MinBytes MaxBytes IsolationLevel 
               SessionId SessionEpoch Topics ForgottenTopicsData RackId
  ReplicaId => INT32
  MaxWaitMs => INT32
  MinBytes => INT32
  MaxBytes => INT32
  IsolationLevel => INT8
  SessionId => INT32
  SessionEpoch => INT32
  Topics => [FetchTopic]
  ForgottenTopicsData => [ForgottenTopic]
  RackId => STRING
  
FetchTopic => Topic Partitions
  Topic => STRING
  Partitions => [FetchPartition]
  
FetchPartition => Partition CurrentLeaderEpoch FetchOffset 
                  LastFetchedEpoch LogStartOffset PartitionMaxBytes
  Partition => INT32
  CurrentLeaderEpoch => INT32
  FetchOffset => INT64
  LastFetchedEpoch => INT32
  LogStartOffset => INT64
  PartitionMaxBytes => INT32
```

#### Response Schema (v13)

```
FetchResponse => ThrottleTimeMs ErrorCode SessionId Responses
  ThrottleTimeMs => INT32
  ErrorCode => INT16
  SessionId => INT32
  Responses => [FetchableTopicResponse]
  
FetchableTopicResponse => Topic Partitions
  Topic => STRING
  Partitions => [FetchablePartitionResponse]
  
FetchablePartitionResponse => Partition ErrorCode HighWatermark 
                              LastStableOffset LogStartOffset
                              AbortedTransactions PreferredReadReplica Records
  Partition => INT32
  ErrorCode => INT16
  HighWatermark => INT64
  LastStableOffset => INT64
  LogStartOffset => INT64
  AbortedTransactions => [AbortedTransaction]
  PreferredReadReplica => INT32
  Records => RECORDS
```

### Metadata API (Key: 3)

Retrieves cluster metadata including topics, partitions, and brokers.

#### Request Schema (v12)

```
MetadataRequest => Topics AllowAutoTopicCreation IncludeClusterAuthorizedOperations
                   IncludeTopicAuthorizedOperations
  Topics => [TopicName]
  AllowAutoTopicCreation => BOOLEAN
  IncludeClusterAuthorizedOperations => BOOLEAN
  IncludeTopicAuthorizedOperations => BOOLEAN
  
TopicName => Name
  Name => COMPACT_STRING
```

#### Response Schema (v12)

```
MetadataResponse => ThrottleTimeMs Brokers ClusterId ControllerId Topics
  ThrottleTimeMs => INT32
  Brokers => [Broker]
  ClusterId => NULLABLE_STRING
  ControllerId => INT32
  Topics => [TopicMetadata]
  
Broker => NodeId Host Port Rack
  NodeId => INT32
  Host => COMPACT_STRING
  Port => INT32
  Rack => COMPACT_NULLABLE_STRING
  
TopicMetadata => ErrorCode Name TopicId IsInternal Partitions
                 TopicAuthorizedOperations
  ErrorCode => INT16
  Name => COMPACT_STRING
  TopicId => UUID
  IsInternal => BOOLEAN
  Partitions => [PartitionMetadata]
  TopicAuthorizedOperations => INT32
  
PartitionMetadata => ErrorCode PartitionIndex LeaderId LeaderEpoch
                     ReplicaNodes IsrNodes OfflineReplicas
  ErrorCode => INT16
  PartitionIndex => INT32
  LeaderId => INT32
  LeaderEpoch => INT32
  ReplicaNodes => [INT32]
  IsrNodes => [INT32]
  OfflineReplicas => [INT32]
```

### FindCoordinator API (Key: 10)

Locates the coordinator (broker) for a consumer group or transaction.

#### Request Schema (v4)

```
FindCoordinatorRequest => Key KeyType
  Key => COMPACT_STRING
  KeyType => INT8 (0 = Group, 1 = Transaction)
```

#### Response Schema (v4)

```
FindCoordinatorResponse => ThrottleTimeMs ErrorCode ErrorMessage
                           NodeId Host Port
  ThrottleTimeMs => INT32
  ErrorCode => INT16
  ErrorMessage => COMPACT_NULLABLE_STRING
  NodeId => INT32
  Host => COMPACT_STRING
  Port => INT32
```

### JoinGroup API (Key: 11)

Joins a consumer group and participates in group coordination.

#### Request Schema (v9)

```
JoinGroupRequest => GroupId SessionTimeoutMs RebalanceTimeoutMs MemberId
                    GroupInstanceId ProtocolType Protocols Reason
  GroupId => COMPACT_STRING
  SessionTimeoutMs => INT32
  RebalanceTimeoutMs => INT32
  MemberId => COMPACT_STRING
  GroupInstanceId => COMPACT_NULLABLE_STRING
  ProtocolType => COMPACT_STRING
  Protocols => [Protocol]
  Reason => COMPACT_NULLABLE_STRING
  
Protocol => Name Metadata
  Name => COMPACT_STRING
  Metadata => COMPACT_BYTES
```

#### Response Schema (v9)

```
JoinGroupResponse => ThrottleTimeMs ErrorCode GenerationId ProtocolType
                     ProtocolName Leader SkipAssignment MemberId Members
  ThrottleTimeMs => INT32
  ErrorCode => INT16
  GenerationId => INT32
  ProtocolType => COMPACT_NULLABLE_STRING
  ProtocolName => COMPACT_NULLABLE_STRING
  Leader => COMPACT_STRING
  SkipAssignment => BOOLEAN
  MemberId => COMPACT_STRING
  Members => [Member]
  
Member => MemberId GroupInstanceId Metadata
  MemberId => COMPACT_STRING
  GroupInstanceId => COMPACT_NULLABLE_STRING
  Metadata => COMPACT_BYTES
```

### OffsetCommit API (Key: 8)

Commits consumer offsets for a consumer group.

#### Request Schema (v9)

```
OffsetCommitRequest => GroupId GenerationIdOrMemberEpoch MemberId
                       GroupInstanceId Topics
  GroupId => COMPACT_STRING
  GenerationIdOrMemberEpoch => INT32
  MemberId => COMPACT_STRING
  GroupInstanceId => COMPACT_NULLABLE_STRING
  Topics => [Topic]
  
Topic => Name Partitions
  Name => COMPACT_STRING
  Partitions => [Partition]
  
Partition => PartitionIndex CommittedOffset CommittedLeaderEpoch
             CommitTimestamp CommittedMetadata
  PartitionIndex => INT32
  CommittedOffset => INT64
  CommittedLeaderEpoch => INT32
  CommitTimestamp => INT64
  CommittedMetadata => COMPACT_NULLABLE_STRING
```

#### Response Schema (v9)

```
OffsetCommitResponse => ThrottleTimeMs Topics
  ThrottleTimeMs => INT32
  Topics => [Topic]
  
Topic => Name Partitions
  Name => COMPACT_STRING
  Partitions => [Partition]
  
Partition => PartitionIndex ErrorCode
  PartitionIndex => INT32
  ErrorCode => INT16
```

### ApiVersions API (Key: 18)

Queries the broker for supported API versions.

#### Request Schema (v3)

```
ApiVersionsRequest => ClientSoftwareName ClientSoftwareVersion
  ClientSoftwareName => COMPACT_STRING
  ClientSoftwareVersion => COMPACT_STRING
```

#### Response Schema (v3)

```
ApiVersionsResponse => ErrorCode ApiKeys ThrottleTimeMs
  ErrorCode => INT16
  ApiKeys => [ApiKey]
  ThrottleTimeMs => INT32
  
ApiKey => ApiKey MinVersion MaxVersion
  ApiKey => INT16
  MinVersion => INT16
  MaxVersion => INT16
```

## Implementation Notes

### Message Size Limits

- Default max message size: 1MB (configurable)
- Max request size: Typically 100MB (configurable)
- Consider batching for efficiency

### Compression

Supported compression types:
- None (0)
- GZIP (1)
- Snappy (2)
- LZ4 (3)
- ZSTD (4)

Compression is applied at the RecordBatch level.

### Idempotence

Producer idempotence requires:
- ProducerId and ProducerEpoch
- Sequence numbers per partition
- Broker deduplication logic

### Exactly-Once Semantics

Achieved through:
- Idempotent producers
- Transactional messaging
- Consumer read isolation levels

## Best Practices

### Client Implementation

1. **Connection Pooling**: Reuse connections for better performance
2. **Request Pipelining**: Send multiple requests without waiting
3. **Batch Operations**: Batch messages for better throughput
4. **Error Handling**: Implement exponential backoff for retriable errors
5. **Version Negotiation**: Always negotiate API versions on connect

### Performance Optimization

1. **Compression**: Use compression for large payloads
2. **Batching**: Batch multiple records in single requests
3. **Async Operations**: Use async I/O for better concurrency
4. **Zero-Copy**: Utilize zero-copy transfers where possible
5. **Connection Reuse**: Maintain persistent connections

### Security Considerations

1. **Always use TLS**: Encrypt data in transit
2. **Strong Authentication**: Use SCRAM or Kerberos over PLAIN
3. **Least Privilege**: Grant minimal required permissions
4. **Audit Logging**: Log all authentication/authorization events
5. **Regular Updates**: Keep up with security patches

## References

- [Apache Kafka Protocol Documentation](https://kafka.apache.org/protocol)
- [KIP-482: The Kafka Protocol is Evolving](https://cwiki.apache.org/confluence/display/KAFKA/KIP-482)
- [Kafka Improvement Proposals (KIPs)](https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Improvement+Proposals)

---

*This document is maintained as part of the Heraclitus Kafka protocol implementation for SignalDB.*