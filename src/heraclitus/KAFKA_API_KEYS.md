# Kafka API Keys Reference

This document provides a comprehensive reference for all Kafka API keys, their versions, and implementation status in Heraclitus.

## Table of Contents

- [API Key Overview](#api-key-overview)
- [API Categories](#api-categories)
- [Complete API Key List](#complete-api-key-list)
- [API Version Matrix](#api-version-matrix)
- [Implementation Status](#implementation-status)
- [API Details](#api-details)

## API Key Overview

Each Kafka API is identified by a unique numeric key. These keys are used in the request header to identify which API is being invoked. The API key, combined with the API version, determines the exact request/response schema to use.

### Key Points

- API keys are 16-bit integers (0-65535)
- Each API can have multiple versions
- Version negotiation happens via ApiVersions request
- Newer versions typically add features while maintaining compatibility

## API Categories

### Core APIs (0-9)
Essential APIs for basic Kafka operations like producing and consuming messages.

### Group Coordination APIs (10-17)
APIs for consumer group management and coordination.

### Security APIs (17, 36, 38-41, 48-50, 61)
Authentication, authorization, and delegation token management.

### Administrative APIs (18-35, 37, 42-47, 56-60)
Cluster administration, topic management, and configuration.

### Transaction APIs (22, 24-28)
Transactional messaging support.

### Streams APIs (51-55)
APIs specific to Kafka Streams operations.

## Complete API Key List

| Key | API Name | Category | Description | Min Version | Max Version |
|-----|----------|----------|-------------|-------------|-------------|
| 0 | Produce | Core | Send messages to topics | 0 | 9 |
| 1 | Fetch | Core | Retrieve messages from topics | 0 | 13 |
| 2 | ListOffsets | Core | List offset information for partitions | 0 | 8 |
| 3 | Metadata | Core | Get cluster metadata | 0 | 12 |
| 4 | LeaderAndIsr | Internal | Update leader and ISR (broker-to-broker) | 0 | 7 |
| 5 | StopReplica | Internal | Stop replica (broker-to-broker) | 0 | 4 |
| 6 | UpdateMetadata | Internal | Update metadata (broker-to-broker) | 0 | 8 |
| 7 | ControlledShutdown | Internal | Controlled broker shutdown | 0 | 3 |
| 8 | OffsetCommit | Core | Commit consumer group offsets | 0 | 9 |
| 9 | OffsetFetch | Core | Fetch consumer group offsets | 0 | 8 |
| 10 | FindCoordinator | Group | Find coordinator for group/transaction | 0 | 4 |
| 11 | JoinGroup | Group | Join consumer group | 0 | 9 |
| 12 | Heartbeat | Group | Send heartbeat to group coordinator | 0 | 4 |
| 13 | LeaveGroup | Group | Leave consumer group | 0 | 5 |
| 14 | SyncGroup | Group | Synchronize group state | 0 | 5 |
| 15 | DescribeGroups | Group | Get consumer group information | 0 | 5 |
| 16 | ListGroups | Group | List all consumer groups | 0 | 4 |
| 17 | SaslHandshake | Security | Initiate SASL authentication | 0 | 1 |
| 18 | ApiVersions | Admin | Query supported API versions | 0 | 3 |
| 19 | CreateTopics | Admin | Create topics | 0 | 7 |
| 20 | DeleteTopics | Admin | Delete topics | 0 | 6 |
| 21 | DeleteRecords | Admin | Delete records up to offset | 0 | 2 |
| 22 | InitProducerId | Transaction | Initialize transactional producer | 0 | 4 |
| 23 | OffsetForLeaderEpoch | Core | Get offset for leader epoch | 0 | 4 |
| 24 | AddPartitionsToTxn | Transaction | Add partitions to transaction | 0 | 4 |
| 25 | AddOffsetsToTxn | Transaction | Add offsets to transaction | 0 | 3 |
| 26 | EndTxn | Transaction | End (commit/abort) transaction | 0 | 3 |
| 27 | WriteTxnMarkers | Internal | Write transaction markers (broker) | 0 | 1 |
| 28 | TxnOffsetCommit | Transaction | Commit offsets in transaction | 0 | 3 |
| 29 | DescribeAcls | Admin | Describe access control lists | 0 | 3 |
| 30 | CreateAcls | Admin | Create access control lists | 0 | 3 |
| 31 | DeleteAcls | Admin | Delete access control lists | 0 | 3 |
| 32 | DescribeConfigs | Admin | Get broker/topic configurations | 0 | 4 |
| 33 | AlterConfigs | Admin | Update broker/topic configurations | 0 | 2 |
| 34 | AlterReplicaLogDirs | Admin | Move replica between log directories | 0 | 2 |
| 35 | DescribeLogDirs | Admin | Get log directory information | 0 | 4 |
| 36 | SaslAuthenticate | Security | Perform SASL authentication | 0 | 2 |
| 37 | CreatePartitions | Admin | Add partitions to existing topics | 0 | 3 |
| 38 | CreateDelegationToken | Security | Create delegation token | 0 | 3 |
| 39 | RenewDelegationToken | Security | Renew delegation token | 0 | 2 |
| 40 | ExpireDelegationToken | Security | Expire delegation token | 0 | 2 |
| 41 | DescribeDelegationToken | Security | Get delegation token info | 0 | 3 |
| 42 | DeleteGroups | Admin | Delete consumer groups | 0 | 2 |
| 43 | ElectLeaders | Admin | Trigger leader election | 0 | 2 |
| 44 | IncrementalAlterConfigs | Admin | Incremental config updates | 0 | 1 |
| 45 | AlterPartitionReassignments | Admin | Alter partition reassignments | 0 | 0 |
| 46 | ListPartitionReassignments | Admin | List partition reassignments | 0 | 0 |
| 47 | OffsetDelete | Admin | Delete consumer group offsets | 0 | 0 |
| 48 | DescribeClientQuotas | Admin | Describe client quotas | 0 | 1 |
| 49 | AlterClientQuotas | Admin | Alter client quotas | 0 | 1 |
| 50 | DescribeUserScramCredentials | Security | Describe SCRAM credentials | 0 | 0 |
| 51 | AlterUserScramCredentials | Security | Alter SCRAM credentials | 0 | 0 |
| 52 | Vote | Raft | Raft vote request | 0 | 0 |
| 53 | BeginQuorumEpoch | Raft | Raft begin epoch | 0 | 0 |
| 54 | EndQuorumEpoch | Raft | Raft end epoch | 0 | 0 |
| 55 | DescribeQuorum | Raft | Describe Raft quorum | 0 | 0 |
| 56 | AlterPartition | Internal | Update partition state | 0 | 3 |
| 57 | UpdateFeatures | Admin | Update broker features | 0 | 1 |
| 58 | Envelope | Internal | Request envelope for forwarding | 0 | 0 |
| 59 | FetchSnapshot | Raft | Fetch Raft snapshot | 0 | 0 |
| 60 | DescribeCluster | Admin | Describe cluster metadata | 0 | 0 |
| 61 | DescribeProducers | Admin | Describe active producers | 0 | 0 |
| 62 | BrokerRegistration | Internal | Register broker with controller | 0 | 0 |
| 63 | BrokerHeartbeat | Internal | Broker heartbeat to controller | 0 | 0 |
| 64 | UnregisterBroker | Internal | Unregister broker from cluster | 0 | 0 |
| 65 | DescribeTransactions | Admin | Describe active transactions | 0 | 0 |
| 66 | ListTransactions | Admin | List all transactions | 0 | 0 |
| 67 | AllocateProducerIds | Internal | Allocate producer ID block | 0 | 0 |
| 68 | ConsumerGroupHeartbeat | Group | New consumer group protocol | 0 | 0 |

## API Version Matrix

### Flexible Versions

APIs that support flexible versions (tagged fields and compact encoding):

| API | Flexible Since Version |
|-----|----------------------|
| Produce | v9 |
| Fetch | v12 |
| ListOffsets | v6 |
| Metadata | v9 |
| OffsetCommit | v8 |
| OffsetFetch | v6 |
| FindCoordinator | v3 |
| JoinGroup | v6 |
| Heartbeat | v4 |
| LeaveGroup | v4 |
| SyncGroup | v4 |
| DescribeGroups | v5 |
| ListGroups | v3 |
| ApiVersions | v3 |
| CreateTopics | v5 |
| DeleteTopics | v4 |

## Implementation Status

### Implemented in Heraclitus

| API Key | API Name | Versions | Status | Notes |
|---------|----------|----------|--------|-------|
| 0 | Produce | v0-v9 | ✅ Complete | Full support including transactions |
| 1 | Fetch | v0-v13 | ✅ Complete | Includes session support |
| 2 | ListOffsets | v0-v7 | ✅ Complete | Timestamp-based queries supported |
| 3 | Metadata | v0-v12 | ✅ Complete | Topic auto-creation supported |
| 8 | OffsetCommit | v0-v8 | ✅ Complete | Supports new consumer protocol |
| 9 | OffsetFetch | v0-v8 | ✅ Complete | Group-wide offset fetch |
| 10 | FindCoordinator | v0-v4 | ✅ Complete | Group and transaction coordinators |
| 11 | JoinGroup | v0-v7 | ✅ Complete | Consumer group coordination |
| 12 | Heartbeat | v0-v4 | ✅ Complete | Keep-alive for group membership |
| 13 | LeaveGroup | v0-v4 | ✅ Complete | Graceful group departure |
| 14 | SyncGroup | v0-v5 | ✅ Complete | Partition assignment sync |
| 15 | DescribeGroups | v0-v5 | ✅ Complete | Group metadata retrieval |
| 16 | ListGroups | v0-v4 | ✅ Complete | Cluster-wide group listing |
| 17 | SaslHandshake | v0-v1 | ✅ Complete | SASL mechanism negotiation |
| 18 | ApiVersions | v0-v3 | ✅ Complete | Version negotiation |
| 19 | CreateTopics | v0-v7 | ✅ Complete | Topic creation with configs |
| 20 | DeleteTopics | v0-v6 | ✅ Complete | Topic deletion |
| 22 | InitProducerId | v0-v4 | ✅ Complete | Transactional producer init |
| 36 | SaslAuthenticate | v0-v2 | ✅ Complete | SASL authentication exchange |

### Partially Implemented

| API Key | API Name | Implemented Versions | Missing Versions | Notes |
|---------|----------|---------------------|------------------|-------|
| 32 | DescribeConfigs | v0-v2 | v3-v4 | Basic config retrieval works |
| 33 | AlterConfigs | v0-v1 | v2 | Config updates supported |

### Not Yet Implemented

| API Key | API Name | Priority | Notes |
|---------|----------|----------|-------|
| 4 | LeaderAndIsr | Low | Internal broker API |
| 5 | StopReplica | Low | Internal broker API |
| 6 | UpdateMetadata | Low | Internal broker API |
| 7 | ControlledShutdown | Low | Internal broker API |
| 21 | DeleteRecords | Medium | Admin functionality |
| 23 | OffsetForLeaderEpoch | Medium | Advanced consumer feature |
| 24-28 | Transaction APIs | High | Full transaction support |
| 29-31 | ACL APIs | Medium | Security features |
| 34-35 | LogDir APIs | Low | Advanced admin features |
| 37 | CreatePartitions | Medium | Dynamic partition expansion |
| 38-41 | Delegation Token APIs | Low | Enterprise security |
| 42 | DeleteGroups | Medium | Group management |
| 43+ | Newer APIs | Low | KRaft and advanced features |

## API Details

### Produce API (0)

**Purpose**: Send messages to Kafka topics

**Key Features**:
- Batch message sending
- Compression support
- Idempotent and transactional producing
- Acknowledgment levels (acks)

**Version History**:
- v0-v2: Basic producing
- v3: Added transactional support
- v4-v6: Incremental improvements
- v7: Added flexible versions preparation
- v8: Header support
- v9: Flexible versions (tagged fields)

### Fetch API (1)

**Purpose**: Retrieve messages from Kafka topics

**Key Features**:
- Efficient batch fetching
- Min/max bytes control
- Isolation level support
- Incremental fetch sessions
- Rack-aware fetching

**Version History**:
- v0-v3: Basic fetching
- v4: Added isolation level
- v5-v10: Incremental improvements
- v11: Added rack awareness
- v12: Flexible versions
- v13: Latest improvements

### Metadata API (3)

**Purpose**: Discover cluster topology and topic metadata

**Key Features**:
- Broker discovery
- Topic/partition information
- Leader/replica details
- Auto topic creation
- Cluster ID

**Version History**:
- v0: Basic metadata
- v1: Added controller ID
- v2-v5: Rack awareness and improvements
- v6-v8: Topic IDs and deletion info
- v9+: Flexible versions and auth operations

### FindCoordinator API (10)

**Purpose**: Locate the coordinator broker for a consumer group or transaction

**Key Features**:
- Group coordinator discovery
- Transaction coordinator discovery
- Automatic coordinator failover support

**Version History**:
- v0: Group coordinator only
- v1: Added transaction coordinator
- v2: Error message support
- v3+: Flexible versions

### JoinGroup API (11)

**Purpose**: Join a consumer group and participate in rebalancing

**Key Features**:
- Dynamic group membership
- Protocol negotiation
- Rebalance timeout
- Static membership (group.instance.id)
- Incremental rebalancing

**Version History**:
- v0-v4: Basic group joining
- v5: Added group.instance.id
- v6+: Flexible versions
- v7-v9: Incremental improvements

### ApiVersions API (18)

**Purpose**: Negotiate API versions between client and broker

**Key Features**:
- Lists all supported APIs
- Min/max version for each API
- Client software identification
- Feature discovery

**Version History**:
- v0: Basic version listing
- v1: Added throttle time
- v2: Client software name/version
- v3: Flexible versions

### CreateTopics API (19)

**Purpose**: Create new topics programmatically

**Key Features**:
- Batch topic creation
- Custom partition/replica assignment
- Topic configurations
- Validation support

**Version History**:
- v0: Basic topic creation
- v1: Added validation flag
- v2-v4: Config improvements
- v5+: Flexible versions
- v7: Latest features

### SaslHandshake API (17)

**Purpose**: Negotiate SASL authentication mechanism

**Key Features**:
- Lists available SASL mechanisms
- Mechanism selection
- Version compatibility

**Supported Mechanisms**:
- PLAIN
- SCRAM-SHA-256
- SCRAM-SHA-512
- GSSAPI (Kerberos)
- OAUTHBEARER

### InitProducerId API (22)

**Purpose**: Initialize a transactional or idempotent producer

**Key Features**:
- Producer ID allocation
- Epoch management
- Transaction initialization
- Idempotence support

**Version History**:
- v0-v1: Basic initialization
- v2: Added producer epoch bump
- v3: Flexible versions
- v4: Latest improvements

## Usage Guidelines

### Version Selection

1. **Always use ApiVersions**: Query supported versions before using APIs
2. **Use highest mutual version**: Select the highest version both client and broker support
3. **Handle version mismatches**: Gracefully fall back or error on unsupported versions

### Best Practices

1. **Batch Operations**: Use batch APIs (CreateTopics, DeleteTopics) for efficiency
2. **Session Management**: Maintain fetch sessions for better performance
3. **Error Handling**: Check API-specific error codes and retry appropriately
4. **Feature Detection**: Use ApiVersions to detect available features

### Implementation Checklist

When implementing a new API:

- [ ] Define request/response structures for all versions
- [ ] Implement version-specific serialization/deserialization
- [ ] Handle flexible versions (tagged fields) if applicable
- [ ] Add comprehensive error handling
- [ ] Implement retry logic for retriable errors
- [ ] Add metrics and logging
- [ ] Write unit tests for all versions
- [ ] Write integration tests
- [ ] Update this documentation

## References

- [Apache Kafka Protocol Documentation](https://kafka.apache.org/protocol)
- [Kafka Improvement Proposals (KIPs)](https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Improvement+Proposals)
- [Kafka Source Code](https://github.com/apache/kafka)

---

*This document is maintained as part of the Heraclitus Kafka protocol implementation for SignalDB.*