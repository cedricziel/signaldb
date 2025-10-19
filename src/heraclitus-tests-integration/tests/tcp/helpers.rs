//! Helper functions for TCP-level Kafka protocol testing
//!
//! This module provides reusable functions for constructing and sending
//! Kafka protocol requests using the kafka-protocol crate primitives.

use anyhow::Result;
use bytes::{BufMut, BytesMut};
use kafka_protocol::messages::{
    ApiVersionsRequest, BrokerId, FetchRequest, GroupId, HeartbeatRequest, JoinGroupRequest,
    LeaveGroupRequest, ListOffsetsRequest, MetadataRequest, OffsetCommitRequest,
    OffsetFetchRequest, ProduceRequest, RequestHeader, SyncGroupRequest, TopicName,
};
use kafka_protocol::protocol::{Encodable, StrBytes, encode_request_header_into_buffer};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Send an ApiVersions request (v0)
pub async fn send_api_versions_request(
    stream: &mut TcpStream,
    correlation_id: i32,
) -> Result<Vec<u8>> {
    let header = RequestHeader::default()
        .with_request_api_key(18) // ApiVersions
        .with_request_api_version(0)
        .with_correlation_id(correlation_id)
        .with_client_id(None);

    let request = ApiVersionsRequest::default();

    send_request(stream, &header, &request, 0).await
}

/// Send an ApiVersions request (v3) with client info
pub async fn send_api_versions_request_v3(
    stream: &mut TcpStream,
    correlation_id: i32,
    client_name: &str,
    client_version: &str,
) -> Result<Vec<u8>> {
    let header = RequestHeader::default()
        .with_request_api_key(18) // ApiVersions
        .with_request_api_version(3)
        .with_correlation_id(correlation_id)
        .with_client_id(None);

    let request = ApiVersionsRequest::default()
        .with_client_software_name(StrBytes::from_string(client_name.to_string()))
        .with_client_software_version(StrBytes::from_string(client_version.to_string()));

    send_request(stream, &header, &request, 3).await
}

/// Send a Metadata request
pub async fn send_metadata_request(
    stream: &mut TcpStream,
    correlation_id: i32,
    topics: Option<Vec<String>>,
    version: i16,
) -> Result<Vec<u8>> {
    use kafka_protocol::messages::metadata_request::MetadataRequestTopic;

    let header = RequestHeader::default()
        .with_request_api_key(3) // Metadata
        .with_request_api_version(version)
        .with_correlation_id(correlation_id)
        .with_client_id(None);

    let request_topics = topics.map(|topic_list| {
        topic_list
            .into_iter()
            .map(|topic| {
                MetadataRequestTopic::default()
                    .with_name(Some(TopicName(StrBytes::from_string(topic))))
            })
            .collect()
    });

    let request = MetadataRequest::default().with_topics(request_topics);

    send_request(stream, &header, &request, version).await
}

/// Send a ListOffsets request
#[allow(dead_code)]
pub async fn send_list_offsets_request(
    stream: &mut TcpStream,
    correlation_id: i32,
    topics: Vec<(String, Vec<(i32, i64)>)>, // (topic_name, [(partition, timestamp)])
    version: i16,
) -> Result<Vec<u8>> {
    use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};

    let header = RequestHeader::default()
        .with_request_api_key(2) // ListOffsets
        .with_request_api_version(version)
        .with_correlation_id(correlation_id)
        .with_client_id(None);

    let request_topics = topics
        .into_iter()
        .map(|(topic_name, partitions)| {
            let request_partitions = partitions
                .into_iter()
                .map(|(partition_index, timestamp)| {
                    ListOffsetsPartition::default()
                        .with_partition_index(partition_index)
                        .with_timestamp(timestamp)
                })
                .collect();

            ListOffsetsTopic::default()
                .with_name(TopicName(StrBytes::from_string(topic_name)))
                .with_partitions(request_partitions)
        })
        .collect();

    let request = ListOffsetsRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_topics(request_topics);

    send_request(stream, &header, &request, version).await
}

/// Send a Produce request
#[allow(dead_code)]
pub async fn send_produce_request(
    stream: &mut TcpStream,
    correlation_id: i32,
    topic: &str,
    partition: i32,
    records: Vec<u8>,
    version: i16,
) -> Result<Vec<u8>> {
    use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};

    let header = RequestHeader::default()
        .with_request_api_key(0) // Produce
        .with_request_api_version(version)
        .with_correlation_id(correlation_id)
        .with_client_id(None);

    let partition_data = PartitionProduceData::default()
        .with_index(partition)
        .with_records(Some(records.into()));

    let topic_data = TopicProduceData::default()
        .with_name(TopicName(StrBytes::from_string(topic.to_string())))
        .with_partition_data(vec![partition_data]);

    let request = ProduceRequest::default()
        .with_acks(-1)
        .with_timeout_ms(5000)
        .with_topic_data(vec![topic_data]);

    send_request(stream, &header, &request, version).await
}

/// Send a Fetch request
#[allow(dead_code)]
pub async fn send_fetch_request(
    stream: &mut TcpStream,
    correlation_id: i32,
    topic: &str,
    partition: i32,
    fetch_offset: i64,
    version: i16,
) -> Result<Vec<u8>> {
    use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};

    let header = RequestHeader::default()
        .with_request_api_key(1) // Fetch
        .with_request_api_version(version)
        .with_correlation_id(correlation_id)
        .with_client_id(None);

    let partition_data = FetchPartition::default()
        .with_partition(partition)
        .with_fetch_offset(fetch_offset)
        .with_partition_max_bytes(1024 * 1024);

    let topic_data = FetchTopic::default()
        .with_topic(TopicName(StrBytes::from_string(topic.to_string())))
        .with_partitions(vec![partition_data]);

    let request = FetchRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_max_wait_ms(500)
        .with_min_bytes(1)
        .with_topics(vec![topic_data]);

    send_request(stream, &header, &request, version).await
}

/// Send a JoinGroup request
#[allow(dead_code)]
pub async fn send_join_group_request(
    stream: &mut TcpStream,
    correlation_id: i32,
    group_id: &str,
    member_id: &str,
    protocol_type: &str,
    version: i16,
) -> Result<Vec<u8>> {
    let header = RequestHeader::default()
        .with_request_api_key(11) // JoinGroup
        .with_request_api_version(version)
        .with_correlation_id(correlation_id)
        .with_client_id(None);

    let request = JoinGroupRequest::default()
        .with_group_id(GroupId(StrBytes::from_string(group_id.to_string())))
        .with_member_id(StrBytes::from_string(member_id.to_string()))
        .with_protocol_type(StrBytes::from_string(protocol_type.to_string()))
        .with_session_timeout_ms(10000)
        .with_rebalance_timeout_ms(10000);

    send_request(stream, &header, &request, version).await
}

/// Send a SyncGroup request
#[allow(dead_code)]
pub async fn send_sync_group_request(
    stream: &mut TcpStream,
    correlation_id: i32,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
    version: i16,
) -> Result<Vec<u8>> {
    let header = RequestHeader::default()
        .with_request_api_key(14) // SyncGroup
        .with_request_api_version(version)
        .with_correlation_id(correlation_id)
        .with_client_id(None);

    let request = SyncGroupRequest::default()
        .with_group_id(GroupId(StrBytes::from_string(group_id.to_string())))
        .with_generation_id(generation_id)
        .with_member_id(StrBytes::from_string(member_id.to_string()));

    send_request(stream, &header, &request, version).await
}

/// Send a Heartbeat request
#[allow(dead_code)]
pub async fn send_heartbeat_request(
    stream: &mut TcpStream,
    correlation_id: i32,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
    version: i16,
) -> Result<Vec<u8>> {
    let header = RequestHeader::default()
        .with_request_api_key(12) // Heartbeat
        .with_request_api_version(version)
        .with_correlation_id(correlation_id)
        .with_client_id(None);

    let request = HeartbeatRequest::default()
        .with_group_id(GroupId(StrBytes::from_string(group_id.to_string())))
        .with_generation_id(generation_id)
        .with_member_id(StrBytes::from_string(member_id.to_string()));

    send_request(stream, &header, &request, version).await
}

/// Send an OffsetCommit request
#[allow(dead_code)]
pub async fn send_offset_commit_request(
    stream: &mut TcpStream,
    correlation_id: i32,
    group_id: &str,
    topics: Vec<(String, Vec<(i32, i64)>)>, // (topic_name, [(partition, offset)])
    version: i16,
) -> Result<Vec<u8>> {
    use kafka_protocol::messages::offset_commit_request::{
        OffsetCommitRequestPartition, OffsetCommitRequestTopic,
    };

    let header = RequestHeader::default()
        .with_request_api_key(8) // OffsetCommit
        .with_request_api_version(version)
        .with_correlation_id(correlation_id)
        .with_client_id(None);

    let request_topics = topics
        .into_iter()
        .map(|(topic_name, partitions)| {
            let request_partitions = partitions
                .into_iter()
                .map(|(partition_index, committed_offset)| {
                    OffsetCommitRequestPartition::default()
                        .with_partition_index(partition_index)
                        .with_committed_offset(committed_offset)
                })
                .collect();

            OffsetCommitRequestTopic::default()
                .with_name(TopicName(StrBytes::from_string(topic_name)))
                .with_partitions(request_partitions)
        })
        .collect();

    let request = OffsetCommitRequest::default()
        .with_group_id(GroupId(StrBytes::from_string(group_id.to_string())))
        .with_topics(request_topics);

    send_request(stream, &header, &request, version).await
}

/// Send an OffsetFetch request
#[allow(dead_code)]
pub async fn send_offset_fetch_request(
    stream: &mut TcpStream,
    correlation_id: i32,
    group_id: &str,
    topics: Option<Vec<(String, Vec<i32>)>>, // (topic_name, [partition_indexes])
    version: i16,
) -> Result<Vec<u8>> {
    use kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic;

    let header = RequestHeader::default()
        .with_request_api_key(9) // OffsetFetch
        .with_request_api_version(version)
        .with_correlation_id(correlation_id)
        .with_client_id(None);

    let request_topics = topics.map(|topic_list| {
        topic_list
            .into_iter()
            .map(|(topic_name, partition_indexes)| {
                OffsetFetchRequestTopic::default()
                    .with_name(TopicName(StrBytes::from_string(topic_name)))
                    .with_partition_indexes(partition_indexes)
            })
            .collect()
    });

    let request = OffsetFetchRequest::default()
        .with_group_id(GroupId(StrBytes::from_string(group_id.to_string())))
        .with_topics(request_topics);

    send_request(stream, &header, &request, version).await
}

/// Send a LeaveGroup request
#[allow(dead_code)]
pub async fn send_leave_group_request(
    stream: &mut TcpStream,
    correlation_id: i32,
    group_id: &str,
    member_id: &str,
    version: i16,
) -> Result<Vec<u8>> {
    let header = RequestHeader::default()
        .with_request_api_key(13) // LeaveGroup
        .with_request_api_version(version)
        .with_correlation_id(correlation_id)
        .with_client_id(None);

    let request = LeaveGroupRequest::default()
        .with_group_id(GroupId(StrBytes::from_string(group_id.to_string())))
        .with_member_id(StrBytes::from_string(member_id.to_string()));

    send_request(stream, &header, &request, version).await
}

/// Generic helper to send a request and read response
async fn send_request<R: Encodable>(
    stream: &mut TcpStream,
    header: &RequestHeader,
    request: &R,
    version: i16,
) -> Result<Vec<u8>> {
    // Encode to buffer
    let mut request_buf = BytesMut::new();
    encode_request_header_into_buffer(&mut request_buf, header)?;
    request.encode(&mut request_buf, version)?;

    // Send request with length prefix
    let mut frame = BytesMut::new();
    frame.put_i32(request_buf.len() as i32);
    frame.extend_from_slice(&request_buf);

    stream.write_all(&frame).await?;
    stream.flush().await?;

    // Read response
    read_response(stream).await
}

/// Read a Kafka protocol response from a stream
pub async fn read_response(stream: &mut TcpStream) -> Result<Vec<u8>> {
    // Read length prefix
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = i32::from_be_bytes(len_buf) as usize;

    // Read response body
    let mut response = vec![0u8; len];
    stream.read_exact(&mut response).await?;

    Ok(response)
}
