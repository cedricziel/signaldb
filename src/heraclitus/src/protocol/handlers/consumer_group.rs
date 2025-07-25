use super::{ApiHandler, HandlerContext};
use crate::{
    error::Result,
    protocol::{
        describe_groups::{DescribeGroupsRequest, DescribeGroupsResponse},
        find_coordinator::{FindCoordinatorRequest, FindCoordinatorResponse},
        heartbeat::{HeartbeatRequest, HeartbeatResponse},
        join_group::{JoinGroupMember, JoinGroupRequest, JoinGroupResponse},
        kafka_protocol::*,
        leave_group::{LeaveGroupMemberResponse, LeaveGroupRequest, LeaveGroupResponse},
        list_groups::{ListGroupsRequest, ListGroupsResponse, ListedGroup},
        offset_commit::{
            OffsetCommitRequest, OffsetCommitResponse, OffsetCommitResponsePartition,
            OffsetCommitResponseTopic,
        },
        offset_fetch::{OffsetFetchRequest, OffsetFetchResponse},
        request::KafkaRequest,
        sync_group::{SyncGroupRequest, SyncGroupResponse},
    },
};
use tracing::{debug, error, info};

pub struct FindCoordinatorHandler;

#[async_trait::async_trait]
impl ApiHandler for FindCoordinatorHandler {
    async fn handle(
        &self,
        request: &KafkaRequest,
        context: &mut HandlerContext,
    ) -> Result<Vec<u8>> {
        info!("Handling find coordinator request v{}", request.api_version);

        // Parse request
        let mut cursor = std::io::Cursor::new(&request.body[..]);
        let find_request = FindCoordinatorRequest::parse(&mut cursor, request.api_version)?;

        debug!(
            "FindCoordinator request: key={}, key_type={}",
            find_request.key, find_request.key_type
        );

        // For now, we're the coordinator for all groups
        let _coordinator_key = find_request.key.clone();

        // Build response
        let response = FindCoordinatorResponse {
            throttle_time_ms: 0,
            error_code: ERROR_NONE,
            error_message: None,
            node_id: 0,
            host: "localhost".to_string(),
            port: context.port as i32,
        };

        // Encode response
        response.encode(request.api_version)
    }

    fn api_key(&self) -> i16 {
        10 // FindCoordinator
    }

    fn supports_version(&self, version: i16) -> bool {
        (0..=4).contains(&version)
    }
}

pub struct JoinGroupHandler;

#[async_trait::async_trait]
impl ApiHandler for JoinGroupHandler {
    async fn handle(
        &self,
        request: &KafkaRequest,
        context: &mut HandlerContext,
    ) -> Result<Vec<u8>> {
        info!("Handling join group request v{}", request.api_version);

        // Parse request
        let mut cursor = std::io::Cursor::new(&request.body[..]);
        let join_request = JoinGroupRequest::parse(&mut cursor, request.api_version)?;

        info!(
            "JoinGroup request: group_id={}, member_id={}, protocol_type={}, protocols={:?}",
            join_request.group_id,
            join_request.member_id,
            join_request.protocol_type,
            join_request
                .protocols
                .iter()
                .map(|p| &p.name)
                .collect::<Vec<_>>()
        );

        // Generate member ID if needed
        let member_id = if join_request.member_id.is_empty() {
            format!(
                "consumer-{}-{}",
                join_request.group_id,
                uuid::Uuid::new_v4()
            )
        } else {
            join_request.member_id.clone()
        };

        // Get or create the consumer group
        let mut group_state = match context
            .state_manager
            .consumer_groups()
            .get_group(&join_request.group_id)
            .await?
        {
            Some(state) => state,
            None => {
                // Create new group
                crate::state::ConsumerGroupState {
                    group_id: join_request.group_id.clone(),
                    generation_id: 0,
                    protocol_type: join_request.protocol_type.clone(),
                    protocol: None,
                    leader: None,
                    members: std::collections::HashMap::new(),
                }
            }
        };

        // Add/update member
        let member = crate::state::ConsumerGroupMember {
            member_id: member_id.clone(),
            client_id: "heraclitus-client".to_string(), // Default client ID
            client_host: "127.0.0.1".to_string(),       // Default client host
            session_timeout_ms: join_request.session_timeout_ms,
            rebalance_timeout_ms: join_request.rebalance_timeout_ms,
            last_heartbeat_ms: chrono::Utc::now().timestamp_millis(),
        };

        group_state.members.insert(member_id.clone(), member);

        // Increment generation if this triggers a rebalance
        group_state.generation_id += 1;

        // Select leader (first member)
        if group_state.leader.is_none() {
            group_state.leader = Some(member_id.clone());
        }

        // Select protocol (for now, use the first one)
        if !join_request.protocols.is_empty() {
            group_state.protocol = Some(join_request.protocols[0].name.clone());
        }

        // Save group state
        context
            .state_manager
            .consumer_groups()
            .save_group(&group_state)
            .await?;

        let is_leader = group_state.leader.as_ref() == Some(&member_id);
        let protocol_name = group_state.protocol.clone();

        // Build response
        let members = if is_leader {
            // Leader gets list of all members
            join_request
                .protocols
                .iter()
                .map(|p| JoinGroupMember {
                    member_id: member_id.clone(),
                    group_instance_id: None,
                    metadata: p.metadata.clone(),
                })
                .collect()
        } else {
            vec![]
        };

        let response = JoinGroupResponse {
            error_code: ERROR_NONE,
            generation_id: group_state.generation_id,
            protocol_type: Some(group_state.protocol_type.clone()),
            protocol_name,
            leader: group_state.leader.clone().unwrap_or_default(),
            member_id,
            members,
            throttle_time_ms: 0,
        };

        // Encode response
        response.encode(request.api_version)
    }

    fn api_key(&self) -> i16 {
        11 // JoinGroup
    }

    fn supports_version(&self, version: i16) -> bool {
        (0..=7).contains(&version)
    }
}

pub struct SyncGroupHandler;

#[async_trait::async_trait]
impl ApiHandler for SyncGroupHandler {
    async fn handle(
        &self,
        request: &KafkaRequest,
        context: &mut HandlerContext,
    ) -> Result<Vec<u8>> {
        info!("Handling sync group request v{}", request.api_version);

        // Parse request
        let mut cursor = std::io::Cursor::new(&request.body[..]);
        let sync_request = SyncGroupRequest::parse(&mut cursor, request.api_version)?;

        info!(
            "SyncGroup request: group_id={}, member_id={}, generation_id={}, assignments={}",
            sync_request.group_id,
            sync_request.member_id,
            sync_request.generation_id,
            sync_request.group_assignments.len()
        );

        // Get the group state
        match context
            .state_manager
            .consumer_groups()
            .get_group(&sync_request.group_id)
            .await?
        {
            Some(group_state) => {
                // Verify generation ID
                if group_state.generation_id != sync_request.generation_id {
                    let response = SyncGroupResponse {
                        error_code: ERROR_ILLEGAL_GENERATION,
                        member_assignment: vec![],
                        throttle_time_ms: 0,
                    };
                    return response.encode(request.api_version);
                }

                // Get member assignment from the request if this member is included
                let member_assignment = sync_request
                    .group_assignments
                    .get(&sync_request.member_id)
                    .cloned()
                    .unwrap_or_else(Vec::new);

                let response = SyncGroupResponse {
                    error_code: ERROR_NONE,
                    member_assignment,
                    throttle_time_ms: 0,
                };
                return response.encode(request.api_version);
            }
            None => {
                let response = SyncGroupResponse {
                    error_code: ERROR_UNKNOWN_MEMBER_ID,
                    member_assignment: vec![],
                    throttle_time_ms: 0,
                };
                return response.encode(request.api_version);
            }
        }
    }

    fn api_key(&self) -> i16 {
        14 // SyncGroup
    }

    fn supports_version(&self, version: i16) -> bool {
        (0..=5).contains(&version)
    }
}

pub struct HeartbeatHandler;

#[async_trait::async_trait]
impl ApiHandler for HeartbeatHandler {
    async fn handle(
        &self,
        request: &KafkaRequest,
        context: &mut HandlerContext,
    ) -> Result<Vec<u8>> {
        info!("Handling heartbeat request v{}", request.api_version);

        // Parse request
        let mut cursor = std::io::Cursor::new(&request.body[..]);
        let heartbeat_request = HeartbeatRequest::parse(&mut cursor, request.api_version)?;

        debug!(
            "Heartbeat request: group_id={}, member_id={}, generation_id={}, instance_id={:?}",
            heartbeat_request.group_id,
            heartbeat_request.member_id,
            heartbeat_request.generation_id,
            heartbeat_request.group_instance_id
        );

        // Get the group state
        match context
            .state_manager
            .consumer_groups()
            .get_group(&heartbeat_request.group_id)
            .await?
        {
            Some(mut group_state) => {
                // Verify generation ID
                if group_state.generation_id != heartbeat_request.generation_id {
                    let response = HeartbeatResponse {
                        error_code: ERROR_ILLEGAL_GENERATION,
                        throttle_time_ms: 0,
                    };
                    return response.encode(request.api_version);
                }

                // Update member heartbeat
                if let Some(member) = group_state.members.get_mut(&heartbeat_request.member_id) {
                    member.last_heartbeat_ms = chrono::Utc::now().timestamp_millis();

                    // Save updated state
                    context
                        .state_manager
                        .consumer_groups()
                        .save_group(&group_state)
                        .await?;

                    let response = HeartbeatResponse {
                        error_code: ERROR_NONE,
                        throttle_time_ms: 0,
                    };
                    return response.encode(request.api_version);
                } else {
                    let response = HeartbeatResponse {
                        error_code: ERROR_UNKNOWN_MEMBER_ID,
                        throttle_time_ms: 0,
                    };
                    return response.encode(request.api_version);
                }
            }
            None => {
                let response = HeartbeatResponse {
                    error_code: ERROR_UNKNOWN_MEMBER_ID,
                    throttle_time_ms: 0,
                };
                return response.encode(request.api_version);
            }
        }
    }

    fn api_key(&self) -> i16 {
        12 // Heartbeat
    }

    fn supports_version(&self, version: i16) -> bool {
        (0..=4).contains(&version)
    }
}

pub struct LeaveGroupHandler;

#[async_trait::async_trait]
impl ApiHandler for LeaveGroupHandler {
    async fn handle(
        &self,
        request: &KafkaRequest,
        context: &mut HandlerContext,
    ) -> Result<Vec<u8>> {
        info!("Handling leave group request v{}", request.api_version);

        // Parse request
        let mut cursor = std::io::Cursor::new(&request.body[..]);
        let leave_request = LeaveGroupRequest::parse(&mut cursor, request.api_version)?;

        info!(
            "LeaveGroup request: group_id={}, member_id={}",
            leave_request.group_id, leave_request.member_id
        );

        // Get the group state
        match context
            .state_manager
            .consumer_groups()
            .get_group(&leave_request.group_id)
            .await?
        {
            Some(mut group_state) => {
                // Remove the member
                if group_state
                    .members
                    .remove(&leave_request.member_id)
                    .is_some()
                {
                    // If this was the leader, select a new one
                    if group_state.leader.as_ref() == Some(&leave_request.member_id) {
                        group_state.leader = group_state.members.keys().next().cloned();
                    }

                    // Save updated state
                    context
                        .state_manager
                        .consumer_groups()
                        .save_group(&group_state)
                        .await?;

                    info!(
                        "Member {} left group {}",
                        leave_request.member_id, leave_request.group_id
                    );
                    let response = LeaveGroupResponse {
                        error_code: ERROR_NONE,
                        throttle_time_ms: 0,
                        members: Some(vec![LeaveGroupMemberResponse {
                            member_id: leave_request.member_id.clone(),
                            group_instance_id: None,
                            error_code: ERROR_NONE,
                        }]),
                    };
                    return response.encode(request.api_version);
                } else {
                    let response = LeaveGroupResponse {
                        error_code: ERROR_UNKNOWN_MEMBER_ID,
                        throttle_time_ms: 0,
                        members: Some(vec![LeaveGroupMemberResponse {
                            member_id: leave_request.member_id.clone(),
                            group_instance_id: None,
                            error_code: ERROR_UNKNOWN_MEMBER_ID,
                        }]),
                    };
                    return response.encode(request.api_version);
                }
            }
            None => {
                let response = LeaveGroupResponse {
                    error_code: ERROR_UNKNOWN_MEMBER_ID,
                    throttle_time_ms: 0,
                    members: Some(vec![LeaveGroupMemberResponse {
                        member_id: leave_request.member_id.clone(),
                        group_instance_id: None,
                        error_code: ERROR_UNKNOWN_MEMBER_ID,
                    }]),
                };
                return response.encode(request.api_version);
            }
        }
    }

    fn api_key(&self) -> i16 {
        13 // LeaveGroup
    }

    fn supports_version(&self, version: i16) -> bool {
        (0..=4).contains(&version)
    }
}

pub struct OffsetCommitHandler;

#[async_trait::async_trait]
impl ApiHandler for OffsetCommitHandler {
    async fn handle(
        &self,
        request: &KafkaRequest,
        context: &mut HandlerContext,
    ) -> Result<Vec<u8>> {
        info!("Handling offset commit request v{}", request.api_version);

        // Parse request
        let mut cursor = std::io::Cursor::new(&request.body[..]);
        let offset_commit_request = OffsetCommitRequest::parse(&mut cursor, request.api_version)?;

        debug!(
            "OffsetCommit request: group_id={}, member_id={:?}, generation_id={:?}, topics={}",
            offset_commit_request.group_id,
            offset_commit_request.member_id,
            offset_commit_request.generation_id,
            offset_commit_request.topics.len()
        );

        // Prepare offsets to save
        let mut offsets_to_save = Vec::new();
        let mut topic_responses = Vec::new();

        for topic in &offset_commit_request.topics {
            let mut partition_responses = Vec::new();

            for partition in &topic.partitions {
                offsets_to_save.push((
                    topic.name.clone(),
                    partition.partition_index,
                    partition.committed_offset,
                    partition.metadata.clone(),
                ));

                partition_responses.push(OffsetCommitResponsePartition {
                    partition_index: partition.partition_index,
                    error_code: ERROR_NONE,
                });
            }

            topic_responses.push(OffsetCommitResponseTopic {
                name: topic.name.clone(),
                partitions: partition_responses,
            });
        }

        // Save all offsets
        if let Err(e) = context
            .state_manager
            .consumer_groups()
            .save_offsets(&offset_commit_request.group_id, &offsets_to_save)
            .await
        {
            error!("Failed to save offsets: {}", e);
            // Update error codes in response
            for topic in &mut topic_responses {
                for partition in &mut topic.partitions {
                    partition.error_code = ERROR_UNKNOWN;
                }
            }
        }

        let response = OffsetCommitResponse {
            throttle_time_ms: 0,
            topics: topic_responses,
        };

        // Encode response
        response.encode(request.api_version)
    }

    fn api_key(&self) -> i16 {
        8 // OffsetCommit
    }

    fn supports_version(&self, version: i16) -> bool {
        (0..=8).contains(&version)
    }
}

pub struct OffsetFetchHandler;

#[async_trait::async_trait]
impl ApiHandler for OffsetFetchHandler {
    async fn handle(
        &self,
        request: &KafkaRequest,
        context: &mut HandlerContext,
    ) -> Result<Vec<u8>> {
        info!("Handling offset fetch request v{}", request.api_version);

        // Parse request
        let mut cursor = std::io::Cursor::new(&request.body[..]);
        let offset_fetch_request = OffsetFetchRequest::parse(&mut cursor, request.api_version)?;

        debug!(
            "OffsetFetch request: group_id={}, topics={:?}",
            offset_fetch_request.group_id,
            offset_fetch_request.topics.as_ref().map(|t| t.len())
        );

        // Get all offsets for this group
        let offsets = context
            .state_manager
            .consumer_groups()
            .get_offsets(&offset_fetch_request.group_id)
            .await?;

        let mut topic_responses = Vec::new();

        match &offset_fetch_request.topics {
            Some(topics) => {
                // Specific topics requested
                for topic in topics {
                    let mut partition_responses = Vec::new();

                    for &partition_index in &topic.partition_indexes {
                        if let Some(offset) = offsets.get(&(topic.name.clone(), partition_index)) {
                            partition_responses.push(
                                crate::protocol::offset_fetch::OffsetFetchResponsePartition {
                                    partition_index,
                                    committed_offset: offset.offset,
                                    committed_leader_epoch: -1,
                                    metadata: offset.metadata.clone(),
                                    error_code: ERROR_NONE,
                                },
                            );
                        } else {
                            partition_responses.push(
                                crate::protocol::offset_fetch::OffsetFetchResponsePartition {
                                    partition_index,
                                    committed_offset: -1,
                                    committed_leader_epoch: -1,
                                    metadata: None,
                                    error_code: ERROR_NONE,
                                },
                            );
                        }
                    }

                    let topic_response = crate::protocol::offset_fetch::OffsetFetchResponseTopic {
                        name: topic.name.clone(),
                        partitions: partition_responses,
                    };
                    topic_responses.push(topic_response);
                }
            }
            None => {
                // All topics requested
                let mut topic_map = std::collections::HashMap::new();

                for ((topic, partition), offset) in offsets {
                    topic_map
                        .entry(topic.clone())
                        .or_insert_with(Vec::new)
                        .push(
                            crate::protocol::offset_fetch::OffsetFetchResponsePartition {
                                partition_index: partition,
                                committed_offset: offset.offset,
                                committed_leader_epoch: -1,
                                metadata: offset.metadata.clone(),
                                error_code: ERROR_NONE,
                            },
                        );
                }

                for (topic, partitions) in topic_map {
                    topic_responses.push(crate::protocol::offset_fetch::OffsetFetchResponseTopic {
                        name: topic,
                        partitions,
                    });
                }
            }
        }

        let response = OffsetFetchResponse {
            throttle_time_ms: 0,
            topics: topic_responses,
            error_code: ERROR_NONE,
        };

        // Encode response
        response.encode(request.api_version)
    }

    fn api_key(&self) -> i16 {
        9 // OffsetFetch
    }

    fn supports_version(&self, version: i16) -> bool {
        (0..=8).contains(&version)
    }
}

pub struct ListGroupsHandler;

#[async_trait::async_trait]
impl ApiHandler for ListGroupsHandler {
    async fn handle(
        &self,
        request: &KafkaRequest,
        context: &mut HandlerContext,
    ) -> Result<Vec<u8>> {
        info!("Handling list groups request v{}", request.api_version);

        // Parse request
        let mut cursor = std::io::Cursor::new(&request.body[..]);
        let _list_request = ListGroupsRequest::parse(&mut cursor, request.api_version)?;

        // List all consumer groups by scanning the metadata store
        let prefix = context.state_manager.object_store().clone();
        let layout = context.state_manager.layout().clone();
        let groups_prefix = layout.consumer_groups_prefix();

        let mut groups = Vec::new();

        // List all group directories
        let mut stream = prefix.list(Some(&object_store::path::Path::from(groups_prefix)));
        while let Some(result) = futures::StreamExt::next(&mut stream).await {
            match result {
                Ok(meta) => {
                    // Extract group ID from path
                    // Path format is: consumer_groups/{group_id}/state.json
                    let path_str = meta.location.as_ref();
                    if path_str.ends_with("/state.json") {
                        // Extract group_id from path
                        let parts: Vec<&str> = path_str.split('/').collect();
                        if parts.len() >= 3 && parts[parts.len() - 3] == "consumer_groups" {
                            let group_id = parts[parts.len() - 2];
                            // Try to load the group to get protocol type
                            if let Ok(Some(group_state)) = context
                                .state_manager
                                .consumer_groups()
                                .get_group(group_id)
                                .await
                            {
                                groups.push(ListedGroup {
                                    group_id: group_state.group_id,
                                    protocol_type: group_state.protocol_type,
                                    group_state: "Stable".to_string(),
                                });
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Error listing groups: {}", e);
                }
            }
        }

        let response = ListGroupsResponse {
            error_code: ERROR_NONE,
            groups,
            throttle_time_ms: 0,
        };
        response.encode(request.api_version)
    }

    fn api_key(&self) -> i16 {
        16 // ListGroups
    }

    fn supports_version(&self, version: i16) -> bool {
        (0..=4).contains(&version)
    }
}

pub struct DescribeGroupsHandler;

#[async_trait::async_trait]
impl ApiHandler for DescribeGroupsHandler {
    async fn handle(
        &self,
        request: &KafkaRequest,
        context: &mut HandlerContext,
    ) -> Result<Vec<u8>> {
        info!("Handling describe groups request v{}", request.api_version);

        // Parse request
        let mut cursor = std::io::Cursor::new(&request.body[..]);
        let describe_request = DescribeGroupsRequest::parse(&mut cursor, request.api_version)?;

        debug!(
            "DescribeGroups request: groups={:?}",
            describe_request.group_ids
        );

        let mut groups = Vec::new();

        for group_id in &describe_request.group_ids {
            match context
                .state_manager
                .consumer_groups()
                .get_group(group_id)
                .await?
            {
                Some(group) => {
                    let mut members = Vec::new();

                    for (member_id, member) in &group.members {
                        // For now, we'll use empty assignment
                        let member_assignment = vec![];

                        members.push(crate::protocol::describe_groups::MemberDescription {
                            member_id: member_id.clone(),
                            group_instance_id: None,
                            client_id: member.client_id.clone(),
                            client_host: member.client_host.clone(),
                            member_metadata: vec![],
                            member_assignment,
                        });
                    }

                    let group_desc = crate::protocol::describe_groups::GroupDescription {
                        error_code: ERROR_NONE,
                        group_id: group.group_id.clone(),
                        state: "Stable".to_string(),
                        protocol_type: group.protocol_type.clone(),
                        protocol_data: group.protocol.clone().unwrap_or_default(),
                        members,
                        authorized_operations: -1,
                    };

                    groups.push(group_desc);
                }
                None => {
                    groups.push(crate::protocol::describe_groups::GroupDescription {
                        error_code: ERROR_COORDINATOR_NOT_AVAILABLE,
                        group_id: group_id.clone(),
                        state: String::new(),
                        protocol_type: String::new(),
                        protocol_data: String::new(),
                        members: vec![],
                        authorized_operations: -1,
                    });
                }
            }
        }

        let response = DescribeGroupsResponse {
            throttle_time_ms: 0,
            groups,
        };

        // Encode response
        response.encode(request.api_version)
    }

    fn api_key(&self) -> i16 {
        15 // DescribeGroups
    }

    fn supports_version(&self, version: i16) -> bool {
        (0..=5).contains(&version)
    }
}
