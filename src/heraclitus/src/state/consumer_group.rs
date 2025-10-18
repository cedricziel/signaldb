use crate::{error::Result, storage::ObjectStorageLayout};
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::warn;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupState {
    pub group_id: String,
    pub generation_id: i32,
    pub protocol_type: String,
    pub protocol: Option<String>,
    pub leader: Option<String>,
    pub members: HashMap<String, ConsumerGroupMember>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupMember {
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub last_heartbeat_ms: i64,
    pub subscribed_topics: Vec<String>,
    pub protocol_version: i16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicPartitionOffset {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub metadata: Option<String>,
}

pub struct ConsumerGroupManager {
    object_store: Arc<dyn ObjectStore>,
    layout: ObjectStorageLayout,
}

impl ConsumerGroupManager {
    pub fn new(object_store: Arc<dyn ObjectStore>, layout: ObjectStorageLayout) -> Self {
        Self {
            object_store,
            layout,
        }
    }

    pub async fn get_group(&self, group_id: &str) -> Result<Option<ConsumerGroupState>> {
        let path = self.layout.consumer_group_path(group_id, "state.json");

        match self.object_store.get(&path).await {
            Ok(get_result) => {
                let bytes = get_result.bytes().await?;
                let state: ConsumerGroupState = serde_json::from_slice(&bytes)?;
                Ok(Some(state))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn save_group(&self, state: &ConsumerGroupState) -> Result<()> {
        let path = self
            .layout
            .consumer_group_path(&state.group_id, "state.json");
        let bytes = serde_json::to_vec(state)?;

        self.object_store.put(&path, bytes.into()).await?;

        Ok(())
    }

    pub async fn get_offsets(
        &self,
        group_id: &str,
    ) -> Result<HashMap<(String, i32), TopicPartitionOffset>> {
        let path = self.layout.consumer_group_path(group_id, "offsets.json");

        match self.object_store.get(&path).await {
            Ok(get_result) => {
                let bytes = get_result.bytes().await?;
                let offsets: Vec<TopicPartitionOffset> = serde_json::from_slice(&bytes)?;

                let mut map = HashMap::new();
                for offset in offsets {
                    map.insert((offset.topic.clone(), offset.partition), offset);
                }
                Ok(map)
            }
            Err(object_store::Error::NotFound { .. }) => Ok(HashMap::new()),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn save_offsets(
        &self,
        group_id: &str,
        offsets: &[(String, i32, i64, Option<String>)],
    ) -> Result<()> {
        // First, get existing offsets to merge with new ones
        let mut existing_offsets = self.get_offsets(group_id).await?;

        // Update existing offsets with new ones
        for (topic, partition, offset, metadata) in offsets {
            let offset_record = TopicPartitionOffset {
                topic: topic.clone(),
                partition: *partition,
                offset: *offset,
                metadata: metadata.clone(),
            };
            existing_offsets.insert((topic.clone(), *partition), offset_record);
        }

        // Convert back to vector for serialization
        let offset_records: Vec<TopicPartitionOffset> = existing_offsets.into_values().collect();

        let path = self.layout.consumer_group_path(group_id, "offsets.json");
        let bytes = serde_json::to_vec(&offset_records)?;

        self.object_store.put(&path, bytes.into()).await?;

        Ok(())
    }

    pub async fn list_groups(&self) -> Result<Vec<String>> {
        use futures::StreamExt;

        // List all objects in the consumer-groups prefix
        let prefix = self.layout.consumer_groups_prefix();
        let prefix_path = object_store::path::Path::from(prefix.clone());
        let stream = self.object_store.list(Some(&prefix_path));

        let mut group_ids = std::collections::HashSet::new();

        // Extract group IDs from the paths
        futures::pin_mut!(stream);
        while let Some(result) = stream.next().await {
            match result {
                Ok(meta) => {
                    // Path format: consumer-groups/{group-id}/state.json
                    let path = meta.location.as_ref();
                    if let Some(stripped) = path.strip_prefix(&prefix) {
                        // Extract group ID from path
                        if let Some(group_id) = stripped.split('/').next() {
                            if !group_id.is_empty() {
                                group_ids.insert(group_id.to_string());
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("Error listing consumer groups: {}", e);
                }
            }
        }

        Ok(group_ids.into_iter().collect())
    }
}
