use crate::{error::Result, storage::ObjectStorageLayout};
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

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
        let offset_records: Vec<TopicPartitionOffset> = offsets
            .iter()
            .map(
                |(topic, partition, offset, metadata)| TopicPartitionOffset {
                    topic: topic.clone(),
                    partition: *partition,
                    offset: *offset,
                    metadata: metadata.clone(),
                },
            )
            .collect();

        let path = self.layout.consumer_group_path(group_id, "offsets.json");
        let bytes = serde_json::to_vec(&offset_records)?;

        self.object_store.put(&path, bytes.into()).await?;

        Ok(())
    }
}
