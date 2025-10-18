use crate::{error::Result, storage::ObjectStorageLayout};
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error, info};

/// Manages message offsets and metadata for each topic partition
pub struct MessageManager {
    object_store: Arc<dyn ObjectStore>,
    layout: ObjectStorageLayout,
    /// In-memory cache of current offsets per partition
    offset_cache: Arc<Mutex<HashMap<(String, i32), PartitionOffsets>>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PartitionOffsets {
    /// Next offset to be assigned
    pub next_offset: i64,
    /// Earliest offset still in storage
    pub log_start_offset: i64,
    /// High water mark (last committed offset + 1)
    pub high_water_mark: i64,
}

impl MessageManager {
    pub fn new(object_store: Arc<dyn ObjectStore>, layout: ObjectStorageLayout) -> Self {
        Self {
            object_store,
            layout,
            offset_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Initialize the manager by loading existing offsets from storage
    pub async fn initialize(&self) -> Result<()> {
        info!("Initializing message manager");
        // In a real implementation, we would scan object storage to find
        // the highest offsets for each partition
        // For now, we'll start fresh
        Ok(())
    }

    /// Get the next offset for a partition and increment it atomically
    pub async fn assign_next_offset(&self, topic: &str, partition: i32) -> Result<i64> {
        let mut cache = self.offset_cache.lock().await;
        let key = (topic.to_string(), partition);

        let offsets = cache.entry(key.clone()).or_insert_with(|| {
            debug!(
                "Creating new offset tracking for topic {} partition {}",
                topic, partition
            );
            PartitionOffsets::default()
        });

        let assigned_offset = offsets.next_offset;
        offsets.next_offset += 1;

        // Periodically persist offset state to object storage
        // For now, we'll do it every 1000 messages
        if assigned_offset % 1000 == 0 {
            let offsets_to_save = offsets.clone();
            let object_store = self.object_store.clone();
            let layout = self.layout.clone();
            let topic_clone = topic.to_string();

            tokio::spawn(async move {
                if let Err(e) = Self::persist_offsets(
                    &object_store,
                    &layout,
                    &topic_clone,
                    partition,
                    &offsets_to_save,
                )
                .await
                {
                    error!("Failed to persist offsets: {}", e);
                }
            });
        }

        Ok(assigned_offset)
    }

    /// Assign offsets to a batch of messages
    pub async fn assign_offsets(
        &self,
        topic: &str,
        partition: i32,
        message_count: usize,
    ) -> Result<i64> {
        let mut cache = self.offset_cache.lock().await;
        let key = (topic.to_string(), partition);

        let offsets = cache.entry(key.clone()).or_insert_with(|| {
            debug!(
                "Creating new offset tracking for topic {} partition {}",
                topic, partition
            );
            PartitionOffsets::default()
        });

        let base_offset = offsets.next_offset;
        offsets.next_offset += message_count as i64;

        // Update high water mark (in real Kafka, this would be after replication)
        offsets.high_water_mark = offsets.next_offset;

        Ok(base_offset)
    }

    /// Get current offset information for a partition
    pub async fn get_partition_offsets(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<PartitionOffsets> {
        let cache = self.offset_cache.lock().await;
        let key = (topic.to_string(), partition);

        match cache.get(&key) {
            Some(offsets) => Ok(offsets.clone()),
            None => {
                // Try to load from storage
                match self.load_offsets(topic, partition).await? {
                    Some(offsets) => Ok(offsets),
                    None => Ok(PartitionOffsets::default()),
                }
            }
        }
    }

    /// Get the log start offset for a partition
    pub async fn get_log_start_offset(&self, topic: &str, partition: i32) -> Result<i64> {
        let offsets = self.get_partition_offsets(topic, partition).await?;
        Ok(offsets.log_start_offset)
    }

    /// Get the high water mark for a partition
    pub async fn get_high_water_mark(&self, topic: &str, partition: i32) -> Result<i64> {
        let offsets = self.get_partition_offsets(topic, partition).await?;
        Ok(offsets.high_water_mark)
    }

    /// Load offsets from object storage
    async fn load_offsets(&self, topic: &str, partition: i32) -> Result<Option<PartitionOffsets>> {
        let path = self
            .layout
            .metadata_path(&format!("offsets/{topic}/partition-{partition}.json"));

        match self.object_store.get(&path).await {
            Ok(get_result) => {
                let bytes = get_result.bytes().await?;
                let offsets: PartitionOffsets = serde_json::from_slice(&bytes)?;

                // Update cache
                let mut cache = self.offset_cache.lock().await;
                cache.insert((topic.to_string(), partition), offsets.clone());

                Ok(Some(offsets))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Persist offsets to object storage
    async fn persist_offsets(
        object_store: &Arc<dyn ObjectStore>,
        layout: &ObjectStorageLayout,
        topic: &str,
        partition: i32,
        offsets: &PartitionOffsets,
    ) -> Result<()> {
        let path = layout.metadata_path(&format!("offsets/{topic}/partition-{partition}.json"));
        let bytes = serde_json::to_vec(offsets)?;

        object_store.put(&path, bytes.into()).await?;
        debug!(
            "Persisted offsets for topic {} partition {}: next_offset={}",
            topic, partition, offsets.next_offset
        );

        Ok(())
    }

    /// Validate producer sequence for idempotency
    pub async fn validate_producer_sequence(
        &self,
        producer_id: i64,
        producer_epoch: i16,
        topic: &str,
        partition: i32,
        sequence: i32,
    ) -> Result<bool> {
        // For now, we'll accept all sequences
        // In a real implementation, we would track producer state
        // and check for duplicate sequences
        debug!(
            "Validating producer {} epoch {} sequence {} for topic {} partition {}",
            producer_id, producer_epoch, sequence, topic, partition
        );
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_assign_next_offset() {
        let object_store = object_store::memory::InMemory::new();
        let layout = ObjectStorageLayout::new("test/");
        let manager = MessageManager::new(Arc::new(object_store), layout);

        // First offset should be 0
        let offset1 = manager.assign_next_offset("topic1", 0).await.unwrap();
        assert_eq!(offset1, 0);

        // Next offset should be 1
        let offset2 = manager.assign_next_offset("topic1", 0).await.unwrap();
        assert_eq!(offset2, 1);

        // Different partition should start at 0
        let offset3 = manager.assign_next_offset("topic1", 1).await.unwrap();
        assert_eq!(offset3, 0);
    }

    #[tokio::test]
    async fn test_assign_offsets_batch() {
        let object_store = object_store::memory::InMemory::new();
        let layout = ObjectStorageLayout::new("test/");
        let manager = MessageManager::new(Arc::new(object_store), layout);

        // Assign offsets for 5 messages
        let base_offset = manager.assign_offsets("topic1", 0, 5).await.unwrap();
        assert_eq!(base_offset, 0);

        // Next assignment should start at 5
        let next_base = manager.assign_offsets("topic1", 0, 3).await.unwrap();
        assert_eq!(next_base, 5);

        // Check that high water mark is updated
        let offsets = manager.get_partition_offsets("topic1", 0).await.unwrap();
        assert_eq!(offsets.high_water_mark, 8);
    }

    #[tokio::test]
    async fn test_get_partition_offsets() {
        let object_store = object_store::memory::InMemory::new();
        let layout = ObjectStorageLayout::new("test/");
        let manager = MessageManager::new(Arc::new(object_store), layout);

        // Initially should return defaults
        let offsets = manager.get_partition_offsets("topic1", 0).await.unwrap();
        assert_eq!(offsets.next_offset, 0);
        assert_eq!(offsets.log_start_offset, 0);
        assert_eq!(offsets.high_water_mark, 0);

        // After assigning some offsets
        manager.assign_offsets("topic1", 0, 10).await.unwrap();
        let offsets = manager.get_partition_offsets("topic1", 0).await.unwrap();
        assert_eq!(offsets.next_offset, 10);
        assert_eq!(offsets.high_water_mark, 10);
    }

    #[tokio::test]
    async fn test_different_topics_isolated() {
        let object_store = object_store::memory::InMemory::new();
        let layout = ObjectStorageLayout::new("test/");
        let manager = MessageManager::new(Arc::new(object_store), layout);

        // Assign offsets to different topics
        let offset1 = manager.assign_next_offset("topic1", 0).await.unwrap();
        let offset2 = manager.assign_next_offset("topic2", 0).await.unwrap();

        assert_eq!(offset1, 0);
        assert_eq!(offset2, 0);

        // They should increment independently
        let offset3 = manager.assign_next_offset("topic1", 0).await.unwrap();
        let offset4 = manager.assign_next_offset("topic2", 0).await.unwrap();

        assert_eq!(offset3, 1);
        assert_eq!(offset4, 1);
    }
}
