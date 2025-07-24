use crate::{error::Result, storage::ObjectStorageLayout};
use futures::StreamExt;
use object_store::ObjectStore;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicMetadata {
    pub name: String,
    pub partitions: i32,
    pub replication_factor: i16,
    pub config: HashMap<String, String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

pub struct MetadataManager {
    object_store: Arc<dyn ObjectStore>,
    layout: ObjectStorageLayout,
    cache: RwLock<MetadataCache>,
    cache_ttl: Duration,
}

struct MetadataCache {
    topics: HashMap<String, (TopicMetadata, Instant)>,
}

impl MetadataManager {
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        layout: ObjectStorageLayout,
        cache_ttl_sec: u64,
    ) -> Self {
        Self {
            object_store,
            layout,
            cache: RwLock::new(MetadataCache {
                topics: HashMap::new(),
            }),
            cache_ttl: Duration::from_secs(cache_ttl_sec),
        }
    }

    pub async fn get_topic(&self, topic_name: &str) -> Result<Option<TopicMetadata>> {
        // Check cache first
        {
            let cache = self.cache.read();
            if let Some((metadata, cached_at)) = cache.topics.get(topic_name) {
                if cached_at.elapsed() < self.cache_ttl {
                    return Ok(Some(metadata.clone()));
                }
            }
        }

        // Load from object storage
        let path = self
            .layout
            .metadata_path(&format!("topics/{topic_name}.json"));

        match self.object_store.get(&path).await {
            Ok(get_result) => {
                let bytes = get_result.bytes().await?;
                let metadata: TopicMetadata = serde_json::from_slice(&bytes)?;

                // Update cache
                {
                    let mut cache = self.cache.write();
                    cache
                        .topics
                        .insert(topic_name.to_string(), (metadata.clone(), Instant::now()));
                }

                Ok(Some(metadata))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn create_topic(&self, metadata: TopicMetadata) -> Result<()> {
        let name = &metadata.name;
        let path = self.layout.metadata_path(&format!("topics/{name}.json"));
        let bytes = serde_json::to_vec(&metadata)?;

        self.object_store.put(&path, bytes.into()).await?;

        // Update cache
        {
            let mut cache = self.cache.write();
            cache
                .topics
                .insert(metadata.name.clone(), (metadata, Instant::now()));
        }

        Ok(())
    }

    pub async fn delete_topic(&self, topic_name: &str) -> Result<()> {
        let path = self
            .layout
            .metadata_path(&format!("topics/{topic_name}.json"));

        // Delete from object storage
        self.object_store.delete(&path).await?;

        // Remove from cache
        {
            let mut cache = self.cache.write();
            cache.topics.remove(topic_name);
        }

        Ok(())
    }

    pub async fn list_topics(&self) -> Result<Vec<String>> {
        // List all objects in the topics metadata directory
        let prefix = self.layout.metadata_path("topics/");
        let mut topics = Vec::new();

        let mut stream = self.object_store.list(Some(&prefix));
        while let Some(result) = stream.next().await {
            let meta = result?;
            if let Some(location) = meta.location.filename() {
                if location.ends_with(".json") {
                    let topic_name = location.strip_suffix(".json").unwrap_or(location);
                    topics.push(topic_name.to_string());
                }
            }
        }

        Ok(topics)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_create_and_get_topic() {
        let object_store = Arc::new(InMemory::new());
        let layout = ObjectStorageLayout::new("test/");
        let manager = MetadataManager::new(object_store, layout, 60);

        let metadata = TopicMetadata {
            name: "test-topic".to_string(),
            partitions: 3,
            replication_factor: 1,
            config: HashMap::new(),
            created_at: chrono::Utc::now(),
        };

        // Create topic
        manager.create_topic(metadata.clone()).await.unwrap();

        // Get topic
        let retrieved = manager.get_topic("test-topic").await.unwrap();
        assert!(retrieved.is_some());
        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.name, "test-topic");
        assert_eq!(retrieved.partitions, 3);
        assert_eq!(retrieved.replication_factor, 1);
    }

    #[tokio::test]
    async fn test_get_nonexistent_topic() {
        let object_store = Arc::new(InMemory::new());
        let layout = ObjectStorageLayout::new("test/");
        let manager = MetadataManager::new(object_store, layout, 60);

        let result = manager.get_topic("nonexistent").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_list_topics() {
        let object_store = Arc::new(InMemory::new());
        let layout = ObjectStorageLayout::new("test/");
        let manager = MetadataManager::new(object_store, layout, 60);

        // Create multiple topics
        for i in 0..3 {
            let metadata = TopicMetadata {
                name: format!("topic-{i}"),
                partitions: 1,
                replication_factor: 1,
                config: HashMap::new(),
                created_at: chrono::Utc::now(),
            };
            manager.create_topic(metadata).await.unwrap();
        }

        // List topics
        let topics = manager.list_topics().await.unwrap();
        assert_eq!(topics.len(), 3);
        assert!(topics.contains(&"topic-0".to_string()));
        assert!(topics.contains(&"topic-1".to_string()));
        assert!(topics.contains(&"topic-2".to_string()));
    }

    #[tokio::test]
    async fn test_cache_functionality() {
        let object_store = Arc::new(InMemory::new());
        let layout = ObjectStorageLayout::new("test/");
        let manager = MetadataManager::new(object_store, layout.clone(), 60);

        let metadata = TopicMetadata {
            name: "cached-topic".to_string(),
            partitions: 2,
            replication_factor: 1,
            config: HashMap::new(),
            created_at: chrono::Utc::now(),
        };

        // Create topic
        manager.create_topic(metadata.clone()).await.unwrap();

        // First get - loads from storage
        let _ = manager.get_topic("cached-topic").await.unwrap();

        // Remove from object storage to verify cache is used
        let path = layout.metadata_path("topics/cached-topic.json");
        manager.object_store.delete(&path).await.unwrap();

        // Second get - should come from cache
        let cached = manager.get_topic("cached-topic").await.unwrap();
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().name, "cached-topic");
    }
}
