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
