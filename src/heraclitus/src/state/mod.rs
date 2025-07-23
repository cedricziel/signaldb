use crate::{config::StateConfig, error::Result, storage::ObjectStorageLayout};
use object_store::ObjectStore;
use std::sync::Arc;

mod consumer_group;
mod metadata;
mod producer;

pub use consumer_group::ConsumerGroupManager;
pub use metadata::MetadataManager;
pub use producer::ProducerStateManager;

pub struct StateManager {
    object_store: Arc<dyn ObjectStore>,
    layout: ObjectStorageLayout,
    metadata: MetadataManager,
    consumer_groups: ConsumerGroupManager,
    producers: ProducerStateManager,
}

impl StateManager {
    pub async fn new(object_store: Arc<dyn ObjectStore>, config: StateConfig) -> Result<Self> {
        let layout = ObjectStorageLayout::new(&config.prefix);

        let metadata = MetadataManager::new(
            object_store.clone(),
            layout.clone(),
            config.metadata_cache_ttl_sec,
        );

        let consumer_groups = ConsumerGroupManager::new(object_store.clone(), layout.clone());

        let producers = ProducerStateManager::new(object_store.clone(), layout.clone());

        Ok(Self {
            object_store,
            layout,
            metadata,
            consumer_groups,
            producers,
        })
    }

    pub fn metadata(&self) -> &MetadataManager {
        &self.metadata
    }

    pub fn consumer_groups(&self) -> &ConsumerGroupManager {
        &self.consumer_groups
    }

    pub fn producers(&self) -> &ProducerStateManager {
        &self.producers
    }

    pub fn object_store(&self) -> &Arc<dyn ObjectStore> {
        &self.object_store
    }

    pub fn layout(&self) -> &ObjectStorageLayout {
        &self.layout
    }
}
