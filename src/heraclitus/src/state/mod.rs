use crate::{config::StateConfig, error::Result, storage::ObjectStorageLayout};
use object_store::ObjectStore;
use std::sync::Arc;

mod consumer_group;
mod messages;
mod metadata;
mod producer;

pub use consumer_group::{ConsumerGroupManager, ConsumerGroupMember, ConsumerGroupState};
pub use messages::MessageManager;
pub use metadata::{MetadataManager, TopicMetadata};
pub use producer::ProducerStateManager;

pub struct StateManager {
    object_store: Arc<dyn ObjectStore>,
    layout: ObjectStorageLayout,
    metadata: MetadataManager,
    messages: MessageManager,
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

        let messages = MessageManager::new(object_store.clone(), layout.clone());
        messages.initialize().await?;

        let consumer_groups = ConsumerGroupManager::new(object_store.clone(), layout.clone());

        let producers = ProducerStateManager::new(object_store.clone(), layout.clone());

        Ok(Self {
            object_store,
            layout,
            metadata,
            messages,
            consumer_groups,
            producers,
        })
    }

    pub fn metadata(&self) -> &MetadataManager {
        &self.metadata
    }

    pub fn messages(&self) -> &MessageManager {
        &self.messages
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
