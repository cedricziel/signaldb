use crate::{error::Result, storage::ObjectStorageLayout};
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProducerState {
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub sequence_numbers: HashMap<(String, i32), i32>, // (topic, partition) -> sequence
}

pub struct ProducerStateManager {
    object_store: Arc<dyn ObjectStore>,
    layout: ObjectStorageLayout,
}

impl ProducerStateManager {
    pub fn new(object_store: Arc<dyn ObjectStore>, layout: ObjectStorageLayout) -> Self {
        Self {
            object_store,
            layout,
        }
    }

    pub async fn get_producer_state(&self, producer_id: i64) -> Result<Option<ProducerState>> {
        let path = self.layout.producer_state_path(producer_id);

        match self.object_store.get(&path).await {
            Ok(get_result) => {
                let bytes = get_result.bytes().await?;
                let state: ProducerState = serde_json::from_slice(&bytes)?;
                Ok(Some(state))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn save_producer_state(&self, state: &ProducerState) -> Result<()> {
        let path = self.layout.producer_state_path(state.producer_id);
        let bytes = serde_json::to_vec(state)?;

        self.object_store.put(&path, bytes.into()).await?;

        Ok(())
    }

    pub async fn update_sequence(
        &self,
        producer_id: i64,
        producer_epoch: i16,
        topic: &str,
        partition: i32,
        sequence: i32,
    ) -> Result<()> {
        let mut state = match self.get_producer_state(producer_id).await? {
            Some(state) => {
                // Verify epoch
                if state.producer_epoch != producer_epoch {
                    let expected = state.producer_epoch;
                    return Err(crate::error::HeraclitusError::InvalidState(format!(
                        "Producer epoch mismatch: expected {expected}, got {producer_epoch}"
                    )));
                }
                state
            }
            None => ProducerState {
                producer_id,
                producer_epoch,
                sequence_numbers: HashMap::new(),
            },
        };

        state
            .sequence_numbers
            .insert((topic.to_string(), partition), sequence);
        self.save_producer_state(&state).await
    }
}
