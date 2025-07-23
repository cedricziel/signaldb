mod batch_writer;
mod message_reader;
mod schema;

pub use batch_writer::BatchWriter;
pub use message_reader::MessageReader;
pub use schema::{KafkaMessage, KafkaMessageBatch};

use object_store::path::Path as ObjectPath;

#[derive(Clone)]
pub struct ObjectStorageLayout {
    prefix: String,
}

impl ObjectStorageLayout {
    pub fn new(prefix: &str) -> Self {
        Self {
            prefix: prefix.trim_end_matches('/').to_string(),
        }
    }

    pub fn messages_path(&self, topic: &str, partition: i32, hour: &str) -> ObjectPath {
        ObjectPath::from(format!(
            "{}/messages/topic={}/partition={}/hour={}/",
            self.prefix, topic, partition, hour
        ))
    }

    pub fn metadata_path(&self, path: &str) -> ObjectPath {
        ObjectPath::from(format!("{}/metadata/{}", self.prefix, path))
    }

    pub fn consumer_group_path(&self, group_id: &str, file: &str) -> ObjectPath {
        ObjectPath::from(format!(
            "{}/metadata/consumer-groups/{}/{}",
            self.prefix, group_id, file
        ))
    }

    pub fn producer_state_path(&self, producer_id: i64) -> ObjectPath {
        ObjectPath::from(format!(
            "{}/metadata/producers/producer-{}.json",
            self.prefix, producer_id
        ))
    }

    pub fn coordination_path(&self, path: &str) -> ObjectPath {
        ObjectPath::from(format!("{}/coordination/{}", self.prefix, path))
    }
}
