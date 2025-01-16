use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct SimpleMessage {
    pub id: String,
    pub name: String,
}
