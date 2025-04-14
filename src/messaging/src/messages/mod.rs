use serde::{Deserialize, Serialize};

pub mod batch;
pub mod span;
pub mod trace;

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct SimpleMessage {
    pub id: String,
    pub name: String,
}
