// Prototype NATS-based service discovery using subject-based approach
use anyhow::Result;
use async_nats::Client;
use futures::Stream;
use futures::{StreamExt, pin_mut};
use serde::{Deserialize, Serialize};
use serde_json;
use async_stream::stream;

/// Service instance metadata
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct Instance {
    pub id: String,
    pub host: String,
    pub port: u16,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
struct Deregister {
    id: String,
}

fn nats_url() -> String {
    std::env::var("NATS_URL").unwrap_or_else(|_| "127.0.0.1:4222".to_string())
}

/// Register self under a role by publishing on `services.{role}.register`
pub async fn register(role: &str, inst: Instance) -> Result<()> {
    let client: Client = async_nats::connect(nats_url()).await?;
    let subject = format!("services.{}.register", role);
    let data = serde_json::to_vec(&inst)?;
    client.publish(subject, data.into()).await?;
    Ok(())
}

/// Deregister self under a role by publishing on `services.{role}.deregister`
pub async fn deregister(role: &str, id: &str) -> Result<()> {
    let client: Client = async_nats::connect(nats_url()).await?;
    let subject = format!("services.{}.deregister", role);
    let dereg = Deregister { id: id.to_string() };
    let data = serde_json::to_vec(&dereg)?;
    client.publish(subject, data.into()).await?;
    Ok(())
}

/// Stream changes for a role: yields a vector of current instances on each update
pub fn watch(role: &str) -> impl Stream<Item = Vec<Instance>> {
    let role = role.to_string();
    stream! {
        // Connect to NATS
        let client = match async_nats::connect(nats_url()).await {
            Ok(c) => c,
            Err(_) => {
                yield Vec::new();
                return;
            }
        };
        let subj_reg = format!("services.{}.register", role);
        let subj_dereg = format!("services.{}.deregister", role);
        let mut sub_reg = match client.subscribe(subj_reg).await {
            Ok(s) => s,
            Err(_) => {
                yield Vec::new();
                return;
            }
        };
        let mut sub_dereg = match client.subscribe(subj_dereg).await {
            Ok(s) => s,
            Err(_) => {
                yield Vec::new();
                return;
            }
        };
        // In-memory map of instances
        let mut instances = std::collections::HashMap::new();
        // Initial snapshot
        yield instances.values().cloned().collect();
        loop {
            tokio::select! {
                msg = sub_reg.next() => {
                    if let Some(msg) = msg {
                        if let Ok(inst) = serde_json::from_slice::<Instance>(&msg.payload) {
                            instances.insert(inst.id.clone(), inst);
                            yield instances.values().cloned().collect();
                        }
                    } else {
                        break;
                    }
                }
                msg = sub_dereg.next() => {
                    if let Some(msg) = msg {
                        if let Ok(d) = serde_json::from_slice::<Deregister>(&msg.payload) {
                            instances.remove(&d.id);
                            yield instances.values().cloned().collect();
                        }
                    } else {
                        break;
                    }
                }
            }
        }
    }
}

/// Lookup current instances: returns the first snapshot from `watch`
pub async fn lookup(role: &str) -> Result<Vec<Instance>> {
    let mut stream = watch(role);
    pin_mut!(stream);
    if let Some(list) = stream.next().await {
        Ok(list)
    } else {
        Ok(Vec::new())
    }
}
// Unit tests for serialization
#[cfg(test)]
mod serde_tests {
    use super::{Instance, Deregister};
    use serde_json;

    #[test]
    fn instance_serde() {
        let inst = Instance { id: "abc".into(), host: "1.2.3.4".into(), port: 80 };
        let json = serde_json::to_string(&inst).unwrap();
        assert_eq!(json, "{\"id\":\"abc\",\"host\":\"1.2.3.4\",\"port\":80}");
    }

    #[test]
    fn deregister_serde() {
        let dereg = Deregister { id: "xyz".into() };
        let json = serde_json::to_string(&dereg).unwrap();
        assert_eq!(json, "{\"id\":\"xyz\"}");
    }
}