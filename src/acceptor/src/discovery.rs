use async_nats::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use anyhow::Error;

/// Service instance metadata for discovery via NATS.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Instance {
    /// Unique instance identifier
    pub id: String,
    /// Host or IP address of the service
    pub host: String,
    /// Port on which the service listens
    pub port: u16,
}

/// Simple NATS-based service registrar using pub/sub.
pub struct NatsDiscovery {
    client: Client,
    role: String,
    instance: Instance,
}

impl NatsDiscovery {
    /// Connect to NATS and prepare to register a service instance under the given role.
    pub async fn new(nats_url: &str, role: &str, instance: Instance) -> Result<Self, Error> {
        let client: Client = async_nats::connect(nats_url).await?;
        Ok(Self {
            client,
            role: role.to_string(),
            instance,
        })
    }

    /// Publish a registration message for this instance.
    pub async fn register(&self) -> Result<(), Error> {
        let subject = format!("services.{}.register", self.role);
        let payload = serde_json::to_vec(&self.instance)?;
        // Publish registration message
        self.client.publish(subject, payload.into()).await?;
        Ok(())
    }

    /// Publish a deregistration message for this instance.
    pub async fn deregister(&self) -> Result<(), Error> {
        let subject = format!("services.{}.deregister", self.role);
        let payload = serde_json::to_vec(&self.instance)?;
        // Publish deregistration message
        self.client.publish(subject, payload.into()).await?;
        Ok(())
    }

    /// Spawn a background task that periodically publishes heartbeats.
    pub fn spawn_heartbeat(&self, interval: Duration) -> tokio::task::JoinHandle<()> {
        let client = self.client.clone();
        let heartbeat_subj = format!("services.{}.heartbeat", self.role);
        let instance = self.instance.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                let payload = serde_json::to_vec(&instance).unwrap();
                if let Err(err) = client.publish(heartbeat_subj.clone(), payload.into()).await {
                    log::error!("Discovery heartbeat failed: {}", err);
                }
            }
        })
    }
}