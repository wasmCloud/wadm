//! A module that defines a generic publisher trait and several common publishers that can be passed
//! into various structs in wadm. Often times this is used for testing, but it also allows for
//! flexibility for others who may want to publish to other sources

use async_nats::{jetstream::Context, Client};

#[async_trait::async_trait]
pub trait Publisher {
    /// Publishes the given data to an optional destination (i.e. subject or topic). Implementors
    /// are responsible for documenting guarantees of delivery for published data
    ///
    /// The destination is optional for two reasons: Sometimes a client cannot be scoped to a
    /// specific topic and also, some implementations may not use subject/topic based delivery
    async fn publish(&self, data: Vec<u8>, destination: Option<&str>) -> anyhow::Result<()>;
}

/// The publisher implementation for a normal NATS client constrained to the given topic. This only
/// has guarantees that the message was sent, not that it was received
#[async_trait::async_trait]
impl Publisher for Client {
    async fn publish(&self, data: Vec<u8>, destination: Option<&str>) -> anyhow::Result<()> {
        let subject = match destination {
            Some(s) => s.to_owned(),
            None => anyhow::bail!("NATS publishes require a destination"),
        };
        self.publish(subject, data.into())
            .await
            .map_err(anyhow::Error::from)
    }
}

/// The publisher implementation for a NATS jetstream client. This implementation will guarantee
/// that a sent message is received by a stream
#[async_trait::async_trait]
impl Publisher for Context {
    async fn publish(&self, data: Vec<u8>, destination: Option<&str>) -> anyhow::Result<()> {
        let subject = match destination {
            Some(s) => s.to_owned(),
            None => anyhow::bail!("NATS publishes require a destination"),
        };
        let ack = self
            .publish(subject, data.into())
            .await
            .map_err(|e| anyhow::anyhow!("Unable to publish message").context(e))?;

        ack.await
            .map(|_| ())
            .map_err(|e| anyhow::anyhow!("Unable to verify receipt of message").context(e))
    }
}
