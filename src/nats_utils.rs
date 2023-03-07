//! Helper utilities for interacting with NATS
use async_nats::jetstream::Context;
use serde::Serialize;

/// A helper function that serializes data and ensures a message was sent and received by a stream.
/// Returns an error if the message could not be serialized, sent, or if the message wasn't
/// acknowledged
pub async fn ensure_send<T: Serialize>(
    context: &Context,
    subject: String,
    data: &T,
) -> anyhow::Result<()> {
    // TODO: wrap this in a struct with the subject and the client so things can be flushed after every message
    let fut = context
        .publish(subject, serde_json::to_vec(data)?.into())
        .await
        .map_err(|e| anyhow::anyhow!("Unable to publish message: {e:?}"))?;
    fut.await
        .map(|_| ())
        .map_err(|e| anyhow::anyhow!("Never received ack from server: {e:?}"))
}
