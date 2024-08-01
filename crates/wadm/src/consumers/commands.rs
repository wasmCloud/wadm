//! A module for creating and consuming a stream of commands from NATS

use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Context, Poll};

use async_nats::{
    jetstream::{
        consumer::pull::{Config as PullConfig, Stream as MessageStream},
        stream::Stream as JsStream,
    },
    Error as NatsError,
};
use futures::{Stream, TryStreamExt};
use tracing::{error, warn};

use super::{CreateConsumer, ScopedMessage, LATTICE_METADATA_KEY, MULTITENANT_METADATA_KEY};
use crate::commands::*;

/// The name of the durable NATS stream and consumer that contains incoming lattice events
pub const COMMANDS_CONSUMER_PREFIX: &str = "wadm_commands";

/// A stream of all commands in a lattice, consumed from a durable NATS stream and consumer
pub struct CommandConsumer {
    stream: MessageStream,
    lattice_id: String,
}

impl CommandConsumer {
    /// Creates a new command consumer, returning an error if unable to create or access the durable
    /// consumer on the given stream.
    ///
    /// The `topic` param should be a valid topic where lattice events are expected to be sent and
    /// should match the given lattice ID. An error will be returned if the given lattice ID is not
    /// contained in the topic
    pub async fn new(
        stream: JsStream,
        topic: &str,
        lattice_id: &str,
        multitenant_prefix: Option<&str>,
    ) -> Result<CommandConsumer, NatsError> {
        if !topic.contains(lattice_id) {
            return Err(format!("Topic {topic} does not match for lattice ID {lattice_id}").into());
        }

        let (consumer_name, metadata) = if let Some(prefix) = multitenant_prefix {
            (
                format!("{COMMANDS_CONSUMER_PREFIX}-{lattice_id}_{prefix}"),
                HashMap::from([
                    (LATTICE_METADATA_KEY.to_string(), lattice_id.to_string()),
                    (MULTITENANT_METADATA_KEY.to_string(), prefix.to_string()),
                ]),
            )
        } else {
            (
                format!("{COMMANDS_CONSUMER_PREFIX}-{lattice_id}"),
                HashMap::from([(LATTICE_METADATA_KEY.to_string(), lattice_id.to_string())]),
            )
        };
        let consumer = stream
            .get_or_create_consumer(
                &consumer_name,
                PullConfig {
                    durable_name: Some(consumer_name.clone()),
                    name: Some(consumer_name.clone()),
                    description: Some(format!(
                        "Durable wadm commands consumer for lattice {lattice_id}"
                    )),
                    ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
                    ack_wait: super::DEFAULT_ACK_TIME,
                    max_deliver: 3,
                    deliver_policy: async_nats::jetstream::consumer::DeliverPolicy::All,
                    filter_subject: topic.to_owned(),
                    metadata,
                    ..Default::default()
                },
            )
            .await?;
        let messages = consumer
            .stream()
            .max_messages_per_batch(1)
            .messages()
            .await?;
        Ok(CommandConsumer {
            stream: messages,
            lattice_id: lattice_id.to_owned(),
        })
    }
}

impl Stream for CommandConsumer {
    type Item = Result<ScopedMessage<Command>, NatsError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.stream.try_poll_next_unpin(cx) {
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(Box::new(e)))),
            Poll::Ready(Some(Ok(msg))) => {
                // Convert to our event type, skipping if we can't do it (and looping around to
                // try the next poll)
                let cmd = match serde_json::from_slice(&msg.payload) {
                    Ok(cmd) => cmd,
                    Err(e) => {
                        warn!(error = ?e, "Unable to decode as command. Skipping message");
                        // This is slightly janky, but rather than having to store and poll the
                        // future (which gets a little gnarly), just pass the message onto a
                        // spawned thread which wakes up the thread when it is done acking.
                        let waker = cx.waker().clone();
                        // NOTE: If we are already in a stream impl, we should be able to spawn
                        // without worrying. A panic isn't the worst here if for some reason we
                        // can't as it means we can't ack the message and we'll be stuck waiting
                        // for it to deliver again until it fails
                        tokio::spawn(async move {
                            if let Err(e) = msg.ack().await {
                                error!(error = %e, "Error when trying to ack skipped message, message will be redelivered")
                            }
                            waker.wake();
                        });
                        // Return a poll pending. It will then wake up and try again once it has acked
                        return Poll::Pending;
                    }
                };
                // NOTE(thomastaylor312): Ideally we'd consume `msg.payload` above with a
                // `Cursor` and `from_reader` and then manually reconstruct the acking using the
                // message context, but I didn't want to waste time optimizing yet
                Poll::Ready(Some(Ok(ScopedMessage {
                    lattice_id: self.lattice_id.clone(),
                    inner: cmd,
                    acker: Some(msg),
                })))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[async_trait::async_trait]
impl CreateConsumer for CommandConsumer {
    type Output = CommandConsumer;

    async fn create(
        stream: async_nats::jetstream::stream::Stream,
        topic: &str,
        lattice_id: &str,
        multitenant_prefix: Option<&str>,
    ) -> Result<Self::Output, NatsError> {
        CommandConsumer::new(stream, topic, lattice_id, multitenant_prefix).await
    }
}
