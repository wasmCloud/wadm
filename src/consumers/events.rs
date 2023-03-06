//! A module for creating and consuming a stream of events from a wasmcloud lattice

use std::convert::TryFrom;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use async_nats::{
    jetstream::{
        consumer::pull::{Config as PullConfig, Stream as MessageStream},
        stream::Stream as JsStream,
    },
    Error as NatsError,
};
use futures::{Stream, TryStreamExt};
use tracing::{error, warn};

use super::{CreateConsumer, ScopedMessage};
use crate::events::*;

/// The name of the durable NATS stream and consumer that contains incoming lattice events
pub const EVENTS_CONSUMER_PREFIX: &str = "wadm_events";
/// The default time given for an event to ack
pub const DEFAULT_ACK_TIME: Duration = Duration::from_secs(2);

/// A stream of all events of a lattice, consumed from a durable NATS stream and consumer
pub struct EventConsumer {
    stream: MessageStream,
    lattice_id: String,
}

impl EventConsumer {
    /// Creates a new event consumer, returning an error if unable to create or access the durable
    /// consumer on the given stream.
    ///
    /// The `topic` param should be a valid topic where lattice events are expected to be sent (e.g.
    /// `wasmbus.evt.<lattice_prefix>`). An error will be returned if the topic is not in a
    /// recognized format. This is due to the need to fetch the lattice id off of the topic and
    /// match the correct inbound events
    pub async fn new(stream: JsStream, topic: &str) -> Result<EventConsumer, NatsError> {
        // Basically there should always be 3 strings when split per topic. <bus prefix>.evt.<lattice_id>
        if topic.split('.').count() != 3 {
            return Err(format!("Subject {topic} is not valid").into());
        }

        // Safety: We can unwrap here because we already checked that we got 3 items on the split above
        let lattice_id = topic.rsplit_once('.').unwrap().1.to_owned();

        let consumer_name = format!("{EVENTS_CONSUMER_PREFIX}_{lattice_id}");
        let consumer = stream
            .get_or_create_consumer(
                &consumer_name,
                PullConfig {
                    durable_name: Some(consumer_name.clone()),
                    name: Some(consumer_name.clone()),
                    description: Some(format!(
                        "Durable wadm events consumer for lattice {lattice_id}"
                    )),
                    ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
                    ack_wait: DEFAULT_ACK_TIME,
                    max_deliver: 3,
                    deliver_policy: async_nats::jetstream::consumer::DeliverPolicy::All,
                    filter_subject: topic.to_owned(),
                    ..Default::default()
                },
            )
            .await?;
        let messages = consumer
            .stream()
            .max_messages_per_batch(1)
            .messages()
            .await?;
        Ok(EventConsumer {
            stream: messages,
            lattice_id,
        })
    }
}

impl Stream for EventConsumer {
    type Item = Result<ScopedMessage<Event>, NatsError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.stream.try_poll_next_unpin(cx) {
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(Some(Ok(msg))) => {
                // Parse as a cloud event, skipping if we can't do it (and looping around to try
                // the next poll)
                let raw_evt: cloudevents::Event = match serde_json::from_slice(&msg.payload) {
                    Ok(evt) => evt,
                    Err(e) => {
                        warn!(error = %e, "Unable to decode message as cloudevent. Skipping message");
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
                // Convert to our event type, skipping if we can't do it
                let evt = match Event::try_from(raw_evt) {
                    Ok(evt) => evt,
                    Err(e) => {
                        warn!(error = ?e, "Unable to decode as event. Skipping message");
                        let waker = cx.waker().clone();
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
                    inner: evt,
                    acker: Some(msg),
                })))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[async_trait::async_trait]
impl CreateConsumer for EventConsumer {
    type Output = EventConsumer;

    async fn create(
        stream: async_nats::jetstream::stream::Stream,
        topic: &str,
    ) -> Result<Self::Output, NatsError> {
        EventConsumer::new(stream, topic).await
    }
}
