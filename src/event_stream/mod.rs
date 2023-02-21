//! A module for creating and consuming a stream of events from a wasmcloud lattice

use std::convert::TryFrom;
use std::mem::Discriminant;
use std::pin::Pin;
use std::task::{Context, Poll};

use async_nats::{Client, Error as NatsError, Subscriber};
use futures::{Stream, StreamExt};
use tracing::warn;

use crate::events::*;

/// A stream of all events of a lattice.
pub struct EventStream {
    client: Client,
    topic: String,
    subscription: Subscriber,
}

impl EventStream {
    /// Creates a new event stream, returning an error if unable to create a subscriber.
    ///
    /// The `topic` param should be a valid topic where lattice events are expected (e.g.
    /// `wasmbus.evt.<lattice_prefix>`)
    pub async fn new(client: Client, topic: &str) -> Result<EventStream, NatsError> {
        let subscription = client.subscribe(topic.to_owned()).await?;
        Ok(EventStream {
            client,
            topic: topic.to_owned(),
            subscription,
        })
    }

    /// Clones the stream by creating a new subscriber for the event bus. This can fail if there is
    /// an error recreating the subscription
    pub async fn try_clone(&self) -> Result<EventStream, NatsError> {
        EventStream::new(self.client.clone(), &self.topic).await
    }

    /// Turn this stream into a filtered stream
    ///
    /// See the documentation for [`FilteredEventStream::new`] for examples on how to pass a list of
    /// events to filter
    pub fn into_filtered(self, filter_by: impl AsRef<[Event]>) -> FilteredEventStream {
        FilteredEventStream::new_internal(self, filter_by)
    }
}

impl Stream for EventStream {
    type Item = Event;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.subscription.poll_next_unpin(cx) {
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(msg)) => {
                    // Parse as a cloud event, skipping if we can't do it (and looping around to try
                    // the next poll)
                    let raw_evt: cloudevents::Event = match serde_json::from_slice(&msg.payload) {
                        Ok(evt) => evt,
                        Err(e) => {
                            warn!(error = %e, "Unable to decode message as cloudevent. Skipping message");
                            continue;
                        }
                    };
                    // Convert to our event type, skipping if we can't do it (and looping around to
                    // try the next poll)
                    match Event::try_from(raw_evt) {
                        Ok(evt) => return Poll::Ready(Some(evt)),
                        Err(e) => {
                            warn!(error = ?e, "Unable to decode as event. Skipping message");
                            continue;
                        }
                    }
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl From<FilteredEventStream> for EventStream {
    fn from(value: FilteredEventStream) -> Self {
        value.stream
    }
}

/// A stream of events that has been filtered on certain event types
///
/// Please note that cloning this stream is a bit more expensive as it is creating another NATS
/// subscriber
pub struct FilteredEventStream {
    filters: Vec<Discriminant<Event>>,
    stream: EventStream,
}

impl FilteredEventStream {
    /// Creates a new filter event stream that only returns the given event types, returning an
    /// error if unable to create a subscriber.
    ///
    /// The `topic` param should be a valid topic where lattice events are expected (e.g.
    /// `wasmbus.evt.<lattice_prefix>.>`)
    ///
    /// Unfortunately, due to limitations in how enums work in rust, you need to manually create
    /// some events using `Default`:
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use alt_wadm::{events::*, event_stream::FilteredEventStream};
    /// let client = async_nats::connect("demo.nats.io:4222").await?;
    /// let filter_by = [Event::ActorStarted(ActorStarted::default()), Event::ActorStopped(ActorStopped::default())];
    /// let _stream = FilteredEventStream::new(client, "wasmbus.ctl.default.>", &filter_by).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new(
        client: Client,
        topic: &str,
        filter_by: impl AsRef<[Event]>,
    ) -> Result<FilteredEventStream, NatsError> {
        let es = EventStream::new(client, topic).await?;
        Ok(FilteredEventStream::new_internal(es, filter_by))
    }

    /// Clones the stream by creating a new subscriber for the event bus. This can fail if there is
    /// an error recreating the subscription
    pub async fn try_clone(&self) -> Result<FilteredEventStream, NatsError> {
        let stream = self.stream.try_clone().await?;
        Ok(FilteredEventStream {
            filters: self.filters.clone(),
            stream,
        })
    }

    fn new_internal(es: EventStream, filter_by: impl AsRef<[Event]>) -> FilteredEventStream {
        let filters: Vec<Discriminant<Event>> = filter_by
            .as_ref()
            .iter()
            .map(std::mem::discriminant)
            .collect();
        FilteredEventStream {
            filters,
            stream: es,
        }
    }
}

impl Stream for FilteredEventStream {
    type Item = Event;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.stream.poll_next_unpin(cx) {
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(evt)) => {
                    // Skip any events that aren't contained in our selected types
                    if self.filters.contains(&std::mem::discriminant(&evt)) {
                        return Poll::Ready(Some(evt));
                    } else {
                        // Try to poll again if we skip this one
                        continue;
                    }
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}
