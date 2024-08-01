//! Contains implementions of durable consumers of events that automatically take a message from a
//! consumer and parse it to concrete types for consumption in a scheduler

use std::fmt::Debug;
use std::ops::{Deref, DerefMut};
use std::time::Duration;

use async_nats::jetstream::{AckKind, Message};
use async_nats::Error as NatsError;
use tracing::{error, warn};

mod commands;
mod events;
pub mod manager;

/// The default time given for a command to ack. This is longer than events due to the possible need for more processing time
pub const DEFAULT_ACK_TIME: Duration = Duration::from_secs(2);

pub const LATTICE_METADATA_KEY: &str = "lattice";
pub const MULTITENANT_METADATA_KEY: &str = "multitenant_prefix";

pub use commands::*;
pub use events::*;

/// An message that is scoped to a specific lattice. This allows to distinguish between items
/// from different lattices and to handle acking. It can support any inner type.
///
/// This type implements AsRef/Mut and Deref/Mut [`Event`] for convenience so it can still be used
/// as a normal event
///
/// When this struct is dropped, it will automatically NAK the event unless an ack has already been
/// sent
pub struct ScopedMessage<T> {
    /// The id of the lattice to which this event belongs
    pub lattice_id: String,

    pub(crate) inner: T,
    // Wrapped in an option so we only do it once
    pub(crate) acker: Option<Message>,
}

impl<T> ScopedMessage<T> {
    /// Acks this Event. This should be called when all work related to this event has been
    /// completed. If this is called before work is done (e.g. like sending a command), instability
    /// could occur. Calling this function again (or after nacking) is a noop.
    ///
    /// This function will only error after it has tried up to 3 times to ack the request. If it
    /// doesn't receive a response after those 3 times, this will return an error.
    pub async fn ack(&mut self) -> Result<(), NatsError> {
        // We want to double ack so we are sure that the server has marked this task as done
        if let Some(msg) = self.acker.take() {
            // Starting at 1 for humans/logging
            let mut retry_count = 1;
            loop {
                match msg.double_ack().await {
                    Ok(_) => break Ok(()),
                    Err(e) if retry_count == 3 => break Err(e),
                    Err(e) => {
                        warn!(error = %e, %retry_count, "Failed to receive ack response, will retry");
                        retry_count += 1;
                        tokio::time::sleep(Duration::from_millis(100)).await
                    }
                }
            }
        } else {
            warn!("Ack has already been sent");
            Ok(())
        }
    }

    /// This is a function for advanced use. If you'd like to send a specific Ack signal back to the
    /// server, use this function
    ///
    /// Returns the underlying NATS error, if any. If an error occurs, you can try to ack again
    pub async fn custom_ack(&mut self, kind: AckKind) -> Result<(), NatsError> {
        if let Some(msg) = self.acker.take() {
            if let Err(e) = msg.ack_with(kind).await {
                // Put it back so it can be called again
                self.acker = Some(msg);
                Err(e)
            } else {
                Ok(())
            }
        } else {
            warn!("Nack has already been sent");
            Ok(())
        }
    }

    /// Nacks this Event. This should be called if there was an error when processing. By default,
    /// this is called when a [`ScopedMessage`] is dropped. Calling this again is a noop.
    ///
    /// This method doesn't have an error because if you nack, it means the message needs to be
    /// rehandled, so the ack wait will eventually expire
    pub async fn nack(&mut self) {
        if let Err(e) = self.custom_ack(AckKind::Nak(None)).await {
            error!(error = %e, "Error when nacking message");
            self.acker = None;
        }
    }
}

impl<T> Drop for ScopedMessage<T> {
    fn drop(&mut self) {
        // self.nack escapes current lifetime, so just manually take the message
        if let Some(msg) = self.acker.take() {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    if let Err(e) = msg.ack_with(AckKind::Nak(None)).await {
                        warn!(error = %e, "Error when sending nack during drop")
                    }
                });
            } else {
                warn!("Couldn't find async runtime to send nack during drop")
            }
        }
    }
}

impl<T> AsRef<T> for ScopedMessage<T> {
    fn as_ref(&self) -> &T {
        &self.inner
    }
}

impl<T> AsMut<T> for ScopedMessage<T> {
    fn as_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

impl<T> Deref for ScopedMessage<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> DerefMut for ScopedMessage<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<T: Debug> Debug for ScopedMessage<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScopedMessage")
            .field("lattice_id", &self.lattice_id)
            .field("inner", &self.inner)
            .finish()
    }
}

/// A helper trait to allow for constructing any consumer
#[async_trait::async_trait]
pub trait CreateConsumer {
    type Output: Unpin;

    /// Create a type of the specified `Output`
    async fn create(
        stream: async_nats::jetstream::stream::Stream,
        topic: &str,
        lattice_id: &str,
        multitenant_prefix: Option<&str>,
    ) -> Result<Self::Output, NatsError>;
}
