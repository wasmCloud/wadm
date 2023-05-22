use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::{
    sync::{Mutex, RwLock},
    task::JoinHandle,
};
use tracing::log::trace;

use crate::{
    commands::Command,
    events::{
        ActorsStartFailed, ActorsStarted, ActorsStopped, Event, Linkdef, LinkdefSet,
        ProviderStartFailed, ProviderStarted,
    },
    model::TraitProperty,
};

pub mod manager;
mod simplescaler;
pub mod spreadscaler;

/// A trait describing a struct that can be configured to compute the difference between
/// desired state and configured state, returning a set of commands to approach desired state.
///
/// Implementers of this trait can choose how to access state, but it's generally recommended to
/// use a [ReadStore](crate::storage::ReadStore) so that it can retrieve current information about
/// state using a common trait that only allows store access and not modification
///
/// Typically a Scaler should be configured with `update_config`, then use the `reconcile` method
/// for an inital set of commands. As events change the state, they should also be given to the Scaler
/// to determine if actions need to be taken in response to an event
#[async_trait]
pub trait Scaler {
    /// Provide a scaler with configuration to use internally when computing commands This should
    /// trigger a reconcile with the new configuration.
    ///
    /// This config can be anything that can be turned into a
    /// [`TraitProperty`](crate::model::TraitProperty). Additional configuration outside of what is
    /// available in a `TraitProperty` can be passed when constructing the scaler
    async fn update_config(&mut self, config: TraitProperty) -> Result<Vec<Command>>;

    /// Compute commands that must be taken given an event that changes the lattice state
    async fn handle_event(&self, event: &Event) -> Result<Vec<Command>>;

    /// Compute commands that must be taken to achieve desired state as specified in config
    async fn reconcile(&self) -> Result<Vec<Command>>;

    /// Returns the list of commands needed to cleanup for a scaler
    ///
    /// This purposefully does not consume the scaler so that if there is a failure it can be kept
    /// around
    async fn cleanup(&self) -> Result<Vec<Command>>;
}

/// A Scaler wrapper struct that can compute a proper backoff for a scaler based
/// on its commands and avoids delivering events to the scaler that would cause
/// it to issue duplicate commands
//TODO(brooksmtownsend): Consider if this should be pub or pub(crate). Gut feeling says that this shouldn't be a type that external crate users are exposed to, they should just be using the Scaler trait. However changing this would mark a lot of different type as pub(crate)
pub struct BackoffAwareScaler {
    scaler: Box<dyn Scaler + Send + Sync>,
    pub model_name: String,
    /// A list of (success, Option<failure>) events that the scaler is expecting
    #[allow(clippy::type_complexity)]
    expected_events: Arc<RwLock<Vec<(Event, Option<Event>)>>>,
    /// Responsible for clearing up the expected events list after a certain amount of time
    event_cleaner: Mutex<Option<JoinHandle<()>>>,
    /// The amount of time to wait before cleaning up the expected events list
    cleanup_timeout: std::time::Duration,
}

impl std::ops::Deref for BackoffAwareScaler {
    type Target = Box<dyn Scaler + Send + Sync>;

    fn deref(&self) -> &Self::Target {
        &self.scaler
    }
}

impl BackoffAwareScaler {
    pub fn new(scaler: Box<dyn Scaler + Send + Sync>, model_name: &str) -> Self {
        Self {
            scaler,
            model_name: model_name.to_string(),
            expected_events: Arc::new(RwLock::new(Vec::new())),
            event_cleaner: Mutex::new(None),
            // This is hardcoded for now but could be adjusted based on the scaler in the future if needed
            cleanup_timeout: std::time::Duration::from_secs(30),
        }
    }

    pub async fn event_count(&self) -> usize {
        self.expected_events.read().await.len()
    }

    pub async fn get_events(&self) -> Vec<(Event, Option<Event>)> {
        self.expected_events.read().await.clone()
    }

    /// Returns true if the given event is expected by the scaler either as a successful event
    /// or a failure event
    pub async fn expecting_event(&self, event: &Event) -> bool {
        let expected_events = self.expected_events.read().await;
        expected_events.iter().any(|(success, fail)| {
            evt_matches_expected(success, event)
                || fail
                    .as_ref()
                    .map(|f| evt_matches_expected(f, event))
                    .unwrap_or(false)
        })
    }

    /// Adds events to the expected events list
    ///
    /// # Arguments
    /// `events` - A list of (success, failure) events to add to the expected events list
    /// `clear_previous` - If true, clears the previous expected events list before adding the new events
    pub async fn add_events<I>(&self, events: I, clear_previous: bool) -> Result<()>
    where
        I: IntoIterator<Item = (Event, Option<Event>)>,
    {
        let mut expected_events = self.expected_events.write().await;
        if clear_previous {
            expected_events.clear();
        }
        expected_events.extend(events);
        self.set_timed_cleanup().await;

        Ok(())
    }

    /// Removes an event pair from the expected events list if one matches the given event
    /// Returns true if the event was removed, false otherwise
    pub async fn remove_event(&self, event: &Event) -> Result<bool> {
        let mut expected_events = self.expected_events.write().await;
        let before_count = expected_events.len();
        expected_events.retain(|(success, fail)| {
            !evt_matches_expected(success, event)
                && !fail
                    .as_ref()
                    .map(|f| evt_matches_expected(f, event))
                    .unwrap_or(false)
        });
        Ok(expected_events.len() != before_count)
    }

    /// Sets the expected events list to the given events
    pub async fn set_events(&self, events: Vec<Event>) -> Result<()> {
        let mut expected_events = self.expected_events.write().await;
        expected_events.clear();
        expected_events.extend(events.into_iter().map(|e| (e, None)));
        self.set_timed_cleanup().await;

        Ok(())
    }

    /// Handles an incoming event for the given scaler.
    ///
    /// This function processes the event and returns a vector of commands to be executed.
    /// It also manages the expected events list, removing successfully handled events
    /// and adding new expected events based on the executed commands.
    ///
    /// # Arguments
    ///
    /// * `scaler`: A reference to the `ScalerWithEvents` struct which represents the scaler with events.
    /// * `event`: A reference to the `Event` struct which represents the incoming event to be handled.
    ///
    /// # Returns
    ///
    /// * `Result<Vec<Command>>`: A `Result` containing a vector of `Command` structs if successful,
    ///   or an error of type `anyhow::Error` if any error occurs while processing the event.
    pub async fn handle_event(
        &self,
        event: &Event,
        model_name: &str,
    ) -> anyhow::Result<Vec<Command>> {
        let commands = if self.remove_event(event).await? {
            trace!("Scaler received event that it was expecting");
            // The scaler was expecting this event and it shouldn't respond with commands
            vec![]
        } else if self.event_count().await > 0 {
            // If a scaler is expecting events still, don't have it handle events. This is effectively
            // the backoff mechanism within wadm
            trace!("Scaler received event but is still expecting events, ignoring");
            vec![]
        } else {
            let commands = self.scaler.handle_event(event).await?;
            // TODO: need to notify here to add events

            // Based on the commands, compute the events that we expect to see for this scaler. The scaler
            // will then ignore incoming events until all of the expected events have been received.
            let expected_events = commands
                .iter()
                .filter_map(|cmd| cmd.corresponding_event(model_name));
            self.add_events(expected_events, false).await?;
            commands
        };

        Ok(commands)
    }

    /// Sets a timed cleanup task to clear the expected events list after a timeout
    async fn set_timed_cleanup(&self) {
        let mut event_cleaner = self.event_cleaner.lock().await;
        // Clear any existing handle
        if let Some(handle) = self.event_cleaner.lock().await.take() {
            handle.abort();
        }
        let expected_events = self.expected_events.clone();
        let timeout = self.cleanup_timeout.clone();

        *event_cleaner = Some(tokio::spawn(async move {
            tokio::time::sleep(timeout).await;
            // We don't use the method to clear here because of borrowing semantics.
            expected_events.write().await.clear();
        }));
    }
}

#[async_trait]
impl Scaler for BackoffAwareScaler {
    async fn update_config(&mut self, config: TraitProperty) -> Result<Vec<Command>> {
        self.scaler.update_config(config).await
    }

    async fn handle_event(&self, event: &Event) -> Result<Vec<Command>> {
        self.scaler.handle_event(event).await
    }

    async fn reconcile(&self) -> Result<Vec<Command>> {
        self.scaler.reconcile().await
    }

    async fn cleanup(&self) -> Result<Vec<Command>> {
        self.scaler.cleanup().await
    }
}

/// A specialized function that compares an incoming lattice event to an "expected" event
/// stored alongside a [Scaler](Scaler).
///
/// This is not a PartialEq or Eq implementation because there are strict assumptions that do not always hold.
/// For example, an incoming and expected event are equal even if their claims are not equal, because we cannot
/// compute that information from a [Command](Command). However, this is not a valid comparison if actually
/// comparing two events for equality.
fn evt_matches_expected(incoming: &Event, expected: &Event) -> bool {
    match (incoming, expected) {
        (
            // NOTE(brooksmtownsend): It may be worth it to simply use the count here as
            // extra information. If we receive the exact event but the count is different, that
            // may mean some instances failed to start on that host. The cause for this isn't
            // well known but if we find ourselves missing expected events we should revisit
            Event::ActorsStarted(ActorsStarted {
                annotations: a1,
                image_ref: i1,
                count: c1,
                host_id: h1,
                ..
            }),
            Event::ActorsStarted(ActorsStarted {
                annotations: a2,
                image_ref: i2,
                count: c2,
                host_id: h2,
                ..
            }),
        ) => a1 == a2 && i1 == i2 && c1 == c2 && h1 == h2,
        (
            Event::ActorsStartFailed(ActorsStartFailed {
                annotations: a1,
                image_ref: i1,
                host_id: h1,
                ..
            }),
            Event::ActorsStartFailed(ActorsStartFailed {
                annotations: a2,
                image_ref: i2,
                host_id: h2,
                ..
            }),
        ) => a1 == a2 && i1 == i2 && h1 == h2,
        (
            Event::ActorsStopped(ActorsStopped {
                annotations: a1,
                public_key: p1,
                count: c1,
                host_id: h1,
                ..
            }),
            Event::ActorsStopped(ActorsStopped {
                annotations: a2,
                public_key: p2,
                count: c2,
                host_id: h2,
                ..
            }),
        ) => a1 == a2 && p1 == p2 && c1 == c2 && h1 == h2,
        (
            Event::ProviderStarted(ProviderStarted {
                annotations: a1,
                image_ref: i1,
                link_name: l1,
                host_id: h1,
                ..
            }),
            Event::ProviderStarted(ProviderStarted {
                annotations: a2,
                image_ref: i2,
                link_name: l2,
                host_id: h2,
                ..
            }),
        ) => a1 == a2 && i1 == i2 && l1 == l2 && h1 == h2,
        // NOTE(brooksmtownsend): This is a little less information than we really need here.
        // Image reference + annotations would be nice
        (
            Event::ProviderStartFailed(ProviderStartFailed {
                link_name: l1,
                host_id: h1,
                ..
            }),
            Event::ProviderStartFailed(ProviderStartFailed {
                link_name: l2,
                host_id: h2,
                ..
            }),
        ) => l1 == l2 && h1 == h2,
        (
            Event::LinkdefSet(LinkdefSet {
                linkdef:
                    Linkdef {
                        actor_id: a1,
                        contract_id: c1,
                        link_name: l1,
                        provider_id: p1,
                        values: v1,
                        ..
                    },
            }),
            Event::LinkdefSet(LinkdefSet {
                linkdef:
                    Linkdef {
                        actor_id: a2,
                        contract_id: c2,
                        link_name: l2,
                        provider_id: p2,
                        values: v2,
                        ..
                    },
            }),
        ) => a1 == a2 && c1 == c2 && l1 == l2 && p1 == p2 && v1 == v2,
        _ => false,
    }
}
