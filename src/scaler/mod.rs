use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::RwLock;

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
        }
    }
    pub async fn event_count(&self) -> usize {
        self.expected_events.read().await.len()
    }

    pub async fn get_events(&self) -> Vec<(Event, Option<Event>)> {
        //TODO: see if I can ref this instead of clone
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
        Ok(())
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
