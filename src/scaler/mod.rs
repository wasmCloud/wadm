use std::{sync::Arc, time::Duration};

use anyhow::Result;
use async_trait::async_trait;
use base64::{engine::general_purpose, Engine as _};
use tokio::{
    sync::{Mutex, RwLock},
    task::JoinHandle,
};
use tracing::{error, instrument, trace, Instrument};

use crate::{
    commands::Command,
    events::{Event, ProviderStartFailed, ProviderStarted},
    model::TraitProperty,
    publisher::Publisher,
    server::StatusInfo,
    workers::{get_commands_and_result, ConfigSource},
};

pub mod configscaler;
pub mod daemonscaler;
pub mod manager;
pub mod spreadscaler;

use manager::Notifications;

use self::configscaler::ConfigScaler;

const DEFAULT_WAIT_TIMEOUT: Duration = Duration::from_secs(30);

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
    /// A unique identifier for this scaler type. This is used for logging and for selecting
    /// specific scalers as needed. Generally this should be something like
    /// `$NAME_OF_SCALER_TYPE-$MODEL_NAME-$OCI_REF`. However, the only requirement is that it can
    /// uniquely identify a scaler
    fn id(&self) -> &str;

    /// Determine the status of this scaler according to reconciliation logic. This is the opportunity
    /// for scalers to indicate that they are unhealthy with a message as to what's missing.
    async fn status(&self) -> StatusInfo;

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

/// The BackoffAwareScaler is a wrapper around a scaler that is responsible for
/// ensuring that a particular [Scaler] has the proper prerequisites in place
/// and should be able to reconcile and issue commands.
///
/// 1. With the introduction of configuration for wadm applications, the most necessary
/// prerequisite for components, providers and links to start is that their
/// configuration is available. Scalers will not be able to issue commands until
/// the configuration exists.
/// 2. For scalers that issue commands that take a long time to complete, like downloading
/// an image for a provider, the BackoffAwareScaler will ensure that the scaler is not
/// overwhelmed with events and will back off until the expected events have been received.
/// 3. In the future (#253) this wrapper should also be responsible for exponential backoff
/// when a scaler is repeatedly issuing the same commands to prevent thrashing.
///
/// All of the above effectively allows the inner Scaler to only worry about the logic around
/// reconciling and handling events, rather than be concerned about whether or not
/// it should handle a specific event, if it's causing jitter, overshoot, etc.
///
/// The `notifier` is used to publish notifications to add, remove, or recompute
/// expected events with scalers on other wadm instances, as only one wadm instance
/// at a time will handle a specific event.
pub(crate) struct BackoffAwareScaler<T, P, S> {
    scaler: T,
    notifier: P,
    notify_subject: String,
    model_name: String,
    required_config: Vec<ConfigScaler<S>>,
    /// A list of (success, Option<failure>) events that the scaler is expecting
    #[allow(clippy::type_complexity)]
    expected_events: Arc<RwLock<Vec<(Event, Option<Event>)>>>,
    /// Responsible for clearing up the expected events list after a certain amount of time
    event_cleaner: Mutex<Option<JoinHandle<()>>>,
    /// The amount of time to wait before cleaning up the expected events list
    cleanup_timeout: std::time::Duration,
}

impl<T, P, S> BackoffAwareScaler<T, P, S>
where
    T: Scaler + Send + Sync,
    P: Publisher + Send + Sync + 'static,
    C: ConfigSource + Send + Sync + Clone + 'static,
{
    /// Wraps the given scaler in a new backoff aware scaler. `cleanup_timeout` can be set to a
    /// desired waiting time, otherwise it will default to 30s
    pub fn new(
        scaler: T,
        notifier: P,
        required_config: Vec<ConfigScaler<S>>,
        notify_subject: &str,
        model_name: &str,
        cleanup_timeout: Option<Duration>,
    ) -> Self {
        Self {
            scaler,
            notifier,
            required_config,
            notify_subject: notify_subject.to_owned(),
            model_name: model_name.to_string(),
            expected_events: Arc::new(RwLock::new(Vec::new())),
            event_cleaner: Mutex::new(None),
            cleanup_timeout: cleanup_timeout.unwrap_or(DEFAULT_WAIT_TIMEOUT),
        }
    }

    pub async fn event_count(&self) -> usize {
        self.expected_events.read().await.len()
    }

    /// Adds events to the expected events list
    ///
    /// # Arguments
    /// `events` - A list of (success, failure) events to add to the expected events list
    /// `clear_previous` - If true, clears the previous expected events list before adding the new events
    async fn add_events<I>(&self, events: I, clear_previous: bool)
    where
        I: IntoIterator<Item = (Event, Option<Event>)>,
    {
        let mut expected_events = self.expected_events.write().await;
        if clear_previous {
            expected_events.clear();
        }
        expected_events.extend(events);
        self.set_timed_cleanup().await;
    }

    /// Removes an event pair from the expected events list if one matches the given event
    /// Returns true if the event was removed, false otherwise
    async fn remove_event(&self, event: &Event) -> Result<bool> {
        let mut expected_events = self.expected_events.write().await;
        let before_count = expected_events.len();
        expected_events.retain(|(success, fail)| {
            // Retain the event if it doesn't match either the success or optional failure event.
            // Most events have a possibility of seeing a failure and either one means we saw the
            // event we were expecting
            !evt_matches_expected(success, event)
                && !fail
                    .as_ref()
                    .map(|f| evt_matches_expected(f, event))
                    .unwrap_or(false)
        });
        Ok(expected_events.len() != before_count)
    }

    /// Handles an incoming event for the given scaler.
    ///
    /// This function processes the event and returns a vector of commands to be executed.
    /// It also manages the expected events list, removing successfully handled events
    /// and adding new expected events based on the executed commands, and using the notifier
    /// to send notifications to other scalers running on different wadm instances.
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
    #[instrument(level = "trace", skip_all, fields(scaler_id = %self.id()))]
    async fn handle_event_internal(&self, event: &Event) -> anyhow::Result<Vec<Command>> {
        let model_name = &self.model_name;
        let commands: Vec<Command> = if self.remove_event(event).await? {
            trace!("Scaler received event that it was expecting");
            let data = serde_json::to_vec(&Notifications::RemoveExpectedEvent {
                name: model_name.to_owned(),
                scaler_id: self.scaler.id().to_owned(),
                event: event.to_owned().try_into()?,
            })?;
            self.notifier
                .publish(data, Some(&self.notify_subject))
                .await?;

            // The scaler was expecting this event and it shouldn't respond with commands
            Vec::with_capacity(0)
        } else if self.event_count().await > 0 {
            trace!("Scaler received event but is still expecting events, ignoring");
            // If a scaler is expecting events still, don't have it handle events. This is effectively
            // the backoff mechanism within wadm
            Vec::with_capacity(0)
        } else {
            trace!("Scaler is not backing off, checking configuration");
            let (commands, res) = get_commands_and_result(
                self.required_config
                    .iter()
                    .map(|config| async { config.handle_event(event).await }),
                "Errors occurred while handling event with config scalers",
            )
            .await;

            if let Err(e) = res {
                error!(
                    "Error occurred while handling event with config scalers: {}",
                    e
                );
            }

            if !commands.is_empty() {
                return Ok(commands);
            }

            trace!("Scaler required configuration is present, handling event");
            let commands = self.scaler.handle_event(event).await?;

            // Based on the commands, compute the events that we expect to see for this scaler. The scaler
            // will then ignore incoming events until all of the expected events have been received.
            let expected_events = commands
                .iter()
                .filter_map(|cmd| cmd.corresponding_event(model_name));

            self.add_events(expected_events, false).await;

            // Only let other scalers know if we generated commands to take
            if !self.expected_events.read().await.is_empty() {
                trace!("Scaler generated commands, notifying other scalers to register expected events");
                let data = serde_json::to_vec(&Notifications::RegisterExpectedEvents {
                    name: model_name.to_owned(),
                    scaler_id: self.scaler.id().to_owned(),
                    triggering_event: Some(event.to_owned().try_into()?),
                })?;

                self.notifier
                    .publish(data, Some(&self.notify_subject))
                    .await?;
            }
            commands
        };

        Ok(commands)
    }

    #[instrument(level = "trace", skip_all, fields(scaler_id = %self.id()))]
    async fn reconcile_internal(&self) -> Result<Vec<Command>> {
        // If we're already in backoff, return an empty list
        let current_event_count = self.event_count().await;
        if current_event_count > 0 {
            trace!(%current_event_count, "Scaler is backing off, not reconciling");
            return Ok(Vec::with_capacity(0));
        }

        let mut commands = Vec::new();
        for config in &self.required_config {
            config.reconcile().await?.into_iter().for_each(|cmd| {
                commands.push(cmd);
            });
        }

        if !commands.is_empty() {
            return Ok(commands);
        }

        match self.scaler.reconcile().await {
            // "Back off" scaler with expected corresponding events if the scaler generated commands
            Ok(commands) if !commands.is_empty() => {
                // Generate expected events
                self.add_events(
                    commands
                        .iter()
                        .filter_map(|command| command.corresponding_event(&self.model_name)),
                    true,
                )
                .await;

                if !self.expected_events.read().await.is_empty() {
                    trace!("Reconcile generated expected events, notifying other scalers to register expected events");
                    let data = serde_json::to_vec(&Notifications::RegisterExpectedEvents {
                        name: self.model_name.to_owned(),
                        scaler_id: self.scaler.id().to_owned(),
                        triggering_event: None,
                    })?;
                    self.notifier
                        .publish(data, Some(&self.notify_subject))
                        .await?;
                    return Ok(commands);
                }

                Ok(commands)
            }
            Ok(commands) => {
                trace!("Reconcile generated no commands, no need to register expected events");
                Ok(commands)
            }
            Err(e) => Err(e),
        }
    }

    async fn cleanup_internal(&self) -> Result<Vec<Command>> {
        let mut commands = Vec::new();
        for config in &self.required_config {
            config.cleanup().await?.into_iter().for_each(|cmd| {
                commands.push(cmd);
            });
        }
        self.scaler.cleanup().await.map(|mut cmds| {
            commands.append(&mut cmds);
            commands
        })
    }

    /// Sets a timed cleanup task to clear the expected events list after a timeout
    async fn set_timed_cleanup(&self) {
        let mut event_cleaner = self.event_cleaner.lock().await;
        // Clear any existing handle
        if let Some(handle) = event_cleaner.take() {
            handle.abort();
        }
        let expected_events = self.expected_events.clone();
        let timeout = self.cleanup_timeout;

        *event_cleaner = Some(tokio::spawn(
            async move {
                tokio::time::sleep(timeout).await;
                trace!("Reached event cleanup timeout, clearing expected events");
                expected_events.write().await.clear();
            }
            .instrument(tracing::trace_span!("event_cleaner", scaler_id = %self.id())),
        ));
    }
}

#[async_trait]
/// The [Scaler](Scaler) trait implementation for the [BackoffAwareScaler](BackoffAwareScaler)
/// is mostly a simple wrapper, with two exceptions, which allow scalers to sync expected
/// events between different wadm instances.
///
/// * `handle_event` calls an internal method that uses a notifier to publish notifications to
///   all Scalers, even running on different wadm instances, to handle that event. The resulting
///   commands from those scalers are ignored as this instance is already handling the event.
/// * `reconcile` calls an internal method that uses a notifier to ensure all Scalers, even
///   running on different wadm instances, compute their expected events in response to the
///   reconciliation commands in order to "back off".
impl<T, P, S> Scaler for BackoffAwareScaler<T, P, S>
where
    T: Scaler + Send + Sync,
    P: Publisher + Send + Sync + 'static,
    C: ConfigSource + Send + Sync + Clone + 'static,
{
    fn id(&self) -> &str {
        // Pass through the ID of the wrapped scaler
        self.scaler.id()
    }

    async fn status(&self) -> StatusInfo {
        self.scaler.status().await
    }

    async fn update_config(&mut self, config: TraitProperty) -> Result<Vec<Command>> {
        self.scaler.update_config(config).await
    }

    async fn handle_event(&self, event: &Event) -> Result<Vec<Command>> {
        self.handle_event_internal(event).await
    }

    async fn reconcile(&self) -> Result<Vec<Command>> {
        self.reconcile_internal().await
    }

    async fn cleanup(&self) -> Result<Vec<Command>> {
        self.cleanup_internal().await
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
            Event::ProviderStarted(ProviderStarted {
                annotations: a1,
                image_ref: i1,
                host_id: h1,
                provider_id: p1,
                ..
            }),
            Event::ProviderStarted(ProviderStarted {
                annotations: a2,
                image_ref: i2,
                host_id: h2,
                provider_id: p2,
                ..
            }),
        ) => a1 == a2 && i1 == i2 && p1 == p2 && h1 == h2,
        (
            Event::ProviderStartFailed(ProviderStartFailed {
                provider_id: p1,
                host_id: h1,
                ..
            }),
            Event::ProviderStartFailed(ProviderStartFailed {
                provider_id: p2,
                host_id: h2,
                ..
            }),
        ) => p1 == p2 && h1 == h2,
        _ => false,
    }
}

/// Hash the named configurations to generate a unique identifier for the scaler
///
/// This is only called when the config is not empty so we don't need to worry about
/// returning empty strings.
pub(crate) fn compute_config_hash(config: &[String]) -> String {
    general_purpose::STANDARD.encode(config.join("_"))
}
