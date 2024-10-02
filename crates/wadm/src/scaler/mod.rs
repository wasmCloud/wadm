use std::{sync::Arc, time::Duration};

use anyhow::Result;
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use tokio::{
    sync::{Mutex, RwLock},
    task::JoinHandle,
};
use tracing::{error, instrument, trace, Instrument};
use wadm_types::{api::StatusInfo, TraitProperty};

use crate::{
    commands::Command,
    events::{ComponentScaleFailed, ComponentScaled, Event, ProviderStartFailed, ProviderStarted},
    publisher::Publisher,
    workers::{get_commands_and_result, ConfigSource, SecretSource},
};

pub mod configscaler;
pub mod daemonscaler;
pub mod manager;
pub mod secretscaler;
pub mod spreadscaler;

use manager::Notifications;

use self::configscaler::ConfigScaler;
use self::secretscaler::SecretScaler;

const DEFAULT_WAIT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_SCALER_KIND: &str = "Scaler";

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
    /// specific scalers as needed. wadm scalers implement this by computing a sha256 hash of
    /// all of the parameters that are used to construct the scaler, therefore ensuring that
    /// the ID is unique for each scaler
    fn id(&self) -> &str;

    /// An optional human-friendly name for this scaler. This is used for logging and for selecting
    /// specific scalers as needed. This is optional and by default returns the same value as `id`,
    /// and does not have to be unique
    fn name(&self) -> String {
        self.id().to_string()
    }

    /// An optional kind of scaler. This is used for logging and for selecting specific scalers as needed
    fn kind(&self) -> &str {
        DEFAULT_SCALER_KIND
    }

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

/// The BackoffWrapper is a wrapper around a scaler that is responsible for
/// ensuring that a particular scaler doesn't get overwhelmed with events and has the
/// necessary prerequisites to reconcile.
///
/// 1. `required_config` & `required_secrets`: With the introduction of configuration
///    for wadm applications, the most necessary prerequisite for components, providers
///    and links to start is that their configuration is available. Scalers will not be
///    able to issue commands until the configuration exists.
/// 2. `expected_events`: For scalers that issue commands that should result in events,
///    the BackoffWrapper is responsible for ensuring that the scaler doesn't continually
///    issue commands that it's already expecting events for. Commonly this will allow a host
///    to download larger images from an OCI repository without being bombarded with repeat requests.
/// 3. `backoff_status`: If a scaler receives an event that it was expecting, but it was a failure
///    event, the scaler should back off exponentially while reporting that failure status. This both
///    allows for diagnosing issues with reconciliation and prevents thrashing.
///
/// All of the above effectively allows the inner Scaler to only worry about the logic around
/// reconciling and handling events, rather than be concerned about whether or not
/// it should handle a specific event, if it's causing jitter, overshoot, etc.
///
/// The `notifier` is used to publish notifications to add, remove, or recompute
/// expected events with scalers on other wadm instances, as only one wadm instance
/// at a time will handle a specific event.
pub(crate) struct BackoffWrapper<T, P, C> {
    scaler: T,
    notifier: P,
    notify_subject: String,
    model_name: String,
    required_config: Vec<ConfigScaler<C>>,
    required_secrets: Vec<SecretScaler<C>>,
    /// A list of (success, Option<failure>) events that the scaler is expecting
    #[allow(clippy::type_complexity)]
    expected_events: Arc<RwLock<Vec<(Event, Option<Event>)>>>,
    /// Responsible for clearing up the expected events list after a certain amount of time
    event_cleaner: Mutex<Option<JoinHandle<()>>>,
    /// The amount of time to wait before cleaning up the expected events list
    cleanup_timeout: std::time::Duration,
    /// The status of the scaler, set when the scaler is backing off due to a
    /// failure event.
    backoff_status: Arc<RwLock<Option<StatusInfo>>>,
    // TODO(#253): Figure out where/when/how to store the backoff and exponentially repeat it
    /// Responsible for cleaning up the backoff status after a specified duration
    status_cleaner: Mutex<Option<JoinHandle<()>>>,
}

impl<T, P, C> BackoffWrapper<T, P, C>
where
    T: Scaler + Send + Sync,
    P: Publisher + Send + Sync + 'static,
    C: ConfigSource + SecretSource + Send + Sync + Clone + 'static,
{
    /// Wraps the given scaler in a new BackoffWrapper. `cleanup_timeout` can be set to a
    /// desired waiting time, otherwise it will default to 30s
    pub fn new(
        scaler: T,
        notifier: P,
        required_config: Vec<ConfigScaler<C>>,
        required_secrets: Vec<SecretScaler<C>>,
        notify_subject: &str,
        model_name: &str,
        cleanup_timeout: Option<Duration>,
    ) -> Self {
        Self {
            scaler,
            notifier,
            required_config,
            required_secrets,
            notify_subject: notify_subject.to_owned(),
            model_name: model_name.to_string(),
            expected_events: Arc::new(RwLock::new(Vec::new())),
            event_cleaner: Mutex::new(None),
            cleanup_timeout: cleanup_timeout.unwrap_or(DEFAULT_WAIT_TIMEOUT),
            backoff_status: Arc::new(RwLock::new(None)),
            status_cleaner: Mutex::new(None),
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
        self.set_timed_event_cleanup().await;
    }

    /// Removes an event pair from the expected events list if one matches the given event
    /// Returns a tuple of bools, the first indicating if the event was removed, and the second
    /// indicating if the event was the failure event
    async fn remove_event(&self, event: &Event) -> Result<(bool, bool)> {
        let mut expected_events = self.expected_events.write().await;
        let before_count = expected_events.len();

        let mut failed_event = false;

        expected_events.retain(|(success, fail)| {
            let matches_success = evt_matches_expected(success, event);
            let matches_failure = fail
                .as_ref()
                .map_or(false, |f| evt_matches_expected(f, event));

            // Update failed_event if the event matches the failure event
            failed_event |= matches_failure;

            // Retain the event if it doesn't match either the success or failure event
            !(matches_success || matches_failure)
        });

        Ok((expected_events.len() < before_count, failed_event))
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
        let (expected_event, failed_event) = self.remove_event(event).await?;
        let commands: Vec<Command> = if expected_event {
            // So here, if we receive a failed event that it was "expecting"
            // Then we know that the scaler status is essentially failed and should retry
            // So we should tell the other scalers to remove the event, AND other scalers
            // in the process of removing that event will know that it failed.
            trace!(failed_event, "Scaler received event that it was expecting");
            if failed_event {
                let failed_message = match event {
                    Event::ProviderStartFailed(evt) => evt.error.clone(),
                    Event::ComponentScaleFailed(evt) => evt.error.clone(),
                    _ => format!("Received a failed event of type '{}'", event.raw_type()),
                };
                *self.backoff_status.write().await = Some(StatusInfo::failed(&failed_message));
                // TODO(#253): Here we could refer to a stored previous duration and increase it
                self.set_timed_status_cleanup(std::time::Duration::from_secs(5))
                    .await;
            }
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
        } else if self.backoff_status.read().await.is_some() {
            trace!("Scaler received event but is in backoff, ignoring");
            Vec::with_capacity(0)
        } else {
            trace!("Scaler is not backing off, checking configuration");
            let (mut config_commands, res) = get_commands_and_result(
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

            let (mut secret_commands, res) = get_commands_and_result(
                self.required_secrets
                    .iter()
                    .map(|secret| async { secret.handle_event(event).await }),
                "Errors occurred while handling event with secret scalers",
            )
            .await;

            if let Err(e) = res {
                error!(
                    "Error occurred while handling event with secret scalers: {}",
                    e
                );
            }

            // If the config scalers or secret scalers have commands to send, return them
            if !config_commands.is_empty() || !secret_commands.is_empty() {
                config_commands.append(&mut secret_commands);
                return Ok(config_commands);
            }

            trace!("Scaler required configuration is present, handling event");
            let commands = self.scaler.handle_event(event).await?;

            // Based on the commands, compute the events that we expect to see for this scaler. The scaler
            // will then ignore incoming events until all of the expected events have been received.
            let expected_events = commands.iter().filter_map(|cmd| cmd.corresponding_event());

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
            trace!(%current_event_count, "Scaler is awaiting an event, not reconciling");
            return Ok(Vec::with_capacity(0));
        }
        if self.backoff_status.read().await.is_some() {
            tracing::info!(%current_event_count, "Scaler is backing off, not reconciling");
            return Ok(Vec::with_capacity(0));
        }

        let mut commands = Vec::new();
        for config in &self.required_config {
            config.reconcile().await?.into_iter().for_each(|cmd| {
                commands.push(cmd);
            });
        }

        let mut secret_commands = Vec::new();
        for secret in &self.required_secrets {
            secret.reconcile().await?.into_iter().for_each(|cmd| {
                secret_commands.push(cmd);
            });
        }
        commands.append(secret_commands.as_mut());

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
                        .filter_map(|command| command.corresponding_event()),
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
        let mut commands = self.scaler.cleanup().await.unwrap_or_default();
        for config in self.required_config.iter() {
            match config.cleanup().await {
                Ok(cmds) => commands.extend(cmds),
                // Explicitly logging, but continuing, in the case of an error to make sure
                // we don't prevent other cleanup tasks from running
                Err(e) => {
                    error!("Error occurred while cleaning up config scalers: {}", e);
                }
            }
        }

        for secret in self.required_secrets.iter() {
            match secret.cleanup().await {
                Ok(cmds) => commands.extend(cmds),
                // Explicitly logging, but continuing, in the case of an error to make sure
                // we don't prevent other cleanup tasks from running
                Err(e) => {
                    error!("Error occurred while cleaning up secret scalers: {}", e);
                }
            }
        }

        Ok(commands)
    }

    /// Sets a timed cleanup task to clear the expected events list after a timeout
    async fn set_timed_event_cleanup(&self) {
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

    /// Sets a timed cleanup task to clear the expected events list after a timeout
    async fn set_timed_status_cleanup(&self, timeout: Duration) {
        let mut status_cleaner = self.status_cleaner.lock().await;
        // Clear any existing handle
        if let Some(handle) = status_cleaner.take() {
            handle.abort();
        }
        let backoff_status = self.backoff_status.clone();

        *status_cleaner = Some(tokio::spawn(
            async move {
                tokio::time::sleep(timeout).await;
                trace!("Reached status cleanup timeout, clearing backoff status");
                backoff_status.write().await.take();
            }
            .instrument(tracing::trace_span!("status_cleaner", scaler_id = %self.id())),
        ));
    }
}

#[async_trait]
/// The [`Scaler`] trait implementation for the [`BackoffWrapper`] is mostly a simple wrapper,
/// with three exceptions, which allow scalers to sync state between different wadm instances.
///
/// * `handle_event` calls an internal method that uses a notifier to publish notifications to
///   all Scalers, even running on different wadm instances, to handle that event. The resulting
///   commands from those scalers are ignored as this instance is already handling the event.
/// * `reconcile` calls an internal method that uses a notifier to ensure all Scalers, even
///   running on different wadm instances, compute their expected events in response to the
///   reconciliation commands in order to "back off".
/// * `status` will first check to see if the scaler is in a backing off state, and if so, return
///   the backoff status. Otherwise, it will return the status of the scaler.
impl<T, P, C> Scaler for BackoffWrapper<T, P, C>
where
    T: Scaler + Send + Sync,
    P: Publisher + Send + Sync + 'static,
    C: ConfigSource + SecretSource + Send + Sync + Clone + 'static,
{
    fn id(&self) -> &str {
        // Pass through the ID of the wrapped scaler
        self.scaler.id()
    }

    fn kind(&self) -> &str {
        // Pass through the kind of the wrapped scaler
        self.scaler.kind()
    }

    fn name(&self) -> String {
        self.scaler.name()
    }

    async fn status(&self) -> StatusInfo {
        // If the scaler has a backoff status, return that, otherwise return the status of the scaler
        if let Some(status) = self.backoff_status.read().await.clone() {
            status
        } else {
            self.scaler.status().await
        }
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
                provider_ref: i1,
                host_id: h1,
                ..
            }),
            Event::ProviderStartFailed(ProviderStartFailed {
                provider_id: p2,
                provider_ref: i2,
                host_id: h2,
                ..
            }),
        ) => p1 == p2 && h1 == h2 && i1 == i2,
        (
            Event::ComponentScaled(ComponentScaled {
                annotations: a1,
                image_ref: i1,
                component_id: c1,
                host_id: h1,
                ..
            }),
            Event::ComponentScaled(ComponentScaled {
                annotations: a2,
                image_ref: i2,
                component_id: c2,
                host_id: h2,
                ..
            }),
        ) => a1 == a2 && i1 == i2 && c1 == c2 && h1 == h2,
        (
            Event::ComponentScaleFailed(ComponentScaleFailed {
                annotations: a1,
                image_ref: i1,
                component_id: c1,
                host_id: h1,
                ..
            }),
            Event::ComponentScaleFailed(ComponentScaleFailed {
                annotations: a2,
                image_ref: i2,
                component_id: c2,
                host_id: h2,
                ..
            }),
        ) => a1 == a2 && i1 == i2 && c1 == c2 && h1 == h2,
        _ => false,
    }
}

/// Computes the sha256 digest of the given parameters to form a unique ID for a scaler
pub(crate) fn compute_id_sha256(params: &[&str]) -> String {
    let mut hasher = Sha256::new();
    for param in params {
        hasher.update(param.as_bytes())
    }
    let hash = hasher.finalize();
    format!("{hash:x}")
}
