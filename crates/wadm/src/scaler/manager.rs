//! A struct that manages creating and removing scalers for all manifests

use std::{collections::HashMap, ops::Deref, sync::Arc, time::Duration};

use anyhow::Result;
use async_nats::jetstream::{
    consumer::pull::{Config as PullConfig, Stream as MessageStream},
    kv::Store as KvStore,
    stream::Stream as JsStream,
    AckKind,
};
use cloudevents::Event as CloudEvent;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{OwnedRwLockReadGuard, RwLock},
    task::JoinHandle,
};
use tracing::{debug, error, instrument, trace, warn};
use wadm_types::{
    api::{Status, StatusInfo},
    CapabilityProperties, Component, ComponentProperties, ConfigProperty, Manifest, Policy,
    Properties, SecretProperty, SpreadScalerProperty, Trait, TraitProperty, DAEMONSCALER_TRAIT,
    LINK_TRAIT, SPREADSCALER_TRAIT,
};
use wasmcloud_secrets_types::SECRET_PREFIX;

use crate::{
    events::Event,
    publisher::Publisher,
    scaler::{spreadscaler::ComponentSpreadScaler, Command, Scaler},
    storage::{snapshot::SnapshotStore, ReadStore},
    workers::{CommandPublisher, ConfigSource, LinkSource, SecretSource, StatusPublisher},
    DEFAULT_LINK_NAME,
};

use super::{
    configscaler::ConfigScaler,
    daemonscaler::{provider::ProviderDaemonScaler, ComponentDaemonScaler},
    secretscaler::SecretScaler,
    spreadscaler::{
        link::{LinkScaler, LinkScalerConfig},
        provider::{ProviderSpreadConfig, ProviderSpreadScaler},
    },
    BackoffWrapper,
};

pub type BoxedScaler = Box<dyn Scaler + Send + Sync + 'static>;
pub type ScalerList = Vec<BoxedScaler>;

pub const WADM_NOTIFY_PREFIX: &str = "wadm.notify";

/// All events sent for manifest notifications
#[derive(Debug, Serialize, Deserialize)]
pub enum Notifications {
    CreateScalers(Manifest),
    DeleteScalers(String),
    /// Register expected events for a manifest. You can either trigger this with an event (which
    /// will result in calling `handle_event`) or without in order to calculate expected events with
    /// a full reconcile (like on first deploy), rather than just handling a single event
    RegisterExpectedEvents {
        name: String,
        scaler_id: String,
        triggering_event: Option<CloudEvent>,
    },
    /// Remove an event from the expected list for a manifest scaler
    RemoveExpectedEvent {
        name: String,
        scaler_id: String,
        event: CloudEvent,
    },
}

/// A wrapper type returned when getting a list of scalers for a model
pub struct Scalers {
    pub scalers: OwnedRwLockReadGuard<HashMap<String, ScalerList>, ScalerList>,
}

impl Deref for Scalers {
    type Target = ScalerList;

    fn deref(&self) -> &Self::Target {
        self.scalers.deref()
    }
}

/// A wrapper type returned when getting all scalers.
pub struct AllScalers {
    pub scalers: OwnedRwLockReadGuard<HashMap<String, ScalerList>>,
}

impl Deref for AllScalers {
    type Target = HashMap<String, ScalerList>;

    fn deref(&self) -> &Self::Target {
        self.scalers.deref()
    }
}

/// A wrapper type returned when getting a specific scaler
pub struct SingleScaler {
    pub scaler: OwnedRwLockReadGuard<HashMap<String, ScalerList>, BoxedScaler>,
}

impl Deref for SingleScaler {
    type Target = BoxedScaler;

    fn deref(&self) -> &Self::Target {
        self.scaler.deref()
    }
}

/// A manager that consumes notifications from a stream for a lattice and then either adds or removes the
/// necessary scalers
#[derive(Clone)]
pub struct ScalerManager<StateStore, P: Clone, L: Clone> {
    handle: Option<Arc<JoinHandle<Result<()>>>>,
    scalers: Arc<RwLock<HashMap<String, ScalerList>>>,
    client: P,
    subject: String,
    lattice_id: String,
    command_publisher: CommandPublisher<P>,
    status_publisher: StatusPublisher<P>,
    snapshot_data: SnapshotStore<StateStore, L>,
}

impl<StateStore, P: Clone, L: Clone> Drop for ScalerManager<StateStore, P, L> {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort()
        }
    }
}

impl<StateStore, P, L> ScalerManager<StateStore, P, L>
where
    StateStore: ReadStore + Send + Sync + Clone + 'static,
    P: Publisher + Clone + Send + Sync + 'static,
    L: LinkSource + ConfigSource + SecretSource + Clone + Send + Sync + 'static,
{
    /// Creates a new ScalerManager configured to notify messages to `wadm.notify.{lattice_id}`
    /// using the given jetstream client. Also creates an ephemeral consumer for notifications on
    /// the given stream
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        client: P,
        stream: JsStream,
        lattice_id: &str,
        multitenant_prefix: Option<&str>,
        state_store: StateStore,
        manifest_store: KvStore,
        command_publisher: CommandPublisher<P>,
        status_publisher: StatusPublisher<P>,
        link_getter: L,
    ) -> Result<ScalerManager<StateStore, P, L>> {
        // Create the consumer first so that we can make sure we don't miss anything during the
        // first reconcile pass
        let subject = format!("{WADM_NOTIFY_PREFIX}.{lattice_id}");
        let consumer = stream
            .create_consumer(PullConfig {
                // TODO(thomastaylor312): We should probably generate a friendly consumer name
                // using an optional unique identifier
                description: Some(format!(
                    "Ephemeral wadm notifier consumer for lattice {lattice_id}"
                )),
                ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
                ack_wait: std::time::Duration::from_secs(2),
                max_deliver: 3,
                deliver_policy: async_nats::jetstream::consumer::DeliverPolicy::All,
                filter_subject: subject.clone(),
                ..Default::default()
            })
            .await
            .map_err(|e| anyhow::anyhow!("Unable to create ephemeral consumer: {e:?}"))?;
        let messages = consumer
            .messages()
            .await
            .map_err(|e| anyhow::anyhow!("Unable to subscribe to consumer: {e:?}"))?;

        // Get current scalers set up
        let manifest_store = crate::server::ModelStorage::new(manifest_store);
        let futs = manifest_store
            .list(multitenant_prefix, lattice_id)
            .await?
            .into_iter()
            .map(|summary| {
                manifest_store.get(multitenant_prefix, lattice_id, summary.name().to_owned())
            });
        let all_manifests = futures::future::join_all(futs)
            .await
            .into_iter()
            .filter_map(|manifest| manifest.transpose())
            .map(|res| res.map(|(manifest, _)| manifest))
            .collect::<Result<Vec<_>>>()?;
        let snapshot_data = SnapshotStore::new(
            state_store.clone(),
            link_getter.clone(),
            lattice_id.to_owned(),
        );
        let scalers: HashMap<String, ScalerList> = all_manifests
            .into_iter()
            .filter_map(|manifest| {
                let data = manifest.get_deployed()?;
                let name = manifest.name().to_owned();
                let scalers = components_to_scalers(
                    &data.spec.components,
                    &data.policy_lookup(),
                    lattice_id,
                    &client,
                    &name,
                    &subject,
                    &snapshot_data,
                );
                Some((name, scalers))
            })
            .collect();

        let scalers = Arc::new(RwLock::new(scalers));

        let mut manager = ScalerManager {
            handle: None,
            scalers,
            client,
            subject,
            lattice_id: lattice_id.to_owned(),
            command_publisher,
            status_publisher,
            snapshot_data,
        };
        let cloned = manager.clone();
        let handle = tokio::spawn(async move { cloned.notify(messages).await });
        manager.handle = Some(Arc::new(handle));
        Ok(manager)
    }

    // NOTE(thomastaylor312): This is a little gross as it is purely for testing, but we needed a
    // way to work around creating a consumer when starting stuff
    #[cfg(test)]
    pub(crate) async fn test_new(
        client: P,
        lattice_id: &str,
        state_store: StateStore,
        command_publisher: CommandPublisher<P>,
        status_publisher: StatusPublisher<P>,
        link_getter: L,
    ) -> ScalerManager<StateStore, P, L> {
        let snapshot_data = SnapshotStore::new(
            state_store.clone(),
            link_getter.clone(),
            lattice_id.to_owned(),
        );
        ScalerManager {
            handle: None,
            scalers: Arc::new(RwLock::new(HashMap::new())),
            client,
            subject: format!("{WADM_NOTIFY_PREFIX}.{lattice_id}"),
            lattice_id: lattice_id.to_owned(),
            command_publisher,
            status_publisher,
            snapshot_data,
        }
    }

    /// Refreshes the snapshot data consumed by all scalers. This is a temporary workaround until we
    /// start caching data
    pub(crate) async fn refresh_data(&self) -> Result<()> {
        self.snapshot_data.refresh().await
    }

    /// Adds scalers for the given manifest. Emitting an event to notify other wadm processes that
    /// they should create them as well. Only returns an error if it can't notify. Returns the
    /// scaler list for immediate use in reconciliation
    ///
    /// This only constructs the scalers and doesn't reconcile. The returned [`Scalers`] type can be
    /// used to set this model to backoff mode
    #[instrument(level = "trace", skip_all, fields(name = %manifest.metadata.name, lattice_id = %self.lattice_id))]
    pub async fn add_scalers<'a>(
        &'a self,
        manifest: &'a Manifest,
        scalers: ScalerList,
    ) -> Result<Scalers> {
        self.add_raw_scalers(&manifest.metadata.name, scalers).await;
        let notification = serde_json::to_vec(&Notifications::CreateScalers(manifest.to_owned()))?;
        self.client
            .publish(notification, Some(&self.subject))
            .await?;

        // The error case here would be _really_ weird as something would have removed it right
        // after we added, but handling it just in case
        self.get_scalers(&manifest.metadata.name)
            .await
            .ok_or_else(|| anyhow::anyhow!("Data error: scalers no longer exist after creation"))
    }

    pub fn scalers_for_manifest<'a>(&'a self, manifest: &'a Manifest) -> ScalerList {
        components_to_scalers(
            &manifest.spec.components,
            &manifest.policy_lookup(),
            &self.lattice_id,
            &self.client,
            &manifest.metadata.name,
            &self.subject,
            &self.snapshot_data,
        )
    }

    /// Gets the scalers for the given model name, returning None if they don't exist.
    #[instrument(level = "trace", skip(self), fields(lattice_id = %self.lattice_id))]
    pub async fn get_scalers<'a>(&'a self, name: &'a str) -> Option<Scalers> {
        let lock = self.scalers.clone().read_owned().await;
        OwnedRwLockReadGuard::try_map(lock, |scalers| scalers.get(name))
            .ok()
            .map(|scalers| Scalers { scalers })
    }

    /// Gets a specific scaler for the given model name with the given ID, returning None if it doesn't exist.
    #[instrument(level = "trace", skip(self), fields(lattice_id = %self.lattice_id))]
    pub async fn get_specific_scaler<'a>(
        &'a self,
        name: &'a str,
        scaler_id: &'a str,
    ) -> Option<SingleScaler> {
        let lock = self.scalers.clone().read_owned().await;
        OwnedRwLockReadGuard::try_map(lock, |scalers| {
            scalers
                .get(name)
                .and_then(|scalers| scalers.iter().find(|s| s.id() == scaler_id))
        })
        .ok()
        .map(|scaler| SingleScaler { scaler })
    }

    /// Gets all current managed scalers
    #[instrument(level = "trace", skip(self), fields(lattice_id = %self.lattice_id))]
    pub async fn get_all_scalers(&self) -> AllScalers {
        AllScalers {
            scalers: self.scalers.clone().read_owned().await,
        }
    }

    /// Removes the scalers for a given model name, publishing all commands needed for cleanup.
    /// Returns `None` when no scaler with the name was found
    ///
    /// This function will notify other wadms that they should remove the scalers as well. If the
    /// notification or handling commands fails, then this function will reinsert the scalers back into the internal map
    /// and return an error (so this function can be called again)
    // NOTE(thomastaylor312): This was designed the way it is to avoid race conditions. We only ever
    // stop components and providers that have the right annotation. So if for some reason this
    // leaves something hanging, we should probably add something to the reaper
    #[instrument(level = "debug", skip(self), fields(lattice_id = %self.lattice_id))]
    pub async fn remove_scalers(&self, name: &str) -> Option<Result<()>> {
        let scalers = match self.remove_scalers_internal(name).await {
            Some(Ok(s)) => Some(s),
            Some(Err(e)) => {
                warn!(err = ?e, "Error when running cleanup steps for scalers. Operation will be retried");
                return Some(Err(e));
            }
            None => None,
        };

        // SAFETY: This is entirely data in our control and should be safe to unwrap
        if let Err(e) = self
            .client
            .publish(
                serde_json::to_vec(&Notifications::DeleteScalers(name.to_owned())).unwrap(),
                Some(&self.subject),
            )
            .await
        {
            error!(error = %e, "Unable to publish notification");
            if let Some(scalers) = scalers {
                self.scalers.write().await.insert(name.to_owned(), scalers);
            }
            Some(Err(e))
        } else {
            Some(Ok(()))
        }
    }

    /// An internal function to allow pushing the scalers without any of the publishing
    async fn add_raw_scalers(&self, name: &str, scalers: ScalerList) {
        self.scalers.write().await.insert(name.to_owned(), scalers);
    }

    /// A function that removes the scalers without any of the publishing
    /// CAUTION: This function does not do any cleanup, so it should only be used in scenarios
    /// where you are prepared to handle that yourself.
    pub(crate) async fn remove_raw_scalers(&self, name: &str) -> Option<ScalerList> {
        self.scalers.write().await.remove(name)
    }

    /// Does everything except sending the notification
    #[instrument(level = "debug", skip(self), fields(lattice_id = %self.lattice_id))]
    async fn remove_scalers_internal(&self, name: &str) -> Option<Result<ScalerList>> {
        // Remove the scalers first to avoid them handling events while we're cleaning up
        let scalers = self.remove_raw_scalers(name).await?;

        // Always refresh data before cleaning up
        if let Err(e) = self.refresh_data().await {
            return Some(Err(e));
        }
        let commands = match futures::future::join_all(
            scalers.iter().map(|scaler| scaler.cleanup()),
        )
        .await
        .into_iter()
        .collect::<Result<Vec<Vec<Command>>, anyhow::Error>>()
        .map(|all| all.into_iter().flatten().collect::<Vec<Command>>())
        {
            Ok(c) => c,
            Err(e) => {
                warn!(err = ?e, "Error when running cleanup steps for scalers. Operation will be retried");
                // Put the scalers back into the map so we can run cleanup again on retry
                self.scalers.write().await.insert(name.to_owned(), scalers);
                return Some(Err(e));
            }
        };
        trace!(?commands, "Publishing cleanup commands");
        if let Err(e) = self.command_publisher.publish_commands(commands).await {
            error!(error = %e, "Unable to publish cleanup commands");
            self.scalers.write().await.insert(name.to_owned(), scalers);
            Some(Err(e))
        } else {
            Some(Ok(scalers))
        }
    }

    #[instrument(level = "debug", skip_all, fields(lattice_id = %self.lattice_id))]
    async fn notify(&self, mut messages: MessageStream) -> Result<()> {
        loop {
            tokio::select! {
                res = messages.next() => {
                    match res {
                        Some(Ok(msg)) => {
                            let notification: Notifications = match serde_json::from_slice(&msg.payload) {
                                Ok(n) => n,
                                Err(e) => {
                                    warn!(error = %e, "Received unparsable message from consumer");
                                    continue;
                                }
                            };

                            match notification {
                                Notifications::CreateScalers(manifest) => {
                                    // We don't want to trigger the notification, so just create the scalers and then insert
                                    let scalers = components_to_scalers(
                                        &manifest.spec.components,
                                        &manifest.policy_lookup(),
                                        &self.lattice_id,
                                        &self.client,
                                        &manifest.metadata.name,
                                        &self.subject,
                                        &self.snapshot_data,
                                    );
                                    let num_scalers = scalers.len();
                                    self.add_raw_scalers(&manifest.metadata.name, scalers).await;
                                    trace!(name = %manifest.metadata.name, %num_scalers, "Finished creating scalers for manifest");
                                }
                                Notifications::DeleteScalers(name) => {
                                    trace!(%name, "Removing scalers for manifest");
                                    match self.remove_scalers_internal(&name).await {
                                        Some(Ok(_)) | None => {
                                            trace!(%name, "Removed manifests or manifests were already removed");
                                            // NOTE(thomastaylor312): We publish the undeployed
                                            // status here after we remove scalers. All wadm
                                            // instances will receive this event (even the one that
                                            // initially deleted it) and so it made more sense to
                                            // publish the status here so we don't get any stray
                                            // reconciling status messages from a wadm instance that
                                            // hasn't deleted the scaler yet
                                            if let Err(e) = self
                                                .status_publisher
                                                .publish_status(&name, Status::new(
                                                    StatusInfo::undeployed("Manifest has been undeployed"),
                                                    Vec::with_capacity(0),
                                                ))
                                                .await
                                            {
                                                warn!(error = ?e, "Failed to set status to undeployed");
                                            }
                                        }
                                        Some(Err(e)) => {
                                            error!(error = %e, %name, "Error when running cleanup steps for scalers. Nacking notification");
                                            if let Err(e) = msg.ack_with(AckKind::Nak(None)).await {
                                                warn!(error = %e, %name, "Unable to nack message");
                                                // We continue here so we don't fall through to the ack
                                                continue;
                                            }
                                        }
                                    }
                                    // NOTE(thomastaylor312): We could find that this strategy actually
                                    // doesn't tear down everything or leaves something hanging. If that is
                                    // the case, a new part of the reaper logic should handle it
                                },
                                // NOTE(thomastaylor312): Please note that both of the
                                // ExpectedEvents blocks are "cheating". If the scaler is a backoff
                                // wrapped scaler, calling `reconcile` or `handle_event` will
                                // trigger the creation of the expected events for this scaler.
                                // Otherwise, it will just run the logic for handling stuff. Either
                                // way, we ignore the returned events. Right now this is totally
                                // fine because scalers should run fairly quickly and we only run
                                // them in the case where something is observed. It also leaves the
                                // flexibility of managing any scaler rather than just backoff
                                // wrapped ones (which is good from a Rust API point of view). If
                                // this starts to become a problem, we can revisit how we handle
                                // this (probably by requiring that this struct always wraps any
                                // scaler in the backoff wrapper and using custom methods from that
                                // type)
                                Notifications::RegisterExpectedEvents{ name, scaler_id, triggering_event } => {
                                    trace!(%name, "Computing and registering expected events for manifest");
                                    if let Some(scaler) = self.get_specific_scaler(&name, &scaler_id).await {
                                        if let Some(event) = triggering_event {
                                            let parsed_event: Event = match event.try_into() {
                                                Ok(e) => e,
                                                Err(e) => {
                                                    error!(error = %e, %name, "Unable to parse given event");
                                                    continue;
                                                }
                                            };
                                            if let Err(e) = scaler.handle_event(&parsed_event).await {
                                                error!(error = %e, %name, %scaler_id, "Unable to register expected events for scaler");
                                            }
                                        } else if let Err(e) = scaler.reconcile().await {
                                            error!(error = %e, %name, %scaler_id, "Unable to register expected events for scaler");
                                        }
                                    } else {
                                        debug!(%name, "Received request to register events for non-existent scalers, ignoring");
                                    }
                                },
                                Notifications::RemoveExpectedEvent{ name, scaler_id, event } => {
                                    trace!(%name, "Removing expected event for manifest");
                                    if let Some(scaler) = self.get_specific_scaler(&name, &scaler_id).await {
                                        let parsed_event: Event = match event.try_into() {
                                            Ok(e) => e,
                                            Err(e) => {
                                                error!(error = %e, %name, "Unable to parse given event");
                                                continue;
                                            }
                                        };
                                        if let Err(e) = scaler.handle_event(&parsed_event).await {
                                            error!(error = %e, %name, %scaler_id, "Unable to register expected events for scaler");
                                        }
                                    } else {
                                        debug!(%name, "Received request to remove event for non-existent scalers, ignoring");
                                    }
                                }
                            }
                            // Always ack if we get here
                            if let Err(e) = msg.double_ack().await {
                                warn!(error = %e,"Unable to ack message");
                            }
                        }
                        Some(Err(e)) => {
                            error!(error = %e, "Error when retrieving message from stream. Will attempt to fetch next message");
                        }
                        None => {
                            // NOTE(thomastaylor312): This could possibly be a fatal error as we won't be able
                            // to create scalers. We may want to determine how to bubble that all the way back
                            // up if it is
                            error!("Notifier for manifests has exited");
                            anyhow::bail!("Notifier has exited")
                        }
                    }
                }
            }
        }
    }
}

const EMPTY_TRAIT_VEC: Vec<Trait> = Vec::new();

/// Converts a list of components into a list of scalers
///
/// # Arguments
/// * `components` - The list of components to convert
/// * `store` - The store to use when creating the scalers so they can access lattice state
/// * `lattice_id` - The lattice id the scalers operate on
/// * `name` - The name of the manifest that the scalers are being created for
pub(crate) fn components_to_scalers<S, P, L>(
    components: &[Component],
    policies: &HashMap<&String, &Policy>,
    lattice_id: &str,
    notifier: &P,
    name: &str,
    notifier_subject: &str,
    snapshot_data: &SnapshotStore<S, L>,
) -> ScalerList
where
    S: ReadStore + Send + Sync + Clone + 'static,
    P: Publisher + Clone + Send + Sync + 'static,
    L: LinkSource + ConfigSource + SecretSource + Clone + Send + Sync + 'static,
{
    let mut scalers: ScalerList = Vec::new();
    for component in components.iter() {
        let traits = component.traits.as_ref();
        match &component.properties {
            Properties::Component { properties: props } => {
                scalers.extend(traits.unwrap_or(&EMPTY_TRAIT_VEC).iter().filter_map(|trt| {
                    let component_id =
                        compute_component_id(name, props.id.as_ref(), &component.name);
                    let (config_scalers, mut config_names) =
                        config_to_scalers(snapshot_data.clone(), name, &props.config);
                    let (secret_scalers, secret_names) =
                        secrets_to_scalers(snapshot_data.clone(), name, &props.secrets, policies);

                    config_names.append(&mut secret_names.clone());
                    match (trt.trait_type.as_str(), &trt.properties) {
                        (SPREADSCALER_TRAIT, TraitProperty::SpreadScaler(p)) => {
                            Some(Box::new(BackoffWrapper::new(
                                ComponentSpreadScaler::new(
                                    snapshot_data.clone(),
                                    props.image.to_owned(),
                                    component_id,
                                    lattice_id.to_owned(),
                                    name.to_owned(),
                                    p.to_owned(),
                                    &component.name,
                                    config_names,
                                ),
                                notifier.to_owned(),
                                config_scalers,
                                secret_scalers,
                                notifier_subject,
                                name,
                                Some(Duration::from_secs(5)),
                            )) as BoxedScaler)
                        }
                        (DAEMONSCALER_TRAIT, TraitProperty::SpreadScaler(p)) => {
                            Some(Box::new(BackoffWrapper::new(
                                ComponentDaemonScaler::new(
                                    snapshot_data.clone(),
                                    props.image.to_owned(),
                                    component_id,
                                    lattice_id.to_owned(),
                                    name.to_owned(),
                                    p.to_owned(),
                                    &component.name,
                                    config_names,
                                ),
                                notifier.to_owned(),
                                config_scalers,
                                secret_scalers,
                                notifier_subject,
                                name,
                                Some(Duration::from_secs(5)),
                            )) as BoxedScaler)
                        }
                        (LINK_TRAIT, TraitProperty::Link(p)) => {
                            components.iter().find_map(|component| {
                                let (mut config_scalers, mut source_config) = config_to_scalers(
                                    snapshot_data.clone(),
                                    name,
                                    &p.source.as_ref().unwrap_or(&Default::default()).config,
                                );
                                let (target_config_scalers, mut target_config) = config_to_scalers(
                                    snapshot_data.clone(),
                                    name,
                                    &p.target.config,
                                );

                                let (target_secret_scalers, target_secrets) = secrets_to_scalers(
                                    snapshot_data.clone(),
                                    name,
                                    &p.target.secrets,
                                    policies,
                                );
                                let (mut source_secret_scalers, source_secrets) =
                                    secrets_to_scalers(
                                        snapshot_data.clone(),
                                        name,
                                        &p.source.as_ref().unwrap_or(&Default::default()).secrets,
                                        policies,
                                    );
                                config_scalers.extend(target_config_scalers);
                                source_secret_scalers.extend(target_secret_scalers);
                                target_config.extend(target_secrets);
                                source_config.extend(source_secrets);
                                match &component.properties {
                                    Properties::Capability {
                                        properties: CapabilityProperties { id, .. },
                                    }
                                    | Properties::Component {
                                        properties: ComponentProperties { id, .. },
                                    } if component.name == p.target.name => {
                                        Some(Box::new(BackoffWrapper::new(
                                            LinkScaler::new(
                                                snapshot_data.clone(),
                                                LinkScalerConfig {
                                                    source_id: component_id.to_string(),
                                                    target: compute_component_id(
                                                        name,
                                                        id.as_ref(),
                                                        &component.name,
                                                    ),
                                                    wit_namespace: p.namespace.to_owned(),
                                                    wit_package: p.package.to_owned(),
                                                    wit_interfaces: p.interfaces.to_owned(),
                                                    name: p.name.to_owned().unwrap_or_else(|| {
                                                        DEFAULT_LINK_NAME.to_string()
                                                    }),
                                                    lattice_id: lattice_id.to_owned(),
                                                    model_name: name.to_owned(),
                                                    source_config,
                                                    target_config,
                                                },
                                                snapshot_data.clone(),
                                            ),
                                            notifier.to_owned(),
                                            config_scalers,
                                            source_secret_scalers,
                                            notifier_subject,
                                            name,
                                            Some(Duration::from_secs(5)),
                                        ))
                                            as BoxedScaler)
                                    }
                                    _ => None,
                                }
                            })
                        }
                        _ => None,
                    }
                }))
            }
            Properties::Capability { properties: props } => {
                let provider_id = compute_component_id(name, props.id.as_ref(), &component.name);
                let mut scaler_specified = false;
                if let Some(traits) = traits {
                    scalers.extend(traits.iter().filter_map(|trt| {
                        match (trt.trait_type.as_str(), &trt.properties) {
                            (SPREADSCALER_TRAIT, TraitProperty::SpreadScaler(p)) => {
                                scaler_specified = true;
                                let (config_scalers, mut config_names) =
                                    config_to_scalers(snapshot_data.clone(), name, &props.config);
                                let (secret_scalers, secret_names) = secrets_to_scalers(
                                    snapshot_data.clone(),
                                    name,
                                    &props.secrets,
                                    policies,
                                );
                                config_names.append(&mut secret_names.clone());

                                Some(Box::new(BackoffWrapper::new(
                                    ProviderSpreadScaler::new(
                                        snapshot_data.clone(),
                                        ProviderSpreadConfig {
                                            lattice_id: lattice_id.to_owned(),
                                            provider_id: provider_id.to_owned(),
                                            provider_reference: props.image.to_owned(),
                                            spread_config: p.to_owned(),
                                            model_name: name.to_owned(),
                                            provider_config: config_names,
                                        },
                                        &component.name,
                                    ),
                                    notifier.to_owned(),
                                    config_scalers,
                                    secret_scalers,
                                    notifier_subject,
                                    name,
                                    // Providers are a bit longer because it can take a bit to download
                                    Some(Duration::from_secs(60)),
                                )) as BoxedScaler)
                            }
                            (DAEMONSCALER_TRAIT, TraitProperty::SpreadScaler(p)) => {
                                scaler_specified = true;
                                let (config_scalers, mut config_names) =
                                    config_to_scalers(snapshot_data.clone(), name, &props.config);
                                let (secret_scalers, secret_names) = secrets_to_scalers(
                                    snapshot_data.clone(),
                                    name,
                                    &props.secrets,
                                    policies,
                                );
                                config_names.append(&mut secret_names.clone());
                                Some(Box::new(BackoffWrapper::new(
                                    ProviderDaemonScaler::new(
                                        snapshot_data.clone(),
                                        ProviderSpreadConfig {
                                            lattice_id: lattice_id.to_owned(),
                                            provider_id: provider_id.to_owned(),
                                            provider_reference: props.image.to_owned(),
                                            spread_config: p.to_owned(),
                                            model_name: name.to_owned(),
                                            provider_config: config_names,
                                        },
                                        &component.name,
                                    ),
                                    notifier.to_owned(),
                                    config_scalers,
                                    secret_scalers,
                                    notifier_subject,
                                    name,
                                    // Providers are a bit longer because it can take a bit to download
                                    Some(Duration::from_secs(60)),
                                )) as BoxedScaler)
                            }
                            (LINK_TRAIT, TraitProperty::Link(p)) => {
                                components.iter().find_map(|component| {
                                    let (mut config_scalers, mut source_config) = config_to_scalers(
                                        snapshot_data.clone(),
                                        name,
                                        &p.source.as_ref().unwrap_or(&Default::default()).config,
                                    );
                                    let (target_config_scalers, mut target_config) =
                                        config_to_scalers(
                                            snapshot_data.clone(),
                                            name,
                                            &p.target.config,
                                        );
                                    let (target_secret_scalers, target_secrets) =
                                        secrets_to_scalers(
                                            snapshot_data.clone(),
                                            name,
                                            &p.target.secrets,
                                            policies,
                                        );
                                    let (mut source_secret_scalers, source_secrets) =
                                        secrets_to_scalers(
                                            snapshot_data.clone(),
                                            name,
                                            &p.source
                                                .as_ref()
                                                .unwrap_or(&Default::default())
                                                .secrets,
                                            policies,
                                        );
                                    config_scalers.extend(target_config_scalers);
                                    source_secret_scalers.extend(target_secret_scalers);

                                    target_config.extend(target_secrets);
                                    source_config.extend(source_secrets);
                                    match &component.properties {
                                        Properties::Component { properties: cappy }
                                            if component.name == p.target.name =>
                                        {
                                            Some(Box::new(BackoffWrapper::new(
                                                LinkScaler::new(
                                                    snapshot_data.clone(),
                                                    LinkScalerConfig {
                                                        source_id: provider_id.to_string(),
                                                        target: compute_component_id(
                                                            name,
                                                            cappy.id.as_ref(),
                                                            &component.name,
                                                        ),
                                                        wit_namespace: p.namespace.to_owned(),
                                                        wit_package: p.package.to_owned(),
                                                        wit_interfaces: p.interfaces.to_owned(),
                                                        name: p.name.to_owned().unwrap_or_else(
                                                            || DEFAULT_LINK_NAME.to_string(),
                                                        ),
                                                        lattice_id: lattice_id.to_owned(),
                                                        model_name: name.to_owned(),
                                                        source_config,
                                                        target_config,
                                                    },
                                                    snapshot_data.clone(),
                                                ),
                                                notifier.to_owned(),
                                                config_scalers,
                                                source_secret_scalers,
                                                notifier_subject,
                                                name,
                                                Some(Duration::from_secs(5)),
                                            ))
                                                as BoxedScaler)
                                        }
                                        _ => None,
                                    }
                                })
                            }
                            _ => None,
                        }
                    }))
                }
                // Allow providers to omit the scaler entirely for simplicity
                if !scaler_specified {
                    let (config_scalers, mut config_names) =
                        config_to_scalers(snapshot_data.clone(), name, &props.config);

                    let (secret_scalers, secret_names) =
                        secrets_to_scalers(snapshot_data.clone(), name, &props.secrets, policies);
                    config_names.append(&mut secret_names.clone());

                    scalers.push(Box::new(BackoffWrapper::new(
                        ProviderSpreadScaler::new(
                            snapshot_data.clone(),
                            ProviderSpreadConfig {
                                lattice_id: lattice_id.to_owned(),
                                provider_id,
                                provider_reference: props.image.to_owned(),
                                spread_config: SpreadScalerProperty {
                                    instances: 1,
                                    spread: vec![],
                                },
                                model_name: name.to_owned(),
                                provider_config: config_names,
                            },
                            &component.name,
                        ),
                        notifier.to_owned(),
                        config_scalers,
                        secret_scalers,
                        notifier_subject,
                        name,
                        // Providers are a bit longer because it can take a bit to download
                        Some(Duration::from_secs(60)),
                    )) as BoxedScaler)
                }
            }
        }
    }
    scalers
}

/// Returns a tuple which is a list of scalers and a list of the names of the configs that the
/// scalers use.
///
/// Any input [ConfigProperty] that has a `properties` field will be converted into a [ConfigScaler], and
/// the name of the configuration will be modified to be unique to the model and component. If the properties
/// field is not present, the name will be used as-is and assumed that it's managed externally to wadm.
fn config_to_scalers<C: ConfigSource + Send + Sync + Clone>(
    config_source: C,
    model_name: &str,
    configs: &[ConfigProperty],
) -> (Vec<ConfigScaler<C>>, Vec<String>) {
    configs
        .iter()
        .map(|config| {
            let name = if config.properties.is_some() {
                compute_component_id(model_name, None, &config.name)
            } else {
                config.name.clone()
            };
            (
                ConfigScaler::new(config_source.clone(), &name, config.properties.as_ref()),
                name,
            )
        })
        .unzip()
}

fn secrets_to_scalers<S: SecretSource + Send + Sync + Clone>(
    secret_source: S,
    model_name: &str,
    secrets: &[SecretProperty],
    policies: &HashMap<&String, &Policy>,
) -> (Vec<SecretScaler<S>>, Vec<String>) {
    secrets
        .iter()
        .map(|s| {
            let name = compute_secret_id(model_name, None, &s.name);
            let policy = *policies.get(&s.properties.policy).unwrap();
            (
                SecretScaler::new(
                    name.clone(),
                    policy.clone(),
                    s.clone(),
                    secret_source.clone(),
                ),
                name,
            )
        })
        .unzip()
}

/// Based on the name of the model and the optionally provided ID, returns a unique ID for the
/// component that is a sanitized version of the component reference and model name, separated
/// by a dash.
pub(crate) fn compute_component_id(
    model_name: &str,
    component_id: Option<&String>,
    component_name: &str,
) -> String {
    if let Some(id) = component_id {
        id.to_owned()
    } else {
        format!(
            "{}-{}",
            model_name
                .to_lowercase()
                .replace(|c: char| !c.is_ascii_alphanumeric(), "_"),
            component_name
                .to_lowercase()
                .replace(|c: char| !c.is_ascii_alphanumeric(), "_")
        )
    }
}

pub(crate) fn compute_secret_id(
    model_name: &str,
    component_id: Option<&String>,
    component_name: &str,
) -> String {
    let name = compute_component_id(model_name, component_id, component_name);
    format!("{SECRET_PREFIX}_{name}")
}

#[cfg(test)]
mod test {
    use crate::scaler::manager::compute_component_id;

    #[test]
    fn compute_proper_component_id() {
        // User supplied ID always takes precedence
        assert_eq!(
            compute_component_id("mymodel", Some(&"myid".to_string()), "echo"),
            "myid"
        );
        assert_eq!(
            compute_component_id(
                "some model name with spaces cause yaml",
                Some(&"myid".to_string()),
                " echo "
            ),
            "myid"
        );
        // Sanitize component reference
        assert_eq!(
            compute_component_id("mymodel", None, "echo-component"),
            "mymodel-echo_component"
        );
        // Ensure we can support spaces in the model name, because YAML strings
        assert_eq!(
            compute_component_id("some model name with spaces cause yaml", None, "echo"),
            "some_model_name_with_spaces_cause_yaml-echo"
        );
        // Ensure we can support spaces in the model name, because YAML strings
        // Ensure we can support lowercasing the reference as well, just in case
        assert_eq!(
            compute_component_id("My ThInG", None, "thing.wasm"),
            "my_thing-thing_wasm"
        );
    }
}
