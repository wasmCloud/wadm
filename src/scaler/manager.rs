//! A struct that manages creating and removing scalers for all manifests

use std::{collections::HashMap, ops::Deref, sync::Arc};

use anyhow::Result;
use async_nats::jetstream::{
    consumer::pull::{Config as PullConfig, Stream as MessageStream},
    kv::Store as KvStore,
    stream::Stream as JsStream,
    AckKind,
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{OwnedRwLockReadGuard, RwLock},
    task::JoinHandle,
};
use tracing::{debug, error, instrument, trace, warn};

use crate::{
    events::Event,
    model::{
        Component, Manifest, Properties, SpreadScalerProperty, Trait, TraitProperty, LINKDEF_TRAIT,
        SPREADSCALER_TRAIT,
    },
    publisher::Publisher,
    scaler::{spreadscaler::ActorSpreadScaler, Command, Scaler},
    storage::ReadStore,
    workers::CommandPublisher,
};

use super::{
    spreadscaler::{link::LinkScaler, provider::ProviderSpreadScaler},
    BackoffAwareScaler,
};

pub type BoxedScaler = Box<dyn Scaler + Send + Sync + 'static>;
pub type ScalerList = Vec<BackoffAwareScaler>;

pub const WADM_NOTIFY_PREFIX: &str = "wadm.notify";

/// All events sent for manifest notifications
#[derive(Debug, Serialize, Deserialize)]
pub enum Notifications {
    CreateScalers(Manifest),
    DeleteScalers(String),
    /// Register expected events for a manifest. Should only be used when a full reconcile
    /// is being done (like on first deploy), rather than just handling a single event
    RegisterExpectedEvents(String),
    /// Remove an event from the expected list for a manifest scaler
    RemoveExpectedEvent((String, Event)),
    /// Clear all expected events for a manifest
    ClearExpectedEvents(String),
}

/// A wrapper type returned when getting a list of scalers. Implements a backoff operation that can
/// be called to tell all returned scalers to back off and notifiying with a message so other
/// consumers can backoff as well
pub struct Scalers<'a, P> {
    pub scalers: OwnedRwLockReadGuard<HashMap<String, ScalerList>, ScalerList>,
    publisher: &'a P,
    name: &'a str,
    subject: &'a str,
}

impl<'a, P> Deref for Scalers<'a, P> {
    type Target = ScalerList;

    fn deref(&self) -> &Self::Target {
        self.scalers.deref()
    }
}

impl<'a, P> Scalers<'a, P>
where
    P: Publisher,
{
    pub async fn reconcile_and_register(&self) -> Result<Vec<Result<Vec<Command>>>> {
        let manifest_name = self.name;

        // Publish the message first because otherwise we could get scalers that don't go to backoff
        // mode
        let data = serde_json::to_vec(&Notifications::RegisterExpectedEvents(
            manifest_name.to_owned(),
        ))?;
        self.publisher.publish(data, Some(self.subject)).await?;

        Ok(self
            .reconcile_and_register_expected_events(manifest_name)
            .await)
    }

    /// Computes commands and registers expected events for each scaler.
    /// Should only be called when you plan to publish all of the commands,
    /// as this function will register expected events for each scaler.
    pub async fn reconcile_and_register_expected_events(
        &self,
        manifest_name: &str,
    ) -> Vec<Result<Vec<Command>>> {
        futures::future::join_all(self.scalers.iter().map(|scaler| async move {
            match scaler.reconcile().await {
                // "Backoff" scalers with expected corresponding events
                Ok(commands) => scaler
                    .add_events(
                        commands
                            .iter()
                            .filter_map(|command| command.corresponding_event(manifest_name)),
                        true,
                    )
                    .await
                    .map(|_| commands),
                Err(e) => Err(e),
            }
        }))
        .await
    }
}

/// A wrapper type returned when getting all scalers. Implements a backoff operation that can be
/// called to tell all returned scalers to back off and notifiying with a message so other consumers
/// can backoff as well
pub struct AllScalers {
    pub scalers: OwnedRwLockReadGuard<HashMap<String, ScalerList>>,
}

impl Deref for AllScalers {
    type Target = HashMap<String, ScalerList>;

    fn deref(&self) -> &Self::Target {
        self.scalers.deref()
    }
}

/// A manager that consumes notifications from a stream for a lattice and then either adds or removes the
/// necessary scalers
#[derive(Clone)]
pub struct ScalerManager<StateStore, P: Clone> {
    handle: Option<Arc<JoinHandle<Result<()>>>>,
    scalers: Arc<RwLock<HashMap<String, ScalerList>>>,
    client: P,
    subject: String,
    lattice_id: String,
    state_store: StateStore,
    publisher: CommandPublisher<P>,
}

impl<StateStore, P: Clone> Drop for ScalerManager<StateStore, P> {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort()
        }
    }
}

impl<StateStore, P> ScalerManager<StateStore, P>
where
    StateStore: ReadStore + Send + Sync + Clone + 'static,
    P: Publisher + Clone + Send + Sync + 'static,
{
    /// Creates a new ScalerManager configured to notify messages to `wadm.notify.{lattice_id}`
    /// using the given jetstream client. Also creates an ephemeral consumer for notifications on
    /// the given stream
    pub async fn new(
        client: P,
        stream: JsStream,
        lattice_id: &str,
        state_store: StateStore,
        manifest_store: KvStore,
        publisher: CommandPublisher<P>,
    ) -> Result<ScalerManager<StateStore, P>> {
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
            .list(lattice_id)
            .await?
            .into_iter()
            .map(|summary| manifest_store.get(lattice_id, summary.name));
        let all_manifests = futures::future::join_all(futs)
            .await
            .into_iter()
            .filter_map(|manifest| manifest.transpose())
            .map(|res| res.map(|(manifest, _)| manifest))
            .collect::<Result<Vec<_>>>()?;
        let scalers: HashMap<String, ScalerList> = all_manifests
            .into_iter()
            .filter_map(|manifest| {
                let data = manifest.get_deployed()?;
                let name = manifest.name().to_owned();
                let scalers =
                    components_to_scalers(&data.spec.components, &state_store, lattice_id, &name);
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
            state_store,
            publisher,
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
        publisher: CommandPublisher<P>,
    ) -> ScalerManager<StateStore, P> {
        ScalerManager {
            handle: None,
            scalers: Arc::new(RwLock::new(HashMap::new())),
            client,
            subject: format!("{WADM_NOTIFY_PREFIX}.{lattice_id}"),
            lattice_id: lattice_id.to_owned(),
            state_store,
            publisher,
        }
    }

    /// Adds scalers for the given manifest. Emitting an event to notify other wadm processes that
    /// they should create them as well. Only returns an error if it can't notify. Returns the
    /// scaler list for immediate use in reconciliation
    ///
    /// This only constructs the scalers and doesn't reconcile. The returned [`Scalers`] type can be
    /// used to set this model to backoff mode
    #[instrument(level = "trace", skip_all, fields(name = %manifest.metadata.name, lattice_id = %self.lattice_id))]
    pub async fn add_scalers<'a>(&'a self, manifest: &'a Manifest) -> Result<Scalers<'a, P>> {
        let scalers = components_to_scalers(
            &manifest.spec.components,
            &self.state_store,
            &self.lattice_id,
            &manifest.metadata.name,
        );
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

    /// Gets the scalers for the given model name, returning None if they don't exist.
    ///
    /// The returned [`Scalers`] type can be used to set this model to backoff mode
    #[instrument(level = "trace", skip(self), fields(lattice_id = %self.lattice_id))]
    pub async fn get_scalers<'a>(&'a self, name: &'a str) -> Option<Scalers<'a, P>> {
        let lock = self.scalers.clone().read_owned().await;
        OwnedRwLockReadGuard::try_map(lock, |scalers| scalers.get(name))
            .ok()
            .map(|scalers| Scalers {
                scalers,
                publisher: &self.client,
                name,
                subject: self.subject.as_str(),
            })
    }

    /// Gets all current managed scalers
    ///
    /// The returned [`AllScalers`] type can be used to set verious models to backoff mode
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
    // stop actors and providers that have the right annotation. So if for some reason this leaves
    // something hanging, we should probably add something to the reaper
    #[instrument(level = "debug", skip(self), fields(lattice_id = %self.lattice_id))]
    pub async fn remove_scalers(&self, name: &str) -> Option<Result<()>> {
        let scalers = match self.remove_scalers_internal(name).await {
            Some(Ok(s)) => s,
            Some(Err(e)) => return Some(Err(e)),
            None => return None,
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
            self.scalers.write().await.insert(name.to_owned(), scalers);
            Some(Err(e))
        } else {
            Some(Ok(()))
        }
    }

    /// An internal function to allow pushing the scalers without any of the publishing
    async fn add_raw_scalers(&self, name: &str, scalers: ScalerList) {
        self.scalers.write().await.insert(name.to_owned(), scalers);
    }

    /// Does everything except sending the notification
    #[instrument(level = "debug", skip(self), fields(lattice_id = %self.lattice_id))]
    async fn remove_scalers_internal(&self, name: &str) -> Option<Result<ScalerList>> {
        let scalers = self.scalers.write().await.remove(name)?;
        let commands =
            match futures::future::join_all(scalers.iter().map(|scaler| scaler.cleanup()))
                .await
                .into_iter()
                .collect::<Result<Vec<Vec<Command>>, anyhow::Error>>()
                .map(|all| all.into_iter().flatten().collect::<Vec<Command>>())
            {
                Ok(c) => c,
                Err(e) => return Some(Err(e)),
            };
        trace!(?commands, "Publishing cleanup commands");
        if let Err(e) = self.publisher.publish_commands(commands).await {
            error!(error = %e, "Unable to publish cleanup commands");
            self.scalers.write().await.insert(name.to_owned(), scalers);
            return Some(Err(e));
        }
        Some(Ok(scalers))
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
                                        &self.state_store,
                                        &self.lattice_id,
                                        &manifest.metadata.name,
                                    );
                                    let num_scalers = scalers.len();
                                    self.add_raw_scalers(&manifest.metadata.name, scalers).await;
                                    trace!(name = %manifest.metadata.name, %num_scalers, "Finished creating scalers for manifest");
                                }
                                Notifications::DeleteScalers(name) => {
                                    trace!(%name, "Removing scalers for manifest");
                                    match self.remove_scalers_internal(&name).await {
                                        Some(Ok(_)) => {
                                            trace!(%name, "Successfully removed scalers for manifest")
                                        }
                                        Some(Err(e)) => {
                                            error!(error = %e, %name, "Error when running cleanup steps for scalers. Nacking notification");
                                            if let Err(e) = msg.ack_with(AckKind::Nak(None)).await {
                                                warn!(error = %e, %name, "Unable to nack message");
                                                // We continue here so we don't fall through to the ack
                                                continue;
                                            }
                                        }
                                        None => {
                                            debug!(%name, "Scalers don't exist or were already deleted");
                                        }
                                    }
                                    // NOTE(thomastaylor312): We could find that this strategy actually
                                    // doesn't tear down everything or leaves something hanging. If that is
                                    // the case, a new part of the reaper logic should handle it
                                },
                                Notifications::RegisterExpectedEvents(name) => {
                                    trace!(%name, "Computing and registering expected events for manifest");
                                    if let Some(scalers) = self.get_scalers(&name).await {
                                        scalers.reconcile_and_register_expected_events(&name).await;
                                    } else {
                                        debug!(%name, "Received request to register events for non-existent scalers, ignoring");
                                    }
                                },
                                Notifications::RemoveExpectedEvent((name, event)) => {
                                    trace!(%name, "Removing expected event for manifest");
                                    if let Some(scalers) = self.get_scalers(&name).await {
                                        futures::future::join_all(scalers.iter().map(|scaler| async {
                                            scaler.remove_event(&event).await
                                        }
                                        )).await;
                                    } else {
                                        debug!(%name, "Received request to remove event for non-existent scalers, ignoring");
                                    }
                                }
                                Notifications::ClearExpectedEvents(name) => {
                                    trace!(%name, "Clearing expected events for all scalers for manifest");
                                    if let Some(scalers) = self.get_scalers(&name).await {
                                        futures::future::join_all(scalers.iter().map(|scaler| async {
                                            scaler.set_events(vec![]).await
                                        }
                                        )).await;
                                    } else {
                                        debug!(%name, "Received request to clear events for non-existent scalers, ignoring");
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
pub(crate) fn components_to_scalers<S: ReadStore + Send + Sync + Clone + 'static>(
    components: &[Component],
    store: &S,
    lattice_id: &str,
    name: &str,
) -> ScalerList {
    let mut scalers: ScalerList = Vec::new();
    for component in components.iter() {
        let traits = component.traits.as_ref();
        match &component.properties {
            Properties::Actor { properties: props } => {
                scalers.extend(
                    traits
                        .unwrap_or(&EMPTY_TRAIT_VEC)
                        .iter()
                        .filter_map(|trt| match (trt.trait_type.as_str(), &trt.properties) {
                            (SPREADSCALER_TRAIT, TraitProperty::SpreadScaler(p)) => {
                                Some(Box::new(ActorSpreadScaler::new(
                                    store.clone(),
                                    props.image.to_owned(),
                                    lattice_id.to_owned(),
                                    name.to_owned(),
                                    p.to_owned(),
                                )) as BoxedScaler)
                            }
                            (LINKDEF_TRAIT, TraitProperty::Linkdef(p)) => components
                                .iter()
                                .find_map(|component| match &component.properties {
                                    Properties::Capability { properties: cappy }
                                        if component.name == p.target =>
                                    {
                                        Some(Box::new(LinkScaler::new(
                                            store.clone(),
                                            props.image.to_owned(),
                                            cappy.image.to_owned(),
                                            cappy.contract.to_owned(),
                                            cappy.link_name.to_owned(),
                                            lattice_id.to_owned(),
                                            name.to_owned(),
                                            p.values.to_owned(),
                                        ))
                                            as BoxedScaler)
                                    }
                                    _ => None,
                                }),
                            _ => None,
                        })
                        .map(|boxed| BackoffAwareScaler::new(boxed, name)),
                )
            }
            Properties::Capability { properties: props } => {
                if let Some(traits) = traits {
                    scalers.extend(
                        traits
                            .iter()
                            .filter_map(|trt| match (trt.trait_type.as_str(), &trt.properties) {
                                (SPREADSCALER_TRAIT, TraitProperty::SpreadScaler(p)) => {
                                    Some(Box::new(ProviderSpreadScaler::new(
                                        store.clone(),
                                        props.image.to_owned(),
                                        props.contract.to_owned(),
                                        props.link_name.to_owned(),
                                        lattice_id.to_owned(),
                                        name.to_owned(),
                                        p.to_owned(),
                                    )) as BoxedScaler)
                                }
                                _ => None,
                            })
                            .map(|boxed| BackoffAwareScaler::new(boxed, name)),
                    )
                } else {
                    // Allow providers to omit the scaler entirely for simplicity
                    scalers.push(BackoffAwareScaler::new(
                        Box::new(ProviderSpreadScaler::new(
                            store.clone(),
                            props.image.to_owned(),
                            props.contract.to_owned(),
                            props.link_name.to_owned(),
                            lattice_id.to_owned(),
                            name.to_owned(),
                            SpreadScalerProperty {
                                replicas: 1,
                                spread: vec![],
                            },
                        )),
                        name,
                    ))
                }
            }
        }
    }
    scalers
}
