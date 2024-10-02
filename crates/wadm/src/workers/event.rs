use std::collections::BTreeMap;
use std::collections::{hash_map::Entry, HashMap, HashSet};

use anyhow::Result;
use tracing::{debug, instrument, trace, warn};
use wadm_types::api::{ScalerStatus, Status, StatusInfo};
use wasmcloud_control_interface::{ComponentDescription, ProviderDescription};

use crate::commands::Command;
use crate::consumers::{
    manager::{WorkError, WorkResult, Worker},
    ScopedMessage,
};
use crate::events::*;
use crate::publisher::Publisher;
use crate::scaler::manager::{ScalerList, ScalerManager};
use crate::storage::{Component, Host, Provider, ProviderStatus, Store, WadmComponentInfo};
use crate::APP_SPEC_ANNOTATION;

use super::event_helpers::*;

pub struct EventWorker<StateStore, C: Clone, P: Clone> {
    store: StateStore,
    ctl_client: C,
    command_publisher: CommandPublisher<P>,
    status_publisher: StatusPublisher<P>,
    scalers: ScalerManager<StateStore, P, C>,
}

impl<StateStore, C, P> EventWorker<StateStore, C, P>
where
    StateStore: Store + Send + Sync + Clone + 'static,
    C: ClaimsSource
        + InventorySource
        + LinkSource
        + ConfigSource
        + SecretSource
        + Clone
        + Send
        + Sync
        + 'static,
    P: Publisher + Clone + Send + Sync + 'static,
{
    /// Creates a new event worker configured to use the given store and control interface client for fetching state
    pub fn new(
        store: StateStore,
        ctl_client: C,
        command_publisher: CommandPublisher<P>,
        status_publisher: StatusPublisher<P>,
        manager: ScalerManager<StateStore, P, C>,
    ) -> EventWorker<StateStore, C, P> {
        EventWorker {
            store,
            ctl_client,
            command_publisher,
            status_publisher,
            scalers: manager,
        }
    }

    // BEGIN HANDLERS
    // NOTE(thomastaylor312): These use anyhow errors because in the _single_ case where we have to
    // call the lattice controller, we no longer just have error types from the store. To handle the
    // multiple error cases, it was just easier to catch it into an anyhow Error and then convert at
    // the end

    #[instrument(level = "debug", skip(self, component), fields(component_id = %component.component_id, host_id = %component.host_id))]
    async fn handle_component_scaled(
        &self,
        lattice_id: &str,
        component: &ComponentScaled,
    ) -> anyhow::Result<()> {
        trace!("Scaling component in store");
        debug!("Fetching current data for component");

        // Update component count in the component state, adding to the state if it didn't exist or removing
        // if the scale is down to zero.
        let mut component_data = Component::from(component);
        if let Some(mut current) = self
            .store
            .get::<Component>(lattice_id, &component.component_id)
            .await?
        {
            trace!(component = ?current, "Found existing component data");

            match current.instances.get_mut(&component.host_id) {
                // If the component is running and is now scaled down to zero, remove it
                Some(current_instances) if component.max_instances == 0 => {
                    current_instances.remove(&component.annotations);
                }
                // If a component is already running on a host, update the running count to the scaled max_instances value
                Some(current_instances) => {
                    current_instances.replace(WadmComponentInfo {
                        count: component.max_instances,
                        annotations: component.annotations.clone(),
                    });
                }
                // Component is not running and now scaled to zero, no action required. This can happen if we
                // update the state before we receive the ComponentScaled event
                None if component.max_instances == 0 => (),
                // If a component isn't running yet, add it with the scaled max_instances value
                None => {
                    current.instances.insert(
                        component.host_id.clone(),
                        HashSet::from([WadmComponentInfo {
                            count: component.max_instances,
                            annotations: component.annotations.clone(),
                        }]),
                    );
                }
            }

            // If we stopped the last instance on a host, remove the host from the component data
            if current
                .instances
                .get(&component.host_id)
                .is_some_and(|instances| instances.is_empty())
            {
                current.instances.remove(&component.host_id);
            }

            // Take the updated counts and store them in the component data
            component_data.instances = current.instances;
        };

        // Update component count in the host state, removing the component if the scale is zero
        if let Some(mut host) = self
            .store
            .get::<Host>(lattice_id, &component.host_id)
            .await?
        {
            trace!(host = ?host, "Found existing host data");

            if component.max_instances == 0 {
                host.components.remove(&component.component_id);
            } else {
                host.components
                    .entry(component.component_id.clone())
                    .and_modify(|count| *count = component.max_instances)
                    .or_insert(component.max_instances);
            }

            self.store
                .store(lattice_id, host.id.to_owned(), host)
                .await?
        }

        if component_data.instances.is_empty() {
            self.store
                .delete::<Component>(lattice_id, &component.component_id)
                .await
                .map_err(anyhow::Error::from)
        } else {
            self.store
                .store(lattice_id, component.component_id.clone(), component_data)
                .await
                .map_err(anyhow::Error::from)
        }
    }

    #[instrument(level = "debug", skip(self, host), fields(host_id = %host.host_id))]
    async fn handle_host_heartbeat(
        &self,
        lattice_id: &str,
        host: &HostHeartbeat,
    ) -> anyhow::Result<()> {
        debug!("Updating store with current host heartbeat information");
        let host_data = Host::from(host);
        self.store
            .store(lattice_id, host.host_id.clone(), host_data)
            .await?;

        // NOTE: We can return an error here and then nack because we'll just reupdate the host data
        // with the exact same host heartbeat entry. There is no possibility of a duplicate
        self.heartbeat_provider_update(lattice_id, host, &host.providers)
            .await?;

        // NOTE: We can return an error here and then nack because we'll just reupdate the host data
        // with the exact same host heartbeat entry. There is no possibility of a duplicate
        self.heartbeat_component_update(lattice_id, host, &host.components)
            .await?;

        Ok(())
    }

    #[instrument(level = "debug", skip(self, host), fields(host_id = %host.id))]
    async fn handle_host_started(
        &self,
        lattice_id: &str,
        host: &HostStarted,
    ) -> anyhow::Result<()> {
        debug!("Updating store with new host");
        // New hosts have nothing running on them yet, so just drop it in the store
        self.store
            .store(lattice_id, host.id.clone(), Host::from(host))
            .await
            .map_err(anyhow::Error::from)
    }

    #[instrument(level = "debug", skip(self, host), fields(host_id = %host.id))]
    async fn handle_host_stopped(
        &self,
        lattice_id: &str,
        host: &HostStopped,
    ) -> anyhow::Result<()> {
        debug!("Handling host stopped event");
        // NOTE(thomastaylor312): Generally to get a host stopped event, the host should have
        // already sent a bunch of stop component/provider events, but for correctness sake, we fetch
        // the current host and make sure all the components and providers are removed
        trace!("Fetching current host data");
        let current: Host = match self.store.get(lattice_id, &host.id).await? {
            Some(h) => h,
            None => {
                debug!("Got host stopped event for a host we didn't have in the store");
                return Ok(());
            }
        };

        trace!("Fetching components from store to remove stopped instances");
        let all_components = self.store.list::<Component>(lattice_id).await?;

        #[allow(clippy::type_complexity)]
        let (components_to_update, components_to_delete): (
            Vec<(String, Component)>,
            Vec<(String, Component)>,
        ) = all_components
            .into_iter()
            .filter_map(|(id, mut component)| {
                if current.components.contains_key(&id) {
                    component.instances.remove(&current.id);
                    Some((id, component))
                } else {
                    None
                }
            })
            .partition(|(_, component)| !component.instances.is_empty());
        trace!("Storing updated components in store");
        self.store
            .store_many(lattice_id, components_to_update)
            .await?;

        trace!("Removing components with no more running instances");
        self.store
            .delete_many::<Component, _, _>(
                lattice_id,
                components_to_delete.into_iter().map(|(id, _)| id),
            )
            .await?;

        trace!("Fetching providers from store to remove stopped instances");
        let all_providers = self.store.list::<Provider>(lattice_id).await?;

        #[allow(clippy::type_complexity)]
        let (providers_to_update, providers_to_delete): (Vec<(String, Provider)>, Vec<(String, Provider)>) = current
            .providers
            .into_iter()
            .filter_map(|info| {
                let key = info.provider_id;
                // NOTE: We can do this without cloning, but it led to some confusing code involving
                // `remove` from the owned `all_providers` map. This is more readable at the expense of
                // a clone for few providers
                match all_providers.get(&key).cloned() {
                    // If we successfully remove the host, map it to the right type, otherwise we can
                    // continue onward
                    Some(mut prov) => prov.hosts.remove(&host.id).map(|_| (key, prov)),
                    None => {
                        warn!(key = %key, "Didn't find provider in storage even though host said it existed");
                        None
                    }
                }
            })
            .partition(|(_, provider)| !provider.hosts.is_empty());
        trace!("Storing updated providers in store");
        self.store
            .store_many(lattice_id, providers_to_update)
            .await?;

        trace!("Removing providers with no more running instances");
        self.store
            .delete_many::<Provider, _, _>(
                lattice_id,
                providers_to_delete.into_iter().map(|(id, _)| id),
            )
            .await?;

        // Order matters here: Now that we've cleaned stuff up, remove the host. We do this last
        // because if any of the above fails after we remove the host, we won't be able to fetch the
        // data to remove the components and providers on a retry.
        debug!("Deleting host from store");
        self.store
            .delete::<Host>(lattice_id, &host.id)
            .await
            .map_err(anyhow::Error::from)
    }

    #[instrument(
        level = "debug",
        skip(self, provider),
        fields(
            provider_id = %provider.provider_id,
            image_ref = %provider.image_ref,
        )
    )]
    async fn handle_provider_started(
        &self,
        lattice_id: &str,
        provider: &ProviderStarted,
    ) -> anyhow::Result<()> {
        debug!("Handling provider started event");
        let id = &provider.provider_id;
        trace!("Fetching current data from store");
        let mut needs_host_update = false;
        let provider_data = if let Some(mut current) =
            self.store.get::<Provider>(lattice_id, id).await?
        {
            // Using the entry api is a bit more efficient because we do a single key lookup
            let mut prov = match current.hosts.entry(provider.host_id.clone()) {
                Entry::Occupied(_) => {
                    trace!("Found host entry for the provider already in store. Will not update");
                    current
                }
                Entry::Vacant(entry) => {
                    entry.insert(ProviderStatus::default());
                    needs_host_update = true;
                    current
                }
            };
            // Update missing fields if they exist. Right now if we just discover a provider from
            // health check, these will be empty
            if prov.issuer.is_empty() || prov.reference.is_empty() {
                let new_prov = Provider::from(provider);
                prov.issuer = new_prov.issuer;
                prov.reference = new_prov.reference;
            }
            prov
        } else {
            trace!("No current provider found in store");
            let mut prov = Provider::from(provider);
            prov.hosts = HashMap::from([(provider.host_id.clone(), ProviderStatus::default())]);
            needs_host_update = true;
            prov
        };

        // Insert provider into host map
        if let (Some(mut host), true) = (
            self.store
                .get::<Host>(lattice_id, &provider.host_id)
                .await?,
            needs_host_update,
        ) {
            trace!(host = ?host, "Found existing host data");

            host.providers.replace(ProviderInfo {
                provider_id: id.to_owned(),
                provider_ref: provider.image_ref.to_owned(),
                annotations: provider.annotations.to_owned(),
            });

            self.store
                .store(lattice_id, host.id.to_owned(), host)
                .await?
        }

        debug!("Storing updated provider in store");
        self.store
            .store(lattice_id, id.to_owned(), provider_data)
            .await
            .map_err(anyhow::Error::from)
    }

    #[instrument(
        level = "debug",
        skip(self, provider),
        fields(
            provider_id = %provider.provider_id,
        )
    )]
    async fn handle_provider_stopped(
        &self,
        lattice_id: &str,
        provider: &ProviderStopped,
    ) -> anyhow::Result<()> {
        debug!("Handling provider stopped event");
        let id = &provider.provider_id;
        trace!("Fetching current data from store");

        // Remove provider from host map
        if let Some(mut host) = self
            .store
            .get::<Host>(lattice_id, &provider.host_id)
            .await?
        {
            trace!(host = ?host, "Found existing host data");

            host.providers.remove(&ProviderInfo {
                provider_id: provider.provider_id.to_owned(),
                // We do not hash based on provider reference, so it can be blank here
                provider_ref: "".to_string(),
                // We don't have this information, nor do we need it since we don't hash based
                // on annotations
                annotations: BTreeMap::default(),
            });

            self.store
                .store(lattice_id, host.id.to_owned(), host)
                .await?
        }

        if let Some(mut current) = self.store.get::<Provider>(lattice_id, id).await? {
            if current.hosts.remove(&provider.host_id).is_none() {
                trace!(host_id = %provider.host_id, "Did not find host entry in provider");
                return Ok(());
            }
            if current.hosts.is_empty() {
                debug!("Provider is no longer running on any hosts. Removing from store");
                self.store
                    .delete::<Provider>(lattice_id, id)
                    .await
                    .map_err(anyhow::Error::from)
            } else {
                debug!("Storing updated provider");
                self.store
                    .store(lattice_id, id.to_owned(), current)
                    .await
                    .map_err(anyhow::Error::from)
            }
        } else {
            trace!("No current provider found in store");
            Ok(())
        }
    }

    #[instrument(
        level = "debug",
        skip(self, provider),
        fields(
            provider_id = %provider.provider_id,
        )
    )]
    async fn handle_provider_health_check(
        &self,
        lattice_id: &str,
        provider: &ProviderHealthCheckInfo,
        failed: Option<bool>,
    ) -> anyhow::Result<()> {
        debug!("Handling provider health check event");
        trace!("Getting current provider");
        let id = &provider.provider_id;
        let host_id = &provider.host_id;
        let mut current: Provider = match self.store.get(lattice_id, id).await? {
            Some(p) => p,
            None => {
                trace!("Didn't find provider in store. Creating");
                Provider {
                    id: id.clone(),
                    ..Default::default()
                }
            }
        };

        let status = match (current.hosts.get(host_id), failed) {
            // If the provider status changed from when we last saw it, modify status
            (_, Some(true)) => Some(ProviderStatus::Failed),
            (_, Some(false)) => Some(ProviderStatus::Running),
            // If the provider is pending or we missed the initial start and we get a health check
            // status, assume it's running fine.
            (Some(ProviderStatus::Pending) | None, None) => Some(ProviderStatus::Running),
            _ => None,
        };

        if let Some(status) = status {
            debug!("Updating store with current status");
            current.hosts.insert(host_id.to_owned(), status);
        }

        // TODO(thomastaylor312): Once we are able to fetch refmaps from the ctl client, we should
        // make it update any empty references with the data from the refmap

        self.store
            .store(lattice_id, id.to_owned(), current)
            .await
            .map_err(anyhow::Error::from)
    }

    // END HANDLER FUNCTIONS
    async fn populate_component_info(
        &self,
        components: &HashMap<String, Component>,
        host_id: &str,
        instance_map: Vec<ComponentDescription>,
    ) -> anyhow::Result<Vec<(String, Component)>> {
        let claims = self.ctl_client.get_claims().await?;

        Ok(instance_map
            .into_iter()
            .map(|component_description| {
                let instance = HashSet::from_iter([WadmComponentInfo {
                    count: component_description.max_instances() as usize,
                    annotations: component_description
                        .annotations()
                        .cloned()
                        .unwrap_or_default(),
                }]);
                if let Some(component) = components.get(component_description.id()) {
                    // Construct modified Component with new instances included
                    let mut new_instances = component.instances.clone();
                    new_instances.insert(host_id.to_owned(), instance);
                    let component = Component {
                        instances: new_instances,
                        reference: component_description.image_ref().into(),
                        name: component_description
                            .name()
                            .unwrap_or(&component.name)
                            .into(),
                        ..component.clone()
                    };

                    (component_description.id().to_string(), component)
                } else if let Some(claim) = claims.get(component_description.id()) {
                    (
                        component_description.id().to_string(),
                        Component {
                            id: component_description.id().into(),
                            name: claim.name.to_owned(),
                            issuer: claim.issuer.to_owned(),
                            instances: HashMap::from_iter([(host_id.to_owned(), instance)]),
                            reference: component_description.image_ref().into(),
                        },
                    )
                } else {
                    debug!("Claims not found for component on host, component is unsigned");

                    (
                        component_description.id().to_string(),
                        Component {
                            id: component_description.id().into(),
                            name: "".to_owned(),
                            issuer: "".to_owned(),
                            instances: HashMap::from_iter([(host_id.to_owned(), instance)]),
                            reference: component_description.image_ref().into(),
                        },
                    )
                }
            })
            .collect::<Vec<(String, Component)>>())
    }

    #[instrument(level = "debug", skip(self, host), fields(host_id = %host.host_id))]
    async fn heartbeat_component_update(
        &self,
        lattice_id: &str,
        host: &HostHeartbeat,
        inventory_components: &Vec<ComponentDescription>,
    ) -> anyhow::Result<()> {
        debug!("Fetching current component state");
        let components = self.store.list::<Component>(lattice_id).await?;

        // Compare stored Components to the "true" list on this host, updating stored
        // Components when they differ from the authoratative heartbeat
        let components_to_update = inventory_components
            .iter()
            .filter_map(|component_description| {
                if components
                    .get(component_description.id())
                    // NOTE(brooksmtownsend): This code maps the component to a boolean indicating if it's up-to-date with the heartbeat or not.
                    // If the component matches what the heartbeat says, we return None, otherwise we return Some(component_description).
                    .map(|component| {
                        // If the stored reference isn't what we receive on the heartbeat, update
                        component_description.image_ref() == component.reference
                            && component
                                .instances
                                .get(&host.host_id)
                                .map(|store_instances| {
                                    // Update if the number of instances is different
                                    if store_instances.len() != component.instances.len() {
                                        return false;
                                    }
                                    // Update if annotations or counts are different
                                    let annotations: BTreeMap<String, String> =
                                        component_description
                                            .annotations()
                                            .cloned()
                                            .map(|a| a.into_iter().collect())
                                            .unwrap_or_default();
                                    store_instances.get(&annotations).map_or(
                                        false,
                                        |store_instance| {
                                            component_description.max_instances() as usize
                                                == store_instance.count
                                        },
                                    )
                                })
                                .unwrap_or(false)
                    })
                    .unwrap_or(false)
                {
                    None
                } else {
                    Some(component_description.to_owned())
                }
            })
            // component ID to all instances on this host
            .collect::<Vec<ComponentDescription>>();

        let components_to_store = self
            .populate_component_info(&components, &host.host_id, components_to_update)
            .await?;

        trace!("Updating components with new status from host");

        self.store
            .store_many(lattice_id, components_to_store)
            .await?;

        Ok(())
    }

    #[instrument(level = "debug", skip(self, heartbeat), fields(host_id = %heartbeat.host_id))]
    async fn heartbeat_provider_update(
        &self,
        lattice_id: &str,
        heartbeat: &HostHeartbeat,
        inventory_providers: &Vec<ProviderDescription>,
    ) -> anyhow::Result<()> {
        debug!("Fetching current provider state");
        let providers = self.store.list::<Provider>(lattice_id).await?;
        let providers_to_update = inventory_providers.iter().filter_map(|info| {
            // NOTE: We can do this without cloning, but it led to some confusing code involving
            // `remove` from the owned `providers` map. This is more readable at the expense of
            // a clone for few providers
            match providers.get(info.id()).cloned() {
                Some(mut prov) => {
                    let mut has_changes = false;
                    if prov.name.is_empty() {
                        prov.name = info.name().map(String::from).unwrap_or_default();
                        has_changes = true;
                    }
                    if prov.reference.is_empty() {
                        prov.reference = info.image_ref().map(String::from).unwrap_or_default();
                        has_changes = true;
                    }
                    if let Entry::Vacant(entry) = prov.hosts.entry(heartbeat.host_id.clone()) {
                        entry.insert(ProviderStatus::default());
                        has_changes = true;
                    }
                    if has_changes {
                        Some((info.id().to_string(), prov))
                    } else {
                        None
                    }
                }
                None => {
                    // If we don't already have the provider, create a basic one so we know it
                    // exists at least. The next provider heartbeat will fix it for us
                    Some((
                        info.id().to_string(),
                        Provider {
                            id: info.id().to_string(),
                            hosts: [(heartbeat.host_id.clone(), ProviderStatus::default())].into(),
                            name: info.name().map(String::from).unwrap_or_default(),
                            reference: info.image_ref().map(String::from).unwrap_or_default(),
                            ..Default::default()
                        },
                    ))
                }
            }
        });

        trace!("Updating providers with new status from host");
        self.store
            .store_many(lattice_id, providers_to_update)
            .await?;

        Ok(())
    }

    #[instrument(level = "debug", skip(self, data), fields(name = %data.manifest.metadata.name))]
    async fn handle_manifest_published(
        &self,
        lattice_id: &str,
        data: &ManifestPublished,
    ) -> anyhow::Result<()> {
        debug!(name = %data.manifest.metadata.name, "Handling published manifest");

        let old_scalers = self
            .scalers
            .remove_raw_scalers(&data.manifest.metadata.name)
            .await;
        let scalers = self.scalers.scalers_for_manifest(&data.manifest);

        // Refresh the snapshot data before cleaning up and/or adding scalers
        self.scalers.refresh_data().await?;
        let cleanup_commands = if let Some(old_scalers) = old_scalers {
            // This relies on the idea that an ID is a unique identifier for a scaler, and any
            // change in the ID is indicative of the fact that the scaler is outdated and should be cleaned up.
            let (_updated_component, outdated_component): (ScalerList, ScalerList) = old_scalers
                .into_iter()
                .partition(|old| scalers.iter().any(|new| new.id() == old.id()));

            // Clean up any resources from scalers that no longer exist
            let futs = outdated_component
                .iter()
                .map(|s| async { s.cleanup().await });
            futures::future::join_all(futs)
                .await
                .into_iter()
                .filter_map(|res: Result<Vec<Command>>| match res {
                    Ok(commands) => Some(commands),
                    Err(e) => {
                        warn!(error = ?e, "Failed to clean up old scaler");
                        None
                    }
                })
                .flatten()
                .collect::<Vec<Command>>()
        } else {
            vec![]
        };

        // Get the results of the first reconcilation pass before we store the scalers. Publish the
        // commands for the ones that succeeded (as those scalers will have entered backoff mode if
        // they are backoff wrapped), and then return an error indicating failure so the event is
        // redelivered. When it is, the ones that succeeded will be in backoff mode and the ones
        // that failed will be retried.

        let scalers = self.scalers.add_scalers(&data.manifest, scalers).await?;

        let (commands, res) = get_commands_and_result(
            scalers.iter().map(|s| s.reconcile()),
            "Errors occurred during initial reconciliation",
        )
        .await;

        let status = detailed_scaler_status(&scalers).await;

        trace!(?status, "Setting status");
        if let Err(e) = self
            .status_publisher
            .publish_status(data.manifest.metadata.name.as_ref(), status)
            .await
        {
            warn!("Failed to set manifest status: {e:}");
        };

        trace!(?commands, "Publishing commands");
        // Handle the result from initial reconciliation. This lets us handle the net new stuff
        // immediately
        self.command_publisher.publish_commands(commands).await?;

        // Now publish the cleanup commands from the old scalers. This will cause the new scalers to
        // react to the components/providers/linkdefs disappearing and create new ones with the new
        // versions
        if let Err(e) = self
            .command_publisher
            .publish_commands(cleanup_commands)
            .await
        {
            warn!(error = ?e, "Failed to publish cleanup commands from old application, some resources may be left behind");
        }

        res
    }

    #[instrument(level = "debug", skip(self))]
    async fn run_scalers_with_hint(&self, event: &Event, name: &str) -> anyhow::Result<()> {
        let scalers = match self.scalers.get_scalers(name).await {
            Some(scalers) => scalers,
            None => {
                debug!("No scalers currently exist for model");
                return Ok(());
            }
        };
        // Refresh the snapshot data before running
        self.scalers.refresh_data().await?;
        let (commands, res) = get_commands_and_result(
            scalers.iter().map(|s| s.handle_event(event)),
            "Errors occurred while handling event",
        )
        .await;

        let status = detailed_scaler_status(&scalers).await;
        trace!(?status, "Setting status");
        if let Err(e) = self.status_publisher.publish_status(name, status).await {
            warn!(error = ?e, "Failed to set status for scaler");
        };

        trace!(?commands, "Publishing commands");
        self.command_publisher.publish_commands(commands).await?;

        res
    }

    #[instrument(level = "debug", skip(self))]
    async fn run_all_scalers(&self, event: &Event) -> anyhow::Result<()> {
        let scalers = self.scalers.get_all_scalers().await;
        // Refresh the snapshot data before running
        self.scalers.refresh_data().await?;
        let futs = scalers.iter().map(|(name, scalers)| async move {
            let (commands, res) = get_commands_and_result(
                scalers.iter().map(|scaler| scaler.handle_event(event)),
                "Errors occurred while handling event with all scalers",
            )
            .await;

            let status = detailed_scaler_status(scalers).await;

            trace!(?status, "Setting status");
            if let Err(e) = self.status_publisher.publish_status(name, status).await {
                warn!(error = ?e, "Failed to set status for scaler");
            };

            (commands, res)
        });

        // Resolve futures, computing commands for scalers, publishing statuses, and combining any errors
        let (commands, res) = futures::future::join_all(futs).await.into_iter().fold(
            (vec![], Ok(())),
            |(mut cmds, res), (mut new_cmds, new_res)| {
                cmds.append(&mut new_cmds);
                let res = match (res, new_res) {
                    (Ok(_), Ok(_)) => Ok(()),
                    (Ok(_), Err(e)) | (Err(e), Ok(_)) => Err(e),
                    (Err(e), Err(e2)) => Err(e.context(e2)),
                };
                (cmds, res)
            },
        );

        trace!(?commands, "Publishing commands");
        self.command_publisher.publish_commands(commands).await?;

        res
    }
}

#[async_trait::async_trait]
impl<StateStore, C, P> Worker for EventWorker<StateStore, C, P>
where
    StateStore: Store + Send + Sync + Clone + 'static,
    C: ClaimsSource
        + InventorySource
        + LinkSource
        + ConfigSource
        + SecretSource
        + Clone
        + Send
        + Sync
        + 'static,
    P: Publisher + Clone + Send + Sync + 'static,
{
    type Message = Event;

    #[instrument(level = "debug", skip(self))]
    async fn do_work(&self, mut message: ScopedMessage<Self::Message>) -> WorkResult<()> {
        // Everything in this block returns a name hint for the success case and an error otherwise
        let res = match message.as_ref() {
            Event::ComponentScaled(component) => self
                .handle_component_scaled(&message.lattice_id, component)
                .await
                .map(|_| {
                    component
                        .annotations
                        .get(APP_SPEC_ANNOTATION)
                        .map(|s| s.as_str())
                }),
            Event::HostHeartbeat(host) => self
                .handle_host_heartbeat(&message.lattice_id, host)
                .await
                .map(|_| None),
            Event::HostStarted(host) => self
                .handle_host_started(&message.lattice_id, host)
                .await
                .map(|_| None),
            Event::HostStopped(host) => self
                .handle_host_stopped(&message.lattice_id, host)
                .await
                .map(|_| None),
            Event::ProviderStarted(provider) => self
                .handle_provider_started(&message.lattice_id, provider)
                .await
                .map(|_| {
                    provider
                        .annotations
                        .get(APP_SPEC_ANNOTATION)
                        .map(|s| s.as_str())
                }),
            // NOTE(thomastaylor312): Provider stopped events need to be handled by all scalers as
            // it they could need to adjust their provider count based on the number of providers
            // available throughout the whole lattice (e.g. if a provider managed by another
            // manifest is removed, but this manifest still needs one). Ideally we should have a way
            // to have a "global" list of required providers in a lattice so we never shut one down
            // just to spin it back up, but for now we'll just deal with this as is
            Event::ProviderStopped(provider) => self
                .handle_provider_stopped(&message.lattice_id, provider)
                .await
                .map(|_| None),
            Event::ProviderHealthCheckStatus(ProviderHealthCheckStatus { data }) => self
                .handle_provider_health_check(&message.lattice_id, data, None)
                .await
                .map(|_| None),
            Event::ProviderHealthCheckPassed(ProviderHealthCheckPassed { data }) => self
                .handle_provider_health_check(&message.lattice_id, data, Some(false))
                .await
                .map(|_| None),
            Event::ProviderHealthCheckFailed(ProviderHealthCheckFailed { data }) => self
                .handle_provider_health_check(&message.lattice_id, data, Some(true))
                .await
                .map(|_| None),
            Event::ManifestPublished(data) => self
                .handle_manifest_published(&message.lattice_id, data)
                .await
                .map(|_| None),
            Event::ManifestUnpublished(data) => {
                debug!("Handling unpublished manifest");

                match self.scalers.remove_scalers(&data.name).await {
                    Some(Ok(_)) => {
                        return message.ack().await.map_err(WorkError::from);
                    }
                    Some(Err(e)) => {
                        message.nack().await;
                        return Err(WorkError::Other(e.into()));
                    }
                    None => Ok(None),
                }
            }
            // All other events we don't care about for state. Explicitly mention them in order
            // to make sure we don't forget to handle them when new events are added.
            Event::LinkdefSet(_)
            | Event::LinkdefDeleted(_)
            | Event::ConfigSet(_)
            | Event::ConfigDeleted(_)
            | Event::ProviderStartFailed(_)
            | Event::ComponentScaleFailed(_) => {
                trace!("Got event we don't care about. Not modifying state.");
                Ok(None)
            }
        };

        let res = match res {
            Ok(Some(name)) => self.run_scalers_with_hint(&message, name).await,
            Ok(None) => self.run_all_scalers(&message).await,
            Err(e) => Err(e),
        }
        .map_err(Box::<dyn std::error::Error + Send + 'static>::from);

        if let Err(e) = res {
            message.nack().await;
            return Err(WorkError::Other(e));
        }

        message.ack().await.map_err(WorkError::from)
    }
}

/// Helper that runs any iterable of futures and returns a list of commands and the proper result to
/// use as a response (Ok if there were no errors, all of the errors combined otherwise)
pub(crate) async fn get_commands_and_result<Fut, I>(
    futs: I,
    error_message: &str,
) -> (Vec<Command>, Result<()>)
where
    Fut: futures::Future<Output = Result<Vec<Command>>>,
    I: IntoIterator<Item = Fut>,
{
    let (commands, errors): (Vec<_>, Vec<_>) = futures::future::join_all(futs)
        .await
        .into_iter()
        .partition(Result::is_ok);
    // SAFETY: We partitioned by Ok and Err, so the unwraps are fine
    (
        commands.into_iter().flat_map(Result::unwrap).collect(),
        map_to_result(
            errors.into_iter().map(Result::unwrap_err).collect(),
            error_message,
        ),
    )
}

/// Helper function to find the [`Status`] of all scalers in a particular manifest.
pub async fn detailed_scaler_status(scalers: &ScalerList) -> Status {
    let futs = scalers.iter().map(|s| async {
        (
            s.id().to_string(),
            s.kind().to_string(),
            s.name(),
            s.status().await,
        )
    });
    let status = futures::future::join_all(futs).await;
    Status::new(
        StatusInfo {
            status_type: status
                .iter()
                .map(|(_id, _name, _kind, s)| s.status_type)
                .sum(),
            message: status
                .iter()
                .filter_map(|(_id, _name, _kind, s)| {
                    let message = s.message.trim();
                    if message.is_empty() {
                        None
                    } else {
                        Some(message.to_owned())
                    }
                })
                .collect::<Vec<_>>()
                .join(", "),
        },
        status
            .into_iter()
            .map(|(id, kind, name, info)| ScalerStatus {
                id,
                name,
                kind,
                info,
            })
            .collect(),
    )
}

fn map_to_result(errors: Vec<anyhow::Error>, error_message: &str) -> Result<()> {
    if errors.is_empty() {
        Ok(())
    } else {
        let mut error = anyhow::anyhow!("{error_message}");
        for e in errors {
            error = error.context(e);
        }
        Err(error)
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use tokio::sync::RwLock;
    use wasmcloud_control_interface::{ComponentDescription, HostInventory, ProviderDescription};

    use super::*;

    use crate::{
        storage::ReadStore,
        test_util::{NoopPublisher, TestLatticeSource, TestStore},
    };

    // NOTE: This test is rather long because we want to run through what an actual state generation
    // loop would look like. This mostly covers happy path, while the other tests cover more of the
    // edge cases
    #[tokio::test]
    async fn test_all_state() {
        let store = Arc::new(TestStore::default());
        let inventory = Arc::new(RwLock::new(HashMap::default()));
        let lattice_source = TestLatticeSource {
            inventory: inventory.clone(),
            ..Default::default()
        };

        let lattice_id = "all_state";

        let command_publisher = CommandPublisher::new(NoopPublisher, "doesntmatter");
        let status_publisher = StatusPublisher::new(NoopPublisher, None, "doesntmatter");
        let worker = EventWorker::new(
            store.clone(),
            lattice_source.clone(),
            command_publisher.clone(),
            status_publisher.clone(),
            ScalerManager::test_new(
                NoopPublisher,
                lattice_id,
                store.clone(),
                command_publisher,
                status_publisher.clone(),
                lattice_source,
            )
            .await,
        );

        let host1_id = "DS1";
        let host2_id = "starkiller";

        /***********************************************************/
        /******************** Host Start Tests *********************/
        /***********************************************************/
        let labels = HashMap::from([("superweapon".to_string(), "true".to_string())]);
        worker
            .handle_host_started(
                lattice_id,
                &HostStarted {
                    friendly_name: "death-star-42".to_string(),
                    id: host1_id.into(),
                    labels: labels.clone(),
                },
            )
            .await
            .expect("Should be able to handle event");

        let current_state = store.list::<Host>(lattice_id).await.unwrap();
        assert_eq!(current_state.len(), 1, "Only one host should be in store");
        let host = current_state
            .get("DS1")
            .expect("Host should exist in state");
        assert_eq!(
            host.friendly_name, "death-star-42",
            "Host should have the proper name in state"
        );
        assert_eq!(host.labels, labels, "Host should have the correct labels");

        let labels2 = HashMap::from([
            ("superweapon".to_string(), "true".to_string()),
            ("lazy_writing".to_string(), "true".to_string()),
        ]);
        worker
            .handle_host_started(
                lattice_id,
                &HostStarted {
                    friendly_name: "starkiller-base-2015".to_string(),
                    id: host2_id.into(),
                    labels: labels2.clone(),
                },
            )
            .await
            .expect("Should be able to handle event");

        let current_state = store.list::<Host>(lattice_id).await.unwrap();
        assert_eq!(current_state.len(), 2, "Both hosts should be in the store");
        let host = current_state
            .get("starkiller")
            .expect("Host should exist in state");
        assert_eq!(
            host.friendly_name, "starkiller-base-2015",
            "Host should have the proper name in state"
        );
        assert_eq!(host.labels, labels2, "Host should have the correct labels");

        // Now just double check that the other host didn't change in response to the new one
        let host = current_state
            .get("DS1")
            .expect("Host should exist in state");
        assert_eq!(
            host.friendly_name, "death-star-42",
            "Host should have the proper name in state"
        );
        assert_eq!(host.labels, labels, "Host should have the correct labels");

        /***********************************************************/
        /****************** Component Scale Tests ******************/
        /***********************************************************/

        let component1_scaled = ComponentScaled {
            claims: Some(ComponentClaims {
                call_alias: Some("Grand Moff".into()),
                issuer: "Sheev Palpatine".into(),
                name: "Grand Moff Tarkin".into(),
                version: Some("0.1.0".into()),
                ..Default::default()
            }),
            image_ref: "coruscant.galactic.empire/tarkin:0.1.0".into(),
            component_id: "TARKIN".into(),
            host_id: host1_id.into(),
            annotations: BTreeMap::default(),
            max_instances: 500,
        };
        worker
            .handle_component_scaled(lattice_id, &component1_scaled)
            .await
            .expect("Should be able to handle component event");
        let components = store.list::<Component>(lattice_id).await.unwrap();
        let component = components
            .get("TARKIN")
            .expect("Component should exist in state");
        let hosts = store.list::<Host>(lattice_id).await.unwrap();
        let host = hosts.get(host1_id).expect("Host should exist in state");
        assert_eq!(
            host.components.get(&component1_scaled.component_id),
            Some(&500),
            "Component count in host should be updated"
        );
        assert_eq!(
            component.count_for_host(host1_id),
            500,
            "Component count should be modified with an increase in scale"
        );

        let component1_scaled = ComponentScaled {
            claims: Some(ComponentClaims {
                call_alias: Some("Grand Moff".into()),
                issuer: "Sheev Palpatine".into(),
                name: "Grand Moff Tarkin".into(),
                version: Some("0.1.0".into()),
                ..Default::default()
            }),
            image_ref: "coruscant.galactic.empire/tarkin:0.1.0".into(),
            component_id: "TARKIN".into(),
            host_id: host1_id.into(),
            annotations: BTreeMap::default(),
            max_instances: 200,
        };
        worker
            .handle_component_scaled(lattice_id, &component1_scaled)
            .await
            .expect("Should be able to handle component event");
        let components = store.list::<Component>(lattice_id).await.unwrap();
        let component = components
            .get("TARKIN")
            .expect("Component should exist in state");
        let hosts = store.list::<Host>(lattice_id).await.unwrap();
        let host = hosts.get(host1_id).expect("Host should exist in state");
        assert_eq!(
            host.components.get(&component1_scaled.component_id),
            Some(&200),
            "Component count in host should be updated"
        );
        assert_eq!(
            component.count_for_host(host1_id),
            200,
            "Component count should be modified with a decrease in scale"
        );

        let component1_scaled = ComponentScaled {
            claims: Some(ComponentClaims {
                call_alias: Some("Grand Moff".into()),
                issuer: "Sheev Palpatine".into(),
                name: "Grand Moff Tarkin".into(),
                version: Some("0.1.0".into()),
                ..Default::default()
            }),
            image_ref: "coruscant.galactic.empire/tarkin:0.1.0".into(),
            component_id: "TARKIN".into(),
            host_id: host1_id.into(),
            annotations: BTreeMap::default(),
            max_instances: 0,
        };
        worker
            .handle_component_scaled(lattice_id, &component1_scaled)
            .await
            .expect("Should be able to handle component event");
        let components = store.list::<Component>(lattice_id).await.unwrap();
        let hosts = store.list::<Host>(lattice_id).await.unwrap();
        let host = hosts.get(host1_id).expect("Host should exist in state");
        assert_eq!(
            host.components.get(&component1_scaled.component_id),
            None,
            "Component in host should be removed"
        );
        assert!(
            !components.contains_key("TARKIN"),
            "Component should be removed from state"
        );

        let component1_scaled = ComponentScaled {
            claims: Some(ComponentClaims {
                call_alias: Some("Grand Moff".into()),
                issuer: "Sheev Palpatine".into(),
                name: "Grand Moff Tarkin".into(),
                version: Some("0.1.0".into()),
                ..Default::default()
            }),
            image_ref: "coruscant.galactic.empire/tarkin:0.1.0".into(),
            component_id: "TARKIN".into(),
            host_id: host1_id.into(),
            annotations: BTreeMap::default(),
            max_instances: 1,
        };
        worker
            .handle_component_scaled(lattice_id, &component1_scaled)
            .await
            .expect("Should be able to handle component event");
        let components = store.list::<Component>(lattice_id).await.unwrap();
        let component = components
            .get("TARKIN")
            .expect("Component should exist in state");
        let hosts = store.list::<Host>(lattice_id).await.unwrap();
        let host = hosts.get(host1_id).expect("Host should exist in state");
        assert_eq!(
            host.components.get(&component1_scaled.component_id),
            Some(&1),
            "Component in host should be readded from scratch"
        );
        assert_eq!(
            component.count_for_host(host1_id),
            1,
            "Component count should be modified with an initial start"
        );
        worker
            .handle_component_scaled(
                lattice_id,
                &ComponentScaled {
                    host_id: host2_id.into(),
                    ..component1_scaled.clone()
                },
            )
            .await
            .expect("Should be able to handle component event");

        let component2_scaled = ComponentScaled {
            claims: Some(ComponentClaims {
                call_alias: Some("Darth".into()),
                issuer: "Sheev Palpatine".into(),
                name: "Darth Vader".into(),
                version: Some("0.1.0".into()),
                ..Default::default()
            }),
            image_ref: "coruscant.galactic.empire/vader:0.1.0".into(),
            host_id: host1_id.into(),
            component_id: "DARTHVADER".into(),
            annotations: BTreeMap::default(),
            max_instances: 2,
        };

        worker
            .handle_component_scaled(lattice_id, &component2_scaled)
            .await
            .expect("Should be able to handle component scaled event");
        worker
            .handle_component_scaled(
                lattice_id,
                &ComponentScaled {
                    host_id: host2_id.into(),
                    ..component2_scaled.clone()
                },
            )
            .await
            .expect("Should be able to handle component event");

        /***********************************************************/
        /****************** Provider Start Tests *******************/
        /***********************************************************/

        let provider1 = ProviderStarted {
            claims: Some(ProviderClaims {
                issuer: "Sheev Palpatine".into(),
                name: "Force Choke".into(),
                version: "0.1.0".into(),
                ..Default::default()
            }),
            image_ref: "coruscant.galactic.empire/force_choke:0.1.0".into(),
            provider_id: "CHOKE".into(),
            host_id: host1_id.into(),
            annotations: BTreeMap::default(),
        };

        let provider2 = ProviderStarted {
            claims: Some(ProviderClaims {
                issuer: "Sheev Palpatine".into(),
                name: "Death Star Laser".into(),
                version: "0.1.0".into(),
                ..Default::default()
            }),
            image_ref: "coruscant.galactic.empire/laser:0.1.0".into(),
            provider_id: "BYEBYEALDERAAN".into(),
            host_id: host2_id.into(),
            annotations: BTreeMap::default(),
        };

        worker
            .handle_provider_started(lattice_id, &provider1)
            .await
            .expect("Should be able to handle provider event");
        let providers = store.list::<Provider>(lattice_id).await.unwrap();
        assert_eq!(providers.len(), 1, "Should only be 1 provider in state");
        assert_provider(&providers, &provider1, &[host1_id]);

        // Now start the second provider on both hosts (so we can test some things in the next test)
        worker
            .handle_provider_started(lattice_id, &provider2)
            .await
            .expect("Should be able to handle provider event");
        worker
            .handle_provider_started(
                lattice_id,
                &ProviderStarted {
                    host_id: host1_id.into(),
                    provider_id: provider2.provider_id.clone(),
                    ..provider2.clone()
                },
            )
            .await
            .expect("Should be able to handle provider event");
        let providers = store.list::<Provider>(lattice_id).await.unwrap();
        assert_eq!(providers.len(), 2, "Should only be 2 providers in state");
        assert_provider(&providers, &provider2, &[host1_id, host2_id]);

        // Check that hosts got updated properly
        let hosts = store.list::<Host>(lattice_id).await.unwrap();
        assert_eq!(hosts.len(), 2, "Should only have 2 hosts");
        let host = hosts.get(host1_id).expect("Host should still exist");
        assert_eq!(
            host.components.len(),
            2,
            "Should have two different components running"
        );
        assert_eq!(
            host.providers.len(),
            2,
            "Should have two different providers running"
        );
        let host = hosts.get(host2_id).expect("Host should still exist");
        assert_eq!(
            host.components.len(),
            2,
            "Should have two different components running"
        );
        assert_eq!(
            host.providers.len(),
            1,
            "Should have a single provider running"
        );

        let component_1_id = "TARKIN";
        let component_2_id = "DARTHVADER";

        worker
            .handle_host_heartbeat(
                lattice_id,
                &HostHeartbeat {
                    components: vec![
                        ComponentDescription::builder()
                            .id(component_1_id.into())
                            .revision(0)
                            .image_ref("ref1".into())
                            .max_instances(2)
                            .build()
                            .expect("failed to build description"),
                        ComponentDescription::builder()
                            .id(component_2_id.into())
                            .revision(0)
                            .image_ref("ref2".into())
                            .max_instances(2)
                            .build()
                            .expect("failed to build description"),
                    ],
                    friendly_name: "death-star-42".into(),
                    labels: labels.clone(),
                    issuer: "".to_string(),
                    providers: vec![
                        ProviderDescription::builder()
                            .id(&provider1.provider_id)
                            .image_ref(&provider1.image_ref)
                            .revision(0)
                            .build()
                            .expect("failed to build provider description"),
                        ProviderDescription::builder()
                            .id(&provider2.provider_id)
                            .image_ref(&provider2.image_ref)
                            .revision(0)
                            .build()
                            .expect("failed to build provider description"),
                    ],
                    uptime_human: "30s".into(),
                    uptime_seconds: 30,
                    version: semver::Version::parse("0.61.0").unwrap(),
                    host_id: host1_id.into(),
                },
            )
            .await
            .expect("Should be able to handle host heartbeat");

        worker
            .handle_host_heartbeat(
                lattice_id,
                &HostHeartbeat {
                    components: vec![
                        ComponentDescription::builder()
                            .id(component_1_id.into())
                            .image_ref("ref1".into())
                            .revision(0)
                            .max_instances(2)
                            .build()
                            .expect("failed to build description"),
                        ComponentDescription::builder()
                            .id(component_2_id.into())
                            .image_ref("ref2".into())
                            .revision(0)
                            .max_instances(2)
                            .build()
                            .expect("failed to build description"),
                    ],
                    issuer: "".to_string(),
                    friendly_name: "starkiller-base-2015".to_string(),
                    labels: labels2.clone(),
                    providers: vec![ProviderDescription::builder()
                        .id(&provider2.provider_id)
                        .image_ref(&provider2.image_ref)
                        .revision(0)
                        .build()
                        .expect("failed to build provider description")],
                    uptime_human: "30s".into(),
                    uptime_seconds: 30,
                    version: semver::Version::parse("0.61.0").unwrap(),
                    host_id: host2_id.into(),
                },
            )
            .await
            .expect("Should be able to handle host heartbeat");

        // Check that our component and provider data is still correct.
        let providers = store.list::<Provider>(lattice_id).await.unwrap();
        assert_eq!(providers.len(), 2, "Should still have 2 providers in state");
        assert_provider(&providers, &provider1, &[host1_id]);
        assert_provider(&providers, &provider2, &[host1_id, host2_id]);

        let components = store.list::<Component>(lattice_id).await.unwrap();
        assert_eq!(
            components.len(),
            2,
            "Should still have 2 components in state"
        );
        assert_component(
            &components,
            &component_1_id,
            &[(host1_id, 2), (host2_id, 2)],
        );
        assert_component(
            &components,
            &component_2_id,
            &[(host1_id, 2), (host2_id, 2)],
        );

        /***********************************************************/
        /************** Component Scale Down Tests *****************/
        /***********************************************************/

        // Stop them on one host first
        let stopped = ComponentScaled {
            claims: None,
            image_ref: "coruscant.galactic.empire/tarkin:0.1.0".into(),
            annotations: BTreeMap::default(),
            component_id: component_1_id.into(),
            host_id: host1_id.into(),
            max_instances: 0,
        };

        worker
            .handle_component_scaled(lattice_id, &stopped)
            .await
            .expect("Should be able to handle component stop event");

        let components = store.list::<Component>(lattice_id).await.unwrap();
        assert_eq!(
            components.len(),
            2,
            "Should still have 2 components in state"
        );
        assert_component(&components, &component_1_id, &[(host2_id, 2)]);
        assert_component(
            &components,
            &component_2_id,
            &[(host1_id, 2), (host2_id, 2)],
        );

        let host = store
            .get::<Host>(lattice_id, host2_id)
            .await
            .expect("Should be able to access store")
            .expect("Should have the host in the store");
        assert_eq!(*host.components.get(component_1_id).unwrap_or(&0), 2_usize);
        assert_eq!(*host.components.get(component_2_id).unwrap_or(&0), 2_usize);

        // Now stop on the other
        let stopped2 = ComponentScaled {
            host_id: host2_id.into(),
            ..stopped
        };

        worker
            .handle_component_scaled(lattice_id, &stopped2)
            .await
            .expect("Should be able to handle component scale event");

        let components = store.list::<Component>(lattice_id).await.unwrap();
        assert_eq!(components.len(), 1, "Should only have 1 component in state");
        // Double check the the old one is still ok
        assert_component(
            &components,
            &component_2_id,
            &[(host1_id, 2), (host2_id, 2)],
        );

        /***********************************************************/
        /******************* Provider Stop Tests *******************/
        /***********************************************************/

        worker
            .handle_provider_stopped(
                lattice_id,
                &ProviderStopped {
                    annotations: BTreeMap::default(),
                    provider_id: provider2.provider_id.clone(),
                    reason: String::new(),
                    host_id: host1_id.into(),
                },
            )
            .await
            .expect("Should be able to handle provider stop event");

        let providers = store.list::<Provider>(lattice_id).await.unwrap();
        assert_eq!(providers.len(), 2, "Should still have 2 providers in state");
        assert_provider(&providers, &provider1, &[host1_id]);
        assert_provider(&providers, &provider2, &[host2_id]);

        // Check that hosts got updated properly
        let hosts = store.list::<Host>(lattice_id).await.unwrap();
        assert_eq!(hosts.len(), 2, "Should only have 2 hosts");
        let host = hosts.get(host1_id).expect("Host should still exist");
        assert_eq!(host.components.len(), 1, "Should have 1 component running");
        assert_eq!(host.providers.len(), 1, "Should have 1 provider running");
        let host = hosts.get(host2_id).expect("Host should still exist");
        assert_eq!(host.components.len(), 1, "Should have 1 component running");
        assert_eq!(
            host.providers.len(),
            1,
            "Should have a single provider running"
        );

        /***********************************************************/
        /***************** Heartbeat Tests Part 2 ******************/
        /***********************************************************/

        // NOTE(brooksmtownsend): Painful manual manipulation of host inventory
        // to satisfy the way we currently query the inventory when handling heartbeats.
        *inventory.write().await = HashMap::from_iter([
            (
                host1_id.to_string(),
                HostInventory::builder()
                    .friendly_name("my-host-3".into())
                    .components(vec![ComponentDescription::builder()
                        .id(component_2_id.into())
                        .image_ref("ref2".into())
                        .revision(0)
                        .max_instances(2)
                        .build()
                        .expect("failed to build description")])
                    .host_id(host1_id.into())
                    .version(semver::Version::parse("0.61.0").unwrap().to_string())
                    .uptime_human("60s".into())
                    .uptime_seconds(60)
                    .build()
                    .expect("failed to build host inventory"),
            ),
            (
                host2_id.to_string(),
                HostInventory::builder()
                    .friendly_name("my-host-4".into())
                    .components(vec![ComponentDescription::builder()
                        .id(component_2_id.into())
                        .image_ref("ref2".into())
                        .revision(0)
                        .max_instances(2)
                        .build()
                        .expect("failed to build description")])
                    .host_id(host2_id.into())
                    .version(semver::Version::parse("1.2.3").unwrap().to_string())
                    .uptime_human("100s".into())
                    .uptime_seconds(100)
                    .build()
                    .expect("failed to build host inventory"),
            ),
        ]);

        // Heartbeat the first host and make sure nothing has changed
        worker
            .handle_host_heartbeat(
                lattice_id,
                &HostHeartbeat {
                    components: vec![ComponentDescription::builder()
                        .id(component_2_id.into())
                        .image_ref("ref2".into())
                        .revision(0)
                        .max_instances(2)
                        .build()
                        .expect("failed to build description")],
                    friendly_name: "death-star-42".to_string(),
                    issuer: "".to_string(),
                    labels,
                    providers: vec![ProviderDescription::builder()
                        .id(&provider1.provider_id)
                        .image_ref(&provider1.image_ref)
                        .revision(1)
                        .build()
                        .expect("failed to build provider description")],
                    uptime_human: "60s".into(),
                    uptime_seconds: 60,
                    version: semver::Version::parse("0.61.0").unwrap(),
                    host_id: host1_id.into(),
                },
            )
            .await
            .expect("Should be able to handle host heartbeat");

        worker
            .handle_host_heartbeat(
                lattice_id,
                &HostHeartbeat {
                    components: vec![ComponentDescription::builder()
                        .id(component_2_id.into())
                        .image_ref("ref2".into())
                        .revision(0)
                        .max_instances(2)
                        .build()
                        .expect("failed to build description")],
                    friendly_name: "starkiller-base-2015".to_string(),
                    labels: labels2,
                    issuer: "".to_string(),
                    providers: vec![ProviderDescription::builder()
                        .id(&provider2.provider_id)
                        .image_ref(&provider2.image_ref)
                        .revision(0)
                        .build()
                        .expect("failed to build provider description")],
                    uptime_human: "60s".into(),
                    uptime_seconds: 60,
                    version: semver::Version::parse("0.61.0").unwrap(),
                    host_id: host2_id.into(),
                },
            )
            .await
            .expect("Should be able to handle host heartbeat");

        // Check that the heartbeat kept state consistent
        let hosts = store.list::<Host>(lattice_id).await.unwrap();
        assert_eq!(hosts.len(), 2, "Should only have 2 hosts");
        let host = hosts.get(host1_id).expect("Host should still exist");
        assert_eq!(host.components.len(), 1, "Should have 1 component running");
        assert_eq!(host.providers.len(), 1, "Should have 1 provider running");
        let host = hosts.get(host2_id).expect("Host should still exist");
        assert_eq!(host.components.len(), 1, "Should have 1 component running");
        assert_eq!(
            host.providers.len(),
            1,
            "Should have a single provider running"
        );

        // Double check providers and components are the same
        let components = store.list::<Component>(lattice_id).await.unwrap();
        assert_eq!(components.len(), 1, "Should only have 1 component in state");
        assert_component(
            &components,
            &component_2_id,
            &[(host1_id, 2), (host2_id, 2)],
        );

        let providers = store.list::<Provider>(lattice_id).await.unwrap();
        assert_eq!(providers.len(), 2, "Should still have 2 providers in state");
        assert_provider(&providers, &provider1, &[host1_id]);
        assert_provider(&providers, &provider2, &[host2_id]);

        /***********************************************************/
        /********************* Host Stop Tests *********************/
        /***********************************************************/

        worker
            .handle_host_stopped(
                lattice_id,
                &HostStopped {
                    labels: HashMap::default(),
                    id: host1_id.into(),
                },
            )
            .await
            .expect("Should be able to handle host stopped event");

        let hosts = store.list::<Host>(lattice_id).await.unwrap();
        assert_eq!(hosts.len(), 1, "Should only have 1 host");
        let host = hosts.get(host2_id).expect("Host should still exist");
        assert_eq!(host.components.len(), 1, "Should have 1 component running");
        assert_eq!(
            host.providers.len(),
            1,
            "Should have a single provider running"
        );

        // Double check providers and components are the same
        let components = store.list::<Component>(lattice_id).await.unwrap();
        assert_eq!(components.len(), 1, "Should only have 1 component in state");
        assert_component(&components, &component_2_id, &[(host2_id, 2)]);

        let providers = store.list::<Provider>(lattice_id).await.unwrap();
        assert_eq!(providers.len(), 1, "Should now have 1 provider in state");
        assert_provider(&providers, &provider2, &[host2_id]);
    }

    #[tokio::test]
    async fn test_discover_running_host() {
        let component1_id = "SKYWALKER";
        let component1_ref = "fakecloud.io/skywalker:0.1.0";
        let component2_id = "ORGANA";
        let component2_ref = "fakecloud.io/organa:0.1.0";
        let lattice_id = "discover_running_host";
        let claims = HashMap::from([
            (
                component1_id.into(),
                Claims {
                    name: "tosche_station".to_string(),
                    capabilities: vec!["wasmcloud:httpserver".to_string()],
                    issuer: "GEORGELUCAS".to_string(),
                },
            ),
            (
                component2_id.into(),
                Claims {
                    name: "alderaan".to_string(),
                    capabilities: vec!["wasmcloud:keyvalue".to_string()],
                    issuer: "GEORGELUCAS".to_string(),
                },
            ),
        ]);
        let store = Arc::new(TestStore::default());
        let inventory = Arc::new(RwLock::new(HashMap::default()));
        let lattice_source = TestLatticeSource {
            claims: claims.clone(),
            inventory: inventory.clone(),
            ..Default::default()
        };
        let command_publisher = CommandPublisher::new(NoopPublisher, "doesntmatter");
        let status_publisher = StatusPublisher::new(NoopPublisher, None, "doesntmatter");
        let worker = EventWorker::new(
            store.clone(),
            lattice_source.clone(),
            command_publisher.clone(),
            status_publisher.clone(),
            ScalerManager::test_new(
                NoopPublisher,
                lattice_id,
                store.clone(),
                command_publisher,
                status_publisher.clone(),
                lattice_source,
            )
            .await,
        );

        let provider_id = "HYPERDRIVE";
        let host_id = "WHATAPIECEOFJUNK";
        // NOTE(brooksmtownsend): Painful manual manipulation of host inventory
        // to satisfy the way we currently query the inventory when handling heartbeats.
        *inventory.write().await = HashMap::from_iter([(
            host_id.to_string(),
            HostInventory::builder()
                .friendly_name("my-host-5".into())
                .components(vec![
                    ComponentDescription::builder()
                        .id(component1_id.into())
                        .image_ref(component1_ref.into())
                        .revision(0)
                        .max_instances(2)
                        .build()
                        .expect("failed to build description"),
                    ComponentDescription::builder()
                        .id(component2_id.into())
                        .image_ref(component2_ref.into())
                        .revision(0)
                        .max_instances(1)
                        .build()
                        .expect("failed to build description"),
                ])
                .host_id(host_id.into())
                .providers(vec![ProviderDescription::builder()
                    .id(&provider_id)
                    .revision(0)
                    .build()
                    .expect("failed to build provider description")])
                .version(semver::Version::parse("0.61.0").unwrap().to_string())
                .uptime_human("60s".into())
                .uptime_seconds(60)
                .build()
                .expect("failed to build host inventory"),
        )]);

        // Heartbeat with components and providers that don't exist in the store yet
        worker
            .handle_host_heartbeat(
                lattice_id,
                &HostHeartbeat {
                    components: vec![
                        ComponentDescription::builder()
                            .id(component1_id.into())
                            .image_ref(component1_ref.into())
                            .revision(0)
                            .max_instances(2)
                            .build()
                            .expect("failed to build description"),
                        ComponentDescription::builder()
                            .id(component2_id.into())
                            .image_ref(component2_ref.into())
                            .revision(0)
                            .max_instances(1)
                            .build()
                            .expect("failed to build description"),
                    ],
                    friendly_name: "millenium_falcon-1977".to_string(),
                    labels: HashMap::default(),
                    issuer: "".to_string(),
                    providers: vec![ProviderDescription::builder()
                        .id(&provider_id)
                        .revision(0)
                        .build()
                        .expect("failed to build provider description")],
                    uptime_human: "60s".into(),
                    uptime_seconds: 60,
                    version: semver::Version::parse("0.61.0").unwrap(),
                    host_id: host_id.into(),
                },
            )
            .await
            .expect("Should be able to handle host heartbeat");

        // We test that the host is created in other tests, so just check that the components and
        // providers were created properly
        let components = store.list::<Component>(lattice_id).await.unwrap();
        assert_eq!(components.len(), 2, "Store should now have two components");
        let component = components
            .get(component1_id)
            .expect("component should exist");
        let expected = claims.get(component1_id).unwrap();
        assert_eq!(component.name, expected.name, "Data should match");
        assert_eq!(component.issuer, expected.issuer, "Data should match");
        assert_eq!(
            component
                .instances
                .get(host_id)
                .expect("Host should exist in count")
                .get(&BTreeMap::new())
                .expect("Should find a component with the correct annotations")
                .count,
            2,
            "Should have the right number of components running"
        );

        let component = components
            .get(component2_id)
            .expect("Component should exist");
        let expected = claims.get(component2_id).unwrap();
        assert_eq!(component.name, expected.name, "Data should match");
        assert_eq!(component.issuer, expected.issuer, "Data should match");
        assert_eq!(
            component
                .instances
                .get(host_id)
                .expect("Host should exist in count")
                .len(),
            1,
            "Should have the right number of components running"
        );

        let providers = store.list::<Provider>(lattice_id).await.unwrap();
        assert_eq!(providers.len(), 1, "Should have 1 provider in the store");
        let provider = providers.get(provider_id).expect("Provider should exist");
        assert_eq!(provider.id, provider_id, "Data should match");
        assert!(
            provider.hosts.contains_key(host_id),
            "Should have found host in provider store"
        );
    }

    #[tokio::test]
    async fn test_provider_status_update() {
        let store = Arc::new(TestStore::default());
        let lattice_source = TestLatticeSource::default();
        let lattice_id = "provider_status";
        let command_publisher = CommandPublisher::new(NoopPublisher, "doesntmatter");
        let status_publisher = StatusPublisher::new(NoopPublisher, None, "doesntmatter");
        let worker = EventWorker::new(
            store.clone(),
            lattice_source.clone(),
            command_publisher.clone(),
            status_publisher.clone(),
            ScalerManager::test_new(
                NoopPublisher,
                lattice_id,
                store.clone(),
                command_publisher,
                status_publisher.clone(),
                lattice_source,
            )
            .await,
        );

        let host_id = "CLOUDCITY";

        // Trigger a provider started and then a health check
        let provider = ProviderStarted {
            claims: Some(ProviderClaims {
                issuer: "Lando Calrissian".into(),
                name: "Tibanna Gas Mining".into(),
                version: "0.1.0".into(),
                ..Default::default()
            }),
            image_ref: "bespin.lando.inc/tibanna:0.1.0".into(),
            provider_id: "GAS".into(),
            host_id: host_id.into(),
            annotations: BTreeMap::default(),
        };

        worker
            .handle_provider_started(lattice_id, &provider)
            .await
            .expect("Should be able to handle provider started event");
        worker
            .handle_provider_health_check(
                lattice_id,
                &ProviderHealthCheckInfo {
                    provider_id: provider.provider_id.clone(),
                    host_id: host_id.into(),
                },
                Some(false),
            )
            .await
            .expect("Should be able to handle a provider health check event");

        let providers = store.list::<Provider>(lattice_id).await.unwrap();
        assert_eq!(providers.len(), 1, "Only 1 provider should exist");
        let prov = providers
            .get(&provider.provider_id)
            .expect("Provider should exist");
        assert!(
            matches!(
                prov.hosts
                    .get(host_id)
                    .expect("Should find status for host"),
                ProviderStatus::Running
            ),
            "Provider should have a running status"
        );

        // Now try a failed status
        worker
            .handle_provider_health_check(
                lattice_id,
                &ProviderHealthCheckInfo {
                    provider_id: provider.provider_id.clone(),
                    host_id: host_id.into(),
                },
                Some(true),
            )
            .await
            .expect("Should be able to handle a provider health check event");

        let providers = store.list::<Provider>(lattice_id).await.unwrap();
        assert_eq!(providers.len(), 1, "Only 1 provider should exist");
        let prov = providers
            .get(&provider.provider_id)
            .expect("Provider should exist");
        assert!(
            matches!(
                prov.hosts
                    .get(host_id)
                    .expect("Should find status for host"),
                ProviderStatus::Failed
            ),
            "Provider should have a running status"
        );
    }

    #[tokio::test]
    async fn test_heartbeat_updates_stale_data() {
        let store = Arc::new(TestStore::default());
        let inventory = Arc::new(RwLock::new(HashMap::default()));
        let lattice_source = TestLatticeSource {
            inventory: inventory.clone(),
            ..Default::default()
        };
        let lattice_id = "update_data";

        let command_publisher = CommandPublisher::new(NoopPublisher, "doesntmatter");
        let status_publisher = StatusPublisher::new(NoopPublisher, None, "doesntmatter");
        let worker = EventWorker::new(
            store.clone(),
            lattice_source.clone(),
            command_publisher.clone(),
            status_publisher.clone(),
            ScalerManager::test_new(
                NoopPublisher,
                lattice_id,
                store.clone(),
                command_publisher,
                status_publisher.clone(),
                lattice_source,
            )
            .await,
        );

        let host_id = "jabbaspalace";

        // Store existing components and providers as-if they had the minimum
        // amount of information
        store
            .store(
                lattice_id,
                "jabba".to_string(),
                Component {
                    id: "jabba".to_string(),
                    instances: HashMap::from([(
                        host_id.to_string(),
                        HashSet::from_iter([WadmComponentInfo {
                            count: 1,
                            annotations: BTreeMap::default(),
                        }]),
                    )]),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        store
            .store(
                lattice_id,
                "jabbatheprovider".to_string(),
                Provider {
                    id: "jabbatheprovider".to_string(),
                    hosts: HashMap::from_iter([(host_id.to_string(), ProviderStatus::Pending)]),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        // Now heartbeat and make sure stuff that isn't running is removed
        worker
            .handle_host_heartbeat(
                lattice_id,
                &HostHeartbeat {
                    components: vec![
                        ComponentDescription::builder()
                            .id("jabba".into())
                            .image_ref("jabba.tatooinecr.io/jabba:latest".into())
                            .name("Da Hutt".into())
                            .annotations(BTreeMap::from_iter([(
                                "da".to_string(),
                                "gobah".to_string(),
                            )]))
                            .revision(0)
                            .max_instances(5)
                            .build()
                            .expect("failed to build description"),
                        ComponentDescription::builder()
                            .id("jabba2".into())
                            .image_ref("jabba.tatooinecr.io/jabba:latest".into())
                            .name("Da Hutt".into())
                            .revision(0)
                            .max_instances(1)
                            .build()
                            .expect("failed to build description"),
                    ],
                    friendly_name: "palace-1983".to_string(),
                    labels: HashMap::default(),
                    issuer: "".to_string(),
                    providers: vec![ProviderDescription::builder()
                        .annotations(BTreeMap::from_iter([(
                            "one".to_string(),
                            "two".to_string(),
                        )]))
                        .id("jabbatheprovider")
                        .image_ref("jabba.tatooinecr.io/provider:latest")
                        .name("Jabba The Provider")
                        .revision(0)
                        .build()
                        .expect("failed to build provider description")],
                    uptime_human: "60s".into(),
                    uptime_seconds: 60,
                    version: semver::Version::parse("0.61.0").unwrap(),
                    host_id: host_id.to_string(),
                },
            )
            .await
            .expect("Should be able to handle host heartbeat");

        let components = store.list::<Component>(lattice_id).await.unwrap();
        assert_eq!(components.len(), 2, "Should have 2 components in the store");
        let component = components.get("jabba").expect("Component should exist");
        assert_eq!(
            component.count(),
            5,
            "Component should have the correct number of instances"
        );
        assert_eq!(component.name, "Da Hutt", "Should have the correct name");
        assert_eq!(
            component.reference, "jabba.tatooinecr.io/jabba:latest",
            "Should have the correct reference"
        );
        assert_eq!(
            component
                .instances
                .get(host_id)
                .expect("instances to be on our specified host")
                .iter()
                .find(|i| !i.annotations.is_empty())
                .expect("instance with annotations to exist")
                .annotations
                .get("da")
                .expect("annotation to exist"),
            "gobah"
        );

        let providers = store
            .list::<Provider>(lattice_id)
            .await
            .expect("should be able to grab providers from store");
        assert_eq!(providers.len(), 1, "Should have 1 provider in the store");
        let provider = providers
            .get("jabbatheprovider")
            .expect("Provider should exist");
        assert_eq!(
            provider.name, "Jabba The Provider",
            "Should have the correct name"
        );
        assert_eq!(
            provider.reference, "jabba.tatooinecr.io/provider:latest",
            "Should have the correct reference"
        );
        let hosts = store
            .list::<Host>(lattice_id)
            .await
            .expect("should be able to get hosts from store");
        assert_eq!(hosts.len(), 1);
        let host = hosts.get(host_id).expect("host with generated ID to exist");
        let host_provider = host
            .providers
            .get(&ProviderInfo {
                provider_id: "jabbatheprovider".to_string(),
                provider_ref: "jabba.tatooinecr.io/provider:latest".to_string(),
                annotations: BTreeMap::default(),
            })
            .expect("provider to exist on host");

        assert_eq!(
            host_provider
                .annotations
                .get("one")
                .expect("annotation to exist"),
            "two"
        );
    }

    fn assert_component(
        components: &HashMap<String, Component>,
        component_id: &str,
        expected_counts: &[(&str, usize)],
    ) {
        let component = components
            .get(component_id)
            .expect("Component should exist in store");
        assert_eq!(
            component.id, component_id,
            "Component ID stored should be correct"
        );
        assert_eq!(
            expected_counts.len(),
            component.instances.len(),
            "Should have the proper number of hosts the component is running on"
        );
        for (expected_host, expected_count) in expected_counts.iter() {
            assert_eq!(
                component.count_for_host(expected_host),
                *expected_count,
                "Component count on host should be correct"
            );
        }
    }

    fn assert_provider(
        providers: &HashMap<String, Provider>,
        event: &ProviderStarted,
        running_on_hosts: &[&str],
    ) {
        let provider = providers
            .get(&event.provider_id)
            .expect("Correct provider should exist in store");
        assert!(
            event
                .claims
                .clone()
                .is_some_and(|claims| claims.name == provider.name),
            "Provider should have the correct data in state"
        );
        assert!(
            provider.hosts.len() == running_on_hosts.len()
                && running_on_hosts
                    .iter()
                    .all(|host_id| provider.hosts.contains_key(*host_id)),
            "Provider should be set to the correct hosts"
        );
    }
}
