use std::collections::{hash_map::Entry, HashMap, HashSet};

use tracing::{debug, instrument, trace};
use wasmcloud_control_interface::Client;

use crate::consumers::{
    manager::{WorkError, WorkResult, Worker},
    ScopedMessage,
};
use crate::events::*;
use crate::storage::{Actor, Host, Provider, ProviderStatus, Store};

pub struct EventWorker<S> {
    store: S,
    ctl_client: Client,
}

impl<S: Clone> Clone for EventWorker<S> {
    fn clone(&self) -> Self {
        EventWorker {
            store: self.store.clone(),
            ctl_client: self.ctl_client.clone(),
        }
    }
}

impl<S: Store + Send + Sync> EventWorker<S> {
    /// Creates a new event worker configured to use the given store and control interface client for fetching state
    pub fn new(store: S, ctl_client: Client) -> EventWorker<S> {
        EventWorker { store, ctl_client }
    }

    // BEGIN HANDLERS
    // NOTE(thomastaylor312): These use anyhow errors because in the _single_ case where we have to
    // call the lattice controller, we no longer just have error types from the store. To handle the
    // multiple error cases, it was just easier to catch it into an anyhow Error and then convert at
    // the end

    // TODO(thomastaylor312): Initially I thought we'd have to update the host state as well with
    // the new provider/actor. However, do we actually need to update the host info or just let the
    // heartbeat take care of it? I think we might be ok because the actor/provider data should have
    // all the info needed to make a decision for scaling in conjunction with the basic host info
    // (like labels). We might have to revisit this when a) we implement the `Scaler` trait or b) if
    // we start serving up lattice state to consumers of wadm (in which case we'd want state changes
    // reflected immediately)

    #[instrument(level = "debug", skip(self, actor), fields(actor_id = %actor.public_key, host_id = %actor.host_id))]
    async fn handle_actor_started(
        &self,
        lattice_id: &str,
        actor: &ActorStarted,
    ) -> anyhow::Result<()> {
        trace!("Adding newly started actor to store");
        debug!("Fetching current data for actor");
        // Because we could have created an actor from the host heartbeat, we just overwrite
        // everything except counts here
        let mut actor_data = Actor::from(actor);
        if let Some(current) = self
            .store
            .get::<Actor>(lattice_id, &actor.public_key)
            .await?
        {
            trace!(actor = ?current, "Found existing actor data");
            // Merge in current counts
            actor_data.count = current.count;
        }

        // Update count of the data
        actor_data
            .count
            .entry(actor.host_id.clone())
            .and_modify(|val| *val += 1)
            .or_insert(1);

        self.store
            .store(lattice_id, actor.public_key.clone(), actor_data)
            .await
            .map_err(anyhow::Error::from)
    }

    #[instrument(level = "debug", skip(self, actor), fields(actor_id = %actor.public_key, host_id = %actor.host_id))]
    async fn handle_actor_stopped(
        &self,
        lattice_id: &str,
        actor: &ActorStopped,
    ) -> anyhow::Result<()> {
        trace!("Removing stopped actor from store");
        debug!("Fetching current data for actor");
        if let Some(mut current) = self
            .store
            .get::<Actor>(lattice_id, &actor.public_key)
            .await?
        {
            trace!(actor = ?current, "Found existing actor data");
            if let Some(current_count) = current.count.get(&actor.host_id) {
                let new_count = current_count - 1;
                if new_count == 0 {
                    trace!(host_id = %actor.host_id, "Stopped last actor on host, removing host entry from actor");
                    current.count.remove(&actor.host_id);
                } else {
                    trace!(count = %new_count, host_id = %actor.host_id, "Setting current actor");
                    current.count.insert(actor.host_id.clone(), new_count);
                }
            }

            if current.count.is_empty() {
                debug!("Last actor instance was removed, removing actor from storage");
                self.store
                    .delete::<Actor>(lattice_id, &actor.public_key)
                    .await
            } else {
                self.store
                    .store(lattice_id, actor.public_key.clone(), current)
                    .await
            }?;
        }
        Ok(())
    }

    #[instrument(level = "debug", skip(self, host), fields(host_id = %host.id))]
    async fn handle_host_heartbeat(
        &self,
        lattice_id: &str,
        host: &HostHeartbeat,
    ) -> anyhow::Result<()> {
        debug!("Updating store with current host heartbeat information");
        // Host updates just overwrite current information, so no need to fetch
        let host_data = Host::from(host);
        self.store
            .store(lattice_id, host.id.clone(), host_data)
            .await?;

        // NOTE: We can return an error here and then nack because we'll just reupdate the host data
        // with the exact same host heartbeat entry. There is no possibility of a duplicate
        self.heartbeat_provider_update(lattice_id, host).await?;

        // NOTE: We can return an error here and then nack because we'll just reupdate the host data
        // with the exact same host heartbeat entry. There is no possibility of a duplicate
        self.heartbeat_actor_update(lattice_id, host).await?;

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
        // already sent a bunch of stop actor/provider events, but for correctness sake, we fetch
        // the current host and make sure all the actors and providers are removed
        trace!("Fetching current host data");
        let current: Host = match self.store.get(lattice_id, &host.id).await? {
            Some(h) => h,
            None => {
                debug!("Got host stopped event for a host we didn't have in the store");
                return Ok(());
            }
        };

        trace!("Fetching actors from store to remove stopped instances");
        let all_actors = self.store.list::<Actor>(lattice_id).await?;

        let actors_to_update = all_actors.into_iter().filter_map(|(id, mut actor)| {
            if current.actors.contains_key(&id) {
                actor.count.remove(&current.id);
                Some((id, actor))
            } else {
                None
            }
        });
        trace!("Storing updated actors in store");
        self.store.store_many(lattice_id, actors_to_update).await?;

        trace!("Fetching providers from store to remove stopped instances");
        let mut all_providers = self.store.list::<Provider>(lattice_id).await?;

        let providers_to_update = current.providers.into_iter().filter_map(|info| {
            let key = crate::storage::provider_id(&info.public_key, &info.link_name);
            let mut prov = all_providers.remove(&key)?;
            prov.hosts.remove(&host.id).map(|_| (key, prov))
        });
        trace!("Storing updated providers in store");
        self.store
            .store_many(lattice_id, providers_to_update)
            .await?;

        // Order matters here: Now that we've cleaned stuff up, remove the host. We do this last
        // because if any of the above fails after we remove the host, we won't be able to fetch the
        // data to remove the actors and providers on a retry.
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
            public_key = %provider.public_key,
            link_name = %provider.link_name,
            contract_id = %provider.contract_id
        )
    )]
    async fn handle_provider_started(
        &self,
        lattice_id: &str,
        provider: &ProviderStarted,
    ) -> anyhow::Result<()> {
        debug!("Handling provider started event");
        let id = crate::storage::provider_id(&provider.public_key, &provider.link_name);
        trace!("Fetching current data from store");
        let provider_data = if let Some(mut current) =
            self.store.get::<Provider>(lattice_id, &id).await?
        {
            // Using the entry api is a bit more efficient because we do a single key lookup
            match current.hosts.entry(provider.host_id.clone()) {
                Entry::Occupied(_) => {
                    trace!("Found host entry for the provider already in store. Returning early");
                    return Ok(());
                }
                Entry::Vacant(entry) => {
                    entry.insert(ProviderStatus::default());
                    current
                }
            }
        } else {
            trace!("No current provider found in store");
            Provider::from(provider)
        };
        debug!("Storing updated provider in store");
        self.store
            .store(lattice_id, id, provider_data)
            .await
            .map_err(anyhow::Error::from)
    }

    #[instrument(
        level = "debug",
        skip(self, provider),
        fields(
            public_key = %provider.public_key,
            link_name = %provider.link_name,
            contract_id = %provider.contract_id
        )
    )]
    async fn handle_provider_stopped(
        &self,
        lattice_id: &str,
        provider: &ProviderStopped,
    ) -> anyhow::Result<()> {
        debug!("Handling provider stopped event");
        let id = crate::storage::provider_id(&provider.public_key, &provider.link_name);
        trace!("Fetching current data from store");
        if let Some(mut current) = self.store.get::<Provider>(lattice_id, &id).await? {
            if current.hosts.remove(&provider.host_id).is_none() {
                trace!(host_id = %provider.host_id, "Did not find host entry in provider");
                return Ok(());
            }
            if current.hosts.is_empty() {
                debug!("Provider is no longer running on any hosts. Removing from store");
                self.store
                    .delete::<Provider>(lattice_id, &id)
                    .await
                    .map_err(anyhow::Error::from)
            } else {
                debug!("Storing updated provider");
                self.store
                    .store(lattice_id, id, current)
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
            public_key = %provider.public_key,
            link_name = %provider.link_name,
        )
    )]
    async fn handle_provider_health_check(
        &self,
        lattice_id: &str,
        host_id: &str,
        provider: &ProviderHealthCheckInfo,
        failed: bool,
    ) -> anyhow::Result<()> {
        debug!("Handling provider health check event");
        trace!("Getting current provider");
        let id = crate::storage::provider_id(&provider.public_key, &provider.link_name);
        let mut current: Provider = match self.store.get(lattice_id, &id).await? {
            Some(p) => p,
            None => {
                trace!("Didn't find provider in store. Creating");
                Provider {
                    id: provider.public_key.clone(),
                    link_name: provider.link_name.clone(),
                    ..Default::default()
                }
            }
        };
        debug!("Updating store with current status");
        let status = if failed {
            ProviderStatus::Failed
        } else {
            ProviderStatus::Running
        };
        current.hosts.insert(host_id.to_owned(), status);
        self.store
            .store(lattice_id, id, current)
            .await
            .map_err(anyhow::Error::from)
    }

    // END HANDLER FUNCTIONS
    async fn populate_actor_info(
        &self,
        mut actor_ids: HashSet<String>,
        host_id: &str,
        count_map: &HashMap<String, usize>,
    ) -> anyhow::Result<Vec<(String, Actor)>> {
        let resp = self
            .ctl_client
            .get_claims()
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?;
        Ok(resp
            .claims
            .into_iter()
            .filter_map(|mut claim| {
                let sub = claim.get("sub").map(|s| s.as_str()).unwrap_or_default();

                actor_ids.take(sub).map(|id| {
                    (
                        id.clone(),
                        Actor {
                            id: id.clone(),
                            name: claim.remove("name").unwrap_or_default(),
                            capabilities: claim
                                .remove("caps")
                                .map(|raw| raw.split(',').map(|s| s.to_owned()).collect())
                                .unwrap_or_default(),
                            issuer: claim.remove("iss").unwrap_or_default(),
                            count: [(
                                host_id.to_owned(),
                                count_map.get(&id).copied().unwrap_or_default(),
                            )]
                            .into(),
                            ..Default::default()
                        },
                    )
                })
            })
            .collect())
    }

    #[instrument(level = "debug", skip(self, host), fields(host_id = %host.id))]
    async fn heartbeat_actor_update(
        &self,
        lattice_id: &str,
        host: &HostHeartbeat,
    ) -> anyhow::Result<()> {
        debug!("Fetching current actor state");
        let mut actors = self.store.list::<Actor>(lattice_id).await?;

        let missing_actors: HashSet<String> = host
            .actors
            .iter()
            .filter_map(|(id, _)| (!actors.contains_key(id)).then_some(id))
            .cloned()
            .collect();

        // TODO: FIXME

        let actors_to_update = host.actors.iter().filter_map(|(id, count)| {
            match actors.remove(id) {
                Some(mut current) => {
                    // An actor count should never be 0, so defaulting is fine. If we didn't modify the count, skip
                    if current
                        .count
                        .insert(host.id.clone(), *count)
                        .unwrap_or_default()
                        == *count
                    {
                        None
                    } else {
                        Some((id.to_owned(), current))
                    }
                }
                None => None,
            }
        });

        let actors_to_update: Vec<(String, Actor)> = if !missing_actors.is_empty() {
            trace!(num_actors = %missing_actors.len(), "Fetching claims for missing actors");
            actors_to_update
                .chain(
                    self.populate_actor_info(missing_actors, &host.id, &host.actors)
                        .await?,
                )
                .collect()
        } else {
            actors_to_update.collect()
        };

        trace!("Updating actors with new status from host");

        self.store.store_many(lattice_id, actors_to_update).await?;

        Ok(())
    }

    #[instrument(level = "debug", skip(self, host), fields(host_id = %host.id))]
    async fn heartbeat_provider_update(
        &self,
        lattice_id: &str,
        host: &HostHeartbeat,
    ) -> anyhow::Result<()> {
        debug!("Fetching current provider state");
        let mut providers = self.store.list::<Provider>(lattice_id).await?;
        let providers_to_update = host.providers.iter().filter_map(|info| {
            let provider_id = crate::storage::provider_id(&info.public_key, &info.link_name);
            match providers.remove(&provider_id) {
                Some(mut prov) => {
                    let mut has_changes = false;
                    // A health check from a provider we hadn't registered doesn't have a link name,
                    // so check if that needs to be set
                    if prov.link_name.is_empty() {
                        prov.link_name = info.link_name.clone();
                        has_changes = true;
                    }
                    if let Entry::Vacant(entry) = prov.hosts.entry(host.id.clone()) {
                        entry.insert(ProviderStatus::default());
                        has_changes = true;
                    }
                    if has_changes {
                        Some((provider_id, prov))
                    } else {
                        None
                    }
                }
                None => {
                    // If we don't already have the provider, create a basic one so we know it
                    // exists at least. The next provider heartbeat will fix it for us
                    Some((
                        provider_id,
                        Provider {
                            id: info.public_key.clone(),
                            contract_id: info.contract_id.clone(),
                            link_name: info.link_name.clone(),
                            hosts: [(host.id.clone(), ProviderStatus::default())].into(),
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
}

#[async_trait::async_trait]
impl<S: Store + Send + Sync> Worker for EventWorker<S> {
    type Message = Event;

    #[instrument(level = "debug", skip(self))]
    async fn do_work(&self, mut message: ScopedMessage<Self::Message>) -> WorkResult<()> {
        let res = match message.as_ref() {
            Event::ActorStarted(actor) => {
                self.handle_actor_started(&message.lattice_id, actor).await
            }
            Event::ActorStopped(actor) => {
                self.handle_actor_stopped(&message.lattice_id, actor).await
            }
            Event::HostHeartbeat(host) => {
                self.handle_host_heartbeat(&message.lattice_id, host).await
            }
            Event::HostStarted(host) => self.handle_host_started(&message.lattice_id, host).await,
            Event::HostStopped(host) => self.handle_host_stopped(&message.lattice_id, host).await,
            Event::LinkdefDeleted(_ld) => {
                // TODO: This will need to be handled when we start managing applications
                Ok(())
            }
            Event::ProviderStarted(provider) => {
                self.handle_provider_started(&message.lattice_id, provider)
                    .await
            }
            Event::ProviderStopped(provider) => {
                self.handle_provider_stopped(&message.lattice_id, provider)
                    .await
            }
            Event::ProviderHealthCheckPassed(ProviderHealthCheckPassed { data, host_id }) => {
                self.handle_provider_health_check(&message.lattice_id, host_id, data, false)
                    .await
            }
            Event::ProviderHealthCheckFailed(ProviderHealthCheckFailed { data, host_id }) => {
                self.handle_provider_health_check(&message.lattice_id, host_id, data, true)
                    .await
            }
            // All other events we don't care about for state.
            _ => {
                trace!("Got event we don't care about. Skipping");
                Ok(())
            }
        }
        .map_err(Box::<dyn std::error::Error + Send + 'static>::from);

        // NOTE: This is where the call to scaler types will go
        if let Err(e) = res {
            message.nack().await;
            return Err(WorkError::Other(e));
        }
        message.ack().await.map_err(WorkError::from)
    }
}
