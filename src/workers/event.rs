use std::collections::{hash_map::Entry, HashMap, HashSet};

use tracing::{debug, instrument, trace, warn};

use crate::consumers::{
    manager::{WorkError, WorkResult, Worker},
    ScopedMessage,
};
use crate::events::*;
use crate::storage::{Actor, Host, Provider, ProviderStatus, Store};

/// A subset of needed claims to help populate state
#[derive(Debug, Clone)]
pub struct Claims {
    pub name: String,
    pub capabilities: Vec<String>,
    pub issuer: String,
}

/// A trait for anything that can fetch a set of claims information about actors.
///
/// NOTE: This trait right now exists as a convenience for two things: First, testing. Without
/// something like this we require a network connection to unit test. Second, there is no concrete
/// claims type returned from the control interface client. This allows us to abstract that away
/// until such time that we do export one and we'll be able to do so without breaking our API
#[async_trait::async_trait]
pub trait ClaimsSource {
    async fn get_claims(&self) -> anyhow::Result<HashMap<String, Claims>>;
}

#[async_trait::async_trait]
impl ClaimsSource for wasmcloud_control_interface::Client {
    async fn get_claims(&self) -> anyhow::Result<HashMap<String, Claims>> {
        Ok(self
            .get_claims()
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .claims
            .into_iter()
            .filter_map(|mut claim| {
                // NOTE(thomastaylor312): I'm removing instead of getting since we own the data and I
                // don't want to clone every time we do this

                // If we don't find a subject, we can't actually get the actor ID, so skip this one
                Some((
                    claim.remove("sub")?,
                    Claims {
                        name: claim.remove("name").unwrap_or_default(),
                        capabilities: claim
                            .remove("caps")
                            .map(|raw| raw.split(',').map(|s| s.to_owned()).collect())
                            .unwrap_or_default(),
                        issuer: claim.remove("iss").unwrap_or_default(),
                    },
                ))
            })
            .collect())
    }
}

pub struct EventWorker<S, C> {
    store: S,
    ctl_client: C,
}

impl<S: Clone, C: Clone> Clone for EventWorker<S, C> {
    fn clone(&self) -> Self {
        EventWorker {
            store: self.store.clone(),
            ctl_client: self.ctl_client.clone(),
        }
    }
}

impl<S, C> EventWorker<S, C>
where
    S: Store + Send + Sync,
    C: ClaimsSource + Send + Sync,
{
    /// Creates a new event worker configured to use the given store and control interface client for fetching state
    pub fn new(store: S, ctl_client: C) -> EventWorker<S, C> {
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
                trace!("Last actor instance was removed, removing actor from storage");
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

        #[allow(clippy::type_complexity)]
        let (actors_to_update, actors_to_delete): (
            Vec<(String, Actor)>,
            Vec<(String, Actor)>,
        ) = all_actors
            .into_iter()
            .filter_map(|(id, mut actor)| {
                if current.actors.contains_key(&id) {
                    actor.count.remove(&current.id);
                    Some((id, actor))
                } else {
                    None
                }
            })
            .partition(|(_, actor)| !actor.count.is_empty());
        trace!("Storing updated actors in store");
        self.store.store_many(lattice_id, actors_to_update).await?;

        trace!("Removing actors with no more running instances");
        self.store
            .delete_many::<Actor, _, _>(lattice_id, actors_to_delete.into_iter().map(|(id, _)| id))
            .await?;

        trace!("Fetching providers from store to remove stopped instances");
        let all_providers = self.store.list::<Provider>(lattice_id).await?;

        #[allow(clippy::type_complexity)]
        let (providers_to_update, providers_to_delete): (Vec<(String, Provider)>, Vec<(String, Provider)>) = current.providers.into_iter().filter_map(|info| {
            let key = crate::storage::provider_id(&info.public_key, &info.link_name);
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
        }).partition(|(_, provider)| !provider.hosts.is_empty());
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
            let mut prov = Provider::from(provider);
            prov.hosts = HashMap::from([(provider.host_id.clone(), ProviderStatus::default())]);
            prov
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
        let claims = self.ctl_client.get_claims().await?;
        Ok(claims
            .into_iter()
            .filter_map(|(id, claim)| {
                actor_ids.take(&id).map(|id| {
                    (
                        id.clone(),
                        Actor {
                            id: id.clone(),
                            name: claim.name,
                            capabilities: claim.capabilities,
                            issuer: claim.issuer,
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
        let providers = self.store.list::<Provider>(lattice_id).await?;
        let providers_to_update = host.providers.iter().filter_map(|info| {
            let provider_id = crate::storage::provider_id(&info.public_key, &info.link_name);
            // NOTE: We can do this without cloning, but it led to some confusing code involving
            // `remove` from the owned `providers` map. This is more readable at the expense of
            // a clone for few providers
            match providers.get(&provider_id).cloned() {
                Some(mut prov) => {
                    let mut has_changes = false;
                    // A health check from a provider we hadn't registered doesn't have a contract
                    // id, so check if that needs to be set
                    if prov.contract_id.is_empty() {
                        prov.contract_id = info.contract_id.clone();
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
impl<S: Store + Send + Sync, C: ClaimsSource + Send + Sync> Worker for EventWorker<S, C> {
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

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use super::*;

    use crate::{storage::ReadStore, test_util::TestStore};

    #[async_trait::async_trait]
    impl ClaimsSource for HashMap<String, Claims> {
        async fn get_claims(&self) -> anyhow::Result<HashMap<String, Claims>> {
            Ok(self.clone())
        }
    }

    // NOTE: This test is rather long because we want to run through what an actual state generation
    // loop would look like. This mostly covers happy path, while the other tests cover more of the
    // edge cases
    #[tokio::test]
    async fn test_all_state() {
        let store = Arc::new(TestStore::default());
        let worker = EventWorker::new(store.clone(), HashMap::default());

        let lattice_id = "all_state";
        let host1_id = "DS1".to_string();
        let host2_id = "starkiller".to_string();

        /***********************************************************/
        /******************** Host Start Tests *********************/
        /***********************************************************/
        let labels = HashMap::from([("superweapon".to_string(), "true".to_string())]);
        worker
            .handle_host_started(
                lattice_id,
                &HostStarted {
                    friendly_name: "death-star-42".to_string(),
                    id: host1_id.clone(),
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
                    id: host2_id.clone(),
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
        /******************** Actor Start Tests ********************/
        /***********************************************************/

        let actor1 = ActorStarted {
            claims: ActorClaims {
                call_alias: Some("Grand Moff".into()),
                capabilites: vec!["empire:command".into()],
                issuer: "Sheev Palpatine".into(),
                name: "Grand Moff Tarkin".into(),
                version: "0.1.0".into(),
                ..Default::default()
            },
            image_ref: "coruscant.galactic.empire/tarkin:0.1.0".into(),
            public_key: "TARKIN".into(),
            host_id: host1_id.clone(),
            annotations: HashMap::default(),
            api_version: 0,
            instance_id: String::new(),
        };

        let actor2 = ActorStarted {
            claims: ActorClaims {
                call_alias: Some("Darth".into()),
                capabilites: vec!["empire:command".into(), "force_user:sith".into()],
                issuer: "Sheev Palpatine".into(),
                name: "Darth Vader".into(),
                version: "0.1.0".into(),
                ..Default::default()
            },
            image_ref: "coruscant.galactic.empire/vader:0.1.0".into(),
            public_key: "DARTHVADER".into(),
            host_id: host1_id.clone(),
            annotations: HashMap::default(),
            api_version: 0,
            instance_id: String::new(),
        };

        // Start a single actor first just to make sure that works properly, then start all of them
        // across the two hosts
        worker
            .handle_actor_started(lattice_id, &actor1)
            .await
            .expect("Should be able to handle actor event");

        let actors = store.list::<Actor>(lattice_id).await.unwrap();
        assert_eq!(actors.len(), 1, "Should only be 1 actor in state");
        assert_actor(&actors, &actor1, &[(&host1_id, 1)]);

        worker
            .handle_actor_started(lattice_id, &actor1)
            .await
            .expect("Should be able to handle actor event");

        for _ in 0..2 {
            worker
                .handle_actor_started(lattice_id, &actor2)
                .await
                .expect("Should be able to handle actor event");

            // Start the actors on the other host as well
            worker
                .handle_actor_started(
                    lattice_id,
                    &ActorStarted {
                        host_id: host2_id.clone(),
                        ..actor1.clone()
                    },
                )
                .await
                .expect("Should be able to handle actor event");

            worker
                .handle_actor_started(
                    lattice_id,
                    &ActorStarted {
                        host_id: host2_id.clone(),
                        ..actor2.clone()
                    },
                )
                .await
                .expect("Should be able to handle actor event");
        }

        let actors = store.list::<Actor>(lattice_id).await.unwrap();
        assert_eq!(
            actors.len(),
            2,
            "Should have the correct number of actors in state"
        );

        // Check the first actor
        assert_actor(&actors, &actor1, &[(&host1_id, 2), (&host2_id, 2)]);
        // Check the second actor
        assert_actor(&actors, &actor2, &[(&host1_id, 2), (&host2_id, 2)]);

        /***********************************************************/
        /****************** Provider Start Tests *******************/
        /***********************************************************/

        let provider1 = ProviderStarted {
            claims: ProviderClaims {
                issuer: "Sheev Palpatine".into(),
                name: "Force Choke".into(),
                version: "0.1.0".into(),
                ..Default::default()
            },
            image_ref: "coruscant.galactic.empire/force_choke:0.1.0".into(),
            public_key: "CHOKE".into(),
            host_id: host1_id.clone(),
            annotations: HashMap::default(),
            instance_id: String::new(),
            contract_id: "force_user:sith".into(),
            link_name: "default".into(),
        };

        let provider2 = ProviderStarted {
            claims: ProviderClaims {
                issuer: "Sheev Palpatine".into(),
                name: "Death Star Laser".into(),
                version: "0.1.0".into(),
                ..Default::default()
            },
            image_ref: "coruscant.galactic.empire/laser:0.1.0".into(),
            public_key: "BYEBYEALDERAAN".into(),
            host_id: host2_id.clone(),
            annotations: HashMap::default(),
            instance_id: String::new(),
            contract_id: "empire:command".into(),
            link_name: "default".into(),
        };

        worker
            .handle_provider_started(lattice_id, &provider1)
            .await
            .expect("Should be able to handle provider event");
        let providers = store.list::<Provider>(lattice_id).await.unwrap();
        assert_eq!(providers.len(), 1, "Should only be 1 provider in state");
        assert_provider(&providers, &provider1, &[&host1_id]);

        // Now start the second provider on both hosts (so we can test some things in the next test)
        worker
            .handle_provider_started(lattice_id, &provider2)
            .await
            .expect("Should be able to handle provider event");
        worker
            .handle_provider_started(
                lattice_id,
                &ProviderStarted {
                    host_id: host1_id.clone(),
                    ..provider2.clone()
                },
            )
            .await
            .expect("Should be able to handle provider event");
        let providers = store.list::<Provider>(lattice_id).await.unwrap();
        assert_eq!(providers.len(), 2, "Should only be 2 providers in state");
        assert_provider(&providers, &provider2, &[&host1_id, &host2_id]);

        // TODO(thomastaylor312): This is just a reminder that if we add in host state updating on
        // provider/actor events, we should test that here before we hit a host heartbeat

        /***********************************************************/
        /******************* Host Heartbeat Test *******************/
        /***********************************************************/

        worker
            .handle_host_heartbeat(
                lattice_id,
                &HostHeartbeat {
                    actors: HashMap::from([
                        (actor1.public_key.clone(), 2),
                        (actor2.public_key.clone(), 2),
                    ]),
                    friendly_name: "death-star-42".to_string(),
                    labels: labels.clone(),
                    providers: vec![
                        ProviderInfo {
                            contract_id: provider1.contract_id.clone(),
                            link_name: provider1.link_name.clone(),
                            public_key: provider1.public_key.clone(),
                        },
                        ProviderInfo {
                            contract_id: provider2.contract_id.clone(),
                            link_name: provider2.link_name.clone(),
                            public_key: provider2.public_key.clone(),
                        },
                    ],
                    uptime_human: "30s".into(),
                    uptime_seconds: 30,
                    version: semver::Version::parse("0.61.0").unwrap(),
                    id: host1_id.clone(),
                    annotations: HashMap::default(),
                },
            )
            .await
            .expect("Should be able to handle host heartbeat");

        worker
            .handle_host_heartbeat(
                lattice_id,
                &HostHeartbeat {
                    actors: HashMap::from([
                        (actor1.public_key.clone(), 2),
                        (actor2.public_key.clone(), 2),
                    ]),
                    friendly_name: "starkiller-base-2015".to_string(),
                    labels: labels2.clone(),
                    providers: vec![ProviderInfo {
                        contract_id: provider2.contract_id.clone(),
                        link_name: provider2.link_name.clone(),
                        public_key: provider2.public_key.clone(),
                    }],
                    uptime_human: "30s".into(),
                    uptime_seconds: 30,
                    version: semver::Version::parse("0.61.0").unwrap(),
                    id: host2_id.clone(),
                    annotations: HashMap::default(),
                },
            )
            .await
            .expect("Should be able to handle host heartbeat");

        // Check that hosts got updated properly
        let hosts = store.list::<Host>(lattice_id).await.unwrap();
        assert_eq!(hosts.len(), 2, "Should only have 2 hosts");
        let host = hosts.get(&host1_id).expect("Host should still exist");
        assert_eq!(
            host.actors.len(),
            2,
            "Should have two different actors running"
        );
        assert_eq!(
            host.providers.len(),
            2,
            "Should have two different providers running"
        );
        let host = hosts.get(&host2_id).expect("Host should still exist");
        assert_eq!(
            host.actors.len(),
            2,
            "Should have two different actors running"
        );
        assert_eq!(
            host.providers.len(),
            1,
            "Should have a single provider running"
        );

        // Check that our actor and provider data is still correct.
        let providers = store.list::<Provider>(lattice_id).await.unwrap();
        assert_eq!(providers.len(), 2, "Should still have 2 providers in state");
        assert_provider(&providers, &provider1, &[&host1_id]);
        assert_provider(&providers, &provider2, &[&host1_id, &host2_id]);

        let actors = store.list::<Actor>(lattice_id).await.unwrap();
        assert_eq!(actors.len(), 2, "Should still have 2 actors in state");
        assert_actor(&actors, &actor1, &[(&host1_id, 2), (&host2_id, 2)]);
        assert_actor(&actors, &actor2, &[(&host1_id, 2), (&host2_id, 2)]);

        /***********************************************************/
        /******************** Actor Stop Tests *********************/
        /***********************************************************/

        // Stop them on one host first
        let stopped = ActorStopped {
            instance_id: actor1.instance_id.clone(),
            public_key: actor1.public_key.clone(),
            host_id: host1_id.clone(),
        };
        worker
            .handle_actor_stopped(lattice_id, &stopped)
            .await
            .expect("Should be able to handle actor stop event");
        worker
            .handle_actor_stopped(lattice_id, &stopped)
            .await
            .expect("Should be able to handle actor stop event");

        let actors = store.list::<Actor>(lattice_id).await.unwrap();
        assert_eq!(actors.len(), 2, "Should still have 2 actors in state");
        assert_actor(&actors, &actor1, &[(&host2_id, 2)]);
        assert_actor(&actors, &actor2, &[(&host1_id, 2), (&host2_id, 2)]);

        // Now stop on the other
        let stopped = ActorStopped {
            host_id: host2_id.clone(),
            ..stopped
        };

        worker
            .handle_actor_stopped(lattice_id, &stopped)
            .await
            .expect("Should be able to handle actor stop event");
        worker
            .handle_actor_stopped(lattice_id, &stopped)
            .await
            .expect("Should be able to handle actor stop event");

        let actors = store.list::<Actor>(lattice_id).await.unwrap();
        assert_eq!(actors.len(), 1, "Should only have 1 actor in state");
        // Double check the the old one is still ok
        assert_actor(&actors, &actor2, &[(&host1_id, 2), (&host2_id, 2)]);

        /***********************************************************/
        /******************* Provider Stop Tests *******************/
        /***********************************************************/

        worker
            .handle_provider_stopped(
                lattice_id,
                &ProviderStopped {
                    contract_id: provider2.contract_id.clone(),
                    instance_id: provider2.instance_id.clone(),
                    link_name: provider2.link_name.clone(),
                    public_key: provider2.public_key.clone(),
                    reason: String::new(),
                    host_id: host1_id.clone(),
                },
            )
            .await
            .expect("Should be able to handle provider stop event");

        let providers = store.list::<Provider>(lattice_id).await.unwrap();
        assert_eq!(providers.len(), 2, "Should still have 2 providers in state");
        assert_provider(&providers, &provider1, &[&host1_id]);
        assert_provider(&providers, &provider2, &[&host2_id]);

        /***********************************************************/
        /***************** Heartbeat Tests Part 2 ******************/
        /***********************************************************/

        // Heartbeat the first host and make sure nothing has changed
        worker
            .handle_host_heartbeat(
                lattice_id,
                &HostHeartbeat {
                    actors: HashMap::from([(actor2.public_key.clone(), 2)]),
                    friendly_name: "death-star-42".to_string(),
                    labels,
                    providers: vec![ProviderInfo {
                        contract_id: provider1.contract_id.clone(),
                        link_name: provider1.link_name.clone(),
                        public_key: provider1.public_key.clone(),
                    }],
                    uptime_human: "60s".into(),
                    uptime_seconds: 60,
                    version: semver::Version::parse("0.61.0").unwrap(),
                    id: host1_id.clone(),
                    annotations: HashMap::default(),
                },
            )
            .await
            .expect("Should be able to handle host heartbeat");

        worker
            .handle_host_heartbeat(
                lattice_id,
                &HostHeartbeat {
                    actors: HashMap::from([(actor2.public_key.clone(), 2)]),
                    friendly_name: "starkiller-base-2015".to_string(),
                    labels: labels2,
                    providers: vec![ProviderInfo {
                        contract_id: provider2.contract_id.clone(),
                        link_name: provider2.link_name.clone(),
                        public_key: provider2.public_key.clone(),
                    }],
                    uptime_human: "60s".into(),
                    uptime_seconds: 60,
                    version: semver::Version::parse("0.61.0").unwrap(),
                    id: host2_id.clone(),
                    annotations: HashMap::default(),
                },
            )
            .await
            .expect("Should be able to handle host heartbeat");

        // Check that hosts got updated properly
        let hosts = store.list::<Host>(lattice_id).await.unwrap();
        assert_eq!(hosts.len(), 2, "Should only have 2 hosts");
        let host = hosts.get(&host1_id).expect("Host should still exist");
        assert_eq!(host.actors.len(), 1, "Should have 1 actor running");
        assert_eq!(host.providers.len(), 1, "Should have 1 provider running");
        let host = hosts.get(&host2_id).expect("Host should still exist");
        assert_eq!(host.actors.len(), 1, "Should have 1 actor running");
        assert_eq!(
            host.providers.len(),
            1,
            "Should have a single provider running"
        );

        // Double check providers and actors are the same
        let actors = store.list::<Actor>(lattice_id).await.unwrap();
        assert_eq!(actors.len(), 1, "Should only have 1 actor in state");
        assert_actor(&actors, &actor2, &[(&host1_id, 2), (&host2_id, 2)]);

        let providers = store.list::<Provider>(lattice_id).await.unwrap();
        assert_eq!(providers.len(), 2, "Should still have 2 providers in state");
        assert_provider(&providers, &provider1, &[&host1_id]);
        assert_provider(&providers, &provider2, &[&host2_id]);

        /***********************************************************/
        /********************* Host Stop Tests *********************/
        /***********************************************************/

        worker
            .handle_host_stopped(
                lattice_id,
                &HostStopped {
                    labels: HashMap::default(),
                    id: host1_id.clone(),
                },
            )
            .await
            .expect("Should be able to handle host stopped event");

        let hosts = store.list::<Host>(lattice_id).await.unwrap();
        assert_eq!(hosts.len(), 1, "Should only have 1 host");
        let host = hosts.get(&host2_id).expect("Host should still exist");
        assert_eq!(host.actors.len(), 1, "Should have 1 actor running");
        assert_eq!(
            host.providers.len(),
            1,
            "Should have a single provider running"
        );

        // Double check providers and actors are the same
        let actors = store.list::<Actor>(lattice_id).await.unwrap();
        assert_eq!(actors.len(), 1, "Should only have 1 actor in state");
        assert_actor(&actors, &actor2, &[(&host2_id, 2)]);

        let providers = store.list::<Provider>(lattice_id).await.unwrap();
        assert_eq!(providers.len(), 1, "Should now have 1 provider in state");
        assert_provider(&providers, &provider2, &[&host2_id]);
    }

    #[tokio::test]
    async fn test_discover_running_host() {
        let actor1_id = "SKYWALKER".to_string();
        let actor2_id = "ORGANA".to_string();
        let lattice_id = "discover_running_host";
        let claims = HashMap::from([
            (
                actor1_id.clone(),
                Claims {
                    name: "tosche_station".to_string(),
                    capabilities: vec!["wasmcloud:httpserver".to_string()],
                    issuer: "GEORGELUCAS".to_string(),
                },
            ),
            (
                actor2_id.clone(),
                Claims {
                    name: "alderaan".to_string(),
                    capabilities: vec!["wasmcloud:keyvalue".to_string()],
                    issuer: "GEORGELUCAS".to_string(),
                },
            ),
        ]);
        let store = Arc::new(TestStore::default());
        let worker = EventWorker::new(store.clone(), claims.clone());

        let provider_id = "HYPERDRIVE".to_string();
        let link_name = "default".to_string();
        let host_id = "WHATAPIECEOFJUNK".to_string();
        // Heartbeat with actors and providers that don't exist in the store yet
        worker
            .handle_host_heartbeat(
                lattice_id,
                &HostHeartbeat {
                    actors: HashMap::from([(actor1_id.clone(), 2), (actor2_id.clone(), 1)]),
                    friendly_name: "millenium_falcon-1977".to_string(),
                    labels: HashMap::default(),
                    providers: vec![ProviderInfo {
                        contract_id: "lightspeed".into(),
                        link_name: link_name.clone(),
                        public_key: provider_id.clone(),
                    }],
                    uptime_human: "60s".into(),
                    uptime_seconds: 60,
                    version: semver::Version::parse("0.61.0").unwrap(),
                    id: host_id.clone(),
                    annotations: HashMap::default(),
                },
            )
            .await
            .expect("Should be able to handle host heartbeat");

        // We test that the host is created in other tests, so just check that the actors and
        // providers were created properly
        let actors = store.list::<Actor>(lattice_id).await.unwrap();
        assert_eq!(actors.len(), 2, "Store should now have two actors");
        let actor = actors.get(&actor1_id).expect("Actor should exist");
        let expected = claims.get(&actor1_id).unwrap();
        assert_eq!(actor.name, expected.name, "Data should match");
        assert_eq!(
            actor.capabilities, expected.capabilities,
            "Data should match"
        );
        assert_eq!(actor.issuer, expected.issuer, "Data should match");
        assert_eq!(
            *actor
                .count
                .get(&host_id)
                .expect("Host should exist in count"),
            2,
            "Should have the right number of actors running"
        );

        let actor = actors.get(&actor2_id).expect("Actor should exist");
        let expected = claims.get(&actor2_id).unwrap();
        assert_eq!(actor.name, expected.name, "Data should match");
        assert_eq!(
            actor.capabilities, expected.capabilities,
            "Data should match"
        );
        assert_eq!(actor.issuer, expected.issuer, "Data should match");
        assert_eq!(
            *actor
                .count
                .get(&host_id)
                .expect("Host should exist in count"),
            1,
            "Should have the right number of actors running"
        );

        let providers = store.list::<Provider>(lattice_id).await.unwrap();
        assert_eq!(providers.len(), 1, "Should have 1 provider in the store");
        let provider = providers
            .get(&crate::storage::provider_id(&provider_id, &link_name))
            .expect("Provider should exist");
        assert_eq!(provider.id, provider_id, "Data should match");
        assert_eq!(provider.link_name, link_name, "Data should match");
        assert!(
            provider.hosts.contains_key(&host_id),
            "Should have found host in provider store"
        );
    }

    #[tokio::test]
    async fn test_provider_status_update() {
        let store = Arc::new(TestStore::default());
        let worker = EventWorker::new(store.clone(), HashMap::default());

        let lattice_id = "provider_status";
        let host_id = "CLOUDCITY".to_string();

        // Trigger a provider started and then a health check
        let provider = ProviderStarted {
            claims: ProviderClaims {
                issuer: "Lando Calrissian".into(),
                name: "Tibanna Gas Mining".into(),
                version: "0.1.0".into(),
                ..Default::default()
            },
            image_ref: "bespin.lando.inc/tibanna:0.1.0".into(),
            public_key: "GAS".into(),
            host_id: host_id.clone(),
            annotations: HashMap::default(),
            instance_id: String::new(),
            contract_id: "mining".into(),
            link_name: "default".into(),
        };

        worker
            .handle_provider_started(lattice_id, &provider)
            .await
            .expect("Should be able to handle provider started event");
        worker
            .handle_provider_health_check(
                lattice_id,
                &host_id,
                &ProviderHealthCheckInfo {
                    link_name: provider.link_name.clone(),
                    public_key: provider.public_key.clone(),
                },
                false,
            )
            .await
            .expect("Should be able to handle a provider health check event");

        let providers = store.list::<Provider>(lattice_id).await.unwrap();
        assert_eq!(providers.len(), 1, "Only 1 provider should exist");
        let prov = providers
            .get(&crate::storage::provider_id(
                &provider.public_key,
                &provider.link_name,
            ))
            .expect("Provider should exist");
        assert!(
            matches!(
                prov.hosts
                    .get(&host_id)
                    .expect("Should find status for host"),
                ProviderStatus::Running
            ),
            "Provider should have a running status"
        );

        // Now try a failed status
        worker
            .handle_provider_health_check(
                lattice_id,
                &host_id,
                &ProviderHealthCheckInfo {
                    link_name: provider.link_name.clone(),
                    public_key: provider.public_key.clone(),
                },
                true,
            )
            .await
            .expect("Should be able to handle a provider health check event");

        let providers = store.list::<Provider>(lattice_id).await.unwrap();
        assert_eq!(providers.len(), 1, "Only 1 provider should exist");
        let prov = providers
            .get(&crate::storage::provider_id(
                &provider.public_key,
                &provider.link_name,
            ))
            .expect("Provider should exist");
        assert!(
            matches!(
                prov.hosts
                    .get(&host_id)
                    .expect("Should find status for host"),
                ProviderStatus::Failed
            ),
            "Provider should have a running status"
        );
    }

    #[tokio::test]
    async fn test_provider_contract_id_from_heartbeat() {
        let store = Arc::new(TestStore::default());
        let worker = EventWorker::new(store.clone(), HashMap::default());

        let lattice_id = "provider_contract_id";
        let host_id = "BEGGARSCANYON";
        let link_name = "default";
        let public_key = "SKYHOPPER";
        let contract_id = "blasting:womprats";

        // Health check a provider that we don't have in the store yet
        worker
            .handle_provider_health_check(
                lattice_id,
                host_id,
                &ProviderHealthCheckInfo {
                    link_name: link_name.to_string(),
                    public_key: public_key.to_string(),
                },
                false,
            )
            .await
            .expect("Should be able to handle a provider health check event");

        // Now heartbeat a host
        worker
            .handle_host_heartbeat(
                lattice_id,
                &HostHeartbeat {
                    actors: HashMap::default(),
                    friendly_name: "tatooine-1977".to_string(),
                    labels: HashMap::default(),
                    providers: vec![ProviderInfo {
                        contract_id: contract_id.to_string(),
                        link_name: link_name.to_string(),
                        public_key: public_key.to_string(),
                    }],
                    uptime_human: "60s".into(),
                    uptime_seconds: 60,
                    version: semver::Version::parse("0.61.0").unwrap(),
                    id: "TATOOINE".to_string(),
                    annotations: HashMap::default(),
                },
            )
            .await
            .expect("Should be able to handle host heartbeat");

        // Now check that our provider exists and has the contract id set
        let providers = store.list::<Provider>(lattice_id).await.unwrap();
        assert_eq!(providers.len(), 1, "Only 1 provider should exist");
        let prov = providers
            .get(&crate::storage::provider_id(public_key, link_name))
            .expect("Provider should exist");
        assert_eq!(
            prov.contract_id, contract_id,
            "Provider should have contract id set"
        );
    }

    #[tokio::test]
    async fn test_heartbeat_updates_stale_data() {
        let store = Arc::new(TestStore::default());
        let worker = EventWorker::new(store.clone(), HashMap::default());

        let lattice_id = "update_data";
        let host_id = "jabbaspalace";

        // Store some existing stuff
        store
            .store(
                lattice_id,
                "jabba".to_string(),
                Actor {
                    id: "jabba".to_string(),
                    count: HashMap::from([(host_id.to_string(), 1)]),
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
                    actors: HashMap::from([("jabba".to_string(), 2)]),
                    friendly_name: "palace-1983".to_string(),
                    labels: HashMap::default(),
                    providers: vec![],
                    uptime_human: "60s".into(),
                    uptime_seconds: 60,
                    version: semver::Version::parse("0.61.0").unwrap(),
                    id: host_id.to_string(),
                    annotations: HashMap::default(),
                },
            )
            .await
            .expect("Should be able to handle host heartbeat");

        let actors = store.list::<Actor>(lattice_id).await.unwrap();
        assert_eq!(actors.len(), 1, "Should have 1 actor in the store");
        let actor = actors.get("jabba").expect("Actor should exist");
        assert_eq!(actor.count(), 2, "Should now have 2 actors");
    }

    fn assert_actor(
        actors: &HashMap<String, Actor>,
        event: &ActorStarted,
        expected_counts: &[(&str, usize)],
    ) {
        let actor = actors
            .get(&event.public_key)
            .expect("Actor should exist in store");
        assert_eq!(
            actor.id, event.public_key,
            "Actor ID stored should be correct"
        );
        assert_eq!(
            actor.call_alias, event.claims.call_alias,
            "Other data in actor should be correct"
        );
        assert_eq!(
            expected_counts.len(),
            actor.count.len(),
            "Should have the proper number of hosts the actor is running on"
        );
        for (expected_host, expected_count) in expected_counts.iter() {
            assert_eq!(
                actor
                    .count
                    .get(*expected_host)
                    .cloned()
                    .expect("Actor should have a count for the host"),
                *expected_count,
                "Actor count on host should be correct"
            );
        }
    }

    fn assert_provider(
        providers: &HashMap<String, Provider>,
        event: &ProviderStarted,
        running_on_hosts: &[&str],
    ) {
        let provider = providers
            .get(&crate::storage::provider_id(
                &event.public_key,
                &event.link_name,
            ))
            .expect("Correct provider should exist in store");
        assert_eq!(
            provider.name, event.claims.name,
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
