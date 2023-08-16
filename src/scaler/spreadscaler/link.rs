use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::{OnceCell, RwLock};
use tracing::{instrument, trace};

use crate::{
    commands::{Command, DeleteLinkdef, PutLinkdef},
    events::{
        Event, LinkdefDeleted, LinkdefSet, ProviderHealthCheckPassed, ProviderHealthCheckStatus,
    },
    model::TraitProperty,
    scaler::Scaler,
    server::StatusInfo,
    storage::{Actor, Provider, ReadStore},
    workers::LinkSource,
    DEFAULT_LINK_NAME,
};

pub const LINK_SCALER_TYPE: &str = "linkdefscaler";

/// Config for a LinkSpreadConfig
pub struct LinkScalerConfig {
    /// OCI, Bindle, or File reference for the actor to link
    actor_reference: String,
    /// OCI, Bindle, or File reference for the provider to link
    provider_reference: String,
    /// Contract ID the provider implements
    provider_contract_id: String,
    /// Contract ID the provider implements
    provider_link_name: String,
    /// Lattice ID the Link is configured for
    lattice_id: String,
    /// The name of the wadm model this SpreadScaler is under
    model_name: String,
    /// Values to attach to this linkdef
    values: HashMap<String, String>,
}

/// The LinkSpreadScaler ensures that link configuration exists on a specified lattice.
pub struct LinkScaler<S, L> {
    pub config: LinkScalerConfig,
    store: S,
    /// Actor ID, stored in a OnceCell to facilitate more efficient fetches
    actor_id: OnceCell<String>,
    /// Provider ID, stored in a OnceCell to facilitate more efficient fetches
    provider_id: OnceCell<String>,
    ctl_client: L,
    id: String,
    status: RwLock<StatusInfo>,
}

#[async_trait]
impl<S, L> Scaler for LinkScaler<S, L>
where
    S: ReadStore + Send + Sync,
    L: LinkSource + Send + Sync,
{
    fn id(&self) -> &str {
        &self.id
    }

    async fn status(&self) -> StatusInfo {
        let _ = self.reconcile().await;
        self.status.read().await.to_owned()
    }

    async fn update_config(&mut self, _config: TraitProperty) -> Result<Vec<Command>> {
        // NOTE(brooksmtownsend): Updating a link scaler essentially means you're creating
        // a totally new scaler, so just do that instead.
        self.reconcile().await
    }

    #[instrument(level = "trace", skip_all, fields(scaler_id = %self.id))]
    async fn handle_event(&self, event: &Event) -> Result<Vec<Command>> {
        match event {
            // We can only publish links once we know actor and provider IDs,
            // so we should pay attention to the Started events if we don't know them yet
            Event::ActorsStarted(actor_started)
                if !self.actor_id.initialized()
                    && actor_started.image_ref == self.config.actor_reference =>
            {
                self.reconcile().await
            }
            Event::ProviderHealthCheckPassed(ProviderHealthCheckPassed { data, .. })
            | Event::ProviderHealthCheckStatus(ProviderHealthCheckStatus { data, .. })
                if self
                    .provider_id()
                    .await
                    .map(|id| id == data.public_key)
                    .unwrap_or(false)
                    && data.contract_id == self.config.provider_contract_id
                    && data.link_name == self.config.provider_link_name =>
            {
                // Wait until we know the provider is healthy before we link. This also avoids the race condition
                self.reconcile().await
            }
            Event::LinkdefDeleted(LinkdefDeleted { linkdef })
            | Event::LinkdefSet(LinkdefSet { linkdef })
                if linkdef.contract_id == self.config.provider_contract_id
                    && linkdef.actor_id == self.actor_id().await.unwrap_or_default()
                    && linkdef.provider_id == self.provider_id().await.unwrap_or_default()
                    && linkdef.link_name == self.config.provider_link_name =>
            {
                self.reconcile().await
            }
            _ => Ok(Vec::new()),
        }
    }

    #[instrument(level = "trace", skip_all, fields(actor_ref = %self.config.actor_reference, provider_ref = %self.config.provider_reference, link_name = %self.config.provider_link_name, scaler_id = %self.id))]
    async fn reconcile(&self) -> Result<Vec<Command>> {
        if let (Ok(actor_id), Ok(provider_id)) = (self.actor_id().await, self.provider_id().await) {
            let linkdefs = self.ctl_client.get_links().await?;
            let (exists, _values_different) = linkdefs
                .into_iter()
                .find(|linkdef| {
                    linkdef.actor_id == actor_id
                        && linkdef.provider_id == provider_id
                        && linkdef.link_name == self.config.provider_link_name
                        && linkdef.contract_id == self.config.provider_contract_id
                })
                .map(|linkdef| (true, linkdef.values != self.config.values))
                .unwrap_or((false, false));

            // TODO: Reenable this functionality once we figure out https://github.com/wasmCloud/wadm/issues/123

            // If it already exists, but values are different, we need to have a delete event first
            // and recreate it with the correct values second
            // let mut commands = values_different
            //     .then(|| {
            //         trace!("Linkdef exists, but values are different, deleting and recreating");
            //         vec![Command::DeleteLinkdef(DeleteLinkdef {
            //             actor_id: actor_id.to_owned(),
            //             provider_id: provider_id.to_owned(),
            //             contract_id: self.config.provider_contract_id.to_owned(),
            //             link_name: self.config.provider_link_name.to_owned(),
            //             model_name: self.config.model_name.to_owned(),
            //         })]
            //     })
            //     .unwrap_or_default();

            // if exists && !values_different {
            //     trace!("Linkdef already exists, skipping");
            // } else if !exists || values_different {
            //     trace!("Linkdef does not exist or needs to be recreated");
            //     commands.push(Command::PutLinkdef(PutLinkdef {
            //         actor_id: actor_id.to_owned(),
            //         provider_id: provider_id.to_owned(),
            //         link_name: self.config.provider_link_name.to_owned(),
            //         contract_id: self.config.provider_contract_id.to_owned(),
            //         values: self.config.values.to_owned(),
            //         model_name: self.config.model_name.to_owned(),
            //     }))
            // };

            let commands = if !exists {
                *self.status.write().await = StatusInfo::compensating(&format!(
                    "Putting link definition between {actor_id} and {provider_id}"
                ));
                vec![Command::PutLinkdef(PutLinkdef {
                    actor_id: actor_id.to_owned(),
                    provider_id: provider_id.to_owned(),
                    link_name: self.config.provider_link_name.to_owned(),
                    contract_id: self.config.provider_contract_id.to_owned(),
                    values: self.config.values.to_owned(),
                    model_name: self.config.model_name.to_owned(),
                })]
            } else {
                *self.status.write().await = StatusInfo::ready("");
                Vec::with_capacity(0)
            };
            Ok(commands)
        } else {
            trace!("Actor ID and provider ID are not initialized, skipping linkdef creation");
            *self.status.write().await = StatusInfo::compensating(&format!(
                "Linkdef pending, waiting for {} and {} to start",
                self.config.actor_reference, self.config.provider_reference
            ));
            Ok(Vec::new())
        }
    }

    async fn cleanup(&self) -> Result<Vec<Command>> {
        if let (Ok(actor_id), Ok(provider_id)) = (self.actor_id().await, self.provider_id().await) {
            Ok(vec![Command::DeleteLinkdef(DeleteLinkdef {
                actor_id: actor_id.to_owned(),
                provider_id: provider_id.to_owned(),
                contract_id: self.config.provider_contract_id.to_owned(),
                link_name: self.config.provider_link_name.to_owned(),
                model_name: self.config.model_name.to_owned(),
            })])
        } else {
            // If we never knew the actor/provider ID, this link scaler
            // never created the link
            Ok(Vec::new())
        }
    }
}

impl<S: ReadStore + Send + Sync, L: LinkSource> LinkScaler<S, L> {
    /// Construct a new LinkScaler with specified configuration values
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        store: S,
        actor_reference: String,
        provider_reference: String,
        provider_contract_id: String,
        provider_link_name: Option<String>,
        lattice_id: String,
        model_name: String,
        values: Option<HashMap<String, String>>,
        ctl_client: L,
    ) -> Self {
        let provider_link_name =
            provider_link_name.unwrap_or_else(|| DEFAULT_LINK_NAME.to_string());
        // NOTE(thomastaylor312): Yep, this is gnarly, but it was all the information that would be
        // useful to have if uniquely identifying a link scaler
        let id = format!("{LINK_SCALER_TYPE}-{model_name}-{provider_link_name}-{actor_reference}-{provider_reference}");
        Self {
            store,
            actor_id: OnceCell::new(),
            provider_id: OnceCell::new(),
            config: LinkScalerConfig {
                actor_reference,
                provider_reference,
                provider_contract_id,
                provider_link_name,
                lattice_id,
                model_name,
                values: values.unwrap_or_default(),
            },
            ctl_client,
            id,
            status: RwLock::new(StatusInfo::compensating("")),
        }
    }

    /// Helper function to retrieve the actor ID for the configured actor
    async fn actor_id(&self) -> Result<&str> {
        self.actor_id
            .get_or_try_init(|| async {
                self.store
                    .list::<Actor>(&self.config.lattice_id)
                    .await?
                    .iter()
                    .find(|(_id, actor)| actor.reference == self.config.actor_reference)
                    .map(|(_id, actor)| actor.id.to_owned())
                    // Default here means the below `get` will find zero running actors, which is fine because
                    // that accurately describes the current lattice having zero instances.
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "Couldn't find an actor id for the actor reference {}",
                            self.config.actor_reference
                        )
                    })
            })
            .await
            .map(|id| id.as_str())
    }

    /// Helper function to retrieve the provider ID for the configured provider
    async fn provider_id(&self) -> Result<&str> {
        self.provider_id
            .get_or_try_init(|| async {
                self.store
                    .list::<Provider>(&self.config.lattice_id)
                    .await
                    .unwrap_or_default()
                    .iter()
                    .find(|(_id, provider)| provider.reference == self.config.provider_reference)
                    .map(|(_id, provider)| provider.id.to_owned())
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "Couldn't find a provider id for the provider reference {}",
                            self.config.provider_reference
                        )
                    })
            })
            .await
            .map(|id| id.as_str())
    }
}

#[cfg(test)]
mod test {
    use wasmcloud_control_interface::LinkDefinition;

    use super::*;

    use crate::{
        storage::Store,
        test_util::{TestLatticeSource, TestStore},
    };

    async fn create_store(lattice_id: &str, actor_ref: &str, provider_ref: &str) -> TestStore {
        let store = TestStore::default();
        store
            .store(
                lattice_id,
                "actor".to_string(),
                Actor {
                    id: "actor".to_string(),
                    reference: actor_ref.to_owned(),
                    ..Default::default()
                },
            )
            .await
            .expect("Couldn't store actor");
        store
            .store(
                lattice_id,
                "provider".to_string(),
                Provider {
                    id: "provider".to_string(),
                    reference: provider_ref.to_owned(),
                    ..Default::default()
                },
            )
            .await
            .expect("Couldn't store actor");
        store
    }

    #[tokio::test]
    async fn test_no_linkdef() {
        let lattice_id = "no-linkdef".to_string();
        let actor_ref = "actor_ref".to_string();
        let provider_ref = "provider_ref".to_string();

        let scaler = LinkScaler::new(
            create_store(&lattice_id, &actor_ref, &provider_ref).await,
            actor_ref,
            provider_ref,
            "contract".to_string(),
            None,
            lattice_id.clone(),
            "model".to_string(),
            None,
            TestLatticeSource::default(),
        );

        // Run a reconcile and make sure it returns a single put linkdef command
        let commands = scaler.reconcile().await.expect("Couldn't reconcile");
        assert_eq!(commands.len(), 1, "Expected 1 command, got {commands:?}");
        assert!(matches!(commands[0], Command::PutLinkdef(_)));
    }

    // TODO: Uncomment once https://github.com/wasmCloud/wadm/issues/123 is fixed

    // #[tokio::test]
    // async fn test_different_values() {
    //     let lattice_id = "different-values".to_string();
    //     let actor_ref = "actor_ref".to_string();
    //     let provider_ref = "provider_ref".to_string();

    //     let values = HashMap::from([("foo".to_string(), "bar".to_string())]);

    //     let mut linkdef = LinkDefinition::default();
    //     linkdef.actor_id = "actor".to_string();
    //     linkdef.provider_id = "provider".to_string();
    //     linkdef.contract_id = "contract".to_string();
    //     linkdef.link_name = "default".to_string();
    //     linkdef.values = [("foo".to_string(), "nope".to_string())].into();

    //     let scaler = LinkScaler::new(
    //         create_store(&lattice_id, &actor_ref, &provider_ref).await,
    //         actor_ref,
    //         provider_ref,
    //         "contract".to_string(),
    //         None,
    //         lattice_id.clone(),
    //         "model".to_string(),
    //         Some(values),
    //         TestLatticeSource {
    //             links: vec![linkdef],
    //             ..Default::default()
    //         },
    //     );

    //     let commands = scaler.reconcile().await.expect("Couldn't reconcile");
    //     assert_eq!(commands.len(), 2);
    //     assert!(matches!(commands[0], Command::DeleteLinkdef(_)));
    //     assert!(matches!(commands[1], Command::PutLinkdef(_)));
    // }

    #[tokio::test]
    async fn test_existing_linkdef() {
        let lattice_id = "existing-linkdef".to_string();
        let actor_ref = "actor_ref".to_string();
        let provider_ref = "provider_ref".to_string();

        let values = HashMap::from([("foo".to_string(), "bar".to_string())]);
        let mut linkdef = LinkDefinition::default();
        linkdef.actor_id = "actor".to_string();
        linkdef.provider_id = "provider".to_string();
        linkdef.contract_id = "contract".to_string();
        linkdef.link_name = "default".to_string();
        linkdef.values = values.clone();

        let scaler = LinkScaler::new(
            create_store(&lattice_id, &actor_ref, &provider_ref).await,
            actor_ref,
            provider_ref,
            "contract".to_string(),
            None,
            lattice_id.clone(),
            "model".to_string(),
            Some(values),
            TestLatticeSource {
                links: vec![linkdef],
                ..Default::default()
            },
        );

        let commands = scaler.reconcile().await.expect("Couldn't reconcile");
        assert_eq!(
            commands.len(),
            0,
            "Scaler shouldn't have returned any commands"
        );
    }
}
