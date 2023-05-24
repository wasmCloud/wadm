use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::OnceCell;
use tracing::{instrument, trace};

use crate::{
    commands::{Command, DeleteLinkdef, PutLinkdef},
    events::{Event, LinkdefDeleted, ProviderHealthCheckPassed, ProviderHealthCheckStatus},
    model::TraitProperty,
    scaler::Scaler,
    storage::{Actor, Provider, ReadStore},
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
pub struct LinkScaler<S: ReadStore + Send + Sync> {
    pub config: LinkScalerConfig,
    store: S,
    /// Actor ID, stored in a OnceCell to facilitate more efficient fetches
    actor_id: OnceCell<String>,
    /// Provider ID, stored in a OnceCell to facilitate more efficient fetches
    provider_id: OnceCell<String>,
    id: String,
}

#[async_trait]
impl<S: ReadStore + Send + Sync> Scaler for LinkScaler<S> {
    fn id(&self) -> &str {
        &self.id
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
            // TODO(thomastaylor312): Come up with a better way to reconcile on startup rather than
            // putting a linkdef on every provider status event. Probably need to put a ctl client
            // in here to do the check, but that was nasty to do right now
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
            // TODO: check to see if linkdef exists. Might need to be deleted first if values differ
            Ok(vec![Command::PutLinkdef(PutLinkdef {
                actor_id: actor_id.to_owned(),
                provider_id: provider_id.to_owned(),
                link_name: self.config.provider_link_name.to_owned(),
                contract_id: self.config.provider_contract_id.to_owned(),
                values: self.config.values.to_owned(),
                model_name: self.config.model_name.to_owned(),
            })])
        } else {
            trace!("Actor ID and provider ID are not initialized, skipping linkdef creation");
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

impl<S: ReadStore + Send + Sync> LinkScaler<S> {
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
            id,
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
